use super::durability::DurabilitySender;
use super::stats::{Payload, WorkerStats};
use super::util::{key_for_id, random_value, KeySelector};
use crate::config::{VariantConfig, WorkloadKind};
use crate::system::{measure_backpressure, ApplicationCounters, ApplicationWindowRecorder};
use anyhow::{bail, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::Db;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const WINDOW_RECORDER_SHARDS: usize = 16;

#[derive(Debug, Clone, Copy)]
struct Arrival {
    scheduled: Instant,
}

pub async fn run_open_phase(
    db: Arc<Db>,
    variant: &VariantConfig,
    duration: Duration,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
) -> Result<(WorkerStats, Duration)> {
    let target_rate = variant
        .target_rate
        .context("open-loop variant has no target rate")?;
    if target_rate == 0 {
        bail!("open-loop target rate must be positive");
    }
    let worker_count = worker_count(target_rate)?;
    let queue_capacity = worker_count;
    let (sender, receiver) = async_channel::bounded::<Arrival>(queue_capacity);
    let scheduler_window = counters
        .as_ref()
        .map(|counters| counters.register_window_recorder());
    let recorder_registry = counters
        .clone()
        .unwrap_or_else(|| Arc::new(ApplicationCounters::new(true)));
    let window_recorders = window_recorders(&recorder_registry);
    let mut tasks = JoinSet::new();
    let variant = Arc::new(variant.clone());
    let mut seed_rng = StdRng::from_os_rng();
    for worker_id in 0..worker_count {
        let db = Arc::clone(&db);
        let receiver = receiver.clone();
        let variant = Arc::clone(&variant);
        let durability = durability.clone();
        let counters = counters.clone();
        let window_recorder = window_recorders[worker_id % window_recorders.len()].clone();
        let rng_seed = seed_rng.random();
        tasks.spawn(async move {
            open_worker(
                db,
                receiver,
                variant,
                durability,
                counters,
                window_recorder,
                rng_seed,
            )
            .await
        });
    }
    drop(receiver);

    let started = Instant::now();
    let deadline = started + duration;
    let period = Duration::from_secs_f64(1.0 / target_rate as f64);
    let mut scheduled = started;
    let mut offered = 0_u64;
    let mut dropped = 0_u64;
    while scheduled < deadline {
        let now = Instant::now();
        if now < scheduled {
            tokio::time::sleep_until(scheduled.into()).await;
        }
        let now = Instant::now();
        let mut offered_batch = 0_u64;
        let mut dropped_batch = 0_u64;
        while scheduled <= now && scheduled < deadline {
            offered += 1;
            offered_batch += 1;
            if sender.try_send(Arrival { scheduled }).is_err() {
                dropped += 1;
                dropped_batch += 1;
            }
            scheduled += period;
        }
        if let Some(window) = &scheduler_window {
            window.record_offered(offered_batch, dropped_batch);
        }
    }
    let now = Instant::now();
    if now < deadline {
        tokio::time::sleep_until(deadline.into()).await;
    }
    let scheduler_elapsed = started.elapsed();
    drop(sender);

    let mut merged = WorkerStats::default();
    while let Some(result) = tasks.join_next().await {
        merged.merge(&result.context("joining open-loop worker")??)?;
    }
    merged.offered = offered;
    merged.dropped = dropped;
    merged.total = merged.total.saturating_add(dropped);
    Ok((merged, scheduler_elapsed))
}

async fn open_worker(
    db: Arc<Db>,
    receiver: async_channel::Receiver<Arrival>,
    variant: Arc<VariantConfig>,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
    window_recorder: ApplicationWindowRecorder,
    rng_seed: u64,
) -> Result<WorkerStats> {
    let mut arrival = match receiver.recv().await {
        Ok(arrival) => arrival,
        Err(_) => return Ok(WorkerStats::default()),
    };
    let mut rng = StdRng::seed_from_u64(rng_seed);
    let selector = KeySelector::uniform(variant.record_count());
    let write_options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let mut stats = WorkerStats::with_window_recorder(Some(window_recorder));
    loop {
        let errors_before = stats.errors;
        let invoked = Instant::now();
        let scheduling_delay = invoked.saturating_duration_since(arrival.scheduled);
        let update =
            variant.workload.kind == WorkloadKind::OpenLoopReadUpdate && rng.random_bool(0.5);
        let id = selector.sample(&mut rng);
        let key = key_for_id(id, variant.key_bytes());
        let (operation, api, successful, payload, api_latency, return_latency) = if update {
            let value = random_value(variant.value_bytes(), &mut rng);
            let api_started = Instant::now();
            let (result, backpressure) = measure_backpressure(db.put_with_options(
                key,
                value.clone(),
                &PutOptions::default(),
                &write_options,
            ))
            .await;
            let api_latency = api_started.elapsed();
            stats.record_backpressure(backpressure);
            match result {
                Ok(handle) => {
                    let returned_at = Instant::now();
                    let return_latency = invoked.elapsed();
                    stats.record_write(returned_at, handle.seqnum());
                    if let Some(tracker) = &durability {
                        tracker.accepted(handle.seqnum(), returned_at);
                    }
                    (
                        "update",
                        "put",
                        true,
                        Payload::write(value.len() as u64),
                        api_latency,
                        return_latency,
                    )
                }
                Err(error) => {
                    let return_latency = invoked.elapsed();
                    tracing::debug!(%error, "open-loop update failed");
                    (
                        "update",
                        "put",
                        false,
                        Payload::default(),
                        api_latency,
                        return_latency,
                    )
                }
            }
        } else {
            let api_started = Instant::now();
            let result = db.get(key).await;
            let api_latency = api_started.elapsed();
            match result {
                Ok(Some(value)) => (
                    "read",
                    "get",
                    true,
                    Payload::read(value.len() as u64),
                    api_latency,
                    invoked.elapsed(),
                ),
                Ok(None) => (
                    "read",
                    "get",
                    false,
                    Payload::default(),
                    api_latency,
                    invoked.elapsed(),
                ),
                Err(error) => {
                    let return_latency = invoked.elapsed();
                    tracing::debug!(%error, "open-loop read failed");
                    (
                        "read",
                        "get",
                        false,
                        Payload::default(),
                        api_latency,
                        return_latency,
                    )
                }
            }
        };
        let completed = Instant::now();
        stats.record_open_loop_completion(
            operation,
            api,
            successful,
            return_latency,
            api_latency,
            completed.saturating_duration_since(arrival.scheduled),
            scheduling_delay,
            payload,
        );
        if let Some(counters) = &counters {
            counters.operations.fetch_add(1, Ordering::Relaxed);
            if stats.errors > errors_before {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        match receiver.recv().await {
            Ok(next) => arrival = next,
            Err(_) => break,
        }
    }
    Ok(stats)
}

fn worker_count(target_rate: u64) -> Result<usize> {
    usize::try_from(target_rate).context("open-loop target rate exceeds platform capacity")
}

fn window_recorders(counters: &ApplicationCounters) -> Vec<ApplicationWindowRecorder> {
    (0..WINDOW_RECORDER_SHARDS)
        .map(|_| counters.register_window_recorder())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{window_recorders, worker_count, WINDOW_RECORDER_SHARDS};
    use crate::config::BenchmarkConfig;
    use crate::system::ApplicationCounters;
    use std::path::Path;

    #[test]
    fn worker_pool_scales_with_target_rate() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let variant = benchmark
            .select(Some("slatedb"), Some("open-loop-read"), Some("rate-10000"))
            .expect("variant")
            .pop()
            .expect("configured variant");

        assert_eq!(variant.clients, None);
        assert_eq!(
            worker_count(variant.target_rate.expect("target rate")).expect("worker count"),
            10_000
        );
    }

    #[test]
    fn window_recorders_are_bounded_independently_of_target_rate() {
        let counters = ApplicationCounters::new(true);
        assert_eq!(window_recorders(&counters).len(), WINDOW_RECORDER_SHARDS);
    }
}
