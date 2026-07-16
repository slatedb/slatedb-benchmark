use super::durability::DurabilitySender;
use super::stats::{Payload, WorkerStats};
use super::util::{key_for_id, random_value, KeySelector};
use crate::config::{VariantConfig, WorkloadKind};
use crate::system::{measure_backpressure, ApplicationCounters};
use anyhow::{bail, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::Db;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

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
    let mut tasks = JoinSet::new();
    let variant = Arc::new(variant.clone());
    let mut seed_rng = StdRng::from_os_rng();
    for _ in 0..worker_count {
        let db = Arc::clone(&db);
        let receiver = receiver.clone();
        let variant = Arc::clone(&variant);
        let durability = durability.clone();
        let counters = counters.clone();
        let rng_seed = seed_rng.random();
        tasks.spawn(async move {
            open_worker(db, receiver, variant, durability, counters, rng_seed).await
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
    let mut stats = WorkerStats::with_window_recorder(
        counters
            .as_ref()
            .map(|counters| counters.register_window_recorder()),
    );
    loop {
        let errors_before = stats.errors;
        let invoked = Instant::now();
        let scheduling_delay = invoked.saturating_duration_since(arrival.scheduled);
        let update =
            variant.workload.kind == WorkloadKind::OpenLoopReadUpdate && rng.random_bool(0.5);
        let id = selector.sample(&mut rng);
        let key = key_for_id(id, variant.key_bytes());
        if update {
            let value = random_value(variant.value_bytes(), &mut rng);
            let (result, backpressure) = measure_backpressure(stats.measure_api(
                "put",
                db.put_with_options(key, value.clone(), &PutOptions::default(), &write_options),
            ))
            .await;
            stats.record_backpressure(backpressure);
            match result {
                Ok(handle) => {
                    let returned_at = Instant::now();
                    stats.record_success(
                        "update",
                        invoked.elapsed(),
                        Payload::write(value.len() as u64),
                    );
                    stats.record_write(returned_at, handle.seqnum());
                    if let Some(tracker) = &durability {
                        tracker.accepted(handle.seqnum(), returned_at);
                    }
                }
                Err(error) => {
                    stats.record_error("update", invoked.elapsed());
                    tracing::debug!(%error, "open-loop update failed");
                }
            }
        } else {
            match stats.measure_api("get", db.get(key)).await {
                Ok(Some(value)) => stats.record_success(
                    "read",
                    invoked.elapsed(),
                    Payload::read(value.len() as u64),
                ),
                Ok(None) => stats.record_error("read", invoked.elapsed()),
                Err(error) => {
                    stats.record_error("read", invoked.elapsed());
                    tracing::debug!(%error, "open-loop read failed");
                }
            }
        }
        let completed = Instant::now();
        stats.record_open_loop_timing(
            completed.saturating_duration_since(arrival.scheduled),
            scheduling_delay,
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

#[cfg(test)]
mod tests {
    use super::worker_count;
    use crate::config::BenchmarkConfig;
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
}
