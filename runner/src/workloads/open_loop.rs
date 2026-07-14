use super::durability::DurabilitySender;
use super::stats::WorkerStats;
use super::util::{key_for_id, random_value, KeySelector};
use crate::config::{VariantConfig, WorkloadKind};
use crate::system::ApplicationCounters;
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
) -> Result<WorkerStats> {
    let target_rate = variant
        .target_rate
        .context("open-loop variant has no target rate")?;
    if target_rate == 0 {
        bail!("open-loop target rate must be positive");
    }
    let worker_count = (target_rate as usize / 100).clamp(32, 256);
    let (sender, receiver) = async_channel::bounded::<Arrival>(target_rate as usize);
    let mut tasks = JoinSet::new();
    for _ in 0..worker_count {
        let db = Arc::clone(&db);
        let receiver = receiver.clone();
        let variant = variant.clone();
        let durability = durability.clone();
        let counters = counters.clone();
        tasks.spawn(async move { open_worker(db, receiver, variant, durability, counters).await });
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
        while scheduled <= now && scheduled < deadline {
            offered += 1;
            if sender.try_send(Arrival { scheduled }).is_err() {
                dropped += 1;
            }
            scheduled += period;
        }
    }
    drop(sender);

    let mut merged = WorkerStats::default();
    while let Some(result) = tasks.join_next().await {
        merged.merge(&result.context("joining open-loop worker")??)?;
    }
    merged.offered = offered;
    merged.dropped = dropped;
    merged.total = merged.total.saturating_add(dropped);
    Ok(merged)
}

async fn open_worker(
    db: Arc<Db>,
    receiver: async_channel::Receiver<Arrival>,
    variant: VariantConfig,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
) -> Result<WorkerStats> {
    let mut rng = StdRng::from_os_rng();
    let selector = KeySelector::uniform(variant.record_count());
    let write_options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let mut stats = WorkerStats::default();
    while let Ok(arrival) = receiver.recv().await {
        let errors_before = stats.errors;
        let invoked = Instant::now();
        let scheduling_delay = invoked.saturating_duration_since(arrival.scheduled);
        let update =
            variant.workload.kind == WorkloadKind::OpenLoopReadUpdate && rng.random_bool(0.5);
        let id = selector.sample(&mut rng);
        let key = key_for_id(id, variant.key_bytes());
        if update {
            let value = random_value(variant.value_bytes(), &mut rng);
            match db
                .put_with_options(key, value.clone(), &PutOptions::default(), &write_options)
                .await
            {
                Ok(handle) => {
                    let returned_at = Instant::now();
                    stats.record_success("update", invoked.elapsed(), value.len() as u64);
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
            match db.get(key).await {
                Ok(Some(value)) => {
                    stats.record_success("read", invoked.elapsed(), value.len() as u64)
                }
                Ok(None) => stats.record_error("read", invoked.elapsed()),
                Err(error) => {
                    stats.record_error("read", invoked.elapsed());
                    tracing::debug!(%error, "open-loop read failed");
                }
            }
        }
        let completed = Instant::now();
        stats
            .histograms
            .record("scheduling_delay", scheduling_delay);
        stats.histograms.record(
            "response",
            completed.saturating_duration_since(arrival.scheduled),
        );
        if let Some(counters) = &counters {
            counters.operations.fetch_add(1, Ordering::Relaxed);
            if stats.errors > errors_before {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(stats)
}
