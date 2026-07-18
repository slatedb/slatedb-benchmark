mod closed;
mod durability;
mod stats;
mod util;

use crate::config::{ResolvedConfig, Task};
use crate::instrumented_store::StoreMetrics;
use crate::system::{sample_until_stopped, ApplicationRegistry, SampledMeasurement};
use anyhow::{bail, Context, Result};
use durability::DurabilityTracker;
use slatedb::Db;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

pub use closed::{populate_dataset, DatasetLoadMetrics};
use stats::WorkerStats;

pub struct WorkloadExecution {
    pub measurement: SampledMeasurement,
    pub client_measurement: Duration,
    pub durability_drain: Duration,
}

pub async fn execute(
    db: Arc<Db>,
    config: &ResolvedConfig,
    store_metrics: Arc<StoreMetrics>,
) -> Result<WorkloadExecution> {
    let warmup = Duration::from_millis(config.task.warmup_ms);
    if !warmup.is_zero() {
        tracing::info!(task = %config.task.task, "starting workload warmup");
        let warmup_stats = closed::run_phase(Arc::clone(&db), config, warmup, None, None).await?;
        if warmup_stats.errors > 0 {
            bail!(
                "warmup completed with {} operation errors",
                warmup_stats.errors
            );
        }
        validate_stats(config.task.task, &warmup_stats)?;
        if config.task.task.may_write() {
            db.flush().await.context("flushing warmup writes")?;
        }
    }

    tracing::info!(task = %config.task.task, "starting workload measurement");
    let registry = Arc::new(ApplicationRegistry::default());
    let (stop_tx, stop_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let sampler = tokio::spawn(sample_until_stopped(
        Arc::clone(&registry),
        store_metrics,
        stop_rx,
        Some(ready_tx),
    ));
    ready_rx
        .await
        .context("metric sampler stopped before taking its baselines")?;
    let mut tracker = config
        .task
        .task
        .may_write()
        .then(|| DurabilityTracker::start(Arc::clone(&db)));
    let durability = tracker.as_ref().map(DurabilityTracker::sender);
    let client_started = Instant::now();
    let stats = closed::run_phase(
        Arc::clone(&db),
        config,
        Duration::from_millis(config.task.measurement_ms),
        Some(Arc::clone(&registry)),
        durability.clone(),
    )
    .await;
    let client_measurement = client_started.elapsed();
    tracing::info!(task = %config.task.task, "workload clients stopped");
    drop(durability);
    let stats = match stats {
        Ok(stats) => stats,
        Err(error) => {
            if let Some(tracker) = tracker.take() {
                tracker.abort();
            }
            let _ = stop_tx.send(true);
            sampler.await.context("joining metric sampler")??;
            return Err(error);
        }
    };

    let final_recorder = registry.recorder();
    let drain_started = config.task.task.may_write().then(Instant::now);
    let flush_result = if config.task.task.may_write() {
        let flush_started = Instant::now();
        let result = db.flush().await;
        match &result {
            Ok(()) => final_recorder.record_success("flush", flush_started.elapsed(), 0),
            Err(_) => final_recorder.record_error("flush", flush_started.elapsed()),
        }
        result.context("draining measured writes")
    } else {
        Ok(())
    };

    let durable_result = if flush_result.is_ok() {
        match tracker.take() {
            Some(tracker) => tracker.finish().await.map(Some),
            None => Ok(None),
        }
    } else {
        if let Some(tracker) = tracker.take() {
            tracker.abort();
        }
        Ok(None)
    };
    let _ = stop_tx.send(true);
    let mut measurement = sampler.await.context("joining metric sampler")??;
    flush_result?;
    let durable_result = durable_result?;
    let durability_drain = drain_started.map_or(Duration::ZERO, |started| started.elapsed());

    if let Some(result) = durable_result {
        if result.lag.len() != stats.writes {
            bail!(
                "durability coverage mismatch: {} writes, {} latency samples",
                stats.writes,
                result.lag.len()
            );
        }
        if let Some(last_write_sequence) = stats.last_write_sequence {
            if result.final_durable_sequence < last_write_sequence {
                bail!(
                    "durability frontier {} did not cover final write {}",
                    result.final_durable_sequence,
                    last_write_sequence
                );
            }
        }
        measurement.add_latency_histogram("durable", result.lag);
    }
    validate_workload(config.task.task, &stats, &measurement)?;
    if measurement.errors() > 0 {
        bail!(
            "workload recorded {} API or HTTP errors",
            measurement.errors()
        );
    }
    tracing::info!(task = %config.task.task, "workload measurement complete");
    Ok(WorkloadExecution {
        measurement,
        client_measurement,
        durability_drain,
    })
}

fn validate_workload(
    task: Task,
    stats: &WorkerStats,
    measurement: &SampledMeasurement,
) -> Result<()> {
    validate_stats(task, stats)?;
    validate_mix(task, measurement)
}

fn validate_stats(task: Task, stats: &WorkerStats) -> Result<()> {
    if stats.errors > 0 {
        bail!("workload completed with {} operation errors", stats.errors);
    }
    match task {
        Task::PointReadUniform
        | Task::PointReadSkewed
        | Task::ReadHeavy
        | Task::Balanced
        | Task::UpdateHeavy => {
            if stats.read_misses > 0 {
                bail!("hit-only workload recorded {} misses", stats.read_misses);
            }
        }
        Task::PointReadMissing => {
            if stats.read_hits > 0 || stats.read_misses == 0 {
                bail!(
                    "missing-read workload recorded {} hits and {} misses",
                    stats.read_hits,
                    stats.read_misses
                );
            }
        }
        Task::TransactionContention => {
            if stats.transaction_attempts
                != stats
                    .transaction_commits
                    .saturating_add(stats.transaction_conflicts)
            {
                bail!("transaction outcomes do not reconcile with attempts");
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_mix(task: Task, measurement: &SampledMeasurement) -> Result<()> {
    let get = measurement.operation_total("get");
    let put = measurement.operation_total("put");
    let total = get.saturating_add(put);
    let expected = match task {
        Task::ReadHeavy => Some(0.95),
        Task::Balanced => Some(0.5),
        Task::UpdateHeavy => Some(0.05),
        _ => None,
    };
    if let Some(expected) = expected {
        let observed = get as f64 / total.max(1) as f64;
        anyhow::ensure!(
            (observed - expected).abs() <= 0.01,
            "observed read mix {observed:.4} differs from expected {expected:.4}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_workload;
    use crate::config::Task;
    use crate::instrumented_store::StoreMetrics;
    use crate::system::{sample_until_stopped, ApplicationRegistry};
    use crate::workloads::stats::WorkerStats;
    use std::sync::Arc;
    use tokio::sync::watch;

    #[tokio::test]
    async fn missing_reads_require_misses_and_no_hits() {
        let registry = Arc::new(ApplicationRegistry::default());
        let (_tx, rx) = watch::channel(true);
        let measurement =
            sample_until_stopped(registry, Arc::new(StoreMetrics::default()), rx, None)
                .await
                .expect("measurement");
        let stats = WorkerStats {
            read_misses: 1,
            ..Default::default()
        };
        validate_workload(Task::PointReadMissing, &stats, &measurement)
            .expect("valid missing reads");
    }
}
