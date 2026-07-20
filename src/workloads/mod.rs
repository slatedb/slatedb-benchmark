mod closed;
mod durability;
mod stats;
mod util;

use crate::config::{ResolvedConfig, TaskConfig};
use crate::instrumented_store::StoreMetrics;
use crate::system::{
    sample_until_stopped_with_rate_control, ApplicationRegistry, RateWindowControl,
    SampledMeasurement,
};
use anyhow::{bail, Context, Result};
use durability::DurabilityTracker;
use slatedb::Db;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};

pub(crate) use closed::DATASET_BATCH_RECORDS;
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
        validate_stats(&config.task, &warmup_stats)?;
        if config.task.task.may_write() {
            db.flush().await.context("flushing warmup writes")?;
        }
    }

    tracing::info!(task = %config.task.task, "starting workload measurement");
    let registry = Arc::new(ApplicationRegistry::default());
    let rate_windows = Arc::new(RateWindowControl::new());
    let (stop_tx, stop_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let sampler = tokio::spawn(sample_until_stopped_with_rate_control(
        Arc::clone(&registry),
        store_metrics,
        Arc::clone(&rate_windows),
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
        .then(|| DurabilityTracker::start(Arc::clone(&db), Arc::clone(&registry)));
    let client_started = Instant::now();
    let stats = closed::run_phase(
        Arc::clone(&db),
        config,
        Duration::from_millis(config.task.measurement_ms),
        Some(Arc::clone(&registry)),
        tracker.as_mut(),
    )
    .await;
    let client_measurement = client_started.elapsed();
    rate_windows.finish();
    tracing::info!(task = %config.task.task, "workload clients stopped");
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
    let durability_drain = drain_started.map_or(Duration::ZERO, |started| started.elapsed());
    let _ = stop_tx.send(true);
    let measurement = sampler.await.context("joining metric sampler")??;
    flush_result?;
    let durable_result = durable_result?;

    if let Some(result) = durable_result {
        if result.count != stats.writes {
            bail!(
                "durability coverage mismatch: {} writes, {} latency samples",
                stats.writes,
                result.count
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
    }
    validate_workload(config, &stats, &measurement)?;
    let application_errors = measurement.application_errors();
    if application_errors > 0 {
        bail!("workload recorded {application_errors} API errors");
    }
    let object_store_attempt_errors = measurement.object_store_attempt_errors();
    if object_store_attempt_errors > 0 {
        tracing::warn!(
            errors = object_store_attempt_errors,
            "object-store request attempts failed without failing the task"
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
    config: &ResolvedConfig,
    stats: &WorkerStats,
    measurement: &SampledMeasurement,
) -> Result<()> {
    validate_stats(&config.task, stats)?;
    validate_mix(&config.task, measurement)
}

fn validate_stats(config: &TaskConfig, stats: &WorkerStats) -> Result<()> {
    if stats.errors > 0 {
        bail!("workload completed with {} operation errors", stats.errors);
    }
    if config.operation_mix.contains_key("get") {
        if config.key_selection == "uniform-absent" {
            if stats.read_hits > 0 || stats.read_misses == 0 {
                bail!(
                    "missing-read workload recorded {} hits and {} misses",
                    stats.read_hits,
                    stats.read_misses
                );
            }
        } else if stats.read_misses > 0 {
            bail!("hit-only workload recorded {} misses", stats.read_misses);
        }
    }
    if config.operation_mix.contains_key("transaction")
        && stats.transaction_attempts
            != stats
                .transaction_commits
                .saturating_add(stats.transaction_conflicts)
    {
        bail!("transaction outcomes do not reconcile with attempts");
    }
    Ok(())
}

fn validate_mix(config: &TaskConfig, measurement: &SampledMeasurement) -> Result<()> {
    let (Some(expected_get), Some(expected_put)) = (
        config.operation_mix.get("get"),
        config.operation_mix.get("put"),
    ) else {
        return Ok(());
    };
    let expected = expected_get / (expected_get + expected_put);
    let get = measurement.operation_total("get");
    let put = measurement.operation_total("put");
    let observed = get as f64 / get.saturating_add(put).max(1) as f64;
    anyhow::ensure!(
        (observed - expected).abs() <= 0.01,
        "observed read mix {observed:.4} differs from configured {expected:.4}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_stats;
    use crate::config::{load, BenchmarkScale, Task};
    use crate::workloads::stats::WorkerStats;
    use std::path::Path;

    #[test]
    fn missing_reads_require_misses_and_no_hits() {
        let config = load(
            Task::PointReadMissing,
            BenchmarkScale::FULL,
            Path::new("config/settings.toml"),
        )
        .expect("missing-read config");
        let stats = WorkerStats {
            read_misses: 1,
            ..Default::default()
        };
        validate_stats(&config.task, &stats).expect("valid missing reads");
    }
}
