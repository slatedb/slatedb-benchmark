mod closed;
mod durability;
mod stats;
mod util;

use crate::config::{VariantConfig, WorkloadKind};
use crate::instrumented_store::{StoreMetrics, StoreSnapshot};
use crate::model::{
    ApplicationPerformance, DurabilityPerformance, DurabilityWindow, HistogramsFile, IngestWindow,
    MetricSeriesValue, MetricValueType, ResourceUse, StoragePerformance, TimeseriesFile,
};
use crate::system::{self, ApplicationWindowRegistry, BenchmarkMetricsRecorder};
use anyhow::{Context, Result};
use durability::DurabilityTracker;
use serde::{Deserialize, Serialize};
use slatedb::Db;
use slatedb_common::metrics::{MetricValue as SlateMetricValue, Metrics};
use stats::WorkerStats;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub use closed::populate_dataset;

pub async fn prepare_bulk_load(db: Arc<Db>, variant: &VariantConfig) -> Result<()> {
    closed::run_bulk_load(db, variant, None, None).await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadOutcome {
    pub application: ApplicationPerformance,
    pub durability: DurabilityPerformance,
    pub resources: ResourceUse,
    pub storage: StoragePerformance,
    pub histograms: HistogramsFile,
    pub timeseries: TimeseriesFile,
    pub elapsed_ns: u64,
    storage_counters: StorageCounters,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageCounters {
    wal_flush_bytes: u64,
    l0_flush_bytes: u64,
    compacted_bytes: u64,
    logical_write_bytes: u64,
}

impl StorageCounters {
    fn merge(&mut self, other: Self) {
        self.wal_flush_bytes = self.wal_flush_bytes.saturating_add(other.wal_flush_bytes);
        self.l0_flush_bytes = self.l0_flush_bytes.saturating_add(other.l0_flush_bytes);
        self.compacted_bytes = self.compacted_bytes.saturating_add(other.compacted_bytes);
        self.logical_write_bytes = self
            .logical_write_bytes
            .saturating_add(other.logical_write_bytes);
    }

    fn write_amplification(&self) -> Option<f64> {
        (self.logical_write_bytes > 0).then_some(
            self.wal_flush_bytes
                .saturating_add(self.l0_flush_bytes)
                .saturating_add(self.compacted_bytes) as f64
                / self.logical_write_bytes as f64,
        )
    }

    fn compaction_throughput(&self, elapsed: Duration) -> Option<f64> {
        (self.compacted_bytes > 0)
            .then_some(self.compacted_bytes as f64 / elapsed.as_secs_f64().max(f64::EPSILON))
    }
}

pub async fn execute_variant(
    db: Arc<Db>,
    variant: &VariantConfig,
    store_metrics: Arc<StoreMetrics>,
    slate_metrics: Arc<BenchmarkMetricsRecorder>,
) -> Result<WorkloadOutcome> {
    let closed_state = closed::ClosedLoopState::new(variant.record_count());
    let warmup = Duration::from_millis(variant.warmup_ms());
    tracing::info!(
        suite = variant.suite.name,
        workload = variant.workload.name,
        variant = variant.variant,
        "starting benchmark variant"
    );
    if !warmup.is_zero() {
        tracing::info!(
            suite = variant.suite.name,
            workload = variant.workload.name,
            variant = variant.variant,
            "warming benchmark"
        );
        closed::run_closed_phase(Arc::clone(&db), variant, warmup, None, None, &closed_state)
            .await?;
        if variant.workload.kind.may_write() {
            db.flush().await.context("draining warmup writes")?;
        }
    }

    let start_store = store_metrics.snapshot();
    let start_slate = slate_metrics.snapshot();
    let windows = Arc::new(ApplicationWindowRegistry::new());
    let measured_started = Instant::now();
    let (stop_tx, stop_rx) = watch::channel(false);
    let sampler = tokio::spawn(system::sample_until_stopped(
        measured_started,
        Arc::clone(&windows),
        Arc::clone(&store_metrics),
        Arc::clone(&slate_metrics),
        Arc::clone(&db),
        stop_rx,
    ));

    let tracks_lag = variant.workload.kind.may_write() && !variant.workload.await_durable;
    let tracker = tracks_lag.then(|| DurabilityTracker::start(Arc::clone(&db), measured_started));
    let durability_sender = tracker.as_ref().map(DurabilityTracker::sender);
    let mut stats = if variant.workload.kind == WorkloadKind::BulkLoad {
        closed::run_bulk_load(
            Arc::clone(&db),
            variant,
            durability_sender,
            Some(Arc::clone(&windows)),
        )
        .await?
    } else {
        closed::run_closed_phase(
            Arc::clone(&db),
            variant,
            Duration::from_millis(variant.measurement_ms()),
            durability_sender,
            Some(Arc::clone(&windows)),
            &closed_state,
        )
        .await?
    };
    let generation_stopped = Instant::now();
    let measurement_elapsed = generation_stopped.saturating_duration_since(measured_started);
    let final_api_recorder = windows.register_window_recorder();
    let (durability, durability_windows) = finish_durability(
        &db,
        &mut stats,
        tracker,
        &final_api_recorder,
        measured_started,
        generation_stopped,
    )
    .await?;

    let _ = stop_tx.send(true);
    let sampled = sampler.await.context("joining metric sampler")??;
    stats.histograms.merge(&sampled.histograms)?;
    let finished = Instant::now();
    let total_elapsed = finished.saturating_duration_since(measured_started);
    let end_store = store_metrics.snapshot();
    let store_delta = end_store.difference(&start_store);
    let end_slate = slate_metrics.snapshot();
    let storage_counters = storage_counters(&start_slate, &end_slate);
    let mut timeseries = system::compact_timeseries(sampled.samples)?;
    timeseries.application_windows = sampled.application_windows;
    timeseries.durability_windows = durability_windows;
    let mut storage = storage_summary(
        &storage_counters,
        &store_delta,
        &end_slate,
        total_elapsed,
        &timeseries,
        variant.workload.kind,
        stats.backpressure_ns,
    );
    storage.database_size_bytes = 0;
    let resources = system::summarize_resources(&timeseries.samples);
    let application = stats.application(measurement_elapsed);
    tracing::info!(
        suite = variant.suite.name,
        workload = variant.workload.name,
        variant = variant.variant,
        elapsed_seconds = total_elapsed.as_secs_f64(),
        operations = application.total_operations,
        "completed benchmark variant measurement"
    );
    Ok(WorkloadOutcome {
        application,
        durability,
        resources,
        storage,
        histograms: stats.histograms.to_file()?,
        timeseries,
        elapsed_ns: system::duration_ns(total_elapsed),
        storage_counters,
    })
}

async fn finish_durability(
    db: &Db,
    stats: &mut WorkerStats,
    tracker: Option<DurabilityTracker>,
    api_recorder: &system::ApplicationWindowRecorder,
    measured_started: Instant,
    generation_stopped: Instant,
) -> Result<(DurabilityPerformance, Option<Vec<DurabilityWindow>>)> {
    let mut durability = DurabilityPerformance::default();
    let mut windows = tracker.is_some().then(Vec::new);

    if stats.writes == 0 {
        if let Some(tracker) = tracker {
            // Join the background subscriber even when a short scaled window
            // happens not to select a write operation.
            windows = Some(tracker.finish().await?.windows);
        }
        return Ok((durability, windows));
    }

    let flush_started = Instant::now();
    let flush = db.flush().await;
    api_recorder.record_api_latency("flush", flush_started.elapsed());
    flush.context("final benchmark flush")?;
    let drained_at = Instant::now();
    durability.final_flush_drain_ns = Some(system::duration_ns(
        drained_at.saturating_duration_since(generation_stopped),
    ));
    durability.last_measured_sequence = stats.last_write_sequence;

    if let Some(tracker) = tracker {
        let tracked = tracker.finish().await?;
        durability.lag = Some(tracked.lag.summary());
        stats.histograms.insert("durability_lag", tracked.lag);
        windows = Some(tracked.windows);
        durability.final_durable_sequence = Some(tracked.final_durable_sequence);
        if let Some(first_write) = stats.first_write_return {
            let seconds = tracked
                .covered_at
                .saturating_duration_since(first_write)
                .as_secs_f64()
                .max(f64::EPSILON);
            durability.durable_ops_per_second = Some(stats.writes as f64 / seconds);
        }
    } else {
        durability.final_durable_sequence = Some(db.status().durable_seq);
        let seconds = drained_at
            .saturating_duration_since(stats.first_write_return.unwrap_or(measured_started))
            .as_secs_f64()
            .max(f64::EPSILON);
        durability.durable_ops_per_second = Some(stats.writes as f64 / seconds);
    }

    Ok((durability, windows))
}

fn storage_summary(
    counters: &StorageCounters,
    store: &StoreSnapshot,
    end_metrics: &Metrics,
    elapsed: Duration,
    timeseries: &TimeseriesFile,
    kind: WorkloadKind,
    backpressure_ns: u64,
) -> StoragePerformance {
    let backlog = gauge_value(
        end_metrics,
        slatedb::compactor::stats::TOTAL_BYTES_BEING_COMPACTED,
    )
    .map(|value| value.max(0) as u64);
    StoragePerformance {
        database_size_bytes: 0,
        average_database_size_bytes: 0,
        object_store: store.clone(),
        compaction_throughput_bytes_per_second: counters.compaction_throughput(elapsed),
        write_amplification: counters.write_amplification(),
        backpressure_ns,
        compaction_backlog_bytes: backlog,
        five_minute_windows: if kind == WorkloadKind::SustainedIngest {
            ingest_windows(timeseries)
        } else {
            Vec::new()
        },
    }
}

fn storage_counters(start: &Metrics, end: &Metrics) -> StorageCounters {
    StorageCounters {
        wal_flush_bytes: counter_delta_any(
            start,
            end,
            &["slatedb.db.wal_flush_bytes", "slatedb.wal.wal_flush_bytes"],
        ),
        l0_flush_bytes: counter_delta(start, end, slatedb::db_stats::L0_FLUSH_BYTES),
        compacted_bytes: counter_delta(start, end, slatedb::compactor::stats::BYTES_COMPACTED),
        logical_write_bytes: counter_delta(start, end, slatedb::db_stats::MEMTABLE_WRITE_BYTES),
    }
}

pub fn extend_with_compaction_phase(
    outcome: &mut WorkloadOutcome,
    mut samples: Vec<crate::model::TimeseriesSample>,
    store: StoreSnapshot,
    start_metrics: &Metrics,
    end_metrics: &Metrics,
    elapsed: Duration,
) -> Result<()> {
    let phase = storage_counters(start_metrics, end_metrics);
    outcome.storage_counters.merge(phase);
    outcome.storage.object_store.merge(store);

    let phase_ns = system::duration_ns(elapsed);
    let base = outcome.elapsed_ns;
    for sample in &mut samples {
        sample.offset_ns = sample.offset_ns.saturating_add(base);
    }
    let phase_timeseries = system::compact_timeseries(samples)?;
    system::append_timeseries(&mut outcome.timeseries, phase_timeseries)?;
    outcome.elapsed_ns = outcome.elapsed_ns.saturating_add(phase_ns);
    outcome.resources = system::summarize_resources(&outcome.timeseries.samples);
    outcome.storage.write_amplification = outcome.storage_counters.write_amplification();
    outcome.storage.compaction_throughput_bytes_per_second = outcome
        .storage_counters
        .compaction_throughput(Duration::from_nanos(outcome.elapsed_ns));
    outcome.storage.compaction_backlog_bytes = gauge_value(
        end_metrics,
        slatedb::compactor::stats::TOTAL_BYTES_BEING_COMPACTED,
    )
    .map(|value| value.max(0) as u64);
    Ok(())
}

fn ingest_windows(timeseries: &TimeseriesFile) -> Vec<IngestWindow> {
    const WINDOW_NS: u64 = 300_000_000_000;
    let samples = &timeseries.samples;
    if samples.is_empty() {
        return Vec::new();
    }
    let mut windows = Vec::new();
    let mut start_offset = 0_u64;
    for (sample_index, sample) in samples.iter().enumerate().skip(1) {
        if sample.offset_ns.saturating_sub(start_offset) >= WINDOW_NS
            || sample.offset_ns == samples[samples.len() - 1].offset_ns
        {
            let operations = timeseries
                .application_windows
                .iter()
                .filter(|window| {
                    window.start_offset_ns >= start_offset
                        && window.start_offset_ns.saturating_add(window.duration_ns)
                            <= sample.offset_ns
                })
                .map(|window| window.completed_operations)
                .fold(0_u64, u64::saturating_add);
            let seconds = sample.offset_ns.saturating_sub(start_offset) as f64 / 1e9;
            let start_index = samples
                .iter()
                .enumerate()
                .rev()
                .find(|(_, candidate)| candidate.offset_ns <= start_offset)
                .map(|(index, _)| index)
                .unwrap_or(0);
            let wal = sample_counter_delta_any(
                timeseries,
                start_index,
                sample_index,
                &["slatedb.db.wal_flush_bytes", "slatedb.wal.wal_flush_bytes"],
            );
            let l0 = sample_counter_delta(
                timeseries,
                start_index,
                sample_index,
                slatedb::db_stats::L0_FLUSH_BYTES,
            );
            let compacted = sample_counter_delta(
                timeseries,
                start_index,
                sample_index,
                slatedb::compactor::stats::BYTES_COMPACTED,
            );
            let logical = sample_counter_delta(
                timeseries,
                start_index,
                sample_index,
                slatedb::db_stats::MEMTABLE_WRITE_BYTES,
            );
            windows.push(IngestWindow {
                start_offset_ns: start_offset,
                operations,
                ops_per_second: operations as f64 / seconds.max(f64::EPSILON),
                compaction_backlog_bytes: sample_gauge(
                    timeseries,
                    sample_index,
                    slatedb::compactor::stats::TOTAL_BYTES_BEING_COMPACTED,
                ),
                write_amplification: (logical > 0).then_some(
                    wal.saturating_add(l0).saturating_add(compacted) as f64 / logical as f64,
                ),
            });
            start_offset = sample.offset_ns;
        }
    }
    windows
}

fn sample_counter_delta(timeseries: &TimeseriesFile, start: usize, end: usize, name: &str) -> u64 {
    sample_counter(timeseries, end, name).saturating_sub(sample_counter(timeseries, start, name))
}

fn sample_counter_delta_any(
    timeseries: &TimeseriesFile,
    start: usize,
    end: usize,
    names: &[&str],
) -> u64 {
    names
        .iter()
        .find(|name| {
            timeseries
                .slatedb_metrics
                .iter()
                .any(|metric| metric.name == **name)
        })
        .map_or(0, |name| sample_counter_delta(timeseries, start, end, name))
}

fn sample_counter(timeseries: &TimeseriesFile, sample: usize, name: &str) -> u64 {
    timeseries
        .slatedb_metrics
        .iter()
        .filter(|metric| metric.name == name && metric.value_type == MetricValueType::Counter)
        .filter_map(
            |metric| match metric.values.get(sample).and_then(Option::as_ref) {
                Some(MetricSeriesValue::Scalar(value)) => value.as_u64(),
                _ => None,
            },
        )
        .sum()
}

fn sample_gauge(timeseries: &TimeseriesFile, sample: usize, name: &str) -> Option<u64> {
    timeseries
        .slatedb_metrics
        .iter()
        .filter(|metric| {
            metric.name == name
                && matches!(
                    metric.value_type,
                    MetricValueType::Gauge | MetricValueType::UpDownCounter
                )
        })
        .filter_map(
            |metric| match metric.values.get(sample).and_then(Option::as_ref) {
                Some(MetricSeriesValue::Scalar(value)) => value
                    .as_i64()
                    .map(|value| value.max(0) as u64)
                    .or_else(|| value.as_u64()),
                _ => None,
            },
        )
        .max()
}

fn counter_delta(start: &Metrics, end: &Metrics, name: &str) -> u64 {
    counter_value(end, name).saturating_sub(counter_value(start, name))
}

fn counter_delta_any(start: &Metrics, end: &Metrics, names: &[&str]) -> u64 {
    names
        .iter()
        .find(|name| !start.by_name(name).is_empty() || !end.by_name(name).is_empty())
        .map_or(0, |name| counter_delta(start, end, name))
}

fn counter_value(metrics: &Metrics, name: &str) -> u64 {
    metrics
        .by_name(name)
        .iter()
        .filter_map(|metric| match metric.value {
            SlateMetricValue::Counter(value) => Some(value),
            _ => None,
        })
        .sum()
}

fn gauge_value(metrics: &Metrics, name: &str) -> Option<i64> {
    metrics
        .by_name(name)
        .iter()
        .filter_map(|metric| match metric.value {
            SlateMetricValue::Gauge(value) | SlateMetricValue::UpDownCounter(value) => Some(value),
            _ => None,
        })
        .max()
}
