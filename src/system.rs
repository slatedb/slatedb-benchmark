use crate::database_size::live_database_size_bytes;
use crate::histogram::HistogramSet;
use crate::instrumented_store::{StoreMetrics, StoreSnapshot};
use crate::model::{
    ApplicationWindow, Environment, MetricHistogramValue, MetricSeries, MetricSeriesValue,
    MetricSnapshot, MetricValue, MetricValueType, ResourceUse, TimeseriesFile, TimeseriesSample,
};
use anyhow::{bail, ensure, Result};
use object_store::path::Path;
use slatedb::Db;
use slatedb_common::metrics::{
    CounterFn, DefaultMetricsRecorder, GaugeFn, HistogramFn, MetricValue as SlateMetricValue,
    Metrics, MetricsRecorder, UpDownCounterFn,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{Disks, Networks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::watch;

tokio::task_local! {
    static BACKPRESSURE_MEASUREMENT: RefCell<BackpressureMeasurement>;
}

pub(crate) fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Default)]
struct BackpressureMeasurement {
    started: Option<Instant>,
    elapsed: Duration,
}

impl BackpressureMeasurement {
    fn start(&mut self) {
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
    }

    fn finish_interval(&mut self) {
        if let Some(started) = self.started.take() {
            self.elapsed = self.elapsed.saturating_add(started.elapsed());
        }
    }

    fn finish(mut self) -> Duration {
        self.finish_interval();
        self.elapsed
    }
}

/// The default SlateDB recorder plus runner-side backpressure timing.
pub struct BenchmarkMetricsRecorder {
    inner: DefaultMetricsRecorder,
}

impl BenchmarkMetricsRecorder {
    pub fn new() -> Self {
        Self {
            inner: DefaultMetricsRecorder::new(),
        }
    }

    pub fn snapshot(&self) -> Metrics {
        self.inner.snapshot()
    }
}

impl Default for BenchmarkMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

struct BackpressureCounter {
    inner: Arc<dyn CounterFn>,
}

impl CounterFn for BackpressureCounter {
    fn increment(&self, value: u64) {
        self.inner.increment(value);
        if value > 0 {
            let _ = BACKPRESSURE_MEASUREMENT.try_with(|measurement| {
                measurement.borrow_mut().start();
            });
        }
    }
}

struct TotalMemSizeGauge {
    inner: Arc<dyn GaugeFn>,
}

impl GaugeFn for TotalMemSizeGauge {
    fn set(&self, value: i64) {
        let _ = BACKPRESSURE_MEASUREMENT.try_with(|measurement| {
            measurement.borrow_mut().finish_interval();
        });
        self.inner.set(value);
    }
}

impl MetricsRecorder for BenchmarkMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let inner = self.inner.register_counter(name, description, labels);
        if name == slatedb::db_stats::BACKPRESSURE_COUNT {
            Arc::new(BackpressureCounter { inner })
        } else {
            inner
        }
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let inner = self.inner.register_gauge(name, description, labels);
        if name == slatedb::db_stats::TOTAL_MEM_SIZE_BYTES {
            Arc::new(TotalMemSizeGauge { inner })
        } else {
            inner
        }
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        self.inner
            .register_up_down_counter(name, description, labels)
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        self.inner
            .register_histogram(name, description, labels, boundaries)
    }
}

pub async fn measure_backpressure<F>(future: F) -> (F::Output, Duration)
where
    F: Future,
{
    BACKPRESSURE_MEASUREMENT
        .scope(
            RefCell::new(BackpressureMeasurement::default()),
            async move {
                let output = future.await;
                let elapsed = BACKPRESSURE_MEASUREMENT
                    .with(|measurement| std::mem::take(&mut *measurement.borrow_mut()).finish());
                (output, elapsed)
            },
        )
        .await
}

#[derive(Debug, Default)]
struct ApplicationWindowDelta {
    completed_operations: u64,
    successful_operations: u64,
    errors: u64,
    read_payload_bytes: u64,
    write_payload_bytes: u64,
    offered_operations: u64,
    dropped_operations: u64,
    histograms: HistogramSet,
}

impl ApplicationWindowDelta {
    fn merge(&mut self, other: &Self) -> Result<()> {
        self.completed_operations = self
            .completed_operations
            .saturating_add(other.completed_operations);
        self.successful_operations = self
            .successful_operations
            .saturating_add(other.successful_operations);
        self.errors = self.errors.saturating_add(other.errors);
        self.read_payload_bytes = self
            .read_payload_bytes
            .saturating_add(other.read_payload_bytes);
        self.write_payload_bytes = self
            .write_payload_bytes
            .saturating_add(other.write_payload_bytes);
        self.offered_operations = self
            .offered_operations
            .saturating_add(other.offered_operations);
        self.dropped_operations = self
            .dropped_operations
            .saturating_add(other.dropped_operations);
        self.histograms.merge(&other.histograms)
    }

    fn reset(&mut self) {
        self.completed_operations = 0;
        self.successful_operations = 0;
        self.errors = 0;
        self.read_payload_bytes = 0;
        self.write_payload_bytes = 0;
        self.offered_operations = 0;
        self.dropped_operations = 0;
        self.histograms.reset();
    }
}

#[derive(Debug, Default)]
struct ApplicationWindowShard {
    active: ApplicationWindowDelta,
    spare: Option<ApplicationWindowDelta>,
}

#[derive(Debug, Clone)]
pub struct ApplicationWindowRecorder {
    inner: Arc<Mutex<ApplicationWindowShard>>,
}

impl ApplicationWindowRecorder {
    fn update(&self, record: impl FnOnce(&mut ApplicationWindowDelta)) {
        let mut shard = self.inner.lock().expect("application window lock poisoned");
        record(&mut shard.active);
    }

    pub fn record_success(
        &self,
        operation: &str,
        latency: Duration,
        read_payload_bytes: u64,
        write_payload_bytes: u64,
    ) {
        self.record_success_internal(
            operation,
            latency,
            read_payload_bytes,
            write_payload_bytes,
            true,
        );
    }

    pub fn record_background_success(
        &self,
        operation: &str,
        latency: Duration,
        read_payload_bytes: u64,
        write_payload_bytes: u64,
    ) {
        self.record_success_internal(
            operation,
            latency,
            read_payload_bytes,
            write_payload_bytes,
            false,
        );
    }

    fn record_success_internal(
        &self,
        operation: &str,
        latency: Duration,
        read_payload_bytes: u64,
        write_payload_bytes: u64,
        include_in_headline: bool,
    ) {
        self.update(|window| {
            window.completed_operations = window.completed_operations.saturating_add(1);
            window.successful_operations = window.successful_operations.saturating_add(1);
            window.read_payload_bytes =
                window.read_payload_bytes.saturating_add(read_payload_bytes);
            window.write_payload_bytes = window
                .write_payload_bytes
                .saturating_add(write_payload_bytes);
            if include_in_headline {
                window.histograms.record("return", latency);
            }
            window
                .histograms
                .record(format!("return/{operation}"), latency);
        });
    }

    pub fn record_error(&self, operation: &str, latency: Duration) {
        self.record_error_internal(operation, latency, true);
    }

    pub fn record_background_error(&self, operation: &str, latency: Duration) {
        self.record_error_internal(operation, latency, false);
    }

    fn record_error_internal(&self, operation: &str, latency: Duration, include_in_headline: bool) {
        self.update(|window| {
            window.completed_operations = window.completed_operations.saturating_add(1);
            window.errors = window.errors.saturating_add(1);
            if include_in_headline {
                window.histograms.record("return", latency);
            }
            window
                .histograms
                .record(format!("return/{operation}"), latency);
        });
    }

    pub fn record_completion(&self, operation: &str, latency: Duration) {
        self.update(|window| {
            window.completed_operations = window.completed_operations.saturating_add(1);
            window.histograms.record("return", latency);
            window
                .histograms
                .record(format!("return/{operation}"), latency);
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_open_loop_completion(
        &self,
        operation: &str,
        api: &str,
        successful: bool,
        return_latency: Duration,
        api_latency: Duration,
        response_latency: Duration,
        scheduling_delay: Duration,
        read_payload_bytes: u64,
        write_payload_bytes: u64,
    ) {
        self.update(|window| {
            window.completed_operations = window.completed_operations.saturating_add(1);
            if successful {
                window.successful_operations = window.successful_operations.saturating_add(1);
                window.read_payload_bytes =
                    window.read_payload_bytes.saturating_add(read_payload_bytes);
                window.write_payload_bytes = window
                    .write_payload_bytes
                    .saturating_add(write_payload_bytes);
            } else {
                window.errors = window.errors.saturating_add(1);
            }
            window.histograms.record("return", return_latency);
            window
                .histograms
                .record(format!("return/{operation}"), return_latency);
            window.histograms.record(format!("api/{api}"), api_latency);
            window.histograms.record("response", response_latency);
            window
                .histograms
                .record("scheduling_delay", scheduling_delay);
        });
    }

    pub fn record_batch_latency(&self, latency: Duration) {
        self.update(|window| window.histograms.record("batch", latency));
    }

    pub fn record_api_latency(&self, api: &str, latency: Duration) {
        self.update(|window| window.histograms.record(format!("api/{api}"), latency));
    }

    pub fn record_offered(&self, offered: u64, dropped: u64) {
        self.update(|window| {
            window.offered_operations = window.offered_operations.saturating_add(offered);
            window.dropped_operations = window.dropped_operations.saturating_add(dropped);
        });
    }
}

#[derive(Debug)]
pub struct ApplicationCounters {
    pub operations: AtomicU64,
    pub errors: AtomicU64,
    open_loop: bool,
    window_shards: Mutex<Vec<Arc<Mutex<ApplicationWindowShard>>>>,
}

impl Default for ApplicationCounters {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ApplicationCounters {
    pub fn new(open_loop: bool) -> Self {
        Self {
            operations: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            open_loop,
            window_shards: Mutex::new(Vec::new()),
        }
    }

    pub fn reset(&self) {
        self.operations.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }

    pub fn register_window_recorder(&self) -> ApplicationWindowRecorder {
        let inner = Arc::new(Mutex::new(ApplicationWindowShard::default()));
        self.window_shards
            .lock()
            .expect("application shard registry lock poisoned")
            .push(Arc::clone(&inner));
        ApplicationWindowRecorder { inner }
    }

    fn drain_window(
        &self,
        start_offset_ns: u64,
        duration_ns: u64,
    ) -> Result<(ApplicationWindow, HistogramSet)> {
        let shards = self
            .window_shards
            .lock()
            .expect("application shard registry lock poisoned")
            .clone();
        let mut merged = ApplicationWindowDelta::default();
        for shard in shards {
            let mut delta = {
                let mut shard = shard.lock().expect("application window lock poisoned");
                let replacement = shard.spare.take().unwrap_or_default();
                std::mem::replace(&mut shard.active, replacement)
            };
            let merge = merged.merge(&delta);
            delta.reset();
            let previous = shard
                .lock()
                .expect("application window lock poisoned")
                .spare
                .replace(delta);
            debug_assert!(
                previous.is_none(),
                "application window drained concurrently"
            );
            merge?;
        }
        let summary = |name: &str| {
            merged
                .histograms
                .get(name)
                .filter(|histogram| !histogram.is_empty())
                .map(|histogram| histogram.summary())
        };
        let window = ApplicationWindow {
            start_offset_ns,
            duration_ns,
            completed_operations: merged.completed_operations,
            successful_operations: merged.successful_operations,
            errors: merged.errors,
            read_payload_bytes: merged.read_payload_bytes,
            write_payload_bytes: merged.write_payload_bytes,
            payload_bytes: merged
                .read_payload_bytes
                .saturating_add(merged.write_payload_bytes),
            offered_operations: self.open_loop.then_some(merged.offered_operations),
            dropped_operations: self.open_loop.then_some(merged.dropped_operations),
            return_latency: summary("return"),
            return_latency_by_operation: merged.histograms.summaries_with_prefix("return/"),
            api_latency: merged.histograms.summaries_with_prefix("api/"),
            response_latency: summary("response"),
            scheduling_delay: summary("scheduling_delay"),
            batch_latency: summary("batch"),
        };
        Ok((window, merged.histograms))
    }
}

pub struct SampledTimeseries {
    pub samples: Vec<TimeseriesSample>,
    pub application_windows: Vec<ApplicationWindow>,
    pub histograms: HistogramSet,
}

fn is_tigris_endpoint(endpoint: &str) -> bool {
    endpoint.contains("tigrisdata.com")
        || endpoint.contains("tigris.dev")
        || endpoint == "https://t3.storage.dev"
}

pub fn inspect_environment(provider: &str, endpoint: &str, region: &str) -> Environment {
    let mut system = System::new_all();
    system.refresh_all();
    let disks = Disks::new_with_refreshed_list();
    let local_disk = disks
        .list()
        .iter()
        .map(|disk| {
            format!(
                "{}:{}",
                disk.name().to_string_lossy(),
                disk.mount_point().display()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    Environment {
        runner_type: std::env::var("SLATEDB_BENCH_RUNNER_TYPE")
            .unwrap_or_else(|_| "local".to_string()),
        hostname: hostname::get()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "unknown".to_string()),
        cpu_model: system
            .cpus()
            .first()
            .map(|cpu| cpu.brand().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        cpu_cores: system.cpus().len(),
        ram_bytes: system.total_memory(),
        local_disk,
        os: format!(
            "{} {}",
            System::name().unwrap_or_else(|| "unknown".to_string()),
            System::os_version().unwrap_or_else(|| "unknown".to_string())
        ),
        kernel: System::kernel_version().unwrap_or_else(|| "unknown".to_string()),
        object_store: if provider.eq_ignore_ascii_case("aws") && is_tigris_endpoint(endpoint) {
            "Tigris".to_string()
        } else {
            provider.to_string()
        },
        endpoint: endpoint.to_string(),
        region: region.to_string(),
    }
}

pub fn verify_environment(environment: &Environment, smoke: bool) -> anyhow::Result<()> {
    if smoke {
        return Ok(());
    }
    anyhow::ensure!(
        environment.runner_type == "warp-ubuntu-latest-x64-16x",
        "published runs require warp-ubuntu-latest-x64-16x, found {}",
        environment.runner_type
    );
    anyhow::ensure!(
        environment.cpu_cores == 16,
        "published runs require 16 CPUs"
    );
    anyhow::ensure!(
        environment.ram_bytes >= 60 * 1024 * 1024 * 1024,
        "published runs require at least 60 GiB RAM"
    );
    anyhow::ensure!(
        environment.object_store == "Tigris",
        "published runs require Tigris"
    );
    anyhow::ensure!(
        environment.endpoint == "https://t3.storage.dev",
        "published runs require https://t3.storage.dev"
    );
    anyhow::ensure!(
        environment.region == "fra",
        "published runs require region fra"
    );
    Ok(())
}

pub(crate) async fn sample_until_stopped(
    started: Instant,
    counters: Arc<ApplicationCounters>,
    store_metrics: Arc<StoreMetrics>,
    slate_metrics: Arc<BenchmarkMetricsRecorder>,
    database_size: DatabaseSizeSource,
    mut stop: watch::Receiver<bool>,
) -> Result<SampledTimeseries> {
    let mut sampler = HostSampler::new();
    let mut samples = vec![sampler.sample(
        started,
        &counters,
        &store_metrics,
        &slate_metrics,
        database_size.bytes(&store_metrics),
    )];
    let mut application_windows = Vec::new();
    let mut histograms = HistogramSet::default();
    let mut window_start_ns = 0_u64;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let sample = sampler.sample(
                    started,
                    &counters,
                    &store_metrics,
                    &slate_metrics,
                    database_size.bytes(&store_metrics),
                );
                if sample.offset_ns > window_start_ns {
                    let (window, window_histograms) = counters.drain_window(
                        window_start_ns,
                        sample.offset_ns.saturating_sub(window_start_ns),
                    )?;
                    histograms.merge(&window_histograms)?;
                    application_windows.push(window);
                    window_start_ns = sample.offset_ns;
                }
                samples.push(sample);
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    let sample = sampler.sample(
                        started,
                        &counters,
                        &store_metrics,
                        &slate_metrics,
                        database_size.bytes(&store_metrics),
                    );
                    if sample.offset_ns > window_start_ns {
                        let (window, window_histograms) = counters.drain_window(
                            window_start_ns,
                            sample.offset_ns.saturating_sub(window_start_ns),
                        )?;
                        histograms.merge(&window_histograms)?;
                        application_windows.push(window);
                    }
                    samples.push(sample);
                    break;
                }
            }
        }
    }
    Ok(SampledTimeseries {
        samples,
        application_windows,
        histograms,
    })
}

pub(crate) enum DatabaseSizeSource {
    LiveDatabase(Arc<Db>),
    TrackedPrefix(Path),
}

impl DatabaseSizeSource {
    fn bytes(&self, store_metrics: &StoreMetrics) -> u64 {
        match self {
            Self::LiveDatabase(db) => live_database_size_bytes(&db.manifest()),
            Self::TrackedPrefix(path) => store_metrics.prefix_bytes(path),
        }
    }
}

pub fn compact_timeseries(mut samples: Vec<TimeseriesSample>) -> Result<TimeseriesFile> {
    let mut series = Vec::<MetricSeries>::new();
    for (sample_index, sample) in samples.iter_mut().enumerate() {
        for metric_series in &mut series {
            metric_series.values.push(None);
        }
        for metric in std::mem::take(&mut sample.slatedb_metrics) {
            let (value_type, boundaries, value) = compact_metric_value(metric.value);
            if let Some(existing) = series
                .iter_mut()
                .find(|existing| existing.name == metric.name && existing.labels == metric.labels)
            {
                ensure_metric_definition(existing, &metric.description, value_type, &boundaries)?;
                if existing.values[sample_index].is_some() {
                    bail!(
                        "duplicate SlateDB metric {} with labels {:?}",
                        metric.name,
                        metric.labels
                    );
                }
                existing.values[sample_index] = Some(value);
            } else {
                let mut values = vec![None; sample_index + 1];
                values[sample_index] = Some(value);
                series.push(MetricSeries {
                    name: metric.name,
                    description: metric.description,
                    labels: metric.labels,
                    value_type,
                    boundaries,
                    values,
                });
            }
        }
    }
    Ok(TimeseriesFile {
        interval_ns: 1_000_000_000,
        samples,
        application_windows: Vec::new(),
        durability_windows: None,
        slatedb_metrics: series,
    })
}

pub fn append_timeseries(target: &mut TimeseriesFile, mut phase: TimeseriesFile) -> Result<()> {
    ensure!(
        target.interval_ns == phase.interval_ns,
        "cannot append time series with different intervals"
    );
    let existing_samples = target.samples.len();
    let phase_samples = phase.samples.len();
    for metric_series in &mut target.slatedb_metrics {
        ensure!(
            metric_series.values.len() == existing_samples,
            "SlateDB metric series length does not match existing sample count"
        );
        metric_series
            .values
            .resize(existing_samples.saturating_add(phase_samples), None);
    }
    for phase_series in phase.slatedb_metrics {
        if let Some(existing) = target.slatedb_metrics.iter_mut().find(|existing| {
            existing.name == phase_series.name && existing.labels == phase_series.labels
        }) {
            ensure_metric_definition(
                existing,
                &phase_series.description,
                phase_series.value_type,
                &phase_series.boundaries,
            )?;
            ensure!(
                phase_series.values.len() == phase_samples,
                "SlateDB metric series length does not match phase sample count"
            );
            existing.values[existing_samples..].clone_from_slice(&phase_series.values);
        } else {
            let mut phase_series = phase_series;
            ensure!(
                phase_series.values.len() == phase_samples,
                "SlateDB metric series length does not match phase sample count"
            );
            let mut values = vec![None; existing_samples];
            values.append(&mut phase_series.values);
            phase_series.values = values;
            target.slatedb_metrics.push(phase_series);
        }
    }
    target
        .application_windows
        .append(&mut phase.application_windows);
    if let Some(mut phase_windows) = phase.durability_windows {
        target
            .durability_windows
            .get_or_insert_with(Vec::new)
            .append(&mut phase_windows);
    }
    target.samples.append(&mut phase.samples);
    Ok(())
}

pub fn summarize_resources(samples: &[TimeseriesSample]) -> ResourceUse {
    if samples.is_empty() {
        return ResourceUse::default();
    }
    let first = &samples[0];
    let last = &samples[samples.len() - 1];
    ResourceUse {
        average_cpu_percent: samples.iter().map(|s| s.cpu_percent).sum::<f64>()
            / samples.len() as f64,
        peak_cpu_percent: samples.iter().map(|s| s.cpu_percent).fold(0.0, f64::max),
        peak_rss_bytes: samples.iter().map(|s| s.rss_bytes).max().unwrap_or(0),
        network_bytes_sent: last
            .network_bytes_sent
            .saturating_sub(first.network_bytes_sent),
        network_bytes_received: last
            .network_bytes_received
            .saturating_sub(first.network_bytes_received),
        disk_bytes_read: last.disk_bytes_read.saturating_sub(first.disk_bytes_read),
        disk_bytes_written: last
            .disk_bytes_written
            .saturating_sub(first.disk_bytes_written),
        disk_read_operations: last
            .disk_read_operations
            .saturating_sub(first.disk_read_operations),
        disk_write_operations: last
            .disk_write_operations
            .saturating_sub(first.disk_write_operations),
    }
}

struct HostSampler {
    system: System,
    networks: Networks,
    pid: Pid,
}

impl HostSampler {
    fn new() -> Self {
        Self {
            system: System::new_all(),
            networks: Networks::new_with_refreshed_list(),
            pid: Pid::from_u32(std::process::id()),
        }
    }

    fn sample(
        &mut self,
        started: Instant,
        counters: &ApplicationCounters,
        store_metrics: &StoreMetrics,
        slate_metrics: &BenchmarkMetricsRecorder,
        database_size_bytes: u64,
    ) -> TimeseriesSample {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );
        self.networks.refresh(true);
        let (cpu_percent, rss_bytes, disk_bytes_read, disk_bytes_written) = self
            .system
            .process(self.pid)
            .map(|process| {
                let disk = process.disk_usage();
                (
                    process.cpu_usage() as f64,
                    process.memory(),
                    disk.total_read_bytes,
                    disk.total_written_bytes,
                )
            })
            .unwrap_or_default();
        let (network_bytes_received, network_bytes_sent) =
            self.networks
                .iter()
                .fold((0_u64, 0_u64), |(received, sent), (_, data)| {
                    (
                        received.saturating_add(data.total_received()),
                        sent.saturating_add(data.total_transmitted()),
                    )
                });
        let (disk_read_operations, disk_write_operations) = linux_io_operations(self.pid);
        let StoreSnapshot {
            operations,
            requests,
            successful_requests,
            request_errors,
            client_errors,
            server_errors,
            transport_errors,
            bytes_read,
            bytes_written,
            operation_bytes_read,
            operation_bytes_written,
            ..
        } = store_metrics.snapshot();
        TimeseriesSample {
            offset_ns: duration_ns(started.elapsed()),
            operations: counters.operations.load(Ordering::Relaxed),
            errors: counters.errors.load(Ordering::Relaxed),
            cpu_percent,
            rss_bytes,
            network_bytes_sent,
            network_bytes_received,
            disk_bytes_read,
            disk_bytes_written,
            disk_read_operations,
            disk_write_operations,
            database_size_bytes,
            object_store_operations: operations,
            object_store_requests: requests,
            object_store_successful_requests: successful_requests,
            object_store_request_errors: request_errors,
            object_store_client_errors: client_errors,
            object_store_server_errors: server_errors,
            object_store_transport_errors: transport_errors,
            object_store_bytes_read: bytes_read,
            object_store_bytes_written: bytes_written,
            object_store_operation_bytes_read: operation_bytes_read,
            object_store_operation_bytes_written: operation_bytes_written,
            slatedb_metrics: slate_metrics
                .snapshot()
                .all()
                .iter()
                .map(convert_metric)
                .collect(),
        }
    }
}

fn linux_io_operations(pid: Pid) -> (u64, u64) {
    let path = format!("/proc/{}/io", pid.as_u32());
    let Ok(contents) = fs::read_to_string(path) else {
        return (0, 0);
    };
    let mut reads = 0;
    let mut writes = 0;
    for line in contents.lines() {
        let mut fields = line.split_ascii_whitespace();
        match (fields.next(), fields.next()) {
            (Some("syscr:"), Some(value)) => reads = value.parse().unwrap_or(0),
            (Some("syscw:"), Some(value)) => writes = value.parse().unwrap_or(0),
            _ => {}
        }
    }
    (reads, writes)
}

fn convert_metric(metric: &slatedb_common::metrics::Metric) -> MetricSnapshot {
    let labels = metric.labels.iter().cloned().collect::<BTreeMap<_, _>>();
    let value = match &metric.value {
        SlateMetricValue::Counter(value) => MetricValue::Counter(*value),
        SlateMetricValue::Gauge(value) => MetricValue::Gauge(*value),
        SlateMetricValue::UpDownCounter(value) => MetricValue::UpDownCounter(*value),
        SlateMetricValue::Histogram {
            count,
            sum,
            min,
            max,
            boundaries,
            bucket_counts,
        } => MetricValue::Histogram {
            count: *count,
            sum: finite_or_zero(*sum),
            min: finite_or_zero(*min),
            max: finite_or_zero(*max),
            boundaries: boundaries.clone(),
            bucket_counts: bucket_counts.clone(),
        },
    };
    MetricSnapshot {
        name: metric.name.clone(),
        description: metric.description.clone(),
        labels,
        value,
    }
}

fn compact_metric_value(
    value: MetricValue,
) -> (MetricValueType, Option<Vec<f64>>, MetricSeriesValue) {
    match value {
        MetricValue::Counter(value) => (
            MetricValueType::Counter,
            None,
            MetricSeriesValue::Scalar(value.into()),
        ),
        MetricValue::Gauge(value) => (
            MetricValueType::Gauge,
            None,
            MetricSeriesValue::Scalar(value.into()),
        ),
        MetricValue::UpDownCounter(value) => (
            MetricValueType::UpDownCounter,
            None,
            MetricSeriesValue::Scalar(value.into()),
        ),
        MetricValue::Histogram {
            count,
            sum,
            min,
            max,
            boundaries,
            bucket_counts,
        } => (
            MetricValueType::Histogram,
            Some(boundaries),
            MetricSeriesValue::Histogram(MetricHistogramValue {
                count,
                sum,
                min,
                max,
                bucket_counts,
            }),
        ),
    }
}

fn ensure_metric_definition(
    series: &MetricSeries,
    description: &str,
    value_type: MetricValueType,
    boundaries: &Option<Vec<f64>>,
) -> Result<()> {
    ensure!(
        series.description == description
            && series.value_type == value_type
            && series.boundaries == *boundaries,
        "SlateDB metric {} changed its definition during sampling",
        series.name
    );
    Ok(())
}

fn finite_or_zero(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::{
        append_timeseries, compact_timeseries, is_tigris_endpoint, measure_backpressure,
        ApplicationCounters, BenchmarkMetricsRecorder,
    };
    use crate::model::{
        MetricSnapshot, MetricValue, MetricValueType, TimeseriesFile, TimeseriesSample,
    };
    use slatedb_common::metrics::{MetricValue as SlateMetricValue, MetricsRecorder};
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn recognizes_current_tigris_endpoint() {
        assert!(is_tigris_endpoint("https://t3.storage.dev"));
    }

    #[test]
    fn drains_application_measurements_into_one_window() {
        let counters = ApplicationCounters::new(true);
        let worker = counters.register_window_recorder();
        worker.record_success("read-modify-write", Duration::from_millis(2), 1024, 256);
        worker.record_background_success("writer-update", Duration::from_secs(1), 0, 128);
        worker.record_error("read", Duration::from_millis(4));
        worker.record_api_latency("get", Duration::from_millis(3));
        worker.record_offered(3, 1);

        let (window, histograms) = counters
            .drain_window(0, 1_000_000_000)
            .expect("drain application window");

        assert_eq!(window.completed_operations, 3);
        assert_eq!(window.successful_operations, 2);
        assert_eq!(window.errors, 1);
        assert_eq!(window.read_payload_bytes, 1024);
        assert_eq!(window.write_payload_bytes, 384);
        assert_eq!(window.payload_bytes, 1408);
        assert_eq!(window.offered_operations, Some(3));
        assert_eq!(window.dropped_operations, Some(1));
        assert_eq!(window.return_latency.expect("return latency").count, 2);
        assert_eq!(window.return_latency_by_operation["writer-update"].count, 1);
        assert!(window.response_latency.is_none());
        assert_eq!(window.api_latency["get"].count, 1);
        assert_eq!(histograms.get("return").expect("return histogram").len(), 2);
        assert_eq!(
            histograms
                .get("return/writer-update")
                .expect("writer return histogram")
                .len(),
            1
        );
        assert_eq!(histograms.get("api/get").expect("API histogram").len(), 1);
    }

    #[test]
    fn records_open_loop_completion_and_resets_reusable_window_buffers() {
        let counters = ApplicationCounters::new(true);
        let worker = counters.register_window_recorder();
        worker.record_open_loop_completion(
            "read",
            "get",
            true,
            Duration::from_millis(2),
            Duration::from_millis(1),
            Duration::from_millis(3),
            Duration::from_micros(50),
            1024,
            0,
        );

        let (first, first_histograms) = counters
            .drain_window(0, 1_000_000_000)
            .expect("drain first application window");
        assert_eq!(first.completed_operations, 1);
        assert_eq!(first.successful_operations, 1);
        assert_eq!(first.errors, 0);
        assert_eq!(first.read_payload_bytes, 1024);
        assert_eq!(first.return_latency.expect("return latency").count, 1);
        assert_eq!(first.api_latency["get"].count, 1);
        assert_eq!(first.response_latency.expect("response latency").count, 1);
        assert_eq!(
            first
                .scheduling_delay
                .expect("scheduling delay latency")
                .count,
            1
        );
        assert_eq!(
            first_histograms
                .get("return/read")
                .expect("read return histogram")
                .len(),
            1
        );

        worker.record_open_loop_completion(
            "read",
            "get",
            false,
            Duration::from_millis(4),
            Duration::from_millis(3),
            Duration::from_millis(5),
            Duration::from_micros(75),
            0,
            0,
        );
        let (second, _) = counters
            .drain_window(1_000_000_000, 1_000_000_000)
            .expect("drain second application window");
        assert_eq!(second.completed_operations, 1);
        assert_eq!(second.successful_operations, 0);
        assert_eq!(second.errors, 1);
        assert_eq!(second.read_payload_bytes, 0);

        let (empty, empty_histograms) = counters
            .drain_window(2_000_000_000, 1_000_000_000)
            .expect("drain empty application window");
        assert_eq!(empty.completed_operations, 0);
        assert!(empty.return_latency.is_none());
        assert!(empty_histograms.get("return").is_none());
    }

    #[tokio::test]
    async fn measures_backpressure_between_counter_and_memory_recheck() {
        let recorder = BenchmarkMetricsRecorder::new();
        let counter = recorder.register_counter(
            slatedb::db_stats::BACKPRESSURE_COUNT,
            "backpressure events",
            &[],
        );
        let gauge = recorder.register_gauge(
            slatedb::db_stats::TOTAL_MEM_SIZE_BYTES,
            "unflushed bytes",
            &[],
        );

        let ((), elapsed) = measure_backpressure(async {
            gauge.set(100);
            counter.increment(1);
            tokio::time::sleep(Duration::from_millis(5)).await;
            gauge.set(50);
        })
        .await;

        assert!(elapsed >= Duration::from_millis(5));
        let snapshot = recorder.snapshot();
        assert!(snapshot
            .by_name(slatedb::db_stats::BACKPRESSURE_COUNT)
            .iter()
            .any(|metric| matches!(&metric.value, SlateMetricValue::Counter(1))));
        assert!(snapshot
            .by_name(slatedb::db_stats::TOTAL_MEM_SIZE_BYTES)
            .iter()
            .any(|metric| matches!(&metric.value, SlateMetricValue::Gauge(50))));
    }

    #[test]
    fn compacts_and_appends_metric_series_by_identity() {
        let mut first = compact_timeseries(vec![
            sample(0, vec![counter("writes", 1)]),
            sample(1, vec![counter("writes", 2)]),
        ])
        .expect("first phase");
        let second = compact_timeseries(vec![
            sample(2, vec![counter("writes", 3), gauge("queue", 7)]),
            sample(3, vec![counter("writes", 4), gauge("queue", 5)]),
        ])
        .expect("second phase");

        append_timeseries(&mut first, second).expect("append phase");

        assert_eq!(first.samples.len(), 4);
        assert_eq!(first.slatedb_metrics.len(), 2);
        let writes = series(&first, "writes");
        assert_eq!(writes.values.len(), 4);
        assert!(writes.values.iter().all(Option::is_some));
        let queue = series(&first, "queue");
        assert_eq!(queue.value_type, MetricValueType::Gauge);
        assert_eq!(queue.values.len(), 4);
        assert!(queue.values[..2].iter().all(Option::is_none));
        assert!(queue.values[2..].iter().all(Option::is_some));

        let encoded = serde_json::to_vec(&first).expect("serialize time series");
        let decoded: TimeseriesFile =
            serde_json::from_slice(&encoded).expect("deserialize time series");
        assert_eq!(decoded.samples.len(), 4);
        assert_eq!(decoded.slatedb_metrics.len(), 2);
        let value = serde_json::to_value(&decoded).expect("time series value");
        assert!(value["samples"]
            .as_array()
            .expect("samples")
            .iter()
            .all(|sample| sample.get("slatedb_metrics").is_none()));
    }

    #[test]
    fn ninety_minute_columnar_metrics_fit_below_github_limit() {
        let mut metrics = Vec::new();
        for index in 0..95 {
            metrics.push(counter(&format!("counter-{index}"), index));
        }
        for index in 0..9 {
            metrics.push(gauge(&format!("gauge-{index}"), index as i64));
        }
        metrics.push(MetricSnapshot {
            name: "running-compactions".to_string(),
            description: "A representative up/down counter description".to_string(),
            labels: BTreeMap::from([("worker".to_string(), "local".to_string())]),
            value: MetricValue::UpDownCounter(1),
        });
        for index in 0..20 {
            metrics.push(histogram(&format!("histogram-{index}")));
        }
        let mut timeseries = compact_timeseries(vec![
            sample(0, metrics.clone()),
            sample(1_000_000_000, metrics),
        ])
        .expect("compact representative metrics");
        let sample = timeseries.samples[1].clone();
        timeseries.samples.resize(5_402, sample);
        for metric in &mut timeseries.slatedb_metrics {
            let value = metric.values[1].clone();
            metric.values.resize(5_402, value);
        }

        let encoded = serde_json::to_vec(&timeseries).expect("serialize projected time series");
        assert!(encoded.len() < 100 * 1024 * 1024);
        let text = std::str::from_utf8(&encoded).expect("JSON is UTF-8");
        assert_eq!(
            text.matches("\"description\"").count(),
            timeseries.slatedb_metrics.len()
        );
    }

    fn sample(offset_ns: u64, slatedb_metrics: Vec<MetricSnapshot>) -> TimeseriesSample {
        TimeseriesSample {
            offset_ns,
            slatedb_metrics,
            ..Default::default()
        }
    }

    fn counter(name: &str, value: u64) -> MetricSnapshot {
        MetricSnapshot {
            name: name.to_string(),
            description: "A representative counter description".to_string(),
            labels: BTreeMap::new(),
            value: MetricValue::Counter(value),
        }
    }

    fn gauge(name: &str, value: i64) -> MetricSnapshot {
        MetricSnapshot {
            name: name.to_string(),
            description: "A representative gauge description".to_string(),
            labels: BTreeMap::new(),
            value: MetricValue::Gauge(value),
        }
    }

    fn histogram(name: &str) -> MetricSnapshot {
        MetricSnapshot {
            name: name.to_string(),
            description: "A representative histogram description".to_string(),
            labels: BTreeMap::new(),
            value: MetricValue::Histogram {
                count: 1_000,
                sum: 50_000.0,
                min: 1.0,
                max: 100.0,
                boundaries: (0..12).map(|value| value as f64 * 10.0).collect(),
                bucket_counts: vec![77; 13],
            },
        }
    }

    fn series<'a>(timeseries: &'a TimeseriesFile, name: &str) -> &'a crate::model::MetricSeries {
        timeseries
            .slatedb_metrics
            .iter()
            .find(|metric| metric.name == name)
            .expect("metric series")
    }
}
