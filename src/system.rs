use crate::histogram::HistogramSet;
use crate::instrumented_store::{StoreMetrics, StoreSnapshot};
use crate::model::{
    ApplicationMetrics, ApplicationSeries, DistributionSummary, Environment, LatencyTimeSeries,
    MachineSeries, MachineStatistics, ObjectStoreMetrics, ObjectStoreSeries, ProcessSeries,
    ProcessStatistics, RateSummary, ThroughputSummary, WorkloadSeries,
};
use anyhow::{Context, Result};
use slatedb_common::metrics::{
    CounterFn, DefaultMetricsRecorder, GaugeFn, HistogramFn, Metrics, MetricsRecorder,
    UpDownCounterFn,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{Disks, Networks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::{oneshot, watch};

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

#[derive(Debug, Clone, Default)]
pub struct OperationDelta {
    pub calls: u64,
    pub logical_bytes: u64,
    pub errors: u64,
}

impl OperationDelta {
    fn merge(&mut self, other: &Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.logical_bytes = self.logical_bytes.saturating_add(other.logical_bytes);
        self.errors = self.errors.saturating_add(other.errors);
    }
}

#[derive(Debug, Clone, Default)]
pub struct ApplicationWindow {
    pub elapsed: Duration,
    pub duration: Duration,
    pub operations: BTreeMap<String, OperationDelta>,
}

#[derive(Debug, Clone, Default)]
struct LatencyWindow {
    elapsed: Duration,
    duration: Duration,
    histograms: HistogramSet,
}

#[derive(Debug, Default)]
struct ApplicationDelta {
    operations: BTreeMap<String, OperationDelta>,
    histograms: HistogramSet,
}

impl ApplicationDelta {
    fn merge(&mut self, other: &Self) -> Result<()> {
        for (name, operation) in &other.operations {
            self.operations
                .entry(name.clone())
                .or_default()
                .merge(operation);
        }
        self.histograms.merge(&other.histograms)
    }

    fn reset(&mut self) {
        self.operations.clear();
        self.histograms.reset();
    }
}

#[derive(Debug, Default)]
struct ApplicationShard {
    active: ApplicationDelta,
    spare: Option<ApplicationDelta>,
}

#[derive(Debug, Clone)]
pub struct ApplicationRecorder {
    inner: Arc<Mutex<ApplicationShard>>,
}

impl ApplicationRecorder {
    pub fn record_success(&self, api: &str, latency: Duration, logical_bytes: u64) {
        self.record(api, latency, logical_bytes, false);
    }

    pub fn record_error(&self, api: &str, latency: Duration) {
        self.record(api, latency, 0, true);
    }

    pub fn record_latency(&self, api: &str, latency: Duration) {
        self.inner
            .lock()
            .expect("application recorder lock")
            .active
            .histograms
            .record(format!("api/{api}"), latency);
    }

    fn record(&self, api: &str, latency: Duration, logical_bytes: u64, error: bool) {
        let mut shard = self.inner.lock().expect("application recorder lock");
        let operation = shard.active.operations.entry(api.to_string()).or_default();
        operation.calls = operation.calls.saturating_add(1);
        operation.logical_bytes = operation.logical_bytes.saturating_add(logical_bytes);
        operation.errors = operation.errors.saturating_add(u64::from(error));
        shard
            .active
            .histograms
            .record(format!("api/{api}"), latency);
    }
}

#[derive(Debug, Default)]
pub struct ApplicationRegistry {
    shards: Mutex<Vec<Arc<Mutex<ApplicationShard>>>>,
}

impl ApplicationRegistry {
    pub fn recorder(&self) -> ApplicationRecorder {
        let inner = Arc::new(Mutex::new(ApplicationShard::default()));
        self.shards
            .lock()
            .expect("application registry lock")
            .push(Arc::clone(&inner));
        ApplicationRecorder { inner }
    }

    fn drain(&self) -> Result<ApplicationDelta> {
        let shards = self
            .shards
            .lock()
            .expect("application registry lock")
            .clone();
        let mut merged = ApplicationDelta::default();
        for shard in shards {
            let mut delta = {
                let mut shard = shard.lock().expect("application recorder lock");
                let replacement = shard.spare.take().unwrap_or_default();
                std::mem::replace(&mut shard.active, replacement)
            };
            merged.merge(&delta)?;
            delta.reset();
            let previous = shard
                .lock()
                .expect("application recorder lock")
                .spare
                .replace(delta);
            debug_assert!(previous.is_none());
        }
        Ok(merged)
    }
}

#[derive(Debug, Clone)]
struct ResourceWindow {
    elapsed: Duration,
    duration: Duration,
    process_cpu_cores: f64,
    process_rss_bytes: f64,
    machine_cpu_percent: f64,
    network_receive_bytes_per_second: f64,
    network_send_bytes_per_second: f64,
    disk_read_bytes_per_second: f64,
    disk_write_bytes_per_second: f64,
    disk_read_operations_per_second: f64,
    disk_write_operations_per_second: f64,
}

#[derive(Debug, Clone)]
struct StoreWindow {
    duration: Duration,
    delta: StoreSnapshot,
}

pub struct SampledMeasurement {
    elapsed: Duration,
    application_total: ApplicationDelta,
    application_windows: Vec<ApplicationWindow>,
    latency_windows: Vec<LatencyWindow>,
    store_start: StoreSnapshot,
    store_end: StoreSnapshot,
    store_windows: Vec<StoreWindow>,
    resources: Vec<ResourceWindow>,
}

#[derive(Debug)]
pub(crate) struct RateWindowControl {
    active: Mutex<bool>,
}

impl RateWindowControl {
    pub(crate) fn new() -> Self {
        Self {
            active: Mutex::new(true),
        }
    }

    /// Stops rate-window capture after any in-flight sample completes.
    pub(crate) fn finish(&self) {
        *self.active.lock().expect("rate window control lock") = false;
    }
}

impl SampledMeasurement {
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn operation_total(&self, name: &str) -> u64 {
        self.application_total
            .operations
            .get(name)
            .map_or(0, |operation| operation.calls)
    }

    pub fn errors(&self) -> u64 {
        self.application_total
            .operations
            .values()
            .map(|operation| operation.errors)
            .sum::<u64>()
            .saturating_add(self.store_end.difference(&self.store_start).errors())
    }

    pub fn application(&self) -> ApplicationMetrics {
        let elapsed = self.elapsed();
        let mut operations = BTreeMap::new();
        let mut throughput = BTreeMap::new();
        for (name, total) in &self.application_total.operations {
            let call_windows = self
                .application_windows
                .iter()
                .map(|window| {
                    let calls = window
                        .operations
                        .get(name)
                        .map_or(0, |operation| operation.calls);
                    rate(calls, window.duration)
                })
                .collect::<Vec<_>>();
            operations.insert(
                name.clone(),
                rate_summary(total.calls, elapsed, &call_windows),
            );
            if total.logical_bytes > 0 {
                let byte_windows = self
                    .application_windows
                    .iter()
                    .map(|window| {
                        let bytes = window
                            .operations
                            .get(name)
                            .map_or(0, |operation| operation.logical_bytes);
                        rate(bytes, window.duration)
                    })
                    .collect::<Vec<_>>();
                throughput.insert(
                    name.clone(),
                    throughput_summary(total.logical_bytes, elapsed, &byte_windows),
                );
            }
        }
        ApplicationMetrics {
            operations,
            throughput,
            latency: self
                .application_total
                .histograms
                .summaries_with_prefix("api/"),
        }
    }

    pub fn object_store(&self) -> ObjectStoreMetrics {
        let elapsed = self.elapsed();
        let total = self.store_end.difference(&self.store_start);
        let mut requests = BTreeMap::new();
        let mut throughput = BTreeMap::new();
        for (method, count) in &total.requests {
            if *count == 0 {
                continue;
            }
            let windows = self
                .store_windows
                .iter()
                .map(|window| {
                    rate(
                        window.delta.requests.get(method).copied().unwrap_or(0),
                        window.duration,
                    )
                })
                .collect::<Vec<_>>();
            requests.insert(method.clone(), rate_summary(*count, elapsed, &windows));
            let bytes = total.body_bytes(method);
            if bytes > 0 {
                let byte_windows = self
                    .store_windows
                    .iter()
                    .map(|window| rate(window.delta.body_bytes(method), window.duration))
                    .collect::<Vec<_>>();
                throughput.insert(
                    method.clone(),
                    throughput_summary(bytes, elapsed, &byte_windows),
                );
            }
        }
        ObjectStoreMetrics {
            requests,
            throughput,
        }
    }

    pub fn process(&self) -> ProcessStatistics {
        ProcessStatistics {
            cpu_cores: summarize_values(
                self.resources
                    .iter()
                    .map(|window| window.process_cpu_cores)
                    .collect(),
            ),
            rss_bytes: summarize_values(
                self.resources
                    .iter()
                    .map(|window| window.process_rss_bytes)
                    .collect(),
            ),
        }
    }

    pub fn machine(&self) -> MachineStatistics {
        let values = |select: fn(&ResourceWindow) -> f64| {
            summarize_values(self.resources.iter().map(select).collect())
        };
        MachineStatistics {
            cpu_percent: values(|window| window.machine_cpu_percent),
            rss_bytes: values(|window| window.process_rss_bytes),
            network_receive_bytes_per_second: values(|window| {
                window.network_receive_bytes_per_second
            }),
            network_send_bytes_per_second: values(|window| window.network_send_bytes_per_second),
            disk_read_bytes_per_second: values(|window| window.disk_read_bytes_per_second),
            disk_write_bytes_per_second: values(|window| window.disk_write_bytes_per_second),
            disk_read_operations_per_second: values(|window| {
                window.disk_read_operations_per_second
            }),
            disk_write_operations_per_second: values(|window| {
                window.disk_write_operations_per_second
            }),
        }
    }

    pub fn series(&self) -> WorkloadSeries {
        let rate_elapsed_ns = self
            .application_windows
            .iter()
            .map(|window| duration_ns(window.elapsed))
            .collect();
        let rate_duration_ns = self
            .application_windows
            .iter()
            .map(|window| duration_ns(window.duration))
            .collect();
        let mut operations_per_second = BTreeMap::new();
        let mut application_bytes_per_second = BTreeMap::new();
        for (name, total) in &self.application_total.operations {
            operations_per_second.insert(
                name.clone(),
                self.application_windows
                    .iter()
                    .map(|window| {
                        rate(
                            window
                                .operations
                                .get(name)
                                .map_or(0, |operation| operation.calls),
                            window.duration,
                        )
                    })
                    .collect(),
            );
            if total.logical_bytes > 0 {
                application_bytes_per_second.insert(
                    name.clone(),
                    self.application_windows
                        .iter()
                        .map(|window| {
                            rate(
                                window
                                    .operations
                                    .get(name)
                                    .map_or(0, |operation| operation.logical_bytes),
                                window.duration,
                            )
                        })
                        .collect(),
                );
            }
        }

        let latency_summaries = self
            .application_total
            .histograms
            .summaries_with_prefix("api/");
        let latency_ns = latency_summaries
            .keys()
            .map(|name| {
                let histogram_name = format!("api/{name}");
                let summaries = self
                    .latency_windows
                    .iter()
                    .map(|window| window.histograms.summary(&histogram_name))
                    .collect::<Vec<_>>();
                let values = LatencyTimeSeries {
                    avg: summaries
                        .iter()
                        .map(|summary| summary.as_ref().map(|value| value.avg_ns))
                        .collect(),
                    p50: summaries
                        .iter()
                        .map(|summary| summary.as_ref().map(|value| value.p50_ns as f64))
                        .collect(),
                    p95: summaries
                        .iter()
                        .map(|summary| summary.as_ref().map(|value| value.p95_ns as f64))
                        .collect(),
                    p99: summaries
                        .iter()
                        .map(|summary| summary.as_ref().map(|value| value.p99_ns as f64))
                        .collect(),
                    p999: summaries
                        .iter()
                        .map(|summary| summary.as_ref().map(|value| value.p999_ns as f64))
                        .collect(),
                };
                (name.clone(), values)
            })
            .collect();

        let store_total = self.store_end.difference(&self.store_start);
        let mut requests_per_second = BTreeMap::new();
        let mut store_bytes_per_second = BTreeMap::new();
        for (method, count) in &store_total.requests {
            if *count == 0 {
                continue;
            }
            requests_per_second.insert(
                method.clone(),
                self.store_windows
                    .iter()
                    .map(|window| {
                        rate(
                            window.delta.requests.get(method).copied().unwrap_or(0),
                            window.duration,
                        )
                    })
                    .collect(),
            );
            if store_total.body_bytes(method) > 0 {
                store_bytes_per_second.insert(
                    method.clone(),
                    self.store_windows
                        .iter()
                        .map(|window| rate(window.delta.body_bytes(method), window.duration))
                        .collect(),
                );
            }
        }

        WorkloadSeries {
            rate_elapsed_ns,
            rate_duration_ns,
            latency_elapsed_ns: self
                .latency_windows
                .iter()
                .map(|window| duration_ns(window.elapsed))
                .collect(),
            latency_duration_ns: self
                .latency_windows
                .iter()
                .map(|window| duration_ns(window.duration))
                .collect(),
            resource_elapsed_ns: self
                .resources
                .iter()
                .map(|window| duration_ns(window.elapsed))
                .collect(),
            resource_duration_ns: self
                .resources
                .iter()
                .map(|window| duration_ns(window.duration))
                .collect(),
            application: ApplicationSeries {
                operations_per_second,
                bytes_per_second: application_bytes_per_second,
                latency_ns,
                latency_histograms: self.application_total.histograms.series_with_prefix("api/"),
            },
            object_store: ObjectStoreSeries {
                requests_per_second,
                bytes_per_second: store_bytes_per_second,
            },
            process: ProcessSeries {
                cpu_cores: self
                    .resources
                    .iter()
                    .map(|window| window.process_cpu_cores)
                    .collect(),
                rss_bytes: self
                    .resources
                    .iter()
                    .map(|window| window.process_rss_bytes)
                    .collect(),
            },
            machine: MachineSeries {
                cpu_percent: self
                    .resources
                    .iter()
                    .map(|window| window.machine_cpu_percent)
                    .collect(),
                rss_bytes: self
                    .resources
                    .iter()
                    .map(|window| window.process_rss_bytes)
                    .collect(),
                network_receive_bytes_per_second: self
                    .resources
                    .iter()
                    .map(|window| window.network_receive_bytes_per_second)
                    .collect(),
                network_send_bytes_per_second: self
                    .resources
                    .iter()
                    .map(|window| window.network_send_bytes_per_second)
                    .collect(),
                disk_read_bytes_per_second: self
                    .resources
                    .iter()
                    .map(|window| window.disk_read_bytes_per_second)
                    .collect(),
                disk_write_bytes_per_second: self
                    .resources
                    .iter()
                    .map(|window| window.disk_write_bytes_per_second)
                    .collect(),
                disk_read_operations_per_second: self
                    .resources
                    .iter()
                    .map(|window| window.disk_read_operations_per_second)
                    .collect(),
                disk_write_operations_per_second: self
                    .resources
                    .iter()
                    .map(|window| window.disk_write_operations_per_second)
                    .collect(),
            },
        }
    }
}

pub async fn sample_until_stopped(
    registry: Arc<ApplicationRegistry>,
    store_metrics: Arc<StoreMetrics>,
    stop: watch::Receiver<bool>,
    ready: Option<oneshot::Sender<()>>,
) -> Result<SampledMeasurement> {
    sample_until_stopped_with_rate_control(
        registry,
        store_metrics,
        Arc::new(RateWindowControl::new()),
        stop,
        ready,
    )
    .await
}

pub(crate) async fn sample_until_stopped_with_rate_control(
    registry: Arc<ApplicationRegistry>,
    store_metrics: Arc<StoreMetrics>,
    rate_windows: Arc<RateWindowControl>,
    mut stop: watch::Receiver<bool>,
    ready: Option<oneshot::Sender<()>>,
) -> Result<SampledMeasurement> {
    let mut host = HostSampler::new();
    let mut previous_host = host.snapshot();
    let started = Instant::now();
    let store_start = store_metrics.snapshot();
    let mut previous_store = store_start.clone();
    let mut previous_at = started;
    let mut application_total = ApplicationDelta::default();
    let mut application_windows = Vec::new();
    let mut latency_windows = Vec::new();
    let mut store_windows = Vec::new();
    let mut resources = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    if let Some(ready) = ready {
        let _ = ready.send(());
    }
    while !*stop.borrow() {
        tokio::select! {
            _ = interval.tick() => {
                // This lock makes `finish` a boundary: the drain cannot start
                // until any sample already in progress has taken its snapshots.
                let rate_windows_active = rate_windows
                    .active
                    .lock()
                    .expect("rate window control lock");
                let now = Instant::now();
                let duration = now.saturating_duration_since(previous_at);
                let elapsed = now.saturating_duration_since(started);
                let application = registry.drain()?;
                let store = store_metrics.snapshot();
                let host_snapshot = host.snapshot();
                application_total.merge(&application)?;
                latency_windows.push(LatencyWindow {
                    elapsed,
                    duration,
                    histograms: application.histograms,
                });
                if *rate_windows_active {
                    application_windows.push(ApplicationWindow {
                        elapsed,
                        duration,
                        operations: application.operations,
                    });
                    store_windows.push(StoreWindow {
                        duration,
                        delta: store.difference(&previous_store),
                    });
                }
                resources.push(host_snapshot.window(&previous_host, elapsed, duration));
                previous_at = now;
                previous_store = store;
                previous_host = host_snapshot;
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
        }
    }
    let application = registry.drain()?;
    let store_end = store_metrics.snapshot();
    let ended = Instant::now();
    let duration = ended.saturating_duration_since(previous_at);
    application_total.merge(&application)?;
    latency_windows.push(LatencyWindow {
        elapsed: ended.saturating_duration_since(started),
        duration,
        histograms: application.histograms,
    });
    if *rate_windows
        .active
        .lock()
        .expect("rate window control lock")
    {
        application_windows.push(ApplicationWindow {
            elapsed: ended.saturating_duration_since(started),
            duration,
            operations: application.operations,
        });
        store_windows.push(StoreWindow {
            duration,
            delta: store_end.difference(&previous_store),
        });
    }
    Ok(SampledMeasurement {
        elapsed: ended.saturating_duration_since(started),
        application_total,
        application_windows,
        latency_windows,
        store_start,
        store_end,
        store_windows,
        resources,
    })
}

pub async fn measure_until_complete<T, F>(
    registry: Arc<ApplicationRegistry>,
    store_metrics: Arc<StoreMetrics>,
    operation: F,
) -> Result<(T, SampledMeasurement)>
where
    F: Future<Output = Result<T>>,
{
    let (stop_tx, stop_rx) = watch::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let sampler = tokio::spawn(sample_until_stopped(
        registry,
        store_metrics,
        stop_rx,
        Some(ready_tx),
    ));
    ready_rx
        .await
        .context("metric sampler stopped before taking its baselines")?;
    let value = operation.await;
    let _ = stop_tx.send(true);
    let measurement = sampler.await.context("joining metric sampler")??;
    Ok((value?, measurement))
}

fn rate(value: u64, duration: Duration) -> f64 {
    value as f64 / duration.as_secs_f64().max(f64::EPSILON)
}

fn rate_summary(total: u64, elapsed: Duration, windows: &[f64]) -> RateSummary {
    let distribution = summarize_values(windows.to_vec());
    RateSummary {
        total,
        avg_per_second: rate(total, elapsed),
        p50_per_second: distribution.p50,
        p95_per_second: distribution.p95,
        p99_per_second: distribution.p99,
        p999_per_second: distribution.p999,
        min_per_second: distribution.min,
        max_per_second: distribution.max,
    }
}

fn throughput_summary(total_bytes: u64, elapsed: Duration, windows: &[f64]) -> ThroughputSummary {
    let distribution = summarize_values(windows.to_vec());
    ThroughputSummary {
        total_bytes,
        avg_bytes_per_second: rate(total_bytes, elapsed),
        p50_bytes_per_second: distribution.p50,
        p95_bytes_per_second: distribution.p95,
        p99_bytes_per_second: distribution.p99,
        p999_bytes_per_second: distribution.p999,
        min_bytes_per_second: distribution.min,
        max_bytes_per_second: distribution.max,
    }
}

fn summarize_values(mut values: Vec<f64>) -> DistributionSummary {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return DistributionSummary::default();
    }
    values.sort_by(f64::total_cmp);
    let percentile = |quantile: f64| {
        let index = (quantile * (values.len().saturating_sub(1)) as f64).round() as usize;
        values[index]
    };
    DistributionSummary {
        avg: values.iter().sum::<f64>() / values.len() as f64,
        p50: percentile(0.5),
        p95: percentile(0.95),
        p99: percentile(0.99),
        p999: percentile(0.999),
        min: values[0],
        max: values[values.len() - 1],
    }
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

pub fn verify_environment(environment: &Environment) -> Result<()> {
    anyhow::ensure!(
        environment.runner_type == "warp-ubuntu-latest-x64-16x",
        "published runs require warp-ubuntu-latest-x64-16x"
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

#[derive(Clone, Default)]
struct HostSnapshot {
    process_cpu_cores: f64,
    process_rss_bytes: u64,
    machine_cpu_percent: f64,
    network_received: u64,
    network_sent: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
    disk_read_operations: u64,
    disk_write_operations: u64,
}

impl HostSnapshot {
    fn window(&self, previous: &Self, elapsed: Duration, duration: Duration) -> ResourceWindow {
        let seconds = duration.as_secs_f64().max(f64::EPSILON);
        ResourceWindow {
            elapsed,
            duration,
            process_cpu_cores: self.process_cpu_cores,
            process_rss_bytes: self.process_rss_bytes as f64,
            machine_cpu_percent: self.machine_cpu_percent,
            network_receive_bytes_per_second: self
                .network_received
                .saturating_sub(previous.network_received)
                as f64
                / seconds,
            network_send_bytes_per_second: self.network_sent.saturating_sub(previous.network_sent)
                as f64
                / seconds,
            disk_read_bytes_per_second: self
                .disk_read_bytes
                .saturating_sub(previous.disk_read_bytes)
                as f64
                / seconds,
            disk_write_bytes_per_second: self
                .disk_write_bytes
                .saturating_sub(previous.disk_write_bytes)
                as f64
                / seconds,
            disk_read_operations_per_second: self
                .disk_read_operations
                .saturating_sub(previous.disk_read_operations)
                as f64
                / seconds,
            disk_write_operations_per_second: self
                .disk_write_operations
                .saturating_sub(previous.disk_write_operations)
                as f64
                / seconds,
        }
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

    fn snapshot(&mut self) -> HostSnapshot {
        self.system.refresh_cpu_usage();
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[self.pid]),
            true,
            ProcessRefreshKind::everything(),
        );
        self.networks.refresh(true);
        let (process_cpu_cores, process_rss_bytes) = self
            .system
            .process(self.pid)
            .map(|process| (process.cpu_usage() as f64 / 100.0, process.memory()))
            .unwrap_or_default();
        let (network_received, network_sent) = self
            .networks
            .iter()
            .filter(|(name, _)| *name != "lo")
            .fold((0_u64, 0_u64), |(received, sent), (_, data)| {
                (
                    received.saturating_add(data.total_received()),
                    sent.saturating_add(data.total_transmitted()),
                )
            });
        let disk = linux_disk_totals();
        HostSnapshot {
            process_cpu_cores,
            process_rss_bytes,
            machine_cpu_percent: self.system.global_cpu_usage() as f64,
            network_received,
            network_sent,
            disk_read_bytes: disk.0,
            disk_write_bytes: disk.1,
            disk_read_operations: disk.2,
            disk_write_operations: disk.3,
        }
    }
}

fn linux_disk_totals() -> (u64, u64, u64, u64) {
    let Ok(contents) = fs::read_to_string("/proc/diskstats") else {
        return (0, 0, 0, 0);
    };
    let mut total = (0_u64, 0_u64, 0_u64, 0_u64);
    for line in contents.lines() {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        if fields.len() < 14 || !is_physical_disk(fields[2]) {
            continue;
        }
        let read_ops = fields[3].parse::<u64>().unwrap_or(0);
        let read_sectors = fields[5].parse::<u64>().unwrap_or(0);
        let write_ops = fields[7].parse::<u64>().unwrap_or(0);
        let write_sectors = fields[9].parse::<u64>().unwrap_or(0);
        total.0 = total.0.saturating_add(read_sectors.saturating_mul(512));
        total.1 = total.1.saturating_add(write_sectors.saturating_mul(512));
        total.2 = total.2.saturating_add(read_ops);
        total.3 = total.3.saturating_add(write_ops);
    }
    total
}

fn is_physical_disk(name: &str) -> bool {
    if name.starts_with("loop") || name.starts_with("ram") || name.starts_with("dm-") {
        return false;
    }
    if name.starts_with("nvme") || name.starts_with("mmcblk") {
        return !name.contains('p');
    }
    if name.starts_with("sd") || name.starts_with("vd") || name.starts_with("xvd") {
        return !name.ends_with(|character: char| character.is_ascii_digit());
    }
    true
}

pub fn counter_value(metrics: &Metrics, name: &str) -> u64 {
    metrics
        .by_name(name)
        .iter()
        .filter_map(|metric| match metric.value {
            slatedb_common::metrics::MetricValue::Counter(value) => Some(value),
            _ => None,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::{
        sample_until_stopped, sample_until_stopped_with_rate_control, summarize_values,
        ApplicationRegistry, RateWindowControl,
    };
    use crate::instrumented_store::{HttpMethod, StoreMetrics};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{oneshot, watch};

    #[test]
    fn distribution_contains_the_published_columns() {
        let summary = summarize_values(vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(summary.avg, 2.5);
        assert_eq!(summary.min, 1.0);
        assert_eq!(summary.max, 4.0);
        assert_eq!(summary.p50, 3.0);
    }

    #[test]
    fn application_recorder_tracks_each_api_separately() {
        let registry = ApplicationRegistry::default();
        let recorder = registry.recorder();
        recorder.record_success("get", Duration::from_millis(1), 420);
        recorder.record_success("put", Duration::from_millis(2), 420);
        recorder.record_latency("durable", Duration::from_millis(3));
        let delta = registry.drain().expect("drain");

        assert_eq!(delta.operations["get"].calls, 1);
        assert_eq!(delta.operations["put"].calls, 1);
        assert!(!delta.operations.contains_key("durable"));
        assert_eq!(delta.histograms.summaries_with_prefix("api/").len(), 3);
    }

    #[tokio::test]
    async fn final_partial_window_includes_application_and_store_activity() {
        let registry = Arc::new(ApplicationRegistry::default());
        let recorder = registry.recorder();
        let store_metrics = Arc::new(StoreMetrics::default());
        let (stop_tx, stop_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = oneshot::channel();
        let sampler = tokio::spawn(sample_until_stopped(
            Arc::clone(&registry),
            Arc::clone(&store_metrics),
            stop_rx,
            Some(ready_tx),
        ));
        ready_rx.await.expect("sampler baseline");

        recorder.record_success("flush", Duration::from_millis(1), 0);
        store_metrics.record_request(HttpMethod::Get);
        store_metrics.record_response_bytes(HttpMethod::Get, 16);
        stop_tx.send(true).expect("stop sampler");

        let measurement = sampler.await.expect("join sampler").expect("measurement");
        let application = measurement.application();
        assert_eq!(application.operations["flush"].total, 1);
        assert!(application.operations["flush"].min_per_second > 0.0);
        let object_store = measurement.object_store();
        assert_eq!(object_store.requests["GET"].total, 1);
        assert!(object_store.requests["GET"].min_per_second > 0.0);
        assert!(object_store.throughput["GET"].min_bytes_per_second > 0.0);
    }

    #[tokio::test]
    async fn drain_activity_is_totaled_without_contaminating_rate_windows() {
        let registry = Arc::new(ApplicationRegistry::default());
        let recorder = registry.recorder();
        let store_metrics = Arc::new(StoreMetrics::default());
        let rate_windows = Arc::new(RateWindowControl::new());
        let (stop_tx, stop_rx) = watch::channel(false);
        let (ready_tx, ready_rx) = oneshot::channel();
        let sampler = tokio::spawn(sample_until_stopped_with_rate_control(
            Arc::clone(&registry),
            Arc::clone(&store_metrics),
            Arc::clone(&rate_windows),
            stop_rx,
            Some(ready_tx),
        ));
        ready_rx.await.expect("sampler baseline");

        recorder.record_success("get", Duration::from_millis(1), 16);
        store_metrics.record_request(HttpMethod::Get);
        store_metrics.record_response_bytes(HttpMethod::Get, 16);
        tokio::time::sleep(Duration::from_millis(1_200)).await;

        rate_windows.finish();
        recorder.record_success("flush", Duration::from_millis(1), 0);
        store_metrics.record_request(HttpMethod::Put);
        store_metrics.record_response_bytes(HttpMethod::Put, 16);
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        stop_tx.send(true).expect("stop sampler");

        let measurement = sampler.await.expect("join sampler").expect("measurement");
        let application = measurement.application();
        assert_eq!(application.operations["get"].total, 1);
        assert!(application.operations["get"].min_per_second > 0.0);
        assert_eq!(application.operations["flush"].total, 1);
        assert_eq!(application.operations["flush"].p50_per_second, 0.0);

        let object_store = measurement.object_store();
        assert_eq!(object_store.requests["GET"].total, 1);
        assert!(object_store.requests["GET"].min_per_second > 0.0);
        assert_eq!(object_store.requests["PUT"].total, 1);
        assert_eq!(object_store.requests["PUT"].p50_per_second, 0.0);

        let series = measurement.series();
        assert_eq!(series.rate_elapsed_ns.len(), series.rate_duration_ns.len());
        assert_eq!(
            series.latency_elapsed_ns.len(),
            series.latency_duration_ns.len()
        );
        assert!(series.resource_elapsed_ns.len() > series.rate_elapsed_ns.len());
        assert!(series.latency_elapsed_ns.len() > series.rate_elapsed_ns.len());
        assert!(series.application.operations_per_second["get"]
            .iter()
            .any(|value| *value > 0.0));
        assert!(series.application.operations_per_second["flush"]
            .iter()
            .all(|value| *value == 0.0));
        assert!(series.object_store.requests_per_second["PUT"]
            .iter()
            .all(|value| *value == 0.0));
        for name in ["get", "flush"] {
            let latency = &series.application.latency_ns[name];
            for values in [
                &latency.avg,
                &latency.p50,
                &latency.p95,
                &latency.p99,
                &latency.p999,
            ] {
                assert_eq!(values.len(), series.latency_elapsed_ns.len());
                assert!(values.iter().any(Option::is_some));
            }
        }
    }
}
