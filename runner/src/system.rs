use crate::instrumented_store::{StoreMetrics, StoreSnapshot};
use crate::model::{Environment, MetricSnapshot, MetricValue, ResourceUse, TimeseriesSample};
use object_store::path::Path;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue as SlateMetricValue};
use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Disks, Networks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::watch;

#[derive(Debug, Default)]
pub struct ApplicationCounters {
    pub operations: AtomicU64,
    pub errors: AtomicU64,
}

impl ApplicationCounters {
    pub fn reset(&self) {
        self.operations.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }
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
        object_store: if provider.eq_ignore_ascii_case("aws")
            && (endpoint.contains("tigrisdata.com") || endpoint.contains("tigris.dev"))
        {
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
        environment.endpoint.contains("fly.storage.tigris.dev"),
        "published runs require the Tigris S3 endpoint"
    );
    anyhow::ensure!(
        environment.region == "fra",
        "published runs require region fra"
    );
    Ok(())
}

pub async fn sample_until_stopped(
    started: Instant,
    counters: Arc<ApplicationCounters>,
    store_metrics: Arc<StoreMetrics>,
    slate_metrics: Arc<DefaultMetricsRecorder>,
    database_path: Path,
    shared_database_bytes: u64,
    mut stop: watch::Receiver<bool>,
) -> Vec<TimeseriesSample> {
    let mut samples = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sampler = HostSampler::new();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                samples.push(sampler.sample(started, &counters, &store_metrics, &slate_metrics, &database_path, shared_database_bytes));
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    samples.push(sampler.sample(started, &counters, &store_metrics, &slate_metrics, &database_path, shared_database_bytes));
                    break;
                }
            }
        }
    }
    samples
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
        slate_metrics: &DefaultMetricsRecorder,
        database_path: &Path,
        shared_database_bytes: u64,
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
            requests,
            bytes_read,
            bytes_written,
            ..
        } = store_metrics.snapshot();
        TimeseriesSample {
            offset_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
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
            database_size_bytes: shared_database_bytes
                .saturating_add(store_metrics.prefix_bytes(database_path)),
            object_store_requests: requests,
            object_store_bytes_read: bytes_read,
            object_store_bytes_written: bytes_written,
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

fn finite_or_zero(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}
