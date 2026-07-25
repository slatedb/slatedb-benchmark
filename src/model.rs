//! Serializable contracts for golden datasets, workload results, and run bundles.

use crate::config::{CacheConfig, DatasetConfig, ResolvedConfig, Task, TaskConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Source revisions that make a benchmark artifact reproducible.
pub struct SourceIdentity {
    /// SlateDB version label used to group published results.
    pub slate_version: String,
    /// Exact SlateDB Git commit compiled into the runner.
    pub slate_commit: String,
    /// Version of the benchmark runner crate.
    pub runner_version: String,
    /// Exact benchmark-runner Git commit.
    pub runner_commit: String,
    /// SHA-256 digest of the runner's `Cargo.lock`.
    pub lockfile_sha256: String,
}

impl SourceIdentity {
    /// Returns source information embedded by the build script.
    pub fn current() -> Self {
        Self {
            slate_version: env!("BENCHMARK_SLATE_VERSION").to_string(),
            slate_commit: env!("BENCHMARK_SLATE_COMMIT").to_string(),
            runner_version: env!("CARGO_PKG_VERSION").to_string(),
            runner_commit: env!("BENCHMARK_RUNNER_COMMIT").to_string(),
            lockfile_sha256: env!("BENCHMARK_LOCK_HASH").to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Hardware, operating system, and object-store context for a benchmark.
pub struct Environment {
    /// CI runner label or `local`.
    pub runner_type: String,
    /// Hostname reported by the operating system.
    pub hostname: String,
    /// CPU model string.
    pub cpu_model: String,
    /// Number of logical CPU cores visible to the process.
    pub cpu_cores: usize,
    /// Total host memory in bytes.
    pub ram_bytes: u64,
    /// Names and mount points of visible local disks.
    pub local_disk: String,
    /// Operating system name and version.
    pub os: String,
    /// Kernel version.
    pub kernel: String,
    /// Object-store provider name.
    pub object_store: String,
    /// Configured object-store endpoint or provider default.
    pub endpoint: String,
    /// Object-store region.
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Immutable checkpoint used as a golden workload starting point.
pub struct CheckpointReference {
    /// Object-store path of the checkpointed database.
    pub database_path: String,
    /// UUID of the detached SlateDB checkpoint.
    pub checkpoint_id: String,
    /// Manifest ID captured by the checkpoint.
    pub manifest_id: u64,
    /// SHA-256 digest of the checkpoint's logical LSM state.
    pub lsm_digest_sha256: String,
    /// Compressed bytes in physical SSTs referenced by the checkpoint.
    pub live_sst_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Size and record-shape metadata for a prepared golden dataset.
pub struct GoldenDatasetMetadata {
    /// Number of records loaded.
    pub record_count: u64,
    /// Encoded key size in bytes.
    pub key_bytes: usize,
    /// Generated value size in bytes.
    pub value_bytes: usize,
    /// Total logical bytes loaded before compression.
    pub logical_bytes: u64,
    /// Compressed bytes in live physical SSTs.
    pub live_sst_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Published configuration for one preparation phase or workload.
pub struct ResultConfiguration {
    /// Decimal scale factor applied to catalog defaults.
    pub scale: f64,
    /// Dataset shape used by the task.
    pub dataset: DatasetConfig,
    /// Benchmark-managed cache capacities.
    pub caches: CacheConfig,
    /// Workload behavior and duration.
    pub task: TaskConfig,
    /// Effective SlateDB settings serialized as JSON.
    pub slate_settings: serde_json::Value,
    /// SlateDB defaults used to identify explicit overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slate_default_settings: Option<serde_json::Value>,
    /// Rust build profile used for the runner.
    pub build_profile: String,
    /// SlateDB Cargo features enabled in the runner.
    pub enabled_features: Vec<String>,
}

impl From<&ResolvedConfig> for ResultConfiguration {
    fn from(config: &ResolvedConfig) -> Self {
        Self {
            scale: config.scale,
            dataset: config.dataset.clone(),
            caches: config.caches.clone(),
            task: config.task.clone(),
            slate_settings: config.slate_settings.clone(),
            slate_default_settings: Some(config.slate_default_settings.clone()),
            build_profile: config.build_profile.clone(),
            enabled_features: config.enabled_features.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Manifest for the compacted dataset shared by golden-backed workloads.
pub struct GoldenManifest {
    /// Completion status; valid manifests use `ok`.
    pub status: String,
    /// Stable golden-dataset identifier.
    pub golden_id: String,
    /// RFC 3339 completion timestamp.
    pub timestamp: String,
    /// Source revisions used to build the dataset.
    pub source: SourceIdentity,
    /// Environment in which preparation ran.
    pub environment: Environment,
    /// Effective compaction-phase configuration.
    pub configuration: ResultConfiguration,
    /// Detached checkpoint cloned by workloads.
    pub checkpoint: CheckpointReference,
    /// Prepared dataset size and record shape.
    pub dataset: GoldenDatasetMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Database state observed immediately before a workload starts.
pub struct InitialState {
    /// State kind, either `golden` or `empty`.
    pub kind: String,
    /// Golden checkpoint UUID, absent for an empty database.
    pub checkpoint_id: Option<String>,
    /// Golden manifest ID, absent for an empty database.
    pub manifest_id: Option<u64>,
    /// SHA-256 digest of the initial logical LSM state.
    pub lsm_digest_sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Total and per-window distribution for an operation count.
pub struct RateSummary {
    /// Calls recorded over the full measurement interval.
    pub total: u64,
    /// Full-interval average calls per second.
    pub avg_per_second: f64,
    /// 0.1st percentile of complete per-second windows.
    pub p001_per_second: f64,
    /// 1st percentile of complete per-second windows.
    pub p01_per_second: f64,
    /// Median of complete per-second windows.
    pub p50_per_second: f64,
    /// 99th percentile of complete per-second windows.
    pub p99_per_second: f64,
    /// 99.9th percentile of complete per-second windows.
    pub p999_per_second: f64,
    /// Minimum complete-window rate.
    pub min_per_second: f64,
    /// Maximum complete-window rate.
    pub max_per_second: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Total and per-window distribution for byte throughput.
pub struct ThroughputSummary {
    /// Logical or physical bytes recorded over the full interval.
    pub total_bytes: u64,
    /// Full-interval average bytes per second.
    pub avg_bytes_per_second: f64,
    /// 0.1st percentile of complete-window bytes per second.
    pub p001_bytes_per_second: f64,
    /// 1st percentile of complete-window bytes per second.
    pub p01_bytes_per_second: f64,
    /// Median complete-window bytes per second.
    pub p50_bytes_per_second: f64,
    /// 99th percentile of complete-window bytes per second.
    pub p99_bytes_per_second: f64,
    /// 99.9th percentile of complete-window bytes per second.
    pub p999_bytes_per_second: f64,
    /// Minimum complete-window bytes per second.
    pub min_bytes_per_second: f64,
    /// Maximum complete-window bytes per second.
    pub max_bytes_per_second: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// HDR-histogram summary of operation latency in nanoseconds.
pub struct LatencySummary {
    /// Number of latency observations.
    pub count: u64,
    /// Arithmetic mean latency in nanoseconds.
    pub avg_ns: f64,
    /// 0.1st-percentile latency in nanoseconds.
    pub p001_ns: u64,
    /// 1st-percentile latency in nanoseconds.
    pub p01_ns: u64,
    /// Median latency in nanoseconds.
    pub p50_ns: u64,
    /// 99th-percentile latency in nanoseconds.
    pub p99_ns: u64,
    /// 99.9th-percentile latency in nanoseconds.
    pub p999_ns: u64,
    /// Minimum observed latency in nanoseconds.
    pub min_ns: u64,
    /// Maximum observed latency in nanoseconds.
    pub max_ns: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Distribution summary for sampled floating-point measurements.
pub struct DistributionSummary {
    /// Arithmetic mean.
    pub avg: f64,
    /// 0.1st percentile.
    pub p001: f64,
    /// 1st percentile.
    pub p01: f64,
    /// Median.
    pub p50: f64,
    /// 99th percentile.
    pub p99: f64,
    /// 99.9th percentile.
    pub p999: f64,
    /// Minimum sample.
    pub min: f64,
    /// Maximum sample.
    pub max: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Application-level operation, throughput, and latency summaries.
pub struct ApplicationMetrics {
    /// Operation name to call-rate summary.
    pub operations: BTreeMap<String, RateSummary>,
    /// Operation name to logical-byte throughput summary.
    pub throughput: BTreeMap<String, ThroughputSummary>,
    /// Operation name to end-to-end latency summary.
    pub latency: BTreeMap<String, LatencySummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Physical object-store request and body-byte summaries.
pub struct ObjectStoreMetrics {
    /// HTTP method to request-rate summary.
    pub requests: BTreeMap<String, RateSummary>,
    /// HTTP method to response and request body throughput summary.
    pub throughput: BTreeMap<String, ThroughputSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Distribution of process-level resource samples.
pub struct ProcessStatistics {
    /// CPU cores consumed by the benchmark process.
    pub cpu_cores: DistributionSummary,
    /// Resident set size in bytes.
    pub rss_bytes: DistributionSummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Distribution of host-level resource and I/O samples.
pub struct MachineStatistics {
    /// Host CPU utilization as a percentage.
    pub cpu_percent: DistributionSummary,
    /// Host memory in use, in bytes.
    pub memory_used_bytes: DistributionSummary,
    /// Network bytes received per second.
    pub network_receive_bytes_per_second: DistributionSummary,
    /// Network bytes sent per second.
    pub network_send_bytes_per_second: DistributionSummary,
    /// Physical disk bytes read per second.
    pub disk_read_bytes_per_second: DistributionSummary,
    /// Physical disk bytes written per second.
    pub disk_write_bytes_per_second: DistributionSummary,
    /// Physical disk read operations per second.
    pub disk_read_operations_per_second: DistributionSummary,
    /// Physical disk write operations per second.
    pub disk_write_operations_per_second: DistributionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Content-addressed reference to a workload sidecar.
pub struct SeriesReference {
    /// Sidecar filename relative to `result.json`.
    pub file: String,
    /// Lowercase SHA-256 digest of the sidecar bytes.
    pub sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Sparse HDR-histogram buckets for one latency metric.
pub struct HistogramSeries {
    /// Inclusive bucket upper bounds in nanoseconds.
    pub upper_bound_ns: Vec<u64>,
    /// Observation counts corresponding to `upper_bound_ns`.
    pub counts: Vec<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Per-window latency summaries for one application operation.
///
/// A missing value means the operation had no observations in that window.
pub struct LatencyTimeSeries {
    /// Mean latency per window in nanoseconds.
    pub avg: Vec<Option<f64>>,
    /// 0.1st-percentile latency per window in nanoseconds.
    pub p001: Vec<Option<f64>>,
    /// 1st-percentile latency per window in nanoseconds.
    pub p01: Vec<Option<f64>>,
    /// Median latency per window in nanoseconds.
    pub p50: Vec<Option<f64>>,
    /// 99th-percentile latency per window in nanoseconds.
    pub p99: Vec<Option<f64>>,
    /// 99.9th-percentile latency per window in nanoseconds.
    pub p999: Vec<Option<f64>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Application metrics sampled over time.
pub struct ApplicationSeries {
    /// Operation name to calls per rate window.
    pub operations_per_second: BTreeMap<String, Vec<f64>>,
    /// Operation name to logical bytes per rate window.
    pub bytes_per_second: BTreeMap<String, Vec<f64>>,
    /// Operation name to latency values per latency window.
    pub latency_ns: BTreeMap<String, LatencyTimeSeries>,
    /// Operation name to full-interval sparse latency histogram.
    pub latency_histograms: BTreeMap<String, HistogramSeries>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Object-store metrics sampled over time.
pub struct ObjectStoreSeries {
    /// HTTP method to requests per rate window.
    pub requests_per_second: BTreeMap<String, Vec<f64>>,
    /// HTTP method to body bytes per rate window.
    pub bytes_per_second: BTreeMap<String, Vec<f64>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Process resource samples over time.
pub struct ProcessSeries {
    /// CPU cores consumed in each resource window.
    pub cpu_cores: Vec<f64>,
    /// Resident set size in bytes in each resource window.
    pub rss_bytes: Vec<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Host resource and I/O samples over time.
pub struct MachineSeries {
    /// Host CPU utilization percentage per resource window.
    pub cpu_percent: Vec<f64>,
    /// Host memory in use, in bytes, per resource window.
    pub memory_used_bytes: Vec<f64>,
    /// Network receive rate per resource window.
    pub network_receive_bytes_per_second: Vec<f64>,
    /// Network send rate per resource window.
    pub network_send_bytes_per_second: Vec<f64>,
    /// Physical disk read-byte rate per resource window.
    pub disk_read_bytes_per_second: Vec<f64>,
    /// Physical disk write-byte rate per resource window.
    pub disk_write_bytes_per_second: Vec<f64>,
    /// Physical disk read-operation rate per resource window.
    pub disk_read_operations_per_second: Vec<f64>,
    /// Physical disk write-operation rate per resource window.
    pub disk_write_operations_per_second: Vec<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Time-series sidecar associated with one workload result.
pub struct WorkloadSeries {
    /// End of each operation and object-store rate window, relative to sampling start.
    pub rate_elapsed_ns: Vec<u64>,
    /// Duration represented by each rate window.
    pub rate_duration_ns: Vec<u64>,
    /// End of each application-latency window, relative to sampling start.
    pub latency_elapsed_ns: Vec<u64>,
    /// Duration represented by each latency window.
    pub latency_duration_ns: Vec<u64>,
    /// End of each process and machine window, relative to sampling start.
    pub resource_elapsed_ns: Vec<u64>,
    /// Duration represented by each resource window.
    pub resource_duration_ns: Vec<u64>,
    /// Application time-series values.
    pub application: ApplicationSeries,
    /// Object-store time-series values.
    pub object_store: ObjectStoreSeries,
    /// Process time-series values.
    pub process: ProcessSeries,
    /// Host time-series values.
    pub machine: MachineSeries,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Summarized result and provenance for one benchmark workload.
pub struct WorkloadResult {
    /// Completion status; valid results use `ok`.
    pub status: String,
    /// Workload represented by this artifact.
    pub task: Task,
    /// Golden dataset identifier supplied to the run.
    pub golden_id: String,
    /// Run identifier shared by sibling workloads.
    pub session: String,
    /// RFC 3339 completion timestamp.
    pub timestamp: String,
    /// GitHub Actions job log URL, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions_log_url: Option<String>,
    /// Source revisions used to build the runner.
    pub source: SourceIdentity,
    /// Execution environment.
    pub environment: Environment,
    /// Effective workload and SlateDB configuration.
    pub configuration: ResultConfiguration,
    /// Database state observed before warmup and measurement.
    pub initial_state: InitialState,
    /// Wall-clock time spent running measured clients.
    pub client_measurement_ns: u64,
    /// Time after clients stopped until all accepted writes became durable.
    pub durability_drain_ns: u64,
    /// Total interval covered by summary metrics.
    pub recorded_interval_ns: u64,
    /// Application-level summary metrics.
    pub application: ApplicationMetrics,
    /// Physical object-store summary metrics.
    pub object_store: ObjectStoreMetrics,
    /// Process summary metrics.
    pub process: ProcessStatistics,
    /// Host summary metrics.
    pub machine: MachineStatistics,
    /// Reference to the workload time-series sidecar.
    pub series: SeriesReference,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// SlateDB patch applied before compiling a benchmark runner.
pub struct AppliedPatch {
    /// Patch filename, which also determines application order.
    pub name: String,
    /// Lowercase SHA-256 digest of the patch contents.
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Manifest for one versioned collection of workload artifacts.
pub struct RunManifest {
    /// Completion status; valid manifests use `ok`.
    pub status: String,
    /// Run identifier, normally derived from the GitHub Actions run.
    pub run_id: String,
    /// Golden dataset identifier shared by the workloads.
    pub golden_id: String,
    /// RFC 3339 timestamp captured when execution began.
    pub started_at: String,
    /// RFC 3339 timestamp captured when bundling completed.
    pub finished_at: String,
    /// Ordered SlateDB patches applied to the runner.
    pub patches: Vec<AppliedPatch>,
    /// Source identity shared by every workload.
    pub source: SourceIdentity,
    /// Runner commit used to build the golden checkpoint.
    pub golden_runner_commit: String,
    /// Task name to effective published configuration.
    pub resolved_configuration: BTreeMap<String, ResultConfiguration>,
    /// Maximum workload parallelism represented by the bundle.
    pub max_parallel: usize,
    /// Relative artifact path to lowercase SHA-256 digest.
    pub results: BTreeMap<String, String>,
}
