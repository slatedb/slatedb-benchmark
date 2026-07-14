use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub schema_version: u32,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
    pub mode: String,
    pub slate_version: String,
    pub slate_commit: String,
    pub runner_version: String,
    pub runner_commit: String,
    pub lockfile_sha256: String,
    pub resolved_configuration: Value,
    pub object_store_baseline: ObjectStoreBaseline,
    pub results: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultRecord {
    pub schema_version: u32,
    pub identity: Identity,
    pub environment: Environment,
    pub object_store_baseline: ObjectStoreBaseline,
    pub configuration: BenchmarkConfiguration,
    pub application: ApplicationPerformance,
    pub durability: DurabilityPerformance,
    pub resources: ResourceUse,
    pub storage: StoragePerformance,
    pub cost: CostEstimate,
    pub initial_state: InitialState,
    pub source_files: SourceFiles,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub slate_version: String,
    pub slate_commit: String,
    pub runner_version: String,
    pub runner_commit: String,
    pub lockfile_sha256: String,
    pub timestamp: String,
    pub profile: String,
    pub workload: String,
    pub variant: String,
    pub mode: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Environment {
    pub runner_type: String,
    pub hostname: String,
    pub cpu_model: String,
    pub cpu_cores: usize,
    pub ram_bytes: u64,
    pub local_disk: String,
    pub os: String,
    pub kernel: String,
    pub object_store: String,
    pub endpoint: String,
    pub region: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectStoreBaseline {
    pub measured_at: String,
    pub put_latency: LatencySummary,
    pub get_latency: LatencySummary,
    pub upload_mib_per_second: f64,
    pub download_mib_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfiguration {
    pub clients: Option<usize>,
    pub target_rate: Option<u64>,
    pub warmup_ns: u64,
    pub measurement_ns: u64,
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub block_cache_bytes: Option<u64>,
    pub metadata_cache_bytes: Option<u64>,
    pub sst_block_bytes: Option<usize>,
    pub slate_settings: Value,
    pub build_profile: String,
    pub enabled_features: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApplicationPerformance {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub accepted_ops_per_second: f64,
    pub completed_ops_per_second: f64,
    pub offered_ops_per_second: Option<f64>,
    pub dropped_operations: Option<u64>,
    pub dropped_ops_per_second: Option<f64>,
    pub payload_mib_per_second: f64,
    pub errors: u64,
    pub return_latency: LatencySummary,
    pub return_latency_by_operation: BTreeMap<String, LatencySummary>,
    pub response_latency: Option<LatencySummary>,
    pub scheduling_delay: Option<LatencySummary>,
    pub batch_latency: Option<LatencySummary>,
    pub key_throughput_per_second: Option<f64>,
    pub transaction_commits: Option<u64>,
    pub transaction_aborts: Option<u64>,
    pub transaction_conflicts: Option<u64>,
    pub transaction_commit_rate: Option<f64>,
    pub transaction_abort_rate: Option<f64>,
    pub transaction_conflict_rate: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DurabilityPerformance {
    pub lag: Option<LatencySummary>,
    pub final_flush_drain_ns: Option<u64>,
    pub durable_ops_per_second: Option<f64>,
    pub last_measured_sequence: Option<u64>,
    pub final_durable_sequence: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUse {
    pub average_cpu_percent: f64,
    pub peak_cpu_percent: f64,
    pub peak_rss_bytes: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
    pub disk_bytes_read: u64,
    pub disk_bytes_written: u64,
    pub disk_read_operations: u64,
    pub disk_write_operations: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoragePerformance {
    pub database_size_bytes: u64,
    pub object_store_requests: BTreeMap<String, u64>,
    pub object_store_errors: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub compaction_throughput_bytes_per_second: Option<f64>,
    pub write_amplification: Option<f64>,
    pub backpressure_ns: u64,
    pub compaction_backlog_bytes: Option<u64>,
    pub five_minute_windows: Vec<IngestWindow>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestWindow {
    pub start_offset_ns: u64,
    pub operations: u64,
    pub ops_per_second: f64,
    pub compaction_backlog_bytes: Option<u64>,
    pub write_amplification: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CostEstimate {
    pub price_table_revision: String,
    pub currency: String,
    pub compute: f64,
    pub requests: f64,
    pub storage: f64,
    pub transfer: f64,
    pub total: f64,
    pub compute_per_million_operations: Option<f64>,
    pub requests_per_million_operations: Option<f64>,
    pub storage_per_million_operations: Option<f64>,
    pub transfer_per_million_operations: Option<f64>,
    pub total_per_million_operations: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InitialState {
    pub checkpoint_id: Option<String>,
    pub manifest_id: Option<u64>,
    pub lsm_digest_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFiles {
    pub histograms: String,
    pub timeseries: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub count: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramsFile {
    pub schema_version: u32,
    pub encoding: String,
    pub significant_digits: u8,
    pub histograms: BTreeMap<String, EncodedHistogram>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodedHistogram {
    pub unit: String,
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesFile {
    pub schema_version: u32,
    pub interval_ns: u64,
    pub samples: Vec<TimeseriesSample>,
    pub slatedb_metrics: Vec<MetricSeries>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeseriesSample {
    pub offset_ns: u64,
    pub operations: u64,
    pub errors: u64,
    pub cpu_percent: f64,
    pub rss_bytes: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
    pub disk_bytes_read: u64,
    pub disk_bytes_written: u64,
    pub disk_read_operations: u64,
    pub disk_write_operations: u64,
    pub database_size_bytes: u64,
    pub object_store_requests: BTreeMap<String, u64>,
    pub object_store_bytes_read: u64,
    pub object_store_bytes_written: u64,
    #[serde(skip)]
    pub slatedb_metrics: Vec<MetricSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSeries {
    pub name: String,
    pub description: String,
    pub labels: BTreeMap<String, String>,
    pub value_type: MetricValueType,
    pub boundaries: Option<Vec<f64>>,
    pub values: Vec<Option<MetricSeriesValue>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricValueType {
    Counter,
    Gauge,
    UpDownCounter,
    Histogram,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetricSeriesValue {
    Scalar(serde_json::Number),
    Histogram(MetricHistogramValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricHistogramValue {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSnapshot {
    pub name: String,
    pub description: String,
    pub labels: BTreeMap<String, String>,
    pub value: MetricValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum MetricValue {
    Counter(u64),
    Gauge(i64),
    UpDownCounter(i64),
    Histogram {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        boundaries: Vec<f64>,
        bucket_counts: Vec<u64>,
    },
}
