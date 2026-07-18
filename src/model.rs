use crate::config::{CacheConfig, DatasetConfig, ResolvedConfig, Task, TaskConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub slate_version: String,
    pub slate_commit: String,
    pub runner_version: String,
    pub runner_commit: String,
    pub lockfile_sha256: String,
}

impl SourceIdentity {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointReference {
    pub database_path: String,
    pub checkpoint_id: String,
    pub manifest_id: u64,
    pub lsm_digest_sha256: String,
    pub live_sst_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoldenDatasetMetadata {
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub logical_bytes: u64,
    pub live_sst_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultConfiguration {
    pub scale: f64,
    pub dataset: DatasetConfig,
    pub caches: CacheConfig,
    pub task: TaskConfig,
    pub slate_settings: serde_json::Value,
    pub build_profile: String,
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
            build_profile: config.build_profile.clone(),
            enabled_features: config.enabled_features.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationResult {
    pub status: String,
    pub task: Task,
    pub golden_id: String,
    pub timestamp: String,
    pub source: SourceIdentity,
    #[serde(default)]
    pub environment: Environment,
    pub configuration: ResultConfiguration,
    pub source_checkpoint: Option<CheckpointReference>,
    pub checkpoint: CheckpointReference,
    pub dataset: GoldenDatasetMetadata,
    #[serde(default)]
    pub recorded_interval_ns: u64,
    #[serde(default)]
    pub application: ApplicationMetrics,
    #[serde(default)]
    pub object_store: ObjectStoreMetrics,
    #[serde(default)]
    pub process: ProcessStatistics,
    #[serde(default)]
    pub machine: MachineStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialState {
    pub kind: String,
    pub checkpoint_id: Option<String>,
    pub manifest_id: Option<u64>,
    pub lsm_digest_sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateSummary {
    pub total: u64,
    pub avg_per_second: f64,
    pub p50_per_second: f64,
    pub p95_per_second: f64,
    pub p99_per_second: f64,
    pub p999_per_second: f64,
    pub min_per_second: f64,
    pub max_per_second: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThroughputSummary {
    pub total_bytes: u64,
    pub avg_bytes_per_second: f64,
    pub p50_bytes_per_second: f64,
    pub p95_bytes_per_second: f64,
    pub p99_bytes_per_second: f64,
    pub p999_bytes_per_second: f64,
    pub min_bytes_per_second: f64,
    pub max_bytes_per_second: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    pub count: u64,
    pub avg_ns: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributionSummary {
    pub avg: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationMetrics {
    pub operations: BTreeMap<String, RateSummary>,
    pub throughput: BTreeMap<String, ThroughputSummary>,
    pub latency: BTreeMap<String, LatencySummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreMetrics {
    pub requests: BTreeMap<String, RateSummary>,
    pub throughput: BTreeMap<String, ThroughputSummary>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessStatistics {
    pub cpu_cores: DistributionSummary,
    pub rss_bytes: DistributionSummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineStatistics {
    pub cpu_percent: DistributionSummary,
    pub rss_bytes: DistributionSummary,
    pub network_receive_bytes_per_second: DistributionSummary,
    pub network_send_bytes_per_second: DistributionSummary,
    pub disk_read_bytes_per_second: DistributionSummary,
    pub disk_write_bytes_per_second: DistributionSummary,
    pub disk_read_operations_per_second: DistributionSummary,
    pub disk_write_operations_per_second: DistributionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadResult {
    pub status: String,
    pub task: Task,
    pub golden_id: String,
    pub session: String,
    pub timestamp: String,
    pub source: SourceIdentity,
    pub environment: Environment,
    pub configuration: ResultConfiguration,
    pub initial_state: InitialState,
    pub client_measurement_ns: u64,
    pub durability_drain_ns: u64,
    pub recorded_interval_ns: u64,
    pub application: ApplicationMetrics,
    pub object_store: ObjectStoreMetrics,
    pub process: ProcessStatistics,
    pub machine: MachineStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub status: String,
    pub golden_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub source: SourceIdentity,
    pub preparation_runner_commits: BTreeMap<String, String>,
    pub resolved_configuration: BTreeMap<String, ResultConfiguration>,
    pub max_parallel: usize,
    pub results: BTreeMap<String, String>,
}
