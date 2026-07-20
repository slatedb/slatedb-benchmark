use anyhow::{ensure, Context, Result};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use slatedb::config::Settings;
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

const RECORD_COUNT: u64 = 300_000_000;
const KEY_BYTES: usize = 20;
const VALUE_BYTES: usize = 400;
const CLIENTS: usize = 64;
const WARMUP_MS: u64 = 5 * 60 * 1_000;
const MEASUREMENT_MS: u64 = 15 * 60 * 1_000;
const IDLE_MS: u64 = 5 * 60 * 1_000;
const INGEST_MS: u64 = 20 * 60 * 1_000;
const BLOCK_CACHE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const METADATA_CACHE_BYTES: u64 = 512 * 1024 * 1024;
const OBJECT_STORE_CACHE_BYTES: u64 = 40 * 1024 * 1024 * 1024;
const MIN_DURATION_MS: u64 = 2_000;
const MIN_BLOCK_CACHE_BYTES: u64 = 8 * 1024 * 1024;
const MIN_METADATA_CACHE_BYTES: u64 = 2 * 1024 * 1024;
const MIN_OBJECT_STORE_CACHE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BenchmarkScale(f64);

impl BenchmarkScale {
    pub const FULL: Self = Self(1.0);

    pub fn factor(self) -> f64 {
        self.0
    }

    pub fn is_full(self) -> bool {
        self.0.to_bits() == Self::FULL.0.to_bits()
    }

    fn validate(value: f64) -> std::result::Result<Self, String> {
        if value.is_finite() && value > 0.0 && value <= 1.0 {
            Ok(Self(value))
        } else {
            Err("scale must be greater than 0 and at most 1.0".to_string())
        }
    }
}

impl Default for BenchmarkScale {
    fn default() -> Self {
        Self::FULL
    }
}

impl std::fmt::Display for BenchmarkScale {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for BenchmarkScale {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if value.ends_with('%') {
            return Err("scale must be a decimal factor such as 1.0 or 0.01".to_string());
        }
        let value = value
            .parse::<f64>()
            .map_err(|error| format!("invalid scale {value:?}: {error}"))?;
        Self::validate(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[clap(rename_all = "kebab-case")]
pub enum Task {
    BulkLoad,
    Compaction,
    Idle,
    PointReadUniform,
    PointReadSkewed,
    PointReadMissing,
    ReadHeavy,
    Balanced,
    UpdateHeavy,
    RangeScan,
    SustainedIngest,
    TransactionContention,
}

impl Task {
    pub const WORKLOADS: [Self; 10] = [
        Self::Idle,
        Self::PointReadUniform,
        Self::PointReadSkewed,
        Self::PointReadMissing,
        Self::ReadHeavy,
        Self::Balanced,
        Self::UpdateHeavy,
        Self::RangeScan,
        Self::SustainedIngest,
        Self::TransactionContention,
    ];

    pub const fn is_preparation(self) -> bool {
        matches!(self, Self::BulkLoad | Self::Compaction)
    }

    pub const fn uses_golden(self) -> bool {
        !matches!(
            self,
            Self::BulkLoad | Self::Compaction | Self::SustainedIngest
        )
    }

    pub const fn may_write(self) -> bool {
        matches!(
            self,
            Self::BulkLoad
                | Self::ReadHeavy
                | Self::Balanced
                | Self::UpdateHeavy
                | Self::SustainedIngest
                | Self::TransactionContention
        )
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BulkLoad => "bulk-load",
            Self::Compaction => "compaction",
            Self::Idle => "idle",
            Self::PointReadUniform => "point-read-uniform",
            Self::PointReadSkewed => "point-read-skewed",
            Self::PointReadMissing => "point-read-missing",
            Self::ReadHeavy => "read-heavy",
            Self::Balanced => "balanced",
            Self::UpdateHeavy => "update-heavy",
            Self::RangeScan => "range-scan",
            Self::SustainedIngest => "sustained-ingest",
            Self::TransactionContention => "transaction-contention",
        }
    }
}

impl std::fmt::Display for Task {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetConfig {
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub value_compression_ratio: f64,
}

impl DatasetConfig {
    pub fn logical_bytes(&self) -> u64 {
        self.record_count.saturating_mul(
            u64::try_from(self.key_bytes.saturating_add(self.value_bytes)).unwrap_or(u64::MAX),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    pub block_bytes: u64,
    pub metadata_bytes: u64,
    pub object_store_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskConfig {
    pub task: Task,
    pub clients: usize,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub initial_state: String,
    pub key_selection: String,
    pub operation_mix: BTreeMap<String, f64>,
    pub scan_limit: Option<usize>,
    pub transaction_hot_keys: Option<u64>,
    pub transaction_reads: Option<usize>,
    pub transaction_updates: Option<usize>,
}

impl TaskConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        let active = !self.task.is_preparation() && self.task != Task::Idle;
        ensure!(
            matches!(
                self.key_selection.as_str(),
                "none"
                    | "uniform"
                    | "scrambled-zipfian-0.99"
                    | "uniform-absent"
                    | "unique-sequential"
                    | "uniform-hot-set"
            ),
            "unknown key selection {}",
            self.key_selection
        );
        ensure!(
            active != self.operation_mix.is_empty(),
            "active workloads must define an operation mix"
        );
        ensure!(
            active != (self.key_selection == "none"),
            "active workloads must define key selection"
        );

        let mut total = 0.0;
        for (operation, fraction) in &self.operation_mix {
            ensure!(
                matches!(operation.as_str(), "get" | "put" | "scan" | "transaction"),
                "unknown workload operation {operation}"
            );
            ensure!(
                fraction.is_finite() && *fraction > 0.0,
                "workload operation {operation} has an invalid fraction"
            );
            total += fraction;
        }
        if active {
            ensure!(
                (total - 1.0).abs() <= 1e-9,
                "workload operation mix sums to {total}, not 1"
            );
            let writes = self.operation_mix.contains_key("put")
                || self.operation_mix.contains_key("transaction");
            ensure!(
                writes == self.task.may_write(),
                "operation mix write behavior disagrees with task {}",
                self.task
            );
        }

        let transactions = self.operation_mix.contains_key("transaction");
        if transactions {
            let hot_keys = self
                .transaction_hot_keys
                .context("transaction workload has no hot-key count")?;
            let reads = self
                .transaction_reads
                .context("transaction workload has no read count")?;
            let updates = self
                .transaction_updates
                .context("transaction workload has no update count")?;
            ensure!(hot_keys > 0, "transaction hot-key count is zero");
            let operation_count = reads
                .checked_add(updates)
                .context("transaction operation count overflows")?;
            ensure!(operation_count > 0, "transaction has no operations");
            ensure!(
                operation_count <= 10_000,
                "transaction has too many operations"
            );
            ensure!(updates > 0, "transaction must contain an update");
            ensure!(
                self.key_selection == "uniform-hot-set",
                "transaction workload must select from its hot set"
            );
        } else {
            ensure!(
                self.transaction_hot_keys.is_none()
                    && self.transaction_reads.is_none()
                    && self.transaction_updates.is_none(),
                "non-transaction workload has transaction settings"
            );
        }

        if self.operation_mix.contains_key("scan") {
            ensure!(
                self.scan_limit.is_some_and(|limit| limit > 0),
                "scan workload has no positive limit"
            );
        } else {
            ensure!(
                self.scan_limit.is_none(),
                "non-scan workload has a scan limit"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub scale: f64,
    pub dataset: DatasetConfig,
    pub caches: CacheConfig,
    pub task: TaskConfig,
    pub slate_settings: serde_json::Value,
    pub build_profile: String,
    pub enabled_features: Vec<String>,
    pub settings: Settings,
}

pub fn load(task: Task, scale: BenchmarkScale, settings_path: &Path) -> Result<ResolvedConfig> {
    let mut settings = Settings::from_file(settings_path)
        .with_context(|| format!("loading SlateDB settings from {}", settings_path.display()))?;
    let dataset = DatasetConfig {
        record_count: scaled_u64(RECORD_COUNT, 1, scale),
        key_bytes: KEY_BYTES,
        value_bytes: VALUE_BYTES,
        value_compression_ratio: 1.0,
    };
    let caches = CacheConfig {
        block_bytes: scaled_u64(BLOCK_CACHE_BYTES, MIN_BLOCK_CACHE_BYTES, scale),
        metadata_bytes: scaled_u64(METADATA_CACHE_BYTES, MIN_METADATA_CACHE_BYTES, scale),
        object_store_bytes: scaled_u64(
            OBJECT_STORE_CACHE_BYTES,
            MIN_OBJECT_STORE_CACHE_BYTES,
            scale,
        ),
    };
    settings.object_store_cache_options.max_cache_size_bytes = Some(
        usize::try_from(caches.object_store_bytes)
            .context("object-store cache capacity exceeds the platform limit")?,
    );
    settings.object_store_cache_options.root_folder = None;
    settings
        .object_store_cache_options
        .preload_disk_cache_on_startup = None;
    let task_config = task_config(task, scale, dataset.record_count);
    task_config
        .validate()
        .context("validating task configuration")?;
    Ok(ResolvedConfig {
        scale: scale.factor(),
        dataset,
        caches,
        task: task_config,
        slate_settings: serde_json::to_value(&settings)
            .context("serializing resolved SlateDB settings")?,
        build_profile: if cfg!(debug_assertions) {
            "debug".to_string()
        } else {
            "release".to_string()
        },
        enabled_features: env!("BENCHMARK_ENABLED_FEATURES")
            .split(',')
            .filter(|feature| !feature.is_empty())
            .map(str::to_string)
            .collect(),
        settings,
    })
}

fn task_config(task: Task, scale: BenchmarkScale, record_count: u64) -> TaskConfig {
    let mut config = TaskConfig {
        task,
        clients: if task.is_preparation() || task == Task::Idle {
            0
        } else {
            CLIENTS
        },
        warmup_ms: if matches!(task, Task::Idle | Task::SustainedIngest) || task.is_preparation() {
            0
        } else {
            scaled_u64(WARMUP_MS, MIN_DURATION_MS, scale)
        },
        measurement_ms: match task {
            Task::BulkLoad | Task::Compaction => 0,
            Task::Idle => scaled_u64(IDLE_MS, MIN_DURATION_MS, scale),
            Task::SustainedIngest => scaled_u64(INGEST_MS, MIN_DURATION_MS, scale),
            _ => scaled_u64(MEASUREMENT_MS, MIN_DURATION_MS, scale),
        },
        initial_state: if task == Task::SustainedIngest {
            "empty".to_string()
        } else if task.is_preparation() {
            "preparation".to_string()
        } else {
            "golden".to_string()
        },
        key_selection: "none".to_string(),
        operation_mix: BTreeMap::new(),
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
    };
    match task {
        Task::PointReadUniform => {
            config.key_selection = "uniform".to_string();
            config.operation_mix.insert("get".to_string(), 1.0);
        }
        Task::PointReadSkewed => {
            config.key_selection = "scrambled-zipfian-0.99".to_string();
            config.operation_mix.insert("get".to_string(), 1.0);
        }
        Task::PointReadMissing => {
            config.key_selection = "uniform-absent".to_string();
            config.operation_mix.insert("get".to_string(), 1.0);
        }
        Task::ReadHeavy => mixed(&mut config, 0.95, 0.05),
        Task::Balanced => mixed(&mut config, 0.5, 0.5),
        Task::UpdateHeavy => mixed(&mut config, 0.05, 0.95),
        Task::RangeScan => {
            config.key_selection = "uniform".to_string();
            config.operation_mix.insert("scan".to_string(), 1.0);
            config.scan_limit = Some(10);
        }
        Task::SustainedIngest => {
            config.key_selection = "unique-sequential".to_string();
            config.operation_mix.insert("put".to_string(), 1.0);
        }
        Task::TransactionContention => {
            config.key_selection = "uniform-hot-set".to_string();
            config.operation_mix.insert("transaction".to_string(), 1.0);
            config.transaction_hot_keys = Some(record_count.min(10_000));
            config.transaction_reads = Some(5);
            config.transaction_updates = Some(5);
        }
        Task::BulkLoad | Task::Compaction | Task::Idle => {}
    }
    config
}

fn mixed(config: &mut TaskConfig, reads: f64, updates: f64) {
    config.key_selection = "scrambled-zipfian-0.99".to_string();
    config.operation_mix.insert("get".to_string(), reads);
    config.operation_mix.insert("put".to_string(), updates);
}

fn scaled_u64(value: u64, minimum: u64, scale: BenchmarkScale) -> u64 {
    if value == 0 {
        return 0;
    }
    ((value as f64 * scale.factor()).round() as u64)
        .max(minimum.min(value))
        .min(value)
}

#[cfg(test)]
mod tests {
    use super::{load, BenchmarkScale, Task};
    use std::path::Path;

    #[test]
    fn published_configuration_matches_the_specification() {
        let config = load(
            Task::Balanced,
            BenchmarkScale::FULL,
            Path::new("config/settings.toml"),
        )
        .expect("published config");

        assert_eq!(config.dataset.record_count, 300_000_000);
        assert_eq!(config.dataset.key_bytes, 20);
        assert_eq!(config.dataset.value_bytes, 400);
        assert_eq!(config.task.clients, 64);
        assert_eq!(config.task.warmup_ms, 300_000);
        assert_eq!(config.task.measurement_ms, 900_000);
        assert_eq!(config.task.operation_mix["get"], 0.5);
        assert_eq!(config.task.operation_mix["put"], 0.5);
    }

    #[test]
    fn decimal_scale_reduces_cost_but_preserves_clients_and_record_sizes() {
        let scale = "0.01".parse::<BenchmarkScale>().expect("scale");
        let config = load(
            Task::SustainedIngest,
            scale,
            Path::new("config/settings.toml"),
        )
        .expect("scaled config");

        assert_eq!(config.dataset.record_count, 3_000_000);
        assert_eq!(config.dataset.key_bytes, 20);
        assert_eq!(config.dataset.value_bytes, 400);
        assert_eq!(config.task.clients, 64);
        assert_eq!(config.task.measurement_ms, 12_000);
        assert_eq!(
            config
                .settings
                .object_store_cache_options
                .max_cache_size_bytes,
            Some(429_496_730)
        );
        let cache = &config.slate_settings["object_store_cache_options"];
        assert_eq!(
            cache
                .get("cache_on_flush")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            cache
                .get("cache_on_compaction")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(config
            .settings
            .object_store_cache_options
            .preload_disk_cache_on_startup
            .is_none());
        assert!(!scale.is_full());
        assert!("1%".parse::<BenchmarkScale>().is_err());
    }

    #[test]
    fn workload_catalog_matches_the_specification() {
        assert_eq!(Task::WORKLOADS.len(), 10);
        assert_eq!(Task::WORKLOADS[0], Task::Idle);
        assert_eq!(Task::WORKLOADS[9], Task::TransactionContention);
    }
}
