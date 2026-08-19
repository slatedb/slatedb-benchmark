//! Workload catalog and layered benchmark configuration resolution.

use anyhow::{ensure, Context, Result};
use clap::ValueEnum;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slatedb::config::Settings;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use ts_rs::TS;

const RECORD_COUNT: u64 = 300_000_000;
const KEY_BYTES: usize = 20;
const VALUE_BYTES: usize = 400;
const CLIENTS: usize = 64;
const WARMUP_MS: u64 = 5 * 60 * 1_000;
const MEASUREMENT_MS: u64 = 15 * 60 * 1_000;
const IDLE_MS: u64 = 5 * 60 * 1_000;
const INGEST_MS: u64 = 20 * 60 * 1_000;
const BLOCK_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const METADATA_CACHE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MIN_DURATION_MS: u64 = 2_000;
const MIN_BLOCK_CACHE_BYTES: u64 = 8 * 1024 * 1024;
const MIN_METADATA_CACHE_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
/// Fraction used to shrink benchmark data and durations for local execution.
///
/// Valid factors are greater than zero and at most one. Scaling preserves
/// minimum cache sizes and durations so small runs still exercise real code
/// paths.
pub struct BenchmarkScale(f64);

impl BenchmarkScale {
    /// An unscaled benchmark run.
    pub const FULL: Self = Self(1.0);

    /// Returns the validated decimal scale factor.
    pub fn factor(self) -> f64 {
        self.0
    }

    /// Returns whether this value represents a full-scale run.
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

macro_rules! define_tasks {
    (
        $(
            $variant:ident {
                name: $name:expr,
                preparation: $preparation:expr,
                clients: $clients:expr,
                warmup_ms: $warmup_ms:expr,
                measurement_ms: $measurement_ms:expr,
                initial_state: $initial_state:expr,
                key_selection: $key_selection:expr,
                operation_mix: $operation_mix:expr,
                scan_limit: $scan_limit:expr,
                transaction_hot_keys: $transaction_hot_keys:expr,
                transaction_reads: $transaction_reads:expr,
                transaction_updates: $transaction_updates:expr,
                may_write: $may_write:expr,
            }
        ),+ $(,)?
    ) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Serialize,
            Deserialize,
            JsonSchema,
            TS,
            ValueEnum,
        )]
        #[repr(usize)]
        #[serde(rename_all = "kebab-case")]
        #[clap(rename_all = "kebab-case")]
        #[doc = "A preparation phase or workload from the benchmark catalog."]
        pub enum Task {
            $(
                #[doc = concat!("The `", $name, "` benchmark task.")]
                $variant
            ),+
        }

        const TASK_CATALOG: &[TaskDefinition] = &[
            $(
                TaskDefinition {
                    task: Task::$variant,
                    name: $name,
                    preparation: $preparation,
                    clients: $clients,
                    warmup_ms: $warmup_ms,
                    measurement_ms: $measurement_ms,
                    initial_state: $initial_state,
                    key_selection: $key_selection,
                    operation_mix: $operation_mix,
                    scan_limit: $scan_limit,
                    transaction_hot_keys: $transaction_hot_keys,
                    transaction_reads: $transaction_reads,
                    transaction_updates: $transaction_updates,
                    may_write: $may_write,
                }
            ),+
        ];
    };
}

impl Task {
    /// Iterates over executable workloads in catalog order.
    ///
    /// Preparation phases are excluded.
    pub fn workloads() -> impl Iterator<Item = Self> {
        TASK_CATALOG
            .iter()
            .filter(|definition| !definition.preparation)
            .map(|definition| definition.task)
    }

    /// Returns whether this task creates or compacts a golden dataset.
    pub fn is_preparation(self) -> bool {
        self.definition().preparation
    }

    /// Returns whether this task starts from the golden checkpoint.
    pub fn uses_golden(self) -> bool {
        self.definition().initial_state == "golden"
    }

    /// Returns whether this task may modify the database.
    pub fn may_write(self) -> bool {
        self.definition().may_write
    }

    /// Returns the stable kebab-case artifact and CLI name.
    pub fn as_str(self) -> &'static str {
        self.definition().name
    }

    fn definition(self) -> &'static TaskDefinition {
        &TASK_CATALOG[self as usize]
    }
}

impl std::fmt::Display for Task {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy)]
struct TaskDefinition {
    task: Task,
    name: &'static str,
    preparation: bool,
    clients: usize,
    warmup_ms: u64,
    measurement_ms: u64,
    initial_state: &'static str,
    key_selection: &'static str,
    operation_mix: &'static [(&'static str, f64)],
    scan_limit: Option<usize>,
    transaction_hot_keys: Option<u64>,
    transaction_reads: Option<usize>,
    transaction_updates: Option<usize>,
    may_write: bool,
}

const NO_OPERATIONS: &[(&str, f64)] = &[];
const GET_ONLY: &[(&str, f64)] = &[("get", 1.0)];
const READ_HEAVY: &[(&str, f64)] = &[("get", 0.95), ("put", 0.05)];
const BALANCED: &[(&str, f64)] = &[("get", 0.5), ("put", 0.5)];
const UPDATE_HEAVY: &[(&str, f64)] = &[("get", 0.05), ("put", 0.95)];
const SCAN_ONLY: &[(&str, f64)] = &[("scan", 1.0)];
const PUT_ONLY: &[(&str, f64)] = &[("put", 1.0)];
const TRANSACTION_ONLY: &[(&str, f64)] = &[("transaction", 1.0)];

define_tasks! {
    BulkLoad {
        name: "bulk-load",
        preparation: true,
        clients: 0,
        warmup_ms: 0,
        measurement_ms: 0,
        initial_state: "preparation",
        key_selection: "none",
        operation_mix: NO_OPERATIONS,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: true,
    },
    Compaction {
        name: "compaction",
        preparation: true,
        clients: 0,
        warmup_ms: 0,
        measurement_ms: 0,
        initial_state: "preparation",
        key_selection: "none",
        operation_mix: NO_OPERATIONS,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: false,
    },
    Idle {
        name: "idle",
        preparation: false,
        clients: 0,
        warmup_ms: 0,
        measurement_ms: IDLE_MS,
        initial_state: "golden",
        key_selection: "none",
        operation_mix: NO_OPERATIONS,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: false,
    },
    PointReadUniform {
        name: "point-read-uniform",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "uniform",
        operation_mix: GET_ONLY,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: false,
    },
    PointReadSkewed {
        name: "point-read-skewed",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "scrambled-zipfian-0.99",
        operation_mix: GET_ONLY,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: false,
    },
    PointReadMissing {
        name: "point-read-missing",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "uniform-absent",
        operation_mix: GET_ONLY,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: false,
    },
    ReadHeavy {
        name: "read-heavy",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "scrambled-zipfian-0.99",
        operation_mix: READ_HEAVY,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: true,
    },
    Balanced {
        name: "balanced",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "scrambled-zipfian-0.99",
        operation_mix: BALANCED,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: true,
    },
    UpdateHeavy {
        name: "update-heavy",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "scrambled-zipfian-0.99",
        operation_mix: UPDATE_HEAVY,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: true,
    },
    RangeScan {
        name: "range-scan",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "uniform",
        operation_mix: SCAN_ONLY,
        scan_limit: Some(10),
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: false,
    },
    SustainedIngest {
        name: "sustained-ingest",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: 0,
        measurement_ms: INGEST_MS,
        initial_state: "empty",
        key_selection: "unique-sequential",
        operation_mix: PUT_ONLY,
        scan_limit: None,
        transaction_hot_keys: None,
        transaction_reads: None,
        transaction_updates: None,
        may_write: true,
    },
    TransactionContention {
        name: "transaction-contention",
        preparation: false,
        clients: CLIENTS,
        warmup_ms: WARMUP_MS,
        measurement_ms: MEASUREMENT_MS,
        initial_state: "golden",
        key_selection: "uniform-hot-set",
        operation_mix: TRANSACTION_ONLY,
        scan_limit: None,
        transaction_hot_keys: Some(10_000),
        transaction_reads: Some(5),
        transaction_updates: Some(5),
        may_write: true,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Shape and logical size of records used by a benchmark task.
pub struct DatasetConfig {
    /// Number of records in the key-selection domain.
    pub record_count: u64,
    /// Encoded key size in bytes.
    pub key_bytes: usize,
    /// Generated value size in bytes.
    pub value_bytes: usize,
    /// Target ratio of uncompressed to compressed value bytes.
    pub value_compression_ratio: f64,
}

impl DatasetConfig {
    /// Returns the total logical bytes represented by the configured records.
    pub fn logical_bytes(&self) -> u64 {
        self.record_count.saturating_mul(
            u64::try_from(self.key_bytes.saturating_add(self.value_bytes)).unwrap_or(u64::MAX),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// In-memory cache capacities used by the benchmark runner.
pub struct CacheConfig {
    /// Block-cache capacity in bytes.
    pub block_bytes: u64,
    /// Metadata-cache capacity in bytes.
    pub metadata_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
/// Runtime shape of one preparation phase or workload.
pub struct TaskConfig {
    /// Catalog task represented by this configuration.
    pub task: Task,
    /// Number of concurrent closed-loop clients.
    pub clients: usize,
    /// Warmup duration in milliseconds.
    pub warmup_ms: u64,
    /// Measured client duration in milliseconds.
    pub measurement_ms: u64,
    /// Initial database state: `empty`, `golden`, or `none`.
    pub initial_state: String,
    /// Key-selection strategy used by workload operations.
    pub key_selection: String,
    /// Operation name to probability map; active workloads sum to one.
    pub operation_mix: BTreeMap<String, f64>,
    /// Maximum number of records returned by each scan.
    pub scan_limit: Option<usize>,
    /// Number of keys in the transaction contention hot set.
    pub transaction_hot_keys: Option<u64>,
    /// Reads performed by each transaction attempt.
    pub transaction_reads: Option<usize>,
    /// Updates performed by each transaction attempt.
    pub transaction_updates: Option<usize>,
}

impl TaskConfig {
    /// Checks catalog invariants before a task is executed or published.
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
/// Fully resolved settings used to open SlateDB and execute one task.
pub struct ResolvedConfig {
    /// Decimal scale factor applied to the catalog defaults.
    pub scale: f64,
    /// Resolved dataset shape.
    pub dataset: DatasetConfig,
    /// Resolved benchmark cache capacities.
    pub caches: CacheConfig,
    /// Resolved task behavior.
    pub task: TaskConfig,
    /// Serialized effective SlateDB settings published with the result.
    pub slate_settings: serde_json::Value,
    /// Serialized SlateDB defaults used to highlight overrides in the website.
    pub slate_default_settings: serde_json::Value,
    /// Rust build profile, normally `debug` or `release`.
    pub build_profile: String,
    /// SlateDB Cargo features enabled in this runner build.
    pub enabled_features: Vec<String>,
    /// Typed effective settings passed to `Db::open`.
    pub settings: Settings,
}

/// Resolves catalog defaults, scale, and SlateDB settings for one task.
///
/// A task-specific `settings.<task>.toml` is a complete replacement for the
/// shared settings file. If neither file exists, SlateDB defaults are used.
/// Legacy part-based object-store cache settings are reset because the runner
/// supplies SlateDB's whole-file mirror directly.
pub fn load(task: Task, scale: BenchmarkScale, settings_path: &Path) -> Result<ResolvedConfig> {
    let mut settings = match settings_path_for_task(task, settings_path)? {
        Some(settings_path) => Settings::from_file(&settings_path).with_context(|| {
            format!("loading SlateDB settings from {}", settings_path.display())
        })?,
        None => Settings::default(),
    };
    let dataset = DatasetConfig {
        record_count: scaled_u64(RECORD_COUNT, 1, scale),
        key_bytes: KEY_BYTES,
        value_bytes: VALUE_BYTES,
        value_compression_ratio: 1.0,
    };
    let caches = CacheConfig {
        block_bytes: scaled_u64(BLOCK_CACHE_BYTES, MIN_BLOCK_CACHE_BYTES, scale),
        metadata_bytes: scaled_u64(METADATA_CACHE_BYTES, MIN_METADATA_CACHE_BYTES, scale),
    };
    settings.object_store_cache_options = Default::default();
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
        slate_default_settings: serde_json::to_value(Settings::default())
            .context("serializing default SlateDB settings")?,
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

fn settings_path_for_task(task: Task, shared_path: &Path) -> Result<Option<PathBuf>> {
    if !task.is_preparation() {
        let workload_path = shared_path.with_file_name(format!("settings.{}.toml", task.as_str()));
        if workload_path
            .try_exists()
            .with_context(|| format!("checking {}", workload_path.display()))?
        {
            return Ok(Some(workload_path));
        }
    }
    if shared_path
        .try_exists()
        .with_context(|| format!("checking {}", shared_path.display()))?
    {
        Ok(Some(shared_path.to_path_buf()))
    } else {
        Ok(None)
    }
}

fn task_config(task: Task, scale: BenchmarkScale, record_count: u64) -> TaskConfig {
    let definition = task.definition();
    TaskConfig {
        task,
        clients: definition.clients,
        warmup_ms: scaled_u64(definition.warmup_ms, MIN_DURATION_MS, scale),
        measurement_ms: scaled_u64(definition.measurement_ms, MIN_DURATION_MS, scale),
        initial_state: definition.initial_state.to_string(),
        key_selection: definition.key_selection.to_string(),
        operation_mix: definition
            .operation_mix
            .iter()
            .map(|(name, fraction)| ((*name).to_string(), *fraction))
            .collect(),
        scan_limit: definition.scan_limit,
        transaction_hot_keys: definition
            .transaction_hot_keys
            .map(|hot_keys| hot_keys.min(record_count)),
        transaction_reads: definition.transaction_reads,
        transaction_updates: definition.transaction_updates,
    }
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
    use super::{load, scaled_u64, BenchmarkScale, Task};
    use slatedb::config::Settings;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn write_settings(root: &Path, relative_path: &str, contents: &str) -> PathBuf {
        let path = root.join(relative_path);
        fs::create_dir_all(path.parent().expect("settings parent"))
            .expect("create settings parent");
        fs::write(&path, contents).expect("write settings");
        path
    }

    #[test]
    fn published_configurations_load_and_validate() {
        let scaled = "0.01".parse::<BenchmarkScale>().expect("scale");
        let tasks = [Task::BulkLoad, Task::Compaction]
            .into_iter()
            .chain(Task::workloads());

        for task in tasks {
            for scale in [BenchmarkScale::FULL, scaled] {
                load(task, scale, Path::new("config/settings.toml")).unwrap_or_else(|error| {
                    panic!("loading {task} at scale {scale} failed: {error:#}")
                });
            }
        }
    }

    #[test]
    fn decimal_scale_applies_factor_and_minimum() {
        let scale = "0.01".parse::<BenchmarkScale>().expect("scale");

        assert_eq!(scaled_u64(10_000, 1, scale), 100);
        assert_eq!(scaled_u64(100, 5, scale), 5);
        assert_eq!(scaled_u64(0, 5, scale), 0);
        assert!(!scale.is_full());
        assert!(BenchmarkScale::FULL.is_full());
        assert!("1%".parse::<BenchmarkScale>().is_err());
    }

    #[test]
    fn workload_settings_replace_shared_settings() {
        let directory = TempDir::new().expect("temporary directory");
        let shared_path = write_settings(
            directory.path(),
            "settings.toml",
            "l0_max_ssts = 31\nl0_max_ssts_per_key = 29\n",
        );
        write_settings(
            directory.path(),
            "settings.balanced.toml",
            "l0_max_ssts = 63\n",
        );

        let config =
            load(Task::Balanced, BenchmarkScale::FULL, &shared_path).expect("workload settings");

        assert_eq!(config.settings.l0_max_ssts, 63);
        assert_eq!(
            config.settings.l0_max_ssts_per_key,
            Settings::default().l0_max_ssts_per_key
        );
    }

    #[test]
    fn workloads_without_settings_use_shared_settings() {
        let directory = TempDir::new().expect("temporary directory");
        let shared_path = write_settings(directory.path(), "settings.toml", "l0_max_ssts = 31\n");
        write_settings(
            directory.path(),
            "settings.balanced.toml",
            "l0_max_ssts = 63\n",
        );

        let config =
            load(Task::ReadHeavy, BenchmarkScale::FULL, &shared_path).expect("shared settings");

        assert_eq!(config.settings.l0_max_ssts, 31);
    }

    #[test]
    fn missing_settings_use_slatedb_defaults() {
        let directory = TempDir::new().expect("temporary directory");
        let shared_path = directory.path().join("settings.toml");

        let config =
            load(Task::Balanced, BenchmarkScale::FULL, &shared_path).expect("default settings");

        assert_eq!(config.settings.l0_max_ssts, Settings::default().l0_max_ssts);
    }

    #[test]
    fn workload_settings_do_not_require_shared_settings() {
        let directory = TempDir::new().expect("temporary directory");
        let shared_path = directory.path().join("settings.toml");
        write_settings(
            directory.path(),
            "settings.balanced.toml",
            "l0_max_ssts = 63\n",
        );

        let config =
            load(Task::Balanced, BenchmarkScale::FULL, &shared_path).expect("workload settings");

        assert_eq!(config.settings.l0_max_ssts, 63);
    }
}
