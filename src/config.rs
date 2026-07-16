use anyhow::{bail, Context, Result};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use slatedb::config::Settings;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkConfig {
    pub suites: Vec<SuiteConfig>,
    #[serde(skip)]
    config_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeConfig {
    pub latency_operations: u64,
    pub latency_object_bytes: usize,
    pub throughput_object_bytes: usize,
    pub throughput_concurrency: usize,
    #[serde(
        rename(deserialize = "throughput_warmup"),
        deserialize_with = "deserialize_duration_ms"
    )]
    pub throughput_warmup_ms: u64,
    #[serde(
        rename(deserialize = "throughput_measurement"),
        deserialize_with = "deserialize_duration_ms"
    )]
    pub throughput_measurement_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SuiteExecution {
    Isolated,
    Sequential,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteConfig {
    #[serde(skip_deserializing)]
    pub name: String,
    pub release: bool,
    pub execution: SuiteExecution,
    pub object_store_probe: ProbeConfig,
    #[serde(
        rename(deserialize = "compaction_quiet"),
        deserialize_with = "deserialize_duration_ms"
    )]
    pub compaction_quiet_ms: u64,
    #[serde(
        rename(deserialize = "compaction_timeout"),
        deserialize_with = "deserialize_duration_ms"
    )]
    pub compaction_timeout_ms: u64,
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub value_compression_ratio: f64,
    pub block_cache_bytes: Option<u64>,
    pub metadata_cache_bytes: Option<u64>,
    pub object_store_cache_bytes: Option<u64>,
    #[serde(
        rename(deserialize = "warmup"),
        deserialize_with = "deserialize_duration_ms"
    )]
    pub warmup_ms: u64,
    #[serde(
        rename(deserialize = "measurement"),
        deserialize_with = "deserialize_duration_ms"
    )]
    pub measurement_ms: u64,
    #[serde(default)]
    pub sst_block_bytes: Option<usize>,
    pub workloads: Vec<WorkloadConfig>,
}

impl SuiteConfig {
    pub fn mode(&self) -> &'static str {
        if self.release {
            "published"
        } else {
            "smoke"
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadConfig {
    pub name: String,
    pub kind: WorkloadKind,
    pub variants: Vec<VariantDefinition>,
    #[serde(default)]
    pub await_durable: bool,
    #[serde(default)]
    pub record_count: Option<u64>,
    #[serde(default)]
    pub key_bytes: Option<usize>,
    #[serde(default)]
    pub value_bytes: Option<usize>,
    #[serde(
        default,
        rename(deserialize = "warmup"),
        deserialize_with = "deserialize_optional_duration_ms"
    )]
    pub warmup_ms: Option<u64>,
    #[serde(
        default,
        rename(deserialize = "measurement"),
        deserialize_with = "deserialize_optional_duration_ms"
    )]
    pub measurement_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariantDefinition {
    pub name: String,
    pub clients: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadKind {
    YcsbA,
    YcsbB,
    YcsbC,
    YcsbD,
    YcsbE,
    YcsbF,
    BulkLoad,
    RandomRead,
    MultiRandomRead,
    ForwardRange,
    ReverseRange,
    Overwrite,
    ReadWhileWriting,
    ForwardRangeWhileWriting,
    ReverseRangeWhileWriting,
    ColdRead,
    SustainedIngest,
    TransactionContention,
    PrefixScan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyDistribution {
    Uniform,
    Zipfian,
}

impl WorkloadKind {
    pub(crate) const fn may_write(self) -> bool {
        !matches!(
            self,
            Self::YcsbC
                | Self::RandomRead
                | Self::MultiRandomRead
                | Self::ForwardRange
                | Self::ReverseRange
                | Self::ColdRead
                | Self::PrefixScan
        )
    }

    pub(crate) const fn default_operation_name(self) -> &'static str {
        match self {
            Self::TransactionContention => "transaction",
            Self::PrefixScan => "prefix-scan",
            Self::MultiRandomRead => "batch-read",
            Self::ForwardRange | Self::ReverseRange | Self::YcsbE => "scan",
            Self::SustainedIngest | Self::BulkLoad | Self::YcsbD => "insert",
            Self::Overwrite => "update",
            _ => "operation",
        }
    }

    pub(crate) const fn key_distribution(self) -> KeyDistribution {
        match self {
            Self::YcsbA | Self::YcsbB | Self::YcsbC | Self::YcsbF => KeyDistribution::Zipfian,
            _ => KeyDistribution::Uniform,
        }
    }

    pub(crate) const fn while_writing_read_kind(self) -> Option<Self> {
        match self {
            Self::ReadWhileWriting => Some(Self::RandomRead),
            Self::ForwardRangeWhileWriting => Some(Self::ForwardRange),
            Self::ReverseRangeWhileWriting => Some(Self::ReverseRange),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogEntry {
    pub suite: String,
    pub workload: String,
    pub variant: String,
}

#[derive(Debug, Clone)]
pub struct VariantConfig {
    pub suite: SuiteConfig,
    pub workload: WorkloadConfig,
    pub variant: String,
    pub clients: usize,
    pub slate_settings: Settings,
}

impl BenchmarkConfig {
    pub fn load_from(config_dir: &Path) -> Result<Self> {
        let mut suite_paths = Vec::new();
        for entry in fs::read_dir(config_dir)
            .with_context(|| format!("reading configuration directory {}", config_dir.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(".suite.toml"))
            {
                suite_paths.push(entry.path());
            }
        }
        suite_paths.sort();
        if suite_paths.is_empty() {
            bail!(
                "configuration directory {} contains no *.suite.toml files",
                config_dir.display()
            );
        }

        let suites = suite_paths
            .iter()
            .map(|suite_path| load_suite(suite_path))
            .collect::<Result<Vec<_>>>()?;
        let benchmark = Self {
            suites,
            config_dir: config_dir.to_path_buf(),
        };
        benchmark.validate()?;
        Ok(benchmark)
    }

    fn validate(&self) -> Result<()> {
        let mut identities = BTreeSet::new();
        let mut suite_names = BTreeSet::new();
        for suite in &self.suites {
            validate_identity_component(&suite.name, "suite")?;
            if !suite_names.insert(suite.name.clone()) {
                bail!("duplicate suite {}", suite.name);
            }
            if suite.workloads.is_empty() {
                bail!("suite {} has no workloads", suite.name);
            }
            if suite.key_bytes == 0 || suite.value_bytes == 0 {
                bail!("suite {} has a zero-sized key or value", suite.name);
            }
            if !(suite.value_compression_ratio.is_finite()
                && 0.0 < suite.value_compression_ratio
                && suite.value_compression_ratio <= 1.0)
            {
                bail!(
                    "suite {} has a value compression ratio outside (0, 1]",
                    suite.name
                );
            }
            if suite.sst_block_bytes.is_some_and(|size| size != 8192) {
                bail!(
                    "suite {} requests an unsupported SST block size",
                    suite.name
                );
            }
            if suite.compaction_quiet_ms == 0 || suite.compaction_timeout_ms == 0 {
                bail!(
                    "suite {} has a zero compaction quiet period or timeout",
                    suite.name
                );
            }
            validate_probe(suite)?;
            self.slate_settings(suite)
                .with_context(|| format!("loading SlateDB settings for {}", suite.name))?;

            if suite.execution == SuiteExecution::Sequential {
                validate_sequential_suite(suite)?;
            }

            let mut workload_names = BTreeSet::new();
            for workload in &suite.workloads {
                validate_identity_component(&workload.name, "workload")?;
                if !workload_names.insert(&workload.name) {
                    bail!(
                        "suite {} contains duplicate workload {}",
                        suite.name,
                        workload.name
                    );
                }
                if workload.variants.is_empty() {
                    bail!("workload {}/{} has no variants", suite.name, workload.name);
                }
                if workload.kind == WorkloadKind::BulkLoad
                    && workload.warmup_ms.unwrap_or(suite.warmup_ms) != 0
                {
                    bail!(
                        "bulk-load workload {}/{} must have zero warmup",
                        suite.name,
                        workload.name
                    );
                }
                for variant in &workload.variants {
                    validate_identity_component(&variant.name, "variant")?;
                    if variant.clients == 0 {
                        bail!(
                            "variant {}/{}/{} has zero clients",
                            suite.name,
                            workload.name,
                            variant.name
                        );
                    }
                    if workload.kind == WorkloadKind::BulkLoad && variant.clients != 1 {
                        bail!(
                            "bulk-load variant {}/{}/{} must define exactly one client",
                            suite.name,
                            workload.name,
                            variant.name
                        );
                    }
                    if !identities.insert((
                        suite.name.clone(),
                        workload.name.clone(),
                        variant.name.clone(),
                    )) {
                        bail!(
                            "duplicate benchmark variant {}/{}/{}",
                            suite.name,
                            workload.name,
                            variant.name
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn catalog(&self, suite_name: Option<&str>) -> Result<Vec<CatalogEntry>> {
        let entries = self
            .suites
            .iter()
            .filter(|suite| suite_name.map_or(suite.release, |name| suite.name == name))
            .flat_map(|suite| {
                suite.workloads.iter().flat_map(move |workload| {
                    workload.variants.iter().map(move |variant| CatalogEntry {
                        suite: suite.name.clone(),
                        workload: workload.name.clone(),
                        variant: variant.name.clone(),
                    })
                })
            })
            .collect::<Vec<_>>();
        if entries.is_empty() {
            bail!("suite selector did not match any configured benchmark variants");
        }
        Ok(entries)
    }

    pub fn select(
        &self,
        suite_name: Option<&str>,
        workload_name: Option<&str>,
        variant_name: Option<&str>,
    ) -> Result<Vec<VariantConfig>> {
        let mut selected = Vec::new();
        for suite in &self.suites {
            let include_suite = suite_name.map_or(suite.release, |name| suite.name == name);
            if !include_suite {
                continue;
            }
            let slate_settings = self.slate_settings(suite)?;
            for workload in &suite.workloads {
                if workload_name.is_some_and(|name| name != workload.name) {
                    continue;
                }
                for variant in &workload.variants {
                    if variant_name.is_some_and(|name| name != variant.name) {
                        continue;
                    }
                    selected.push(VariantConfig {
                        suite: suite.clone(),
                        workload: workload.clone(),
                        variant: variant.name.clone(),
                        clients: variant.clients,
                        slate_settings: slate_settings.clone(),
                    });
                }
            }
        }
        if selected.is_empty() {
            bail!("selectors did not match any configured benchmark variants");
        }
        Ok(selected)
    }

    pub fn slate_settings(&self, suite: &SuiteConfig) -> Result<Settings> {
        let path = self
            .config_dir
            .join(format!("{}.settings.toml", suite.name));
        let mut settings = Settings::from_file(&path)
            .with_context(|| format!("loading SlateDB settings file {}", path.display()))?;
        settings.object_store_cache_options.root_folder = None;
        settings.object_store_cache_options.max_cache_size_bytes = suite
            .object_store_cache_bytes
            .map(usize::try_from)
            .transpose()
            .context("object-store cache capacity exceeds the platform limit")?;
        Ok(settings)
    }
}

impl VariantConfig {
    pub fn record_count(&self) -> u64 {
        self.workload
            .record_count
            .unwrap_or(self.suite.record_count)
    }

    pub fn key_bytes(&self) -> usize {
        self.workload.key_bytes.unwrap_or(self.suite.key_bytes)
    }

    pub fn value_bytes(&self) -> usize {
        self.workload.value_bytes.unwrap_or(self.suite.value_bytes)
    }

    pub fn value_compression_ratio(&self) -> f64 {
        self.suite.value_compression_ratio
    }

    pub fn warmup_ms(&self) -> u64 {
        self.workload.warmup_ms.unwrap_or(self.suite.warmup_ms)
    }

    pub fn measurement_ms(&self) -> u64 {
        self.workload
            .measurement_ms
            .unwrap_or(self.suite.measurement_ms)
    }
}

fn load_suite(suite_path: &Path) -> Result<SuiteConfig> {
    let file_name = suite_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("suite file name is not valid UTF-8")?;
    let name = file_name
        .strip_suffix(".suite.toml")
        .filter(|name| !name.is_empty())
        .with_context(|| format!("invalid suite file name {}", suite_path.display()))?
        .to_string();
    let mut suite: SuiteConfig = read_toml(suite_path)?;
    suite.name = name;
    Ok(suite)
}

fn read_toml<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading configuration file {}", path.display()))?;
    toml::from_str(&contents)
        .with_context(|| format!("parsing configuration file {}", path.display()))
}

fn deserialize_duration_ms<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_duration_ms(&value).map_err(D::Error::custom)
}

fn deserialize_optional_duration_ms<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| parse_duration_ms(&value).map_err(D::Error::custom))
        .transpose()
}

fn parse_duration_ms(value: &str) -> std::result::Result<u64, String> {
    let duration = humantime::parse_duration(value)
        .map_err(|error| format!("invalid duration {value:?}: {error}"))?;
    u64::try_from(duration.as_millis()).map_err(|_| format!("duration {value:?} is too large"))
}

fn validate_probe(suite: &SuiteConfig) -> Result<()> {
    let probe = &suite.object_store_probe;
    if probe.latency_operations == 0
        || probe.latency_object_bytes == 0
        || probe.throughput_object_bytes == 0
        || probe.throughput_concurrency == 0
        || probe.throughput_measurement_ms == 0
    {
        bail!(
            "object-store probe settings for suite {} must be positive",
            suite.name
        );
    }
    Ok(())
}

fn validate_sequential_suite(suite: &SuiteConfig) -> Result<()> {
    let bulk_loads = suite
        .workloads
        .iter()
        .filter(|workload| workload.kind == WorkloadKind::BulkLoad)
        .count();
    if bulk_loads != 1 || suite.workloads[0].kind != WorkloadKind::BulkLoad {
        bail!(
            "sequential suite {} must begin with exactly one bulk-load workload",
            suite.name
        );
    }
    if suite.workloads[0].variants.len() != 1 {
        bail!(
            "sequential suite {} bulk-load must have exactly one variant",
            suite.name
        );
    }
    Ok(())
}

fn validate_identity_component(value: &str, kind: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{kind} names must be 1-128 ASCII letters, digits, '.', '-', or '_': {value}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BenchmarkConfig, KeyDistribution, WorkloadKind};
    use slatedb::config::CompressionCodec;
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn release_catalog_contains_every_documented_variant() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        assert_eq!(benchmark.catalog(None).expect("catalog").len(), 24);
    }

    #[test]
    fn suite_duration_strings_deserialize_into_runtime_fields() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let suite = benchmark
            .suites
            .iter()
            .find(|suite| suite.name == "rocksdb")
            .expect("rocksdb suite");

        assert_eq!(suite.measurement_ms, 90 * 60 * 1_000);
        assert_eq!(suite.compaction_timeout_ms, 2 * 60 * 60 * 1_000);
        assert_eq!(suite.object_store_probe.throughput_warmup_ms, 5_000);
        assert_eq!(suite.workloads[0].measurement_ms, Some(0));

        let resolved = serde_json::to_value(suite).expect("resolved configuration");
        assert!(resolved.get("measurement_ms").is_some());
        assert!(resolved.get("measurement").is_none());
    }

    #[test]
    fn workload_kind_owns_execution_classifications() {
        assert!(WorkloadKind::YcsbA.may_write());
        assert!(!WorkloadKind::YcsbC.may_write());
        assert_eq!(
            WorkloadKind::YcsbA.key_distribution(),
            KeyDistribution::Zipfian
        );
        assert_eq!(
            WorkloadKind::SustainedIngest.key_distribution(),
            KeyDistribution::Uniform
        );
        assert_eq!(
            WorkloadKind::PrefixScan.default_operation_name(),
            "prefix-scan"
        );
        assert_eq!(
            WorkloadKind::ReadWhileWriting.while_writing_read_kind(),
            Some(WorkloadKind::RandomRead)
        );
    }

    #[test]
    fn smoke_is_a_small_non_release_suite() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let release = benchmark.catalog(None).expect("release catalog");
        let smoke = benchmark.catalog(Some("smoke")).expect("smoke catalog");
        assert!(smoke.len() < release.len());
        assert!(smoke.iter().all(|entry| entry.suite == "smoke"));
        assert!(release.iter().all(|entry| entry.suite != "smoke"));
    }

    #[test]
    fn catalog_has_the_documented_suite_workload_variant_counts() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let mut suites = BTreeMap::new();
        let mut workloads = BTreeMap::new();
        for entry in benchmark.catalog(None).expect("catalog") {
            *suites.entry(entry.suite.clone()).or_insert(0) += 1;
            *workloads.entry((entry.suite, entry.workload)).or_insert(0) += 1;
        }
        assert_eq!(
            suites,
            BTreeMap::from([
                ("rocksdb".to_string(), 9),
                ("slatedb".to_string(), 9),
                ("ycsb".to_string(), 6),
            ])
        );
        assert_eq!(workloads.len(), 19);
    }

    #[test]
    fn variants_are_explicit_configuration() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let variant = benchmark
            .select(Some("rocksdb"), Some("read-while-writing"), None)
            .expect("variant")
            .pop()
            .expect("configured variant");
        assert_eq!(variant.variant, "readers-64-writer-1");
        assert_eq!(variant.clients, 64);
    }

    #[test]
    fn bulk_load_requires_zero_effective_warmup() {
        let mut benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let suite = benchmark
            .suites
            .iter_mut()
            .find(|suite| suite.name == "rocksdb")
            .expect("rocksdb suite");
        suite.warmup_ms = 1;

        let error = benchmark
            .validate()
            .expect_err("inherited bulk-load warmup should fail");
        assert!(error
            .to_string()
            .contains("bulk-load workload rocksdb/bulk-load must have zero warmup"));

        let suite = benchmark
            .suites
            .iter_mut()
            .find(|suite| suite.name == "rocksdb")
            .expect("rocksdb suite");
        suite.workloads[0].warmup_ms = Some(0);
        benchmark
            .validate()
            .expect("zero workload override should be valid");
    }

    #[test]
    fn bulk_load_requires_the_single_loader_it_executes() {
        let mut benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let suite = benchmark
            .suites
            .iter_mut()
            .find(|suite| suite.name == "rocksdb")
            .expect("rocksdb suite");
        suite.workloads[0].variants[0].clients = 64;

        let error = benchmark
            .validate()
            .expect_err("multi-client bulk load should fail");
        assert!(error.to_string().contains(
            "bulk-load variant rocksdb/bulk-load/clients-1 must define exactly one client"
        ));
    }

    #[test]
    fn value_compression_ratio_must_be_within_unit_interval() {
        let mut benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        benchmark.suites[0].value_compression_ratio = 0.0;

        let error = benchmark
            .validate()
            .expect_err("zero compression ratio should fail");
        assert!(error
            .to_string()
            .contains("value compression ratio outside (0, 1]"));
    }

    #[test]
    fn rocksdb_workloads_follow_declaration_order() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let suite = benchmark
            .suites
            .iter()
            .find(|suite| suite.name == "rocksdb")
            .expect("rocksdb suite");
        assert_eq!(
            suite
                .workloads
                .iter()
                .map(|workload| workload.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "bulk-load",
                "random-read",
                "multi-random-read",
                "forward-range",
                "reverse-range",
                "overwrite",
                "read-while-writing",
                "forward-range-while-writing",
                "reverse-range-while-writing",
            ]
        );
    }

    #[test]
    fn suite_settings_overlay_slatedb_defaults() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        let suite = benchmark
            .suites
            .iter()
            .find(|suite| suite.name == "rocksdb")
            .expect("rocksdb suite");

        let suite_settings = benchmark.slate_settings(suite).expect("suite settings");
        assert_eq!(
            suite_settings.flush_interval,
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            suite_settings.compression_codec,
            Some(CompressionCodec::Zstd)
        );
        assert_eq!(suite.value_compression_ratio, 0.5);
        assert!(suite_settings.wal_enabled);
        assert!(suite_settings.compactor_options.is_some());
        assert_eq!(suite.block_cache_bytes, Some(6 * 1024 * 1024 * 1024));
        assert_eq!(suite.metadata_cache_bytes, Some(128 * 1024 * 1024));
        assert_eq!(
            suite.object_store_cache_bytes,
            Some(16 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            suite_settings
                .object_store_cache_options
                .max_cache_size_bytes,
            Some(16 * 1024 * 1024 * 1024)
        );
        assert!(suite_settings
            .object_store_cache_options
            .root_folder
            .is_none());
    }
}
