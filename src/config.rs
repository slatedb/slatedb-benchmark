use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize)]
pub struct ProbeConfig {
    pub latency_operations: u64,
    pub latency_object_bytes: usize,
    pub throughput_object_bytes: usize,
    pub throughput_concurrency: usize,
    pub throughput_warmup_ms: u64,
    pub throughput_measurement_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SuiteExecution {
    Isolated,
    Sequential,
}

#[derive(Debug, Clone, Serialize)]
pub struct SuiteConfig {
    pub name: String,
    pub release: bool,
    pub execution: SuiteExecution,
    pub object_store_probe: ProbeConfig,
    pub compaction_quiet_ms: u64,
    pub compaction_timeout_ms: u64,
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub block_cache_bytes: Option<u64>,
    pub metadata_cache_bytes: Option<u64>,
    pub object_store_cache_bytes: Option<u64>,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
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

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadConfig {
    pub name: String,
    pub kind: WorkloadKind,
    pub variants: Vec<VariantDefinition>,
    pub await_durable: bool,
    pub record_count: Option<u64>,
    pub key_bytes: Option<usize>,
    pub value_bytes: Option<usize>,
    pub warmup_ms: Option<u64>,
    pub measurement_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariantDefinition {
    pub name: String,
    #[serde(default)]
    pub clients: Option<usize>,
    #[serde(default)]
    pub target_rate: Option<u64>,
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
    OpenLoopRead,
    OpenLoopReadUpdate,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SuiteFile {
    release: bool,
    execution: SuiteExecution,
    object_store_probe: ProbeFile,
    compaction_quiet: String,
    compaction_timeout: String,
    record_count: u64,
    key_bytes: usize,
    value_bytes: usize,
    block_cache_bytes: Option<u64>,
    metadata_cache_bytes: Option<u64>,
    object_store_cache_bytes: Option<u64>,
    warmup: String,
    measurement: String,
    #[serde(default)]
    sst_block_bytes: Option<usize>,
    workloads: Vec<WorkloadFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeFile {
    latency_operations: u64,
    latency_object_bytes: usize,
    throughput_object_bytes: usize,
    throughput_concurrency: usize,
    throughput_warmup: String,
    throughput_measurement: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadFile {
    name: String,
    kind: WorkloadKind,
    variants: Vec<VariantDefinition>,
    #[serde(default)]
    await_durable: bool,
    #[serde(default)]
    record_count: Option<u64>,
    #[serde(default)]
    key_bytes: Option<usize>,
    #[serde(default)]
    value_bytes: Option<usize>,
    #[serde(default)]
    warmup: Option<String>,
    #[serde(default)]
    measurement: Option<String>,
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
    pub clients: Option<usize>,
    pub target_rate: Option<u64>,
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
                for variant in &workload.variants {
                    validate_identity_component(&variant.name, "variant")?;
                    if variant.clients == Some(0) || variant.target_rate == Some(0) {
                        bail!(
                            "variant {}/{}/{} has zero concurrency or target rate",
                            suite.name,
                            workload.name,
                            variant.name
                        );
                    }
                    let open_loop = matches!(
                        workload.kind,
                        WorkloadKind::OpenLoopRead | WorkloadKind::OpenLoopReadUpdate
                    );
                    if open_loop {
                        if variant.target_rate.is_none() || variant.clients.is_some() {
                            bail!(
                                "open-loop variant {}/{}/{} must define only target_rate",
                                suite.name,
                                workload.name,
                                variant.name
                            );
                        }
                    } else if variant.clients.is_none() || variant.target_rate.is_some() {
                        bail!(
                            "closed-loop variant {}/{}/{} must define only clients",
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
                        target_rate: variant.target_rate,
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
    let file: SuiteFile = read_toml(suite_path)?;
    let workloads = file
        .workloads
        .into_iter()
        .map(|workload| {
            let field_prefix = format!("workloads.{}", workload.name);
            Ok(WorkloadConfig {
                name: workload.name,
                kind: workload.kind,
                variants: workload.variants,
                await_durable: workload.await_durable,
                record_count: workload.record_count,
                key_bytes: workload.key_bytes,
                value_bytes: workload.value_bytes,
                warmup_ms: workload
                    .warmup
                    .as_deref()
                    .map(|value| {
                        parse_duration_ms(value, suite_path, &format!("{field_prefix}.warmup"))
                    })
                    .transpose()?,
                measurement_ms: workload
                    .measurement
                    .as_deref()
                    .map(|value| {
                        parse_duration_ms(value, suite_path, &format!("{field_prefix}.measurement"))
                    })
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(SuiteConfig {
        name,
        release: file.release,
        execution: file.execution,
        object_store_probe: ProbeConfig {
            latency_operations: file.object_store_probe.latency_operations,
            latency_object_bytes: file.object_store_probe.latency_object_bytes,
            throughput_object_bytes: file.object_store_probe.throughput_object_bytes,
            throughput_concurrency: file.object_store_probe.throughput_concurrency,
            throughput_warmup_ms: parse_duration_ms(
                &file.object_store_probe.throughput_warmup,
                suite_path,
                "object_store_probe.throughput_warmup",
            )?,
            throughput_measurement_ms: parse_duration_ms(
                &file.object_store_probe.throughput_measurement,
                suite_path,
                "object_store_probe.throughput_measurement",
            )?,
        },
        compaction_quiet_ms: parse_duration_ms(
            &file.compaction_quiet,
            suite_path,
            "compaction_quiet",
        )?,
        compaction_timeout_ms: parse_duration_ms(
            &file.compaction_timeout,
            suite_path,
            "compaction_timeout",
        )?,
        record_count: file.record_count,
        key_bytes: file.key_bytes,
        value_bytes: file.value_bytes,
        block_cache_bytes: file.block_cache_bytes,
        metadata_cache_bytes: file.metadata_cache_bytes,
        object_store_cache_bytes: file.object_store_cache_bytes,
        warmup_ms: parse_duration_ms(&file.warmup, suite_path, "warmup")?,
        measurement_ms: parse_duration_ms(&file.measurement, suite_path, "measurement")?,
        sst_block_bytes: file.sst_block_bytes,
        workloads,
    })
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

fn parse_duration_ms(value: &str, path: &Path, field: &str) -> Result<u64> {
    let duration = humantime::parse_duration(value)
        .with_context(|| format!("parsing {field} in {}", path.display()))?;
    u64::try_from(duration.as_millis())
        .with_context(|| format!("{field} in {} is too large", path.display()))
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
    use super::BenchmarkConfig;
    use slatedb::config::CompressionCodec;
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn release_catalog_contains_every_documented_variant() {
        let benchmark = BenchmarkConfig::load_from(Path::new("config")).expect("config");
        assert_eq!(benchmark.catalog(None).expect("catalog").len(), 42);
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
                ("slatedb".to_string(), 15),
                ("ycsb".to_string(), 18),
            ])
        );
        assert_eq!(workloads.len(), 21);
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
        assert_eq!(variant.clients, Some(64));
        assert_eq!(variant.target_rate, None);
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
