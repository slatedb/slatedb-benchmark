use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use slatedb::config::Settings;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize)]
pub struct SuiteConfig {
    pub schema_version: u32,
    pub profiles: Vec<ProfileConfig>,
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
pub enum ProfileExecution {
    Isolated,
    Sequential,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileConfig {
    pub name: String,
    pub release: bool,
    pub execution: ProfileExecution,
    pub object_store_probe: ProbeConfig,
    pub compaction_quiet_ms: u64,
    pub compaction_timeout_ms: u64,
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub block_cache_bytes: Option<u64>,
    pub metadata_cache_bytes: Option<u64>,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub sst_block_bytes: Option<usize>,
    pub workloads: Vec<WorkloadConfig>,
}

impl ProfileConfig {
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
struct ProfileFile {
    schema_version: u32,
    release: bool,
    execution: ProfileExecution,
    object_store_probe: ProbeFile,
    compaction_quiet: String,
    compaction_timeout: String,
    record_count: u64,
    key_bytes: usize,
    value_bytes: usize,
    block_cache_bytes: Option<u64>,
    metadata_cache_bytes: Option<u64>,
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
    pub profile: String,
    pub workload: String,
    pub variant: String,
}

#[derive(Debug, Clone)]
pub struct VariantConfig {
    pub profile: ProfileConfig,
    pub workload: WorkloadConfig,
    pub variant: String,
    pub clients: Option<usize>,
    pub target_rate: Option<u64>,
    pub slate_settings: Settings,
}

impl SuiteConfig {
    pub fn load() -> Result<Self> {
        Self::load_from(Path::new("config"))
    }

    pub fn load_from(config_dir: &Path) -> Result<Self> {
        let mut profile_paths = Vec::new();
        for entry in fs::read_dir(config_dir)
            .with_context(|| format!("reading configuration directory {}", config_dir.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(".profile.toml"))
            {
                profile_paths.push(entry.path());
            }
        }
        profile_paths.sort();
        if profile_paths.is_empty() {
            bail!(
                "configuration directory {} contains no *.profile.toml files",
                config_dir.display()
            );
        }

        let profiles = profile_paths
            .iter()
            .map(|profile_path| load_profile(profile_path))
            .collect::<Result<Vec<_>>>()?;
        let suite = Self {
            schema_version: 1,
            profiles,
            config_dir: config_dir.to_path_buf(),
        };
        suite.validate()?;
        Ok(suite)
    }

    fn validate(&self) -> Result<()> {
        let mut identities = BTreeSet::new();
        let mut profile_names = BTreeSet::new();
        for profile in &self.profiles {
            if !profile_names.insert(profile.name.clone()) {
                bail!("duplicate profile {}", profile.name);
            }
            if profile.workloads.is_empty() {
                bail!("profile {} has no workloads", profile.name);
            }
            if profile.key_bytes == 0 || profile.value_bytes == 0 {
                bail!("profile {} has a zero-sized key or value", profile.name);
            }
            if profile.sst_block_bytes.is_some_and(|size| size != 8192) {
                bail!(
                    "profile {} requests an unsupported SST block size",
                    profile.name
                );
            }
            if profile.compaction_quiet_ms == 0 || profile.compaction_timeout_ms == 0 {
                bail!(
                    "profile {} has a zero compaction quiet period or timeout",
                    profile.name
                );
            }
            validate_probe(profile)?;
            self.slate_settings(profile)
                .with_context(|| format!("loading SlateDB settings for {}", profile.name))?;

            if profile.execution == ProfileExecution::Sequential {
                validate_sequential_profile(profile)?;
            }

            let mut workload_names = BTreeSet::new();
            for workload in &profile.workloads {
                if !workload_names.insert(&workload.name) {
                    bail!(
                        "profile {} contains duplicate workload {}",
                        profile.name,
                        workload.name
                    );
                }
                if workload.variants.is_empty() {
                    bail!(
                        "workload {}/{} has no variants",
                        profile.name,
                        workload.name
                    );
                }
                for variant in &workload.variants {
                    if variant.name.trim().is_empty() {
                        bail!(
                            "workload {}/{} has a variant with an empty name",
                            profile.name,
                            workload.name
                        );
                    }
                    if variant.clients == Some(0) || variant.target_rate == Some(0) {
                        bail!(
                            "variant {}/{}/{} has zero concurrency or target rate",
                            profile.name,
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
                                profile.name,
                                workload.name,
                                variant.name
                            );
                        }
                    } else if variant.clients.is_none() || variant.target_rate.is_some() {
                        bail!(
                            "closed-loop variant {}/{}/{} must define only clients",
                            profile.name,
                            workload.name,
                            variant.name
                        );
                    }
                    if !identities.insert((
                        profile.name.clone(),
                        workload.name.clone(),
                        variant.name.clone(),
                    )) {
                        bail!(
                            "duplicate benchmark variant {}/{}/{}",
                            profile.name,
                            workload.name,
                            variant.name
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn catalog(&self, profile_name: Option<&str>) -> Result<Vec<CatalogEntry>> {
        let entries = self
            .profiles
            .iter()
            .filter(|profile| match profile_name {
                Some(name) => profile.name == name,
                None => profile.release,
            })
            .flat_map(|profile| {
                profile.workloads.iter().flat_map(move |workload| {
                    workload.variants.iter().map(move |variant| CatalogEntry {
                        profile: profile.name.clone(),
                        workload: workload.name.clone(),
                        variant: variant.name.clone(),
                    })
                })
            })
            .collect::<Vec<_>>();
        if entries.is_empty() {
            bail!("profile selector did not match any configured benchmark variants");
        }
        Ok(entries)
    }

    pub fn select(
        &self,
        profile_name: Option<&str>,
        workload_name: Option<&str>,
        variant_name: Option<&str>,
    ) -> Result<Vec<VariantConfig>> {
        let mut selected = Vec::new();
        for profile in &self.profiles {
            let include_profile = match profile_name {
                Some(name) => profile.name == name,
                None => profile.release,
            };
            if !include_profile {
                continue;
            }
            let slate_settings = self.slate_settings(profile)?;
            for workload in &profile.workloads {
                if workload_name.is_some_and(|name| name != workload.name) {
                    continue;
                }
                for variant in &workload.variants {
                    if variant_name.is_some_and(|name| name != variant.name) {
                        continue;
                    }
                    selected.push(VariantConfig {
                        profile: profile.clone(),
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

    pub fn slate_settings(&self, profile: &ProfileConfig) -> Result<Settings> {
        let path = self
            .config_dir
            .join(format!("{}.settings.toml", profile.name));
        Settings::from_file(&path)
            .with_context(|| format!("loading SlateDB settings file {}", path.display()))
    }
}

impl VariantConfig {
    pub fn record_count(&self) -> u64 {
        self.workload
            .record_count
            .unwrap_or(self.profile.record_count)
    }

    pub fn key_bytes(&self) -> usize {
        self.workload.key_bytes.unwrap_or(self.profile.key_bytes)
    }

    pub fn value_bytes(&self) -> usize {
        self.workload
            .value_bytes
            .unwrap_or(self.profile.value_bytes)
    }

    pub fn warmup_ms(&self) -> u64 {
        self.workload.warmup_ms.unwrap_or(self.profile.warmup_ms)
    }

    pub fn measurement_ms(&self) -> u64 {
        self.workload
            .measurement_ms
            .unwrap_or(self.profile.measurement_ms)
    }
}

fn load_profile(profile_path: &Path) -> Result<ProfileConfig> {
    let file_name = profile_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("profile file name is not valid UTF-8")?;
    let name = file_name
        .strip_suffix(".profile.toml")
        .filter(|name| !name.is_empty())
        .with_context(|| format!("invalid profile file name {}", profile_path.display()))?
        .to_string();
    let file: ProfileFile = read_toml(profile_path)?;
    if file.schema_version != 1 {
        bail!(
            "unsupported configuration schema {} in {}",
            file.schema_version,
            profile_path.display()
        );
    }

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
                        parse_duration_ms(value, profile_path, &format!("{field_prefix}.warmup"))
                    })
                    .transpose()?,
                measurement_ms: workload
                    .measurement
                    .as_deref()
                    .map(|value| {
                        parse_duration_ms(
                            value,
                            profile_path,
                            &format!("{field_prefix}.measurement"),
                        )
                    })
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ProfileConfig {
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
                profile_path,
                "object_store_probe.throughput_warmup",
            )?,
            throughput_measurement_ms: parse_duration_ms(
                &file.object_store_probe.throughput_measurement,
                profile_path,
                "object_store_probe.throughput_measurement",
            )?,
        },
        compaction_quiet_ms: parse_duration_ms(
            &file.compaction_quiet,
            profile_path,
            "compaction_quiet",
        )?,
        compaction_timeout_ms: parse_duration_ms(
            &file.compaction_timeout,
            profile_path,
            "compaction_timeout",
        )?,
        record_count: file.record_count,
        key_bytes: file.key_bytes,
        value_bytes: file.value_bytes,
        block_cache_bytes: file.block_cache_bytes,
        metadata_cache_bytes: file.metadata_cache_bytes,
        warmup_ms: parse_duration_ms(&file.warmup, profile_path, "warmup")?,
        measurement_ms: parse_duration_ms(&file.measurement, profile_path, "measurement")?,
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

fn validate_probe(profile: &ProfileConfig) -> Result<()> {
    let probe = &profile.object_store_probe;
    if probe.latency_operations == 0
        || probe.latency_object_bytes == 0
        || probe.throughput_object_bytes == 0
        || probe.throughput_concurrency == 0
        || probe.throughput_measurement_ms == 0
    {
        bail!(
            "object-store probe settings for profile {} must be positive",
            profile.name
        );
    }
    Ok(())
}

fn validate_sequential_profile(profile: &ProfileConfig) -> Result<()> {
    let bulk_loads = profile
        .workloads
        .iter()
        .filter(|workload| workload.kind == WorkloadKind::BulkLoad)
        .count();
    if bulk_loads != 1 || profile.workloads[0].kind != WorkloadKind::BulkLoad {
        bail!(
            "sequential profile {} must begin with exactly one bulk-load workload",
            profile.name
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SuiteConfig;
    use slatedb::config::CompressionCodec;
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    #[test]
    fn release_catalog_contains_every_documented_variant() {
        let suite = SuiteConfig::load_from(Path::new("../config")).expect("config");
        assert_eq!(suite.catalog(None).expect("catalog").len(), 42);
    }

    #[test]
    fn smoke_is_a_small_non_release_profile() {
        let suite = SuiteConfig::load_from(Path::new("../config")).expect("config");
        let release = suite.catalog(None).expect("release catalog");
        let smoke = suite.catalog(Some("smoke")).expect("smoke catalog");
        assert!(smoke.len() < release.len());
        assert!(smoke.iter().all(|entry| entry.profile == "smoke"));
        assert!(release.iter().all(|entry| entry.profile != "smoke"));
    }

    #[test]
    fn catalog_has_the_documented_profile_workload_variant_counts() {
        let suite = SuiteConfig::load_from(Path::new("../config")).expect("config");
        let mut profiles = BTreeMap::new();
        let mut workloads = BTreeMap::new();
        for entry in suite.catalog(None).expect("catalog") {
            *profiles.entry(entry.profile.clone()).or_insert(0) += 1;
            *workloads
                .entry((entry.profile, entry.workload))
                .or_insert(0) += 1;
        }
        assert_eq!(
            profiles,
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
        let suite = SuiteConfig::load_from(Path::new("../config")).expect("config");
        let variant = suite
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
        let suite = SuiteConfig::load_from(Path::new("../config")).expect("config");
        let profile = suite
            .profiles
            .iter()
            .find(|profile| profile.name == "rocksdb")
            .expect("rocksdb profile");
        assert_eq!(
            profile
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
    fn profile_settings_overlay_slatedb_defaults() {
        let suite = SuiteConfig::load_from(Path::new("../config")).expect("config");
        let profile = suite
            .profiles
            .iter()
            .find(|profile| profile.name == "rocksdb")
            .expect("rocksdb profile");

        let profile_settings = suite.slate_settings(profile).expect("profile settings");
        assert_eq!(
            profile_settings.flush_interval,
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            profile_settings.compression_codec,
            Some(CompressionCodec::Zstd)
        );
        assert!(profile_settings.wal_enabled);
        assert!(profile_settings.compactor_options.is_some());
    }
}
