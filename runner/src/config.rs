use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteConfig {
    pub schema_version: u32,
    pub mode: String,
    pub object_store_probe: ProbeConfig,
    pub profiles: Vec<ProfileConfig>,
    pub compaction_quiet_ms: u64,
    pub compaction_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeConfig {
    pub latency_operations: u64,
    pub latency_object_bytes: usize,
    pub throughput_object_bytes: usize,
    pub throughput_concurrency: usize,
    pub throughput_warmup_ms: u64,
    pub throughput_measurement_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub name: String,
    pub record_count: u64,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub block_cache_bytes: Option<u64>,
    pub metadata_cache_bytes: Option<u64>,
    pub flush_interval_ms: u64,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub compression: Option<String>,
    #[serde(default)]
    pub sst_block_bytes: Option<usize>,
    pub workloads: Vec<WorkloadConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadConfig {
    pub name: String,
    pub kind: WorkloadKind,
    pub variants: Vec<String>,
    #[serde(default)]
    pub await_durable: bool,
    #[serde(default)]
    pub record_count: Option<u64>,
    #[serde(default)]
    pub key_bytes: Option<usize>,
    #[serde(default)]
    pub value_bytes: Option<usize>,
    #[serde(default)]
    pub warmup_ms: Option<u64>,
    #[serde(default)]
    pub measurement_ms: Option<u64>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublishedConfig {
    schema_version: u32,
    mode: String,
    object_store_probe: ProbeConfig,
    profiles: Vec<ProfileConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct SmokeConfig {
    schema_version: u32,
    mode: String,
    object_store_probe: ProbeConfig,
    profile_overrides: BTreeMap<String, ConfigOverride>,
    workload_overrides: BTreeMap<String, ConfigOverride>,
    compaction_quiet_ms: u64,
    compaction_timeout_ms: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ConfigOverride {
    record_count: Option<u64>,
    warmup_ms: Option<u64>,
    measurement_ms: Option<u64>,
    block_cache_bytes: Option<u64>,
    metadata_cache_bytes: Option<u64>,
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
}

impl SuiteConfig {
    pub fn load(smoke: bool) -> Result<Self> {
        Self::load_from(Path::new("config"), smoke)
    }

    pub fn load_from(config_dir: &Path, smoke: bool) -> Result<Self> {
        let published_path = config_dir.join("published.json");
        let published: PublishedConfig = serde_json::from_slice(
            &fs::read(&published_path)
                .with_context(|| format!("reading {}", published_path.display()))?,
        )
        .with_context(|| format!("parsing {}", published_path.display()))?;

        let mut suite = SuiteConfig {
            schema_version: published.schema_version,
            mode: published.mode,
            object_store_probe: published.object_store_probe,
            profiles: published.profiles,
            compaction_quiet_ms: 15_000,
            compaction_timeout_ms: 600_000,
        };
        if smoke {
            let smoke_path = config_dir.join("smoke.json");
            let smoke: SmokeConfig = serde_json::from_slice(
                &fs::read(&smoke_path)
                    .with_context(|| format!("reading {}", smoke_path.display()))?,
            )
            .with_context(|| format!("parsing {}", smoke_path.display()))?;
            if smoke.schema_version != suite.schema_version {
                bail!("smoke and published configuration schema versions differ");
            }
            suite.mode = smoke.mode;
            suite.object_store_probe = smoke.object_store_probe;
            suite.compaction_quiet_ms = smoke.compaction_quiet_ms;
            suite.compaction_timeout_ms = smoke.compaction_timeout_ms;
            for profile in &mut suite.profiles {
                if let Some(override_) = smoke.profile_overrides.get(&profile.name) {
                    apply_profile_override(profile, override_);
                }
                for workload in &mut profile.workloads {
                    let key = format!("{}/{}", profile.name, workload.name);
                    if let Some(override_) = smoke.workload_overrides.get(&key) {
                        apply_workload_override(workload, override_);
                    }
                }
            }
        }
        suite.validate()?;
        Ok(suite)
    }

    fn validate(&self) -> Result<()> {
        if self.schema_version != 1 {
            bail!("unsupported configuration schema {}", self.schema_version);
        }
        if !matches!(self.mode.as_str(), "published" | "smoke") {
            bail!("unsupported benchmark mode {}", self.mode);
        }
        if self.object_store_probe.latency_operations == 0
            || self.object_store_probe.latency_object_bytes == 0
            || self.object_store_probe.throughput_object_bytes == 0
            || self.object_store_probe.throughput_concurrency == 0
            || self.object_store_probe.throughput_measurement_ms == 0
        {
            bail!("object-store probe settings must be positive");
        }
        let mut identities = BTreeSet::new();
        for profile in &self.profiles {
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
            for workload in &profile.workloads {
                if workload.variants.is_empty() {
                    bail!(
                        "workload {}/{} has no variants",
                        profile.name,
                        workload.name
                    );
                }
                for variant in &workload.variants {
                    let (clients, target_rate) = parse_variant(variant).with_context(|| {
                        format!(
                            "invalid variant {}/{}/{}",
                            profile.name, workload.name, variant
                        )
                    })?;
                    if clients == Some(0) || target_rate == Some(0) {
                        bail!("variant {variant} has zero concurrency or target rate");
                    }
                    let open_loop = matches!(
                        workload.kind,
                        WorkloadKind::OpenLoopRead | WorkloadKind::OpenLoopReadUpdate
                    );
                    if open_loop != target_rate.is_some() {
                        bail!(
                            "workload {}/{} has the wrong variant type",
                            profile.name,
                            workload.name
                        );
                    }
                    if !identities.insert((
                        profile.name.clone(),
                        workload.name.clone(),
                        variant.clone(),
                    )) {
                        bail!(
                            "duplicate benchmark variant {}/{}/{}",
                            profile.name,
                            workload.name,
                            variant
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn catalog(&self) -> Vec<CatalogEntry> {
        self.profiles
            .iter()
            .flat_map(|profile| {
                profile.workloads.iter().flat_map(move |workload| {
                    workload.variants.iter().map(move |variant| CatalogEntry {
                        profile: profile.name.clone(),
                        workload: workload.name.clone(),
                        variant: variant.clone(),
                    })
                })
            })
            .collect()
    }

    pub fn select(
        &self,
        profile_name: Option<&str>,
        workload_name: Option<&str>,
        variant_name: Option<&str>,
    ) -> Result<Vec<VariantConfig>> {
        let mut selected = Vec::new();
        for profile in &self.profiles {
            if profile_name.is_some_and(|name| name != profile.name) {
                continue;
            }
            for workload in &profile.workloads {
                if workload_name.is_some_and(|name| name != workload.name) {
                    continue;
                }
                for variant in &workload.variants {
                    if variant_name.is_some_and(|name| name != variant) {
                        continue;
                    }
                    let (clients, target_rate) = parse_variant(variant)?;
                    selected.push(VariantConfig {
                        profile: profile.clone(),
                        workload: workload.clone(),
                        variant: variant.clone(),
                        clients,
                        target_rate,
                    });
                }
            }
        }
        if selected.is_empty() {
            bail!("selectors did not match any configured benchmark variants");
        }
        Ok(selected)
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

fn apply_profile_override(profile: &mut ProfileConfig, override_: &ConfigOverride) {
    if let Some(value) = override_.record_count {
        profile.record_count = value;
    }
    if let Some(value) = override_.warmup_ms {
        profile.warmup_ms = value;
    }
    if let Some(value) = override_.measurement_ms {
        profile.measurement_ms = value;
    }
    if let Some(value) = override_.block_cache_bytes {
        profile.block_cache_bytes = Some(value);
    }
    if let Some(value) = override_.metadata_cache_bytes {
        profile.metadata_cache_bytes = Some(value);
    }
}

fn apply_workload_override(workload: &mut WorkloadConfig, override_: &ConfigOverride) {
    if let Some(value) = override_.record_count {
        workload.record_count = Some(value);
    }
    if let Some(value) = override_.warmup_ms {
        workload.warmup_ms = Some(value);
    }
    if let Some(value) = override_.measurement_ms {
        workload.measurement_ms = Some(value);
    }
}

fn parse_variant(variant: &str) -> Result<(Option<usize>, Option<u64>)> {
    if let Some(value) = variant.strip_prefix("clients-") {
        return Ok((Some(value.parse()?), None));
    }
    if let Some(value) = variant.strip_prefix("rate-") {
        return Ok((None, Some(value.parse()?)));
    }
    if variant == "readers-64-writer-1" {
        return Ok((Some(64), None));
    }
    bail!("unrecognized variant name {variant}")
}

#[cfg(test)]
mod tests {
    use super::SuiteConfig;
    use std::collections::BTreeMap;
    use std::path::Path;

    #[test]
    fn published_catalog_contains_every_documented_variant() {
        let suite = SuiteConfig::load_from(Path::new("../config"), false).expect("config");
        assert_eq!(suite.catalog().len(), 42);
    }

    #[test]
    fn smoke_preserves_catalog() {
        let published = SuiteConfig::load_from(Path::new("../config"), false).expect("published");
        let smoke = SuiteConfig::load_from(Path::new("../config"), true).expect("smoke");
        assert_eq!(published.catalog(), smoke.catalog());
        assert!(smoke.profiles[0].record_count < published.profiles[0].record_count);
    }

    #[test]
    fn catalog_has_the_documented_profile_workload_variant_counts() {
        let suite = SuiteConfig::load_from(Path::new("../config"), false).expect("config");
        let mut profiles = BTreeMap::new();
        let mut workloads = BTreeMap::new();
        for entry in suite.catalog() {
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
}
