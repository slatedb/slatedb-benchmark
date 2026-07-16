mod session;

use crate::cli::{RunArgs, WorkerArgs};
use crate::config::{BenchmarkConfig, SuiteConfig, SuiteExecution, VariantConfig, WorkloadKind};
use crate::database_size::live_database_size_bytes;
use crate::instrumented_store::{StoreMetrics, StoreSnapshot};
use crate::model::{
    BenchmarkConfiguration, EncodedHistogram, Environment, Identity, InitialState,
    ObjectStoreBaseline, ResultRecord, RunManifest, SourceFiles, TimeseriesFile,
};
use crate::object_store_probe::{delete_prefix, probe, ObjectStoreContext};
use crate::system::{
    inspect_environment, sample_until_stopped, verify_environment, ApplicationCounters,
    BenchmarkMetricsRecorder, DatabaseSizeSource, SampledTimeseries,
};
use crate::validation::{validate_result, validate_run};
use crate::workloads::{
    execute_variant, extend_with_compaction_phase, populate_dataset, prepare_bulk_load,
    WorkloadOutcome,
};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::Serialize;
use sha2::{Digest, Sha256};
use slatedb::admin::{AdminBuilder, CloneSourceSpec};
use slatedb::config::{CheckpointOptions, Settings};
use slatedb::db_cache::{
    foyer::{FoyerCache, FoyerCacheOptions},
    DbCache, SplitCache,
};
use slatedb::{Db, SstBlockSize, VersionedManifest};
use slatedb_common::metrics::{MetricValue, Metrics, MetricsRecorder};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone)]
struct GoldenDatabase {
    path: Path,
    checkpoint_id: Uuid,
    manifest_id: u64,
    lsm_digest: String,
    size_bytes: u64,
}

struct GoldenManager {
    store: Arc<dyn ObjectStore>,
    root: Path,
    values: HashMap<String, GoldenDatabase>,
}

struct ResultContext<'a> {
    environment: &'a Environment,
    baseline: &'a ObjectStoreBaseline,
    baseline_histograms: &'a BTreeMap<String, EncodedHistogram>,
    args: &'a RunArgs,
}

struct BulkLoadExecution {
    initial: InitialState,
    outcome: Option<WorkloadOutcome>,
}

struct CompactionMeasurement {
    started: Instant,
    start_store: StoreSnapshot,
    start_slate: Metrics,
    stop_tx: tokio::sync::watch::Sender<bool>,
    sampler: tokio::task::JoinHandle<Result<SampledTimeseries>>,
    store_metrics: Arc<StoreMetrics>,
    recorder: Arc<BenchmarkMetricsRecorder>,
}

struct CompletedCompactionMeasurement {
    sampled: SampledTimeseries,
    store_delta: StoreSnapshot,
    start_slate: Metrics,
    end_slate: Metrics,
    elapsed: Duration,
}

impl CompactionMeasurement {
    fn start(
        outcome: &WorkloadOutcome,
        store_metrics: Arc<StoreMetrics>,
        recorder: Arc<BenchmarkMetricsRecorder>,
        database_path: Path,
    ) -> Self {
        let started = Instant::now();
        let start_store = store_metrics.snapshot();
        let start_slate = recorder.snapshot();
        let counters = Arc::new(ApplicationCounters::default());
        counters
            .operations
            .store(outcome.application.total_operations, Ordering::Relaxed);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let sampler = tokio::spawn(sample_until_stopped(
            started,
            counters,
            Arc::clone(&store_metrics),
            Arc::clone(&recorder),
            DatabaseSizeSource::TrackedPrefix(database_path),
            stop_rx,
        ));
        Self {
            started,
            start_store,
            start_slate,
            stop_tx,
            sampler,
            store_metrics,
            recorder,
        }
    }

    async fn finish(self) -> Result<CompletedCompactionMeasurement> {
        let _ = self.stop_tx.send(true);
        let sampled = self
            .sampler
            .await
            .context("joining bulk compaction sampler")??;
        Ok(CompletedCompactionMeasurement {
            sampled,
            store_delta: self.store_metrics.snapshot().difference(&self.start_store),
            start_slate: self.start_slate,
            end_slate: self.recorder.snapshot(),
            elapsed: self.started.elapsed(),
        })
    }
}

impl CompletedCompactionMeasurement {
    fn apply(self, outcome: &mut WorkloadOutcome) -> Result<()> {
        extend_with_compaction_phase(
            outcome,
            self.sampled.samples,
            self.store_delta,
            &self.start_slate,
            &self.end_slate,
            self.elapsed,
        )
    }
}

pub async fn execute(args: RunArgs) -> Result<()> {
    if args.output.exists() && args.session.is_none() {
        bail!("output directory {} already exists", args.output.display());
    }
    fs::create_dir_all(&args.output)
        .with_context(|| format!("creating {}", args.output.display()))?;
    let result = execute_inner(&args).await;
    if result.is_err() {
        tracing::error!("benchmark run failed; partial output remains for diagnosis");
    }
    result
}

async fn execute_inner(args: &RunArgs) -> Result<()> {
    let started_at = Utc::now();
    let benchmark = BenchmarkConfig::load_from(&args.config_dir)?;
    let selected = benchmark.select(
        args.suite.as_deref(),
        args.workload.as_deref(),
        args.variant.as_deref(),
    )?;
    let mode = selected
        .first()
        .context("benchmark selection is empty")?
        .suite
        .mode()
        .to_string();
    if selected.iter().any(|variant| variant.suite.mode() != mode) {
        bail!("a benchmark run cannot mix release and non-release suites");
    }

    let object_store = ObjectStoreContext::load()?;
    let environment = inspect_environment(
        &object_store.provider,
        &object_store.endpoint,
        &object_store.region,
    );

    if args.session.is_some() {
        return session::execute(args, &benchmark, selected, &object_store, &environment).await;
    }

    let mut by_suite = BTreeMap::<String, Vec<VariantConfig>>::new();
    for variant in selected {
        by_suite
            .entry(variant.suite.name.clone())
            .or_default()
            .push(variant);
    }
    let suite_count = by_suite.len();
    let mut object_store_baselines = BTreeMap::new();
    let mut result_paths = Vec::new();
    for (suite_name, variants) in by_suite {
        let suite = &variants.first().context("suite selection is empty")?.suite;
        verify_environment(&environment, !suite.release)?;
        tracing::info!(
            suite = suite_name,
            "probing object store before dataset preparation"
        );
        let probe_root = object_store.root.clone().join(suite_name.as_str());
        let (baseline, baseline_histograms) = probe(
            Arc::clone(&object_store.raw),
            &probe_root,
            &suite.object_store_probe,
        )
        .await?;
        let baseline_path = if suite_count == 1 {
            args.output.join("object-store.json")
        } else {
            args.output.join(format!("object-store-{suite_name}.json"))
        };
        write_json(&baseline_path, &baseline)?;
        let result_context = ResultContext {
            environment: &environment,
            baseline: &baseline,
            baseline_histograms: &baseline_histograms,
            args,
        };
        result_paths
            .extend(execute_suite(variants, &benchmark, &object_store, &result_context).await?);
        object_store_baselines.insert(suite_name, baseline);
    }

    let run = RunManifest {
        status: "ok".to_string(),
        started_at: started_at.to_rfc3339(),
        finished_at: Utc::now().to_rfc3339(),
        mode,
        slate_version: env!("BENCHMARK_SLATE_VERSION").to_string(),
        slate_commit: env!("BENCHMARK_SLATE_COMMIT").to_string(),
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        runner_commit: env!("BENCHMARK_RUNNER_COMMIT").to_string(),
        lockfile_sha256: env!("BENCHMARK_LOCK_HASH").to_string(),
        resolved_configuration: serde_json::to_value(&benchmark)?,
        object_store_baselines,
        results: result_paths,
    };
    validate_run(&run, &args.schema_dir)?;
    write_json(&args.output.join("run.json"), &run)?;
    println!(
        "{{\"status\":\"ok\",\"run\":\"{}\"}}",
        args.output.join("run.json").display()
    );
    Ok(())
}

async fn execute_suite(
    selected: Vec<VariantConfig>,
    benchmark: &BenchmarkConfig,
    object_store: &ObjectStoreContext,
    result_context: &ResultContext<'_>,
) -> Result<Vec<String>> {
    let suite = selected
        .first()
        .context("suite selection is empty")?
        .suite
        .clone();
    let run_root = object_store
        .root
        .clone()
        .join(suite.name.as_str())
        .join(format!("run-{}", Uuid::new_v4()));
    let store: Arc<dyn ObjectStore> = object_store.instrumented.clone();
    let mut golden_manager = GoldenManager {
        store: Arc::clone(&store),
        root: run_root.clone(),
        values: HashMap::new(),
    };

    let run_result = async {
        if suite.execution == SuiteExecution::Sequential {
            return run_sequential_suite(
                selected,
                benchmark,
                &run_root,
                Arc::clone(&store),
                object_store.instrumented.metrics(),
                !object_store.provider.eq_ignore_ascii_case("memory"),
                result_context,
            )
            .await;
        }

        let mut result_paths = Vec::new();
        for variant in &selected {
            let golden = golden_manager.prepare(variant).await?;
            let clone_path = run_root
                .clone()
                .join("clones")
                .join(variant.workload.name.as_str())
                .join(format!("{}-{}", variant.variant, Uuid::new_v4()));
            result_paths.push(
                execute_isolated_variant(
                    variant,
                    clone_path,
                    &golden,
                    Arc::clone(&store),
                    object_store.instrumented.metrics(),
                    !object_store.provider.eq_ignore_ascii_case("memory"),
                    result_context,
                )
                .await?,
            );
        }
        Ok(result_paths)
    }
    .await;

    let cleanup_result = golden_manager.cleanup().await;
    let run_cleanup = delete_prefix(store, &run_root).await;
    let result_paths = run_result?;
    cleanup_result?;
    run_cleanup?;
    Ok(result_paths)
}

async fn execute_isolated_variant(
    variant: &VariantConfig,
    clone_path: Path,
    golden: &GoldenDatabase,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<StoreMetrics>,
    fresh_process: bool,
    result_context: &ResultContext<'_>,
) -> Result<String> {
    clone_database(
        Arc::clone(&store),
        &clone_path,
        &golden.path,
        golden.checkpoint_id,
    )
    .await?;
    let result = async {
        let recorder = Arc::new(BenchmarkMetricsRecorder::new());
        let db = open_database(
            clone_path.clone(),
            Arc::clone(&store),
            &variant.suite,
            &variant.slate_settings,
            Arc::clone(&recorder),
        )
        .await?;
        let clone_digest = lsm_digest(&db)?;
        if clone_digest != golden.lsm_digest {
            bail!("clone {} does not match checkpoint LSM state", clone_path);
        }
        let initial = InitialState {
            checkpoint_id: Some(golden.checkpoint_id.to_string()),
            manifest_id: Some(golden.manifest_id),
            lsm_digest_sha256: clone_digest,
        };
        let initial_database_bytes = live_database_size_bytes(&db.manifest());
        db.close()
            .await
            .context("closing clone validation handle")?;
        let mut outcome = if fresh_process {
            execute_variant_in_fresh_process(
                variant,
                &clone_path,
                &golden.lsm_digest,
                result_context.args,
            )
            .await?
        } else {
            let object_store_cache = object_store_cache_directory(&variant.suite)?;
            let recorder = Arc::new(BenchmarkMetricsRecorder::new());
            let db = open_database_with_object_store_cache(
                clone_path.clone(),
                Arc::clone(&store),
                &variant.suite,
                &variant.slate_settings,
                object_store_cache.as_ref().map(|cache| cache.path()),
                Arc::clone(&recorder),
            )
            .await?;
            let outcome = execute_variant(Arc::clone(&db), variant, store_metrics, recorder).await;
            db.close().await.context("closing benchmark clone")?;
            outcome?
        };
        let final_manifest = AdminBuilder::new(clone_path.clone(), Arc::clone(&store))
            .build()
            .read_manifest(None)
            .await
            .context("reading final isolated benchmark manifest")?
            .context("final isolated benchmark manifest does not exist")?;
        outcome.storage.database_size_bytes = live_database_size_bytes(&final_manifest);
        write_variant_result(
            variant,
            outcome,
            initial,
            initial_database_bytes,
            result_context,
        )
    }
    .await;
    let cleanup = delete_prefix(store, &clone_path).await;
    match (result, cleanup) {
        (Ok(path), Ok(())) => Ok(path),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.context("cleaning up benchmark clone")),
    }
}

pub async fn execute_worker(args: WorkerArgs) -> Result<()> {
    let benchmark = BenchmarkConfig::load_from(&args.config_dir)?;
    let mut selected =
        benchmark.select(Some(&args.suite), Some(&args.workload), Some(&args.variant))?;
    let variant = selected
        .pop()
        .context("worker selection did not resolve to a variant")?;
    let context = ObjectStoreContext::load()?;
    let database_path = Path::from(args.database_path);
    context
        .instrumented
        .seed_prefix(&database_path)
        .await
        .context("seeding worker database-size tracker")?;
    let store: Arc<dyn ObjectStore> = context.instrumented.clone();
    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let db = open_database_with_object_store_cache(
        database_path.clone(),
        store,
        &variant.suite,
        &variant.slate_settings,
        args.object_store_cache_root.as_deref(),
        Arc::clone(&recorder),
    )
    .await?;
    let digest = lsm_digest(&db)?;
    if digest != args.expected_lsm_digest {
        bail!("fresh worker opened an unexpected LSM state");
    }
    let outcome = execute_variant(
        Arc::clone(&db),
        &variant,
        context.instrumented.metrics(),
        recorder,
    )
    .await;
    db.close()
        .await
        .context("closing fresh-process benchmark clone")?;
    write_json(&args.output, &outcome?)
}

async fn execute_variant_in_fresh_process(
    variant: &VariantConfig,
    database_path: &Path,
    expected_lsm_digest: &str,
    args: &RunArgs,
) -> Result<WorkloadOutcome> {
    let object_store_cache = object_store_cache_directory(&variant.suite)?;
    let output = args.output.join(format!(".worker-{}.json", Uuid::new_v4()));
    let executable = std::env::current_exe().context("locating benchmark executable")?;
    let mut command = tokio::process::Command::new(executable);
    command
        .arg("worker")
        .arg("--suite")
        .arg(&variant.suite.name)
        .arg("--workload")
        .arg(&variant.workload.name)
        .arg("--variant")
        .arg(&variant.variant)
        .arg("--database-path")
        .arg(database_path.to_string())
        .arg("--expected-lsm-digest")
        .arg(expected_lsm_digest)
        .arg("--output")
        .arg(&output)
        .arg("--config-dir")
        .arg(&args.config_dir);
    if let Some(cache) = &object_store_cache {
        command.arg("--object-store-cache-root").arg(cache.path());
    }
    let status = command
        .status()
        .await
        .context("running fresh benchmark worker")?;
    if !status.success() {
        bail!("fresh benchmark worker exited with {status}");
    }
    let bytes =
        fs::read(&output).with_context(|| format!("reading worker result {}", output.display()))?;
    let outcome: WorkloadOutcome = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing worker result {}", output.display()))?;
    fs::remove_file(&output)
        .with_context(|| format!("removing worker result {}", output.display()))?;
    Ok(outcome)
}

impl GoldenManager {
    async fn prepare(&mut self, variant: &VariantConfig) -> Result<GoldenDatabase> {
        let key = golden_key(variant)?;
        if let Some(golden) = self.values.get(&key) {
            return Ok(golden.clone());
        }
        let path = self.root.clone().join("golden").join(key.as_str());
        let golden = prepare_golden_database(variant, path, Arc::clone(&self.store), &key).await?;
        self.values.insert(key, golden.clone());
        Ok(golden)
    }

    async fn cleanup(&self) -> Result<()> {
        for golden in self.values.values() {
            let admin = AdminBuilder::new(golden.path.clone(), Arc::clone(&self.store)).build();
            admin
                .delete_checkpoint(golden.checkpoint_id)
                .await
                .context("deleting golden checkpoint")?;
            delete_prefix(Arc::clone(&self.store), &golden.path).await?;
        }
        Ok(())
    }
}

fn golden_key(variant: &VariantConfig) -> Result<String> {
    let prefix_layout = variant.workload.kind == WorkloadKind::PrefixScan;
    let record_count = if variant.workload.kind == WorkloadKind::SustainedIngest {
        0
    } else {
        variant.record_count()
    };
    Ok(format!(
        "{}-{}-{}-{}-compression-{:016x}-{}-{}",
        if prefix_layout { "prefix" } else { "records" },
        record_count,
        variant.key_bytes(),
        variant.value_bytes(),
        variant.value_compression_ratio().to_bits(),
        variant.suite.sst_block_bytes.unwrap_or_default(),
        settings_digest(&variant.slate_settings)?
    ))
}

async fn prepare_golden_database(
    variant: &VariantConfig,
    path: Path,
    store: Arc<dyn ObjectStore>,
    key: &str,
) -> Result<GoldenDatabase> {
    let prefix_layout = variant.workload.kind == WorkloadKind::PrefixScan;
    let record_count = if variant.workload.kind == WorkloadKind::SustainedIngest {
        0
    } else {
        variant.record_count()
    };
    tracing::info!(
        dataset = key,
        records = record_count,
        "preparing golden database"
    );
    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let db = open_database(
        path.clone(),
        Arc::clone(&store),
        &variant.suite,
        &variant.slate_settings,
        Arc::clone(&recorder),
    )
    .await?;
    populate_dataset(
        Arc::clone(&db),
        record_count,
        variant.key_bytes(),
        variant.value_bytes(),
        variant.value_compression_ratio(),
        prefix_layout,
    )
    .await?;
    wait_for_compaction(&db, &recorder, &variant.suite).await?;
    db.close().await.context("closing golden database")?;

    let admin = AdminBuilder::new(path.clone(), Arc::clone(&store)).build();
    let checkpoint = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime: None,
            source: None,
            name: Some(format!("benchmark-{key}")),
        })
        .await
        .context("creating golden checkpoint")?;
    let checkpoint_manifest = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .context("reading golden checkpoint manifest")?
        .context("golden checkpoint manifest does not exist")?;
    Ok(GoldenDatabase {
        path: path.clone(),
        checkpoint_id: checkpoint.id,
        manifest_id: checkpoint.manifest_id,
        lsm_digest: manifest_lsm_digest(&checkpoint_manifest)?,
        size_bytes: live_database_size_bytes(&checkpoint_manifest),
    })
}

async fn close_database_after<T>(
    db: &Db,
    operation: Result<T>,
    close_context: &'static str,
) -> Result<T> {
    let close_result = db.close().await.context(close_context);
    let value = operation?;
    close_result?;
    Ok(value)
}

async fn execute_bulk_load_and_compact(
    bulk_variant: &VariantConfig,
    compaction_settings: &Settings,
    database_path: &Path,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<StoreMetrics>,
    measure_bulk_load: bool,
) -> Result<BulkLoadExecution> {
    let object_store_cache = object_store_cache_directory(&bulk_variant.suite)?;
    let bulk_recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let bulk_db = open_database_with_object_store_cache(
        database_path.clone(),
        Arc::clone(&store),
        &bulk_variant.suite,
        &bulk_variant.slate_settings,
        object_store_cache.as_ref().map(|cache| cache.path()),
        Arc::clone(&bulk_recorder),
    )
    .await?;
    let bulk_result = async {
        let initial = InitialState {
            checkpoint_id: None,
            manifest_id: Some(bulk_db.status().current_manifest.id()),
            lsm_digest_sha256: lsm_digest(&bulk_db)?,
        };
        let outcome = if measure_bulk_load {
            Some(
                execute_variant(
                    Arc::clone(&bulk_db),
                    bulk_variant,
                    Arc::clone(&store_metrics),
                    Arc::clone(&bulk_recorder),
                )
                .await?,
            )
        } else {
            prepare_bulk_load(Arc::clone(&bulk_db), bulk_variant).await?;
            bulk_db
                .flush()
                .await
                .context("flushing bulk-load database")?;
            None
        };
        Ok(BulkLoadExecution { initial, outcome })
    }
    .await;
    let mut execution =
        close_database_after(&bulk_db, bulk_result, "closing bulk-load database").await?;

    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let measurement = execution.outcome.as_ref().map(|outcome| {
        CompactionMeasurement::start(
            outcome,
            Arc::clone(&store_metrics),
            Arc::clone(&recorder),
            database_path.clone(),
        )
    });
    let compaction_db = match open_database_with_object_store_cache(
        database_path.clone(),
        store,
        &bulk_variant.suite,
        compaction_settings,
        object_store_cache.as_ref().map(|cache| cache.path()),
        Arc::clone(&recorder),
    )
    .await
    {
        Ok(db) => db,
        Err(error) => {
            if let Some(measurement) = measurement {
                let _ = measurement.finish().await;
            }
            return Err(error);
        }
    };
    let compaction_result =
        wait_for_compaction(&compaction_db, &recorder, &bulk_variant.suite).await;
    let measurement_result = match measurement {
        Some(measurement) => measurement.finish().await.map(Some),
        None => Ok(None),
    };
    let completed_measurement = close_database_after(
        &compaction_db,
        compaction_result.and(measurement_result),
        "closing post-bulk compaction database",
    )
    .await?;
    if let (Some(outcome), Some(measurement)) = (execution.outcome.as_mut(), completed_measurement)
    {
        measurement.apply(outcome)?;
    }
    Ok(execution)
}

async fn run_sequential_suite(
    selected: Vec<VariantConfig>,
    benchmark: &BenchmarkConfig,
    run_root: &Path,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<StoreMetrics>,
    fresh_process_variants: bool,
    result_context: &ResultContext<'_>,
) -> Result<Vec<String>> {
    let suite = selected
        .first()
        .context("sequential suite selection is empty")?
        .suite
        .clone();
    let bulk_workload = suite
        .workloads
        .iter()
        .find(|workload| workload.kind == WorkloadKind::BulkLoad)
        .context("sequential suite requires a bulk-load workload")?;
    let bulk_variant_name = bulk_workload
        .variants
        .first()
        .context("bulk-load workload has no variants")?
        .name
        .clone();
    let mut bulk_variant = benchmark
        .select(
            Some(&suite.name),
            Some(&bulk_workload.name),
            Some(&bulk_variant_name),
        )?
        .pop()
        .context("bulk-load variant is missing")?;
    bulk_variant.slate_settings = bulk_load_settings(&bulk_variant.slate_settings);
    let path = run_root.clone().join("sequential-suite");
    let selected_bulk = selected
        .iter()
        .any(|variant| variant.workload.kind == WorkloadKind::BulkLoad);
    let suite_settings = benchmark.slate_settings(&suite)?;
    let BulkLoadExecution {
        initial: bulk_initial,
        outcome: mut bulk_outcome,
    } = execute_bulk_load_and_compact(
        &bulk_variant,
        &suite_settings,
        &path,
        Arc::clone(&store),
        Arc::clone(&store_metrics),
        selected_bulk,
    )
    .await?;
    let compacted_manifest = AdminBuilder::new(path.clone(), Arc::clone(&store))
        .build()
        .read_manifest(None)
        .await
        .context("reading compacted bulk-load manifest")?
        .context("compacted bulk-load manifest does not exist")?;
    let compacted_size = live_database_size_bytes(&compacted_manifest);
    let mut paths = Vec::new();
    if let Some(mut outcome) = bulk_outcome.take() {
        outcome.storage.database_size_bytes = compacted_size;
        paths.push(write_variant_result(
            &bulk_variant,
            outcome,
            bulk_initial,
            0,
            result_context,
        )?);
    }

    for variant in selected
        .iter()
        .filter(|variant| variant.workload.kind != WorkloadKind::BulkLoad)
    {
        let (outcome, initial, initial_size) = execute_rocks_variant(
            variant,
            &path,
            Arc::clone(&store),
            Arc::clone(&store_metrics),
            fresh_process_variants,
            result_context.args,
        )
        .await?;
        paths.push(write_variant_result(
            variant,
            outcome,
            initial,
            initial_size,
            result_context,
        )?);
    }
    delete_prefix(store, &path).await?;
    Ok(paths)
}

fn bulk_load_settings(suite_settings: &Settings) -> Settings {
    let mut settings = suite_settings.clone();
    settings.wal_enabled = false;
    settings.compactor_options = None;
    settings.l0_max_ssts = u32::MAX as usize;
    settings.l0_max_ssts_per_key = u32::MAX as usize;
    settings
}

async fn execute_rocks_variant(
    variant: &VariantConfig,
    database_path: &Path,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<StoreMetrics>,
    fresh_process: bool,
    args: &RunArgs,
) -> Result<(WorkloadOutcome, InitialState, u64)> {
    let admin = AdminBuilder::new(database_path.clone(), Arc::clone(&store)).build();
    let manifest = admin
        .read_manifest(None)
        .await
        .context("reading RocksDB suite manifest")?
        .context("RocksDB suite manifest does not exist")?;
    let initial = InitialState {
        checkpoint_id: None,
        manifest_id: Some(manifest.id()),
        lsm_digest_sha256: manifest_lsm_digest(&manifest)?,
    };
    let initial_database_bytes = live_database_size_bytes(&manifest);

    let mut outcome = if fresh_process {
        execute_variant_in_fresh_process(variant, database_path, &initial.lsm_digest_sha256, args)
            .await?
    } else {
        let object_store_cache = object_store_cache_directory(&variant.suite)?;
        let recorder = Arc::new(BenchmarkMetricsRecorder::new());
        let db = open_database_with_object_store_cache(
            database_path.clone(),
            Arc::clone(&store),
            &variant.suite,
            &variant.slate_settings,
            object_store_cache.as_ref().map(|cache| cache.path()),
            Arc::clone(&recorder),
        )
        .await?;
        if lsm_digest(&db)? != initial.lsm_digest_sha256 {
            bail!("reopened RocksDB suite database has an unexpected LSM state");
        }
        let outcome = execute_variant(Arc::clone(&db), variant, store_metrics, recorder).await;
        db.close()
            .await
            .context("closing RocksDB suite variant database")?;
        outcome?
    };
    let final_manifest = admin
        .read_manifest(None)
        .await
        .context("reading final RocksDB suite manifest")?
        .context("final RocksDB suite manifest does not exist")?;
    outcome.storage.database_size_bytes = live_database_size_bytes(&final_manifest);
    Ok((outcome, initial, initial_database_bytes))
}

fn enabled_features() -> Vec<String> {
    env!("BENCHMARK_ENABLED_FEATURES")
        .split(',')
        .filter(|feature| !feature.is_empty())
        .map(str::to_string)
        .collect()
}

fn write_variant_result(
    variant: &VariantConfig,
    mut outcome: WorkloadOutcome,
    initial_state: InitialState,
    initial_database_bytes: u64,
    context: &ResultContext<'_>,
) -> Result<String> {
    let version = env!("BENCHMARK_SLATE_VERSION");
    let relative_directory = PathBuf::from("results")
        .join(version)
        .join(&variant.suite.name)
        .join(&variant.workload.name)
        .join(&variant.variant);
    let directory = context.args.output.join(&relative_directory);
    fs::create_dir_all(&directory)?;
    let mut histograms = outcome.histograms.clone();
    histograms
        .histograms
        .extend(context.baseline_histograms.clone());
    let average_database_bytes = average_database_bytes(
        &outcome.timeseries,
        outcome.elapsed_ns,
        initial_database_bytes,
        outcome.storage.database_size_bytes,
    );
    outcome.storage.average_database_size_bytes = average_database_bytes;
    let result = ResultRecord {
        identity: Identity {
            slate_version: version.to_string(),
            slate_commit: env!("BENCHMARK_SLATE_COMMIT").to_string(),
            runner_version: env!("CARGO_PKG_VERSION").to_string(),
            runner_commit: env!("BENCHMARK_RUNNER_COMMIT").to_string(),
            lockfile_sha256: env!("BENCHMARK_LOCK_HASH").to_string(),
            timestamp: Utc::now().to_rfc3339(),
            suite: variant.suite.name.clone(),
            workload: variant.workload.name.clone(),
            variant: variant.variant.clone(),
            mode: variant.suite.mode().to_string(),
        },
        elapsed_ns: outcome.elapsed_ns,
        environment: context.environment.clone(),
        object_store_baseline: context.baseline.clone(),
        configuration: BenchmarkConfiguration {
            clients: variant.clients,
            warmup_ns: variant.warmup_ms().saturating_mul(1_000_000),
            measurement_ns: variant.measurement_ms().saturating_mul(1_000_000),
            record_count: variant.record_count(),
            key_bytes: variant.key_bytes(),
            value_bytes: variant.value_bytes(),
            value_compression_ratio: variant.value_compression_ratio(),
            block_cache_bytes: variant.suite.block_cache_bytes,
            metadata_cache_bytes: Some(
                variant
                    .suite
                    .metadata_cache_bytes
                    .unwrap_or(slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY),
            ),
            object_store_cache_bytes: variant.suite.object_store_cache_bytes,
            sst_block_bytes: variant.suite.sst_block_bytes,
            slate_settings: serde_json::to_value(&variant.slate_settings)?,
            build_profile: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
            enabled_features: enabled_features(),
        },
        application: outcome.application,
        durability: outcome.durability,
        resources: outcome.resources,
        storage: outcome.storage,
        initial_state,
        source_files: SourceFiles {
            histograms: "histograms.json".to_string(),
            timeseries: "timeseries.json".to_string(),
        },
    };
    validate_result(
        &result,
        &histograms,
        &outcome.timeseries,
        &context.args.schema_dir,
    )?;
    write_json(&directory.join("result.json"), &result)?;
    write_json(&directory.join("histograms.json"), &histograms)?;
    write_compact_json(&directory.join("timeseries.json"), &outcome.timeseries)?;
    Ok(relative_directory.join("result.json").display().to_string())
}

async fn clone_database(
    store: Arc<dyn ObjectStore>,
    clone_path: &Path,
    parent_path: &Path,
    checkpoint_id: Uuid,
) -> Result<()> {
    let admin = AdminBuilder::new(clone_path.clone(), store).build();
    admin
        .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
            parent_path.clone(),
            checkpoint_id,
        ))
        .build()
        .await
        .context("creating shallow benchmark clone")
}

async fn open_database(
    path: Path,
    store: Arc<dyn ObjectStore>,
    suite: &SuiteConfig,
    settings: &Settings,
    recorder: Arc<BenchmarkMetricsRecorder>,
) -> Result<Arc<Db>> {
    open_database_with_object_store_cache(path, store, suite, settings, None, recorder).await
}

async fn open_database_with_object_store_cache(
    path: Path,
    store: Arc<dyn ObjectStore>,
    suite: &SuiteConfig,
    settings: &Settings,
    object_store_cache_root: Option<&FsPath>,
    recorder: Arc<BenchmarkMetricsRecorder>,
) -> Result<Arc<Db>> {
    let mut settings = settings.clone();
    settings.object_store_cache_options.root_folder =
        object_store_cache_root.map(FsPath::to_path_buf);
    settings.object_store_cache_options.max_cache_size_bytes = suite
        .object_store_cache_bytes
        .map(usize::try_from)
        .transpose()
        .context("object-store cache capacity exceeds the platform limit")?;
    let db_recorder: Arc<dyn MetricsRecorder> = recorder;
    let mut builder = Db::builder(path, store)
        .with_settings(settings)
        .with_metrics_recorder(db_recorder);
    builder = builder.with_db_cache(cache_for(suite));
    if suite.sst_block_bytes == Some(8192) {
        builder = builder.with_sst_block_size(SstBlockSize::Block8Kib);
    }
    Ok(Arc::new(builder.build().await.context("opening SlateDB")?))
}

fn cache_for(suite: &SuiteConfig) -> Arc<dyn DbCache> {
    let block_cache = suite.block_cache_bytes.map(|max_capacity| {
        Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity,
            ..Default::default()
        })) as Arc<dyn DbCache>
    });
    let metadata_capacity = suite
        .metadata_cache_bytes
        .unwrap_or(slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY);
    let metadata_cache = Some(Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: metadata_capacity,
        ..Default::default()
    })) as Arc<dyn DbCache>);
    Arc::new(
        SplitCache::new()
            .with_block_cache(block_cache)
            .with_meta_cache(metadata_cache)
            .build(),
    )
}

fn object_store_cache_directory(suite: &SuiteConfig) -> Result<Option<tempfile::TempDir>> {
    suite
        .object_store_cache_bytes
        .map(|_| {
            tempfile::Builder::new()
                .prefix("slatedb-benchmark-object-store-cache-")
                .tempdir()
                .context("creating temporary object-store cache")
        })
        .transpose()
}

async fn wait_for_compaction(
    db: &Db,
    recorder: &BenchmarkMetricsRecorder,
    suite: &SuiteConfig,
) -> Result<()> {
    let timeout = Duration::from_millis(suite.compaction_timeout_ms);
    let quiet = Duration::from_millis(suite.compaction_quiet_ms);
    let started = Instant::now();
    let mut stable_since = Instant::now();
    let mut manifest_id = db.status().current_manifest.id();
    loop {
        if started.elapsed() > timeout {
            bail!("timed out waiting for compaction to become idle");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let current = db.status().current_manifest.id();
        if current != manifest_id {
            manifest_id = current;
            stable_since = Instant::now();
        }
        let running = recorder
            .snapshot()
            .by_name(slatedb::compactor::stats::RUNNING_COMPACTIONS)
            .iter()
            .filter_map(|metric| match metric.value {
                MetricValue::Gauge(value) | MetricValue::UpDownCounter(value) => Some(value),
                _ => None,
            })
            .sum::<i64>();
        if running == 0 && stable_since.elapsed() >= quiet {
            return Ok(());
        }
    }
}

fn lsm_digest(db: &Db) -> Result<String> {
    let status = db.status();
    manifest_lsm_digest(&status.current_manifest)
}

fn manifest_lsm_digest(manifest: &VersionedManifest) -> Result<String> {
    let layout = serde_json::json!({
        "last_compacted_l0_sst_view_id": manifest.last_compacted_l0_sst_view_id(),
        "last_compacted_l0_sst_id": manifest.last_compacted_l0_sst_id(),
        "l0": manifest.l0(),
        "compacted": manifest.compacted(),
        "segments": manifest.segments(),
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&layout)?)
    ))
}

fn settings_digest(settings: &Settings) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(settings)?)
    ))
}

fn write_json(path: &FsPath, value: &impl Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("writing {}", path.display()))
}

fn write_compact_json(path: &FsPath, value: &impl Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec(value)?)
        .with_context(|| format!("writing {}", path.display()))
}

fn average_database_bytes(
    timeseries: &TimeseriesFile,
    elapsed_ns: u64,
    initial_bytes: u64,
    final_bytes: u64,
) -> u64 {
    if elapsed_ns == 0 || timeseries.samples.len() < 2 {
        return initial_bytes.saturating_add(final_bytes) / 2;
    }
    let mut byte_nanoseconds = 0_f64;
    for window in timeseries.samples.windows(2) {
        let left = &window[0];
        let right = &window[1];
        let duration = right.offset_ns.saturating_sub(left.offset_ns) as f64;
        let average = (left.database_size_bytes as f64 + right.database_size_bytes as f64) / 2.0;
        byte_nanoseconds += average * duration;
    }
    let last = &timeseries.samples[timeseries.samples.len() - 1];
    if last.offset_ns < elapsed_ns {
        byte_nanoseconds += final_bytes as f64 * elapsed_ns.saturating_sub(last.offset_ns) as f64;
    }
    (byte_nanoseconds / elapsed_ns as f64).max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::{
        average_database_bytes, bulk_load_settings, close_database_after, enabled_features,
        execute_rocks_variant, lsm_digest, object_store_cache_directory, open_database,
    };
    use crate::cli::RunArgs;
    use crate::config::{
        BenchmarkConfig, ProbeConfig, SuiteConfig, SuiteExecution, VariantConfig,
        VariantDefinition, WorkloadConfig, WorkloadKind,
    };
    use crate::instrumented_store::InstrumentedStore;
    use crate::model::{TimeseriesFile, TimeseriesSample};
    use crate::system::BenchmarkMetricsRecorder;
    use crate::workloads::populate_dataset;
    use anyhow::Result;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb::config::{PutOptions, Settings, WriteOptions};
    use slatedb::Db;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Arc;

    #[test]
    fn database_size_uses_trapezoidal_time_integration() {
        let timeseries = TimeseriesFile {
            interval_ns: 1_000_000_000,
            application_windows: Vec::new(),
            durability_windows: None,
            slatedb_metrics: Vec::new(),
            samples: vec![
                TimeseriesSample {
                    offset_ns: 0,
                    database_size_bytes: 100,
                    ..Default::default()
                },
                TimeseriesSample {
                    offset_ns: 1_000_000_000,
                    database_size_bytes: 300,
                    ..Default::default()
                },
                TimeseriesSample {
                    offset_ns: 2_000_000_000,
                    database_size_bytes: 500,
                    ..Default::default()
                },
            ],
        };
        assert_eq!(
            average_database_bytes(&timeseries, 2_000_000_000, 0, 500),
            300
        );
    }

    #[test]
    fn bulk_load_settings_disable_durability_and_l0_backpressure() {
        let suite_settings = Settings::default();
        let settings = bulk_load_settings(&suite_settings);

        assert!(!settings.wal_enabled);
        assert!(settings.compactor_options.is_none());
        assert_eq!(settings.l0_max_ssts, u32::MAX as usize);
        assert_eq!(settings.l0_max_ssts_per_key, u32::MAX as usize);
        assert!(suite_settings.wal_enabled);
        assert!(suite_settings.compactor_options.is_some());
    }

    #[test]
    fn enabled_features_match_explicit_slatedb_dependency_features() {
        let manifest: toml::Value =
            toml::from_str(include_str!("../Cargo.toml")).expect("parse Cargo.toml");
        let dependency = manifest["dependencies"]["slatedb"]
            .as_table()
            .expect("slatedb dependency table");
        assert_eq!(
            dependency["default-features"].as_bool(),
            Some(false),
            "SlateDB defaults must not hide enabled features"
        );
        let mut expected = dependency["features"]
            .as_array()
            .expect("slatedb features")
            .iter()
            .map(|feature| feature.as_str().expect("feature string").to_string())
            .collect::<Vec<_>>();
        expected.sort();
        expected.dedup();

        assert_eq!(enabled_features(), expected);
    }

    #[test]
    fn runner_commit_matches_checked_out_revision() {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .expect("run git rev-parse");
        if output.status.success() {
            assert_eq!(
                env!("BENCHMARK_RUNNER_COMMIT"),
                String::from_utf8_lossy(&output.stdout).trim()
            );
        }
    }

    #[test]
    fn object_store_cache_directory_is_temporary() -> Result<()> {
        let benchmark = BenchmarkConfig::load_from(std::path::Path::new("config"))?;
        let suite = benchmark
            .suites
            .iter()
            .find(|suite| suite.name == "smoke")
            .expect("smoke suite");
        let path = {
            let cache = object_store_cache_directory(suite)?.expect("object-store cache");
            let path = cache.path().to_path_buf();
            assert!(path.is_dir());
            path
        };
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn lsm_digest_changes_with_persisted_tree() -> Result<()> {
        let settings = Settings {
            compactor_options: None,
            flush_interval: None,
            wal_enabled: false,
            ..Default::default()
        };
        let db = Db::builder("digest-test", Arc::new(InMemory::new()))
            .with_settings(settings)
            .build()
            .await?;
        let write_options = WriteOptions {
            await_durable: false,
            ..Default::default()
        };
        let empty_digest = lsm_digest(&db)?;

        for index in 0..64 {
            db.put_with_options(
                format!("key-{index:04}"),
                b"value",
                &PutOptions::default(),
                &write_options,
            )
            .await?;
        }
        db.flush().await?;
        let sixty_four_record_digest = lsm_digest(&db)?;

        for index in 64..160 {
            db.put_with_options(
                format!("key-{index:04}"),
                b"value",
                &PutOptions::default(),
                &write_options,
            )
            .await?;
        }
        db.flush().await?;
        let hundred_sixty_record_digest = lsm_digest(&db)?;

        assert_ne!(empty_digest, sixty_four_record_digest);
        assert_ne!(sixty_four_record_digest, hundred_sixty_record_digest);
        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn database_is_closed_when_operation_fails() -> Result<()> {
        let db = Db::open("close-after-error-test", Arc::new(InMemory::new())).await?;
        let operation = Err(anyhow::anyhow!("operation failed"));

        let error = close_database_after::<()>(&db, operation, "closing test database")
            .await
            .expect_err("operation should fail");

        assert_eq!(error.to_string(), "operation failed");
        assert!(db.put(b"key", b"value").await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_rocks_variants_reopen_and_carry_forward_state() -> Result<()> {
        let overwrite = WorkloadConfig {
            name: "overwrite".to_string(),
            kind: WorkloadKind::Overwrite,
            variants: vec![VariantDefinition {
                name: "clients-1".to_string(),
                clients: 1,
            }],
            await_durable: false,
            record_count: None,
            key_bytes: None,
            value_bytes: None,
            warmup_ms: None,
            measurement_ms: None,
        };
        let random_read = WorkloadConfig {
            name: "random-read".to_string(),
            kind: WorkloadKind::RandomRead,
            variants: vec![VariantDefinition {
                name: "clients-1".to_string(),
                clients: 1,
            }],
            await_durable: false,
            record_count: None,
            key_bytes: None,
            value_bytes: None,
            warmup_ms: None,
            measurement_ms: None,
        };
        let suite = SuiteConfig {
            name: "rocksdb".to_string(),
            release: false,
            execution: SuiteExecution::Sequential,
            object_store_probe: ProbeConfig {
                latency_operations: 1,
                latency_object_bytes: 1,
                throughput_object_bytes: 1,
                throughput_concurrency: 1,
                throughput_warmup_ms: 0,
                throughput_measurement_ms: 1,
            },
            compaction_quiet_ms: 10,
            compaction_timeout_ms: 1_000,
            record_count: 8,
            key_bytes: 16,
            value_bytes: 64,
            value_compression_ratio: 1.0,
            block_cache_bytes: Some(1024 * 1024),
            metadata_cache_bytes: Some(1024 * 1024),
            object_store_cache_bytes: None,
            warmup_ms: 0,
            measurement_ms: 200,
            sst_block_bytes: None,
            workloads: vec![overwrite.clone(), random_read.clone()],
        };
        let slate_settings = Settings {
            flush_interval: Some(std::time::Duration::from_millis(10)),
            ..Default::default()
        };
        let variant = |workload| VariantConfig {
            suite: suite.clone(),
            workload,
            variant: "clients-1".to_string(),
            clients: 1,
            slate_settings: slate_settings.clone(),
        };
        let args = RunArgs {
            suite: None,
            session: None,
            workload: None,
            variant: None,
            output: PathBuf::new(),
            config_dir: PathBuf::from("config"),
            schema_dir: PathBuf::from("schema"),
        };
        let path = Path::from("rocks-reopen-test");
        let instrumented = Arc::new(InstrumentedStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = instrumented.clone();
        let recorder = Arc::new(BenchmarkMetricsRecorder::new());
        let db = open_database(
            path.clone(),
            Arc::clone(&store),
            &suite,
            &slate_settings,
            recorder,
        )
        .await?;
        populate_dataset(Arc::clone(&db), 8, 16, 64, 1.0, false).await?;
        db.close().await?;

        let (first, before_overwrite, initial_overwrite_size) = execute_rocks_variant(
            &variant(overwrite),
            &path,
            Arc::clone(&store),
            instrumented.metrics(),
            false,
            &args,
        )
        .await?;
        let (second, before_read, initial_read_size) = execute_rocks_variant(
            &variant(random_read),
            &path,
            store,
            instrumented.metrics(),
            false,
            &args,
        )
        .await?;

        assert!(first.application.successful_operations > 0);
        assert!(second.application.successful_operations > 0);
        assert_eq!(second.application.errors, 0);
        assert_ne!(
            before_overwrite.lsm_digest_sha256,
            before_read.lsm_digest_sha256
        );
        assert!(initial_overwrite_size > 0);
        assert_eq!(first.storage.database_size_bytes, initial_read_size);
        Ok(())
    }
}
