use crate::cli::{RunArgs, WorkerArgs};
use crate::config::{BenchmarkConfig, SuiteConfig, SuiteExecution, VariantConfig, WorkloadKind};
use crate::cost::PriceTable;
use crate::model::{
    BenchmarkConfiguration, Identity, InitialState, ResultRecord, RunManifest, SourceFiles,
};
use crate::object_store_probe::{delete_prefix, probe, ObjectStoreContext};
use crate::system::{inspect_environment, verify_environment, BenchmarkMetricsRecorder};
use crate::validation::{validate_result, validate_run};
use crate::workloads::{
    execute_variant, extend_with_compaction_phase, populate_dataset, prepare_bulk_load,
    WorkloadOutcome,
};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use futures::TryStreamExt;
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
use slatedb_common::metrics::{MetricValue, MetricsRecorder};
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
    cleanup_paths: Vec<Path>,
}

pub async fn execute(args: RunArgs) -> Result<()> {
    if args.output.exists() {
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

    let prices = PriceTable::load(&args.schema_dir)?;
    let object_store = ObjectStoreContext::load()?;
    let environment = inspect_environment(
        &object_store.provider,
        &object_store.endpoint,
        &object_store.region,
    );

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
        result_paths.extend(
            execute_suite(
                variants,
                &benchmark,
                &object_store,
                &environment,
                &baseline,
                &baseline_histograms,
                &prices,
                args,
            )
            .await?,
        );
        object_store_baselines.insert(suite_name, baseline);
    }

    let run = RunManifest {
        schema_version: 1,
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

#[allow(clippy::too_many_arguments)]
async fn execute_suite(
    selected: Vec<VariantConfig>,
    benchmark: &BenchmarkConfig,
    object_store: &ObjectStoreContext,
    environment: &crate::model::Environment,
    baseline: &crate::model::ObjectStoreBaseline,
    baseline_histograms: &BTreeMap<String, crate::model::EncodedHistogram>,
    prices: &PriceTable,
    args: &RunArgs,
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
        cleanup_paths: Vec::new(),
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
                environment,
                baseline,
                baseline_histograms,
                prices,
                args,
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
            clone_database(
                Arc::clone(&store),
                &clone_path,
                &golden.path,
                golden.checkpoint_id,
            )
            .await?;
            golden_manager.cleanup_paths.push(clone_path.clone());

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
            db.close()
                .await
                .context("closing clone validation handle")?;
            let mut outcome = if object_store.provider.eq_ignore_ascii_case("memory") {
                let recorder = Arc::new(BenchmarkMetricsRecorder::new());
                let db = open_database(
                    clone_path.clone(),
                    Arc::clone(&store),
                    &variant.suite,
                    &variant.slate_settings,
                    Arc::clone(&recorder),
                )
                .await?;
                let outcome = execute_variant(
                    Arc::clone(&db),
                    variant,
                    object_store.instrumented.metrics(),
                    recorder,
                    clone_path.clone(),
                    golden.size_bytes,
                )
                .await;
                db.close().await.context("closing benchmark clone")?;
                outcome?
            } else {
                execute_variant_in_fresh_process(
                    variant,
                    &clone_path,
                    golden.size_bytes,
                    &golden.lsm_digest,
                    args,
                )
                .await?
            };
            let clone_size = prefix_size(Arc::clone(&store), &clone_path).await?;
            outcome.storage.database_size_bytes = golden.size_bytes.saturating_add(clone_size);
            result_paths.push(write_variant_result(
                variant,
                outcome,
                initial,
                environment,
                baseline,
                baseline_histograms,
                prices,
                golden.size_bytes,
                args,
            )?);
            delete_prefix(Arc::clone(&store), &clone_path).await?;
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
    let db = open_database(
        database_path.clone(),
        store,
        &variant.suite,
        &variant.slate_settings,
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
        database_path,
        args.shared_database_bytes,
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
    shared_database_bytes: u64,
    expected_lsm_digest: &str,
    args: &RunArgs,
) -> Result<WorkloadOutcome> {
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
        .arg("--shared-database-bytes")
        .arg(shared_database_bytes.to_string())
        .arg("--expected-lsm-digest")
        .arg(expected_lsm_digest)
        .arg("--output")
        .arg(&output)
        .arg("--config-dir")
        .arg(&args.config_dir);
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
        let prefix_layout = variant.workload.kind == WorkloadKind::PrefixScan;
        let record_count = if variant.workload.kind == WorkloadKind::SustainedIngest {
            0
        } else {
            variant.record_count()
        };
        let key = format!(
            "{}-{}-{}-{}-{}-{}",
            if prefix_layout { "prefix" } else { "records" },
            record_count,
            variant.key_bytes(),
            variant.value_bytes(),
            variant.suite.sst_block_bytes.unwrap_or_default(),
            settings_digest(&variant.slate_settings)?
        );
        if let Some(golden) = self.values.get(&key) {
            return Ok(golden.clone());
        }
        let path = self.root.clone().join("golden").join(key.as_str());
        tracing::info!(
            dataset = key,
            records = record_count,
            "preparing golden database"
        );
        let recorder = Arc::new(BenchmarkMetricsRecorder::new());
        let db = open_database(
            path.clone(),
            Arc::clone(&self.store),
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
            prefix_layout,
        )
        .await?;
        wait_for_compaction(&db, &recorder, &variant.suite).await?;
        db.close().await.context("closing golden database")?;

        let admin = AdminBuilder::new(path.clone(), Arc::clone(&self.store)).build();
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
        let digest = manifest_lsm_digest(&checkpoint_manifest)?;
        let size_bytes = prefix_size(Arc::clone(&self.store), &path).await?;
        let golden = GoldenDatabase {
            path: path.clone(),
            checkpoint_id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
            lsm_digest: digest,
            size_bytes,
        };
        self.cleanup_paths.push(path);
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
        }
        for path in &self.cleanup_paths {
            delete_prefix(Arc::clone(&self.store), path).await?;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_sequential_suite(
    selected: Vec<VariantConfig>,
    benchmark: &BenchmarkConfig,
    run_root: &Path,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<crate::instrumented_store::StoreMetrics>,
    fresh_process_variants: bool,
    environment: &crate::model::Environment,
    baseline: &crate::model::ObjectStoreBaseline,
    baseline_histograms: &BTreeMap<String, crate::model::EncodedHistogram>,
    prices: &PriceTable,
    args: &RunArgs,
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
    let bulk_recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let bulk_db = open_database(
        path.clone(),
        Arc::clone(&store),
        &suite,
        &bulk_variant.slate_settings,
        Arc::clone(&bulk_recorder),
    )
    .await?;
    let bulk_initial = InitialState {
        checkpoint_id: None,
        manifest_id: Some(bulk_db.status().current_manifest.id()),
        lsm_digest_sha256: lsm_digest(&bulk_db)?,
    };
    let selected_bulk = selected
        .iter()
        .any(|variant| variant.workload.kind == WorkloadKind::BulkLoad);
    let mut bulk_outcome = if selected_bulk {
        Some(
            execute_variant(
                Arc::clone(&bulk_db),
                &bulk_variant,
                Arc::clone(&store_metrics),
                Arc::clone(&bulk_recorder),
                path.clone(),
                0,
            )
            .await?,
        )
    } else {
        prepare_bulk_load(Arc::clone(&bulk_db), &bulk_variant).await?;
        bulk_db.flush().await?;
        None
    };
    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let compaction_measurement = bulk_outcome.as_ref().map(|outcome| {
        let started = Instant::now();
        let start_store = store_metrics.snapshot();
        let start_slate = recorder.snapshot();
        let counters = Arc::new(crate::system::ApplicationCounters::default());
        counters
            .operations
            .store(outcome.application.total_operations, Ordering::Relaxed);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let sampler = tokio::spawn(crate::system::sample_until_stopped(
            started,
            counters,
            Arc::clone(&store_metrics),
            Arc::clone(&recorder),
            path.clone(),
            0,
            stop_rx,
        ));
        (started, start_store, start_slate, stop_tx, sampler)
    });
    bulk_db
        .close()
        .await
        .context("closing bulk-load database")?;

    let suite_settings = benchmark.slate_settings(&suite)?;
    let compaction_db = open_database(
        path.clone(),
        Arc::clone(&store),
        &suite,
        &suite_settings,
        Arc::clone(&recorder),
    )
    .await?;
    wait_for_compaction(&compaction_db, &recorder, &suite).await?;
    if let (Some(outcome), Some((started, start_store, start_slate, stop_tx, sampler))) =
        (bulk_outcome.as_mut(), compaction_measurement)
    {
        let _ = stop_tx.send(true);
        let sampled = sampler.await.context("joining bulk compaction sampler")??;
        let elapsed = started.elapsed();
        let store_delta = store_metrics.snapshot().difference(&start_store);
        let end_slate = recorder.snapshot();
        extend_with_compaction_phase(
            outcome,
            sampled.samples,
            store_delta,
            &start_slate,
            &end_slate,
            elapsed,
        )?;
    }
    compaction_db
        .close()
        .await
        .context("closing post-bulk compaction database")?;
    let compacted_size = prefix_size(Arc::clone(&store), &path).await?;
    let mut paths = Vec::new();
    if let Some(mut outcome) = bulk_outcome.take() {
        outcome.storage.database_size_bytes = compacted_size;
        paths.push(write_variant_result(
            &bulk_variant,
            outcome,
            bulk_initial,
            environment,
            baseline,
            baseline_histograms,
            prices,
            0,
            args,
        )?);
    }

    for variant in selected
        .iter()
        .filter(|variant| variant.workload.kind != WorkloadKind::BulkLoad)
    {
        let initial_size = prefix_size(Arc::clone(&store), &path).await?;
        let (mut outcome, initial) = execute_rocks_variant(
            variant,
            &path,
            Arc::clone(&store),
            Arc::clone(&store_metrics),
            fresh_process_variants,
            args,
        )
        .await?;
        outcome.storage.database_size_bytes = prefix_size(Arc::clone(&store), &path).await?;
        paths.push(write_variant_result(
            variant,
            outcome,
            initial,
            environment,
            baseline,
            baseline_histograms,
            prices,
            initial_size,
            args,
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
    store_metrics: Arc<crate::instrumented_store::StoreMetrics>,
    fresh_process: bool,
    args: &RunArgs,
) -> Result<(WorkloadOutcome, InitialState)> {
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

    let outcome = if fresh_process {
        execute_variant_in_fresh_process(
            variant,
            database_path,
            0,
            &initial.lsm_digest_sha256,
            args,
        )
        .await?
    } else {
        let recorder = Arc::new(BenchmarkMetricsRecorder::new());
        let db = open_database(
            database_path.clone(),
            store,
            &variant.suite,
            &variant.slate_settings,
            Arc::clone(&recorder),
        )
        .await?;
        if lsm_digest(&db)? != initial.lsm_digest_sha256 {
            bail!("reopened RocksDB suite database has an unexpected LSM state");
        }
        let outcome = execute_variant(
            Arc::clone(&db),
            variant,
            store_metrics,
            recorder,
            database_path.clone(),
            0,
        )
        .await;
        db.close()
            .await
            .context("closing RocksDB suite variant database")?;
        outcome?
    };
    Ok((outcome, initial))
}

#[allow(clippy::too_many_arguments)]
fn write_variant_result(
    variant: &VariantConfig,
    outcome: WorkloadOutcome,
    initial_state: InitialState,
    environment: &crate::model::Environment,
    baseline: &crate::model::ObjectStoreBaseline,
    baseline_histograms: &BTreeMap<String, crate::model::EncodedHistogram>,
    prices: &PriceTable,
    initial_database_bytes: u64,
    args: &RunArgs,
) -> Result<String> {
    let version = env!("BENCHMARK_SLATE_VERSION");
    let relative_directory = PathBuf::from("results")
        .join(version)
        .join(&variant.suite.name)
        .join(&variant.workload.name)
        .join(&variant.variant);
    let directory = args.output.join(&relative_directory);
    fs::create_dir_all(&directory)?;
    let mut histograms = outcome.histograms.clone();
    histograms.histograms.extend(baseline_histograms.clone());
    let average_database_bytes = average_database_bytes(
        &outcome.timeseries,
        outcome.elapsed_ns,
        initial_database_bytes,
        outcome.storage.database_size_bytes,
    );
    let cost = prices.estimate(
        outcome.elapsed_ns,
        average_database_bytes,
        &outcome.storage,
        outcome.application.successful_operations,
    );
    let result = ResultRecord {
        schema_version: 1,
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
        environment: environment.clone(),
        object_store_baseline: baseline.clone(),
        configuration: BenchmarkConfiguration {
            clients: variant.clients,
            target_rate: variant.target_rate,
            warmup_ns: variant.warmup_ms().saturating_mul(1_000_000),
            measurement_ns: variant.measurement_ms().saturating_mul(1_000_000),
            record_count: variant.record_count(),
            key_bytes: variant.key_bytes(),
            value_bytes: variant.value_bytes(),
            block_cache_bytes: variant.suite.block_cache_bytes,
            metadata_cache_bytes: Some(
                variant
                    .suite
                    .metadata_cache_bytes
                    .unwrap_or(slatedb::db_cache::DEFAULT_META_CACHE_CAPACITY),
            ),
            sst_block_bytes: variant.suite.sst_block_bytes,
            slate_settings: serde_json::to_value(&variant.slate_settings)?,
            build_profile: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
            enabled_features: vec![
                "aws".to_string(),
                "foyer".to_string(),
                "wal_disable".to_string(),
                "zstd".to_string(),
            ],
        },
        application: outcome.application,
        durability: outcome.durability,
        resources: outcome.resources,
        storage: outcome.storage,
        cost,
        initial_state,
        source_files: SourceFiles {
            histograms: "histograms.json".to_string(),
            timeseries: "timeseries.json".to_string(),
        },
    };
    validate_result(&result, &histograms, &outcome.timeseries, &args.schema_dir)?;
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
    let db_recorder: Arc<dyn MetricsRecorder> = recorder;
    let mut builder = Db::builder(path, store)
        .with_settings(settings.clone())
        .with_metrics_recorder(db_recorder);
    if let Some(cache) = cache_for(suite) {
        builder = builder.with_db_cache(cache);
    }
    if suite.sst_block_bytes == Some(8192) {
        builder = builder.with_sst_block_size(SstBlockSize::Block8Kib);
    }
    Ok(Arc::new(builder.build().await.context("opening SlateDB")?))
}

fn cache_for(suite: &SuiteConfig) -> Option<Arc<dyn DbCache>> {
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
    Some(Arc::new(
        SplitCache::new()
            .with_block_cache(block_cache)
            .with_meta_cache(metadata_cache)
            .build(),
    ))
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

async fn prefix_size(store: Arc<dyn ObjectStore>, path: &Path) -> Result<u64> {
    store
        .list(Some(path))
        .map_ok(|meta| meta.size)
        .try_fold(0_u64, |total, size| async move {
            Ok(total.saturating_add(size))
        })
        .await
        .context("measuring database size")
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
    timeseries: &crate::model::TimeseriesFile,
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
        average_database_bytes, bulk_load_settings, execute_rocks_variant, lsm_digest,
        open_database,
    };
    use crate::cli::RunArgs;
    use crate::config::{
        ProbeConfig, SuiteConfig, SuiteExecution, VariantConfig, VariantDefinition, WorkloadConfig,
        WorkloadKind,
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
    use std::sync::Arc;

    #[test]
    fn database_size_uses_trapezoidal_time_integration() {
        let timeseries = TimeseriesFile {
            schema_version: 1,
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
    async fn in_memory_rocks_variants_reopen_and_carry_forward_state() -> Result<()> {
        let overwrite = WorkloadConfig {
            name: "overwrite".to_string(),
            kind: WorkloadKind::Overwrite,
            variants: vec![VariantDefinition {
                name: "clients-1".to_string(),
                clients: Some(1),
                target_rate: None,
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
                clients: Some(1),
                target_rate: None,
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
            block_cache_bytes: Some(1024 * 1024),
            metadata_cache_bytes: Some(1024 * 1024),
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
            clients: Some(1),
            target_rate: None,
            slate_settings: slate_settings.clone(),
        };
        let args = RunArgs {
            suite: None,
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
        populate_dataset(Arc::clone(&db), 8, 16, 64, false).await?;
        db.close().await?;

        let (first, before_overwrite) = execute_rocks_variant(
            &variant(overwrite),
            &path,
            Arc::clone(&store),
            instrumented.metrics(),
            false,
            &args,
        )
        .await?;
        let (second, before_read) = execute_rocks_variant(
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
        Ok(())
    }
}
