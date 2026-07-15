use super::{
    bulk_load_settings, clone_database, execute_isolated_variant, execute_rocks_variant,
    golden_key, lsm_digest, manifest_lsm_digest, object_store_cache_directory,
    open_database_with_object_store_cache, prefix_size, prepare_golden_database,
    wait_for_compaction, write_json, write_variant_result, GoldenDatabase, ResultContext,
};
use crate::cli::RunArgs;
use crate::config::{BenchmarkConfig, SuiteConfig, SuiteExecution, VariantConfig, WorkloadKind};
use crate::model::{EncodedHistogram, Environment, InitialState, ObjectStoreBaseline, RunManifest};
use crate::object_store_probe::{delete_prefix, probe, ObjectStoreContext};
use crate::system::{sample_until_stopped, ApplicationCounters, BenchmarkMetricsRecorder};
use crate::validation::{validate_output, validate_run};
use crate::workloads::{execute_variant, extend_with_compaction_phase};
use anyhow::{bail, ensure, Context, Result};
use chrono::Utc;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::admin::AdminBuilder;
use slatedb::config::CheckpointOptions;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path as FsPath, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

const STATE_FILE: &str = "resume.json";
const OUTPUT_MARKER: &str = ".benchmark-session";
const RESULT_FILES: [&str; 3] = ["result.json", "histograms.json", "timeseries.json"];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionState {
    session: String,
    suite: String,
    started_at: String,
    slate_version: String,
    slate_commit: String,
    runner_commit: String,
    lockfile_sha256: String,
    configuration_sha256: String,
    environment: Environment,
    object_store_baseline: ObjectStoreBaseline,
    baseline_histograms: BTreeMap<String, EncodedHistogram>,
    completed: Vec<CompletedWorkload>,
    sequential_databases: Vec<DatabaseCheckpoint>,
    goldens: BTreeMap<String, DatabaseCheckpoint>,
    measurement_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompletedWorkload {
    name: String,
    results: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabaseCheckpoint {
    path: String,
    checkpoint_id: Uuid,
    manifest_id: u64,
    lsm_digest_sha256: String,
    size_bytes: u64,
}

impl DatabaseCheckpoint {
    fn path(&self) -> Path {
        Path::from(self.path.as_str())
    }

    fn from_golden(golden: &GoldenDatabase) -> Self {
        Self {
            path: golden.path.to_string(),
            checkpoint_id: golden.checkpoint_id,
            manifest_id: golden.manifest_id,
            lsm_digest_sha256: golden.lsm_digest.clone(),
            size_bytes: golden.size_bytes,
        }
    }

    fn to_golden(&self) -> GoldenDatabase {
        GoldenDatabase {
            path: self.path(),
            checkpoint_id: self.checkpoint_id,
            manifest_id: self.manifest_id,
            lsm_digest: self.lsm_digest_sha256.clone(),
            size_bytes: self.size_bytes,
        }
    }
}

pub(super) async fn execute(
    args: &RunArgs,
    benchmark: &BenchmarkConfig,
    requested: Vec<VariantConfig>,
    object_store: &ObjectStoreContext,
    environment: &Environment,
) -> Result<()> {
    let session = args.session.as_deref().context("session name is missing")?;
    validate_session_name(session)?;
    let suite_name = args
        .suite
        .as_deref()
        .context("resumable sessions require --suite")?;
    let workload_name = args
        .workload
        .as_deref()
        .context("resumable sessions require --workload")?;
    ensure!(
        args.variant.is_none(),
        "resumable sessions execute complete workloads, not individual variants"
    );
    ensure!(
        requested
            .iter()
            .all(|variant| variant.workload.name == workload_name),
        "session selection contains more than one workload"
    );
    let all_variants = benchmark.select(Some(suite_name), None, None)?;
    let suite = &all_variants
        .first()
        .context("session suite contains no variants")?
        .suite;
    ensure_output_directory(&args.output, session, suite_name)?;
    crate::system::verify_environment(environment, !suite.release)?;

    let session_root = object_store.root.clone().join("sessions").join(session);
    let state_path = session_root.clone().join(STATE_FILE);
    let configuration_sha256 = configuration_digest(suite, &all_variants[0])?;
    let mut state = match load_state(Arc::clone(&object_store.raw), &state_path).await? {
        Some(state) => {
            validate_state(&state, session, suite, &configuration_sha256, environment)?;
            state
        }
        None => {
            tracing::info!(session, suite = suite_name, "creating benchmark session");
            let (object_store_baseline, baseline_histograms) = probe(
                Arc::clone(&object_store.raw),
                &session_root.clone().join("probe"),
                &suite.object_store_probe,
            )
            .await?;
            let state = SessionState {
                session: session.to_string(),
                suite: suite_name.to_string(),
                started_at: Utc::now().to_rfc3339(),
                slate_version: env!("BENCHMARK_SLATE_VERSION").to_string(),
                slate_commit: env!("BENCHMARK_SLATE_COMMIT").to_string(),
                runner_commit: env!("BENCHMARK_RUNNER_COMMIT").to_string(),
                lockfile_sha256: env!("BENCHMARK_LOCK_HASH").to_string(),
                configuration_sha256,
                environment: environment.clone(),
                object_store_baseline,
                baseline_histograms,
                completed: Vec::new(),
                sequential_databases: Vec::new(),
                goldens: BTreeMap::new(),
                measurement_complete: false,
            };
            save_state(Arc::clone(&object_store.raw), &state_path, &state).await?;
            state
        }
    };

    hydrate_results(
        Arc::clone(&object_store.raw),
        &session_root,
        &args.output,
        &state.completed,
    )
    .await?;
    write_json(
        &args.output.join("object-store.json"),
        &state.object_store_baseline,
    )?;

    if state
        .completed
        .iter()
        .any(|completed| completed.name == workload_name)
    {
        tracing::info!(
            session,
            workload = workload_name,
            "restored completed workload"
        );
        write_run_manifest(args, benchmark, suite_name, &state)?;
        cleanup_completed_session(&mut state, object_store, &session_root, &state_path).await?;
        print_success(args);
        return Ok(());
    }

    let next = suite
        .workloads
        .get(state.completed.len())
        .context("session already completed every configured workload")?;
    ensure!(
        next.name == workload_name,
        "session expects workload {}, not {}",
        next.name,
        workload_name
    );
    clear_local_workload_output(&args.output, suite_name, workload_name)?;

    let baseline = state.object_store_baseline.clone();
    let baseline_histograms = state.baseline_histograms.clone();
    let result_context = ResultContext {
        environment,
        baseline: &baseline,
        baseline_histograms: &baseline_histograms,
        args,
    };
    let store: Arc<dyn ObjectStore> = object_store.instrumented.clone();
    let stage_root = session_root
        .clone()
        .join("databases")
        .join("stages")
        .join(format!("{:03}-{}", state.completed.len(), workload_name));
    delete_prefix(Arc::clone(&store), &stage_root)
        .await
        .context("removing an abandoned workload candidate")?;

    let (results, sequential_checkpoint) = match suite.execution {
        SuiteExecution::Sequential => {
            execute_sequential_workload(
                &requested,
                stage_root,
                &state,
                Arc::clone(&store),
                object_store.instrumented.metrics(),
                !object_store.provider.eq_ignore_ascii_case("memory"),
                session,
                &result_context,
            )
            .await?
        }
        SuiteExecution::Isolated => {
            let results = execute_isolated_workload(
                &requested,
                stage_root,
                &mut state,
                &state_path,
                &session_root,
                object_store,
                &result_context,
            )
            .await?;
            (results, None)
        }
    };

    state.completed.push(CompletedWorkload {
        name: workload_name.to_string(),
        results: results.clone(),
    });
    if let Some(checkpoint) = sequential_checkpoint {
        state.sequential_databases.push(checkpoint);
    }
    state.measurement_complete = state.completed.len() == suite.workloads.len();
    write_run_manifest(args, benchmark, suite_name, &state)?;
    validate_output(&args.output)?;
    for result in &results {
        persist_result(
            Arc::clone(&object_store.raw),
            &session_root,
            &args.output,
            result,
        )
        .await?;
    }
    save_state(Arc::clone(&object_store.raw), &state_path, &state).await?;
    cleanup_completed_session(&mut state, object_store, &session_root, &state_path).await?;
    print_success(args);
    Ok(())
}

async fn execute_isolated_workload(
    variants: &[VariantConfig],
    stage_root: Path,
    state: &mut SessionState,
    state_path: &Path,
    session_root: &Path,
    object_store: &ObjectStoreContext,
    result_context: &ResultContext<'_>,
) -> Result<Vec<String>> {
    let store: Arc<dyn ObjectStore> = object_store.instrumented.clone();
    let mut results = Vec::with_capacity(variants.len());
    for variant in variants {
        let key = golden_key(variant)?;
        let golden = match state.goldens.get(&key) {
            Some(checkpoint) => checkpoint.to_golden(),
            None => {
                let path = session_root
                    .clone()
                    .join("databases")
                    .join("goldens")
                    .join(key.as_str());
                delete_prefix(Arc::clone(&store), &path)
                    .await
                    .context("removing an abandoned golden database")?;
                let golden =
                    prepare_golden_database(variant, path, Arc::clone(&store), &key).await?;
                state
                    .goldens
                    .insert(key.clone(), DatabaseCheckpoint::from_golden(&golden));
                save_state(Arc::clone(&object_store.raw), state_path, state).await?;
                golden
            }
        };
        let clone_path = stage_root
            .clone()
            .join(format!("{}-{}", variant.variant, Uuid::new_v4()));
        results.push(
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
    Ok(results)
}

#[allow(clippy::too_many_arguments)]
async fn execute_sequential_workload(
    variants: &[VariantConfig],
    stage_root: Path,
    state: &SessionState,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<crate::instrumented_store::StoreMetrics>,
    fresh_process: bool,
    session: &str,
    result_context: &ResultContext<'_>,
) -> Result<(Vec<String>, Option<DatabaseCheckpoint>)> {
    let first = variants.first().context("workload contains no variants")?;
    if first.workload.kind == WorkloadKind::BulkLoad {
        ensure!(
            variants.len() == 1 && state.sequential_databases.is_empty(),
            "bulk-load must be the first sequential workload with one variant"
        );
        let (result, checkpoint) = execute_bulk_load(
            first,
            stage_root.join("database"),
            store,
            store_metrics,
            session,
            result_context,
        )
        .await?;
        return Ok((vec![result], Some(checkpoint)));
    }

    let head = state
        .sequential_databases
        .last()
        .context("sequential session has no checkpoint for the next workload")?;
    let database_path = stage_root.join("database");
    clone_database(
        Arc::clone(&store),
        &database_path,
        &head.path(),
        head.checkpoint_id,
    )
    .await?;
    let mut results = Vec::with_capacity(variants.len());
    for (index, variant) in variants.iter().enumerate() {
        let initial_database_bytes = head
            .size_bytes
            .saturating_add(prefix_size(Arc::clone(&store), &database_path).await?);
        let (mut outcome, initial) = execute_rocks_variant(
            variant,
            &database_path,
            Arc::clone(&store),
            Arc::clone(&store_metrics),
            fresh_process,
            head.size_bytes,
            result_context.args,
        )
        .await?;
        if index == 0 {
            ensure!(
                initial.lsm_digest_sha256 == head.lsm_digest_sha256,
                "resumed database does not match the last committed checkpoint"
            );
        }
        let initial = InitialState {
            checkpoint_id: (index == 0).then(|| head.checkpoint_id.to_string()),
            ..initial
        };
        outcome.storage.database_size_bytes = head
            .size_bytes
            .saturating_add(prefix_size(Arc::clone(&store), &database_path).await?);
        results.push(write_variant_result(
            variant,
            outcome,
            initial,
            initial_database_bytes,
            result_context,
        )?);
    }
    let checkpoint = checkpoint_database(
        database_path,
        store,
        head.size_bytes,
        &format!("benchmark-{session}-{}", first.workload.name),
    )
    .await?;
    Ok((results, Some(checkpoint)))
}

async fn execute_bulk_load(
    variant: &VariantConfig,
    database_path: Path,
    store: Arc<dyn ObjectStore>,
    store_metrics: Arc<crate::instrumented_store::StoreMetrics>,
    session: &str,
    result_context: &ResultContext<'_>,
) -> Result<(String, DatabaseCheckpoint)> {
    let mut bulk_variant = variant.clone();
    bulk_variant.slate_settings = bulk_load_settings(&bulk_variant.slate_settings);
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
    let initial = InitialState {
        checkpoint_id: None,
        manifest_id: Some(bulk_db.status().current_manifest.id()),
        lsm_digest_sha256: lsm_digest(&bulk_db)?,
    };
    let measured = execute_variant(
        Arc::clone(&bulk_db),
        &bulk_variant,
        Arc::clone(&store_metrics),
        Arc::clone(&bulk_recorder),
        database_path.clone(),
        0,
    )
    .await;
    let mut outcome = match measured {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = bulk_db.close().await;
            return Err(error);
        }
    };

    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
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
        database_path.clone(),
        0,
        stop_rx,
    ));
    bulk_db
        .close()
        .await
        .context("closing resumable bulk-load database")?;

    let compaction_db = open_database_with_object_store_cache(
        database_path.clone(),
        Arc::clone(&store),
        &bulk_variant.suite,
        &variant.slate_settings,
        object_store_cache.as_ref().map(|cache| cache.path()),
        Arc::clone(&recorder),
    )
    .await?;
    let compaction_result =
        wait_for_compaction(&compaction_db, &recorder, &bulk_variant.suite).await;
    let _ = stop_tx.send(true);
    let sampled = sampler
        .await
        .context("joining resumable bulk compaction sampler")??;
    let elapsed = started.elapsed();
    let store_delta = store_metrics.snapshot().difference(&start_store);
    let end_slate = recorder.snapshot();
    let close_result = compaction_db.close().await;
    compaction_result?;
    close_result.context("closing resumable post-bulk compaction database")?;
    extend_with_compaction_phase(
        &mut outcome,
        sampled.samples,
        store_delta,
        &start_slate,
        &end_slate,
        elapsed,
    )?;

    let checkpoint = checkpoint_database(
        database_path,
        Arc::clone(&store),
        0,
        &format!("benchmark-{session}-bulk-load"),
    )
    .await?;
    outcome.storage.database_size_bytes = checkpoint.size_bytes;
    let result = write_variant_result(&bulk_variant, outcome, initial, 0, result_context)?;
    Ok((result, checkpoint))
}

async fn checkpoint_database(
    path: Path,
    store: Arc<dyn ObjectStore>,
    shared_database_bytes: u64,
    name: &str,
) -> Result<DatabaseCheckpoint> {
    let admin = AdminBuilder::new(path.clone(), Arc::clone(&store)).build();
    let checkpoint = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime: None,
            source: None,
            name: Some(name.to_string()),
        })
        .await
        .context("creating resumable benchmark checkpoint")?;
    let manifest = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .context("reading resumable benchmark checkpoint")?
        .context("resumable benchmark checkpoint manifest does not exist")?;
    Ok(DatabaseCheckpoint {
        path: path.to_string(),
        checkpoint_id: checkpoint.id,
        manifest_id: checkpoint.manifest_id,
        lsm_digest_sha256: manifest_lsm_digest(&manifest)?,
        size_bytes: shared_database_bytes.saturating_add(prefix_size(store, &path).await?),
    })
}

fn write_run_manifest(
    args: &RunArgs,
    benchmark: &BenchmarkConfig,
    suite_name: &str,
    state: &SessionState,
) -> Result<()> {
    ensure!(
        !state.completed.is_empty(),
        "session has not completed a workload"
    );
    let suite = benchmark
        .suites
        .iter()
        .find(|suite| suite.name == suite_name)
        .context("session suite is missing from configuration")?;
    let run = RunManifest {
        status: "ok".to_string(),
        started_at: state.started_at.clone(),
        finished_at: Utc::now().to_rfc3339(),
        mode: suite.mode().to_string(),
        slate_version: state.slate_version.clone(),
        slate_commit: state.slate_commit.clone(),
        runner_version: env!("CARGO_PKG_VERSION").to_string(),
        runner_commit: state.runner_commit.clone(),
        lockfile_sha256: state.lockfile_sha256.clone(),
        resolved_configuration: serde_json::to_value(benchmark)?,
        object_store_baselines: BTreeMap::from([(
            suite_name.to_string(),
            state.object_store_baseline.clone(),
        )]),
        results: state
            .completed
            .iter()
            .flat_map(|completed| completed.results.iter().cloned())
            .collect(),
    };
    validate_run(&run, &args.schema_dir)?;
    write_json(&args.output.join("run.json"), &run)
}

async fn cleanup_completed_session(
    state: &mut SessionState,
    object_store: &ObjectStoreContext,
    session_root: &Path,
    state_path: &Path,
) -> Result<()> {
    if state.measurement_complete
        && (!state.sequential_databases.is_empty() || !state.goldens.is_empty())
    {
        let store: Arc<dyn ObjectStore> = object_store.instrumented.clone();
        delete_prefix(store, &session_root.clone().join("databases"))
            .await
            .context("cleaning up completed session databases")?;
        state.sequential_databases.clear();
        state.goldens.clear();
        save_state(Arc::clone(&object_store.raw), state_path, state).await?;
    }
    Ok(())
}

fn clear_local_workload_output(output: &FsPath, suite: &str, workload: &str) -> Result<()> {
    let path = output
        .join("results")
        .join(env!("BENCHMARK_SLATE_VERSION"))
        .join(suite)
        .join(workload);
    if path.exists() {
        fs::remove_dir_all(&path).with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

fn print_success(args: &RunArgs) {
    println!(
        "{{\"status\":\"ok\",\"run\":\"{}\"}}",
        args.output.join("run.json").display()
    );
}

fn validate_session_name(session: &str) -> Result<()> {
    ensure!(
        !session.is_empty()
            && session.len() <= 128
            && session != "."
            && session != ".."
            && session
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "session names must be 1-128 ASCII letters, digits, '.', '-', or '_'"
    );
    Ok(())
}

fn ensure_output_directory(output: &FsPath, session: &str, suite: &str) -> Result<()> {
    let marker = output.join(OUTPUT_MARKER);
    let expected = format!("{session}\n{suite}\n");
    if marker.exists() {
        let actual =
            fs::read_to_string(&marker).with_context(|| format!("reading {}", marker.display()))?;
        ensure!(
            actual == expected,
            "output directory belongs to a different benchmark session"
        );
        return Ok(());
    }
    let mut entries = fs::read_dir(output)
        .with_context(|| format!("reading output directory {}", output.display()))?;
    ensure!(
        entries.next().is_none(),
        "existing output directory is not a resumable session directory"
    );
    fs::write(&marker, expected).with_context(|| format!("writing {}", marker.display()))
}

fn configuration_digest(suite: &SuiteConfig, variant: &VariantConfig) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(serde_json::to_vec(suite)?);
    digest.update(serde_json::to_vec(&variant.slate_settings)?);
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_state(
    state: &SessionState,
    session: &str,
    suite: &SuiteConfig,
    configuration_sha256: &str,
    environment: &Environment,
) -> Result<()> {
    ensure!(
        state.session == session,
        "session record has a different name"
    );
    ensure!(
        state.suite == suite.name,
        "session belongs to a different suite"
    );
    ensure!(
        state.slate_version == env!("BENCHMARK_SLATE_VERSION")
            && state.slate_commit == env!("BENCHMARK_SLATE_COMMIT"),
        "session was created for a different SlateDB build"
    );
    ensure!(
        state.runner_commit == env!("BENCHMARK_RUNNER_COMMIT")
            && state.lockfile_sha256 == env!("BENCHMARK_LOCK_HASH"),
        "session was created by a different benchmark runner build"
    );
    ensure!(
        state.configuration_sha256 == configuration_sha256,
        "session configuration changed"
    );
    ensure!(
        compatible_environment(&state.environment, environment),
        "session runner or object-store environment changed"
    );
    ensure!(
        state.completed.len() <= suite.workloads.len(),
        "session contains more workloads than the suite"
    );
    for (completed, workload) in state.completed.iter().zip(&suite.workloads) {
        ensure!(
            completed.name == workload.name && completed.results.len() == workload.variants.len(),
            "session results do not match the configured workload order"
        );
    }
    ensure!(
        state.measurement_complete == (state.completed.len() == suite.workloads.len()),
        "session completion marker is inconsistent"
    );
    match suite.execution {
        SuiteExecution::Sequential => {
            ensure!(
                state.goldens.is_empty(),
                "sequential session contains golden databases"
            );
            ensure!(
                (state.measurement_complete && state.sequential_databases.is_empty())
                    || state.sequential_databases.len() == state.completed.len(),
                "sequential checkpoints do not match completed workloads"
            );
        }
        SuiteExecution::Isolated => ensure!(
            state.sequential_databases.is_empty(),
            "isolated session contains sequential checkpoints"
        ),
    }
    Ok(())
}

fn compatible_environment(left: &Environment, right: &Environment) -> bool {
    left.runner_type == right.runner_type
        && left.cpu_model == right.cpu_model
        && left.cpu_cores == right.cpu_cores
        && left.ram_bytes == right.ram_bytes
        && left.local_disk == right.local_disk
        && left.os == right.os
        && left.object_store == right.object_store
        && left.endpoint == right.endpoint
        && left.region == right.region
}

async fn load_state(store: Arc<dyn ObjectStore>, path: &Path) -> Result<Option<SessionState>> {
    let result = match store.get(path).await {
        Ok(result) => result,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error).context("loading benchmark session state"),
    };
    let bytes = result
        .bytes()
        .await
        .context("reading benchmark session state")?;
    Ok(Some(
        serde_json::from_slice(&bytes).context("parsing benchmark session state")?,
    ))
}

async fn save_state(store: Arc<dyn ObjectStore>, path: &Path, state: &SessionState) -> Result<()> {
    store
        .put(path, PutPayload::from(serde_json::to_vec_pretty(state)?))
        .await
        .context("saving benchmark session state")?;
    Ok(())
}

async fn persist_result(
    store: Arc<dyn ObjectStore>,
    session_root: &Path,
    output: &FsPath,
    result_path: &str,
) -> Result<()> {
    let relative = PathBuf::from(result_path);
    let directory = relative.parent().context("result path has no parent")?;
    for file_name in RESULT_FILES {
        let relative_file = directory.join(file_name);
        let local = output.join(&relative_file);
        let bytes = fs::read(&local).with_context(|| format!("reading {}", local.display()))?;
        let remote = append_path(session_root.clone().join("artifacts"), &relative_file)?;
        store
            .put(&remote, PutPayload::from(bytes))
            .await
            .with_context(|| format!("persisting completed result {remote}"))?;
    }
    Ok(())
}

async fn hydrate_results(
    store: Arc<dyn ObjectStore>,
    session_root: &Path,
    output: &FsPath,
    completed: &[CompletedWorkload],
) -> Result<()> {
    for result in completed.iter().flat_map(|workload| &workload.results) {
        let relative = PathBuf::from(result);
        let directory = relative.parent().context("result path has no parent")?;
        let local_directory = output.join(directory);
        fs::create_dir_all(&local_directory)
            .with_context(|| format!("creating {}", local_directory.display()))?;
        for file_name in RESULT_FILES {
            let relative_file = directory.join(file_name);
            let remote = append_path(session_root.clone().join("artifacts"), &relative_file)?;
            let bytes = store
                .get(&remote)
                .await
                .with_context(|| format!("loading completed result {remote}"))?
                .bytes()
                .await
                .with_context(|| format!("reading completed result {remote}"))?;
            let local = output.join(relative_file);
            fs::write(&local, bytes).with_context(|| format!("writing {}", local.display()))?;
        }
    }
    Ok(())
}

fn append_path(mut root: Path, relative: &FsPath) -> Result<Path> {
    for component in relative.components() {
        match component {
            Component::Normal(value) => {
                let value = value.to_str().context("result path is not valid UTF-8")?;
                root = root.join(value);
            }
            _ => bail!("result path is not relative"),
        }
    }
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::{
        append_path, checkpoint_database, compatible_environment, hydrate_results, load_state,
        persist_result, save_state, CompletedWorkload, SessionState, RESULT_FILES,
    };
    use crate::model::{Environment, ObjectStoreBaseline};
    use crate::runner::clone_database;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn state() -> SessionState {
        SessionState {
            session: "test-session".to_string(),
            suite: "rocksdb".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            slate_version: "test".to_string(),
            slate_commit: "test".to_string(),
            runner_commit: "test".to_string(),
            lockfile_sha256: "test".to_string(),
            configuration_sha256: "test".to_string(),
            environment: Environment::default(),
            object_store_baseline: ObjectStoreBaseline::default(),
            baseline_histograms: BTreeMap::new(),
            completed: Vec::new(),
            sequential_databases: Vec::new(),
            goldens: BTreeMap::new(),
            measurement_complete: false,
        }
    }

    #[tokio::test]
    async fn session_state_and_workload_bundle_survive_a_new_output_directory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let root = Path::from("sessions/test-session");
        let state_path = root.clone().join("resume.json");
        let mut state = state();
        let relative = PathBuf::from("results/test/rocksdb/bulk-load/clients-1/result.json");
        state.completed.push(CompletedWorkload {
            name: "bulk-load".to_string(),
            results: vec![relative.display().to_string()],
        });

        let first = tempdir().expect("first output");
        let directory = first.path().join(relative.parent().expect("result parent"));
        fs::create_dir_all(&directory).expect("create result directory");
        for name in RESULT_FILES {
            fs::write(directory.join(name), name).expect("write result file");
        }
        persist_result(
            Arc::clone(&store),
            &root,
            first.path(),
            &relative.display().to_string(),
        )
        .await
        .expect("persist result");
        save_state(Arc::clone(&store), &state_path, &state)
            .await
            .expect("save state");

        let loaded = load_state(Arc::clone(&store), &state_path)
            .await
            .expect("load state")
            .expect("state exists");
        let second = tempdir().expect("second output");
        hydrate_results(store, &root, second.path(), &loaded.completed)
            .await
            .expect("hydrate results");

        for name in RESULT_FILES {
            let parent = relative.parent().expect("result parent");
            assert_eq!(
                fs::read_to_string(second.path().join(parent).join(name))
                    .expect("read hydrated file"),
                name
            );
        }
    }

    #[tokio::test]
    async fn retry_clones_the_last_committed_checkpoint() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let committed_path = Path::from("sessions/recovery/databases/committed");
        let db = slatedb::Db::open(committed_path.clone(), Arc::clone(&store))
            .await
            .expect("open committed database");
        db.put(b"key", b"committed")
            .await
            .expect("write committed value");
        db.flush().await.expect("flush committed database");
        db.close().await.expect("close committed database");
        let committed =
            checkpoint_database(committed_path.clone(), Arc::clone(&store), 0, "committed")
                .await
                .expect("checkpoint committed database");

        let abandoned_path = Path::from("sessions/recovery/databases/abandoned");
        clone_database(
            Arc::clone(&store),
            &abandoned_path,
            &committed_path,
            committed.checkpoint_id,
        )
        .await
        .expect("clone abandoned candidate");
        let abandoned = slatedb::Db::open(abandoned_path, Arc::clone(&store))
            .await
            .expect("open abandoned candidate");
        abandoned
            .put(b"key", b"uncommitted")
            .await
            .expect("write uncommitted value");
        abandoned.flush().await.expect("flush abandoned candidate");
        abandoned.close().await.expect("close abandoned candidate");

        let retry_path = Path::from("sessions/recovery/databases/retry");
        clone_database(
            Arc::clone(&store),
            &retry_path,
            &committed_path,
            committed.checkpoint_id,
        )
        .await
        .expect("clone retry candidate");
        let retry = slatedb::Db::open(retry_path, store)
            .await
            .expect("open retry candidate");
        assert_eq!(
            retry.get(b"key").await.expect("read retry value"),
            Some(Bytes::from_static(b"committed"))
        );
        retry.close().await.expect("close retry candidate");
    }

    #[test]
    fn resume_environment_ignores_hostname_but_not_machine_shape() {
        let left = Environment {
            hostname: "one".to_string(),
            runner_type: "runner".to_string(),
            cpu_model: "cpu".to_string(),
            cpu_cores: 16,
            ram_bytes: 64,
            local_disk: "disk".to_string(),
            os: "linux".to_string(),
            object_store: "Tigris".to_string(),
            endpoint: "endpoint".to_string(),
            region: "fra".to_string(),
            ..Default::default()
        };
        let mut right = left.clone();
        right.hostname = "two".to_string();
        assert!(compatible_environment(&left, &right));
        right.cpu_cores = 8;
        assert!(!compatible_environment(&left, &right));
    }

    #[test]
    fn artifact_paths_reject_parent_components() {
        assert!(append_path(Path::from("root"), std::path::Path::new("../result.json")).is_err());
    }
}
