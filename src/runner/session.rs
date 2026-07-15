use super::{
    bulk_load_settings, clone_database, execute_bulk_load_and_compact, execute_isolated_variant,
    execute_rocks_variant, golden_key, manifest_lsm_digest, prepare_golden_database, write_json,
    write_variant_result, GoldenDatabase, ResultContext,
};
use crate::cli::RunArgs;
use crate::config::{BenchmarkConfig, SuiteConfig, SuiteExecution, VariantConfig, WorkloadKind};
use crate::database_size::live_database_size_bytes;
use crate::model::{EncodedHistogram, Environment, InitialState, ObjectStoreBaseline, RunManifest};
use crate::object_store_probe::{delete_prefix, probe, ObjectStoreContext};
use crate::validation::{validate_output, validate_run};
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
use std::sync::Arc;
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
    let selected_workload = args.workload.as_deref();
    ensure!(
        args.variant.is_none(),
        "resumable sessions execute complete workloads, not individual variants"
    );
    if let Some(workload_name) = selected_workload {
        ensure!(
            requested
                .iter()
                .all(|variant| variant.workload.name == workload_name),
            "workload session selection contains more than one workload"
        );
    }
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

    for workload in &suite.workloads {
        let workload_name = workload.name.as_str();
        if selected_workload.is_some_and(|selected| selected != workload_name) {
            continue;
        }
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
            continue;
        }
        let workload_variants = requested
            .iter()
            .filter(|variant| variant.workload.name == workload_name)
            .cloned()
            .collect::<Vec<_>>();
        ensure!(
            !workload_variants.is_empty(),
            "session selection does not contain workload {workload_name}"
        );

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
                    &workload_variants,
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
                    &workload_variants,
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
    }

    write_run_manifest(args, benchmark, suite_name, &state)?;
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
        let (outcome, initial, initial_database_bytes) = execute_rocks_variant(
            variant,
            &database_path,
            Arc::clone(&store),
            Arc::clone(&store_metrics),
            fresh_process,
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
    let execution = execute_bulk_load_and_compact(
        &bulk_variant,
        &variant.slate_settings,
        &database_path,
        Arc::clone(&store),
        store_metrics,
        true,
    )
    .await?;
    let mut outcome = execution
        .outcome
        .context("measured bulk load did not produce an outcome")?;
    let initial = execution.initial;

    let checkpoint = checkpoint_database(
        database_path,
        Arc::clone(&store),
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
        size_bytes: live_database_size_bytes(&manifest),
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
    use crate::cli::RunArgs;
    use crate::config::BenchmarkConfig;
    use crate::instrumented_store::InstrumentedStore;
    use crate::model::{Environment, ObjectStoreBaseline};
    use crate::object_store_probe::ObjectStoreContext;
    use crate::runner::clone_database;
    use crate::validation::validate_output;
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
    async fn suite_session_restores_completed_workloads_into_a_new_output() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let instrumented = Arc::new(InstrumentedStore::new(Arc::clone(&raw)));
        let object_store = ObjectStoreContext {
            raw: Arc::clone(&raw),
            instrumented,
            root: Path::from("suite-resume"),
            provider: "memory".to_string(),
            endpoint: "memory".to_string(),
            region: "test".to_string(),
        };
        let benchmark = BenchmarkConfig::load_from(std::path::Path::new("config"))
            .expect("load benchmark config");
        let selected = benchmark
            .select(Some("smoke"), None, None)
            .expect("select smoke suite");
        let environment = Environment {
            runner_type: "test".to_string(),
            hostname: "test-host".to_string(),
            cpu_model: "test-cpu".to_string(),
            cpu_cores: 1,
            ram_bytes: 1,
            local_disk: "memory".to_string(),
            os: "test".to_string(),
            kernel: "test".to_string(),
            object_store: "memory".to_string(),
            endpoint: "memory".to_string(),
            region: "test".to_string(),
        };
        let first_output = tempdir().expect("first output");
        let args = RunArgs {
            suite: Some("smoke".to_string()),
            session: Some("suite-session".to_string()),
            workload: None,
            variant: None,
            output: first_output.path().to_path_buf(),
            config_dir: PathBuf::from("config"),
            schema_dir: PathBuf::from("schema"),
        };

        super::execute(
            &args,
            &benchmark,
            selected.clone(),
            &object_store,
            &environment,
        )
        .await
        .expect("execute complete suite session");
        validate_output(first_output.path()).expect("validate first output");

        let state_path = Path::from("suite-resume/sessions/suite-session/resume.json");
        let state = load_state(Arc::clone(&raw), &state_path)
            .await
            .expect("load completed state")
            .expect("completed state exists");
        let suite = selected.first().expect("selected variant").suite.clone();
        assert!(state.measurement_complete);
        assert_eq!(
            state
                .completed
                .iter()
                .map(|workload| workload.name.as_str())
                .collect::<Vec<_>>(),
            suite
                .workloads
                .iter()
                .map(|workload| workload.name.as_str())
                .collect::<Vec<_>>()
        );

        let second_output = tempdir().expect("second output");
        let resumed_args = RunArgs {
            output: second_output.path().to_path_buf(),
            ..args
        };
        super::execute(
            &resumed_args,
            &benchmark,
            selected,
            &object_store,
            &environment,
        )
        .await
        .expect("resume completed suite session");
        validate_output(second_output.path()).expect("validate resumed output");
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
            checkpoint_database(committed_path.clone(), Arc::clone(&store), "committed")
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

    #[tokio::test]
    async fn checkpoint_clone_hops_do_not_inflate_live_database_size() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source_path = Path::from("sessions/size/source");
        let db = slatedb::Db::open(source_path.clone(), Arc::clone(&store))
            .await
            .expect("open source database");
        for index in 0..32 {
            db.put(
                format!("key-{index:04}"),
                Bytes::from(vec![index as u8; 256]),
            )
            .await
            .expect("write source value");
        }
        db.flush().await.expect("flush source database");
        db.close().await.expect("close source database");
        let source = checkpoint_database(source_path.clone(), Arc::clone(&store), "source")
            .await
            .expect("checkpoint source database");

        let first_clone_path = Path::from("sessions/size/first-clone");
        clone_database(
            Arc::clone(&store),
            &first_clone_path,
            &source_path,
            source.checkpoint_id,
        )
        .await
        .expect("create first clone");
        let first_clone =
            checkpoint_database(first_clone_path.clone(), Arc::clone(&store), "first-clone")
                .await
                .expect("checkpoint first clone");

        let second_clone_path = Path::from("sessions/size/second-clone");
        clone_database(
            Arc::clone(&store),
            &second_clone_path,
            &first_clone_path,
            first_clone.checkpoint_id,
        )
        .await
        .expect("create second clone");
        let second_clone = checkpoint_database(second_clone_path, store, "second-clone")
            .await
            .expect("checkpoint second clone");

        assert!(source.size_bytes > 0);
        assert_eq!(source.size_bytes, first_clone.size_bytes);
        assert_eq!(first_clone.size_bytes, second_clone.size_bytes);
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
