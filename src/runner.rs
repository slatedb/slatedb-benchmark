use crate::cli::RunArgs;
use crate::config::{self, ResolvedConfig, Task};
use crate::database_size::live_database_size_bytes;
use crate::model::{
    CheckpointReference, GoldenDatasetMetadata, InitialState, PreparationResult,
    ResultConfiguration, SeriesReference, SourceIdentity, WorkloadResult, WorkloadSeries,
};
use crate::object_store::{delete_prefix, ObjectStoreContext};
use crate::system::{
    duration_ns, inspect_environment, measure_until_complete, verify_environment,
    ApplicationRegistry, BenchmarkMetricsRecorder, SampledMeasurement,
};
use crate::validation::{
    validate_preparation_result, validate_workload_result, validate_workload_series,
};
use crate::workloads::{self, DatasetLoadMetrics};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::Utc;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use slatedb::admin::{Admin, AdminBuilder, CloneSourceSpec};
use slatedb::compactor::{Compaction, CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{CheckpointOptions, Settings};
use slatedb::db_cache::{
    foyer::{FoyerCache, FoyerCacheOptions},
    DbCache, SplitCache,
};
use slatedb::{Db, VersionedManifest};
use slatedb_common::metrics::MetricsRecorder;
use std::fs;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const SETTINGS_PATH: &str = "config/settings.toml";
const COMPACTION_QUIET: Duration = Duration::from_secs(15);

pub async fn execute(args: RunArgs) -> Result<()> {
    validate_name(&args.golden, "golden")?;
    if args.task.is_preparation() {
        if args.session.is_some() {
            bail!("--session is only valid for workload tasks");
        }
    } else {
        validate_name(
            args.session
                .as_deref()
                .context("--session is required for workload tasks")?,
            "session",
        )?;
    }
    fs::create_dir_all(&args.output)
        .with_context(|| format!("creating {}", args.output.display()))?;
    remove_local_output(&args.output.join("result.json"))?;
    remove_local_output(&args.output.join("series.json"))?;
    remove_local_output(&args.output.join("failure.json"))?;
    let config = config::load(args.task, args.scale, FsPath::new(SETTINGS_PATH))?;
    let object_store = ObjectStoreContext::load()?;
    let result = match args.task {
        Task::BulkLoad => run_bulk_load(&args, &config, &object_store).await,
        Task::FullCompaction => run_full_compaction(&args, &config, &object_store).await,
        _ => run_workload(&args, &config, &object_store).await,
    };
    if let Err(error) = &result {
        tracing::error!(task = %args.task, "benchmark task failed; partial data remains for diagnosis");
        if let Err(diagnostic_error) =
            write_failure_diagnostic(&args, &config, &object_store, error)
        {
            tracing::warn!(%diagnostic_error, "failed to write task diagnostic");
        }
    }
    result
}

fn remove_local_output(path: &FsPath) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing stale {}", path.display())),
    }
}

fn write_failure_diagnostic(
    args: &RunArgs,
    config: &ResolvedConfig,
    context: &ObjectStoreContext,
    error: &anyhow::Error,
) -> Result<()> {
    let diagnostic = serde_json::json!({
        "status": "failed",
        "task": args.task,
        "golden_id": args.golden,
        "session": args.session,
        "timestamp": Utc::now().to_rfc3339(),
        "source": SourceIdentity::current(),
        "configuration": ResultConfiguration::from(config),
        "error": format!("{error:#}"),
        "object_store": context.instrumented.metrics().snapshot(),
    });
    let path = args.output.join("failure.json");
    fs::write(&path, serde_json::to_vec_pretty(&diagnostic)?)
        .with_context(|| format!("writing {}", path.display()))
}

async fn run_bulk_load(
    args: &RunArgs,
    config: &ResolvedConfig,
    context: &ObjectStoreContext,
) -> Result<()> {
    let config = bulk_load_config(config)?;
    let environment = inspect_environment(&context.provider, &context.endpoint, &context.region);
    let task_root = golden_task_root(context, &args.golden, Task::BulkLoad);
    let result_path = task_root.clone().join("result.json");
    if let Some(existing) =
        load_optional::<PreparationResult>(Arc::clone(&context.control), &result_path).await?
    {
        if existing.recorded_interval_ns > 0 {
            validate_preparation_result(&existing)?;
            ensure_preparation_matches(&existing, args, &config)?;
            verify_checkpoint_reference(Arc::clone(&context.control), &existing.checkpoint).await?;
            validate_uncompacted_checkpoint(Arc::clone(&context.control), &existing.checkpoint)
                .await?;
            write_local_result(&args.output, &existing)?;
            return print_success(args, true);
        }
        tracing::info!(task = %args.task, "rebuilding preparation result without recorded metrics");
    }
    delete_prefix(Arc::clone(&context.control), &task_root).await?;
    let database_path = task_root.clone().join("database");
    let cache = object_store_cache_directory()?;
    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let db = open_database(
        database_path.clone(),
        context.instrumented.clone(),
        &config,
        &config.settings,
        Some(cache.path()),
        Arc::clone(&recorder),
    )
    .await?;
    let application = Arc::new(ApplicationRegistry::default());
    let load = measure_until_complete(
        Arc::clone(&application),
        context.instrumented.metrics(),
        workloads::populate_dataset(
            Arc::clone(&db),
            &config,
            DatasetLoadMetrics::new(
                context.instrumented.metrics(),
                recorder,
                application.recorder(),
            ),
        ),
    )
    .await;
    let ((), measurement) = close_database_after(&db, load, "closing bulk-load database").await?;
    ensure_measurement_has_no_application_errors(&measurement)?;
    let checkpoint = checkpoint_database(
        database_path,
        Arc::clone(&context.control),
        &format!("benchmark-{}-bulk-load", args.golden),
    )
    .await?;
    validate_uncompacted_checkpoint(Arc::clone(&context.control), &checkpoint).await?;
    let result = PreparationResult {
        status: "ok".to_string(),
        task: Task::BulkLoad,
        golden_id: args.golden.clone(),
        timestamp: Utc::now().to_rfc3339(),
        source: SourceIdentity::current(),
        environment,
        configuration: ResultConfiguration::from(&config),
        source_checkpoint: None,
        dataset: dataset_metadata(&config, &checkpoint),
        checkpoint,
        recorded_interval_ns: duration_ns(measurement.elapsed()),
        application: measurement.application(),
        object_store: measurement.object_store(),
        process: measurement.process(),
        machine: measurement.machine(),
    };
    validate_preparation_result(&result)?;
    write_local_result(&args.output, &result)?;
    create_result(Arc::clone(&context.control), &result_path, &result).await?;
    print_success(args, false)
}

async fn run_full_compaction(
    args: &RunArgs,
    config: &ResolvedConfig,
    context: &ObjectStoreContext,
) -> Result<()> {
    let environment = inspect_environment(&context.provider, &context.endpoint, &context.region);
    let bulk_path = golden_task_root(context, &args.golden, Task::BulkLoad).join("result.json");
    let bulk: PreparationResult = load_required(Arc::clone(&context.control), &bulk_path).await?;
    validate_preparation_result(&bulk)?;
    ensure_shared_configuration(&bulk.configuration, config, false)?;
    if bulk.source.slate_commit != env!("BENCHMARK_SLATE_COMMIT") {
        bail!("bulk-load checkpoint was created by a different SlateDB commit");
    }
    verify_checkpoint_reference(Arc::clone(&context.control), &bulk.checkpoint).await?;
    validate_uncompacted_checkpoint(Arc::clone(&context.control), &bulk.checkpoint).await?;

    let task_root = golden_task_root(context, &args.golden, Task::FullCompaction);
    let result_path = task_root.clone().join("result.json");
    if let Some(existing) =
        load_optional::<PreparationResult>(Arc::clone(&context.control), &result_path).await?
    {
        if existing.recorded_interval_ns > 0 {
            validate_preparation_result(&existing)?;
            ensure_preparation_matches(&existing, args, config)?;
            anyhow::ensure!(
                existing.source_checkpoint.as_ref() == Some(&bulk.checkpoint),
                "existing full-compaction result belongs to another bulk-load checkpoint"
            );
            verify_checkpoint_reference(Arc::clone(&context.control), &existing.checkpoint).await?;
            validate_compacted_checkpoint(Arc::clone(&context.control), &existing.checkpoint)
                .await?;
            write_local_result(&args.output, &existing)?;
            return print_success(args, true);
        }
        tracing::info!(task = %args.task, "rebuilding preparation result without recorded metrics");
    }
    delete_prefix(Arc::clone(&context.control), &task_root).await?;
    let database_path = task_root.clone().join("database");
    clone_database(
        Arc::clone(&context.control),
        &database_path,
        &Path::from(bulk.checkpoint.database_path.clone()),
        parse_checkpoint_id(&bulk.checkpoint)?,
    )
    .await?;
    let cache = object_store_cache_directory()?;
    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let db = open_database(
        database_path.clone(),
        context.instrumented.clone(),
        config,
        &config.settings,
        Some(cache.path()),
        recorder,
    )
    .await?;
    let application = Arc::new(ApplicationRegistry::default());
    let compaction = measure_until_complete(
        application,
        context.instrumented.metrics(),
        compact_database_fully(database_path.clone(), Arc::clone(&context.control)),
    )
    .await;
    let ((), measurement) =
        close_database_after(&db, compaction, "closing full-compaction database").await?;
    ensure_measurement_has_no_application_errors(&measurement)?;
    let checkpoint = checkpoint_database(
        database_path,
        Arc::clone(&context.control),
        &format!("benchmark-{}-full-compaction", args.golden),
    )
    .await?;
    validate_compacted_checkpoint(Arc::clone(&context.control), &checkpoint).await?;
    let result = PreparationResult {
        status: "ok".to_string(),
        task: Task::FullCompaction,
        golden_id: args.golden.clone(),
        timestamp: Utc::now().to_rfc3339(),
        source: SourceIdentity::current(),
        environment,
        configuration: ResultConfiguration::from(config),
        source_checkpoint: Some(bulk.checkpoint),
        dataset: dataset_metadata(config, &checkpoint),
        checkpoint,
        recorded_interval_ns: duration_ns(measurement.elapsed()),
        application: measurement.application(),
        object_store: measurement.object_store(),
        process: measurement.process(),
        machine: measurement.machine(),
    };
    validate_preparation_result(&result)?;
    write_local_result(&args.output, &result)?;
    create_result(Arc::clone(&context.control), &result_path, &result).await?;
    print_success(args, false)
}

async fn run_workload(
    args: &RunArgs,
    config: &ResolvedConfig,
    context: &ObjectStoreContext,
) -> Result<()> {
    let session = args.session.as_deref().context("workload session")?;
    let golden = if args.task.uses_golden() {
        let full_path =
            golden_task_root(context, &args.golden, Task::FullCompaction).join("result.json");
        let golden: PreparationResult =
            load_required(Arc::clone(&context.control), &full_path).await?;
        validate_preparation_result(&golden)?;
        ensure_shared_configuration(&golden.configuration, config, true)?;
        if golden.source.slate_commit != env!("BENCHMARK_SLATE_COMMIT") {
            bail!("golden checkpoint was created by a different SlateDB commit");
        }
        verify_checkpoint_reference(Arc::clone(&context.control), &golden.checkpoint).await?;
        validate_compacted_checkpoint(Arc::clone(&context.control), &golden.checkpoint).await?;
        Some(golden)
    } else {
        None
    };
    let environment = inspect_environment(&context.provider, &context.endpoint, &context.region);
    if std::env::var("SLATEDB_BENCH_PUBLISHED").as_deref() == Ok("true") {
        anyhow::ensure!(config.scale == 1.0, "published runs require scale 1.0");
        verify_environment(&environment)?;
    }

    let task_root = context
        .root
        .clone()
        .join("sessions")
        .join(session)
        .join(args.task.as_str());
    let result_path = task_root.clone().join("result.json");
    let series_path = task_root.clone().join("series.json");
    if let Some(existing) =
        load_optional::<WorkloadResult>(Arc::clone(&context.control), &result_path).await?
    {
        validate_workload_result(&existing)?;
        ensure_workload_matches(&existing, args, config, golden.as_ref())?;
        let series_bytes = load_required_bytes(Arc::clone(&context.control), &series_path).await?;
        validate_series_digest(&existing.series, &series_bytes)?;
        let series: WorkloadSeries =
            serde_json::from_slice(&series_bytes).context("parsing stored workload series")?;
        validate_workload_series(&existing, &series)?;
        write_local_bytes(&args.output, "series.json", &series_bytes)?;
        write_local_result(&args.output, &existing)?;
        return print_success(args, true);
    }
    delete_prefix(Arc::clone(&context.control), &task_root).await?;
    let database_path = task_root.clone().join("database");
    let mut initial = if args.task.uses_golden() {
        let golden = golden.as_ref().context("golden checkpoint")?;
        clone_database(
            Arc::clone(&context.control),
            &database_path,
            &Path::from(golden.checkpoint.database_path.clone()),
            parse_checkpoint_id(&golden.checkpoint)?,
        )
        .await?;
        InitialState {
            kind: "golden".to_string(),
            checkpoint_id: Some(golden.checkpoint.checkpoint_id.clone()),
            manifest_id: Some(golden.checkpoint.manifest_id),
            lsm_digest_sha256: golden.checkpoint.lsm_digest_sha256.clone(),
        }
    } else {
        InitialState {
            kind: "empty".to_string(),
            checkpoint_id: None,
            manifest_id: None,
            lsm_digest_sha256: String::new(),
        }
    };
    let cache = object_store_cache_directory()?;
    let recorder = Arc::new(BenchmarkMetricsRecorder::new());
    let db = open_database(
        database_path.clone(),
        context.instrumented.clone(),
        config,
        &config.settings,
        Some(cache.path()),
        recorder,
    )
    .await?;
    let actual_digest = match lsm_digest(&db) {
        Ok(digest) => digest,
        Err(error) => {
            let _ = db.close().await;
            return Err(error);
        }
    };
    if args.task.uses_golden() {
        if actual_digest != initial.lsm_digest_sha256 {
            let close = db.close().await.context("closing invalid workload clone");
            close?;
            bail!("workload clone does not match the golden checkpoint");
        }
    } else {
        initial.lsm_digest_sha256 = actual_digest;
    }
    let execution =
        workloads::execute(Arc::clone(&db), config, context.instrumented.metrics()).await;
    let close = db.close().await.context("closing workload database");
    let compaction_check =
        ensure_no_failed_compactions(database_path, Arc::clone(&context.control)).await;
    let execution = execution?;
    close?;
    compaction_check?;

    let series = execution.measurement.series();
    let series_bytes = serde_json::to_vec_pretty(&series)?;
    let result = WorkloadResult {
        status: "ok".to_string(),
        task: args.task,
        golden_id: args.golden.clone(),
        session: session.to_string(),
        timestamp: Utc::now().to_rfc3339(),
        source: SourceIdentity::current(),
        environment,
        configuration: ResultConfiguration::from(config),
        initial_state: initial,
        client_measurement_ns: duration_ns(execution.client_measurement),
        durability_drain_ns: duration_ns(execution.durability_drain),
        recorded_interval_ns: duration_ns(execution.measurement.elapsed()),
        application: execution.measurement.application(),
        object_store: execution.measurement.object_store(),
        process: execution.measurement.process(),
        machine: execution.measurement.machine(),
        series: SeriesReference {
            file: "series.json".to_string(),
            sha256: sha256_bytes(&series_bytes),
        },
    };
    validate_workload_result(&result)?;
    validate_workload_series(&result, &series)?;
    write_local_bytes(&args.output, "series.json", &series_bytes)?;
    write_local_result(&args.output, &result)?;
    create_bytes(
        Arc::clone(&context.control),
        &series_path,
        series_bytes,
        "workload series",
    )
    .await?;
    create_result(Arc::clone(&context.control), &result_path, &result).await?;
    print_success(args, false)
}

fn golden_task_root(context: &ObjectStoreContext, golden: &str, task: Task) -> Path {
    context
        .root
        .clone()
        .join("goldens")
        .join(golden)
        .join(task.as_str())
}

fn ensure_preparation_matches(
    result: &PreparationResult,
    args: &RunArgs,
    config: &ResolvedConfig,
) -> Result<()> {
    anyhow::ensure!(
        result.task == args.task,
        "existing result belongs to another task"
    );
    anyhow::ensure!(
        result.golden_id == args.golden,
        "existing result belongs to another golden ID"
    );
    anyhow::ensure!(
        result.source.slate_commit == env!("BENCHMARK_SLATE_COMMIT"),
        "existing golden uses a different SlateDB commit"
    );
    anyhow::ensure!(
        result.configuration == ResultConfiguration::from(config),
        "existing preparation result uses a different resolved configuration"
    );
    Ok(())
}

fn ensure_workload_matches(
    result: &WorkloadResult,
    args: &RunArgs,
    config: &ResolvedConfig,
    golden: Option<&PreparationResult>,
) -> Result<()> {
    anyhow::ensure!(
        result.task == args.task,
        "existing result belongs to another task"
    );
    anyhow::ensure!(
        result.golden_id == args.golden,
        "existing result belongs to another golden ID"
    );
    anyhow::ensure!(
        result.session == args.session.as_deref().unwrap_or_default(),
        "existing result belongs to another session"
    );
    anyhow::ensure!(
        result.source.runner_commit == env!("BENCHMARK_RUNNER_COMMIT"),
        "existing result was created by a different runner commit"
    );
    anyhow::ensure!(
        result.source.slate_commit == env!("BENCHMARK_SLATE_COMMIT"),
        "existing result was created by a different SlateDB commit"
    );
    anyhow::ensure!(
        result.configuration == ResultConfiguration::from(config),
        "existing result uses a different resolved configuration"
    );
    if let Some(golden) = golden {
        anyhow::ensure!(
            result.initial_state.checkpoint_id.as_deref()
                == Some(golden.checkpoint.checkpoint_id.as_str())
                && result.initial_state.manifest_id == Some(golden.checkpoint.manifest_id)
                && result.initial_state.lsm_digest_sha256 == golden.checkpoint.lsm_digest_sha256,
            "existing result belongs to another golden checkpoint"
        );
    }
    Ok(())
}

fn ensure_shared_configuration(
    existing: &ResultConfiguration,
    config: &ResolvedConfig,
    include_settings: bool,
) -> Result<()> {
    let current = ResultConfiguration::from(config);
    anyhow::ensure!(
        existing.scale == current.scale,
        "golden scale does not match"
    );
    anyhow::ensure!(
        existing.dataset == current.dataset,
        "golden dataset does not match"
    );
    anyhow::ensure!(
        existing.caches == current.caches,
        "golden caches do not match"
    );
    anyhow::ensure!(
        existing.build_profile == current.build_profile,
        "golden build profile does not match"
    );
    anyhow::ensure!(
        existing.enabled_features == current.enabled_features,
        "golden SlateDB features do not match"
    );
    if include_settings {
        anyhow::ensure!(
            existing.slate_settings == current.slate_settings,
            "golden SlateDB settings do not match"
        );
    }
    Ok(())
}

fn bulk_load_config(config: &ResolvedConfig) -> Result<ResolvedConfig> {
    let mut config = config.clone();
    config.settings.compactor_options = None;
    config.settings.l0_max_ssts = u32::MAX as usize;
    config.settings.l0_max_ssts_per_key = u32::MAX as usize;
    config.slate_settings =
        serde_json::to_value(&config.settings).context("serializing bulk-load SlateDB settings")?;
    Ok(config)
}

fn dataset_metadata(
    config: &ResolvedConfig,
    checkpoint: &CheckpointReference,
) -> GoldenDatasetMetadata {
    GoldenDatasetMetadata {
        record_count: config.dataset.record_count,
        key_bytes: config.dataset.key_bytes,
        value_bytes: config.dataset.value_bytes,
        logical_bytes: config.dataset.logical_bytes(),
        live_sst_bytes: checkpoint.live_sst_bytes,
    }
}

async fn load_optional<T>(store: Arc<dyn ObjectStore>, path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let get = match store.get(path).await {
        Ok(get) => get,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("loading {path}")),
    };
    let bytes = get
        .bytes()
        .await
        .with_context(|| format!("reading {path}"))?;
    let value = serde_json::from_slice(&bytes).with_context(|| format!("parsing {path}"))?;
    Ok(Some(value))
}

async fn load_required<T>(store: Arc<dyn ObjectStore>, path: &Path) -> Result<T>
where
    T: DeserializeOwned,
{
    load_optional(store, path)
        .await?
        .with_context(|| format!("required result {path} does not exist"))
}

async fn load_required_bytes(store: Arc<dyn ObjectStore>, path: &Path) -> Result<Bytes> {
    store
        .get(path)
        .await
        .with_context(|| format!("loading {path}"))?
        .bytes()
        .await
        .with_context(|| format!("reading {path}"))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_series_digest(reference: &SeriesReference, bytes: &[u8]) -> Result<()> {
    anyhow::ensure!(
        reference.file == "series.json",
        "workload series file must be series.json"
    );
    anyhow::ensure!(
        sha256_bytes(bytes) == reference.sha256,
        "workload series digest does not match result.json"
    );
    Ok(())
}

async fn create_bytes(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    bytes: Vec<u8>,
    description: &str,
) -> Result<()> {
    store
        .put_opts(path, PutPayload::from(bytes), PutMode::Create.into())
        .await
        .with_context(|| format!("creating {description} {path}"))?;
    Ok(())
}

async fn create_result(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    value: &impl Serialize,
) -> Result<()> {
    create_bytes(
        store,
        path,
        serde_json::to_vec_pretty(value)?,
        "completion result",
    )
    .await
}

fn write_local_result(output: &FsPath, value: &impl Serialize) -> Result<()> {
    write_local_bytes(output, "result.json", &serde_json::to_vec_pretty(value)?)
}

fn write_local_bytes(output: &FsPath, name: &str, bytes: &[u8]) -> Result<()> {
    fs::write(output.join(name), bytes)
        .with_context(|| format!("writing {}/{name}", output.display()))
}

fn print_success(args: &RunArgs, skipped: bool) -> Result<()> {
    println!(
        "{}",
        serde_json::json!({
            "status": "ok",
            "task": args.task,
            "result": args.output.join("result.json"),
            "skipped": skipped,
        })
    );
    Ok(())
}

fn validate_name(value: &str, kind: &str) -> Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value != "."
            && value != ".."
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{kind} names must be 1-128 ASCII letters, digits, '.', '-', or '_'"
    );
    Ok(())
}

async fn clone_database(
    store: Arc<dyn ObjectStore>,
    clone_path: &Path,
    parent_path: &Path,
    checkpoint_id: Uuid,
) -> Result<()> {
    AdminBuilder::new(clone_path.clone(), store)
        .build()
        .create_clone_builder_from_source(CloneSourceSpec::with_checkpoint(
            parent_path.clone(),
            checkpoint_id,
        ))
        .build()
        .await
        .context("creating shallow database clone")
}

async fn open_database(
    path: Path,
    store: Arc<dyn ObjectStore>,
    config: &ResolvedConfig,
    settings: &Settings,
    object_store_cache_root: Option<&FsPath>,
    recorder: Arc<BenchmarkMetricsRecorder>,
) -> Result<Arc<Db>> {
    let mut settings = settings.clone();
    settings.object_store_cache_options.root_folder =
        object_store_cache_root.map(FsPath::to_path_buf);
    settings.object_store_cache_options.max_cache_size_bytes = Some(
        usize::try_from(config.caches.object_store_bytes)
            .context("object-store cache capacity exceeds the platform limit")?,
    );
    let metrics: Arc<dyn MetricsRecorder> = recorder;
    let db = Db::builder(path, store)
        .with_settings(settings)
        .with_metrics_recorder(metrics)
        .with_db_cache(cache_for(config))
        .build()
        .await
        .context("opening SlateDB")?;
    Ok(Arc::new(db))
}

fn cache_for(config: &ResolvedConfig) -> Arc<dyn DbCache> {
    let block: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: config.caches.block_bytes,
        ..Default::default()
    }));
    let metadata: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: config.caches.metadata_bytes,
        ..Default::default()
    }));
    Arc::new(
        SplitCache::new()
            .with_block_cache(Some(block))
            .with_meta_cache(Some(metadata))
            .build(),
    )
}

fn object_store_cache_directory() -> Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix("slatedb-benchmark-object-store-cache-")
        .tempdir()
        .context("creating temporary object-store cache")
}

async fn close_database_after<T>(
    db: &Db,
    operation: Result<T>,
    close_context: &'static str,
) -> Result<T> {
    let close = db.close().await.context(close_context);
    let value = operation?;
    close?;
    Ok(value)
}

fn ensure_measurement_has_no_application_errors(measurement: &SampledMeasurement) -> Result<()> {
    let errors = measurement.application_errors();
    anyhow::ensure!(errors == 0, "preparation recorded {errors} API errors");
    log_nonfatal_object_store_attempt_errors(measurement, "preparation");
    Ok(())
}

fn log_nonfatal_object_store_attempt_errors(measurement: &SampledMeasurement, task_kind: &str) {
    let errors = measurement.object_store_attempt_errors();
    if errors > 0 {
        tracing::warn!(
            task_kind,
            errors,
            "object-store request attempts failed without failing the task"
        );
    }
}

async fn checkpoint_database(
    path: Path,
    store: Arc<dyn ObjectStore>,
    name: &str,
) -> Result<CheckpointReference> {
    let admin = AdminBuilder::new(path.clone(), store).build();
    let checkpoint = admin
        .create_detached_checkpoint(&CheckpointOptions {
            lifetime: None,
            source: None,
            name: Some(name.to_string()),
        })
        .await
        .context("creating detached checkpoint")?;
    let manifest = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .context("reading checkpoint manifest")?
        .context("checkpoint manifest does not exist")?;
    Ok(CheckpointReference {
        database_path: path.to_string(),
        checkpoint_id: checkpoint.id.to_string(),
        manifest_id: checkpoint.manifest_id,
        lsm_digest_sha256: manifest_lsm_digest(&manifest)?,
        live_sst_bytes: live_database_size_bytes(&manifest),
    })
}

fn parse_checkpoint_id(checkpoint: &CheckpointReference) -> Result<Uuid> {
    checkpoint
        .checkpoint_id
        .parse()
        .context("checkpoint ID is invalid")
}

async fn verify_checkpoint_reference(
    store: Arc<dyn ObjectStore>,
    checkpoint: &CheckpointReference,
) -> Result<()> {
    let id = parse_checkpoint_id(checkpoint)?;
    let admin = AdminBuilder::new(Path::from(checkpoint.database_path.clone()), store).build();
    let found = admin
        .list_checkpoints(None)
        .await
        .context("listing database checkpoints")?
        .into_iter()
        .find(|candidate| candidate.id == id)
        .context("recorded checkpoint no longer exists")?;
    anyhow::ensure!(
        found.manifest_id == checkpoint.manifest_id,
        "recorded checkpoint manifest changed"
    );
    let manifest = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .context("reading recorded checkpoint manifest")?
        .context("recorded checkpoint manifest no longer exists")?;
    anyhow::ensure!(
        manifest_lsm_digest(&manifest)? == checkpoint.lsm_digest_sha256,
        "recorded checkpoint LSM digest changed"
    );
    anyhow::ensure!(
        live_database_size_bytes(&manifest) == checkpoint.live_sst_bytes,
        "recorded checkpoint live size changed"
    );
    Ok(())
}

async fn validate_uncompacted_checkpoint(
    store: Arc<dyn ObjectStore>,
    checkpoint: &CheckpointReference,
) -> Result<()> {
    let manifest = AdminBuilder::new(Path::from(checkpoint.database_path.clone()), store)
        .build()
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .context("reading bulk-load checkpoint manifest")?
        .context("bulk-load checkpoint manifest does not exist")?;
    let l0_count = manifest.l0().len()
        + manifest
            .segments()
            .iter()
            .map(|segment| segment.l0().len())
            .sum::<usize>();
    let sorted_run_count = manifest.compacted().len()
        + manifest
            .segments()
            .iter()
            .map(|segment| segment.compacted().len())
            .sum::<usize>();
    anyhow::ensure!(l0_count > 0, "bulk-load checkpoint contains no L0 SSTs");
    anyhow::ensure!(
        sorted_run_count == 0,
        "bulk-load checkpoint contains compacted sorted runs"
    );
    Ok(())
}

async fn validate_compacted_checkpoint(
    store: Arc<dyn ObjectStore>,
    checkpoint: &CheckpointReference,
) -> Result<()> {
    let manifest = AdminBuilder::new(Path::from(checkpoint.database_path.clone()), store)
        .build()
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .context("reading full-compaction checkpoint manifest")?
        .context("full-compaction checkpoint manifest does not exist")?;
    validate_fully_compacted_manifest(&manifest)
}

async fn compact_database_fully(database_path: Path, store: Arc<dyn ObjectStore>) -> Result<()> {
    let admin = AdminBuilder::new(database_path, store).build();
    loop {
        wait_for_compactor_quiet(&admin).await?;
        let state = admin
            .read_compactor_state_view()
            .await
            .context("reading compactor state before full compaction")?;
        let Some(spec) = next_full_compaction_spec(state.manifest())? else {
            validate_fully_compacted_manifest(state.manifest())?;
            return Ok(());
        };
        let compaction = admin
            .submit_compaction(spec)
            .await
            .context("submitting full-database compaction")?;
        wait_for_submitted_compaction(&admin, &compaction).await?;
    }
}

async fn wait_for_compactor_quiet(admin: &Admin) -> Result<()> {
    let mut stable_since = Instant::now();
    let mut last_state = None;
    loop {
        let state = admin
            .read_compactor_state_view()
            .await
            .context("reading compactor state while waiting for idle")?;
        let state_version = (
            state.manifest().id(),
            state.compactions().map(|compactions| compactions.id()),
        );
        if last_state != Some(state_version) {
            last_state = Some(state_version);
            stable_since = Instant::now();
        }
        let mut active = false;
        if let Some(compactions) = state.compactions() {
            for compaction in compactions.recent_compactions() {
                match compaction.status() {
                    CompactionStatus::Submitted
                    | CompactionStatus::Scheduled
                    | CompactionStatus::Running
                    | CompactionStatus::Compacted => active = true,
                    CompactionStatus::Failed => {
                        bail!("compaction {} failed", compaction.id());
                    }
                    CompactionStatus::Completed => {}
                }
            }
        }
        if !active && stable_since.elapsed() >= COMPACTION_QUIET {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_submitted_compaction(admin: &Admin, submitted: &Compaction) -> Result<()> {
    let id = submitted.id();
    loop {
        let compaction = admin
            .read_compaction(id, None)
            .await
            .with_context(|| format!("reading submitted compaction {id}"))?
            .with_context(|| format!("submitted compaction {id} disappeared"))?;
        match compaction.status() {
            CompactionStatus::Completed => return Ok(()),
            CompactionStatus::Failed => bail!("full-database compaction {id} failed"),
            CompactionStatus::Submitted
            | CompactionStatus::Scheduled
            | CompactionStatus::Running
            | CompactionStatus::Compacted => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn ensure_no_failed_compactions(path: Path, store: Arc<dyn ObjectStore>) -> Result<()> {
    let state = AdminBuilder::new(path, store)
        .build()
        .read_compactor_state_view()
        .await
        .context("reading compactor state after workload")?;
    if let Some(compactions) = state.compactions() {
        for compaction in compactions.recent_compactions() {
            if compaction.status() == CompactionStatus::Failed {
                bail!("compaction {} failed during the workload", compaction.id());
            }
        }
    }
    Ok(())
}

fn next_full_compaction_spec(manifest: &VersionedManifest) -> Result<Option<CompactionSpec>> {
    let fresh_destination = manifest
        .compacted()
        .iter()
        .map(|run| run.id)
        .chain(
            manifest
                .segments()
                .iter()
                .flat_map(|segment| segment.compacted().iter().map(|run| run.id)),
        )
        .max()
        .map_or(Ok(0), |id| {
            id.checked_add(1).context("sorted-run ID space exhausted")
        })?;
    let root_sources = manifest
        .l0()
        .iter()
        .map(|view| SourceId::SstView(view.id))
        .chain(
            manifest
                .compacted()
                .iter()
                .map(|run| SourceId::SortedRun(run.id)),
        )
        .collect();
    if let Some(spec) = full_tree_compaction_spec(
        Bytes::new(),
        root_sources,
        manifest.compacted().iter().map(|run| run.id).collect(),
        manifest.l0().len(),
        fresh_destination,
    ) {
        return Ok(Some(spec));
    }
    for segment in manifest.segments() {
        let sources = segment
            .l0()
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .chain(
                segment
                    .compacted()
                    .iter()
                    .map(|run| SourceId::SortedRun(run.id)),
            )
            .collect();
        if let Some(spec) = full_tree_compaction_spec(
            segment.prefix().clone(),
            sources,
            segment.compacted().iter().map(|run| run.id).collect(),
            segment.l0().len(),
            fresh_destination,
        ) {
            return Ok(Some(spec));
        }
    }
    Ok(None)
}

fn full_tree_compaction_spec(
    segment: Bytes,
    sources: Vec<SourceId>,
    sorted_run_ids: Vec<u32>,
    l0_count: usize,
    fresh_destination: u32,
) -> Option<CompactionSpec> {
    if l0_count == 0 && sorted_run_ids.len() <= 1 {
        return None;
    }
    let destination = sorted_run_ids
        .into_iter()
        .min()
        .unwrap_or(fresh_destination);
    Some(CompactionSpec::for_segment(segment, sources, destination))
}

fn validate_fully_compacted_manifest(manifest: &VersionedManifest) -> Result<()> {
    anyhow::ensure!(
        manifest.l0().is_empty() && manifest.compacted().len() <= 1,
        "full compaction left root L0 or multiple sorted runs"
    );
    for segment in manifest.segments() {
        anyhow::ensure!(
            segment.l0().is_empty() && segment.compacted().len() <= 1,
            "full compaction left segment L0 or multiple sorted runs"
        );
    }
    Ok(())
}

fn lsm_digest(db: &Db) -> Result<String> {
    manifest_lsm_digest(&db.status().current_manifest)
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

#[cfg(test)]
mod tests {
    use super::{full_tree_compaction_spec, sha256_bytes, validate_name, validate_series_digest};
    use crate::model::SeriesReference;
    use bytes::Bytes;
    use slatedb::compactor::SourceId;

    #[test]
    fn names_reject_path_components() {
        assert!(validate_name("golden-1", "golden").is_ok());
        assert!(validate_name("../golden", "golden").is_err());
        assert!(validate_name("golden/one", "golden").is_err());
    }

    #[test]
    fn one_sorted_run_requires_no_more_full_compaction() {
        assert!(full_tree_compaction_spec(
            Bytes::new(),
            vec![SourceId::SortedRun(7)],
            vec![7],
            0,
            8,
        )
        .is_none());
    }

    #[test]
    fn workload_series_digest_detects_modified_bytes() {
        let reference = SeriesReference {
            file: "series.json".to_string(),
            sha256: sha256_bytes(b"original"),
        };
        validate_series_digest(&reference, b"original").expect("matching digest");
        assert!(validate_series_digest(&reference, b"modified").is_err());
    }
}
