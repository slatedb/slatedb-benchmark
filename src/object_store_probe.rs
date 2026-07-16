use crate::config::ProbeConfig;
use crate::histogram::LatencyHistogram;
use crate::instrumented_http::InstrumentedHttpConnector;
use crate::instrumented_store::{InstrumentedStore, StoreMetrics};
use crate::model::{EncodedHistogram, ObjectStoreBaseline};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use chrono::Utc;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use rand::RngCore;
use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use uuid::Uuid;

pub struct ObjectStoreContext {
    pub raw: Arc<dyn ObjectStore>,
    pub instrumented: Arc<InstrumentedStore>,
    /// A store with no benchmark metrics, used for runner control-plane operations.
    pub control: Arc<dyn ObjectStore>,
    pub root: Path,
    pub provider: String,
    pub endpoint: String,
    pub region: String,
}

impl ObjectStoreContext {
    pub fn load() -> Result<Self> {
        let provider = env::var("CLOUD_PROVIDER").unwrap_or_else(|_| "aws".to_string());
        let configured_endpoint = env::var("AWS_ENDPOINT_URL_S3")
            .or_else(|_| env::var("AWS_ENDPOINT"))
            .ok();
        let endpoint = configured_endpoint
            .clone()
            .unwrap_or_else(|| "https://t3.storage.dev".to_string());
        let metrics = Arc::new(StoreMetrics::default());
        let (raw, control): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>) = match provider
            .to_ascii_lowercase()
            .as_str()
        {
            "aws" => {
                let bucket = env::var("SLATEDB_BENCH_BUCKET")
                    .or_else(|_| env::var("AWS_BUCKET_NAME"))
                    .context("SLATEDB_BENCH_BUCKET is required")?;
                let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                let raw: Arc<dyn ObjectStore> = Arc::new(
                    builder
                        .clone()
                        .with_http_connector(InstrumentedHttpConnector::new(
                            Arc::clone(&metrics),
                            configured_endpoint.as_deref(),
                        ))
                        .build()
                        .context("building S3-compatible object store")?,
                );
                let control: Arc<dyn ObjectStore> = Arc::new(
                    builder
                        .build()
                        .context("building S3-compatible control store")?,
                );
                (raw, control)
            }
            "memory" => {
                let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
                (Arc::clone(&store), store)
            }
            "local" => {
                let path = env::var("LOCAL_PATH").context("LOCAL_PATH is required")?;
                let store: Arc<dyn ObjectStore> = Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(path)
                        .context("building local object store")?,
                );
                (Arc::clone(&store), store)
            }
            other => bail!("unsupported CLOUD_PROVIDER {other}; expected aws, memory, or local"),
        };
        let prefix = env::var("SLATEDB_BENCH_PREFIX").unwrap_or_else(|_| "manual".to_string());
        let region = env::var("SLATEDB_BENCH_REGION")
            .or_else(|_| env::var("AWS_REGION"))
            .unwrap_or_else(|_| "fra".to_string());
        let instrumented = Arc::new(InstrumentedStore::with_metrics(Arc::clone(&raw), metrics));
        Ok(Self {
            raw,
            instrumented,
            control,
            root: Path::from(prefix),
            provider,
            endpoint,
            region,
        })
    }
}

pub async fn probe(
    store: Arc<dyn ObjectStore>,
    root: &Path,
    config: &ProbeConfig,
) -> Result<(ObjectStoreBaseline, BTreeMap<String, EncodedHistogram>)> {
    let prefix = root
        .clone()
        .join(format!("object-store-probe-{}", Uuid::new_v4()));
    let result = run_probe(Arc::clone(&store), &prefix, config).await;
    let cleanup = delete_prefix(store, &prefix).await;
    match (result, cleanup) {
        (Ok(baseline), Ok(())) => Ok(baseline),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.context("cleaning up object-store probe")),
    }
}

async fn run_probe(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    config: &ProbeConfig,
) -> Result<(ObjectStoreBaseline, BTreeMap<String, EncodedHistogram>)> {
    let latency_payload = random_bytes(config.latency_object_bytes);
    let mut put_latency = LatencyHistogram::new();
    let mut get_latency = LatencyHistogram::new();
    let mut latency_paths = Vec::with_capacity(config.latency_operations as usize);

    for index in 0..config.latency_operations {
        let path = prefix.clone().join("latency").join(format!("{index:08}"));
        let started = Instant::now();
        store
            .put(&path, PutPayload::from_bytes(latency_payload.clone()))
            .await
            .with_context(|| format!("latency PUT {path}"))?;
        put_latency.record(started.elapsed());
        latency_paths.push(path);
    }
    for path in &latency_paths {
        let started = Instant::now();
        let bytes = store
            .get(path)
            .await
            .with_context(|| format!("latency GET {path}"))?
            .bytes()
            .await
            .with_context(|| format!("reading latency GET {path}"))?;
        if bytes.len() != config.latency_object_bytes {
            bail!("object-store latency probe returned an unexpected object size");
        }
        get_latency.record(started.elapsed());
    }

    let throughput_payload = random_bytes(config.throughput_object_bytes);
    let warmup = Duration::from_millis(config.throughput_warmup_ms);
    let measurement = Duration::from_millis(config.throughput_measurement_ms);
    run_upload_phase(
        Arc::clone(&store),
        prefix,
        throughput_payload.clone(),
        config.throughput_concurrency,
        warmup,
        false,
    )
    .await?;
    let (upload_bytes, upload_elapsed) = run_upload_phase(
        Arc::clone(&store),
        prefix,
        throughput_payload.clone(),
        config.throughput_concurrency,
        measurement,
        true,
    )
    .await?;

    let download_paths = prepare_download_objects(
        Arc::clone(&store),
        prefix,
        throughput_payload,
        config.throughput_concurrency,
    )
    .await?;
    run_download_phase(Arc::clone(&store), &download_paths, warmup).await?;
    let (download_bytes, download_elapsed) =
        run_download_phase(store, &download_paths, measurement).await?;

    let baseline = ObjectStoreBaseline {
        measured_at: Utc::now().to_rfc3339(),
        put_latency: put_latency.summary(),
        get_latency: get_latency.summary(),
        upload_mib_per_second: throughput_mib(upload_bytes, upload_elapsed),
        download_mib_per_second: throughput_mib(download_bytes, download_elapsed),
    };
    let histograms = BTreeMap::from([
        ("object_store/put".to_string(), put_latency.encode()?),
        ("object_store/get".to_string(), get_latency.encode()?),
    ]);
    Ok((baseline, histograms))
}

async fn run_upload_phase(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    payload: Bytes,
    concurrency: usize,
    duration: Duration,
    measured: bool,
) -> Result<(u64, Duration)> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut tasks = JoinSet::new();
    for worker in 0..concurrency {
        let store = Arc::clone(&store);
        let prefix = prefix.clone();
        let payload = payload.clone();
        tasks.spawn(async move {
            let mut bytes = 0_u64;
            let mut sequence = 0_u64;
            while Instant::now() < deadline {
                let phase = if measured { "measure" } else { "warmup" };
                let path = prefix
                    .clone()
                    .join("upload")
                    .join(phase)
                    .join(worker.to_string())
                    .join(sequence.to_string());
                store
                    .put(&path, PutPayload::from_bytes(payload.clone()))
                    .await?;
                bytes = bytes.saturating_add(payload.len() as u64);
                sequence += 1;
            }
            Ok::<u64, object_store::Error>(bytes)
        });
    }
    let mut bytes = 0_u64;
    while let Some(result) = tasks.join_next().await {
        bytes = bytes.saturating_add(result.context("joining upload probe worker")??);
    }
    Ok((bytes, started.elapsed()))
}

async fn prepare_download_objects(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    payload: Bytes,
    concurrency: usize,
) -> Result<Vec<Path>> {
    let paths = (0..concurrency)
        .map(|worker| prefix.clone().join("download").join(worker.to_string()))
        .collect::<Vec<_>>();
    stream::iter(paths.iter().cloned())
        .map(Ok::<_, object_store::Error>)
        .try_for_each_concurrent(concurrency, |path| {
            let store = Arc::clone(&store);
            let payload = payload.clone();
            async move {
                store.put(&path, PutPayload::from_bytes(payload)).await?;
                Ok(())
            }
        })
        .await
        .context("preparing download throughput objects")?;
    Ok(paths)
}

async fn run_download_phase(
    store: Arc<dyn ObjectStore>,
    paths: &[Path],
    duration: Duration,
) -> Result<(u64, Duration)> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut tasks = JoinSet::new();
    for path in paths {
        let path = path.clone();
        let store = Arc::clone(&store);
        tasks.spawn(async move {
            let mut bytes = 0_u64;
            while Instant::now() < deadline {
                let value = store.get(&path).await?.bytes().await?;
                bytes = bytes.saturating_add(value.len() as u64);
            }
            Ok::<u64, object_store::Error>(bytes)
        });
    }
    let mut bytes = 0_u64;
    while let Some(result) = tasks.join_next().await {
        bytes = bytes.saturating_add(result.context("joining download probe worker")??);
    }
    Ok((bytes, started.elapsed()))
}

fn throughput_mib(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON)
}

fn random_bytes(size: usize) -> Bytes {
    let mut value = vec![0_u8; size];
    rand::rng().fill_bytes(&mut value);
    Bytes::from(value)
}

pub async fn delete_prefix(store: Arc<dyn ObjectStore>, prefix: &Path) -> Result<()> {
    let locations: BoxStream<'static, object_store::Result<Path>> = store
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .boxed();
    store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .context("deleting object-store prefix")?;
    Ok(())
}
