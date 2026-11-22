//! Object-store construction, instrumentation, and prefix cleanup.

use crate::instrumented_http::InstrumentedHttpConnector;
use crate::instrumented_store::{InstrumentedStore, StoreMetrics};
use anyhow::{bail, Context, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::{BackoffConfig, ObjectStore, RetryConfig};
use std::env;
use std::sync::Arc;
use std::time::Duration;

const DELETE_BATCH_SIZE: usize = 1_000;

/// Object stores and location metadata used during one runner invocation.
///
/// Workload data flows through `instrumented`, while runner control-plane
/// reads and writes use `control` so they do not contaminate workload metrics.
pub struct ObjectStoreContext {
    /// Store used by SlateDB and included in published object-store metrics.
    pub instrumented: Arc<InstrumentedStore>,
    /// A store with no benchmark metrics, used for runner control-plane operations.
    pub control: Arc<dyn ObjectStore>,
    /// Root prefix under which golden datasets and sessions are stored.
    pub root: Path,
    /// Normalized provider name.
    pub provider: String,
    /// Configured endpoint description.
    pub endpoint: String,
    /// Configured object-store region.
    pub region: String,
}

impl ObjectStoreContext {
    /// Builds object stores from benchmark and AWS environment variables.
    pub fn load() -> Result<Self> {
        let provider = env::var("CLOUD_PROVIDER")
            .unwrap_or_else(|_| "s3".to_string())
            .to_ascii_lowercase();
        let metrics = Arc::new(StoreMetrics::default());
        let (raw, control, endpoint): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>, String) =
            match provider.as_str() {
                "s3" => {
                    let bucket = env::var("SLATEDB_BENCH_BUCKET")
                        .or_else(|_| env::var("AWS_BUCKET_NAME"))
                        .context("SLATEDB_BENCH_BUCKET is required")?;
                    let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                    let configured_endpoint = configured_s3_endpoint(&builder);
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
                            .with_retry(control_retry_config())
                            .build()
                            .context("building S3-compatible control store")?,
                    );
                    let endpoint = configured_endpoint.unwrap_or_else(|| "AWS default".to_string());
                    (raw, control, endpoint)
                }
                "memory" => {
                    let store: Arc<dyn ObjectStore> =
                        Arc::new(object_store::memory::InMemory::new());
                    (Arc::clone(&store), store, "memory".to_string())
                }
                "local" => {
                    let path = env::var("LOCAL_PATH").context("LOCAL_PATH is required")?;
                    let store: Arc<dyn ObjectStore> = Arc::new(
                        object_store::local::LocalFileSystem::new_with_prefix(&path)
                            .context("building local object store")?,
                    );
                    (Arc::clone(&store), store, path)
                }
                other => {
                    bail!("unsupported CLOUD_PROVIDER {other}; expected s3, memory, or local")
                }
            };
        let prefix = env::var("SLATEDB_BENCH_PREFIX").unwrap_or_else(|_| "manual".to_string());
        let region = env::var("SLATEDB_BENCH_REGION")
            .or_else(|_| env::var("AWS_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());
        let instrumented = Arc::new(InstrumentedStore::with_metrics(Arc::clone(&raw), metrics));
        Ok(Self {
            instrumented,
            control,
            root: Path::from(prefix),
            provider,
            endpoint,
            region,
        })
    }
}

fn configured_s3_endpoint(builder: &AmazonS3Builder) -> Option<String> {
    builder
        .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
        .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::Endpoint))
}

fn control_retry_config() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(15),
            base: 2.0,
        },
        max_retries: 15,
        retry_timeout: Duration::from_secs(3 * 60),
    }
}

/// Deletes every object below `prefix`.
///
/// Chunks object keys into sequential 1,000-key batches so S3 bulk
/// `DeleteObjects` calls stay within S3's 3,500 DELETE/s per-prefix limit
/// instead of fanning out `object_store`'s default 20 concurrent batches.
pub async fn delete_prefix(store: Arc<dyn ObjectStore>, prefix: &Path) -> Result<()> {
    let mut batches = store
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .try_chunks(DELETE_BATCH_SIZE);
    while let Some(batch) = batches.next().await {
        let batch = batch
            .map_err(|error| error.1)
            .context("listing object-store prefix for deletion")?;
        let locations: BoxStream<'static, object_store::Result<Path>> =
            futures::stream::iter(batch.into_iter().map(Ok)).boxed();
        store
            .delete_stream(locations)
            .try_collect::<Vec<_>>()
            .await
            .context("deleting object-store prefix")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{configured_s3_endpoint, delete_prefix, DELETE_BATCH_SIZE};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::sync::Arc;

    #[test]
    fn configured_endpoint_uses_builder_precedence_without_a_fabricated_default() {
        assert_eq!(configured_s3_endpoint(&AmazonS3Builder::new()), None);

        let builder = AmazonS3Builder::new()
            .with_config(AmazonS3ConfigKey::Endpoint, "https://generic.example")
            .with_config(AmazonS3ConfigKey::S3Endpoint, "https://s3-specific.example");
        assert_eq!(
            configured_s3_endpoint(&builder).as_deref(),
            Some("https://s3-specific.example")
        );
    }

    #[tokio::test]
    async fn delete_prefix_deletes_multiple_batches_and_preserves_siblings() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let total = DELETE_BATCH_SIZE + 50;
        for index in 0..total {
            let path = Path::from(format!("session/db/obj-{index:04}"));
            store
                .put(&path, Bytes::from_static(b"x").into())
                .await
                .expect("seed target object");
        }
        let sibling = Path::from("session/keep/result.json");
        store
            .put(&sibling, Bytes::from_static(b"{}").into())
            .await
            .expect("seed sibling object");

        delete_prefix(Arc::clone(&store), &Path::from("session/db"))
            .await
            .expect("delete target prefix");

        let remaining_target: Vec<_> = store
            .list(Some(&Path::from("session/db")))
            .try_collect()
            .await
            .expect("list target prefix");
        assert!(remaining_target.is_empty());

        let remaining_sibling: Vec<_> = store
            .list(Some(&Path::from("session/keep")))
            .try_collect()
            .await
            .expect("list sibling prefix");
        assert_eq!(remaining_sibling.len(), 1);
    }
}
