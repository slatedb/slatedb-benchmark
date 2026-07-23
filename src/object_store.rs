use crate::instrumented_http::InstrumentedHttpConnector;
use crate::instrumented_store::{InstrumentedStore, StoreMetrics};
use anyhow::{bail, Context, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::ObjectStore;
use std::env;
use std::sync::Arc;

pub struct ObjectStoreContext {
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
        let metrics = Arc::new(StoreMetrics::default());
        let (raw, control, endpoint): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>, String) =
            match provider.to_ascii_lowercase().as_str() {
                "aws" => {
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
                    bail!("unsupported CLOUD_PROVIDER {other}; expected aws, memory, or local")
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

#[cfg(test)]
mod tests {
    use super::configured_s3_endpoint;
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

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
}
