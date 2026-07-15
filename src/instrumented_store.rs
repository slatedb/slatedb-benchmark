use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as StoreResult,
    UploadPart,
};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct StoreMetrics {
    put: AtomicU64,
    get: AtomicU64,
    head: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    copy: AtomicU64,
    create_multipart: AtomicU64,
    complete_multipart: AtomicU64,
    abort_multipart: AtomicU64,
    errors: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    objects: Mutex<BTreeMap<String, u64>>,
}

#[derive(Debug, Clone, Default)]
pub struct StoreSnapshot {
    pub requests: BTreeMap<String, u64>,
    pub errors: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

impl StoreMetrics {
    pub fn snapshot(&self) -> StoreSnapshot {
        let mut requests = BTreeMap::new();
        for (name, value) in [
            ("put", self.put.load(Ordering::Relaxed)),
            ("get", self.get.load(Ordering::Relaxed)),
            ("head", self.head.load(Ordering::Relaxed)),
            ("list", self.list.load(Ordering::Relaxed)),
            ("delete", self.delete.load(Ordering::Relaxed)),
            ("copy", self.copy.load(Ordering::Relaxed)),
            (
                "create_multipart",
                self.create_multipart.load(Ordering::Relaxed),
            ),
            (
                "complete_multipart",
                self.complete_multipart.load(Ordering::Relaxed),
            ),
            (
                "abort_multipart",
                self.abort_multipart.load(Ordering::Relaxed),
            ),
        ] {
            requests.insert(name.to_string(), value);
        }
        StoreSnapshot {
            requests,
            errors: self.errors.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }

    pub fn prefix_bytes(&self, prefix: &Path) -> u64 {
        let prefix = prefix.to_string();
        let child_prefix = format!("{prefix}/");
        self.objects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|(location, _)| *location == &prefix || location.starts_with(&child_prefix))
            .map(|(_, size)| *size)
            .sum()
    }

    fn record_object(&self, location: &Path, size: u64) {
        self.objects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(location.to_string(), size);
    }

    fn remove_object(&self, location: &Path) -> Option<u64> {
        self.objects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&location.to_string())
    }

    fn object_size(&self, location: &Path) -> Option<u64> {
        self.objects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&location.to_string())
            .copied()
    }
}

impl StoreSnapshot {
    pub fn difference(&self, start: &Self) -> Self {
        let requests = self
            .requests
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    value.saturating_sub(start.requests.get(name).copied().unwrap_or(0)),
                )
            })
            .collect();
        Self {
            requests,
            errors: self.errors.saturating_sub(start.errors),
            bytes_read: self.bytes_read.saturating_sub(start.bytes_read),
            bytes_written: self.bytes_written.saturating_sub(start.bytes_written),
        }
    }
}

#[derive(Clone)]
pub struct InstrumentedStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<StoreMetrics>,
}

impl InstrumentedStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            metrics: Arc::new(StoreMetrics::default()),
        }
    }

    pub fn metrics(&self) -> Arc<StoreMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Record objects that predate this wrapper instance without charging the
    /// benchmark's request counters. This is used before measurement when a
    /// database is opened by a fresh process.
    pub async fn seed_prefix(&self, prefix: &Path) -> StoreResult<()> {
        let objects = self
            .inner
            .list(Some(prefix))
            .try_collect::<Vec<_>>()
            .await?;
        for object in objects {
            self.metrics.record_object(&object.location, object.size);
        }
        Ok(())
    }

    fn error<T>(&self, result: &StoreResult<T>) {
        if result.is_err() {
            self.metrics.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Debug for InstrumentedStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstrumentedStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Display for InstrumentedStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "instrumented({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for InstrumentedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.metrics.put.fetch_add(1, Ordering::Relaxed);
        let size = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, options).await;
        self.error(&result);
        if result.is_ok() {
            self.metrics
                .bytes_written
                .fetch_add(size, Ordering::Relaxed);
            self.metrics.record_object(location, size);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.metrics
            .create_multipart
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put_multipart_opts(location, options).await;
        self.error(&result);
        result.map(|inner| {
            Box::new(InstrumentedUpload {
                inner,
                metrics: Arc::clone(&self.metrics),
                location: location.clone(),
                object_bytes: 0,
            }) as Box<dyn MultipartUpload>
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let is_head = options.head;
        let counter = if is_head {
            &self.metrics.head
        } else {
            &self.metrics.get
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_opts(location, options).await;
        self.error(&result);
        if let (Ok(get), false) = (&result, is_head) {
            self.metrics.bytes_read.fetch_add(
                get.range.end.saturating_sub(get.range.start),
                Ordering::Relaxed,
            );
        }
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> StoreResult<Vec<Bytes>> {
        self.metrics
            .get
            .fetch_add(ranges.len() as u64, Ordering::Relaxed);
        let result = self.inner.get_ranges(location, ranges).await;
        self.error(&result);
        if let Ok(values) = &result {
            let size = values.iter().map(|value| value.len() as u64).sum::<u64>();
            self.metrics.bytes_read.fetch_add(size, Ordering::Relaxed);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        let metrics = Arc::clone(&self.metrics);
        self.inner
            .delete_stream(locations)
            .map(move |result| {
                metrics.delete.fetch_add(1, Ordering::Relaxed);
                match &result {
                    Ok(location) => {
                        metrics.remove_object(location);
                    }
                    Err(_) => {
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                result
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.metrics.list.fetch_add(1, Ordering::Relaxed);
        let metrics = Arc::clone(&self.metrics);
        self.inner
            .list(prefix)
            .map(move |result| {
                if result.is_err() {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                }
                result
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.metrics.list.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.list_with_delimiter(prefix).await;
        self.error(&result);
        result
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.metrics.copy.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.copy_opts(from, to, options).await;
        self.error(&result);
        if result.is_ok() {
            if let Some(size) = self.metrics.object_size(from) {
                self.metrics.record_object(to, size);
            }
        }
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.metrics.copy.fetch_add(1, Ordering::Relaxed);
        self.metrics.delete.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.rename_opts(from, to, options).await;
        self.error(&result);
        if result.is_ok() {
            if let Some(size) = self.metrics.remove_object(from) {
                self.metrics.record_object(to, size);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::InstrumentedStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};
    use std::sync::Arc;

    #[tokio::test]
    async fn counts_head_separately_from_get() {
        let store = InstrumentedStore::new(Arc::new(InMemory::new()));
        let location = Path::from("object");
        let payload = PutPayload::from_static(b"value");
        store
            .put(&location, payload)
            .await
            .expect("store test object");

        let meta = store.head(&location).await.expect("head test object");

        assert_eq!(meta.size, 5);
        let after_head = store.metrics().snapshot();
        assert_eq!(after_head.requests.get("head"), Some(&1));
        assert_eq!(after_head.requests.get("get"), Some(&0));
        assert_eq!(after_head.bytes_read, 0);

        store.get(&location).await.expect("get test object");

        let after_get = store.metrics().snapshot();
        assert_eq!(after_get.requests.get("head"), Some(&1));
        assert_eq!(after_get.requests.get("get"), Some(&1));
        assert_eq!(after_get.bytes_read, 5);
    }
}

#[derive(Debug)]
struct InstrumentedUpload {
    inner: Box<dyn MultipartUpload>,
    metrics: Arc<StoreMetrics>,
    location: Path,
    object_bytes: u64,
}

#[async_trait]
impl MultipartUpload for InstrumentedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let size = data.content_length() as u64;
        self.metrics.put.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_written
            .fetch_add(size, Ordering::Relaxed);
        self.object_bytes = self.object_bytes.saturating_add(size);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        self.metrics
            .complete_multipart
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.complete().await;
        if result.is_ok() {
            self.metrics
                .record_object(&self.location, self.object_bytes);
        } else {
            self.metrics.errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn abort(&mut self) -> StoreResult<()> {
        self.metrics.abort_multipart.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.abort().await;
        if result.is_err() {
            self.metrics.errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}
