use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as StoreResult,
    UploadPart,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const STORE_OPERATION_NAMES: [&str; 9] = [
    "put",
    "get",
    "head",
    "list",
    "delete",
    "copy",
    "create_multipart",
    "complete_multipart",
    "abort_multipart",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreOperation {
    Put = 0,
    Get = 1,
    Head = 2,
    List = 3,
    Delete = 4,
    Copy = 5,
    CreateMultipart = 6,
    CompleteMultipart = 7,
    AbortMultipart = 8,
}

impl StoreOperation {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug)]
struct HttpRequestMetrics {
    requests: [AtomicU64; STORE_OPERATION_NAMES.len()],
    successful_requests: [AtomicU64; STORE_OPERATION_NAMES.len()],
    request_errors: [AtomicU64; STORE_OPERATION_NAMES.len()],
    client_errors: [AtomicU64; STORE_OPERATION_NAMES.len()],
    server_errors: [AtomicU64; STORE_OPERATION_NAMES.len()],
    transport_errors: [AtomicU64; STORE_OPERATION_NAMES.len()],
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl Default for HttpRequestMetrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            successful_requests: std::array::from_fn(|_| AtomicU64::new(0)),
            request_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            client_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            server_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            transport_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }
}

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
    operation_bytes_read: AtomicU64,
    operation_bytes_written: AtomicU64,
    http: HttpRequestMetrics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshot {
    pub operations: BTreeMap<String, u64>,
    pub requests: BTreeMap<String, u64>,
    pub successful_requests: BTreeMap<String, u64>,
    pub request_errors: BTreeMap<String, u64>,
    pub client_errors: BTreeMap<String, u64>,
    pub server_errors: BTreeMap<String, u64>,
    pub transport_errors: BTreeMap<String, u64>,
    pub errors: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub operation_bytes_read: u64,
    pub operation_bytes_written: u64,
}

impl StoreMetrics {
    pub fn snapshot(&self) -> StoreSnapshot {
        let mut operations = BTreeMap::new();
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
            operations.insert(name.to_string(), value);
        }
        StoreSnapshot {
            operations,
            requests: snapshot_http_counts(&self.http.requests),
            successful_requests: snapshot_http_counts(&self.http.successful_requests),
            request_errors: snapshot_http_counts(&self.http.request_errors),
            client_errors: snapshot_http_counts(&self.http.client_errors),
            server_errors: snapshot_http_counts(&self.http.server_errors),
            transport_errors: snapshot_http_counts(&self.http.transport_errors),
            errors: self.errors.load(Ordering::Relaxed),
            bytes_read: self.http.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.http.bytes_written.load(Ordering::Relaxed),
            operation_bytes_read: self.operation_bytes_read.load(Ordering::Relaxed),
            operation_bytes_written: self.operation_bytes_written.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_http_request(&self, operation: StoreOperation, bytes: u64) {
        self.http.requests[operation.index()].fetch_add(1, Ordering::Relaxed);
        self.http.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_http_success(&self, operation: StoreOperation) {
        self.http.successful_requests[operation.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_http_client_error(&self, operation: StoreOperation) {
        self.record_http_error(operation);
        self.http.client_errors[operation.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_http_server_error(&self, operation: StoreOperation) {
        self.record_http_error(operation);
        self.http.server_errors[operation.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_http_other_error(&self, operation: StoreOperation) {
        self.record_http_error(operation);
    }

    pub(crate) fn record_http_transport_error(&self, operation: StoreOperation) {
        self.record_http_error(operation);
        self.http.transport_errors[operation.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_http_bytes_read(&self, bytes: u64) {
        self.http.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_http_error(&self, operation: StoreOperation) {
        self.http.request_errors[operation.index()].fetch_add(1, Ordering::Relaxed);
    }
}

fn snapshot_http_counts(
    values: &[AtomicU64; STORE_OPERATION_NAMES.len()],
) -> BTreeMap<String, u64> {
    STORE_OPERATION_NAMES
        .iter()
        .zip(values)
        .map(|(name, value)| ((*name).to_string(), value.load(Ordering::Relaxed)))
        .collect()
}

impl StoreSnapshot {
    pub fn difference(&self, start: &Self) -> Self {
        Self {
            operations: difference_counts(&self.operations, &start.operations),
            requests: difference_counts(&self.requests, &start.requests),
            successful_requests: difference_counts(
                &self.successful_requests,
                &start.successful_requests,
            ),
            request_errors: difference_counts(&self.request_errors, &start.request_errors),
            client_errors: difference_counts(&self.client_errors, &start.client_errors),
            server_errors: difference_counts(&self.server_errors, &start.server_errors),
            transport_errors: difference_counts(&self.transport_errors, &start.transport_errors),
            errors: self.errors.saturating_sub(start.errors),
            bytes_read: self.bytes_read.saturating_sub(start.bytes_read),
            bytes_written: self.bytes_written.saturating_sub(start.bytes_written),
            operation_bytes_read: self
                .operation_bytes_read
                .saturating_sub(start.operation_bytes_read),
            operation_bytes_written: self
                .operation_bytes_written
                .saturating_sub(start.operation_bytes_written),
        }
    }

    pub fn merge(&mut self, other: Self) {
        merge_counts(&mut self.operations, other.operations);
        merge_counts(&mut self.requests, other.requests);
        merge_counts(&mut self.successful_requests, other.successful_requests);
        merge_counts(&mut self.request_errors, other.request_errors);
        merge_counts(&mut self.client_errors, other.client_errors);
        merge_counts(&mut self.server_errors, other.server_errors);
        merge_counts(&mut self.transport_errors, other.transport_errors);
        self.errors = self.errors.saturating_add(other.errors);
        self.bytes_read = self.bytes_read.saturating_add(other.bytes_read);
        self.bytes_written = self.bytes_written.saturating_add(other.bytes_written);
        self.operation_bytes_read = self
            .operation_bytes_read
            .saturating_add(other.operation_bytes_read);
        self.operation_bytes_written = self
            .operation_bytes_written
            .saturating_add(other.operation_bytes_written);
    }
}

fn merge_counts(target: &mut BTreeMap<String, u64>, source: BTreeMap<String, u64>) {
    for (operation, count) in source {
        let current = target.entry(operation).or_default();
        *current = current.saturating_add(count);
    }
}

fn difference_counts(
    end: &BTreeMap<String, u64>,
    start: &BTreeMap<String, u64>,
) -> BTreeMap<String, u64> {
    end.iter()
        .map(|(name, value)| {
            (
                name.clone(),
                value.saturating_sub(start.get(name).copied().unwrap_or(0)),
            )
        })
        .collect()
}

#[derive(Clone)]
pub struct InstrumentedStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<StoreMetrics>,
}

impl InstrumentedStore {
    #[cfg(test)]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self::with_metrics(inner, Arc::new(StoreMetrics::default()))
    }

    pub(crate) fn with_metrics(inner: Arc<dyn ObjectStore>, metrics: Arc<StoreMetrics>) -> Self {
        Self { inner, metrics }
    }

    pub fn metrics(&self) -> Arc<StoreMetrics> {
        Arc::clone(&self.metrics)
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
                .operation_bytes_written
                .fetch_add(size, Ordering::Relaxed);
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
            self.metrics.operation_bytes_read.fetch_add(
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
            self.metrics
                .operation_bytes_read
                .fetch_add(size, Ordering::Relaxed);
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
                if result.is_err() {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
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
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.metrics.copy.fetch_add(1, Ordering::Relaxed);
        self.metrics.delete.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.rename_opts(from, to, options).await;
        self.error(&result);
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
        assert_eq!(after_head.operations.get("head"), Some(&1));
        assert_eq!(after_head.operations.get("get"), Some(&0));
        assert_eq!(after_head.operation_bytes_read, 0);

        store.get(&location).await.expect("get test object");

        let after_get = store.metrics().snapshot();
        assert_eq!(after_get.operations.get("head"), Some(&1));
        assert_eq!(after_get.operations.get("get"), Some(&1));
        assert_eq!(after_get.operation_bytes_read, 5);
        assert_eq!(after_get.requests.get("get"), Some(&0));
    }
}

#[derive(Debug)]
struct InstrumentedUpload {
    inner: Box<dyn MultipartUpload>,
    metrics: Arc<StoreMetrics>,
}

#[async_trait]
impl MultipartUpload for InstrumentedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let size = data.content_length() as u64;
        self.metrics.put.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .operation_bytes_written
            .fetch_add(size, Ordering::Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        self.metrics
            .complete_multipart
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.complete().await;
        if result.is_err() {
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
