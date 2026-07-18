use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as StoreResult,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const HTTP_METHODS: [HttpMethod; 6] = [
    HttpMethod::Get,
    HttpMethod::Put,
    HttpMethod::Head,
    HttpMethod::Delete,
    HttpMethod::Post,
    HttpMethod::Other,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HttpMethod {
    Get = 0,
    Put = 1,
    Head = 2,
    Delete = 3,
    Post = 4,
    Other = 5,
}

impl HttpMethod {
    pub(crate) fn from_http(method: &http::Method) -> Self {
        match *method {
            http::Method::GET => Self::Get,
            http::Method::PUT => Self::Put,
            http::Method::HEAD => Self::Head,
            http::Method::DELETE => Self::Delete,
            http::Method::POST => Self::Post,
            _ => Self::Other,
        }
    }

    fn index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Put => "PUT",
            Self::Head => "HEAD",
            Self::Delete => "DELETE",
            Self::Post => "POST",
            Self::Other => "OTHER",
        }
    }
}

#[derive(Debug)]
pub struct StoreMetrics {
    requests: [AtomicU64; HTTP_METHODS.len()],
    successful_requests: [AtomicU64; HTTP_METHODS.len()],
    not_found_responses: [AtomicU64; HTTP_METHODS.len()],
    request_errors: [AtomicU64; HTTP_METHODS.len()],
    client_errors: [AtomicU64; HTTP_METHODS.len()],
    server_errors: [AtomicU64; HTTP_METHODS.len()],
    transport_errors: [AtomicU64; HTTP_METHODS.len()],
    request_bytes: [AtomicU64; HTTP_METHODS.len()],
    response_bytes: [AtomicU64; HTTP_METHODS.len()],
}

impl Default for StoreMetrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            successful_requests: std::array::from_fn(|_| AtomicU64::new(0)),
            not_found_responses: std::array::from_fn(|_| AtomicU64::new(0)),
            request_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            client_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            server_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            transport_errors: std::array::from_fn(|_| AtomicU64::new(0)),
            request_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            response_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshot {
    pub requests: BTreeMap<String, u64>,
    pub successful_requests: BTreeMap<String, u64>,
    pub not_found_responses: BTreeMap<String, u64>,
    pub request_errors: BTreeMap<String, u64>,
    pub client_errors: BTreeMap<String, u64>,
    pub server_errors: BTreeMap<String, u64>,
    pub transport_errors: BTreeMap<String, u64>,
    pub request_bytes: BTreeMap<String, u64>,
    pub response_bytes: BTreeMap<String, u64>,
}

impl StoreMetrics {
    pub fn snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            requests: snapshot(&self.requests),
            successful_requests: snapshot(&self.successful_requests),
            not_found_responses: snapshot(&self.not_found_responses),
            request_errors: snapshot(&self.request_errors),
            client_errors: snapshot(&self.client_errors),
            server_errors: snapshot(&self.server_errors),
            transport_errors: snapshot(&self.transport_errors),
            request_bytes: snapshot(&self.request_bytes),
            response_bytes: snapshot(&self.response_bytes),
        }
    }

    pub(crate) fn record_request(&self, method: HttpMethod) {
        self.requests[method.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_request_bytes(&self, method: HttpMethod, bytes: u64) {
        self.request_bytes[method.index()].fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_success(&self, method: HttpMethod) {
        self.successful_requests[method.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_not_found(&self, method: HttpMethod) {
        self.not_found_responses[method.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_client_error(&self, method: HttpMethod) {
        self.record_error(method);
        self.client_errors[method.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_server_error(&self, method: HttpMethod) {
        self.record_error(method);
        self.server_errors[method.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_other_error(&self, method: HttpMethod) {
        self.record_error(method);
    }

    pub(crate) fn record_transport_error(&self, method: HttpMethod) {
        self.record_error(method);
        self.transport_errors[method.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_response_bytes(&self, method: HttpMethod, bytes: u64) {
        self.response_bytes[method.index()].fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_error(&self, method: HttpMethod) {
        self.request_errors[method.index()].fetch_add(1, Ordering::Relaxed);
    }
}

impl StoreSnapshot {
    pub fn difference(&self, start: &Self) -> Self {
        Self {
            requests: difference(&self.requests, &start.requests),
            successful_requests: difference(&self.successful_requests, &start.successful_requests),
            not_found_responses: difference(&self.not_found_responses, &start.not_found_responses),
            request_errors: difference(&self.request_errors, &start.request_errors),
            client_errors: difference(&self.client_errors, &start.client_errors),
            server_errors: difference(&self.server_errors, &start.server_errors),
            transport_errors: difference(&self.transport_errors, &start.transport_errors),
            request_bytes: difference(&self.request_bytes, &start.request_bytes),
            response_bytes: difference(&self.response_bytes, &start.response_bytes),
        }
    }

    pub fn body_bytes(&self, method: &str) -> u64 {
        self.request_bytes
            .get(method)
            .copied()
            .unwrap_or(0)
            .saturating_add(self.response_bytes.get(method).copied().unwrap_or(0))
    }

    pub fn errors(&self) -> u64 {
        self.request_errors.values().copied().sum()
    }
}

fn snapshot(values: &[AtomicU64; HTTP_METHODS.len()]) -> BTreeMap<String, u64> {
    HTTP_METHODS
        .iter()
        .zip(values)
        .map(|(method, value)| (method.name().to_string(), value.load(Ordering::Relaxed)))
        .collect()
}

fn difference(end: &BTreeMap<String, u64>, start: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
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
    pub(crate) fn with_metrics(inner: Arc<dyn ObjectStore>, metrics: Arc<StoreMetrics>) -> Self {
        Self { inner, metrics }
    }

    pub fn metrics(&self) -> Arc<StoreMetrics> {
        Arc::clone(&self.metrics)
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
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> StoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::{HttpMethod, StoreMetrics};

    #[test]
    fn snapshots_physical_http_methods_and_body_bytes() {
        let metrics = StoreMetrics::default();
        metrics.record_request(HttpMethod::Get);
        metrics.record_request_bytes(HttpMethod::Get, 7);
        metrics.record_response_bytes(HttpMethod::Get, 11);
        metrics.record_success(HttpMethod::Get);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests["GET"], 1);
        assert_eq!(snapshot.successful_requests["GET"], 1);
        assert_eq!(snapshot.body_bytes("GET"), 18);
    }
}
