use crate::instrumented_store::{StoreMetrics, StoreOperation};
use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, StatusCode, Uri};
use http_body::{Body, Frame, SizeHint};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    ReqwestConnector,
};
use object_store::ClientOptions;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub(crate) struct InstrumentedHttpConnector<C = ReqwestConnector> {
    inner: C,
    metrics: Arc<StoreMetrics>,
    target_authority: Option<String>,
}

impl InstrumentedHttpConnector<ReqwestConnector> {
    pub(crate) fn new(metrics: Arc<StoreMetrics>, target_endpoint: Option<&str>) -> Self {
        Self {
            inner: ReqwestConnector::default(),
            metrics,
            target_authority: target_endpoint.and_then(endpoint_authority),
        }
    }
}

#[cfg(test)]
impl<C> InstrumentedHttpConnector<C> {
    fn with_connector(inner: C, metrics: Arc<StoreMetrics>) -> Self {
        Self {
            inner,
            metrics,
            target_authority: None,
        }
    }
}

impl<C: HttpConnector> HttpConnector for InstrumentedHttpConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.inner.connect(options)?;
        Ok(HttpClient::new(InstrumentedHttpService {
            inner,
            metrics: Arc::clone(&self.metrics),
            target_authority: self.target_authority.clone(),
        }))
    }
}

#[derive(Debug)]
struct InstrumentedHttpService {
    inner: HttpClient,
    metrics: Arc<StoreMetrics>,
    target_authority: Option<String>,
}

#[async_trait]
impl HttpService for InstrumentedHttpService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let operation = classify_s3_request(&request, self.target_authority.as_deref());
        if let Some(operation) = operation {
            self.metrics.record_http_request(
                operation,
                u64::try_from(request.body().content_length()).unwrap_or(u64::MAX),
            );
        }

        let response = self.inner.execute(request).await;
        match (response, operation) {
            (Ok(response), Some(operation)) => {
                record_status(&self.metrics, operation, response.status());
                let (parts, body) = response.into_parts();
                let body = HttpResponseBody::new(InstrumentedResponseBody {
                    inner: body,
                    metrics: Arc::clone(&self.metrics),
                    operation,
                    body_error_recorded: false,
                });
                Ok(HttpResponse::from_parts(parts, body))
            }
            (Err(error), Some(operation)) => {
                self.metrics.record_http_transport_error(operation);
                Err(error)
            }
            (result, None) => result,
        }
    }
}

#[derive(Debug)]
struct InstrumentedResponseBody {
    inner: HttpResponseBody,
    metrics: Arc<StoreMetrics>,
    operation: StoreOperation,
    body_error_recorded: bool,
}

impl Body for InstrumentedResponseBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(bytes) = frame.data_ref() {
                    self.metrics
                        .record_http_bytes_read(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                }
            }
            Poll::Ready(Some(Err(_))) if !self.body_error_recorded => {
                self.metrics.record_http_transport_error(self.operation);
                self.body_error_recorded = true;
            }
            _ => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn endpoint_authority(endpoint: &str) -> Option<String> {
    endpoint
        .parse::<Uri>()
        .ok()?
        .authority()
        .map(|authority| authority.as_str().to_ascii_lowercase())
}

fn classify_s3_request(
    request: &HttpRequest,
    target_authority: Option<&str>,
) -> Option<StoreOperation> {
    let authority = request.uri().authority()?;
    if let Some(target) = target_authority {
        if !matches_target_authority(authority, target) {
            return None;
        }
    } else if is_metadata_host(authority.host()) {
        return None;
    }

    let query = request.uri().query();
    let method = request.method();
    if method == Method::HEAD {
        Some(StoreOperation::Head)
    } else if method == Method::GET
        && (has_query_key(query, "list-type") || has_query_key(query, "uploads"))
    {
        Some(StoreOperation::List)
    } else if method == Method::GET {
        Some(StoreOperation::Get)
    } else if method == Method::PUT && request.headers().contains_key("x-amz-copy-source") {
        Some(StoreOperation::Copy)
    } else if method == Method::PUT {
        Some(StoreOperation::Put)
    } else if method == Method::POST && has_query_key(query, "delete") {
        Some(StoreOperation::Delete)
    } else if method == Method::POST && has_query_key(query, "uploads") {
        Some(StoreOperation::CreateMultipart)
    } else if method == Method::POST && has_query_key(query, "uploadId") {
        Some(StoreOperation::CompleteMultipart)
    } else if method == Method::DELETE && has_query_key(query, "uploadId") {
        Some(StoreOperation::AbortMultipart)
    } else if method == Method::DELETE {
        Some(StoreOperation::Delete)
    } else {
        None
    }
}

fn matches_target_authority(authority: &http::uri::Authority, target: &str) -> bool {
    if authority.as_str().eq_ignore_ascii_case(target) {
        return true;
    }
    let Ok(target) = target.parse::<http::uri::Authority>() else {
        return false;
    };
    let authority_host = authority.host().to_ascii_lowercase();
    let target_host = target.host().to_ascii_lowercase();
    authority_host == target_host
        || authority_host
            .strip_suffix(&format!(".{target_host}"))
            .is_some_and(|prefix| !prefix.is_empty())
}

fn has_query_key(query: Option<&str>, key: &str) -> bool {
    query.is_some_and(|query| {
        query.split('&').any(|part| {
            part.split_once('=')
                .map_or(part, |(name, _)| name)
                .eq_ignore_ascii_case(key)
        })
    })
}

fn is_metadata_host(host: &str) -> bool {
    matches!(host, "169.254.169.254" | "169.254.170.2" | "fd00:ec2::254")
}

fn record_status(metrics: &StoreMetrics, operation: StoreOperation, status: StatusCode) {
    if status.is_success() {
        metrics.record_http_success(operation);
    } else if status.is_client_error() {
        metrics.record_http_client_error(operation);
    } else if status.is_server_error() {
        metrics.record_http_server_error(operation);
    } else {
        metrics.record_http_other_error(operation);
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_s3_request, InstrumentedHttpConnector};
    use crate::instrumented_store::{StoreMetrics, StoreOperation};
    use async_trait::async_trait;
    use bytes::Bytes;
    use http::{Request, Response, StatusCode};
    use http_body_util::{BodyExt, Full};
    use object_store::aws::AmazonS3Builder;
    use object_store::client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpRequestBody, HttpResponse,
        HttpResponseBody, HttpService,
    };
    use object_store::path::Path;
    use object_store::ClientOptions;
    use object_store::ObjectStoreExt;
    use object_store::PutPayload;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    #[test]
    fn classifies_s3_physical_operations() {
        let cases = [
            ("GET", "https://s3.example/bucket/key", StoreOperation::Get),
            (
                "HEAD",
                "https://s3.example/bucket/key",
                StoreOperation::Head,
            ),
            (
                "GET",
                "https://s3.example/bucket?list-type=2",
                StoreOperation::List,
            ),
            (
                "POST",
                "https://s3.example/bucket?delete",
                StoreOperation::Delete,
            ),
            (
                "POST",
                "https://s3.example/bucket/key?uploads",
                StoreOperation::CreateMultipart,
            ),
            (
                "POST",
                "https://s3.example/bucket/key?uploadId=one",
                StoreOperation::CompleteMultipart,
            ),
            (
                "DELETE",
                "https://s3.example/bucket/key?uploadId=one",
                StoreOperation::AbortMultipart,
            ),
        ];
        for (method, uri, expected) in cases {
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .body(HttpRequestBody::empty())
                .expect("valid request");
            assert_eq!(classify_s3_request(&request, None), Some(expected));
        }

        let copy = Request::builder()
            .method("PUT")
            .uri("https://s3.example/bucket/to")
            .header("x-amz-copy-source", "/bucket/from")
            .body(HttpRequestBody::empty())
            .expect("valid copy request");
        assert_eq!(classify_s3_request(&copy, None), Some(StoreOperation::Copy));
    }

    #[tokio::test]
    async fn counts_each_attempt_status_and_full_response_body() {
        let metrics = Arc::new(StoreMetrics::default());
        let error_body = Bytes::from_static(b"slow down");
        let success_body = Bytes::from_static(b"response including coalesced gaps");
        let expected_bytes = (error_body.len() + success_body.len()) as u64;
        let scripted = ScriptedConnector::new([
            (StatusCode::SERVICE_UNAVAILABLE, error_body),
            (StatusCode::OK, success_body),
        ]);
        let connector = InstrumentedHttpConnector::with_connector(scripted, Arc::clone(&metrics));
        let client = connector
            .connect(&ClientOptions::default())
            .expect("connect scripted client");

        for _ in 0..2 {
            let request = Request::builder()
                .method("GET")
                .uri("https://s3.example/bucket/key")
                .body(HttpRequestBody::empty())
                .expect("valid request");
            client
                .execute(request)
                .await
                .expect("scripted response")
                .into_body()
                .bytes()
                .await
                .expect("read response body");
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests.get("get"), Some(&2));
        assert_eq!(snapshot.successful_requests.get("get"), Some(&1));
        assert_eq!(snapshot.request_errors.get("get"), Some(&1));
        assert_eq!(snapshot.server_errors.get("get"), Some(&1));
        assert_eq!(snapshot.transport_errors.get("get"), Some(&0));
        assert_eq!(snapshot.bytes_read, expected_bytes);
    }

    #[tokio::test]
    async fn observes_retries_below_the_object_store_api() {
        let metrics = Arc::new(StoreMetrics::default());
        let scripted = ScriptedConnector::new([
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Bytes::from_static(b"slow down"),
            ),
            (StatusCode::OK, Bytes::new()),
        ]);
        let connector = InstrumentedHttpConnector::with_connector(scripted, Arc::clone(&metrics));
        let store = AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint("https://s3.example")
            .with_access_key_id("access-key")
            .with_secret_access_key("secret-key")
            .with_http_connector(connector)
            .build()
            .expect("build scripted S3 store");

        store
            .put(&Path::from("key"), PutPayload::from_static(b"value"))
            .await
            .expect("retry put");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests.get("put"), Some(&2));
        assert_eq!(snapshot.successful_requests.get("put"), Some(&1));
        assert_eq!(snapshot.server_errors.get("put"), Some(&1));
        assert_eq!(snapshot.bytes_written, 10);
    }

    #[derive(Debug, Clone)]
    struct ScriptedConnector {
        responses: Arc<Mutex<VecDeque<(StatusCode, Bytes)>>>,
    }

    impl ScriptedConnector {
        fn new(responses: impl IntoIterator<Item = (StatusCode, Bytes)>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            }
        }
    }

    impl HttpConnector for ScriptedConnector {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(ScriptedService {
                responses: Arc::clone(&self.responses),
            }))
        }
    }

    #[derive(Debug)]
    struct ScriptedService {
        responses: Arc<Mutex<VecDeque<(StatusCode, Bytes)>>>,
    }

    #[async_trait]
    impl HttpService for ScriptedService {
        async fn call(&self, _request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let (status, bytes) = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .expect("scripted response available");
            let body = Full::new(bytes).map_err(|never: Infallible| match never {});
            Response::builder()
                .status(status)
                .header("etag", "\"scripted-etag\"")
                .body(HttpResponseBody::new(body))
                .map_err(|error| {
                    HttpError::new(object_store::client::HttpErrorKind::Unknown, error)
                })
        }
    }
}
