use crate::instrumented_store::{HttpMethod, StoreMetrics};
use async_trait::async_trait;
use bytes::Bytes;
use http::{StatusCode, Uri};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use object_store::client::{
    ClientConfigKey, HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest,
    HttpRequestBody, HttpResponse, HttpResponseBody, HttpService,
};
use object_store::ClientOptions;
use rand::seq::SliceRandom;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::error::Error;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::task::JoinSet;

#[derive(Debug)]
pub(crate) struct InstrumentedHttpConnector {
    metrics: Arc<StoreMetrics>,
    target_authority: Option<String>,
}

impl InstrumentedHttpConnector {
    pub(crate) fn new(metrics: Arc<StoreMetrics>, target_endpoint: Option<&str>) -> Self {
        Self {
            metrics,
            target_authority: target_endpoint.and_then(endpoint_authority),
        }
    }
}

impl HttpConnector for InstrumentedHttpConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(InstrumentedReqwestService {
            client: build_reqwest_client(options)?,
            metrics: Arc::clone(&self.metrics),
            target_authority: self.target_authority.clone(),
        }))
    }
}

#[derive(Debug)]
struct InstrumentedReqwestService {
    client: reqwest::Client,
    metrics: Arc<StoreMetrics>,
    target_authority: Option<String>,
}

#[async_trait]
impl HttpService for InstrumentedReqwestService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let method = classify_s3_request(&request, self.target_authority.as_deref());
        if let Some(method) = method {
            self.metrics.record_request(method);
        }

        let (parts, body) = request.into_parts();
        let url = parts
            .uri
            .to_string()
            .parse::<reqwest::Url>()
            .map_err(|error| HttpError::new(HttpErrorKind::Request, error))?;
        let mut request = reqwest::Request::new(parts.method, url);
        *request.headers_mut() = parts.headers;
        *request.body_mut() = Some(match method {
            Some(method) => reqwest::Body::wrap(InstrumentedRequestBody {
                inner: body,
                metrics: Arc::clone(&self.metrics),
                method,
            }),
            None => reqwest::Body::wrap(body),
        });

        let response = self
            .client
            .execute(request)
            .await
            .map_err(map_reqwest_error);
        match (response, method) {
            (Ok(response), Some(method)) => {
                record_status(&self.metrics, method, response.status());
                let response: http::Response<reqwest::Body> = response.into();
                let (parts, body) = response.into_parts();
                let body = HttpResponseBody::new(body.map_err(map_reqwest_error));
                let body = HttpResponseBody::new(InstrumentedResponseBody {
                    inner: body,
                    metrics: Arc::clone(&self.metrics),
                    method,
                    body_error_recorded: false,
                });
                Ok(HttpResponse::from_parts(parts, body))
            }
            (Err(error), Some(method)) => {
                self.metrics.record_transport_error(method);
                Err(error)
            }
            (Ok(response), None) => {
                let response: http::Response<reqwest::Body> = response.into();
                let (parts, body) = response.into_parts();
                Ok(HttpResponse::from_parts(
                    parts,
                    HttpResponseBody::new(body.map_err(map_reqwest_error)),
                ))
            }
            (Err(error), None) => Err(error),
        }
    }
}

#[derive(Debug)]
struct InstrumentedRequestBody {
    inner: HttpRequestBody,
    metrics: Arc<StoreMetrics>,
    method: HttpMethod,
}

impl Body for InstrumentedRequestBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if let Some(bytes) = frame.data_ref() {
                self.metrics.record_request_bytes(
                    self.method,
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                );
            }
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

#[cfg(test)]
#[derive(Debug)]
struct TestInstrumentedHttpConnector<C> {
    inner: C,
    metrics: Arc<StoreMetrics>,
}

#[cfg(test)]
impl<C: HttpConnector> HttpConnector for TestInstrumentedHttpConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(TestInstrumentedHttpService {
            inner: self.inner.connect(options)?,
            metrics: Arc::clone(&self.metrics),
        }))
    }
}

#[cfg(test)]
#[derive(Debug)]
struct TestInstrumentedHttpService {
    inner: HttpClient,
    metrics: Arc<StoreMetrics>,
}

#[cfg(test)]
#[async_trait]
impl HttpService for TestInstrumentedHttpService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let method = classify_s3_request(&request, None);
        if let Some(method) = method {
            self.metrics.record_request(method);
            self.metrics.record_request_bytes(
                method,
                u64::try_from(request.body().content_length()).unwrap_or(u64::MAX),
            );
        }
        let response = self.inner.execute(request).await;
        match (response, method) {
            (Ok(response), Some(method)) => {
                record_status(&self.metrics, method, response.status());
                let (parts, body) = response.into_parts();
                Ok(HttpResponse::from_parts(
                    parts,
                    HttpResponseBody::new(InstrumentedResponseBody {
                        inner: body,
                        metrics: Arc::clone(&self.metrics),
                        method,
                        body_error_recorded: false,
                    }),
                ))
            }
            (Err(error), Some(method)) => {
                self.metrics.record_transport_error(method);
                Err(error)
            }
            (result, None) => result,
        }
    }
}

fn build_reqwest_client(options: &ClientOptions) -> object_store::Result<reqwest::Client> {
    let mut builder = reqwest::ClientBuilder::new();
    if let Some(user_agent) = options.get_config_value(&ClientConfigKey::UserAgent) {
        builder = builder.user_agent(user_agent);
    } else {
        builder = builder.user_agent("object_store/0.14.0");
    }
    if let Some(headers) = options.get_default_headers() {
        builder = builder.default_headers(headers.clone());
    }
    if let Some(proxy_url) = options.get_config_value(&ClientConfigKey::ProxyUrl) {
        let mut proxy = reqwest::Proxy::all(proxy_url).map_err(object_store_client_error)?;
        if let Some(excludes) = options.get_config_value(&ClientConfigKey::ProxyExcludes) {
            proxy = proxy.no_proxy(reqwest::NoProxy::from_string(&excludes));
        }
        builder = builder.proxy(proxy);
    }
    if let Some(certificate) = options.get_config_value(&ClientConfigKey::ProxyCaCertificate) {
        let certificate = reqwest::tls::Certificate::from_pem(certificate.as_bytes())
            .map_err(object_store_client_error)?;
        builder = builder.tls_certs_merge(std::iter::once(certificate));
    }
    if client_bool(options, ClientConfigKey::NoSystemCertificates)? {
        builder = builder.tls_certs_only(std::iter::empty::<reqwest::tls::Certificate>());
    }
    if let Some(timeout) = client_duration(options, ClientConfigKey::Timeout)? {
        builder = builder.timeout(timeout);
    }
    if let Some(timeout) = client_duration(options, ClientConfigKey::ConnectTimeout)? {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = client_duration(options, ClientConfigKey::ReadTimeout)? {
        builder = builder.read_timeout(timeout);
    }
    if let Some(timeout) = client_duration(options, ClientConfigKey::PoolIdleTimeout)? {
        builder = builder.pool_idle_timeout(timeout);
    }
    if let Some(max) = client_usize(options, ClientConfigKey::PoolMaxIdlePerHost)? {
        builder = builder.pool_max_idle_per_host(max);
    }
    if let Some(interval) = client_duration(options, ClientConfigKey::Http2KeepAliveInterval)? {
        builder = builder.http2_keep_alive_interval(interval);
    }
    if let Some(timeout) = client_duration(options, ClientConfigKey::Http2KeepAliveTimeout)? {
        builder = builder.http2_keep_alive_timeout(timeout);
    }
    if client_bool(options, ClientConfigKey::Http2KeepAliveWhileIdle)? {
        builder = builder.http2_keep_alive_while_idle(true);
    }
    if let Some(size) = client_u32(options, ClientConfigKey::Http2MaxFrameSize)? {
        builder = builder.http2_max_frame_size(Some(size));
    }
    if client_bool(options, ClientConfigKey::Http1Only)? {
        builder = builder.http1_only();
    }
    if client_bool(options, ClientConfigKey::Http2Only)? {
        builder = builder.http2_prior_knowledge();
    }
    if client_bool(options, ClientConfigKey::AllowInvalidCertificates)? {
        builder = builder.danger_accept_invalid_certs(true);
    }
    builder = builder.no_gzip().no_brotli().no_zstd().no_deflate();
    if client_bool(options, ClientConfigKey::RandomizeAddresses)? {
        builder = builder.dns_resolver(Arc::new(ShuffleResolver));
    }
    builder
        .https_only(!client_bool(options, ClientConfigKey::AllowHttp)?)
        .build()
        .map_err(object_store_client_error)
}

fn client_bool(options: &ClientOptions, key: ClientConfigKey) -> object_store::Result<bool> {
    options.get_config_value(&key).map_or(Ok(false), |value| {
        value.parse().map_err(object_store_client_error)
    })
}

fn client_usize(
    options: &ClientOptions,
    key: ClientConfigKey,
) -> object_store::Result<Option<usize>> {
    options
        .get_config_value(&key)
        .map(|value| value.parse().map_err(object_store_client_error))
        .transpose()
}

fn client_u32(options: &ClientOptions, key: ClientConfigKey) -> object_store::Result<Option<u32>> {
    options
        .get_config_value(&key)
        .map(|value| value.parse().map_err(object_store_client_error))
        .transpose()
}

fn client_duration(
    options: &ClientOptions,
    key: ClientConfigKey,
) -> object_store::Result<Option<Duration>> {
    options
        .get_config_value(&key)
        .map(|value| humantime::parse_duration(&value).map_err(object_store_client_error))
        .transpose()
}

fn object_store_client_error(error: impl Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "HTTP client",
        source: Box::new(error),
    }
}

fn map_reqwest_error(error: reqwest::Error) -> HttpError {
    let kind = if error.is_timeout() {
        HttpErrorKind::Timeout
    } else if error.is_connect() {
        HttpErrorKind::Connect
    } else if error.is_decode() {
        HttpErrorKind::Decode
    } else {
        HttpErrorKind::Request
    };
    HttpError::new(kind, error.without_url())
}

#[derive(Debug)]
struct ShuffleResolver;

impl Resolve for ShuffleResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let mut tasks = JoinSet::new();
            tasks.spawn_blocking(move || {
                let mut addresses = (name.as_str(), 0).to_socket_addrs()?.collect::<Vec<_>>();
                addresses.shuffle(&mut rand::rng());
                Ok(Box::new(addresses.into_iter()) as Addrs)
            });
            tasks
                .join_next()
                .await
                .expect("DNS resolver task exists")
                .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)?
        })
    }
}

#[derive(Debug)]
struct InstrumentedResponseBody {
    inner: HttpResponseBody,
    metrics: Arc<StoreMetrics>,
    method: HttpMethod,
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
                    self.metrics.record_response_bytes(
                        self.method,
                        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                    );
                }
            }
            Poll::Ready(Some(Err(_))) if !self.body_error_recorded => {
                self.metrics.record_transport_error(self.method);
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
) -> Option<HttpMethod> {
    let authority = request.uri().authority()?;
    if let Some(target) = target_authority {
        if !matches_target_authority(authority, target) {
            return None;
        }
    } else if is_metadata_host(authority.host()) {
        return None;
    }

    if request.method() == http::Method::GET
        && request.uri().query().is_some_and(is_list_objects_query)
    {
        return Some(HttpMethod::List);
    }

    Some(HttpMethod::from_http(request.method()))
}

fn is_list_objects_query(query: &str) -> bool {
    query.split('&').any(|parameter| {
        parameter
            .split_once('=')
            .is_some_and(|(key, value)| key == "list-type" && value == "2")
    })
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

fn is_metadata_host(host: &str) -> bool {
    matches!(host, "169.254.169.254" | "169.254.170.2" | "fd00:ec2::254")
}

fn record_status(metrics: &StoreMetrics, method: HttpMethod, status: StatusCode) {
    if status.is_success() || status.is_redirection() {
        metrics.record_success(method);
    } else if status == StatusCode::NOT_FOUND {
        metrics.record_not_found(method);
    } else if status.is_client_error() {
        metrics.record_client_error(method);
    } else if status.is_server_error() {
        metrics.record_server_error(method);
    } else {
        metrics.record_other_error(method);
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_s3_request, TestInstrumentedHttpConnector};
    use crate::instrumented_store::{HttpMethod, StoreMetrics};
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
    fn classifies_physical_http_methods() {
        let cases = [
            ("GET", "https://s3.example/bucket/key", HttpMethod::Get),
            ("HEAD", "https://s3.example/bucket/key", HttpMethod::Head),
            (
                "GET",
                "https://s3.example/bucket?list-type=2",
                HttpMethod::List,
            ),
            (
                "GET",
                "https://s3.example/bucket?prefix=data&list-type=2",
                HttpMethod::List,
            ),
            ("POST", "https://s3.example/bucket?delete", HttpMethod::Post),
            (
                "POST",
                "https://s3.example/bucket/key?uploads",
                HttpMethod::Post,
            ),
            (
                "POST",
                "https://s3.example/bucket/key?uploadId=one",
                HttpMethod::Post,
            ),
            (
                "DELETE",
                "https://s3.example/bucket/key?uploadId=one",
                HttpMethod::Delete,
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
        assert_eq!(classify_s3_request(&copy, None), Some(HttpMethod::Put));
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
        let connector = TestInstrumentedHttpConnector {
            inner: scripted,
            metrics: Arc::clone(&metrics),
        };
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
        assert_eq!(snapshot.requests.get("GET"), Some(&2));
        assert_eq!(snapshot.successful_requests.get("GET"), Some(&1));
        assert_eq!(snapshot.request_errors.get("GET"), Some(&1));
        assert_eq!(snapshot.server_errors.get("GET"), Some(&1));
        assert_eq!(snapshot.transport_errors.get("GET"), Some(&0));
        assert_eq!(snapshot.response_bytes.get("GET"), Some(&expected_bytes));
    }

    #[tokio::test]
    async fn counts_not_found_without_treating_it_as_a_request_error() {
        let metrics = Arc::new(StoreMetrics::default());
        let scripted =
            ScriptedConnector::new([(StatusCode::NOT_FOUND, Bytes::from_static(b"missing"))]);
        let connector = TestInstrumentedHttpConnector {
            inner: scripted,
            metrics: Arc::clone(&metrics),
        };
        let client = connector
            .connect(&ClientOptions::default())
            .expect("connect scripted client");
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

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests.get("GET"), Some(&1));
        assert_eq!(snapshot.not_found_responses.get("GET"), Some(&1));
        assert_eq!(snapshot.request_errors.get("GET"), Some(&0));
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
        let connector = TestInstrumentedHttpConnector {
            inner: scripted,
            metrics: Arc::clone(&metrics),
        };
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
        assert_eq!(snapshot.requests.get("PUT"), Some(&2));
        assert_eq!(snapshot.successful_requests.get("PUT"), Some(&1));
        assert_eq!(snapshot.server_errors.get("PUT"), Some(&1));
        assert_eq!(snapshot.request_bytes.get("PUT"), Some(&10));
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
