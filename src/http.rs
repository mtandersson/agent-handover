use crate::config::RunnerConfig;
use crate::enrollment::TokenSource;
use axum::Router;
use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use hmac::{Hmac, Mac};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use serde_json::Value;
use sha2::Sha256;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const SIGNATURE_HEADER: &str = "x-notion-signature";
pub const MAX_WEBHOOK_BODY_BYTES: usize = 1024 * 1024;
const WEBHOOK_BODY_READ_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_ACTIVE_CONNECTIONS: usize = 16;
const CONNECTION_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Default)]
struct AdmissionMetrics {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl AdmissionMetrics {
    fn entered(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
    }

    fn exited(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct ActiveConnection(Arc<AdmissionMetrics>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.exited();
    }
}

pub trait EventDispatcher: Send + Sync + 'static {
    fn dispatch(&self, event: Value) -> Result<(), String>;
}

pub struct PendingEventDispatcher;

impl EventDispatcher for PendingEventDispatcher {
    fn dispatch(&self, _event: Value) -> Result<(), String> {
        Ok(())
    }
}

struct HttpState<D> {
    webhook_path: Arc<str>,
    health_path: Arc<str>,
    token: Arc<[u8]>,
    dispatcher: Arc<D>,
    body_read_timeout: Duration,
}

impl<D> Clone for HttpState<D> {
    fn clone(&self) -> Self {
        Self {
            webhook_path: Arc::clone(&self.webhook_path),
            health_path: Arc::clone(&self.health_path),
            token: Arc::clone(&self.token),
            dispatcher: Arc::clone(&self.dispatcher),
            body_read_timeout: self.body_read_timeout,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HttpResponse {
    status: StatusCode,
    body: &'static str,
}

struct HttpRequest<'a> {
    method: &'a Method,
    path: &'a str,
    headers: &'a HeaderMap,
    body: &'a [u8],
}

pub fn serve<T: TokenSource, D: EventDispatcher>(
    config: &RunnerConfig,
    token_source: &T,
    dispatcher: D,
) -> Result<String, String> {
    let token = token_source.load()?;
    let address = config
        .bind_address
        .parse::<SocketAddr>()
        .map_err(|_| "configured HTTP bind address is invalid".to_owned())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("cannot start HTTP runtime: {error}"))?;
    runtime.block_on(async {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| format!("cannot bind HTTP server: {error}"))?;
        run_server(
            listener,
            state(config, token.into_bytes(), dispatcher),
            std::future::pending::<()>(),
            CONNECTION_DEADLINE,
            Arc::new(AdmissionMetrics::default()),
        )
        .await
    })?;
    Ok("HTTP server stopped".to_owned())
}

fn state<D: EventDispatcher>(config: &RunnerConfig, token: Vec<u8>, dispatcher: D) -> HttpState<D> {
    HttpState {
        webhook_path: Arc::from(config.webhook_path.as_str()),
        health_path: Arc::from(config.health_path.as_str()),
        token: Arc::from(token),
        dispatcher: Arc::new(dispatcher),
        body_read_timeout: WEBHOOK_BODY_READ_TIMEOUT,
    }
}

async fn run_server<D, F>(
    listener: TcpListener,
    state: HttpState<D>,
    shutdown: F,
    connection_deadline: Duration,
    metrics: Arc<AdmissionMetrics>,
) -> Result<(), String>
where
    D: EventDispatcher,
    F: Future<Output = ()> + Send + 'static,
{
    let app = Router::new().fallback(any(endpoint::<D>)).with_state(state);
    let permits = Arc::new(Semaphore::new(MAX_ACTIVE_CONNECTIONS));
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|error| format!("cannot accept HTTP connection: {error}"))?;
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let service = TowerToHyperService::new(app.clone());
                let metrics = Arc::clone(&metrics);
                connections.spawn(async move {
                    metrics.entered();
                    let _active = ActiveConnection(metrics);
                    let _permit = permit;
                    let connection = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service);
                    let _ = tokio::time::timeout(connection_deadline, connection).await;
                });
            }
        }
    }

    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn endpoint<D: EventDispatcher>(
    State(state): State<HttpState<D>>,
    request: Request,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();

    if path == state.health_path.as_ref() {
        return if method == Method::GET {
            response(StatusCode::OK, "ok\n").into_response()
        } else {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n").into_response()
        };
    }
    if path != state.webhook_path.as_ref() {
        return response(StatusCode::NOT_FOUND, "not found\n").into_response();
    }
    if method != Method::POST {
        return response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n").into_response();
    }

    let (parts, body) = request.into_parts();
    let Some(signature) = single_signature(&parts.headers) else {
        return response(StatusCode::UNAUTHORIZED, "unauthorized\n").into_response();
    };
    if content_length_exceeds_limit(&parts.headers) {
        return response(StatusCode::PAYLOAD_TOO_LARGE, "payload too large\n").into_response();
    }
    let body = match tokio::time::timeout(
        state.body_read_timeout,
        to_bytes(body, MAX_WEBHOOK_BODY_BYTES),
    )
    .await
    {
        Err(_) => {
            return response(StatusCode::REQUEST_TIMEOUT, "request timeout\n").into_response();
        }
        Ok(Err(error))
            if error
                .source()
                .is_some_and(|source| source.is::<http_body_util::LengthLimitError>()) =>
        {
            return response(StatusCode::PAYLOAD_TOO_LARGE, "payload too large\n").into_response();
        }
        Ok(Err(_)) => return response(StatusCode::BAD_REQUEST, "bad request\n").into_response(),
        Ok(Ok(body)) => body,
    };
    handle(
        HttpRequest {
            method: &method,
            path: &path,
            headers: &parts.headers,
            body: &body,
        },
        &state,
        Some(signature),
    )
    .into_response()
}

fn content_length_exceeds_limit(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_WEBHOOK_BODY_BYTES)
}

fn single_signature(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(SIGNATURE_HEADER).iter();
    let signature = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    valid_signature_format(signature).then_some(signature)
}

fn handle<D: EventDispatcher>(
    request: HttpRequest<'_>,
    state: &HttpState<D>,
    signature: Option<&str>,
) -> HttpResponse {
    if request.path == state.health_path.as_ref() {
        return if request.method == Method::GET {
            response(StatusCode::OK, "ok\n")
        } else {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n")
        };
    }
    if request.path != state.webhook_path.as_ref() {
        return response(StatusCode::NOT_FOUND, "not found\n");
    }
    if request.method != Method::POST {
        return response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
    }
    let signature = signature.or_else(|| single_signature(request.headers));
    if !valid_signature(signature, request.body, &state.token) {
        return response(StatusCode::UNAUTHORIZED, "unauthorized\n");
    }
    let event = match serde_json::from_slice(request.body) {
        Ok(event) => event,
        Err(_) => return response(StatusCode::BAD_REQUEST, "bad request\n"),
    };
    match state.dispatcher.dispatch(event) {
        Ok(()) => response(StatusCode::OK, "accepted\n"),
        Err(_) => response(StatusCode::INTERNAL_SERVER_ERROR, "event dispatch failed\n"),
    }
}

fn valid_signature_format(signature: &str) -> bool {
    signature.strip_prefix("sha256=").is_some_and(|encoded| {
        encoded.len() == 64
            && encoded
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn valid_signature(signature: Option<&str>, body: &[u8], token: &[u8]) -> bool {
    let Some(signature) = signature.filter(|value| valid_signature_format(value)) else {
        return false;
    };
    let Some(encoded) = signature.strip_prefix("sha256=") else {
        return false;
    };
    let mut supplied = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let Ok(pair) = std::str::from_utf8(pair) else {
            return false;
        };
        let Ok(byte) = u8::from_str_radix(pair, 16) else {
            return false;
        };
        supplied[index] = byte;
    }
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(token) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&supplied).is_ok()
}

fn response(status: StatusCode, body: &'static str) -> HttpResponse {
    HttpResponse { status, body }
}

impl IntoResponse for HttpResponse {
    fn into_response(self) -> Response {
        (
            self.status,
            [("content-type", "text/plain; charset=utf-8")],
            self.body,
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;

    const TOKEN: &[u8] = b"verification-secret-placeholder";

    #[derive(Default)]
    struct RecordingDispatcher {
        events: Mutex<Vec<Value>>,
    }

    impl EventDispatcher for RecordingDispatcher {
        fn dispatch(&self, event: Value) -> Result<(), String> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn config(address: &str) -> RunnerConfig {
        RunnerConfig {
            reconciliation_interval_seconds: 60,
            bind_address: address.to_owned(),
            webhook_path: "/custom/webhook".to_owned(),
            health_path: "/custom/health".to_owned(),
        }
    }

    fn test_state() -> HttpState<RecordingDispatcher> {
        state(
            &config("127.0.0.1:0"),
            TOKEN.to_vec(),
            RecordingDispatcher::default(),
        )
    }

    fn signature(body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(TOKEN).unwrap();
        mac.update(body);
        format!("sha256={}", hex(mac.finalize().into_bytes().as_slice()))
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn request<'a>(
        method: &'a Method,
        path: &'a str,
        headers: &'a HeaderMap,
        body: &'a [u8],
    ) -> HttpRequest<'a> {
        HttpRequest {
            method,
            path,
            headers,
            body,
        }
    }

    #[test]
    fn fake_handler_covers_paths_exact_bytes_and_authentication_order() {
        let state = test_state();
        let empty = HeaderMap::new();
        assert_eq!(
            handle(
                request(&Method::GET, "/custom/health", &empty, b""),
                &state,
                None
            ),
            response(StatusCode::OK, "ok\n")
        );

        let body = r#"{ "type" : "page.updated", "value": "å" }"#.as_bytes();
        let signed = signature(body);
        assert_eq!(
            handle(
                request(&Method::POST, "/custom/webhook", &empty, body),
                &state,
                Some(&signed)
            ),
            response(StatusCode::OK, "accepted\n")
        );
        assert_eq!(state.dispatcher.events.lock().unwrap().len(), 1);

        let reformatted = r#"{"type":"page.updated","value":"å"}"#.as_bytes();
        assert_eq!(
            handle(
                request(&Method::POST, "/custom/webhook", &empty, reformatted),
                &state,
                Some(&signed)
            ),
            response(StatusCode::UNAUTHORIZED, "unauthorized\n")
        );
        assert_eq!(
            handle(
                request(&Method::POST, "/custom/webhook", &empty, b"not-json-secret"),
                &state,
                None
            ),
            response(StatusCode::UNAUTHORIZED, "unauthorized\n")
        );
        assert_eq!(state.dispatcher.events.lock().unwrap().len(), 1);
    }

    #[test]
    fn signature_verifier_rejects_every_malformed_length_without_panicking() {
        let body = br#"{"type":"page.updated"}"#;
        for malformed in [
            "sha256=",
            "sha256=0",
            "sha256=000000000000000000000000000000000000000000000000000000000000000",
            "sha256=00000000000000000000000000000000000000000000000000000000000000000",
            "sha256=00000000000000000000000000000000000000000000000000000000000000gg",
            "sha256=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            assert!(!valid_signature(Some(malformed), body, TOKEN));
        }
    }

    async fn start(
        address: &str,
        body_timeout: Duration,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), String>>,
        Arc<AdmissionMetrics>,
    ) {
        let listener = TcpListener::bind(address).await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut state = test_state();
        state.body_read_timeout = body_timeout;
        let (stop, stopped) = oneshot::channel();
        let metrics = Arc::new(AdmissionMetrics::default());
        let task = tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = stopped.await;
            },
            Duration::from_secs(2),
            Arc::clone(&metrics),
        ));
        (address, stop, task, metrics)
    }

    async fn exchange(address: SocketAddr, bytes: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(bytes).await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }

    async fn exchange_with_eof(address: SocketAddr, bytes: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(bytes).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }

    fn status(response: &str) -> u16 {
        response.split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    async fn stop(sender: oneshot::Sender<()>, task: tokio::task::JoinHandle<Result<(), String>>) {
        sender.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn real_loopback_routes_and_authenticates_exact_wire_bytes() {
        let (address, shutdown, task, _) = start("127.0.0.1:0", Duration::from_millis(200)).await;
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /custom/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            200
        );
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            404
        );
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"POST /unknown HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            404
        );
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /custom/webhook HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            405
        );
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: malformed\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            401
        );

        let body = br#"{ "type": "page.updated" }"#;
        let signed = signature(body);
        let wire = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nx-NoTiOn-SiGnAtUrE: {signed}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let mut request = wire.into_bytes();
        request.extend_from_slice(body);
        assert_eq!(status(&exchange(address, &request).await), 200);

        let duplicate = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: {signed}\r\nx-notion-signature: {signed}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        assert_eq!(status(&exchange(address, duplicate.as_bytes()).await), 401);
        stop(shutdown, task).await;
    }

    #[tokio::test]
    async fn body_limits_truncation_and_slow_clients_do_not_block_health() {
        let (address, shutdown, task, _) = start("127.0.0.1:0", Duration::from_millis(100)).await;
        let invalid = "sha256=0000000000000000000000000000000000000000000000000000000000000000";
        let oversized = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: {invalid}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_WEBHOOK_BODY_BYTES + 1
        );
        assert_eq!(status(&exchange(address, oversized.as_bytes()).await), 413);

        let unauthenticated = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_WEBHOOK_BODY_BYTES
        );
        assert_eq!(
            status(&exchange(address, unauthenticated.as_bytes()).await),
            401
        );

        let chunk = vec![b'a'; MAX_WEBHOOK_BODY_BYTES + 1];
        let chunk_header = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: {invalid}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
            chunk.len()
        );
        let mut chunked = chunk_header.into_bytes();
        chunked.extend_from_slice(&chunk);
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        assert_eq!(status(&exchange(address, &chunked).await), 413);

        let mut slow = tokio::net::TcpStream::connect(address).await.unwrap();
        let complete_slow_body = vec![b'a'; 100];
        let slow_signature = signature(&complete_slow_body);
        let partial = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: {slow_signature}\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{{"
        );
        slow.write_all(partial.as_bytes()).await.unwrap();
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /custom/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            200
        );
        let mut slow_response = String::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            slow.read_to_string(&mut slow_response),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(status(&slow_response), 408);

        let truncated = format!(
            "POST /custom/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: {invalid}\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{{}}"
        );
        assert_eq!(
            status(&exchange_with_eof(address, truncated.as_bytes()).await),
            400
        );
        stop(shutdown, task).await;
    }

    #[tokio::test]
    async fn disconnected_response_does_not_stop_later_health_requests() {
        let (address, shutdown, task, _) = start("127.0.0.1:0", Duration::from_millis(200)).await;
        let mut disconnected = tokio::net::TcpStream::connect(address).await.unwrap();
        disconnected
            .write_all(b"GET /custom/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let disconnected = disconnected.into_std().unwrap();
        let reset = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // SAFETY: the pointer refers to a live `linger` value for the duration
        // of the call, and the descriptor remains owned by `disconnected`.
        let result = unsafe {
            libc::setsockopt(
                disconnected.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::from_ref(&reset).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(result, 0);
        drop(disconnected);
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /custom/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            200
        );
        stop(shutdown, task).await;
    }

    #[tokio::test]
    async fn connection_admission_is_bounded_and_deadlines_release_capacity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = Arc::new(AdmissionMetrics::default());
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(run_server(
            listener,
            test_state(),
            async {
                let _ = stopped.await;
            },
            Duration::from_millis(150),
            Arc::clone(&metrics),
        ));

        let mut admitted = Vec::new();
        for _ in 0..MAX_ACTIVE_CONNECTIONS {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream
                .write_all(b"GET /partial HTTP/1.1\r\nHost:")
                .await
                .unwrap();
            admitted.push(stream);
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.active.load(Ordering::SeqCst) < MAX_ACTIVE_CONNECTIONS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut excess = tokio::net::TcpStream::connect(address).await.unwrap();
        excess
            .write_all(b"GET /excess HTTP/1.1\r\nHost:")
            .await
            .unwrap();
        let mut byte = [0_u8; 1];
        let shed = tokio::time::timeout(Duration::from_secs(1), excess.read(&mut byte))
            .await
            .expect("excess connection must be shed");
        assert!(matches!(shed, Ok(0) | Err(_)));
        assert_eq!(metrics.peak.load(Ordering::SeqCst), MAX_ACTIVE_CONNECTIONS);

        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /custom/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            200
        );

        let mut partial = tokio::net::TcpStream::connect(address).await.unwrap();
        partial.write_all(b"GET / HTTP/1.1\r\nHost:").await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.active.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop(shutdown, task).await;
        assert_eq!(metrics.active.load(Ordering::SeqCst), 0);
        drop(partial);
        drop(admitted);
    }

    #[tokio::test]
    async fn ipv6_loopback_serves_health_when_available() {
        let Ok(listener) = TcpListener::bind("[::1]:0").await else {
            return;
        };
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(run_server(
            listener,
            test_state(),
            async {
                let _ = stopped.await;
            },
            Duration::from_secs(2),
            Arc::new(AdmissionMetrics::default()),
        ));
        assert_eq!(
            status(
                &exchange(
                    address,
                    b"GET /custom/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .await
            ),
            200
        );
        stop(shutdown, task).await;
    }
}
