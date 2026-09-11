use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use hyper::server::conn::http1;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{
    fs,
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Semaphore, oneshot},
    task::JoinSet,
};
use uuid::Uuid;

const ENROLLMENT_FILE: &str = "notion-webhook-enrollment.json";
const ENROLLMENT_ADDRESS: &str = "127.0.0.1:8080";
pub(crate) const WEBHOOK_BASE_PATH: &str = "/notion/webhook";
const MAX_BODY_BYTES: usize = 1024 * 1024;
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECTION_DEADLINE: Duration = Duration::from_secs(10);
const MAX_ACTIVE_CONNECTIONS: usize = 16;
static NEXT_TEMPORARY_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerificationPayload {
    verification_token: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Enrollment {
    callback_id: Uuid,
    verification_token: String,
}

type EnrollmentResultSender = oneshot::Sender<Result<String, String>>;

impl Enrollment {
    pub(crate) fn webhook_path(&self) -> String {
        format!("{WEBHOOK_BASE_PATH}/{}", self.callback_id)
    }

    pub(crate) fn verification_token(&self) -> &[u8] {
        self.verification_token.as_bytes()
    }
}

pub(crate) trait EnrollmentSource {
    fn load(&self) -> Result<Enrollment, String>;
}

#[derive(Clone)]
pub struct FileTokenStore {
    state_directory: PathBuf,
}

impl FileTokenStore {
    pub fn new(state_directory: PathBuf) -> Self {
        Self { state_directory }
    }
    fn destination(&self) -> PathBuf {
        self.state_directory.join(ENROLLMENT_FILE)
    }

    fn ensure_available(&self, rotate: bool) -> Result<(), String> {
        crate::config::ensure_private_directory(&self.state_directory)?;
        inspect_destination(&self.destination(), rotate)
    }

    fn persist(&self, enrollment: &Enrollment, rotate: bool) -> Result<(), String> {
        self.ensure_available(rotate)?;
        let bytes = serde_json::to_vec(enrollment)
            .map_err(|_| "cannot encode private webhook enrollment".to_owned())?;
        let temporary = temporary_path(&self.state_directory);
        write_private_file(&temporary, &bytes)?;
        let destination = self.destination();
        let installed = if rotate {
            fs::rename(&temporary, &destination)
                .map_err(|_| "cannot rotate private webhook enrollment".to_owned())
        } else {
            fs::hard_link(&temporary, &destination).map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    "a Notion webhook enrollment already exists; use --rotate to replace it"
                        .to_owned()
                } else {
                    "cannot install private webhook enrollment".to_owned()
                }
            })
        };
        if installed.is_err() {
            let _ = fs::remove_file(&temporary);
            return installed;
        }
        let _ = fs::remove_file(&temporary);
        fs::File::open(&self.state_directory)
            .and_then(|file| file.sync_all())
            .map_err(|_| {
                "webhook enrollment was installed, but durability confirmation failed".to_owned()
            })
    }
}

impl EnrollmentSource for FileTokenStore {
    fn load(&self) -> Result<Enrollment, String> {
        crate::config::ensure_private_directory(&self.state_directory)?;
        let enrollment: Enrollment =
            serde_json::from_slice(&read_private_file(&self.destination())?).map_err(|_| {
                "private webhook enrollment is malformed; run webhook-enroll --rotate".to_owned()
            })?;
        if enrollment.verification_token.trim().is_empty() {
            Err("private webhook enrollment is incomplete; run webhook-enroll --rotate".to_owned())
        } else {
            Ok(enrollment)
        }
    }
}

#[derive(Clone)]
struct HttpState {
    expected_path: Arc<str>,
    callback_id: Uuid,
    store: FileTokenStore,
    rotate: bool,
    result: Arc<Mutex<Option<EnrollmentResultSender>>>,
}

pub async fn enroll<W: Write>(
    hostname: &str,
    store: &FileTokenStore,
    rotate: bool,
    output: &mut W,
) -> Result<String, String> {
    validate_hostname(hostname)?;
    store.ensure_available(rotate)?;
    let callback_id = Uuid::new_v4();
    let listener = TcpListener::bind(ENROLLMENT_ADDRESS).await.map_err(|_| {
        "cannot bind the loopback enrollment listener; port 8080 may be occupied".to_owned()
    })?;
    enroll_with_listener(hostname, callback_id, listener, store, rotate, output).await
}

async fn enroll_with_listener<W: Write>(
    hostname: &str,
    callback_id: Uuid,
    listener: TcpListener,
    store: &FileTokenStore,
    rotate: bool,
    output: &mut W,
) -> Result<String, String> {
    let path = format!("{WEBHOOK_BASE_PATH}/{callback_id}");
    writeln!(
        output,
        "Register this callback URL in Notion: https://{hostname}{path}"
    )
    .and_then(|()| output.flush())
    .map_err(|_| "cannot display the callback URL".to_owned())?;
    let (sender, receiver) = oneshot::channel();
    let state = HttpState {
        expected_path: Arc::from(path),
        callback_id,
        store: store.clone(),
        rotate,
        result: Arc::new(Mutex::new(Some(sender))),
    };
    let app = Router::new().fallback(any(endpoint)).with_state(state);
    let verification = async move {
        receiver
            .await
            .map_err(|_| "enrollment listener stopped before verification".to_owned())?
    };
    tokio::pin!(verification);
    let verification_token = run_bounded(listener, app, &mut verification).await?;
    Ok(format!(
        "Notion webhook verification token: {verification_token}"
    ))
}

async fn endpoint(State(state): State<HttpState>, request: Request) -> Response {
    if request.uri().path() != state.expected_path.as_ref() {
        return (StatusCode::NOT_FOUND, "not found\n").into_response();
    }
    if request.method() != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n").into_response();
    }
    if request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_BODY_BYTES)
    {
        return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large\n").into_response();
    }
    let body = match tokio::time::timeout(
        BODY_READ_TIMEOUT,
        to_bytes(request.into_body(), MAX_BODY_BYTES),
    )
    .await
    {
        Err(_) => return (StatusCode::REQUEST_TIMEOUT, "request timeout\n").into_response(),
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "payload too large\n").into_response();
        }
    };
    let payload: VerificationPayload = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(_) => return (StatusCode::BAD_REQUEST, "bad request\n").into_response(),
    };
    if payload.verification_token.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "bad request\n").into_response();
    }
    let mut result = state.result.lock().await;
    let Some(sender) = result.take() else {
        return (StatusCode::CONFLICT, "already enrolled\n").into_response();
    };
    let verification_token = payload.verification_token;
    let persisted = state.store.persist(
        &Enrollment {
            callback_id: state.callback_id,
            verification_token: verification_token.clone(),
        },
        state.rotate,
    );
    let status = if persisted.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let _ = sender.send(persisted.map(|()| verification_token));
    (
        status,
        if status == StatusCode::OK {
            "enrolled\n"
        } else {
            "enrollment failed\n"
        },
    )
        .into_response()
}

async fn run_bounded<F>(
    listener: TcpListener,
    app: Router,
    shutdown: &mut std::pin::Pin<&mut F>,
) -> Result<String, String>
where
    F: std::future::Future<Output = Result<String, String>>,
{
    let permits = Arc::new(Semaphore::new(MAX_ACTIVE_CONNECTIONS));
    let mut connections = JoinSet::new();
    let outcome = loop {
        tokio::select! {
            result = &mut *shutdown => break result,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|_| "cannot accept enrollment connection".to_owned())?;
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { drop(stream); continue; };
                let service = TowerToHyperService::new(app.clone());
                connections.spawn(async move {
                    let _permit = permit;
                    let mut builder = http1::Builder::new();
                    builder.keep_alive(false);
                    let connection = builder.serve_connection(TokioIo::new(stream), service);
                    let _ = tokio::time::timeout(CONNECTION_DEADLINE, connection).await;
                });
            }
        }
    };
    while connections.join_next().await.is_some() {}
    outcome
}

fn validate_hostname(hostname: &str) -> Result<(), String> {
    let valid = !hostname.is_empty()
        && hostname.len() <= 253
        && !hostname.starts_with('.')
        && !hostname.ends_with('.')
        && hostname.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    if valid {
        Ok(())
    } else {
        Err("hostname is invalid".to_owned())
    }
}

fn inspect_destination(path: &Path, rotate: bool) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err("refusing unsafe private webhook enrollment path".to_owned())
        }
        Ok(metadata) if metadata.mode() & 0o777 != 0o600 => {
            Err("private webhook enrollment file must have mode 0600".to_owned())
        }
        Ok(_) if !rotate => {
            Err("a Notion webhook enrollment already exists; use --rotate to replace it".to_owned())
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("cannot inspect private webhook enrollment".to_owned()),
    }
}

fn read_private_file(path: &Path) -> Result<Vec<u8>, String> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                "no Notion webhook enrollment exists; run webhook-enroll first".to_owned()
            } else {
                "cannot safely open private webhook enrollment".to_owned()
            }
        })?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect private webhook enrollment".to_owned())?;
    if !metadata.is_file() || metadata.mode() & 0o777 != 0o600 {
        return Err("private webhook enrollment must be a mode 0600 regular file".to_owned());
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| "cannot read private webhook enrollment".to_owned())?;
    Ok(bytes)
}

fn temporary_path(directory: &Path) -> PathBuf {
    directory.join(format!(
        ".notion-webhook-enrollment.tmp-{}-{}",
        std::process::id(),
        NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| "cannot create private webhook enrollment".to_owned())?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .and_then(|()| file.write_all(bytes))
        .and_then(|()| file.sync_all())
        .map_err(|_| "cannot persist private webhook enrollment".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn temporary_directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "agent-handover-enrollment-test-{}-{}",
            std::process::id(),
            NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn state() -> (
        HttpState,
        oneshot::Receiver<Result<String, String>>,
        PathBuf,
    ) {
        let (sender, receiver) = oneshot::channel();
        let root = temporary_directory();
        (
            HttpState {
                expected_path: Arc::from("/notion/webhook/00000000-0000-4000-8000-000000000000"),
                callback_id: Uuid::nil(),
                store: FileTokenStore::new(root.join("state")),
                rotate: false,
                result: Arc::new(Mutex::new(Some(sender))),
            },
            receiver,
            root,
        )
    }

    fn request(method: Method, path: &str, body: &str) -> Request {
        Request::builder()
            .method(method)
            .uri(path)
            .body(axum::body::Body::from(body.to_owned()))
            .unwrap()
    }

    #[test]
    fn accepts_only_plain_valid_hostnames() {
        for hostname in ["example.com", "webhook.example.test", "localhost"] {
            assert_eq!(validate_hostname(hostname), Ok(()));
        }
        for hostname in [
            "",
            "https://example.com",
            "bad/path",
            "-bad.example",
            "bad..example",
        ] {
            assert!(validate_hostname(hostname).is_err());
        }
    }

    #[tokio::test]
    async fn only_the_exact_path_and_post_method_can_enroll() {
        let (state, mut receiver, root) = state();
        assert_eq!(
            endpoint(
                State(state.clone()),
                request(
                    Method::POST,
                    "/notion/webhook",
                    r#"{"verification_token":"secret"}"#
                )
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            endpoint(
                State(state.clone()),
                request(Method::GET, state.expected_path.as_ref(), "")
            )
            .await
            .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert!(receiver.try_recv().is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn valid_payload_is_accepted_without_echoing_the_token() {
        let (state, receiver, root) = state();
        let response = endpoint(
            State(state.clone()),
            request(
                Method::POST,
                state.expected_path.as_ref(),
                r#"{"verification_token":"secret"}"#,
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(receiver.await.unwrap().unwrap(), "secret");
        let enrollment = state.store.load().unwrap();
        assert_eq!(enrollment.verification_token(), b"secret");
        assert_eq!(
            enrollment.webhook_path(),
            "/notion/webhook/00000000-0000-0000-0000-000000000000"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn malformed_and_declared_oversized_payloads_are_rejected() {
        let (state, _, root) = state();
        assert_eq!(
            endpoint(
                State(state.clone()),
                request(Method::POST, state.expected_path.as_ref(), "not json")
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
        fs::remove_dir_all(root).unwrap();
        let oversized = Request::builder()
            .method(Method::POST)
            .uri(state.expected_path.as_ref())
            .header("content-length", MAX_BODY_BYTES + 1)
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            endpoint(State(state), oversized).await.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn enrollment_pair_is_private_and_rotation_replaces_it_together() {
        let root = temporary_directory();
        let store = FileTokenStore::new(root.join("state/agent-handover"));
        let first = Enrollment {
            callback_id: Uuid::new_v4(),
            verification_token: "first".to_owned(),
        };
        store.persist(&first, false).unwrap();
        let path = store.destination();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(store.load().unwrap().verification_token(), b"first");

        let error = store
            .persist(
                &Enrollment {
                    callback_id: Uuid::new_v4(),
                    verification_token: "refused-secret".to_owned(),
                },
                false,
            )
            .unwrap_err();
        assert!(error.contains("already exists"));
        assert!(!error.contains("refused-secret"));
        assert_eq!(store.load().unwrap().verification_token(), b"first");

        let second = Enrollment {
            callback_id: Uuid::new_v4(),
            verification_token: "second".to_owned(),
        };
        store.persist(&second, true).unwrap();
        let saved: Enrollment = serde_json::from_slice(&read_private_file(&path).unwrap()).unwrap();
        assert_eq!(saved.callback_id, second.callback_id);
        assert_eq!(saved.verification_token, "second");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn serving_requires_a_complete_well_formed_enrollment() {
        for (contents, expected) in [
            (
                br#"{"verification_token":"secret"}"#.as_slice(),
                "private webhook enrollment is malformed; run webhook-enroll --rotate",
            ),
            (
                br#"{"callback_id":"not-a-uuid","verification_token":"secret"}"#.as_slice(),
                "private webhook enrollment is malformed; run webhook-enroll --rotate",
            ),
            (
                br#"{"callback_id":"00000000-0000-4000-8000-000000000071","verification_token":""}"#.as_slice(),
                "private webhook enrollment is incomplete; run webhook-enroll --rotate",
            ),
        ] {
            let root = temporary_directory();
            let store = FileTokenStore::new(root.join("state"));
            crate::config::ensure_private_directory(&store.state_directory).unwrap();
            write_private_file(&store.destination(), contents).unwrap();
            assert_eq!(store.load().unwrap_err(), expected);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn occupied_enrollment_port_fails_without_disclosing_private_values() {
        let Ok(_listener) = TcpListener::bind(ENROLLMENT_ADDRESS).await else {
            return;
        };
        let root = temporary_directory();
        let store = FileTokenStore::new(root.join("private/state"));
        let error = enroll("private.example.test", &store, false, &mut Vec::new())
            .await
            .unwrap_err();
        assert!(error.contains("port 8080 may be occupied"));
        assert!(!error.contains("private.example.test"));
        assert!(!error.contains(root.to_string_lossy().as_ref()));
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn loopback_flow_prints_exact_url_persists_then_exits() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let root = temporary_directory();
        let store = FileTokenStore::new(root.join("state"));
        let callback_id = Uuid::parse_str("00000000-0000-4000-8000-000000000070").unwrap();
        let mut output = Vec::new();
        let enrollment = enroll_with_listener(
            "handover.example.test",
            callback_id,
            listener,
            &store,
            false,
            &mut output,
        );
        let client = async {
            tokio::task::yield_now().await;
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            let body = r#"{"verification_token":"one-time-secret"}"#;
            let request = format!(
                "POST /notion/webhook/{callback_id} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        };
        let (result, ()) = tokio::join!(enrollment, client);
        assert!(result.unwrap().contains("one-time-secret"));
        let saved = store.load().unwrap();
        assert_eq!(saved.verification_token(), b"one-time-secret");
        assert_eq!(
            saved.webhook_path(),
            format!("/notion/webhook/{callback_id}")
        );
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "Register this callback URL in Notion: https://handover.example.test/notion/webhook/00000000-0000-4000-8000-000000000070\n"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
