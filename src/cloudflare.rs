use crate::config::{CloudflaredConfig, HostPaths, RunnerConfig, ensure_private_directory};
use serde::Deserialize;
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CONFIGURATION_FILE: &str = "cloudflared-config.yml";
const REMOTE_TOKEN_FILE: &str = "cloudflared-token";
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const READY_POLL: Duration = Duration::from_millis(50);
const MAX_METRICS_RESPONSE_BYTES: usize = 64 * 1024;

/// Read-only boundary for the remote-tunnel configuration API.  It deliberately
/// has no mutation operation: enrollment may prove ingress, never establish it.
pub(crate) trait RemoteTunnelConfiguration {
    fn get<'a>(
        &'a self,
        account_id: &'a str,
        tunnel_id: &'a str,
        api_token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RemoteTunnelConfig, String>> + Send + 'a>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemoteTunnelConfig {
    pub(crate) ingress: Vec<RemoteIngress>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemoteIngress {
    pub(crate) hostname: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) service: String,
}

pub(crate) struct CloudflareApi {
    client: reqwest::Client,
}
impl CloudflareApi {
    pub(crate) fn new() -> Result<Self, String> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| "cannot configure Cloudflare verification".to_owned())?,
        })
    }
}
impl RemoteTunnelConfiguration for CloudflareApi {
    fn get<'a>(
        &'a self,
        account_id: &'a str,
        tunnel_id: &'a str,
        api_token: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<RemoteTunnelConfig, String>> + Send + 'a>> {
        Box::pin(async move {
            let url = format!(
                "https://api.cloudflare.com/client/v4/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations"
            );
            let mut response = self
                .client
                .get(url)
                .bearer_auth(api_token)
                .send()
                .await
                .map_err(|_| "cannot retrieve remote Cloudflare tunnel configuration".to_owned())?;
            if !response.status().is_success() {
                return Err("cannot retrieve remote Cloudflare tunnel configuration".to_owned());
            }
            if response
                .content_length()
                .is_some_and(|length| length > 1024 * 1024)
            {
                return Err("remote Cloudflare tunnel configuration is oversized".to_owned());
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "cannot retrieve remote Cloudflare tunnel configuration".to_owned())?
            {
                if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
                    return Err("remote Cloudflare tunnel configuration is oversized".to_owned());
                }
                bytes.extend_from_slice(&chunk);
            }
            parse_remote_configuration(&bytes)
        })
    }
}

#[derive(Deserialize)]
struct ApiResponse {
    success: bool,
    result: ApiResult,
}
#[derive(Deserialize)]
struct ApiResult {
    config: ApiConfig,
}
#[derive(Deserialize)]
struct ApiConfig {
    ingress: Vec<ApiIngress>,
}
#[derive(Deserialize)]
struct ApiIngress {
    hostname: Option<String>,
    path: Option<String>,
    service: String,
}
fn parse_remote_configuration(bytes: &[u8]) -> Result<RemoteTunnelConfig, String> {
    let response: ApiResponse = serde_json::from_slice(bytes)
        .map_err(|_| "remote Cloudflare tunnel configuration is malformed".to_owned())?;
    if !response.success {
        return Err("remote Cloudflare tunnel configuration was rejected".to_owned());
    }
    Ok(RemoteTunnelConfig {
        ingress: response
            .result
            .config
            .ingress
            .into_iter()
            .map(|rule| RemoteIngress {
                hostname: rule.hostname,
                path: rule.path,
                service: rule.service,
            })
            .collect(),
    })
}

pub(crate) async fn verify_remote_ingress<C: RemoteTunnelConfiguration>(
    client: &C,
    profile: &CloudflaredConfig,
    runner: &RunnerConfig,
    callback_id: Uuid,
) -> Result<(), String> {
    let (Some(account_id), Some(api_token), Some(_token)) =
        (&profile.account_id, &profile.api_token, &profile.token)
    else {
        return Err("remote Cloudflare tunnel verification is not configured".to_owned());
    };
    let config = client
        .get(account_id, &profile.tunnel_id, api_token)
        .await?;
    let expected_path = format!("^{}/{}$", runner.webhook_path, callback_id);
    let expected_service = format!("http://{}", runner.bind_address);
    let [route, fallback] = config.ingress.as_slice() else {
        return Err(
            "remote Cloudflare tunnel ingress does not restrict the webhook callback".to_owned(),
        );
    };
    if route.hostname.as_deref() != Some(profile.hostname.as_str())
        || route.path.as_deref() != Some(expected_path.as_str())
        || route.service != expected_service
        || fallback.hostname.is_some()
        || fallback.path.is_some()
        || fallback.service != "http_status:404"
    {
        return Err(
            "remote Cloudflare tunnel ingress does not restrict the webhook callback".to_owned(),
        );
    }
    Ok(())
}

/// The one enrollment-time cloudflared child. It has its own process group so
/// cleanup includes descendants created by the connector.
pub(crate) struct ConnectorSupervisor {
    child: Child,
    process_group: libc::pid_t,
}

impl ConnectorSupervisor {
    pub(crate) fn start(
        profile: &CloudflaredConfig,
        connector: &PreparedConnector,
        cancelled: &AtomicBool,
    ) -> Result<Self, String> {
        // A token identifies a remotely managed tunnel. Its lifecycle and
        // configuration contract are intentionally a separate delivery mode.
        let metrics = reserve_loopback_metrics_address()?;
        Self::start_at(profile, connector, &metrics, cancelled)
    }

    fn start_at(
        profile: &CloudflaredConfig,
        connector: &PreparedConnector,
        metrics: &str,
        cancelled: &AtomicBool,
    ) -> Result<Self, String> {
        // Resolve before clearing the child environment. Config accepts a
        // PATH-resolved `cloudflared`, while the connector deliberately gives
        // its child no inherited environment.
        let executable = crate::config::resolve_executable(&profile.executable)
            .ok_or_else(|| "cannot start managed Cloudflare tunnel".to_owned())?;
        let mut command = Command::new(executable);
        command.env_clear();
        match &connector.authentication {
            ConnectorAuthentication::CredentialConfiguration(configuration_file) => {
                // Credential-file tunnels own their ingress locally.
                command
                    .arg("--config")
                    .arg(configuration_file)
                    .arg("--metrics")
                    .arg(metrics)
                    .arg("tunnel")
                    .arg("run")
                    .arg(&profile.tunnel_id);
            }
            ConnectorAuthentication::RemoteTokenFile(token_file) => {
                // A remotely managed tunnel obtains ingress exclusively from
                // the Cloudflare dashboard. `--token-file` is supported by
                // cloudflared 2025.4.0 and later and does not expose the
                // credential in argv or environment.
                command
                    .arg("--metrics")
                    .arg(metrics)
                    .arg("tunnel")
                    .arg("run")
                    .arg("--token-file")
                    .arg(token_file);
            }
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .map_err(|_| "cannot start managed Cloudflare tunnel".to_owned())?;
        let supervisor = Self {
            process_group: child.id() as libc::pid_t,
            child,
        };
        supervisor.wait_until_ready(metrics, cancelled)?;
        Ok(supervisor)
    }

    fn wait_until_ready(&self, metrics: &str, cancelled: &AtomicBool) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if cancelled.load(Ordering::Relaxed) {
                return Err("managed Cloudflare tunnel enrollment cancelled".to_owned());
            }
            if child_has_exited(self.child.id())? {
                return Err("managed Cloudflare tunnel exited before becoming ready".to_owned());
            }
            if metrics_show_active_connection(metrics) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("managed Cloudflare tunnel did not become ready in time".to_owned());
            }
            thread::sleep(READY_POLL);
        }
    }

    pub(crate) fn exited(&self) -> Result<bool, String> {
        child_has_exited(self.child.id())
    }
}

impl Drop for ConnectorSupervisor {
    fn drop(&mut self) {
        // SIGKILL gives bounded cleanup on all enrollment exits. The direct
        // child remains unreaped until after the identity check, so its group
        // ID cannot be recycled into an unrelated target.
        let observed = unsafe { libc::getpgid(self.process_group) };
        if observed == self.process_group {
            unsafe { libc::kill(-self.process_group, libc::SIGKILL) };
        }
        let _ = self.child.wait();
    }
}

fn reserve_loopback_metrics_address() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|_| "cannot reserve loopback connector metrics address".to_owned())?;
    let address = listener
        .local_addr()
        .map_err(|_| "cannot reserve loopback connector metrics address".to_owned())?;
    Ok(address.to_string())
}

fn child_has_exited(pid: u32) -> Result<bool, String> {
    let mut information = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            information.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result != 0 {
        return Err("cannot supervise managed Cloudflare tunnel".to_owned());
    }
    let information = unsafe { information.assume_init() };
    Ok(unsafe { information.si_pid() } != 0)
}

fn metrics_show_active_connection(address: &str) -> bool {
    let Ok(address) = address.parse() else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&address, READY_POLL) else {
        return false;
    };
    let deadline = Instant::now() + READY_POLL;
    let _ = stream.set_read_timeout(Some(READY_POLL));
    let _ = stream.set_write_timeout(Some(READY_POLL));
    if stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .is_err()
    {
        return false;
    }
    if stream.shutdown(Shutdown::Write).is_err() {
        return false;
    }
    let mut response = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        if Instant::now() >= deadline || response.len() >= MAX_METRICS_RESPONSE_BYTES {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let _ = stream.set_read_timeout(Some(remaining));
        match stream.read(&mut buffer) {
            Ok(0) => return metrics_contain_active_connection(&response),
            Ok(length) => {
                let available = MAX_METRICS_RESPONSE_BYTES - response.len();
                response.extend_from_slice(&buffer[..length.min(available)]);
                if metrics_contain_active_connection(&response) {
                    return true;
                }
            }
            Err(_) => return false,
        }
    }
}

fn metrics_contain_active_connection(response: &[u8]) -> bool {
    String::from_utf8_lossy(response).lines().any(|line| {
        line.starts_with("cloudflared_tunnel_ha_connections")
            && line
                .split_whitespace()
                .last()
                .and_then(|value| value.parse::<f64>().ok())
                .is_some_and(|connections| connections >= 1.0)
    })
}

/// A private, local connector configuration.  Starting cloudflared is left to
/// the supervision boundary; this module neither contacts Cloudflare nor
/// changes an account resource.
#[derive(PartialEq, Eq)]
pub(crate) struct PreparedConnector {
    authentication: ConnectorAuthentication,
}

#[derive(PartialEq, Eq)]
enum ConnectorAuthentication {
    CredentialConfiguration(PathBuf),
    RemoteTokenFile(PathBuf),
}

impl std::fmt::Debug for PreparedConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedConnector")
            .field("authentication", &"<redacted>")
            .finish()
    }
}

pub(crate) fn prepare(
    profile: &CloudflaredConfig,
    paths: &HostPaths,
    runner: &RunnerConfig,
    callback_id: Uuid,
) -> Result<PreparedConnector, String> {
    let credentials_file = profile
        .credentials_file
        .as_ref()
        .ok_or_else(|| "managed Cloudflare tunnel credentials are unavailable".to_owned())?;
    let private_configuration_directory = paths
        .config_file
        .parent()
        .ok_or_else(|| "cannot locate private connector credential directory".to_owned())?;
    ensure_private_directory(private_configuration_directory)
        .map_err(|_| "cannot secure private connector credential directory".to_owned())?;
    if credentials_file.parent() != Some(private_configuration_directory)
        || !safe_credential_filename(credentials_file)
    {
        return Err(
            "cloudflared credentials must be stored in the private configuration directory"
                .to_owned(),
        );
    }
    require_private_file(credentials_file)?;
    let directory = paths.state_directory.join("cloudflared");
    ensure_private_directory(&directory)
        .map_err(|_| "cannot prepare private connector configuration directory".to_owned())?;
    let destination = directory.join(CONFIGURATION_FILE);
    write_private_configuration(&destination, &render(profile, runner, callback_id))?;
    Ok(PreparedConnector {
        authentication: ConnectorAuthentication::CredentialConfiguration(destination),
    })
}

/// Store the dashboard-issued connector token in private state so cloudflared
/// can consume it with `--token-file`. This keeps the credential out of argv
/// and the connector environment while deliberately avoiding a local ingress
/// configuration for remotely managed tunnels.
pub(crate) fn prepare_remote(
    profile: &CloudflaredConfig,
    paths: &HostPaths,
) -> Result<PreparedConnector, String> {
    let token = profile
        .token
        .as_deref()
        .ok_or_else(|| "managed Cloudflare tunnel token is unavailable".to_owned())?;
    let directory = paths.state_directory.join("cloudflared");
    ensure_private_directory(&directory)
        .map_err(|_| "cannot prepare private connector credential directory".to_owned())?;
    let destination = directory.join(REMOTE_TOKEN_FILE);
    write_private_token(&destination, token)?;
    Ok(PreparedConnector {
        authentication: ConnectorAuthentication::RemoteTokenFile(destination),
    })
}

/// Establish the ordering boundary for remote enrollment: a token file can
/// only exist after the exact dashboard ingress has been verified. A caller
/// cannot start a connector without the resulting prepared capability.
pub(crate) async fn verify_and_prepare_remote<C: RemoteTunnelConfiguration>(
    client: &C,
    profile: &CloudflaredConfig,
    paths: &HostPaths,
    runner: &RunnerConfig,
    callback_id: Uuid,
) -> Result<PreparedConnector, String> {
    verify_remote_ingress(client, profile, runner, callback_id).await?;
    prepare_remote(profile, paths)
}

fn safe_credential_filename(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            !name.is_empty()
                && name.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
                })
        })
}

fn render(profile: &CloudflaredConfig, runner: &RunnerConfig, callback_id: Uuid) -> String {
    // Configured hostnames are validated as DNS names and UUIDs have a fixed
    // syntax, so these values cannot introduce YAML structure.
    let credentials = {
        let path = profile
            .credentials_file
            .as_ref()
            .expect("credential mode was checked before rendering");
        // The path is intentionally written only to a 0600 generated file.
        // JSON strings are valid YAML scalars. This protects the entire
        // host-derived path, not only the credential filename.
        format!(
            "credentials-file: {}\n",
            serde_json::to_string(&path.to_string_lossy()).unwrap()
        )
    };
    format!(
        "tunnel: {}\n{credentials}ingress:\n  - hostname: {}\n    path: ^{}/{}$\n    service: http://{}\n  - service: http_status:404\n",
        profile.tunnel_id, profile.hostname, runner.webhook_path, callback_id, runner.bind_address,
    )
}

fn require_private_file(path: &Path) -> Result<(), String> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| "cannot safely read cloudflared credentials".to_owned())?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect cloudflared credentials".to_owned())?;
    if !metadata.is_file() || metadata.mode() & 0o777 != 0o600 {
        return Err(
            "cloudflared credentials must be a private regular file with mode 0600".to_owned(),
        );
    }
    Ok(())
}

fn write_private_configuration(destination: &Path, contents: &str) -> Result<(), String> {
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("private connector configuration path is unsafe".to_owned());
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)
        .map_err(|_| "cannot write private connector configuration".to_owned())?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "cannot secure private connector configuration".to_owned())?;
    file.write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|_| "cannot write private connector configuration".to_owned())?;
    Ok(())
}

fn write_private_token(destination: &Path, token: &str) -> Result<(), String> {
    if let Ok(metadata) = fs::symlink_metadata(destination) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("private connector credential path is unsafe".to_owned());
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(destination)
        .map_err(|_| "cannot write private connector credential".to_owned())?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "cannot secure private connector credential".to_owned())?;
    file.write_all(token.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|_| "cannot write private connector credential".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Read, Write};
    use std::os::unix::fs::{OpenOptionsExt, symlink};
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "agent-handover-cloudflared-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn profile(credentials_file: PathBuf) -> CloudflaredConfig {
        CloudflaredConfig {
            executable: "cloudflared".into(),
            hostname: "handover.example.test".into(),
            tunnel_id: "00000000-0000-4000-8000-000000000079".into(),
            credentials_file: Some(credentials_file),
            token: None,
            account_id: None,
            api_token: None,
        }
    }

    fn runner() -> RunnerConfig {
        RunnerConfig {
            reconciliation_interval_seconds: 60,
            bind_address: "127.0.0.1:8080".into(),
            webhook_path: "/notion/webhook".into(),
            health_path: "/health".into(),
        }
    }

    struct FakeRemote(Result<RemoteTunnelConfig, String>);
    impl RemoteTunnelConfiguration for FakeRemote {
        fn get<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<RemoteTunnelConfig, String>> + Send + 'a>> {
            Box::pin(async move { self.0.clone() })
        }
    }
    fn remote_profile() -> CloudflaredConfig {
        CloudflaredConfig {
            executable: "cloudflared".into(),
            hostname: "handover.example.test".into(),
            tunnel_id: "00000000-0000-4000-8000-000000000079".into(),
            credentials_file: None,
            token: Some("connector-token".into()),
            account_id: Some("0123456789abcdef0123456789abcdef".into()),
            api_token: Some("read-token".into()),
        }
    }
    #[tokio::test]
    async fn accepts_only_the_exact_remote_callback_route_and_non_forwarding_fallback() {
        let callback = Uuid::parse_str("00000000-0000-4000-8000-000000000078").unwrap();
        let remote = FakeRemote(Ok(RemoteTunnelConfig {
            ingress: vec![
                RemoteIngress {
                    hostname: Some("handover.example.test".into()),
                    path: Some("^/notion/webhook/00000000-0000-4000-8000-000000000078$".into()),
                    service: "http://127.0.0.1:8080".into(),
                },
                RemoteIngress {
                    hostname: None,
                    path: None,
                    service: "http_status:404".into(),
                },
            ],
        }));
        verify_remote_ingress(&remote, &remote_profile(), &runner(), callback)
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn rejects_remote_ingress_that_can_forward_another_callback_path() {
        let remote = FakeRemote(Ok(RemoteTunnelConfig {
            ingress: vec![
                RemoteIngress {
                    hostname: Some("handover.example.test".into()),
                    path: Some("^/notion/webhook/.*$".into()),
                    service: "http://127.0.0.1:8080".into(),
                },
                RemoteIngress {
                    hostname: None,
                    path: None,
                    service: "http_status:404".into(),
                },
            ],
        }));
        assert_eq!(
            verify_remote_ingress(&remote, &remote_profile(), &runner(), Uuid::nil())
                .await
                .unwrap_err(),
            "remote Cloudflare tunnel ingress does not restrict the webhook callback"
        );
    }

    #[tokio::test]
    async fn rejected_remote_ingress_never_prepares_a_token_connector() {
        let root = temporary_directory();
        let paths = HostPaths {
            config_file: root.join("config/agent-handover/config.toml"),
            state_directory: root.join("state"),
        };
        let remote = FakeRemote(Ok(RemoteTunnelConfig {
            ingress: vec![
                RemoteIngress {
                    hostname: Some("handover.example.test".into()),
                    path: Some("^/notion/webhook/.*$".into()),
                    service: "http://127.0.0.1:8080".into(),
                },
                RemoteIngress {
                    hostname: None,
                    path: None,
                    service: "http_status:404".into(),
                },
            ],
        }));
        assert_eq!(
            verify_and_prepare_remote(&remote, &remote_profile(), &paths, &runner(), Uuid::nil())
                .await
                .err()
                .unwrap(),
            "remote Cloudflare tunnel ingress does not restrict the webhook callback"
        );
        assert!(
            !paths
                .state_directory
                .join("cloudflared")
                .join(REMOTE_TOKEN_FILE)
                .exists()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writes_private_exact_path_ingress_configuration() {
        let root = temporary_directory();
        let configuration_directory = root.join("config/agent-handover");
        fs::create_dir_all(&configuration_directory).unwrap();
        let credentials = configuration_directory.join("credentials.json");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&credentials)
            .unwrap();
        let paths = HostPaths {
            config_file: configuration_directory.join("config.toml"),
            state_directory: root.join("state"),
        };
        let prepared = prepare(
            &profile(credentials.clone()),
            &paths,
            &runner(),
            Uuid::parse_str("00000000-0000-4000-8000-000000000078").unwrap(),
        )
        .unwrap();
        let ConnectorAuthentication::CredentialConfiguration(configuration_file) =
            &prepared.authentication
        else {
            panic!("credential profile must prepare a local configuration");
        };
        assert_eq!(
            fs::read_to_string(configuration_file).unwrap(),
            "tunnel: 00000000-0000-4000-8000-000000000079\ncredentials-file: \"".to_owned()
                + credentials.to_str().unwrap()
                + "\""
                + "\ningress:\n  - hostname: handover.example.test\n    path: ^/notion/webhook/00000000-0000-4000-8000-000000000078$\n    service: http://127.0.0.1:8080\n  - service: http_status:404\n"
        );
        assert_eq!(
            fs::metadata(configuration_file).unwrap().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(paths.state_directory.join("cloudflared"))
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_unsafe_credential_and_generated_configuration_paths() {
        let root = temporary_directory();
        let configuration_directory = root.join("config/agent-handover");
        fs::create_dir_all(&configuration_directory).unwrap();
        let credentials = configuration_directory.join("credentials.json");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o644)
            .open(&credentials)
            .unwrap();
        let paths = HostPaths {
            config_file: configuration_directory.join("config.toml"),
            state_directory: root.join("state"),
        };
        assert!(
            prepare(
                &profile(credentials.clone()),
                &paths,
                &runner(),
                Uuid::nil()
            )
            .unwrap_err()
            .contains("mode 0600")
        );
        fs::create_dir_all(paths.state_directory.join("cloudflared")).unwrap();
        symlink(
            root.join("target"),
            paths
                .state_directory
                .join("cloudflared/cloudflared-config.yml"),
        )
        .unwrap();
        fs::set_permissions(&credentials, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            prepare(
                &profile(configuration_directory.join("credentials.json")),
                &paths,
                &runner(),
                Uuid::nil(),
            )
            .unwrap_err(),
            "private connector configuration path is unsafe"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_credential_paths_that_escape_or_inject_yaml() {
        let root = temporary_directory();
        let configuration_directory = root.join("config/agent-handover");
        fs::create_dir_all(&configuration_directory).unwrap();
        let paths = HostPaths {
            config_file: configuration_directory.join("config.toml"),
            state_directory: root.join("state"),
        };
        let escaped = configuration_directory.join("../credentials.json");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join("config/credentials.json"))
            .unwrap();
        assert_eq!(
            prepare(&profile(escaped), &paths, &runner(), Uuid::nil()).unwrap_err(),
            "cloudflared credentials must be stored in the private configuration directory"
        );
        let injected = configuration_directory.join("credentials\n ingress.json");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&injected)
            .unwrap();
        assert_eq!(
            prepare(&profile(injected), &paths, &runner(), Uuid::nil()).unwrap_err(),
            "cloudflared credentials must be stored in the private configuration directory"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn quotes_yaml_significant_configuration_directory_characters() {
        let root = temporary_directory();
        let configuration_directory = root.join("config: private # host");
        fs::create_dir_all(&configuration_directory).unwrap();
        let credentials = configuration_directory.join("credentials.json");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&credentials)
            .unwrap();
        let paths = HostPaths {
            config_file: configuration_directory.join("config.toml"),
            state_directory: root.join("state"),
        };

        let prepared = prepare(
            &profile(credentials.clone()),
            &paths,
            &runner(),
            Uuid::nil(),
        )
        .unwrap();
        let ConnectorAuthentication::CredentialConfiguration(configuration_file) =
            prepared.authentication
        else {
            panic!("credential profile must prepare a local configuration");
        };
        let contents = fs::read_to_string(configuration_file).unwrap();
        assert!(contents.contains(&format!(
            "credentials-file: {}\n",
            serde_json::to_string(&credentials.to_string_lossy()).unwrap()
        )));
        assert!(contents.contains("ingress:\n  - hostname: handover.example.test"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepared_connector_debug_does_not_disclose_private_path() {
        let prepared = PreparedConnector {
            authentication: ConnectorAuthentication::CredentialConfiguration(
                "/private/host/config.yml".into(),
            ),
        };
        assert!(!format!("{prepared:?}").contains("/private/host"));
    }

    #[test]
    fn readiness_requires_a_metrics_connection_count() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 256];
            let _ = stream.read(&mut request).unwrap();
            stream
                .write_all(b"HTTP/1.0 200 OK\r\n\r\ncloudflared_tunnel_ha_connections 1\n")
                .unwrap();
        });
        assert!(metrics_show_active_connection(&address));
        server.join().unwrap();
    }

    #[test]
    fn readiness_probe_bounds_a_continuous_metrics_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 256];
            let _ = stream.read(&mut request).unwrap();
            while stream.write_all(b"not a metric yet\n").is_ok() {}
        });

        let started = Instant::now();
        assert!(!metrics_show_active_connection(&address));
        assert!(started.elapsed() < Duration::from_secs(1));
        server.join().unwrap();
    }

    #[test]
    fn dropping_a_supervisor_reaps_its_dedicated_process_group() {
        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg("/bin/sleep 60 & wait")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id() as libc::pid_t;
        let supervisor = ConnectorSupervisor {
            child,
            process_group: pid,
        };
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[test]
    fn fake_connector_receives_only_the_supported_noninteractive_arguments() {
        let root = temporary_directory();
        let arguments = root.join("arguments");
        let environment = root.join("environment");
        let executable = root.join("fake-cloudflared");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s|%s\\n' \"${{HOME-unset}}\" \"${{PATH-unset}}\" > {}\nexit 1\n",
                arguments.display(),
                environment.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = CloudflaredConfig {
            executable,
            hostname: "handover.example.test".into(),
            tunnel_id: "00000000-0000-4000-8000-000000000079".into(),
            credentials_file: Some(root.join("credentials.json")),
            token: None,
            account_id: None,
            api_token: None,
        };
        let error = ConnectorSupervisor::start(
            &profile,
            &PreparedConnector {
                authentication: ConnectorAuthentication::CredentialConfiguration(
                    root.join("private-config.yml"),
                ),
            },
            &AtomicBool::new(false),
        )
        .err()
        .unwrap();
        assert_eq!(
            error,
            "managed Cloudflare tunnel exited before becoming ready"
        );
        let arguments = fs::read_to_string(arguments).unwrap();
        let arguments = arguments.lines().collect::<Vec<_>>();
        assert_eq!(arguments[0], "--config");
        assert_eq!(arguments[2], "--metrics");
        assert_eq!(
            arguments[4..],
            ["tunnel", "run", "00000000-0000-4000-8000-000000000079"]
        );
        // POSIX shells may install their own fallback PATH, but no host HOME
        // or other parent configuration is passed to the connector.
        assert!(
            fs::read_to_string(environment)
                .unwrap()
                .starts_with("unset|")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_token_connector_uses_a_private_token_file_without_local_ingress_or_secret_argv() {
        let root = temporary_directory();
        let arguments = root.join("arguments");
        let environment = root.join("environment");
        let executable = root.join("fake-cloudflared");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s|%s|%s\\n' \"${{HOME-unset}}\" \"${{PATH-unset}}\" \"${{TUNNEL_TOKEN-unset}}\" > {}\nexit 1\n",
                arguments.display(),
                environment.display(),
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = CloudflaredConfig {
            executable,
            ..remote_profile()
        };
        let paths = HostPaths {
            config_file: root.join("config/agent-handover/config.toml"),
            state_directory: root.join("state"),
        };
        let connector = prepare_remote(&profile, &paths).unwrap();
        let ConnectorAuthentication::RemoteTokenFile(token_file) = &connector.authentication else {
            panic!("remote profile must prepare a token file");
        };
        assert_eq!(fs::read_to_string(token_file).unwrap(), "connector-token");
        assert_eq!(fs::metadata(token_file).unwrap().mode() & 0o777, 0o600);
        assert!(
            !paths
                .state_directory
                .join("cloudflared")
                .join(CONFIGURATION_FILE)
                .exists()
        );

        assert_eq!(
            ConnectorSupervisor::start(&profile, &connector, &AtomicBool::new(false))
                .err()
                .unwrap(),
            "managed Cloudflare tunnel exited before becoming ready"
        );
        let arguments = fs::read_to_string(arguments).unwrap();
        assert!(!arguments.contains("connector-token"));
        assert_eq!(arguments.lines().collect::<Vec<_>>()[0], "--metrics");
        assert_eq!(
            arguments.lines().skip(2).collect::<Vec<_>>(),
            [
                "tunnel",
                "run",
                "--token-file",
                token_file.to_str().unwrap()
            ]
        );
        let environment = fs::read_to_string(environment).unwrap();
        assert!(environment.starts_with("unset|"));
        assert!(environment.trim_end().ends_with("|unset"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fake_connector_must_publish_metrics_before_its_group_is_supervised() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_connector(&root, true);
        let pid = supervisor.process_group;
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancellation_during_readiness_reaps_the_fake_connector() {
        let root = temporary_directory();
        let executable = root.join("fake-cloudflared");
        fs::write(&executable, "#!/bin/sh\n/bin/sleep 60 & wait\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = CloudflaredConfig {
            executable,
            hostname: "handover.example.test".into(),
            tunnel_id: "00000000-0000-4000-8000-000000000079".into(),
            credentials_file: Some(root.join("credentials.json")),
            token: None,
            account_id: None,
            api_token: None,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = listener.local_addr().unwrap().to_string();
        drop(listener);
        let cancelled = AtomicBool::new(false);
        thread::scope(|scope| {
            scope.spawn(|| {
                thread::sleep(Duration::from_millis(10));
                cancelled.store(true, Ordering::Relaxed);
            });
            assert_eq!(
                ConnectorSupervisor::start_at(
                    &profile,
                    &PreparedConnector {
                        authentication: ConnectorAuthentication::CredentialConfiguration(
                            root.join("private-config.yml"),
                        ),
                    },
                    &metrics,
                    &cancelled,
                )
                .err()
                .unwrap(),
                "managed Cloudflare tunnel enrollment cancelled"
            );
        });
        fs::remove_dir_all(root).unwrap();
    }

    fn start_fake_ready_connector(root: &Path, keep_running: bool) -> ConnectorSupervisor {
        let executable = root.join("fake-cloudflared");
        let netcat = std::env::var("CLOUDFLARED_TEST_NC")
            .expect("the Nix test environment must provide netcat");
        let listener = if keep_running { "while :; do" } else { "" };
        let suffix = if keep_running { "done &\nwait" } else { "" };
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nport=\"${{4##*:}}\"\n{listener} printf 'HTTP/1.0 200 OK\\r\\n\\r\\ncloudflared_tunnel_ha_connections 2\\n' | {netcat} -l 127.0.0.1 \"$port\"; {suffix}\n"
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = CloudflaredConfig {
            executable,
            hostname: "handover.example.test".into(),
            tunnel_id: "00000000-0000-4000-8000-000000000079".into(),
            credentials_file: Some(root.join("credentials.json")),
            token: None,
            account_id: None,
            api_token: None,
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = listener.local_addr().unwrap().to_string();
        drop(listener);
        ConnectorSupervisor::start_at(
            &profile,
            &PreparedConnector {
                authentication: ConnectorAuthentication::CredentialConfiguration(
                    root.join("private-config.yml"),
                ),
            },
            &metrics,
            &AtomicBool::new(false),
        )
        .unwrap()
    }

    fn start_fake_ready_remote_connector(root: &Path, keep_running: bool) -> ConnectorSupervisor {
        let executable = root.join("fake-cloudflared");
        let netcat = std::env::var("CLOUDFLARED_TEST_NC")
            .expect("the Nix test environment must provide netcat");
        let listener = if keep_running { "while :; do" } else { "" };
        let suffix = if keep_running { "done &\nwait" } else { "" };
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nmetrics=\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = --metrics ]; then metrics=$2; fi\n  shift\ndone\nport=\"${{metrics##*:}}\"\n{listener} printf 'HTTP/1.0 200 OK\\r\\n\\r\\ncloudflared_tunnel_ha_connections 2\\n' | {netcat} -l 127.0.0.1 \"$port\"; {suffix}\n"
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = CloudflaredConfig {
            executable,
            ..remote_profile()
        };
        let paths = HostPaths {
            config_file: root.join("config/agent-handover/config.toml"),
            state_directory: root.join("state"),
        };
        let connector = prepare_remote(&profile, &paths).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = listener.local_addr().unwrap().to_string();
        drop(listener);
        ConnectorSupervisor::start_at(&profile, &connector, &metrics, &AtomicBool::new(false))
            .unwrap()
    }

    #[test]
    fn remote_token_connector_reaches_readiness_and_cleans_up_its_group() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_remote_connector(&root, true);
        let pid = supervisor.process_group;
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_token_cancellation_during_readiness_reaps_the_fake_connector() {
        let root = temporary_directory();
        let pid_file = root.join("pid");
        let executable = root.join("fake-cloudflared");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\necho $$ > {}\n/bin/sleep 60 & wait\n",
                pid_file.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let profile = CloudflaredConfig {
            executable,
            ..remote_profile()
        };
        let paths = HostPaths {
            config_file: root.join("config/agent-handover/config.toml"),
            state_directory: root.join("state"),
        };
        let connector = prepare_remote(&profile, &paths).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = listener.local_addr().unwrap().to_string();
        drop(listener);
        let cancelled = AtomicBool::new(false);
        thread::scope(|scope| {
            scope.spawn(|| {
                thread::sleep(Duration::from_millis(10));
                cancelled.store(true, Ordering::Relaxed);
            });
            assert_eq!(
                ConnectorSupervisor::start_at(&profile, &connector, &metrics, &cancelled)
                    .err()
                    .unwrap(),
                "managed Cloudflare tunnel enrollment cancelled"
            );
        });
        let pid = fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_token_timeout_cleanup_reaps_a_ready_fake_connector_group() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_remote_connector(&root, true);
        let pid = supervisor.process_group;
        // The enrollment timeout drops the same supervisor capability.
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn remote_token_connector_exit_after_readiness_is_observable_and_reaped() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_remote_connector(&root, false);
        let pid = supervisor.process_group;
        let deadline = Instant::now() + Duration::from_secs(1);
        while !supervisor.exited().unwrap() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(supervisor.exited().unwrap());
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn timeout_cleanup_reaps_a_ready_fake_connector_group() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_connector(&root, true);
        let pid = supervisor.process_group;
        // The enrollment timeout exits its select scope, which drops this
        // supervisor; exercise that cleanup boundary directly and deterministically.
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancellation_cleanup_reaps_a_ready_fake_connector_group() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_connector(&root, true);
        let pid = supervisor.process_group;
        // Signal cancellation follows the same bounded select exit as above.
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn post_readiness_fake_connector_exit_is_observable_and_reaped() {
        let root = temporary_directory();
        let supervisor = start_fake_ready_connector(&root, false);
        let pid = supervisor.process_group;
        let deadline = Instant::now() + Duration::from_secs(1);
        while !supervisor.exited().unwrap() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(supervisor.exited().unwrap());
        drop(supervisor);
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        fs::remove_dir_all(root).unwrap();
    }
}
