use crate::config::{CloudflaredConfig, HostPaths, RunnerConfig, ensure_private_directory};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CONFIGURATION_FILE: &str = "cloudflared-config.yml";
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const READY_POLL: Duration = Duration::from_millis(50);
const MAX_METRICS_RESPONSE_BYTES: usize = 64 * 1024;

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
        let child = Command::new(executable)
            .env_clear()
            .arg("--config")
            .arg(&connector.configuration_file)
            .arg("--metrics")
            .arg(metrics)
            .arg("tunnel")
            .arg("run")
            .arg(&profile.tunnel_id)
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
    pub(crate) configuration_file: PathBuf,
}

impl std::fmt::Debug for PreparedConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedConnector")
            .field("configuration_file", &"<redacted>")
            .finish()
    }
}

pub(crate) fn prepare(
    profile: &CloudflaredConfig,
    paths: &HostPaths,
    runner: &RunnerConfig,
    callback_id: Uuid,
) -> Result<PreparedConnector, String> {
    let credentials_file = &profile.credentials_file;
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
        configuration_file: destination,
    })
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
        let path = &profile.credentials_file;
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
            credentials_file,
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
        assert_eq!(
            fs::read_to_string(&prepared.configuration_file).unwrap(),
            "tunnel: 00000000-0000-4000-8000-000000000079\ncredentials-file: \"".to_owned()
                + credentials.to_str().unwrap()
                + "\""
                + "\ningress:\n  - hostname: handover.example.test\n    path: ^/notion/webhook/00000000-0000-4000-8000-000000000078$\n    service: http://127.0.0.1:8080\n  - service: http_status:404\n"
        );
        assert_eq!(
            fs::metadata(&prepared.configuration_file).unwrap().mode() & 0o777,
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
        let contents = fs::read_to_string(prepared.configuration_file).unwrap();
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
            configuration_file: "/private/host/config.yml".into(),
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
            credentials_file: root.join("credentials.json"),
        };
        let error = ConnectorSupervisor::start(
            &profile,
            &PreparedConnector {
                configuration_file: root.join("private-config.yml"),
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
            credentials_file: root.join("credentials.json"),
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
                        configuration_file: root.join("private-config.yml"),
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
            credentials_file: root.join("credentials.json"),
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = listener.local_addr().unwrap().to_string();
        drop(listener);
        ConnectorSupervisor::start_at(
            &profile,
            &PreparedConnector {
                configuration_file: root.join("private-config.yml"),
            },
            &metrics,
            &AtomicBool::new(false),
        )
        .unwrap()
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
