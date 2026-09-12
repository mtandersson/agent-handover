use crate::config::{CloudflaredConfig, HostPaths, RunnerConfig, ensure_private_directory};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const CONFIGURATION_FILE: &str = "cloudflared-config.yml";

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
    if let Some(credentials_file) = &profile.credentials_file {
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
    }
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
    let credentials = profile
        .credentials_file
        .as_ref()
        .map(|path| {
            // The path is intentionally written only to a 0600 generated file.
            // JSON strings are valid YAML scalars. This protects the entire
            // host-derived path, not only the credential filename.
            format!(
                "credentials-file: {}\n",
                serde_json::to_string(&path.to_string_lossy()).unwrap()
            )
        })
        .unwrap_or_default();
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
    use std::os::unix::fs::{OpenOptionsExt, symlink};
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

    fn profile(credentials_file: Option<PathBuf>) -> CloudflaredConfig {
        CloudflaredConfig {
            executable: "cloudflared".into(),
            hostname: "handover.example.test".into(),
            tunnel_id: "00000000-0000-4000-8000-000000000079".into(),
            credentials_file,
            token: None,
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
            &profile(Some(credentials.clone())),
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
            prepare(&profile(Some(credentials)), &paths, &runner(), Uuid::nil())
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
        let token_profile = CloudflaredConfig {
            token: Some("token-placeholder".into()),
            credentials_file: None,
            ..profile(None)
        };
        assert_eq!(
            prepare(&token_profile, &paths, &runner(), Uuid::nil()).unwrap_err(),
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
            prepare(&profile(Some(escaped)), &paths, &runner(), Uuid::nil()).unwrap_err(),
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
            prepare(&profile(Some(injected)), &paths, &runner(), Uuid::nil()).unwrap_err(),
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
            &profile(Some(credentials.clone())),
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
    fn token_form_writes_ingress_without_disclosing_the_token() {
        let root = temporary_directory();
        let paths = HostPaths {
            config_file: root.join("config/agent-handover/config.toml"),
            state_directory: root.join("state"),
        };
        let profile = CloudflaredConfig {
            token: Some("token-placeholder".into()),
            credentials_file: None,
            ..profile(None)
        };
        let prepared = prepare(&profile, &paths, &runner(), Uuid::nil()).unwrap();
        let contents = fs::read_to_string(&prepared.configuration_file).unwrap();
        assert!(contents.contains("path: ^/notion/webhook/00000000-0000-0000-0000-000000000000$"));
        assert!(contents.ends_with("  - service: http_status:404\n"));
        assert!(!contents.contains("token-placeholder"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepared_connector_debug_does_not_disclose_private_path() {
        let prepared = PreparedConnector {
            configuration_file: "/private/host/config.yml".into(),
        };
        assert!(!format!("{prepared:?}").contains("/private/host"));
    }
}
