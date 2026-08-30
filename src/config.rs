use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::net::SocketAddr;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const APPLICATION_DIRECTORY: &str = "agent-handover";
const CONFIG_FILE: &str = "config.toml";

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub notion: NotionConfig,
    pub task_properties: TaskProperties,
    pub task_values: TaskValues,
    pub codex: CodexConfig,
    pub runner: RunnerConfig,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NotionConfig {
    pub token: String,
    pub task_data_source_id: String,
    pub journal_data_source_id: String,
}

impl fmt::Debug for NotionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotionConfig")
            .field("token", &"<redacted>")
            .field("task_data_source_id", &"<redacted>")
            .field("journal_data_source_id", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("notion", &self.notion)
            .field("task_properties", &self.task_properties)
            .field("task_values", &self.task_values)
            .field("codex", &self.codex)
            .field("runner", &self.runner)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskProperties {
    pub title: String,
    pub executor: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskValues {
    pub codex: String,
    pub pending: String,
    pub running: String,
    pub error: String,
    pub done: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodexConfig {
    pub executable: PathBuf,
    pub working_directory: PathBuf,
    pub profile: String,
    pub sandbox: SandboxPolicy,
    #[serde(default)]
    pub permitted_environment: Vec<String>,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxPolicy {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl fmt::Display for SandboxPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    pub reconciliation_interval_seconds: u64,
    pub bind_address: String,
    pub webhook_path: String,
    pub health_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostPaths {
    pub config_file: PathBuf,
    pub state_directory: PathBuf,
}

impl HostPaths {
    pub fn discover() -> Result<Self, String> {
        Self::from_environment(
            env::var_os("XDG_CONFIG_HOME"),
            env::var_os("XDG_STATE_HOME"),
            env::var_os("HOME"),
        )
    }

    fn from_environment(
        xdg_config_home: Option<std::ffi::OsString>,
        xdg_state_home: Option<std::ffi::OsString>,
        home: Option<std::ffi::OsString>,
    ) -> Result<Self, String> {
        let config_home = environment_base("XDG_CONFIG_HOME", xdg_config_home)?;
        let state_home = environment_base("XDG_STATE_HOME", xdg_state_home)?;
        let home = if config_home.is_none() || state_home.is_none() {
            environment_base("HOME", home)?
        } else {
            None
        };
        let config_home = config_home.or_else(|| home.as_ref().map(|value| value.join(".config")));
        let state_home =
            state_home.or_else(|| home.as_ref().map(|value| value.join(".local/state")));

        let config_home = config_home
            .ok_or_else(|| "cannot locate configuration: set XDG_CONFIG_HOME or HOME".to_owned())?;
        let state_home = state_home
            .ok_or_else(|| "cannot locate state: set XDG_STATE_HOME or HOME".to_owned())?;

        Ok(Self {
            config_file: config_home.join(APPLICATION_DIRECTORY).join(CONFIG_FILE),
            state_directory: state_home.join(APPLICATION_DIRECTORY),
        })
    }
}

fn environment_base(
    name: &str,
    value: Option<std::ffi::OsString>,
) -> Result<Option<PathBuf>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(format!(
            "environment variable {name} must contain an absolute path"
        ));
    }
    Ok(Some(path))
}

pub fn load(paths: &HostPaths) -> Result<Config, String> {
    ensure_private_parent(&paths.config_file)?;
    ensure_private_directory(&paths.state_directory)?;
    let mut file = open_sensitive_file(&paths.config_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).map_err(|error| {
        format!(
            "cannot read configuration file {}: {error}",
            paths.config_file.display()
        )
    })?;
    let config = toml::from_str::<Config>(&contents).map_err(|_| {
        format!(
            "invalid configuration file {}: check TOML syntax and required fields",
            paths.config_file.display()
        )
    })?;
    validate(&config)?;
    Ok(config)
}

fn ensure_private_parent(file: &Path) -> Result<(), String> {
    let parent = file
        .parent()
        .ok_or_else(|| "configuration path has no parent directory".to_owned())?;
    ensure_private_directory(parent)
}

pub(crate) fn ensure_private_directory(path: &Path) -> Result<(), String> {
    if !path.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .map_err(|error| {
                format!(
                    "cannot create private directory {}: {error}",
                    path.display()
                )
            })?;
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "cannot inspect private directory {}: {error}",
            path.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "private directory {} must not be a symbolic link",
            path.display()
        ));
    }
    if !metadata.is_dir() {
        return Err(format!(
            "private path {} is not a directory",
            path.display()
        ));
    }
    let mode = metadata.mode() & 0o777;
    if mode != 0o700 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
            format!(
                "cannot secure private directory {} as 0700: {error}",
                path.display()
            )
        })?;
    }
    Ok(())
}

fn open_sensitive_file(path: &Path) -> Result<fs::File, String> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                format!(
                    "configuration file {} does not exist; create it with mode 0600",
                    path.display()
                )
            } else {
                format!(
                    "cannot safely open configuration file {}: {error}",
                    path.display()
                )
            }
        })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "cannot inspect opened configuration file {}: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "configuration path {} is not a file",
            path.display()
        ));
    }
    let mode = metadata.mode() & 0o777;
    if mode != 0o600 {
        return Err(format!(
            "configuration file {} must have mode 0600, not {mode:04o}",
            path.display()
        ));
    }
    Ok(file)
}

fn validate(config: &Config) -> Result<(), String> {
    for (name, value) in [
        ("notion.token", config.notion.token.as_str()),
        (
            "notion.task_data_source_id",
            config.notion.task_data_source_id.as_str(),
        ),
        (
            "notion.journal_data_source_id",
            config.notion.journal_data_source_id.as_str(),
        ),
        (
            "task_properties.title",
            config.task_properties.title.as_str(),
        ),
        (
            "task_properties.executor",
            config.task_properties.executor.as_str(),
        ),
        (
            "task_properties.status",
            config.task_properties.status.as_str(),
        ),
        ("task_values.codex", config.task_values.codex.as_str()),
        ("codex.profile", config.codex.profile.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("configuration field {name} must not be empty"));
        }
    }

    let property_names = [
        config.task_properties.title.as_str(),
        config.task_properties.executor.as_str(),
        config.task_properties.status.as_str(),
    ];
    if property_names.into_iter().collect::<HashSet<_>>().len() != property_names.len() {
        return Err("task title, executor, and status property names must be distinct".to_owned());
    }

    let statuses = [
        config.task_values.pending.as_str(),
        config.task_values.running.as_str(),
        config.task_values.error.as_str(),
        config.task_values.done.as_str(),
    ];
    if statuses.iter().any(|value| value.trim().is_empty()) {
        return Err("Pending, Running, Error, and Done status values must not be empty".to_owned());
    }
    if statuses.into_iter().collect::<HashSet<_>>().len() != statuses.len() {
        return Err("Pending, Running, Error, and Done status values must be distinct".to_owned());
    }

    if config.codex.executable.as_os_str().is_empty() {
        return Err("configuration field codex.executable must not be empty".to_owned());
    }
    if !config.codex.working_directory.is_absolute() {
        return Err(
            "configuration field codex.working_directory must be an absolute path".to_owned(),
        );
    }
    if config.codex.timeout_seconds == 0 {
        return Err(
            "configuration field codex.timeout_seconds must be greater than zero".to_owned(),
        );
    }
    if config.runner.reconciliation_interval_seconds == 0 {
        return Err(
            "configuration field runner.reconciliation_interval_seconds must be greater than zero"
                .to_owned(),
        );
    }

    let mut environment = HashSet::new();
    for (index, name) in config.codex.permitted_environment.iter().enumerate() {
        let valid = !name.is_empty()
            && !name.contains('=')
            && name
                .chars()
                .all(|character| character == '_' || character.is_ascii_alphanumeric())
            && !name.starts_with(|character: char| character.is_ascii_digit());
        if !valid {
            return Err(format!(
                "configuration field codex.permitted_environment[{index}] is not a valid environment variable name"
            ));
        }
        if !environment.insert(name) {
            return Err(format!(
                "configuration field codex.permitted_environment[{index}] duplicates an earlier entry"
            ));
        }
    }

    let bind_address = config
        .runner
        .bind_address
        .parse::<SocketAddr>()
        .map_err(|_| {
            "configuration field runner.bind_address must be an IP address and port".to_owned()
        })?;
    if !bind_address.ip().is_loopback() {
        return Err("runner.bind_address must be a loopback address".to_owned());
    }
    validate_endpoint_path("runner.webhook_path", &config.runner.webhook_path)?;
    validate_endpoint_path("runner.health_path", &config.runner.health_path)?;
    if config.runner.webhook_path == config.runner.health_path {
        return Err("runner.webhook_path and runner.health_path must be distinct".to_owned());
    }
    Ok(())
}

fn validate_endpoint_path(name: &str, value: &str) -> Result<(), String> {
    if !value.starts_with('/') || value.len() == 1 || value.contains('?') || value.contains('#') {
        return Err(format!(
            "configuration field {name} must be an absolute HTTP path"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::symlink;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn temporary_directory() -> PathBuf {
        let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "agent-handover-config-test-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn fixture() -> &'static str {
        r#"
[notion]
token = "secret-placeholder"
task_data_source_id = "task-data-source-placeholder"
journal_data_source_id = "journal-data-source-placeholder"

[task_properties]
title = "Name"
executor = "Executor"
status = "Status"

[task_values]
codex = "Codex"
pending = "Pending"
running = "Running"
error = "Error"
done = "Done"

[codex]
executable = "codex"
working_directory = "/srv/project-placeholder"
profile = "runner-placeholder"
sandbox = "workspace-write"
permitted_environment = ["PATH", "NOTION_TOKEN"]
timeout_seconds = 900

[runner]
reconciliation_interval_seconds = 60
bind_address = "127.0.0.1:8080"
webhook_path = "/notion/webhook"
health_path = "/health"
"#
    }

    fn write_fixture(root: &Path, contents: &str, mode: u32) -> HostPaths {
        let config_directory = root.join("config/agent-handover");
        fs::create_dir_all(&config_directory).unwrap();
        let config_file = config_directory.join("config.toml");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&config_file)
            .unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        HostPaths {
            config_file,
            state_directory: root.join("state/agent-handover"),
        }
    }

    #[test]
    fn resolves_xdg_locations_before_home_fallbacks() {
        let paths = HostPaths::from_environment(
            Some(OsString::from("/xdg/config")),
            Some(OsString::from("/xdg/state")),
            Some(OsString::from("/home/operator")),
        )
        .unwrap();
        assert_eq!(
            paths.config_file,
            Path::new("/xdg/config/agent-handover/config.toml")
        );
        assert_eq!(
            paths.state_directory,
            Path::new("/xdg/state/agent-handover")
        );
    }

    #[test]
    fn resolves_standard_home_fallbacks() {
        let paths = HostPaths::from_environment(None, None, Some(OsString::from("/home/operator")))
            .unwrap();
        assert_eq!(
            paths.config_file,
            Path::new("/home/operator/.config/agent-handover/config.toml")
        );
        assert_eq!(
            paths.state_directory,
            Path::new("/home/operator/.local/state/agent-handover")
        );
    }

    #[test]
    fn treats_empty_xdg_locations_as_unset() {
        let paths = HostPaths::from_environment(
            Some(OsString::new()),
            Some(OsString::new()),
            Some(OsString::from("/home/operator")),
        )
        .unwrap();
        assert_eq!(
            paths.config_file,
            Path::new("/home/operator/.config/agent-handover/config.toml")
        );
        assert_eq!(
            paths.state_directory,
            Path::new("/home/operator/.local/state/agent-handover")
        );
    }

    #[test]
    fn rejects_relative_xdg_locations() {
        let error = HostPaths::from_environment(
            Some(OsString::from("relative/config")),
            None,
            Some(OsString::from("/home/operator")),
        )
        .unwrap_err();
        assert_eq!(
            error,
            "environment variable XDG_CONFIG_HOME must contain an absolute path"
        );
    }

    #[test]
    fn rejects_relative_home_fallbacks() {
        let error = HostPaths::from_environment(None, None, Some(OsString::from("relative/home")))
            .unwrap_err();
        assert_eq!(
            error,
            "environment variable HOME must contain an absolute path"
        );
    }

    #[test]
    fn ignores_home_when_both_absolute_xdg_locations_are_available() {
        let paths = HostPaths::from_environment(
            Some(OsString::from("/xdg/config")),
            Some(OsString::from("/xdg/state")),
            Some(OsString::from("relative/home")),
        )
        .unwrap();
        assert_eq!(
            paths.config_file,
            Path::new("/xdg/config/agent-handover/config.toml")
        );
    }

    #[test]
    fn loads_a_complete_private_configuration_and_secures_directories() {
        let root = temporary_directory();
        let paths = write_fixture(&root, fixture(), 0o600);
        fs::set_permissions(
            paths.config_file.parent().unwrap(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();

        let config = load(&paths).unwrap();

        assert_eq!(config.task_values.codex, "Codex");
        assert_eq!(
            fs::metadata(paths.config_file.parent().unwrap())
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(paths.state_directory).unwrap().mode() & 0o777,
            0o700
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_a_configuration_file_that_is_not_private() {
        let root = temporary_directory();
        let paths = write_fixture(&root, fixture(), 0o644);
        let error = load(&paths).unwrap_err();
        assert!(error.contains("must have mode 0600"));
        assert!(!error.contains("secret-placeholder"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_incomplete_schema_mappings_without_exposing_secrets() {
        let root = temporary_directory();
        let contents = fixture().replace("status = \"Status\"", "status = \"\"");
        let paths = write_fixture(&root, &contents, 0o600);
        let error = load(&paths).unwrap_err();
        assert_eq!(
            error,
            "configuration field task_properties.status must not be empty"
        );
        assert!(!error.contains("secret-placeholder"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_non_loopback_binding() {
        let root = temporary_directory();
        let contents = fixture().replace("127.0.0.1:8080", "0.0.0.0:8080");
        let paths = write_fixture(&root, &contents, 0o600);
        assert_eq!(
            load(&paths).unwrap_err(),
            "runner.bind_address must be a loopback address"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn permitted_environment_errors_do_not_echo_invalid_or_duplicate_entries() {
        let root = temporary_directory();
        let contents = fixture().replace(
            "[\"PATH\", \"NOTION_TOKEN\"]",
            "[\"PATH\", \"TOKEN=actual-secret\"]",
        );
        let paths = write_fixture(&root, &contents, 0o600);
        let error = load(&paths).unwrap_err();
        assert_eq!(
            error,
            "configuration field codex.permitted_environment[1] is not a valid environment variable name"
        );
        assert!(!error.contains("TOKEN=actual-secret"));
        fs::remove_dir_all(root).unwrap();

        let root = temporary_directory();
        let contents = fixture().replace("[\"PATH\", \"NOTION_TOKEN\"]", "[\"PATH\", \"PATH\"]");
        let paths = write_fixture(&root, &contents, 0o600);
        let error = load(&paths).unwrap_err();
        assert_eq!(
            error,
            "configuration field codex.permitted_environment[1] duplicates an earlier entry"
        );
        assert!(!error.contains("PATH"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn debug_output_redacts_the_notion_token() {
        let root = temporary_directory();
        let paths = write_fixture(&root, fixture(), 0o600);
        let config = load(&paths).unwrap();
        let output = format!("{config:?}");
        assert!(output.contains("<redacted>"));
        assert!(!output.contains("secret-placeholder"));
        assert!(!output.contains("task-data-source-placeholder"));
        assert!(!output.contains("journal-data-source-placeholder"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_symbolic_link_configuration_directories() {
        let root = temporary_directory();
        let target = root.join("real-config");
        fs::create_dir(&target).unwrap();
        let linked = root.join("config/agent-handover");
        fs::create_dir(linked.parent().unwrap()).unwrap();
        symlink(&target, &linked).unwrap();
        let paths = HostPaths {
            config_file: linked.join("config.toml"),
            state_directory: root.join("state/agent-handover"),
        };
        let error = load(&paths).unwrap_err();
        assert!(error.contains("must not be a symbolic link"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_symbolic_link_state_directories() {
        let root = temporary_directory();
        let paths = write_fixture(&root, fixture(), 0o600);
        let target = root.join("real-state");
        fs::create_dir(&target).unwrap();
        fs::create_dir(paths.state_directory.parent().unwrap()).unwrap();
        symlink(&target, &paths.state_directory).unwrap();
        let error = load(&paths).unwrap_err();
        assert!(error.contains("must not be a symbolic link"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_symbolic_link_configuration_files() {
        let root = temporary_directory();
        let target = root.join("real-config.toml");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&target)
            .unwrap();
        file.write_all(fixture().as_bytes()).unwrap();
        let config_directory = root.join("config/agent-handover");
        fs::create_dir_all(&config_directory).unwrap();
        let config_file = config_directory.join("config.toml");
        symlink(&target, &config_file).unwrap();
        let paths = HostPaths {
            config_file,
            state_directory: root.join("state/agent-handover"),
        };
        let error = load(&paths).unwrap_err();
        assert!(error.contains("cannot safely open configuration file"));
        fs::remove_dir_all(root).unwrap();
    }
}
