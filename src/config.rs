use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::net::SocketAddr;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const APPLICATION_DIRECTORY: &str = "agent-handover";
const CONFIG_FILE: &str = "config.toml";
const NTN_TOKEN_TIMEOUT: Duration = Duration::from_secs(5);
const NTN_TOKEN_OUTPUT_LIMIT: usize = 4 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub notion: NotionConfig,
    pub task_properties: TaskProperties,
    pub task_values: TaskValues,
    pub journal_properties: JournalProperties,
    pub journal_values: JournalValues,
    pub codex: CodexConfig,
    pub runner: RunnerConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    notion: FileNotionConfig,
    task_properties: TaskProperties,
    task_values: TaskValues,
    journal_properties: JournalProperties,
    journal_values: JournalValues,
    codex: CodexConfig,
    runner: RunnerConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileNotionConfig {
    token: Option<String>,
    task_data_source_id: String,
    journal_data_source_id: String,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NotionConfig {
    pub token: String,
    pub task_data_source_id: String,
    pub journal_data_source_id: String,
}

trait NotionTokenResolver {
    fn resolve(&self) -> Result<String, String>;
}

struct NtnTokenResolver {
    executable: PathBuf,
    timeout: Duration,
    output_limit: usize,
}

impl Default for NtnTokenResolver {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("ntn"),
            timeout: NTN_TOKEN_TIMEOUT,
            output_limit: NTN_TOKEN_OUTPUT_LIMIT,
        }
    }
}

impl NotionTokenResolver for NtnTokenResolver {
    fn resolve(&self) -> Result<String, String> {
        let mut child = Command::new(&self.executable)
            .args(["auth", "token", "--plain"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            // Credential discovery intentionally uses the runner's inherited
            // user environment (PATH, HOME/XDG, keyring, and ntn overrides).
            .spawn()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    "cannot resolve Notion token: ntn is unavailable; configure notion.token or install ntn"
                        .to_owned()
                } else {
                    "cannot resolve Notion token: ntn could not be started; configure notion.token or check ntn"
                        .to_owned()
                }
            })?;
        let process_group = child.id() as libc::pid_t;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "cannot resolve Notion token: ntn output was unavailable".to_owned())?;
        let output_limit = self.output_limit;
        let (output_sender, output_receiver) = mpsc::sync_channel(1);
        let reader = thread::spawn(move || {
            let mut output = Vec::new();
            let result = stdout
                .take((output_limit + 1) as u64)
                .read_to_end(&mut output)
                .map(|_| output);
            let _ = output_sender.send(result);
        });

        let deadline = Instant::now() + self.timeout;
        let successful = loop {
            match child_exited_without_reaping(child.id()) {
                Ok(Some(successful)) => break successful,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    terminate_process_group(&mut child, process_group, reader)?;
                    return Err(
                        "cannot resolve Notion token: ntn timed out; configure notion.token or check ntn authentication"
                            .to_owned(),
                    );
                }
                Err(()) => {
                    terminate_process_group(&mut child, process_group, reader)?;
                    return Err(
                        "cannot resolve Notion token: ntn execution failed; configure notion.token or check ntn authentication"
                            .to_owned(),
                    );
                }
            }
        };
        if !successful {
            terminate_process_group(&mut child, process_group, reader)?;
            return Err(
                "cannot resolve Notion token: ntn is unauthenticated or failed; run `ntn login` or configure notion.token"
                    .to_owned(),
            );
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let output = match output_receiver.recv_timeout(remaining) {
            Ok(output) => output,
            Err(_) => {
                terminate_process_group(&mut child, process_group, reader)?;
                return Err("cannot resolve Notion token: ntn output timed out".to_owned());
            }
        };
        let status = child.wait();
        let reader_result = reader
            .join()
            .map_err(|_| "cannot resolve Notion token: ntn output could not be read".to_owned());
        let status =
            status.map_err(|_| "cannot resolve Notion token: ntn execution failed".to_owned())?;
        reader_result?;
        let output = output
            .map_err(|_| "cannot resolve Notion token: ntn output could not be read".to_owned())?;
        if !status.success() {
            return Err(
                "cannot resolve Notion token: ntn is unauthenticated or failed; run `ntn login` or configure notion.token"
                    .to_owned(),
            );
        }
        normalize_resolved_token(output, self.output_limit)
    }
}

fn child_exited_without_reaping(pid: u32) -> Result<Option<bool>, ()> {
    loop {
        let mut information = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        // SAFETY: `information` points to writable storage for siginfo_t, the
        // PID is the retained direct child, and waitid initializes it on
        // success.
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                information.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            // SAFETY: waitid returned success and initialized siginfo_t.
            let information = unsafe { information.assume_init() };
            if unsafe { information.si_pid() } == 0 {
                return Ok(None);
            }
            let successful = information.si_code == libc::CLD_EXITED
                // SAFETY: waitid reported an exited child, so si_status is set.
                && unsafe { information.si_status() } == 0;
            return Ok(Some(successful));
        }
        if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            return Err(());
        }
    }
}

fn terminate_process_group<T>(
    child: &mut Child,
    process_group: libc::pid_t,
    reader: thread::JoinHandle<T>,
) -> Result<ExitStatus, String> {
    // The unreaped child remains the process-group leader, preventing its PGID
    // from being reused between exit observation and this group signal. Check
    // that identity again before signaling so an anomalous wait cannot target
    // an unrelated group.
    // SAFETY: process_group is the positive PID returned for the child that was
    // launched with process_group(0).
    let observed_group = unsafe { libc::getpgid(process_group) };
    let (signal_result, signal_error) = if observed_group == process_group {
        // SAFETY: the retained child still leads this dedicated group;
        // negation addresses that group rather than an individual process.
        let result = unsafe { libc::kill(-process_group, libc::SIGKILL) };
        (result, std::io::Error::last_os_error())
    } else if observed_group == -1
        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    {
        (0, std::io::Error::from_raw_os_error(libc::ESRCH))
    } else {
        (-1, std::io::Error::from_raw_os_error(libc::EINVAL))
    };
    let status = child.wait();
    let reader_result = reader
        .join()
        .map_err(|_| "cannot resolve Notion token: ntn cleanup failed".to_owned());
    let status =
        status.map_err(|_| "cannot resolve Notion token: ntn cleanup failed".to_owned())?;
    reader_result?;
    if signal_result != 0 && signal_error.raw_os_error() != Some(libc::ESRCH) {
        return Err("cannot resolve Notion token: ntn cleanup failed".to_owned());
    }
    Ok(status)
}

fn normalize_resolved_token(output: Vec<u8>, output_limit: usize) -> Result<String, String> {
    if output.len() > output_limit {
        return Err("cannot resolve Notion token: ntn returned oversized output".to_owned());
    }
    let output = String::from_utf8(output)
        .map_err(|_| "cannot resolve Notion token: ntn returned an invalid token".to_owned())?;
    let token = output.trim();
    if token.is_empty() || !token.chars().all(|character| character.is_ascii_graphic()) {
        return Err("cannot resolve Notion token: ntn returned an invalid token".to_owned());
    }
    Ok(token.to_owned())
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
            .field("journal_properties", &self.journal_properties)
            .field("journal_values", &self.journal_values)
            .field("codex", &self.codex)
            .field("runner", &self.runner)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskProperties {
    pub title: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskValues {
    pub pending: String,
    pub running: String,
    pub error: String,
    pub done: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JournalProperties {
    pub run_id: String,
    pub task: String,
    pub executor: String,
    pub started_at: String,
    pub ended_at: String,
    pub outcome: String,
    pub summary: String,
    pub actions: String,
    pub warnings: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JournalValues {
    pub executor: String,
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
    load_with_resolver(paths, &NtnTokenResolver::default())
}

fn load_with_resolver(
    paths: &HostPaths,
    token_resolver: &dyn NotionTokenResolver,
) -> Result<Config, String> {
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
    let file_config = toml::from_str::<FileConfig>(&contents).map_err(|_| {
        format!(
            "invalid configuration file {}: check TOML syntax and required fields",
            paths.config_file.display()
        )
    })?;
    let token = match file_config.notion.token {
        Some(token) => token,
        None => token_resolver.resolve()?,
    };
    let config = Config {
        notion: NotionConfig {
            token,
            task_data_source_id: file_config.notion.task_data_source_id,
            journal_data_source_id: file_config.notion.journal_data_source_id,
        },
        task_properties: file_config.task_properties,
        task_values: file_config.task_values,
        journal_properties: file_config.journal_properties,
        journal_values: file_config.journal_values,
        codex: file_config.codex,
        runner: file_config.runner,
    };
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
            "task_properties.status",
            config.task_properties.status.as_str(),
        ),
        ("codex.profile", config.codex.profile.as_str()),
        (
            "journal_properties.run_id",
            config.journal_properties.run_id.as_str(),
        ),
        (
            "journal_properties.task",
            config.journal_properties.task.as_str(),
        ),
        (
            "journal_properties.executor",
            config.journal_properties.executor.as_str(),
        ),
        (
            "journal_properties.started_at",
            config.journal_properties.started_at.as_str(),
        ),
        (
            "journal_properties.ended_at",
            config.journal_properties.ended_at.as_str(),
        ),
        (
            "journal_properties.outcome",
            config.journal_properties.outcome.as_str(),
        ),
        (
            "journal_properties.summary",
            config.journal_properties.summary.as_str(),
        ),
        (
            "journal_properties.actions",
            config.journal_properties.actions.as_str(),
        ),
        (
            "journal_properties.warnings",
            config.journal_properties.warnings.as_str(),
        ),
        (
            "journal_values.executor",
            config.journal_values.executor.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(format!("configuration field {name} must not be empty"));
        }
    }

    let property_names = [
        config.task_properties.title.as_str(),
        config.task_properties.status.as_str(),
    ];
    if property_names.into_iter().collect::<HashSet<_>>().len() != property_names.len() {
        return Err("task title and status property names must be distinct".to_owned());
    }

    let journal_property_names = [
        config.journal_properties.run_id.as_str(),
        config.journal_properties.task.as_str(),
        config.journal_properties.executor.as_str(),
        config.journal_properties.started_at.as_str(),
        config.journal_properties.ended_at.as_str(),
        config.journal_properties.outcome.as_str(),
        config.journal_properties.summary.as_str(),
        config.journal_properties.actions.as_str(),
        config.journal_properties.warnings.as_str(),
    ];
    if journal_property_names
        .into_iter()
        .collect::<HashSet<_>>()
        .len()
        != journal_property_names.len()
    {
        return Err("journal property names must be distinct".to_owned());
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
    use std::cell::Cell;
    use std::ffi::OsString;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::symlink;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    static NTN_PROCESS_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct FakeTokenResolver {
        calls: Cell<usize>,
        result: Result<String, String>,
    }

    impl NotionTokenResolver for FakeTokenResolver {
        fn resolve(&self) -> Result<String, String> {
            self.calls.set(self.calls.get() + 1);
            self.result.clone()
        }
    }

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
status = "Status"

[task_values]
pending = "Pending"
running = "Running"
error = "Error"
done = "Done"

[journal_properties]
run_id = "Run ID"
task = "Task"
executor = "Executor"
started_at = "Started at"
ended_at = "Ended at"
outcome = "Outcome"
summary = "Summary"
actions = "Actions"
warnings = "Warnings"

[journal_values]
executor = "Codex"

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

    fn write_executable(root: &Path, name: &str, contents: &str) -> PathBuf {
        let path = root.join(name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(&path)
            .unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        path
    }

    fn resolver(executable: PathBuf, timeout: Duration, output_limit: usize) -> NtnTokenResolver {
        NtnTokenResolver {
            executable,
            timeout,
            output_limit,
        }
    }

    fn assert_process_disappears(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if !Path::new(&format!("/proc/{pid}")).exists() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "resolver descendant {pid} was not reaped"
            );
            thread::sleep(Duration::from_millis(10));
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

        assert_eq!(config.codex.executable, Path::new("codex"));
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
    fn configured_notion_token_has_priority_without_credential_discovery() {
        let root = temporary_directory();
        let paths = write_fixture(&root, fixture(), 0o600);
        let token_resolver = FakeTokenResolver {
            calls: Cell::new(0),
            result: Err("resolver must not run".to_owned()),
        };
        let config = load_with_resolver(&paths, &token_resolver).unwrap();
        assert_eq!(config.notion.token, "secret-placeholder");
        assert_eq!(token_resolver.calls.get(), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicitly_configured_empty_token_is_rejected_without_fallback() {
        let root = temporary_directory();
        let contents = fixture().replace("token = \"secret-placeholder\"", "token = \" \"");
        let paths = write_fixture(&root, &contents, 0o600);
        let token_resolver = FakeTokenResolver {
            calls: Cell::new(0),
            result: Ok("resolved-placeholder".to_owned()),
        };
        assert_eq!(
            load_with_resolver(&paths, &token_resolver).unwrap_err(),
            "configuration field notion.token must not be empty"
        );
        assert_eq!(token_resolver.calls.get(), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_notion_token_uses_resolved_credential() {
        let root = temporary_directory();
        let contents = fixture().replace("token = \"secret-placeholder\"\n", "");
        let paths = write_fixture(&root, &contents, 0o600);
        let token_resolver = FakeTokenResolver {
            calls: Cell::new(0),
            result: Ok("resolved-placeholder".to_owned()),
        };
        let config = load_with_resolver(&paths, &token_resolver).unwrap();
        assert_eq!(config.notion.token, "resolved-placeholder");
        assert_eq!(token_resolver.calls.get(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_notion_token_requires_successful_credential_discovery() {
        let root = temporary_directory();
        let contents = fixture().replace("token = \"secret-placeholder\"\n", "");
        let paths = write_fixture(&root, &contents, 0o600);
        let token_resolver = FakeTokenResolver {
            calls: Cell::new(0),
            result: Err("credential discovery unavailable".to_owned()),
        };
        assert_eq!(
            load_with_resolver(&paths, &token_resolver).unwrap_err(),
            "credential discovery unavailable"
        );
        assert_eq!(token_resolver.calls.get(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ntn_token_command_is_noninteractive_and_trims_outer_whitespace() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let executable = write_executable(
            &root,
            "ntn-placeholder",
            "#!/bin/sh\n[ \"$1 $2 $3\" = \"auth token --plain\" ] || exit 9\nprintf '  resolved-placeholder\\n'\n",
        );
        let token = resolver(executable, Duration::from_secs(1), 128)
            .resolve()
            .unwrap();
        assert_eq!(token, "resolved-placeholder");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ntn_token_command_rejects_invalid_and_oversized_output() {
        assert!(
            normalize_resolved_token(b"first-placeholder\nsecond-placeholder\n".to_vec(), 128)
                .unwrap_err()
                .contains("invalid token")
        );
        assert!(
            normalize_resolved_token(b"placeholder".to_vec(), 4)
                .unwrap_err()
                .contains("oversized output")
        );
        assert!(
            normalize_resolved_token(b" \n\t".to_vec(), 128)
                .unwrap_err()
                .contains("invalid token")
        );
    }

    #[test]
    fn ntn_token_command_limits_captured_output() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let executable = write_executable(
            &root,
            "ntn-verbose-placeholder",
            "#!/bin/sh\nprintf 'oversized-placeholder'\n",
        );
        let error = resolver(executable, Duration::from_secs(1), 4)
            .resolve()
            .unwrap_err();
        assert!(error.contains("oversized output"));
        assert!(!error.contains("placeholder"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ntn_token_command_bounds_execution_time() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let executable =
            write_executable(&root, "ntn-slow-placeholder", "#!/bin/sh\nexec sleep 2\n");
        let error = resolver(executable, Duration::from_millis(25), 128)
            .resolve()
            .unwrap_err();
        assert!(error.contains("timed out"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ntn_timeout_terminates_descendants_that_inherit_stdout() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let pid_file = root.join("descendant.pid");
        let executable = write_executable(
            &root,
            "ntn-descendant-placeholder",
            &format!(
                "#!/bin/sh\nsleep 30 &\ndescendant=$!\nprintf '%s' \"$descendant\" > {}\nwait \"$descendant\"\n",
                pid_file.display()
            ),
        );
        let started = Instant::now();
        let error = resolver(executable, Duration::from_millis(100), 128)
            .resolve()
            .unwrap_err();
        assert!(error.contains("timed out"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
        let pid = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
        assert_process_disappears(pid);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ntn_output_timeout_cleans_descendants_and_reader_threads_repeatedly() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let executable = root.join("ntn-orphan-placeholder");
        for attempt in 0..3 {
            let pid_file = root.join(format!("descendant-{attempt}.pid"));
            if attempt == 0 {
                write_executable(
                    &root,
                    "ntn-orphan-placeholder",
                    &format!(
                        "#!/bin/sh\nsleep 30 &\nprintf '%s' \"$!\" > {}\nexit 0\n",
                        pid_file.display()
                    ),
                );
            } else {
                let script = format!(
                    "#!/bin/sh\nsleep 30 &\nprintf '%s' \"$!\" > {}\nexit 0\n",
                    pid_file.display()
                );
                fs::write(&executable, script).unwrap();
                fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
            }
            let started = Instant::now();
            let error = resolver(executable.clone(), Duration::from_secs(1), 128)
                .resolve()
                .unwrap_err();
            assert!(error.contains("output timed out"));
            assert!(started.elapsed() < Duration::from_secs(2));
            let pid = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
            assert_process_disappears(pid);
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ntn_token_command_failures_do_not_expose_command_output() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let executable = write_executable(
            &root,
            "ntn-failing-placeholder",
            "#!/bin/sh\nprintf 'stdout-secret-placeholder'\nprintf 'stderr-secret-placeholder' >&2\nexit 1\n",
        );
        let error = resolver(executable, Duration::from_secs(1), 128)
            .resolve()
            .unwrap_err();
        assert!(error.contains("unauthenticated or failed"));
        assert!(!error.contains("secret-placeholder"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unavailable_ntn_has_an_actionable_error() {
        let _process_guard = NTN_PROCESS_TEST_LOCK.lock().unwrap();
        let root = temporary_directory();
        let error = resolver(
            root.join("missing-ntn-placeholder"),
            Duration::from_secs(1),
            128,
        )
        .resolve()
        .unwrap_err();
        assert!(error.contains("ntn is unavailable"));
        assert!(error.contains("configure notion.token"));
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
    fn rejects_legacy_notion_executor_mappings() {
        let root = temporary_directory();
        let contents = fixture()
            .replace(
                "title = \"Name\"",
                "title = \"Name\"\nexecutor = \"Executor\"",
            )
            .replace(
                "pending = \"Pending\"",
                "codex = \"Codex\"\npending = \"Pending\"",
            );
        let paths = write_fixture(&root, &contents, 0o600);
        let error = load(&paths).unwrap_err();
        assert!(error.contains("invalid configuration file"));
        assert!(!error.contains("Executor"));
        assert!(!error.contains("Codex"));
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
