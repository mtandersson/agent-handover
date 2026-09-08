use crate::config::CodexConfig;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const MAX_RESULT_BYTES: usize = 64 * 1024;

/// Provider-neutral boundary used by orchestration.  The task document is the
/// complete instruction supplied to an executor; callers must not add prompt
/// fragments from other sources.
pub(crate) trait Executor: Send + Sync {
    fn execute(&self, request: ExecutorRequest) -> Result<ExecutorResult, String>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutorRequest {
    pub(crate) instructions: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutorResult {
    pub(crate) outcome: Outcome,
    pub(crate) summary: String,
    pub(crate) actions: Vec<String>,
    pub(crate) warnings: Vec<String>,
}

impl ExecutorResult {
    pub(crate) fn is_valid(&self) -> bool {
        !self.summary.trim().is_empty()
            && self
                .actions
                .iter()
                .chain(&self.warnings)
                .all(|value| !value.trim().is_empty())
            && serde_json::to_vec(self).is_ok_and(|encoded| encoded.len() <= MAX_RESULT_BYTES)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Done,
    Error,
}

pub(crate) struct CodexExecutor {
    config: CodexConfig,
}

impl CodexExecutor {
    pub(crate) fn new(config: CodexConfig) -> Self {
        Self { config }
    }

    fn arguments(&self, schema: &Path, output: &Path) -> Vec<String> {
        vec![
            "exec".to_owned(),
            "--ephemeral".to_owned(),
            "--sandbox".to_owned(),
            self.config.sandbox.to_string(),
            "--profile".to_owned(),
            self.config.profile.clone(),
            "--output-schema".to_owned(),
            schema.display().to_string(),
            "--output-last-message".to_owned(),
            output.display().to_string(),
            "-".to_owned(),
        ]
    }
}

impl Executor for CodexExecutor {
    fn execute(&self, request: ExecutorRequest) -> Result<ExecutorResult, String> {
        let temporary = TemporaryFiles::create()?;
        fs::write(temporary.schema(), RESULT_SCHEMA)
            .map_err(|_| "cannot prepare Codex result schema".to_owned())?;
        let mut command = Command::new(&self.config.executable);
        command
            .args(self.arguments(&temporary.schema(), &temporary.output()))
            .current_dir(&self.config.working_directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .env_clear();
        for name in &self.config.permitted_environment {
            if let Some(value) = env::var_os(name) {
                command.env(name, value);
            }
        }
        let mut child = command
            .spawn()
            .map_err(|_| "Codex executor could not be started".to_owned())?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "Codex executor input was unavailable".to_owned())?;
        let instructions = request.instructions;
        let writer = thread::spawn(move || stdin.write_all(instructions.as_bytes()));
        let process_group = child.id() as libc::pid_t;
        let deadline = Instant::now() + Duration::from_secs(self.config.timeout_seconds);
        loop {
            match child_exited_without_reaping(child.id()) {
                Ok(Some(false)) => {
                    terminate_process_group(&mut child, process_group);
                    let _ = writer.join();
                    return Err("Codex executor exited unsuccessfully".to_owned());
                }
                Ok(Some(true)) => {
                    // The direct child is deliberately still unreaped here,
                    // so it remains the process-group leader while cleanup
                    // safely terminates any background descendants.
                    terminate_process_group(&mut child, process_group);
                    writer
                        .join()
                        .map_err(|_| "cannot provide task instructions to Codex".to_owned())?
                        .map_err(|_| "cannot provide task instructions to Codex".to_owned())?;
                    break;
                }
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Ok(None) => {
                    terminate_process_group(&mut child, process_group);
                    let _ = writer.join();
                    return Err("Codex executor timed out".to_owned());
                }
                Err(()) => return Err("Codex executor could not be monitored".to_owned()),
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(temporary.output())
            .map_err(|_| "Codex executor did not produce a result".to_owned())?;
        let metadata = file
            .metadata()
            .map_err(|_| "cannot inspect Codex result".to_owned())?;
        if !metadata.file_type().is_file() || metadata.file_type().is_fifo() {
            return Err("Codex result is not a regular file".to_owned());
        }
        if metadata.len() > MAX_RESULT_BYTES as u64 {
            return Err("Codex executor returned an oversized result".to_owned());
        }
        let mut bytes = Vec::new();
        file.take(MAX_RESULT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read Codex result".to_owned())?;
        if bytes.len() > MAX_RESULT_BYTES {
            return Err("Codex executor returned an oversized result".to_owned());
        }
        let result: ExecutorResult = serde_json::from_slice(&bytes)
            .map_err(|_| "Codex executor returned an invalid result".to_owned())?;
        if !result.is_valid() {
            return Err("Codex executor returned an invalid result".to_owned());
        }
        Ok(result)
    }
}

fn child_exited_without_reaping(pid: u32) -> Result<Option<bool>, ()> {
    let mut information = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: `information` is writable storage and the retained PID is a
    // direct child. WNOWAIT leaves that child available for safe cleanup.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            information.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result != 0 {
        return Err(());
    }
    // SAFETY: waitid initialized siginfo_t after returning success.
    let information = unsafe { information.assume_init() };
    if unsafe { information.si_pid() } == 0 {
        return Ok(None);
    }
    Ok(Some(
        information.si_code == libc::CLD_EXITED && unsafe { information.si_status() } == 0,
    ))
}

fn terminate_process_group(child: &mut std::process::Child, process_group: libc::pid_t) {
    // Confirm that the unreaped direct child is still its group's leader before
    // signaling. This prevents a PID reuse race from targeting another group.
    // SAFETY: `process_group` was the direct child's positive PID.
    let group = unsafe { libc::getpgid(process_group) };
    if group == process_group {
        // SAFETY: the direct child still owns this dedicated process group.
        let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }
    let _ = child.wait();
}

struct TemporaryFiles {
    directory: PathBuf,
}

impl TemporaryFiles {
    fn create() -> Result<Self, String> {
        for suffix in 0..100_u32 {
            let directory = env::temp_dir().join(format!(
                "agent-handover-codex-{}-{suffix}",
                std::process::id()
            ));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
                        .map_err(|_| "cannot secure Codex temporary files".to_owned())?;
                    return Ok(Self { directory });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err("cannot prepare Codex temporary files".to_owned()),
            }
        }
        Err("cannot prepare Codex temporary files".to_owned())
    }

    fn schema(&self) -> PathBuf {
        self.directory.join("result-schema.json")
    }
    fn output(&self) -> PathBuf {
        self.directory.join("result.json")
    }
}

impl Drop for TemporaryFiles {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

const RESULT_SCHEMA: &str = r#"{"type":"object","properties":{"outcome":{"type":"string","enum":["done","error"]},"summary":{"type":"string"},"actions":{"type":"array","items":{"type":"string"}},"warnings":{"type":"array","items":{"type":"string"}}},"required":["outcome","summary","actions","warnings"],"additionalProperties":false}"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SandboxPolicy;
    use std::os::unix::fs::PermissionsExt;

    fn executor() -> CodexExecutor {
        CodexExecutor::new(CodexConfig {
            executable: "codex-placeholder".into(),
            working_directory: "/tmp".into(),
            profile: "runner".into(),
            sandbox: SandboxPolicy::WorkspaceWrite,
            permitted_environment: vec!["PATH".into()],
            timeout_seconds: 1,
        })
    }

    #[test]
    fn codex_arguments_preserve_the_configured_sandbox_and_profile() {
        let arguments = executor().arguments(Path::new("/schema"), Path::new("/result"));
        assert_eq!(
            arguments,
            [
                "exec",
                "--ephemeral",
                "--sandbox",
                "workspace-write",
                "--profile",
                "runner",
                "--output-schema",
                "/schema",
                "--output-last-message",
                "/result",
                "-"
            ]
        );
    }

    #[test]
    fn result_requires_all_structured_fields_and_rejects_unknown_fields() {
        assert!(
            serde_json::from_str::<ExecutorResult>(
                r#"{"outcome":"done","summary":"finished","actions":[],"warnings":[]}"#
            )
            .is_ok()
        );
        assert!(
            serde_json::from_str::<ExecutorResult>(
                r#"{"outcome":"blocked","summary":"x","actions":[],"warnings":[]}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ExecutorResult>(
                r#"{"outcome":"done","summary":"x","actions":[],"warnings":[],"output":"private"}"#
            )
            .is_err()
        );
    }

    fn script(body: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "agent-handover-executor-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    fn configured(executable: PathBuf) -> CodexExecutor {
        configured_with_environment(executable, vec![])
    }

    fn configured_with_environment(
        executable: PathBuf,
        permitted_environment: Vec<String>,
    ) -> CodexExecutor {
        CodexExecutor::new(CodexConfig {
            executable,
            working_directory: PathBuf::from("/tmp"),
            profile: "runner".to_owned(),
            sandbox: SandboxPolicy::ReadOnly,
            permitted_environment,
            timeout_seconds: 1,
        })
    }

    #[test]
    fn controlled_command_result_is_validated_without_exposing_its_output() {
        let path = env::var("PATH").unwrap();
        let program = script(&format!(
            "IFS= read -r actual\n[ \"$actual\" = 'private instructions' ] || exit 9\n[ \"$PATH\" = '{path}' ] || exit 8\n[ -z \"$HOME\" ] || exit 7\n[ \"$PWD\" = /tmp ] || exit 6\nwhile [ \"$#\" -gt 0 ]; do if [ \"$1\" = --output-last-message ]; then output=$2; break; fi; shift; done\nprintf '%s' '{{\"outcome\":\"done\",\"summary\":\"finished\",\"actions\":[\"changed file\"],\"warnings\":[]}}' > \"$output\""
        ));
        let result = configured_with_environment(program.clone(), vec!["PATH".to_owned()])
            .execute(ExecutorRequest {
                instructions: "private instructions".to_owned(),
            })
            .unwrap();
        assert_eq!(result.outcome, Outcome::Done);
        let _ = fs::remove_file(program);
    }

    #[test]
    fn process_failure_timeout_and_malformed_results_fail_safely() {
        let failure_program = script("exit 1");
        let failure = configured(failure_program.clone())
            .execute(ExecutorRequest {
                instructions: "private instructions".to_owned(),
            })
            .unwrap_err();
        assert_eq!(failure, "Codex executor exited unsuccessfully");
        let _ = fs::remove_file(failure_program);

        let timeout_program = script("while :; do :; done");
        let timeout = configured(timeout_program.clone())
            .execute(ExecutorRequest {
                instructions: "private instructions".to_owned(),
            })
            .unwrap_err();
        assert_eq!(timeout, "Codex executor timed out");
        let _ = fs::remove_file(timeout_program);

        let malformed_program = script(
            "while [ \"$#\" -gt 0 ]; do if [ \"$1\" = --output-last-message ]; then output=$2; break; fi; shift; done\nprintf 'not json' > \"$output\"",
        );
        let malformed = configured(malformed_program.clone())
            .execute(ExecutorRequest {
                instructions: "private instructions".to_owned(),
            })
            .unwrap_err();
        assert_eq!(malformed, "Codex executor returned an invalid result");
        let _ = fs::remove_file(malformed_program);

        let blocked_program = script(
            "while [ \"$#\" -gt 0 ]; do if [ \"$1\" = --output-last-message ]; then output=$2; break; fi; shift; done\nprintf '%s' '{\"outcome\":\"error\",\"summary\":\"blocked\",\"actions\":[],\"warnings\":[\"needs access\"]}' > \"$output\"",
        );
        let blocked = configured(blocked_program.clone())
            .execute(ExecutorRequest {
                instructions: "private instructions".to_owned(),
            })
            .unwrap();
        assert_eq!(blocked.outcome, Outcome::Error);
        assert_eq!(blocked.summary, "blocked");
        let _ = fs::remove_file(blocked_program);
    }
}
