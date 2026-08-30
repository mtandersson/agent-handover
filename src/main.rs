use std::env;
use std::io;
use std::process::ExitCode;

mod config;
mod discovery;
mod enrollment;
mod http;
mod notion;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Command {
    Serve,
    RunOnce,
    WebhookEnroll { rotate: bool },
}

fn parse_command(args: &[String]) -> Result<Option<Command>, String> {
    match args {
        [] => Ok(None),
        [command] if command == "serve" => Ok(Some(Command::Serve)),
        [command] if command == "run-once" => Ok(Some(Command::RunOnce)),
        [command] if command == "webhook-enroll" => {
            Ok(Some(Command::WebhookEnroll { rotate: false }))
        }
        [command, flag] if command == "webhook-enroll" && flag == "--rotate" => {
            Ok(Some(Command::WebhookEnroll { rotate: true }))
        }
        [flag] if flag == "--version" || flag == "-V" => Ok(None),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: agent-handover [--version | serve | run-once | webhook-enroll [--rotate]]".to_owned()
}

fn command_response(
    command: Command,
    config: &config::Config,
    paths: &config::HostPaths,
) -> String {
    let name = match command {
        Command::Serve => "serve",
        Command::RunOnce => "run-once",
        Command::WebhookEnroll { .. } => "webhook-enroll",
    };
    format!(
        "{name} configuration ready: state={}, codex={}, workdir={}, profile={}, sandbox={}, environment={}, timeout={}s, reconcile={}s, bind={}, webhook={}, health={}",
        paths.state_directory.display(),
        config.codex.executable.display(),
        config.codex.working_directory.display(),
        config.codex.profile,
        config.codex.sandbox,
        config.codex.permitted_environment.join(","),
        config.codex.timeout_seconds,
        config.runner.reconciliation_interval_seconds,
        config.runner.bind_address,
        config.runner.webhook_path,
        config.runner.health_path,
    )
}

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();

    let result = match parse_command(&args) {
        Ok(Some(command)) => config::HostPaths::discover().and_then(|paths| {
            config::load(&paths).and_then(|config| match command {
                Command::WebhookEnroll { rotate } => enrollment::enroll(
                    io::stdin().lock(),
                    &enrollment::FileTokenStore::new(paths.state_directory),
                    rotate,
                ),
                Command::Serve => {
                    notion::NotionHttpClient::new(&config.notion, &config.task_properties).and_then(
                        |notion| {
                            let dispatcher = discovery::NotionEventDispatcher::new(
                                notion,
                                discovery::PendingDiscoverySink,
                                &config.notion,
                                &config.task_values,
                            );
                            http::serve(
                                &config.runner,
                                &enrollment::FileTokenStore::new(paths.state_directory),
                                dispatcher,
                            )
                        },
                    )
                }
                Command::RunOnce => Ok(command_response(command, &config, &paths)),
            })
        }),
        Ok(None) if args.iter().any(|arg| arg == "--version" || arg == "-V") => {
            Ok(format!("agent-handover {}", env!("CARGO_PKG_VERSION")))
        }
        Ok(None) => Ok("agent-handover is ready".to_owned()),
        Err(error) => Err(error),
    };

    match result {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, command_response, parse_command, usage};
    use crate::config::{
        CodexConfig, Config, HostPaths, NotionConfig, RunnerConfig, SandboxPolicy, TaskProperties,
        TaskValues,
    };
    use std::path::PathBuf;

    #[test]
    fn reports_that_the_command_is_ready_without_arguments() {
        assert_eq!(parse_command(&[]), Ok(None));
    }

    #[test]
    fn reports_the_package_version() {
        assert_eq!(parse_command(&["--version".to_owned()]), Ok(None));
    }

    #[test]
    fn parses_each_runner_command() {
        assert_eq!(
            parse_command(&["serve".to_owned()]),
            Ok(Some(Command::Serve))
        );
        assert_eq!(
            parse_command(&["run-once".to_owned()]),
            Ok(Some(Command::RunOnce))
        );
        assert_eq!(
            parse_command(&["webhook-enroll".to_owned()]),
            Ok(Some(Command::WebhookEnroll { rotate: false }))
        );
        assert_eq!(
            parse_command(&["webhook-enroll".to_owned(), "--rotate".to_owned()]),
            Ok(Some(Command::WebhookEnroll { rotate: true }))
        );
    }

    #[test]
    fn rejects_unsupported_arguments_with_the_usage() {
        assert_eq!(parse_command(&["unexpected".to_owned()]), Err(usage()));
    }

    #[test]
    fn command_summary_exposes_operational_configuration_but_not_secrets() {
        let config = Config {
            notion: NotionConfig {
                token: "secret-placeholder".to_owned(),
                task_data_source_id: "task-placeholder".to_owned(),
                journal_data_source_id: "journal-placeholder".to_owned(),
            },
            task_properties: TaskProperties {
                title: "Name".to_owned(),
                executor: "Executor".to_owned(),
                status: "Status".to_owned(),
            },
            task_values: TaskValues {
                codex: "Codex".to_owned(),
                pending: "Pending".to_owned(),
                running: "Running".to_owned(),
                error: "Error".to_owned(),
                done: "Done".to_owned(),
            },
            codex: CodexConfig {
                executable: PathBuf::from("codex"),
                working_directory: PathBuf::from("/srv/project-placeholder"),
                profile: "runner-placeholder".to_owned(),
                sandbox: SandboxPolicy::WorkspaceWrite,
                permitted_environment: vec!["PATH".to_owned()],
                timeout_seconds: 900,
            },
            runner: RunnerConfig {
                reconciliation_interval_seconds: 60,
                bind_address: "127.0.0.1:8080".to_owned(),
                webhook_path: "/notion/webhook".to_owned(),
                health_path: "/health".to_owned(),
            },
        };
        let paths = HostPaths {
            config_file: PathBuf::from("/config-placeholder/config.toml"),
            state_directory: PathBuf::from("/state-placeholder"),
        };

        for command in [Command::Serve, Command::RunOnce] {
            let summary = command_response(command, &config, &paths);
            assert!(summary.contains("configuration ready"));
            assert!(summary.contains("sandbox=workspace-write"));
            assert!(!summary.contains("secret-placeholder"));
            assert!(!summary.contains("task-placeholder"));
        }
    }
}
