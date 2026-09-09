use std::env;
use std::io;
use std::process::ExitCode;

mod config;
mod coordination;
mod discovery;
mod enrollment;
mod executor;
mod http;
mod notion;
mod orchestration;
mod reconciliation;
mod serving;
mod state;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Command {
    Serve,
    RunOnce,
    WebhookEnroll { rotate: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Invocation {
    Command(Command),
    Help,
    Version,
}

fn parse_command(args: &[String]) -> Result<Invocation, String> {
    match args {
        [] => Ok(Invocation::Help),
        [flag] if flag == "--help" || flag == "-h" => Ok(Invocation::Help),
        [command] if command == "serve" => Ok(Invocation::Command(Command::Serve)),
        [command] if command == "run-once" => Ok(Invocation::Command(Command::RunOnce)),
        [command] if command == "webhook-enroll" => {
            Ok(Invocation::Command(Command::WebhookEnroll {
                rotate: false,
            }))
        }
        [command, flag] if command == "webhook-enroll" && flag == "--rotate" => {
            Ok(Invocation::Command(Command::WebhookEnroll { rotate: true }))
        }
        [flag] if flag == "--version" || flag == "-V" => Ok(Invocation::Version),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: agent-handover [--help | --version | serve | run-once | webhook-enroll [--rotate]]\n\
     Run `agent-handover --help` for setup and command guidance."
        .to_owned()
}

fn help() -> String {
    r#"agent-handover runs queued Notion tasks through Codex on this host.

Usage:
  agent-handover [--help | -h] [--version | -V]
  agent-handover <command>

Before you begin:
  1. Build or install agent-handover.
  2. Create a private, mode 0600 configuration file at
     $XDG_CONFIG_HOME/agent-handover/config.toml (or
     ~/.config/agent-handover/config.toml).
  3. Configure a Notion connection with access to the task and journal data
     sources, their property mappings, and a Codex executable and absolute
     working directory. See README.md for the configuration template.
  4. Share both Notion data sources with that connection. Eligible tasks have
     Status = Pending; their page body is the instruction sent to Codex.

Commands:
  run-once
      Reconcile current Pending tasks once and execute them sequentially.
      Requires the host configuration. It does not require a webhook token or
      public tunnel.

  webhook-enroll [--rotate]
      Read a Notion webhook verification JSON payload from standard input and
      store its token privately. Use --rotate only to replace an enrolled token.
      Requires the host configuration.

  serve
      Reconcile Pending tasks at startup, then keep reconciling while serving
      authenticated webhooks and health checks. Tasks execute sequentially.
      Requires the host configuration and an enrolled webhook token. To receive
      Notion events, route a public HTTPS webhook subscription to the configured
      loopback webhook path through an externally managed tunnel.

Examples:
  agent-handover run-once
  printf '%s\n' '{"verification_token":"<TOKEN>"}' | agent-handover webhook-enroll
  agent-handover serve

Use --version to print the installed version."#
        .to_owned()
}

fn run_once_response(count: usize) -> String {
    format!("run-once reconciled {count} Pending task(s)")
}

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();

    let result = match parse_command(&args) {
        Ok(Invocation::Command(command)) => config::HostPaths::discover().and_then(|paths| {
            config::load(&paths).and_then(|config| match command {
                Command::WebhookEnroll { rotate } => enrollment::enroll(
                    io::stdin().lock(),
                    &enrollment::FileTokenStore::new(paths.state_directory),
                    rotate,
                ),
                Command::Serve => notion::NotionHttpClient::new(
                    &config.notion,
                    &config.task_properties,
                    &config.journal_properties,
                    &config.journal_values,
                )
                .and_then(|notion| {
                    serving::serve(
                        &config,
                        &enrollment::FileTokenStore::new(paths.state_directory.clone()),
                        paths.state_directory,
                        notion,
                        &config.notion,
                        &config.task_values,
                    )
                }),
                Command::RunOnce => notion::NotionHttpClient::new(
                    &config.notion,
                    &config.task_properties,
                    &config.journal_properties,
                    &config.journal_values,
                )
                .and_then(|notion| {
                    let runner_lock = std::sync::Arc::new(
                        state::AttemptStore::new(paths.state_directory.clone()).acquire()?,
                    );
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|_| "cannot initialize run-once runtime".to_owned())?;
                    let workflow = orchestration::ExecutionWorkflow::new(
                        runner_lock,
                        notion.clone(),
                        executor::CodexExecutor::new(config.codex.clone()),
                        config.task_values.clone(),
                        config.journal_values.executor.clone(),
                    );
                    let coordinator = coordination::RevisionCoordinator::new(workflow);
                    let count = runtime.block_on(async {
                        coordinator.recover().await?;
                        reconciliation::reconcile_once(
                            &notion,
                            &coordinator,
                            &config.notion,
                            &config.task_values,
                        )
                        .await
                    })?;
                    Ok(run_once_response(count))
                }),
            })
        }),
        Ok(Invocation::Help) => Ok(help()),
        Ok(Invocation::Version) => Ok(format!("agent-handover {}", env!("CARGO_PKG_VERSION"))),
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
    use super::{Command, Invocation, help, parse_command, run_once_response, usage};

    #[test]
    fn shows_help_without_arguments_or_with_help_flags() {
        assert!(help().contains("Before you begin:"));
        assert!(help().contains("webhook-enroll [--rotate]"));
        assert_eq!(parse_command(&[]), Ok(Invocation::Help));
        assert_eq!(parse_command(&["--help".to_owned()]), Ok(Invocation::Help));
        assert_eq!(parse_command(&["-h".to_owned()]), Ok(Invocation::Help));
    }

    #[test]
    fn reports_the_package_version() {
        assert_eq!(
            parse_command(&["--version".to_owned()]),
            Ok(Invocation::Version)
        );
        assert_eq!(parse_command(&["-V".to_owned()]), Ok(Invocation::Version));
    }

    #[test]
    fn parses_each_runner_command() {
        assert_eq!(
            parse_command(&["serve".to_owned()]),
            Ok(Invocation::Command(Command::Serve))
        );
        assert_eq!(
            parse_command(&["run-once".to_owned()]),
            Ok(Invocation::Command(Command::RunOnce))
        );
        assert_eq!(
            parse_command(&["webhook-enroll".to_owned()]),
            Ok(Invocation::Command(Command::WebhookEnroll {
                rotate: false
            }))
        );
        assert_eq!(
            parse_command(&["webhook-enroll".to_owned(), "--rotate".to_owned()]),
            Ok(Invocation::Command(Command::WebhookEnroll { rotate: true }))
        );
    }

    #[test]
    fn rejects_unsupported_arguments_with_the_usage() {
        let error = parse_command(&["unexpected".to_owned()]).unwrap_err();
        assert_eq!(error, usage());
        assert!(error.contains("Run `agent-handover --help`"));
    }

    #[test]
    fn run_once_reports_only_the_completed_cycle_count() {
        assert_eq!(
            run_once_response(3),
            "run-once reconciled 3 Pending task(s)"
        );
    }
}
