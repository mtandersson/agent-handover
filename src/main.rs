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

fn run_once_response(count: usize) -> String {
    format!("run-once reconciled {count} Pending task(s)")
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
    use super::{Command, parse_command, run_once_response, usage};

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
    fn run_once_reports_only_the_completed_cycle_count() {
        assert_eq!(
            run_once_response(3),
            "run-once reconciled 3 Pending task(s)"
        );
    }
}
