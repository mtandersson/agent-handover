use std::env;
use std::fs;
use std::io;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

mod cloudflare;
mod config;
mod coordination;
mod discovery;
mod enrollment;
mod executor;
mod http;
mod logging;
mod notion;
mod orchestration;
mod reconciliation;
mod serving;
mod state;

static ENROLLMENT_CANCELLED: AtomicBool = AtomicBool::new(false);

extern "C" fn request_enrollment_cancellation(_: libc::c_int) {
    ENROLLMENT_CANCELLED.store(true, Ordering::Relaxed);
}

struct EnrollmentSignalGuard {
    interrupt: libc::sigaction,
    terminate: libc::sigaction,
}

impl EnrollmentSignalGuard {
    fn install() -> Result<Self, String> {
        ENROLLMENT_CANCELLED.store(false, Ordering::Relaxed);
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = request_enrollment_cancellation as *const () as usize;
        unsafe { libc::sigemptyset(&mut action.sa_mask) };
        let mut interrupt = unsafe { std::mem::zeroed::<libc::sigaction>() };
        if unsafe { libc::sigaction(libc::SIGINT, &action, &mut interrupt) } != 0 {
            return Err("cannot supervise managed tunnel enrollment".to_owned());
        }
        let mut terminate = unsafe { std::mem::zeroed::<libc::sigaction>() };
        if unsafe { libc::sigaction(libc::SIGTERM, &action, &mut terminate) } != 0 {
            unsafe { libc::sigaction(libc::SIGINT, &interrupt, std::ptr::null_mut()) };
            return Err("cannot supervise managed tunnel enrollment".to_owned());
        }
        Ok(Self {
            interrupt,
            terminate,
        })
    }
}

impl Drop for EnrollmentSignalGuard {
    fn drop(&mut self) {
        unsafe {
            libc::sigaction(libc::SIGINT, &self.interrupt, std::ptr::null_mut());
            libc::sigaction(libc::SIGTERM, &self.terminate, std::ptr::null_mut());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Serve,
    RunOnce,
    WebhookEnroll { hostname: String, rotate: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Invocation {
    Command(Command),
    Help,
    CommandHelp(&'static str),
    Version,
}

fn parse_command(args: &[String]) -> Result<Invocation, String> {
    match args {
        [] => Ok(Invocation::Help),
        [flag] if flag == "--help" || flag == "-h" => Ok(Invocation::Help),
        [command, flag]
            if matches!(command.as_str(), "serve" | "run-once" | "webhook-enroll")
                && (flag == "--help" || flag == "-h") =>
        {
            Ok(Invocation::CommandHelp(match command.as_str() {
                "serve" => "serve",
                "run-once" => "run-once",
                "webhook-enroll" => "webhook-enroll",
                _ => unreachable!("guard limits command names"),
            }))
        }
        [command] if command == "serve" => Ok(Invocation::Command(Command::Serve)),
        [command] if command == "run-once" => Ok(Invocation::Command(Command::RunOnce)),
        [command, flag, hostname] if command == "webhook-enroll" && flag == "--hostname" => {
            Ok(Invocation::Command(Command::WebhookEnroll {
                hostname: hostname.clone(),
                rotate: false,
            }))
        }
        [command, flag, hostname, rotate]
            if command == "webhook-enroll" && flag == "--hostname" && rotate == "--rotate" =>
        {
            Ok(Invocation::Command(Command::WebhookEnroll {
                hostname: hostname.clone(),
                rotate: true,
            }))
        }
        [flag] if flag == "--version" || flag == "-V" => Ok(Invocation::Version),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: agent-handover [--help | --version | serve | run-once | webhook-enroll --hostname <HOST> [--rotate]]\n\
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
  5. Optional: configure [cloudflared] only for an existing named Cloudflare
     Tunnel. Credential-file mode lets this runner supervise private local
     ingress; remote-token mode keeps ingress in Cloudflare and requires a
     separate read-only API credential. Omit it for any manually supervised
     tunnel. See README.md before enrolling or rotating a remote-token tunnel.

Commands:
  run-once
      Reconcile current Pending tasks once and execute them sequentially.
      Requires the host configuration. It does not require a webhook token or
      public tunnel.

  webhook-enroll --hostname <HOST> [--rotate]
      Generate a secret callback URL, listen for one Notion verification POST
      on 127.0.0.1:8080, and store the callback ID and token privately. No host
      configuration is required. An optional credential-file [cloudflared]
      profile generates private ingress and supervises the connector during
      enrollment. Remote-token enrollment and rotation use the manual flow
      because the exact callback must be printed before its dashboard route can
      be configured. HOST must be a plain DNS hostname; it is validated locally
      without a network lookup. The command prints the exact HTTPS URL, Notion
      setup links, required events, API version, and verification steps.

      Repeat enrollment is refused. Use --rotate to generate a replacement URL
      and atomically replace the callback ID and token after Notion verifies it.
      Delete and recreate the Notion subscription because a verified URL cannot
      be changed.

  serve
      Reconcile Pending tasks at startup, then keep reconciling while serving
      authenticated webhooks and health checks. Tasks execute sequentially.
      Requires the host configuration and an enrolled webhook token. To receive
      Notion events, route a public HTTPS webhook subscription to the configured
      secret loopback webhook path. With optional [cloudflared], the runner
      supervises an existing named tunnel; otherwise the operator supervises
      the tunnel. Credential-file mode generates private local ingress.
      Remote-token mode verifies operator-managed Cloudflare ingress and never
      changes it. Unlike the one-time enrollment request, serve requires
      X-Notion-Signature authentication over the unmodified body.

Examples:
  agent-handover run-once
  agent-handover webhook-enroll --hostname handover.example.com
  agent-handover serve

Use --version to print the installed version."#
        .to_owned()
}

fn command_help(command: &str) -> String {
    match command {
        "serve" => {
            r#"Serve authenticated Notion webhooks and reconcile Pending tasks.

Usage:
  agent-handover serve

The runner reconciles at startup and periodically while accepting health checks
and authenticated webhook signals. Tasks execute sequentially. A configured
host profile and enrolled webhook token are required.

Options:
  -h, --help  Print this help"#
        }
        "run-once" => {
            r#"Reconcile and drain current Pending Notion tasks once.

Usage:
  agent-handover run-once

Tasks execute sequentially using the configured host profile. A webhook token
and public tunnel are not required.

Options:
  -h, --help  Print this help"#
        }
        "webhook-enroll" => {
            r#"Enroll a Notion connection webhook token on this host.

Usage:
  agent-handover webhook-enroll --hostname <HOST> [--rotate]

Arguments:
  --hostname <HOST>  Public DNS hostname without a scheme, port, path, or
                     credentials

Options:
  --rotate     Replace an existing enrollment after verification succeeds
  -h, --help   Print this help

The command prints a secret callback URL and listens on 127.0.0.1:8080 for one
Notion verification request. Nothing is persisted until verification succeeds.
See README.md for tunnel modes and the complete enrollment procedure.

Example:
  agent-handover webhook-enroll --hostname handover.example.com"#
        }
        _ => unreachable!("only parsed command names have command help"),
    }
    .to_owned()
}

fn run_once_response(count: usize) -> String {
    format!("run-once reconciled {count} Pending task(s)")
}

fn main() -> ExitCode {
    logging::initialize();
    let args = env::args().skip(1).collect::<Vec<_>>();

    let result = match parse_command(&args) {
        Ok(Invocation::Command(Command::WebhookEnroll { hostname, rotate })) => {
            config::HostPaths::discover().and_then(|paths| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| "cannot initialize webhook enrollment runtime".to_owned())?;
                let store = enrollment::FileTokenStore::new(paths.state_directory.clone());
                // Enrollment remains usable without a host profile for an
                // externally managed tunnel. If one exists, only its
                // credential-file delivery mode is managed here.
                if fs::symlink_metadata(&paths.config_file).is_ok() {
                    // An unrelated or incomplete host configuration must not
                    // turn the long-standing config-free manual enrollment
                    // flow into an error. A valid managed profile opts in.
                    match config::load(&paths) {
                        Ok(configured) => {
                        let runner = configured.runner;
                        let profile = configured.cloudflared;
                        if let Some(profile) = profile {
                        if profile.hostname != hostname {
                            return Err("managed tunnel hostname does not match webhook enrollment hostname".to_owned());
                        }
                        store.ensure_available(rotate)?;
                        let callback_id = uuid::Uuid::new_v4();
                        if profile.uses_remote_configuration() {
                            return runtime.block_on(async {
                                let listener = enrollment::bind_listener().await?;
                                let api = cloudflare::CloudflareApi::new()?;
                                let connector = cloudflare::verify_and_prepare_remote(
                                    &api,
                                    &profile,
                                    &paths,
                                    &runner,
                                    callback_id,
                                )
                                .await?;
                                let _signal_guard = EnrollmentSignalGuard::install()?;
                                let supervisor = cloudflare::ConnectorSupervisor::start(
                                    &profile,
                                    &connector,
                                    &ENROLLMENT_CANCELLED,
                                )?;
                                let stdout = io::stdout();
                                let mut output = stdout.lock();
                                let enrollment = enrollment::enroll_with_listener(
                                    &hostname,
                                    callback_id,
                                    listener,
                                    &store,
                                    rotate,
                                    enrollment::TunnelGuidance::ManagedConnectorReady,
                                    &mut output,
                                );
                                tokio::pin!(enrollment);
                                let connector_exit = async {
                                    loop {
                                        if ENROLLMENT_CANCELLED.load(Ordering::Relaxed) {
                                            return Err("managed Cloudflare tunnel enrollment cancelled".to_owned());
                                        }
                                        if supervisor.exited()? {
                                            return Err("managed Cloudflare tunnel exited during enrollment".to_owned());
                                        }
                                        tokio::time::sleep(Duration::from_millis(50)).await;
                                    }
                                };
                                tokio::pin!(connector_exit);
                                tokio::select! {
                                    result = &mut enrollment => result,
                                    result = &mut connector_exit => result,
                                    _ = tokio::time::sleep(Duration::from_secs(900)) => Err("managed tunnel enrollment timed out".to_owned()),
                                }
                            });
                        } else {
                            return runtime.block_on(async {
                            let listener = enrollment::bind_listener().await?;
                            let connector = cloudflare::prepare(
                                &profile,
                                &paths,
                                &runner,
                                callback_id,
                            )?;
                            let _signal_guard = EnrollmentSignalGuard::install()?;
                            let supervisor = cloudflare::ConnectorSupervisor::start(
                                &profile,
                                &connector,
                                &ENROLLMENT_CANCELLED,
                            )?;
                            let stdout = io::stdout();
                            let mut output = stdout.lock();
                            let enrollment = enrollment::enroll_with_listener(
                                &hostname,
                                callback_id,
                                listener,
                                &store,
                                rotate,
                                enrollment::TunnelGuidance::ManagedConnectorReady,
                                &mut output,
                            );
                            tokio::pin!(enrollment);
                            let connector_exit = async {
                                loop {
                                    if ENROLLMENT_CANCELLED.load(Ordering::Relaxed) {
                                        return Err("managed Cloudflare tunnel enrollment cancelled".to_owned());
                                    }
                                    if supervisor.exited()? {
                                        return Err("managed Cloudflare tunnel exited during enrollment".to_owned());
                                    }
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                            };
                            tokio::pin!(connector_exit);
                            tokio::select! {
                                result = &mut enrollment => result,
                                result = &mut connector_exit => result,
                                _ = tokio::time::sleep(Duration::from_secs(900)) => Err("managed tunnel enrollment timed out".to_owned()),
                            }
                            });
                        }
                        }
                        }
                        Err(_) if config::cloudflared_is_configured(&paths)? => {
                            return Err("cannot load managed Cloudflare tunnel profile".to_owned());
                        }
                        Err(_) => {}
                    }
                }
                runtime.block_on(enrollment::enroll(&hostname, &store, rotate, &mut io::stdout().lock()))
            })
        }
        Ok(Invocation::Command(command)) => config::HostPaths::discover().and_then(|paths| {
            config::load(&paths).and_then(|config| match command {
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
                        paths,
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
                    )
                    .with_retry_policy(
                        config.runner.auto_retry_max_retries,
                        config.runner.auto_retry_delay_seconds,
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
                Command::WebhookEnroll { .. } => unreachable!("enrollment handled without config"),
            })
        }),
        Ok(Invocation::Help) => Ok(help()),
        Ok(Invocation::CommandHelp(command)) => Ok(command_help(command)),
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
    use super::{Command, Invocation, command_help, help, parse_command, run_once_response, usage};

    #[test]
    fn shows_help_without_arguments_or_with_help_flags() {
        let help = help();
        assert!(help.contains("Before you begin:"));
        assert!(help.contains("webhook-enroll --hostname <HOST> [--rotate]"));
        assert!(help.contains("plain DNS hostname"));
        assert!(help.contains("credential-file [cloudflared]"));
        assert!(help.contains("Remote-token enrollment and rotation use the manual flow"));
        assert!(help.contains("Delete and recreate the Notion subscription"));
        assert!(help.contains("X-Notion-Signature authentication"));
        assert!(help.contains("Credential-file mode lets this runner supervise"));
        assert!(help.contains("Remote-token mode verifies operator-managed"));
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
    fn shows_help_for_each_command_with_either_help_flag() {
        for command in ["serve", "run-once", "webhook-enroll"] {
            let expected = Invocation::CommandHelp(command);
            assert_eq!(
                parse_command(&[command.to_owned(), "-h".to_owned()]),
                Ok(expected.clone())
            );
            assert_eq!(
                parse_command(&[command.to_owned(), "--help".to_owned()]),
                Ok(expected)
            );
            assert!(command_help(command).contains(&format!("agent-handover {command}")));
        }

        let enrollment_help = command_help("webhook-enroll");
        assert!(enrollment_help.contains("--hostname <HOST>"));
        assert!(enrollment_help.contains("--rotate"));
        assert!(enrollment_help.contains("127.0.0.1:8080"));
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
            parse_command(&[
                "webhook-enroll".to_owned(),
                "--hostname".to_owned(),
                "example.test".to_owned()
            ]),
            Ok(Invocation::Command(Command::WebhookEnroll {
                hostname: "example.test".to_owned(),
                rotate: false
            }))
        );
        assert_eq!(
            parse_command(&[
                "webhook-enroll".to_owned(),
                "--hostname".to_owned(),
                "example.test".to_owned(),
                "--rotate".to_owned()
            ]),
            Ok(Invocation::Command(Command::WebhookEnroll {
                hostname: "example.test".to_owned(),
                rotate: true
            }))
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
