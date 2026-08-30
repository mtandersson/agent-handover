use std::env;
use std::process::ExitCode;

fn response(args: &[String]) -> Result<String, String> {
    match args {
        [] => Ok("agent-handover is ready".to_owned()),
        [flag] if flag == "--version" || flag == "-V" => {
            Ok(format!("agent-handover {}", env!("CARGO_PKG_VERSION")))
        }
        _ => Err("usage: agent-handover [--version]".to_owned()),
    }
}

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();

    match response(&args) {
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
    use super::response;

    #[test]
    fn reports_that_the_command_is_ready_without_arguments() {
        assert_eq!(response(&[]), Ok("agent-handover is ready".to_owned()));
    }

    #[test]
    fn reports_the_package_version() {
        assert_eq!(
            response(&["--version".to_owned()]),
            Ok(format!("agent-handover {}", env!("CARGO_PKG_VERSION")))
        );
    }

    #[test]
    fn rejects_unsupported_arguments_with_the_usage() {
        assert_eq!(
            response(&["unexpected".to_owned()]),
            Err("usage: agent-handover [--version]".to_owned())
        );
    }
}
