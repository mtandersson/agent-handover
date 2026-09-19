use tracing_subscriber::EnvFilter;

pub(crate) fn initialize() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("agent_handover=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .with_writer(std::io::stderr)
        .try_init();
}

pub(crate) fn task_attempt_started() {
    tracing::info!("task attempt preparation started");
}

pub(crate) fn task_attempt_finished(result: &Result<(), String>) {
    if result.is_ok() {
        tracing::info!(outcome = "completed", "task attempt finished");
    } else {
        tracing::warn!(outcome = "failed", "task attempt finished");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    struct Writer(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Writer;

        fn make_writer(&'a self) -> Self::Writer {
            Writer(Arc::clone(&self.0))
        }
    }

    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn capture(filter: &str, emit: impl FnOnce()) -> String {
        let output = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_env_filter(EnvFilter::new(filter))
            .with_writer(output.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        let bytes = output.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn documented_debug_filter_is_supported() {
        assert!(EnvFilter::try_new("agent_handover=debug").is_ok());
    }

    #[test]
    fn debug_filter_adds_debug_events_without_hiding_info_events() {
        let info = capture("info", || {
            tracing::info!("visible lifecycle");
            tracing::debug!("diagnostic detail");
        });
        assert!(info.contains("visible lifecycle"));
        assert!(!info.contains("diagnostic detail"));

        let debug = capture("debug", || {
            tracing::info!("visible lifecycle");
            tracing::debug!("diagnostic detail");
        });
        assert!(debug.contains("visible lifecycle"));
        assert!(debug.contains("diagnostic detail"));
    }

    #[test]
    fn every_started_task_attempt_has_a_privacy_safe_terminal_event() {
        for result in [Ok(()), Err("private-task-or-adapter-error".to_owned())] {
            let output = capture("info", || {
                task_attempt_started();
                task_attempt_finished(&result);
            });
            assert!(output.contains("task attempt preparation started"));
            assert!(output.contains("task attempt finished"));
            assert!(!output.contains("private-task-or-adapter-error"));
        }
    }
}
