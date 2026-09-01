use crate::config::{Config, NotionConfig, TaskValues};
use crate::coordination::{PendingPreparationSink, PreparationSink, RevisionCoordinator};
use crate::discovery::NotionEventDispatcher;
use crate::enrollment::TokenSource;
use crate::http;
use crate::notion::{NotionAdapter, NotionHttpClient};
use crate::reconciliation::reconcile_once;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;

pub(crate) fn serve<T: TokenSource>(
    config: &Config,
    token_source: &T,
    notion: NotionHttpClient,
    notion_config: &NotionConfig,
    task_values: &TaskValues,
) -> Result<String, String> {
    let token = token_source.load()?;
    let address: SocketAddr = config
        .runner
        .bind_address
        .parse()
        .map_err(|_| "configured HTTP bind address is invalid".to_owned())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("cannot start HTTP runtime: {error}"))?;
    let coordinator = Arc::new(RevisionCoordinator::new(PendingPreparationSink));
    let dispatcher = NotionEventDispatcher::with_coordinator(
        notion.clone(),
        Arc::clone(&coordinator),
        notion_config,
        task_values,
    );
    runtime.block_on(async {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| format!("cannot bind HTTP server: {error}"))?;
        let shutdown = shutdown_signal()?;
        run(
            listener,
            &config.runner,
            token.into_bytes(),
            notion,
            coordinator,
            dispatcher,
            notion_config,
            task_values,
            shutdown,
        )
        .await
    })?;
    Ok("HTTP server stopped".to_owned())
}

fn shutdown_signal() -> Result<impl Future<Output = ()> + Send + 'static, String> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| "cannot install interrupt shutdown handler".to_owned())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| "cannot install termination shutdown handler".to_owned())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn run<N, S, F>(
    listener: TcpListener,
    runner: &crate::config::RunnerConfig,
    token: Vec<u8>,
    notion: N,
    coordinator: Arc<RevisionCoordinator<S>>,
    dispatcher: NotionEventDispatcher<N, S>,
    notion_config: &NotionConfig,
    task_values: &TaskValues,
    shutdown: F,
) -> Result<(), String>
where
    N: NotionAdapter + Clone,
    S: PreparationSink,
    F: Future<Output = ()> + Send + 'static,
{
    tokio::pin!(shutdown);
    {
        let startup = reconcile_once(&notion, &coordinator, notion_config, task_values);
        tokio::pin!(startup);
        tokio::select! {
            result = &mut startup => {
                result.map_err(|_| "serve startup reconciliation failed".to_owned())?;
            }
            _ = &mut shutdown => {
                startup
                    .await
                    .map_err(|_| "serve startup reconciliation failed".to_owned())?;
                return Ok(());
            }
        }
    }

    let interval = Duration::from_secs(runner.reconciliation_interval_seconds);
    let dispatcher_drain = dispatcher.clone();
    let (stop, http_stop) = watch::channel(false);
    let scheduler_stop = stop.subscribe();
    let mut http = Box::pin(http::run(listener, runner, token, dispatcher, async move {
        let mut stop = http_stop;
        let _ = stop.changed().await;
    }));
    let mut scheduler = Box::pin(periodic_reconciliation(
        notion,
        coordinator,
        notion_config.clone(),
        task_values.clone(),
        interval,
        scheduler_stop,
    ));
    let result = tokio::select! {
        _ = &mut shutdown => {
            let _ = stop.send(true);
            let (http_result, ()) = tokio::join!(&mut http, &mut scheduler);
            http_result
        }
        result = &mut http => {
            let _ = stop.send(true);
            scheduler.await;
            result
        }
        () = &mut scheduler => {
            let _ = stop.send(true);
            let _ = http.await;
            Err("reconciliation scheduler stopped unexpectedly".to_owned())
        }
    };
    dispatcher_drain.wait_for_idle().await;
    result
}

async fn periodic_reconciliation<N, S>(
    notion: N,
    coordinator: Arc<RevisionCoordinator<S>>,
    notion_config: NotionConfig,
    task_values: TaskValues,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) where
    N: NotionAdapter,
    S: PreparationSink,
{
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticks.tick().await;
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return;
            }
            _ = ticks.tick() => {
                if reconcile_once(&notion, &coordinator, &notion_config, &task_values)
                    .await
                    .is_err()
                {
                    eprintln!("periodic reconciliation failed; will retry at the configured interval");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RunnerConfig;
    use crate::discovery::DiscoveredTask;
    use crate::notion::{PendingTaskPage, TaskRevision, TaskState};
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    use std::collections::{HashMap, VecDeque};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{Semaphore, oneshot};

    const TOKEN: &[u8] = b"verification-secret-placeholder";

    struct QueryStep {
        result: Result<PendingTaskPage, String>,
        started: Option<Arc<Semaphore>>,
        release: Option<Arc<Semaphore>>,
    }

    #[derive(Clone)]
    struct FakeNotion {
        queries: Arc<StdMutex<VecDeque<QueryStep>>>,
        current: Arc<StdMutex<HashMap<String, TaskState>>>,
        query_calls: Arc<AtomicUsize>,
    }

    impl FakeNotion {
        fn new(steps: Vec<QueryStep>, tasks: Vec<TaskState>) -> Self {
            Self {
                queries: Arc::new(StdMutex::new(steps.into())),
                current: Arc::new(StdMutex::new(
                    tasks
                        .into_iter()
                        .map(|task| (task.page_id.clone(), task))
                        .collect(),
                )),
                query_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl NotionAdapter for FakeNotion {
        fn query_pending_tasks<'a>(
            &'a self,
            _cursor: Option<&'a str>,
            _pending_status: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<PendingTaskPage, String>> + Send + 'a>> {
            Box::pin(async move {
                self.query_calls.fetch_add(1, Ordering::SeqCst);
                let step = self
                    .queries
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(QueryStep {
                        result: Ok(page(Vec::new())),
                        started: None,
                        release: None,
                    });
                if let Some(started) = step.started {
                    started.add_permits(1);
                }
                if let Some(release) = step.release {
                    release.acquire().await.unwrap().forget();
                }
                step.result
            })
        }

        fn refetch_task<'a>(
            &'a self,
            page_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>> {
            Box::pin(async move {
                self.current
                    .lock()
                    .unwrap()
                    .get(page_id)
                    .cloned()
                    .ok_or_else(|| "cannot refetch task from Notion".to_owned())
            })
        }

        fn render_task<'a>(
            &'a self,
            page_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
            Box::pin(async move { Ok(format!("instructions-{page_id}")) })
        }
    }

    struct RecordingPreparation {
        active: AtomicUsize,
        peak: AtomicUsize,
        pages: StdMutex<Vec<String>>,
        block_call: AtomicUsize,
        started: Semaphore,
        release: Semaphore,
    }

    impl Default for RecordingPreparation {
        fn default() -> Self {
            Self {
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                pages: StdMutex::new(Vec::new()),
                block_call: AtomicUsize::new(0),
                started: Semaphore::new(0),
                release: Semaphore::new(0),
            }
        }
    }

    impl PreparationSink for RecordingPreparation {
        fn prepare<'a>(
            &'a self,
            task: DiscoveredTask,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                let call = self.pages.lock().unwrap().len() + 1;
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                if self.block_call.load(Ordering::SeqCst) == call {
                    self.started.add_permits(1);
                    self.release.acquire().await.unwrap().forget();
                }
                self.pages.lock().unwrap().push(task.state.page_id);
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn task(id: &str, revision: &str) -> TaskState {
        TaskState {
            page_id: id.to_owned(),
            revision: TaskRevision::parse(revision).unwrap(),
            data_source_id: Some("task-source-placeholder".to_owned()),
            status: Some("Pending".to_owned()),
            in_trash: false,
        }
    }

    fn page(tasks: Vec<TaskState>) -> PendingTaskPage {
        PendingTaskPage {
            tasks,
            next_cursor: None,
            response_bytes: 100,
        }
    }

    fn step(result: Result<PendingTaskPage, String>) -> QueryStep {
        QueryStep {
            result,
            started: None,
            release: None,
        }
    }

    fn configs(interval: u64) -> (RunnerConfig, NotionConfig, TaskValues) {
        (
            RunnerConfig {
                reconciliation_interval_seconds: interval,
                bind_address: "127.0.0.1:0".to_owned(),
                webhook_path: "/notion/webhook".to_owned(),
                health_path: "/health".to_owned(),
            },
            NotionConfig {
                token: "secret-placeholder".to_owned(),
                task_data_source_id: "task-source-placeholder".to_owned(),
                journal_data_source_id: "journal-source-placeholder".to_owned(),
            },
            TaskValues {
                pending: "Pending".to_owned(),
                running: "Running".to_owned(),
                error: "Error".to_owned(),
                done: "Done".to_owned(),
            },
        )
    }

    async fn exchange(address: std::net::SocketAddr, request: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    fn signed_request(body: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(TOKEN).unwrap();
        mac.update(body);
        let signature = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut request = format!(
            "POST /notion/webhook HTTP/1.1\r\nHost: localhost\r\nX-Notion-Signature: sha256={signature}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(body);
        request
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("expected {expected} reconciliation calls");
    }

    #[tokio::test]
    async fn startup_reconciliation_prepares_a_missed_task_before_http_intake_starts() {
        let missed = task("missed-page", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(vec![step(Ok(page(vec![missed.clone()])))], vec![missed]);
        let sink = RecordingPreparation::default();
        sink.block_call.store(1, Ordering::SeqCst);
        let coordinator = Arc::new(RevisionCoordinator::new(sink));
        let retained = Arc::clone(&coordinator.sink);
        let (runner, notion_config, values) = configs(60);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });

        // The startup handoff is active, but the HTTP accept loop is not yet running.
        let coordinator_probe = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect(address).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(coordinator_probe.is_ok());
        // A bound socket may connect before accept; no response is served yet.
        let health = b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        assert!(
            tokio::time::timeout(Duration::from_millis(50), exchange(address, health))
                .await
                .is_err()
        );

        retained.release.add_permits(1);
        let response = exchange(address, health).await;
        assert!(response.starts_with("HTTP/1.1 200"));
        assert_eq!(retained.pages.lock().unwrap().as_slice(), ["missed-page"]);
        shutdown.send(()).unwrap();
        assert!(task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn startup_failure_is_actionable_and_does_not_expose_remote_content() {
        let notion = FakeNotion::new(
            vec![step(Err("private remote detail".to_owned()))],
            Vec::new(),
        );
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let (runner, notion_config, values) = configs(60);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert_eq!(
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                std::future::pending::<()>(),
            )
            .await
            .unwrap_err(),
            "serve startup reconciliation failed"
        );
    }

    #[tokio::test]
    async fn shutdown_during_startup_waits_for_preparation_and_skips_http_intake() {
        let candidate = task("page-a", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(
            vec![step(Ok(page(vec![candidate.clone()])))],
            vec![candidate],
        );
        let sink = RecordingPreparation::default();
        sink.block_call.store(1, Ordering::SeqCst);
        let coordinator = Arc::new(RevisionCoordinator::new(sink));
        let retained = Arc::clone(&coordinator.sink);
        let (runner, notion_config, values) = configs(60);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let served = tokio::spawn(async move {
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        retained.started.acquire().await.unwrap().forget();
        shutdown.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(!served.is_finished());
        retained.release.add_permits(1);
        assert!(served.await.unwrap().is_ok());
        assert_eq!(retained.pages.lock().unwrap().as_slice(), ["page-a"]);
    }

    #[tokio::test(start_paused = true)]
    async fn configured_ticks_continue_after_a_content_free_cycle_failure() {
        let recovered = task("missed-page", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(
            vec![
                step(Ok(page(Vec::new()))),
                step(Err("private remote detail".to_owned())),
                step(Ok(page(vec![recovered.clone()]))),
            ],
            vec![recovered],
        );
        let calls = Arc::clone(&notion.query_calls);
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let (runner, notion_config, values) = configs(10);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let served = tokio::spawn(async move {
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        wait_for_calls(&calls, 1).await;
        assert!(
            exchange(
                address,
                b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            .await
            .starts_with("HTTP/1.1 200")
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        wait_for_calls(&calls, 2).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        wait_for_calls(&calls, 3).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        shutdown.send(()).unwrap();
        assert!(served.await.unwrap().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn webhook_intake_remains_active_during_periodic_reconciliation() {
        let candidate = task("page-a", "2026-01-01T00:00:00Z");
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let notion = FakeNotion::new(
            vec![
                step(Ok(page(Vec::new()))),
                QueryStep {
                    result: Ok(page(vec![candidate.clone()])),
                    started: Some(Arc::clone(&started)),
                    release: Some(Arc::clone(&release)),
                },
            ],
            vec![candidate],
        );
        let calls = Arc::clone(&notion.query_calls);
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let sink = Arc::clone(&coordinator.sink);
        let (runner, notion_config, values) = configs(10);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let served = tokio::spawn(async move {
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        wait_for_calls(&calls, 1).await;
        assert!(
            exchange(
                address,
                b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            .await
            .starts_with("HTTP/1.1 200")
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        started.acquire().await.unwrap().forget();

        let body = br#"{"id":"event-placeholder","timestamp":"2026-01-01T00:00:00Z","type":"page.properties_updated","entity":{"id":"page-a","type":"page"},"data":{"parent":{"type":"database","data_source_id":"task-source-placeholder"}}}"#;
        let response = exchange(address, &signed_request(body)).await;
        assert!(response.starts_with("HTTP/1.1 200"));
        assert_eq!(sink.pages.lock().unwrap().as_slice(), ["page-a"]);
        release.add_permits(1);
        tokio::task::yield_now().await;
        assert_eq!(sink.pages.lock().unwrap().as_slice(), ["page-a"]);
        assert_eq!(sink.peak.load(Ordering::SeqCst), 1);
        shutdown.send(()).unwrap();
        assert!(served.await.unwrap().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn clean_shutdown_waits_for_active_preparation_to_finish() {
        let candidate = task("page-a", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(
            vec![
                step(Ok(page(Vec::new()))),
                step(Ok(page(vec![candidate.clone()]))),
            ],
            vec![candidate],
        );
        let calls = Arc::clone(&notion.query_calls);
        let sink = RecordingPreparation::default();
        sink.block_call.store(1, Ordering::SeqCst);
        let coordinator = Arc::new(RevisionCoordinator::new(sink));
        let retained = Arc::clone(&coordinator.sink);
        let (runner, notion_config, values) = configs(10);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let served = tokio::spawn(async move {
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        wait_for_calls(&calls, 1).await;
        assert!(
            exchange(
                address,
                b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
            )
            .await
            .starts_with("HTTP/1.1 200")
        );
        tokio::time::advance(Duration::from_secs(10)).await;
        retained.started.acquire().await.unwrap().forget();
        shutdown.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(!served.is_finished());
        retained.release.add_permits(1);
        assert!(served.await.unwrap().is_ok());
        assert_eq!(retained.pages.lock().unwrap().as_slice(), ["page-a"]);
    }

    #[tokio::test]
    async fn clean_shutdown_drains_webhook_preparation_after_http_intake_stops() {
        let candidate = task("page-a", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(vec![step(Ok(page(Vec::new())))], vec![candidate]);
        let sink = RecordingPreparation::default();
        sink.block_call.store(1, Ordering::SeqCst);
        let coordinator = Arc::new(RevisionCoordinator::new(sink));
        let retained = Arc::clone(&coordinator.sink);
        let (runner, notion_config, values) = configs(60);
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &values,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel();
        let served = tokio::spawn(async move {
            run(
                listener,
                &runner,
                TOKEN.to_vec(),
                notion,
                coordinator,
                dispatcher,
                &notion_config,
                &values,
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        let body = br#"{"id":"event-placeholder","timestamp":"2026-01-01T00:00:00Z","type":"page.properties_updated","entity":{"id":"page-a","type":"page"},"data":{"parent":{"type":"database","data_source_id":"task-source-placeholder"}}}"#;
        let request = signed_request(body);
        let intake = tokio::spawn(async move { exchange(address, &request).await });
        retained.started.acquire().await.unwrap().forget();
        shutdown.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(!served.is_finished());
        retained.release.add_permits(1);
        assert!(served.await.unwrap().is_ok());
        let _ = intake.await.unwrap();
        assert_eq!(retained.pages.lock().unwrap().as_slice(), ["page-a"]);
    }
}
