use crate::config::TaskValues;
use crate::coordination::PreparationSink;
use crate::discovery::DiscoveredTask;
use crate::executor::{Executor, ExecutorRequest};
use crate::notion::{InitialJournalAttempt, NotionAdapter};
use crate::state::{LockedAttemptStore, PreparedAttempt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) struct ExecutionWorkflow<N, E> {
    store: Arc<LockedAttemptStore>,
    notion: N,
    executor: Arc<E>,
    task_values: TaskValues,
    executor_name: String,
}

impl<N, E> ExecutionWorkflow<N, E> {
    pub(crate) fn new(
        store: Arc<LockedAttemptStore>,
        notion: N,
        executor: E,
        task_values: TaskValues,
        executor_name: String,
    ) -> Self {
        Self {
            store,
            notion,
            executor: Arc::new(executor),
            task_values,
            executor_name,
        }
    }
}

impl<N, E> PreparationSink for ExecutionWorkflow<N, E>
where
    N: NotionAdapter,
    E: Executor + 'static,
{
    fn prepare<'a>(
        &'a self,
        task: DiscoveredTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            if self.store.task_revision_has_launch_intent(
                &task.state.page_id,
                task.state.revision.as_str(),
            )? {
                return Ok(());
            }
            let started_at = OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|_| "cannot prepare attempt timestamp".to_owned())?;
            let ready = prepare_visible_attempt_for_revision(
                &self.store,
                &self.notion,
                &task.state.page_id,
                &self.task_values.running,
                &self.executor_name,
                &started_at,
                task.state.revision.as_str(),
            )
            .await?;
            self.store.record_launch_intent(ready.run_id())?;
            let run_id = ready.run_id().to_owned();
            let executor = Arc::clone(&self.executor);
            let result = tokio::task::spawn_blocking(move || {
                executor.execute(ExecutorRequest {
                    instructions: task.instructions,
                })
            })
            .await
            .map_err(|_| "executor task stopped unexpectedly".to_owned())?
            .map_err(|_| "executor action failed; automatic retry is disabled".to_owned())?;
            let outcome = result.outcome.clone();
            self.store.store_result(&run_id, result)?;
            match outcome {
                crate::executor::Outcome::Done => Ok(()),
                crate::executor::Outcome::Error => Err(
                    "executor reported incomplete or blocked work; automatic retry is disabled"
                        .to_owned(),
                ),
            }
        })
    }
}

#[derive(Debug)]
pub(crate) struct LaunchReadyAttempt {
    prepared: PreparedAttempt,
}

impl LaunchReadyAttempt {
    pub(crate) fn run_id(&self) -> &str {
        self.prepared.run_id()
    }
}

#[cfg(test)]
pub(crate) async fn prepare_visible_attempt<N: NotionAdapter>(
    store: &LockedAttemptStore,
    notion: &N,
    task_page_id: &str,
    running_status: &str,
    executor: &str,
    started_at: &str,
) -> Result<LaunchReadyAttempt, String> {
    prepare_visible_attempt_inner(
        store,
        notion,
        task_page_id,
        running_status,
        executor,
        started_at,
        None,
    )
    .await
}

async fn prepare_visible_attempt_for_revision<N: NotionAdapter>(
    store: &LockedAttemptStore,
    notion: &N,
    task_page_id: &str,
    running_status: &str,
    executor: &str,
    started_at: &str,
    task_revision: &str,
) -> Result<LaunchReadyAttempt, String> {
    prepare_visible_attempt_inner(
        store,
        notion,
        task_page_id,
        running_status,
        executor,
        started_at,
        Some(task_revision),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_visible_attempt_inner<N: NotionAdapter>(
    store: &LockedAttemptStore,
    notion: &N,
    task_page_id: &str,
    running_status: &str,
    executor: &str,
    started_at: &str,
    task_revision: Option<&str>,
) -> Result<LaunchReadyAttempt, String> {
    let prepared = match task_revision {
        Some(revision) => store.prepare_revision(task_page_id, revision)?,
        None => store.prepare(task_page_id)?,
    };
    notion
        .update_task_status(task_page_id, running_status)
        .await?;
    let visible_task = notion.refetch_task(task_page_id).await?;
    if visible_task.status.as_deref() != Some(running_status) {
        return Err("Notion task status is not visibly Running".to_owned());
    }

    let journal = InitialJournalAttempt {
        run_id: prepared.run_id().to_owned(),
        task_page_id: task_page_id.to_owned(),
        executor: executor.to_owned(),
        started_at: started_at.to_owned(),
    };
    let create_result = notion.create_initial_journal(&journal).await;
    let visible_journal = notion.find_journal_by_run_id(prepared.run_id()).await?;
    match visible_journal {
        Some(record) if record == journal => Ok(LaunchReadyAttempt { prepared }),
        Some(_) => Err("Notion journal readback does not match the prepared attempt".to_owned()),
        None => Err(create_result
            .err()
            .unwrap_or_else(|| "Notion journal attempt is not visible".to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NotionConfig;
    use crate::discovery::NotionEventDispatcher;
    use crate::executor::{ExecutorResult, Outcome};
    use crate::http::EventDispatcher;
    use crate::notion::{PendingTaskPage, TaskRevision, TaskState};
    use crate::reconciliation::reconcile_once;
    use crate::state::AttemptStore;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone)]
    struct FakeNotion {
        state: Arc<Mutex<FakeState>>,
    }

    struct FakeState {
        visible_status: Option<String>,
        journal: Option<InitialJournalAttempt>,
        create_error: Option<String>,
        hide_journal: bool,
        hide_status: bool,
        creates: usize,
    }

    #[derive(Clone)]
    struct FakeExecutor {
        calls: Arc<Mutex<Vec<String>>>,
        notion: FakeNotion,
        fail: bool,
        outcome: Outcome,
    }

    impl Executor for FakeExecutor {
        fn execute(&self, request: ExecutorRequest) -> Result<ExecutorResult, String> {
            let state = self.notion.state.lock().unwrap();
            assert_eq!(state.visible_status.as_deref(), Some("Running"));
            assert!(state.journal.is_some());
            drop(state);
            self.calls.lock().unwrap().push(request.instructions);
            if self.fail {
                return Err("private executor output".to_owned());
            }
            Ok(ExecutorResult {
                outcome: self.outcome.clone(),
                summary: "completed".to_owned(),
                actions: vec!["acted".to_owned()],
                warnings: Vec::new(),
            })
        }
    }

    impl FakeNotion {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeState {
                    visible_status: None,
                    journal: None,
                    create_error: None,
                    hide_journal: false,
                    hide_status: false,
                    creates: 0,
                })),
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
                Ok(PendingTaskPage {
                    tasks: vec![TaskState {
                        page_id: "task-placeholder".to_owned(),
                        revision: TaskRevision::parse("2026-01-01T00:00:00Z").unwrap(),
                        data_source_id: Some("source-placeholder".to_owned()),
                        status: self.state.lock().unwrap().visible_status.clone(),
                        in_trash: false,
                    }],
                    next_cursor: None,
                    response_bytes: 100,
                })
            })
        }

        fn refetch_task<'a>(
            &'a self,
            page_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>> {
            Box::pin(async move {
                Ok(TaskState {
                    page_id: page_id.to_owned(),
                    revision: TaskRevision::parse("2026-01-01T00:00:00Z").unwrap(),
                    data_source_id: Some("source-placeholder".to_owned()),
                    status: self.state.lock().unwrap().visible_status.clone(),
                    in_trash: false,
                })
            })
        }
        fn render_task<'a>(
            &'a self,
            _page_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
            Box::pin(async move { Ok(format!("rendered-{_page_id}")) })
        }
        fn update_task_status<'a>(
            &'a self,
            _page_id: &'a str,
            status: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                let mut state = self.state.lock().unwrap();
                if !state.hide_status {
                    state.visible_status = Some(status.to_owned());
                }
                Ok(())
            })
        }
        fn create_initial_journal<'a>(
            &'a self,
            attempt: &'a InitialJournalAttempt,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                let mut state = self.state.lock().unwrap();
                state.creates += 1;
                state.journal = Some(attempt.clone());
                state.create_error.clone().map_or(Ok(()), Err)
            })
        }
        fn find_journal_by_run_id<'a>(
            &'a self,
            run_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<InitialJournalAttempt>, String>> + Send + 'a>>
        {
            Box::pin(async move {
                let state = self.state.lock().unwrap();
                Ok((!state.hide_journal)
                    .then(|| state.journal.clone())
                    .flatten()
                    .filter(|entry| entry.run_id == run_id))
            })
        }
    }

    fn directory() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "agent-handover-orchestration-test-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn discovered_at(instructions: &str, revision: &str) -> DiscoveredTask {
        DiscoveredTask {
            state: TaskState {
                page_id: "task-placeholder".to_owned(),
                revision: TaskRevision::parse(revision).unwrap(),
                data_source_id: Some("source-placeholder".to_owned()),
                status: Some("Pending".to_owned()),
                in_trash: false,
            },
            instructions: instructions.to_owned(),
        }
    }

    fn discovered(instructions: &str) -> DiscoveredTask {
        discovered_at(instructions, "2026-01-01T00:00:00Z")
    }

    fn values() -> TaskValues {
        TaskValues {
            pending: "Pending".to_owned(),
            running: "Running".to_owned(),
            error: "Error".to_owned(),
            done: "Done".to_owned(),
        }
    }

    fn notion_config() -> NotionConfig {
        NotionConfig {
            token: "secret-placeholder".to_owned(),
            task_data_source_id: "source-placeholder".to_owned(),
            journal_data_source_id: "journal-placeholder".to_owned(),
        }
    }

    #[tokio::test]
    async fn reconciliation_discovery_reaches_the_executor_workflow() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().visible_status = Some("Pending".to_owned());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            FakeExecutor {
                calls: Arc::clone(&calls),
                notion: notion.clone(),
                fail: false,
                outcome: Outcome::Done,
            },
            values(),
            "Codex".to_owned(),
        );
        let coordinator = crate::coordination::RevisionCoordinator::new(workflow);

        assert_eq!(
            reconcile_once(&notion, &coordinator, &notion_config(), &values())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["rendered-task-placeholder"]
        );
        drop(coordinator);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn webhook_discovery_reaches_the_executor_workflow() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().visible_status = Some("Pending".to_owned());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let coordinator = Arc::new(crate::coordination::RevisionCoordinator::new(
            ExecutionWorkflow::new(
                Arc::clone(&store),
                notion.clone(),
                FakeExecutor {
                    calls: Arc::clone(&calls),
                    notion: notion.clone(),
                    fail: false,
                    outcome: Outcome::Done,
                },
                values(),
                "Codex".to_owned(),
            ),
        ));
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion,
            Arc::clone(&coordinator),
            &notion_config(),
            &values(),
        );
        let event = serde_json::json!({
            "id": "event-placeholder",
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "page.properties_updated",
            "entity": { "id": "task-placeholder", "type": "page" },
            "data": { "parent": {
                "type": "database",
                "data_source_id": "source-placeholder"
            }}
        });

        dispatcher.dispatch(event).await.unwrap();

        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["rendered-task-placeholder"]
        );
        drop(dispatcher);
        drop(coordinator);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn workflow_launches_after_visibility_and_durably_stores_the_result() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            FakeExecutor {
                calls: Arc::clone(&calls),
                notion,
                fail: false,
                outcome: Outcome::Done,
            },
            values(),
            "Codex".to_owned(),
        );

        workflow
            .prepare(discovered("sole task body"))
            .await
            .unwrap();

        assert_eq!(calls.lock().unwrap().as_slice(), ["sole task body"]);
        let attempts = store.list_prepared().unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].result().unwrap().summary, "completed");
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn workflow_durably_stores_a_valid_error_outcome_before_reporting_failure() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            FakeExecutor {
                calls: Arc::new(Mutex::new(Vec::new())),
                notion,
                fail: false,
                outcome: Outcome::Error,
            },
            values(),
            "Codex".to_owned(),
        );

        assert_eq!(
            workflow.prepare(discovered("task body")).await.unwrap_err(),
            "executor reported incomplete or blocked work; automatic retry is disabled"
        );
        let attempts = store.list_prepared().unwrap();
        assert_eq!(attempts[0].result().unwrap().outcome, Outcome::Error);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn workflow_never_relaunches_after_a_failed_executor_action() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            FakeExecutor {
                calls: Arc::clone(&calls),
                notion,
                fail: true,
                outcome: Outcome::Done,
            },
            values(),
            "Codex".to_owned(),
        );

        assert_eq!(
            workflow
                .prepare(discovered("private instructions"))
                .await
                .unwrap_err(),
            "executor action failed; automatic retry is disabled"
        );
        workflow
            .prepare(discovered("private instructions"))
            .await
            .unwrap();

        assert_eq!(calls.lock().unwrap().len(), 1);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn a_newer_pending_revision_can_start_a_manual_retry() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            FakeExecutor {
                calls: Arc::clone(&calls),
                notion,
                fail: true,
                outcome: Outcome::Done,
            },
            values(),
            "Codex".to_owned(),
        );

        assert!(workflow.prepare(discovered("first")).await.is_err());
        assert!(
            workflow
                .prepare(discovered_at("retry", "2026-01-02T00:00:00Z"))
                .await
                .is_err()
        );

        assert_eq!(calls.lock().unwrap().as_slice(), ["first", "retry"]);
        assert_eq!(store.list_prepared().unwrap().len(), 2);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn exposes_an_attempt_only_after_running_and_its_journal_are_visible() {
        let journal_directory = directory();
        let locked = AttemptStore::new(journal_directory.clone())
            .acquire()
            .unwrap();
        let notion = FakeNotion::new();
        let ready = prepare_visible_attempt(
            &locked,
            &notion,
            "task-placeholder",
            "Running",
            "Codex",
            "2026-01-01T00:00:00Z",
        )
        .await
        .unwrap();
        assert!(locked.load(ready.run_id()).is_ok());
        assert_eq!(locked.list_prepared().unwrap().len(), 1);
        let state = notion.state.lock().unwrap();
        assert_eq!(state.creates, 1);
        assert_eq!(state.journal.as_ref().unwrap().run_id, ready.run_id());
        drop(state);
        drop(locked);
        std::fs::remove_dir_all(journal_directory).unwrap();
    }

    #[tokio::test]
    async fn resolves_an_ambiguous_create_by_run_id_without_creating_again() {
        let status_directory = directory();
        let locked = AttemptStore::new(status_directory.clone())
            .acquire()
            .unwrap();
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().create_error = Some("ambiguous response".to_owned());
        assert!(
            prepare_visible_attempt(
                &locked,
                &notion,
                "task-placeholder",
                "Running",
                "Codex",
                "2026-01-01T00:00:00Z"
            )
            .await
            .is_ok()
        );
        assert_eq!(notion.state.lock().unwrap().creates, 1);
        drop(locked);
        std::fs::remove_dir_all(status_directory).unwrap();
    }

    #[tokio::test]
    async fn refuses_launch_readiness_when_status_or_journal_is_not_visible() {
        let journal_directory = directory();
        let locked = AttemptStore::new(journal_directory.clone())
            .acquire()
            .unwrap();
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().hide_journal = true;
        assert_eq!(
            prepare_visible_attempt(
                &locked,
                &notion,
                "task-placeholder",
                "Running",
                "Codex",
                "2026-01-01T00:00:00Z"
            )
            .await
            .unwrap_err(),
            "Notion journal attempt is not visible"
        );
        drop(locked);
        std::fs::remove_dir_all(journal_directory).unwrap();

        let status_directory = directory();
        let locked = AttemptStore::new(status_directory.clone())
            .acquire()
            .unwrap();
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().hide_status = true;
        assert_eq!(
            prepare_visible_attempt(
                &locked,
                &notion,
                "task-placeholder",
                "Running",
                "Codex",
                "2026-01-01T00:00:00Z"
            )
            .await
            .unwrap_err(),
            "Notion task status is not visibly Running"
        );
        assert_eq!(notion.state.lock().unwrap().creates, 0);
        drop(locked);
        std::fs::remove_dir_all(status_directory).unwrap();
    }
}
