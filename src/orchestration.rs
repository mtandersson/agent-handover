use crate::config::TaskValues;
use crate::coordination::PreparationSink;
use crate::discovery::DiscoveredTask;
use crate::executor::{Executor, ExecutorRequest, ExecutorResult, Outcome};
use crate::notion::{FinalJournalAttempt, InitialJournalAttempt, NotionAdapter};
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

impl<N, E> ExecutionWorkflow<N, E>
where
    N: NotionAdapter,
    E: Executor + 'static,
{
    async fn launch(
        &self,
        task: DiscoveredTask,
        prepared: Option<PreparedAttempt>,
    ) -> Result<(), String> {
        crate::logging::task_attempt_started();
        let result = self.launch_inner(task, prepared).await;
        crate::logging::task_attempt_finished(&result);
        result
    }

    async fn launch_inner(
        &self,
        task: DiscoveredTask,
        prepared: Option<PreparedAttempt>,
    ) -> Result<(), String> {
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
            prepared,
        )
        .await?;
        tracing::info!("task attempt prepared");
        self.store.record_launch_intent(ready.run_id())?;
        let run_id = ready.run_id().to_owned();
        let executor = Arc::clone(&self.executor);
        tracing::info!("task executor started");
        let execution = tokio::task::spawn_blocking(move || {
            executor.execute(ExecutorRequest {
                instructions: task.instructions,
            })
        })
        .await;
        // Once launch_intent is durable every executor failure must cross the
        // same durable-result boundary as a normal agent Error. Otherwise the
        // task stays Running until a runner restart, with an unfinished journal.
        // Never record arbitrary executor error text, which may contain secrets.
        let (result, executor_failed) = match execution {
            Ok(Ok(result)) => (result, false),
            Ok(Err(_)) | Err(_) => {
                tracing::warn!(outcome = "failed", "task executor finished");
                (
                    ExecutorResult {
                        outcome: Outcome::Error,
                        summary: "executor failed before returning a valid result".to_owned(),
                        actions: Vec::new(),
                        warnings: vec![
                            "executor failure; partial external effects may have occurred"
                                .to_owned(),
                        ],
                    },
                    true,
                )
            }
        };
        let outcome = result.outcome.clone();
        match outcome {
            Outcome::Done => tracing::info!(outcome = "done", "task executor finished"),
            Outcome::Error => tracing::info!(outcome = "error", "task executor finished"),
        }
        let completed_at = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|_| "cannot prepare attempt timestamp".to_owned())?;
        self.store.store_result(&run_id, &completed_at, result)?;
        self.finalize(self.store.load(&run_id)?).await?;
        match (outcome, executor_failed) {
            (Outcome::Done, _) => Ok(()),
            (Outcome::Error, true) => {
                Err("executor action failed; terminal Error recorded".to_owned())
            }
            (Outcome::Error, false) => Err(
                "executor reported incomplete or blocked work; automatic retry is disabled"
                    .to_owned(),
            ),
        }
    }

    async fn finalize(&self, attempt: PreparedAttempt) -> Result<(), String> {
        let result = attempt
            .result()
            .cloned()
            .ok_or_else(|| "attempt result is unavailable".to_owned())?;
        let initial = self
            .notion
            .find_journal_by_run_id(attempt.run_id())
            .await?
            .ok_or_else(|| "Notion journal attempt is not visible".to_owned())?;
        if initial.run_id != attempt.run_id()
            || initial.task_page_id != attempt.task_key()
            || initial.executor != self.executor_name
        {
            return Err("Notion journal readback does not match the durable attempt".to_owned());
        }
        let final_attempt = FinalJournalAttempt {
            initial,
            ended_at: attempt
                .completed_at()
                .ok_or_else(|| "attempt completion time is unavailable".to_owned())?
                .to_owned(),
            result,
        };
        let write = self.notion.finalize_journal(&final_attempt).await;
        match self
            .notion
            .find_final_journal_by_run_id(attempt.run_id())
            .await?
        {
            Some(visible) if visible == final_attempt => {}
            Some(_) => {
                return Err(
                    "Notion journal finalization does not match the durable result".to_owned(),
                );
            }
            None => {
                return Err(write
                    .err()
                    .unwrap_or_else(|| "Notion journal finalization is not visible".to_owned()));
            }
        }
        let status = match final_attempt.result.outcome {
            crate::executor::Outcome::Done => &self.task_values.done,
            crate::executor::Outcome::Error => &self.task_values.error,
        };
        let status_write = self
            .notion
            .update_task_status(attempt.task_key(), status)
            .await;
        let task = self.notion.refetch_task(attempt.task_key()).await?;
        if task.page_id != attempt.task_key()
            || task.in_trash
            || task.status.as_deref() != Some(status)
        {
            return Err(status_write
                .err()
                .unwrap_or_else(|| "Notion terminal task status is not visible".to_owned()));
        }
        self.store.mark_finalized(attempt.run_id())
    }

    async fn recover_interrupted_launch(&self, attempt: PreparedAttempt) -> Result<(), String> {
        tracing::warn!("interrupted task attempt recovery started");
        let result = self.recover_interrupted_launch_inner(attempt).await;
        if result.is_ok() {
            tracing::info!(
                outcome = "completed",
                "interrupted task attempt recovery finished"
            );
        } else {
            tracing::warn!(
                outcome = "failed",
                "interrupted task attempt recovery finished"
            );
        }
        result
    }

    async fn recover_interrupted_launch_inner(
        &self,
        attempt: PreparedAttempt,
    ) -> Result<(), String> {
        let completed_at = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|_| "cannot prepare attempt timestamp".to_owned())?;
        self.store.store_result(
            attempt.run_id(),
            &completed_at,
            ExecutorResult {
                outcome: Outcome::Error,
                summary: "runner restarted after launch intent; outcome is unknown".to_owned(),
                actions: Vec::new(),
                warnings: vec!["outcome unknown; external effects may have occurred".to_owned()],
            },
        )?;
        self.finalize(self.store.load(attempt.run_id())?).await
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
            self.launch(task, None).await
        })
    }

    fn recover<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<usize, String>> + Send + 'a>> {
        Box::pin(async move {
            let results = self.store.result_stored()?;
            let finalized = results.len();
            for attempt in results {
                self.finalize(attempt).await?;
            }
            let interrupted = self.store.launch_intents()?;
            let recovered_interrupted = interrupted.len();
            for attempt in interrupted {
                self.recover_interrupted_launch(attempt).await?;
            }
            let prepared = self.store.prepared_before_launch()?;
            let mut recovered = finalized + recovered_interrupted;
            for attempt in prepared {
                let revision = attempt
                    .task_revision()
                    .ok_or_else(|| "prepared attempt is missing its task revision".to_owned())?;
                let state = self.notion.refetch_task(attempt.task_key()).await?;
                if state.page_id != attempt.task_key()
                    || !matches!(
                        state.status.as_deref(),
                        Some(status)
                            if status == self.task_values.pending
                                || status == self.task_values.running
                    )
                {
                    return Err(
                        "prepared attempt cannot be recovered from current task state".to_owned(),
                    );
                }
                let task = DiscoveredTask {
                    state: crate::notion::TaskState {
                        revision: crate::notion::TaskRevision::parse(revision)?,
                        ..state
                    },
                    instructions: self.notion.render_task(attempt.task_key()).await?,
                };
                self.launch(task, Some(attempt)).await?;
                recovered += 1;
            }
            Ok(recovered)
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
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_visible_attempt_for_revision<N: NotionAdapter>(
    store: &LockedAttemptStore,
    notion: &N,
    task_page_id: &str,
    running_status: &str,
    executor: &str,
    started_at: &str,
    task_revision: &str,
    existing: Option<PreparedAttempt>,
) -> Result<LaunchReadyAttempt, String> {
    prepare_visible_attempt_inner(
        store,
        notion,
        task_page_id,
        running_status,
        executor,
        started_at,
        Some(task_revision),
        existing,
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
    existing: Option<PreparedAttempt>,
) -> Result<LaunchReadyAttempt, String> {
    let resuming = existing.is_some();
    let prepared = match (existing, task_revision) {
        (Some(prepared), _) => prepared,
        (None, Some(revision)) => store
            .prepared_for_revision(task_page_id, revision)?
            .map_or_else(|| store.prepare_revision(task_page_id, revision), Ok)?,
        (None, None) => store.prepare(task_page_id)?,
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
    let already_visible = notion.find_journal_by_run_id(prepared.run_id()).await?;
    if let Some(record) = already_visible {
        return if record == journal
            || (resuming
                && record.run_id == journal.run_id
                && record.task_page_id == journal.task_page_id
                && record.executor == journal.executor)
        {
            Ok(LaunchReadyAttempt { prepared })
        } else {
            Err("Notion journal readback does not match the prepared attempt".to_owned())
        };
    }
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
    use crate::notion::{FinalJournalAttempt, PendingTaskPage, TaskRevision, TaskState};
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
        revision: String,
        visible_status: Option<String>,
        journal: Option<InitialJournalAttempt>,
        final_journal: Option<FinalJournalAttempt>,
        create_error: Option<String>,
        hide_journal: bool,
        hide_status: bool,
        hide_terminal_status: bool,
        terminal_status_error: Option<String>,
        finalize_error: Option<String>,
        hide_final_journal: bool,
        terminal_status_writes: usize,
        finalizes: usize,
        wrong_terminal_page: bool,
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
                    revision: "2026-01-01T00:00:00Z".to_owned(),
                    visible_status: None,
                    journal: None,
                    final_journal: None,
                    create_error: None,
                    hide_journal: false,
                    hide_status: false,
                    hide_terminal_status: false,
                    terminal_status_error: None,
                    finalize_error: None,
                    hide_final_journal: false,
                    terminal_status_writes: 0,
                    finalizes: 0,
                    wrong_terminal_page: false,
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
                let state = self.state.lock().unwrap();
                Ok(PendingTaskPage {
                    tasks: vec![TaskState {
                        page_id: "task-placeholder".to_owned(),
                        revision: TaskRevision::parse(&state.revision).unwrap(),
                        data_source_id: Some("source-placeholder".to_owned()),
                        status: state.visible_status.clone(),
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
                let state = self.state.lock().unwrap();
                let terminal = matches!(state.visible_status.as_deref(), Some("Done" | "Error"));
                Ok(TaskState {
                    page_id: if terminal && state.wrong_terminal_page {
                        "different-task".to_owned()
                    } else {
                        page_id.to_owned()
                    },
                    revision: TaskRevision::parse(&state.revision).unwrap(),
                    data_source_id: Some("source-placeholder".to_owned()),
                    status: state.visible_status.clone(),
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
                if !state.hide_status
                    && !(state.hide_terminal_status && matches!(status, "Done" | "Error"))
                {
                    state.visible_status = Some(status.to_owned());
                }
                if matches!(status, "Done" | "Error") {
                    state.terminal_status_writes += 1;
                    state.terminal_status_error.clone().map_or(Ok(()), Err)
                } else {
                    Ok(())
                }
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
        fn finalize_journal<'a>(
            &'a self,
            attempt: &'a FinalJournalAttempt,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                let mut state = self.state.lock().unwrap();
                state.finalizes += 1;
                if !state.hide_final_journal {
                    state.final_journal = Some(attempt.clone());
                }
                state.finalize_error.clone().map_or(Ok(()), Err)
            })
        }
        fn find_final_journal_by_run_id<'a>(
            &'a self,
            run_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<FinalJournalAttempt>, String>> + Send + 'a>>
        {
            Box::pin(async move {
                let state = self.state.lock().unwrap();
                Ok((!state.hide_final_journal)
                    .then(|| state.final_journal.clone())
                    .flatten()
                    .filter(|attempt| attempt.initial.run_id == run_id))
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

    fn task_event(id: &str, timestamp: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "timestamp": timestamp,
            "type": "page.properties_updated",
            "entity": { "id": "task-placeholder", "type": "page" },
            "data": { "parent": {
                "type": "database",
                "data_source_id": "source-placeholder"
            }}
        })
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
                notion: notion.clone(),
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
        assert!(store.result_stored().unwrap().is_empty());
        let notion_state = notion.state.lock().unwrap();
        assert_eq!(notion_state.visible_status.as_deref(), Some("Done"));
        assert_eq!(
            notion_state.final_journal.as_ref().unwrap().result.outcome,
            Outcome::Done
        );
        drop(notion_state);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn startup_recovers_the_same_prepared_run_after_remote_visibility() {
        let state_directory = directory();
        let run_id = {
            let store = AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap();
            store
                .prepare_revision("task-placeholder", "2026-01-01T00:00:00Z")
                .unwrap()
                .run_id()
                .to_owned()
        };
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        {
            let mut state = notion.state.lock().unwrap();
            state.visible_status = Some("Running".to_owned());
            state.journal = Some(InitialJournalAttempt {
                run_id: run_id.clone(),
                task_page_id: "task-placeholder".to_owned(),
                executor: "Codex".to_owned(),
                started_at: "2026-01-01T00:00:00Z".to_owned(),
            });
        }
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

        assert_eq!(workflow.recover().await.unwrap(), 1);

        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["rendered-task-placeholder"]
        );
        assert_eq!(notion.state.lock().unwrap().creates, 0);
        let attempts = store.list_prepared().unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].run_id(), run_id);
        assert_eq!(attempts[0].result().unwrap().summary, "completed");
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn restart_before_executor_invocation_finalizes_launch_intent_as_unknown_error() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let attempt = store
            .prepare_revision("task-placeholder", "2026-01-01T00:00:00Z")
            .unwrap();
        store.record_launch_intent(attempt.run_id()).unwrap();
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().visible_status = Some("Running".to_owned());
        notion.state.lock().unwrap().journal = Some(InitialJournalAttempt {
            run_id: attempt.run_id().to_owned(),
            task_page_id: "task-placeholder".to_owned(),
            executor: "Codex".to_owned(),
            started_at: "2026-01-01T00:00:00Z".to_owned(),
        });
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

        assert_eq!(workflow.recover().await.unwrap(), 1);
        assert!(calls.lock().unwrap().is_empty());
        {
            let state = notion.state.lock().unwrap();
            let final_attempt = state.final_journal.as_ref().unwrap();
            assert_eq!(final_attempt.initial.run_id, attempt.run_id());
            assert_eq!(final_attempt.result.outcome, Outcome::Error);
            assert_eq!(
                final_attempt.result.warnings,
                ["outcome unknown; external effects may have occurred"]
            );
            assert_eq!(state.visible_status.as_deref(), Some("Error"));
        }
        assert_eq!(workflow.recover().await.unwrap(), 0);
        assert_eq!(notion.state.lock().unwrap().finalizes, 1);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn restart_after_executor_invocation_never_relaunches_the_interrupted_attempt() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let attempt = store
            .prepare_revision("task-placeholder", "2026-01-01T00:00:00Z")
            .unwrap();
        store.record_launch_intent(attempt.run_id()).unwrap();
        let notion = FakeNotion::new();
        {
            let mut state = notion.state.lock().unwrap();
            state.visible_status = Some("Running".to_owned());
            state.journal = Some(InitialJournalAttempt {
                run_id: attempt.run_id().to_owned(),
                task_page_id: "task-placeholder".to_owned(),
                executor: "Codex".to_owned(),
                started_at: "2026-01-01T00:00:00Z".to_owned(),
            });
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let executor = FakeExecutor {
            calls: Arc::clone(&calls),
            notion: notion.clone(),
            fail: false,
            outcome: Outcome::Done,
        };
        executor
            .execute(ExecutorRequest {
                instructions: "task body".to_owned(),
            })
            .unwrap();
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            executor,
            values(),
            "Codex".to_owned(),
        );

        assert_eq!(workflow.recover().await.unwrap(), 1);
        assert_eq!(calls.lock().unwrap().as_slice(), ["task body"]);
        let final_attempt = notion.state.lock().unwrap().final_journal.clone().unwrap();
        assert_eq!(final_attempt.initial.run_id, attempt.run_id());
        assert_eq!(final_attempt.result.outcome, Outcome::Error);
        assert_eq!(
            final_attempt.result.warnings,
            ["outcome unknown; external effects may have occurred"]
        );
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn startup_never_launches_ambiguous_prepared_authority() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        store
            .prepare_revision("task-placeholder", "2026-01-01T00:00:00Z")
            .unwrap();
        store
            .prepare_revision("task-placeholder", "2026-01-01T00:00:00Z")
            .unwrap();
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().visible_status = Some("Running".to_owned());
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

        assert_eq!(
            workflow.recover().await.unwrap_err(),
            "prepared attempt authority is ambiguous"
        );
        assert!(calls.lock().unwrap().is_empty());
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
                notion: notion.clone(),
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
        assert_eq!(
            notion.state.lock().unwrap().visible_status.as_deref(),
            Some("Error")
        );
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn startup_replays_terminal_writes_without_another_executor_launch() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        let calls = Arc::new(Mutex::new(Vec::new()));
        notion.state.lock().unwrap().hide_terminal_status = true;
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
        assert_eq!(
            workflow.prepare(discovered("task body")).await.unwrap_err(),
            "Notion terminal task status is not visible"
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(store.result_stored().unwrap().len(), 1);

        notion.state.lock().unwrap().hide_terminal_status = false;
        assert_eq!(workflow.recover().await.unwrap(), 1);
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert!(store.result_stored().unwrap().is_empty());
        assert_eq!(
            notion.state.lock().unwrap().visible_status.as_deref(),
            Some("Done")
        );
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn terminal_status_readback_resolves_an_ambiguous_write() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().terminal_status_error = Some("ambiguous response".to_owned());
        notion.state.lock().unwrap().finalize_error = Some("ambiguous response".to_owned());
        let workflow = ExecutionWorkflow::new(
            Arc::clone(&store),
            notion.clone(),
            FakeExecutor {
                calls: Arc::new(Mutex::new(Vec::new())),
                notion: notion.clone(),
                fail: false,
                outcome: Outcome::Done,
            },
            values(),
            "Codex".to_owned(),
        );

        workflow.prepare(discovered("task body")).await.unwrap();

        assert_eq!(
            notion.state.lock().unwrap().visible_status.as_deref(),
            Some("Done")
        );
        assert!(store.result_stored().unwrap().is_empty());
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn startup_replays_an_invisible_journal_finalization_before_terminal_status() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().hide_final_journal = true;
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

        assert_eq!(
            workflow.prepare(discovered("task body")).await.unwrap_err(),
            "Notion journal finalization is not visible"
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(notion.state.lock().unwrap().terminal_status_writes, 0);
        assert_eq!(store.result_stored().unwrap().len(), 1);

        notion.state.lock().unwrap().hide_final_journal = false;
        assert_eq!(workflow.recover().await.unwrap(), 1);
        let state = notion.state.lock().unwrap();
        assert_eq!(state.finalizes, 2);
        assert_eq!(state.terminal_status_writes, 1);
        drop(state);
        assert_eq!(calls.lock().unwrap().len(), 1);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn recovery_rejects_a_journal_that_does_not_match_durable_authority() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let attempt = store
            .prepare_revision("task-placeholder", "2026-01-01T00:00:00Z")
            .unwrap();
        store.record_launch_intent(attempt.run_id()).unwrap();
        store
            .store_result(
                attempt.run_id(),
                "2026-01-01T00:01:00Z",
                ExecutorResult {
                    outcome: Outcome::Done,
                    summary: "completed".to_owned(),
                    actions: vec!["acted".to_owned()],
                    warnings: vec![],
                },
            )
            .unwrap();
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().journal = Some(InitialJournalAttempt {
            run_id: attempt.run_id().to_owned(),
            task_page_id: "different-task".to_owned(),
            executor: "Codex".to_owned(),
            started_at: "2026-01-01T00:00:00Z".to_owned(),
        });
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

        assert_eq!(
            workflow.recover().await.unwrap_err(),
            "Notion journal readback does not match the durable attempt"
        );
        let state = notion.state.lock().unwrap();
        assert_eq!(state.finalizes, 0);
        assert_eq!(state.terminal_status_writes, 0);
        assert!(calls.lock().unwrap().is_empty());
        drop(state);
        drop(workflow);
        drop(store);
        std::fs::remove_dir_all(state_directory).unwrap();
    }

    #[tokio::test]
    async fn terminal_readback_rejects_a_different_task_identity() {
        let state_directory = directory();
        let store = Arc::new(
            AttemptStore::new(state_directory.clone())
                .acquire()
                .unwrap(),
        );
        let notion = FakeNotion::new();
        notion.state.lock().unwrap().wrong_terminal_page = true;
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

        assert_eq!(
            workflow.prepare(discovered("task body")).await.unwrap_err(),
            "Notion terminal task status is not visible"
        );
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(store.result_stored().unwrap().len(), 1);
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
            "executor action failed; terminal Error recorded"
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
    async fn error_to_pending_creates_one_distinct_attempt_for_the_new_revision() {
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
                    outcome: Outcome::Error,
                },
                values(),
                "Codex".to_owned(),
            ),
        ));
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config(),
            &values(),
        );

        assert_eq!(
            dispatcher
                .dispatch(task_event("first", "2026-01-01T00:00:00Z"))
                .await
                .unwrap_err(),
            "executor reported incomplete or blocked work; automatic retry is disabled"
        );
        assert_eq!(
            notion.state.lock().unwrap().visible_status.as_deref(),
            Some("Error")
        );
        assert_eq!(
            reconcile_once(&notion, &coordinator, &notion_config(), &values())
                .await
                .unwrap(),
            0
        );
        dispatcher
            .dispatch(task_event("first", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();

        {
            let mut state = notion.state.lock().unwrap();
            state.visible_status = Some("Pending".to_owned());
            state.revision = "2026-01-02T00:00:00Z".to_owned();
        }
        assert!(
            dispatcher
                .dispatch(task_event("late-signal", "2025-12-31T00:00:00Z"))
                .await
                .is_err()
        );
        dispatcher
            .dispatch(task_event("duplicate-retry", "2026-01-02T00:00:00Z"))
            .await
            .unwrap();
        assert_eq!(
            reconcile_once(&notion, &coordinator, &notion_config(), &values())
                .await
                .unwrap(),
            0
        );

        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["rendered-task-placeholder", "rendered-task-placeholder"]
        );
        let attempts = store.list_prepared().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_ne!(attempts[0].run_id(), attempts[1].run_id());
        assert!(attempts.iter().all(|attempt| attempt.result().is_some()));
        assert!(store.result_stored().unwrap().is_empty());
        let state = notion.state.lock().unwrap();
        assert_eq!(state.creates, 2);
        assert_eq!(state.finalizes, 2);
        assert_eq!(state.terminal_status_writes, 2);
        drop(state);
        drop(dispatcher);
        drop(coordinator);
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
