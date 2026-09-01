use crate::notion::{InitialJournalAttempt, NotionAdapter};
use crate::state::{LockedAttemptStore, PreparedAttempt};

#[derive(Debug)]
pub(crate) struct LaunchReadyAttempt {
    prepared: PreparedAttempt,
}

impl LaunchReadyAttempt {
    pub(crate) fn run_id(&self) -> &str {
        self.prepared.run_id()
    }
}

pub(crate) async fn prepare_visible_attempt<N: NotionAdapter>(
    store: &LockedAttemptStore,
    notion: &N,
    task_page_id: &str,
    running_status: &str,
    executor: &str,
    started_at: &str,
) -> Result<LaunchReadyAttempt, String> {
    let prepared = store.prepare(task_page_id)?;
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
    use crate::notion::{TaskRevision, TaskState};
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
            Box::pin(async { Ok(String::new()) })
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
