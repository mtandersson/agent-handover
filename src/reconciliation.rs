use crate::config::{NotionConfig, TaskValues};
use crate::coordination::{PreparationSink, RevisionCoordinator};
use crate::discovery::DiscoveredTask;
use crate::notion::{NotionAdapter, TaskState};
use std::collections::HashSet;

const MAX_QUERY_PAGES: usize = 10_000;
const MAX_QUERY_TASKS: usize = 10_000;
const MAX_QUERY_BYTES: usize = 16 * 1024 * 1024;

pub(crate) async fn reconcile_once<N: NotionAdapter, S: PreparationSink>(
    notion: &N,
    coordinator: &RevisionCoordinator<S>,
    notion_config: &NotionConfig,
    task_values: &TaskValues,
) -> Result<usize, String> {
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut candidates = Vec::new();
    let mut response_bytes = 0usize;

    for _ in 0..MAX_QUERY_PAGES {
        let page = notion
            .query_pending_tasks(cursor.as_deref(), &task_values.pending)
            .await?;
        response_bytes = response_bytes
            .checked_add(page.response_bytes)
            .filter(|total| *total <= MAX_QUERY_BYTES)
            .ok_or_else(|| "Notion Pending task query exceeds the total size limit".to_owned())?;
        if candidates.len().saturating_add(page.tasks.len()) > MAX_QUERY_TASKS {
            return Err("Notion Pending task query exceeds the task limit".to_owned());
        }
        candidates.extend(page.tasks);
        match page.next_cursor {
            Some(next) if seen_cursors.insert(next.clone()) => cursor = Some(next),
            Some(_) => return Err("Notion Pending task query contains a cursor cycle".to_owned()),
            None => {
                cursor = None;
                break;
            }
        }
    }
    if cursor.is_some() {
        return Err("Notion Pending task query exceeds the page limit".to_owned());
    }

    let mut seen_pages = HashSet::new();
    let mut eligible = Vec::new();
    for observed in candidates {
        if !seen_pages.insert(observed.page_id.clone())
            || !eligible_state(&observed, notion_config, task_values)
        {
            continue;
        }
        let current = notion.refetch_task(&observed.page_id).await?;
        if current.revision != observed.revision
            || !eligible_state(&current, notion_config, task_values)
        {
            continue;
        }
        eligible.push(current);
    }

    eligible.sort_by(|left, right| {
        left.revision
            .instant()
            .cmp(&right.revision.instant())
            .then_with(|| left.page_id.cmp(&right.page_id))
    });

    let mut count = 0;
    for state in eligible {
        let instructions = notion.render_task(&state.page_id).await?;
        if coordinator
            .prepare(DiscoveredTask {
                state,
                instructions,
            })
            .await?
        {
            count += 1;
        }
    }
    Ok(count)
}

fn eligible_state(
    task: &TaskState,
    notion_config: &NotionConfig,
    task_values: &TaskValues,
) -> bool {
    !task.in_trash
        && task.data_source_id.as_deref() == Some(&notion_config.task_data_source_id)
        && task.status.as_deref() == Some(&task_values.pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::NotionEventDispatcher;
    use crate::http::EventDispatcher;
    use crate::notion::{PendingTaskPage, TaskRevision};
    use std::collections::{HashMap, VecDeque};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Clone)]
    struct FakeNotion {
        pages: Arc<Mutex<VecDeque<PendingTaskPage>>>,
        current: Arc<Mutex<HashMap<String, TaskState>>>,
        cursors: Arc<Mutex<Vec<Option<String>>>>,
    }

    impl FakeNotion {
        fn new(pages: Vec<PendingTaskPage>, current: Vec<TaskState>) -> Self {
            Self {
                pages: Arc::new(Mutex::new(pages.into())),
                current: Arc::new(Mutex::new(
                    current
                        .into_iter()
                        .map(|task| (task.page_id.clone(), task))
                        .collect(),
                )),
                cursors: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl NotionAdapter for FakeNotion {
        fn query_pending_tasks<'a>(
            &'a self,
            cursor: Option<&'a str>,
            pending_status: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<PendingTaskPage, String>> + Send + 'a>> {
            Box::pin(async move {
                assert_eq!(pending_status, "Pending");
                self.cursors.lock().unwrap().push(cursor.map(str::to_owned));
                self.pages
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or_else(|| "unexpected query".to_owned())
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

    #[derive(Default)]
    struct RecordingPreparation {
        active: AtomicUsize,
        peak: AtomicUsize,
        page_ids: Mutex<Vec<String>>,
    }

    impl PreparationSink for RecordingPreparation {
        fn prepare<'a>(
            &'a self,
            task: DiscoveredTask,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1)).await;
                assert_eq!(
                    task.instructions,
                    format!("instructions-{}", task.state.page_id)
                );
                self.page_ids.lock().unwrap().push(task.state.page_id);
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

    fn page(tasks: Vec<TaskState>, next_cursor: Option<&str>) -> PendingTaskPage {
        PendingTaskPage {
            tasks,
            next_cursor: next_cursor.map(str::to_owned),
            response_bytes: 100,
        }
    }

    fn config() -> (NotionConfig, TaskValues) {
        (
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

    #[tokio::test]
    async fn paginates_a_missed_task_and_prepares_the_observed_set_in_stable_order() {
        let early_b = task("page-b", "2026-01-01T00:00:00Z");
        let late = task("page-c", "2026-01-02T00:00:00Z");
        let early_a = task("page-a", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(
            vec![
                page(vec![late.clone()], Some("next-placeholder")),
                page(vec![early_b.clone(), early_a.clone()], None),
            ],
            vec![early_a, early_b, late],
        );
        let preparation = RevisionCoordinator::new(RecordingPreparation::default());
        let (notion_config, task_values) = config();

        assert_eq!(
            reconcile_once(&notion, &preparation, &notion_config, &task_values)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            *notion.cursors.lock().unwrap(),
            vec![None, Some("next-placeholder".to_owned())]
        );
        assert_eq!(
            *preparation.sink.page_ids.lock().unwrap(),
            vec!["page-a", "page-b", "page-c"]
        );
        assert_eq!(preparation.sink.peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn excludes_unrelated_trashed_non_pending_and_stale_tasks() {
        let valid = task("valid", "2026-01-01T00:00:00Z");
        let mut unrelated = task("unrelated", "2026-01-01T00:00:00Z");
        unrelated.data_source_id = Some("other-source-placeholder".to_owned());
        let mut trashed = task("trashed", "2026-01-01T00:00:00Z");
        trashed.in_trash = true;
        let non_pending_observed = task("non-pending", "2026-01-01T00:00:00Z");
        let mut non_pending_current = non_pending_observed.clone();
        non_pending_current.status = Some("Running".to_owned());
        let stale_observed = task("stale", "2026-01-01T00:00:00Z");
        let stale_current = task("stale", "2026-01-02T00:00:00Z");
        let observed = vec![
            valid.clone(),
            unrelated.clone(),
            trashed.clone(),
            non_pending_observed,
            stale_observed,
        ];
        let notion = FakeNotion::new(
            vec![page(observed, None)],
            vec![
                valid,
                unrelated,
                trashed,
                non_pending_current,
                stale_current,
            ],
        );
        let preparation = RevisionCoordinator::new(RecordingPreparation::default());
        let (notion_config, task_values) = config();

        assert_eq!(
            reconcile_once(&notion, &preparation, &notion_config, &task_values)
                .await
                .unwrap(),
            1
        );
        assert_eq!(*preparation.sink.page_ids.lock().unwrap(), vec!["valid"]);
    }

    #[tokio::test]
    async fn concurrent_webhook_and_reconciliation_prepare_one_authoritative_revision() {
        let candidate = task("page-a", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(vec![page(vec![candidate.clone()], None)], vec![candidate]);
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let (notion_config, task_values) = config();
        let dispatcher = NotionEventDispatcher::with_coordinator(
            notion.clone(),
            Arc::clone(&coordinator),
            &notion_config,
            &task_values,
        );
        let signal = serde_json::json!({
            "id": "event-placeholder",
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "page.properties_updated",
            "entity": {"id": "page-a", "type": "page"},
            "data": {
                "parent": {
                    "type": "database",
                    "data_source_id": "task-source-placeholder"
                }
            }
        });

        let (webhook, reconciled) = tokio::join!(
            dispatcher.dispatch(signal),
            reconcile_once(&notion, &coordinator, &notion_config, &task_values),
        );

        webhook.unwrap();
        assert!(reconciled.unwrap() <= 1);
        assert_eq!(
            coordinator.sink.page_ids.lock().unwrap().as_slice(),
            ["page-a"]
        );
    }

    #[tokio::test]
    async fn duplicate_reconciliation_results_prepare_one_authoritative_revision() {
        let candidate = task("page-a", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(
            vec![
                page(vec![candidate.clone()], None),
                page(vec![candidate.clone()], None),
            ],
            vec![candidate],
        );
        let coordinator = RevisionCoordinator::new(RecordingPreparation::default());
        let (notion_config, task_values) = config();

        assert_eq!(
            reconcile_once(&notion, &coordinator, &notion_config, &task_values)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            reconcile_once(&notion, &coordinator, &notion_config, &task_values)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            coordinator.sink.page_ids.lock().unwrap().as_slice(),
            ["page-a"]
        );
    }

    #[tokio::test]
    async fn rejects_query_cursor_cycles_and_total_response_overflow() {
        let (notion_config, task_values) = config();
        let preparation = RevisionCoordinator::new(RecordingPreparation::default());
        let cycle = FakeNotion::new(
            vec![
                page(Vec::new(), Some("repeat")),
                page(Vec::new(), Some("repeat")),
            ],
            Vec::new(),
        );
        assert_eq!(
            reconcile_once(&cycle, &preparation, &notion_config, &task_values)
                .await
                .unwrap_err(),
            "Notion Pending task query contains a cursor cycle"
        );

        let oversized = FakeNotion::new(
            vec![PendingTaskPage {
                tasks: Vec::new(),
                next_cursor: None,
                response_bytes: MAX_QUERY_BYTES + 1,
            }],
            Vec::new(),
        );
        assert_eq!(
            reconcile_once(&oversized, &preparation, &notion_config, &task_values)
                .await
                .unwrap_err(),
            "Notion Pending task query exceeds the total size limit"
        );
    }

    #[tokio::test]
    async fn enforces_task_and_page_limits_at_their_exact_boundaries() {
        let (notion_config, task_values) = config();
        let preparation = RevisionCoordinator::new(crate::coordination::PendingPreparationSink);

        let mut boundary_tasks = Vec::with_capacity(MAX_QUERY_TASKS);
        for index in 0..MAX_QUERY_TASKS {
            let mut candidate = task(&format!("task-{index}"), "2026-01-01T00:00:00Z");
            candidate.data_source_id = Some("unrelated-source-placeholder".to_owned());
            boundary_tasks.push(candidate);
        }
        let at_task_limit = FakeNotion::new(vec![page(boundary_tasks.clone(), None)], Vec::new());
        assert_eq!(
            reconcile_once(&at_task_limit, &preparation, &notion_config, &task_values,)
                .await
                .unwrap(),
            0
        );
        boundary_tasks.push(task("one-too-many", "2026-01-01T00:00:00Z"));
        let over_task_limit = FakeNotion::new(vec![page(boundary_tasks, None)], Vec::new());
        assert_eq!(
            reconcile_once(&over_task_limit, &preparation, &notion_config, &task_values,)
                .await
                .unwrap_err(),
            "Notion Pending task query exceeds the task limit"
        );

        let pages_at_limit = (0..MAX_QUERY_PAGES)
            .map(|index| {
                let next = (index + 1 < MAX_QUERY_PAGES).then(|| format!("cursor-{index}"));
                page(Vec::new(), next.as_deref())
            })
            .collect();
        let at_page_limit = FakeNotion::new(pages_at_limit, Vec::new());
        assert_eq!(
            reconcile_once(&at_page_limit, &preparation, &notion_config, &task_values,)
                .await
                .unwrap(),
            0
        );

        let pages_over_limit = (0..MAX_QUERY_PAGES)
            .map(|index| page(Vec::new(), Some(&format!("cursor-{index}"))))
            .collect();
        let over_page_limit = FakeNotion::new(pages_over_limit, Vec::new());
        assert_eq!(
            reconcile_once(&over_page_limit, &preparation, &notion_config, &task_values,)
                .await
                .unwrap_err(),
            "Notion Pending task query exceeds the page limit"
        );
    }

    #[tokio::test]
    async fn stops_after_a_content_free_preparation_failure() {
        struct FailingPreparation;
        impl PreparationSink for FailingPreparation {
            fn prepare<'a>(
                &'a self,
                _task: DiscoveredTask,
            ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
                Box::pin(async { Err("cannot prepare discovered task".to_owned()) })
            }
        }
        let candidate = task("private-page-placeholder", "2026-01-01T00:00:00Z");
        let notion = FakeNotion::new(vec![page(vec![candidate.clone()], None)], vec![candidate]);
        let (notion_config, task_values) = config();

        assert_eq!(
            reconcile_once(
                &notion,
                &RevisionCoordinator::new(FailingPreparation),
                &notion_config,
                &task_values,
            )
            .await
            .unwrap_err(),
            "cannot prepare discovered task"
        );
    }
}
