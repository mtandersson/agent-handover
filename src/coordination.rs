use crate::discovery::DiscoveredTask;
use crate::notion::TaskRevision;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, OwnedMutexGuard};

const REMEMBERED_PAGE_REVISIONS: usize = 4096;

pub(crate) trait PreparationSink: Send + Sync + 'static {
    fn prepare<'a>(
        &'a self,
        task: DiscoveredTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

    fn recover<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<usize, String>> + Send + 'a>> {
        Box::pin(async { Ok(0) })
    }
}

#[cfg(test)]
pub(crate) struct PendingPreparationSink;

#[cfg(test)]
impl PreparationSink for PendingPreparationSink {
    fn prepare<'a>(
        &'a self,
        _task: DiscoveredTask,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct CoordinationState {
    accepted_revisions: HashMap<String, TaskRevision>,
    accepted_order: VecDeque<String>,
    page_gates: HashMap<String, Arc<PageGate>>,
}

impl CoordinationState {
    fn remember(&mut self, page_id: String, revision: TaskRevision, limit: usize) {
        if self.accepted_revisions.contains_key(&page_id) {
            self.accepted_order.retain(|known| known != &page_id);
        }
        self.accepted_revisions.insert(page_id.clone(), revision);
        self.accepted_order.push_back(page_id);
        while self.accepted_order.len() > limit {
            if let Some(expired) = self.accepted_order.pop_front() {
                self.accepted_revisions.remove(&expired);
            }
        }
    }
}

struct PageGate {
    lock: Arc<Mutex<()>>,
    users: AtomicUsize,
}

impl PageGate {
    fn new() -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
            users: AtomicUsize::new(0),
        }
    }
}

pub(crate) struct RevisionCoordinator<S> {
    pub(crate) sink: Arc<S>,
    state: Arc<Mutex<CoordinationState>>,
    preparation: Arc<Mutex<()>>,
    revision_limit: usize,
}

impl<S> RevisionCoordinator<S>
where
    S: PreparationSink,
{
    pub(crate) fn new(sink: S) -> Self {
        Self::with_limit(sink, REMEMBERED_PAGE_REVISIONS)
    }

    fn with_limit(sink: S, revision_limit: usize) -> Self {
        Self {
            sink: Arc::new(sink),
            state: Arc::new(Mutex::new(CoordinationState::default())),
            preparation: Arc::new(Mutex::new(())),
            revision_limit,
        }
    }

    pub(crate) async fn prepare(&self, task: DiscoveredTask) -> Result<bool, String> {
        let lease = self.acquire_page(&task.state.page_id).await;
        let accepted = self.prepare_serialized(task).await;
        lease.release().await;
        accepted
    }

    pub(crate) async fn recover(&self) -> Result<usize, String> {
        let _preparation = self.preparation.lock().await;
        self.sink.recover().await
    }

    async fn prepare_serialized(&self, task: DiscoveredTask) -> Result<bool, String> {
        let _preparation = self.preparation.lock().await;
        {
            let state = self.state.lock().await;
            if state
                .accepted_revisions
                .get(&task.state.page_id)
                .is_some_and(|known| known.instant() >= task.state.revision.instant())
            {
                return Ok(false);
            }
        }

        self.sink.prepare(task.clone()).await?;

        let mut state = self.state.lock().await;
        if !state
            .accepted_revisions
            .get(&task.state.page_id)
            .is_some_and(|known| known.instant() >= task.state.revision.instant())
        {
            state.remember(task.state.page_id, task.state.revision, self.revision_limit);
        }
        Ok(true)
    }

    async fn acquire_page(&self, page_id: &str) -> PageLease {
        let gate = {
            let mut state = self.state.lock().await;
            let gate = Arc::clone(
                state
                    .page_gates
                    .entry(page_id.to_owned())
                    .or_insert_with(|| Arc::new(PageGate::new())),
            );
            gate.users.fetch_add(1, Ordering::SeqCst);
            gate
        };
        let reservation = PageReservation {
            state: Arc::clone(&self.state),
            gate: Some(Arc::clone(&gate)),
        };
        let guard = Arc::clone(&gate.lock).lock_owned().await;
        reservation.into_lease(guard)
    }
}

struct PageReservation {
    state: Arc<Mutex<CoordinationState>>,
    gate: Option<Arc<PageGate>>,
}

impl PageReservation {
    fn into_lease(mut self, guard: OwnedMutexGuard<()>) -> PageLease {
        PageLease {
            reservation: Some(PageReservation {
                state: Arc::clone(&self.state),
                gate: self.gate.take(),
            }),
            guard: Some(guard),
        }
    }

    async fn release(mut self) {
        let gate = self.gate.take().expect("page reservation owns its gate");
        let mut state = self.state.lock().await;
        release_page(&mut state, &gate);
    }
}

impl Drop for PageReservation {
    fn drop(&mut self) {
        let Some(gate) = self.gate.take() else {
            return;
        };
        if let Ok(mut state) = self.state.try_lock() {
            release_page(&mut state, &gate);
            return;
        }
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let mut state = state.lock().await;
            release_page(&mut state, &gate);
        });
    }
}

struct PageLease {
    reservation: Option<PageReservation>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl PageLease {
    async fn release(mut self) {
        self.guard.take();
        self.reservation
            .take()
            .expect("page lease owns its reservation")
            .release()
            .await;
    }
}

impl Drop for PageLease {
    fn drop(&mut self) {
        self.guard.take();
    }
}

fn release_page(state: &mut CoordinationState, gate: &Arc<PageGate>) {
    if gate.users.fetch_sub(1, Ordering::SeqCst) == 1 {
        state
            .page_gates
            .retain(|_, known| !Arc::ptr_eq(known, gate));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notion::{TaskRevision, TaskState};
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingPreparation {
        active: AtomicUsize,
        peak: AtomicUsize,
        accepted: StdMutex<Vec<(String, TaskRevision)>>,
        failures: AtomicUsize,
    }

    impl PreparationSink for RecordingPreparation {
        fn prepare<'a>(
            &'a self,
            task: DiscoveredTask,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                if self
                    .failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                        count.checked_sub(1)
                    })
                    .is_ok()
                {
                    return Err("preparation unavailable".to_owned());
                }
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.accepted
                    .lock()
                    .unwrap()
                    .push((task.state.page_id, task.state.revision));
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn task(page_id: &str, revision: &str) -> DiscoveredTask {
        DiscoveredTask {
            state: TaskState {
                page_id: page_id.to_owned(),
                revision: TaskRevision::parse(revision).unwrap(),
                data_source_id: Some("task-source-placeholder".to_owned()),
                status: Some("Pending".to_owned()),
                in_trash: false,
            },
            instructions: format!("instructions-{page_id}"),
        }
    }

    #[tokio::test]
    async fn one_revision_is_prepared_once_across_concurrent_sources() {
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let webhook = Arc::clone(&coordinator);
        let reconciliation = Arc::clone(&coordinator);
        let revision = "2026-01-01T00:00:00Z";
        let (first, second) = tokio::join!(
            webhook.prepare(task("page-a", revision)),
            reconciliation.prepare(task("page-a", revision)),
        );
        assert_eq!(
            [first.unwrap(), second.unwrap()]
                .into_iter()
                .filter(|v| *v)
                .count(),
            1
        );
        assert_eq!(coordinator.sink.accepted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn preparation_is_sequential_across_unrelated_pages() {
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let (older, newer, unrelated) = tokio::join!(
            coordinator.prepare(task("page-a", "2026-01-01T00:00:00Z")),
            coordinator.prepare(task("page-a", "2026-01-02T00:00:00Z")),
            coordinator.prepare(task("page-b", "2026-01-01T00:00:00Z")),
        );
        older.unwrap();
        newer.unwrap();
        unrelated.unwrap();
        assert_eq!(coordinator.sink.accepted.lock().unwrap().len(), 3);
        assert_eq!(coordinator.sink.peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn older_and_equal_revisions_do_not_follow_a_newer_accepted_revision() {
        let coordinator = RevisionCoordinator::new(RecordingPreparation::default());
        assert!(
            coordinator
                .prepare(task("page-a", "2026-01-03T00:00:00Z"))
                .await
                .unwrap()
        );
        assert!(
            !coordinator
                .prepare(task("page-a", "2026-01-02T00:00:00Z"))
                .await
                .unwrap()
        );
        assert!(
            !coordinator
                .prepare(task("page-a", "2026-01-03T01:00:00+01:00"))
                .await
                .unwrap()
        );
        assert_eq!(coordinator.sink.accepted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_handoff_is_retried_and_only_success_commits_the_revision() {
        let sink = RecordingPreparation::default();
        sink.failures.store(1, Ordering::SeqCst);
        let coordinator = RevisionCoordinator::new(sink);
        let discovered = task("page-a", "2026-01-01T00:00:00Z");
        assert_eq!(
            coordinator.prepare(discovered.clone()).await.unwrap_err(),
            "preparation unavailable"
        );
        assert!(coordinator.prepare(discovered).await.unwrap());
        assert_eq!(coordinator.sink.accepted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_later_manual_retry_revision_is_prepared() {
        let coordinator = RevisionCoordinator::new(RecordingPreparation::default());
        assert!(
            coordinator
                .prepare(task("page-a", "2026-01-01T00:00:00Z"))
                .await
                .unwrap()
        );
        assert!(
            coordinator
                .prepare(task("page-a", "2026-01-03T00:00:00Z"))
                .await
                .unwrap()
        );
        assert_eq!(coordinator.sink.accepted.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn revision_cache_evicts_the_least_recently_accepted_page() {
        let coordinator = RevisionCoordinator::with_limit(RecordingPreparation::default(), 2);
        coordinator
            .prepare(task("page-a", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        coordinator
            .prepare(task("page-b", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        coordinator
            .prepare(task("page-a", "2026-01-02T00:00:00Z"))
            .await
            .unwrap();
        coordinator
            .prepare(task("page-c", "2026-01-01T00:00:00Z"))
            .await
            .unwrap();

        let state = coordinator.state.lock().await;
        assert_eq!(
            state.accepted_order,
            VecDeque::from(["page-a".to_owned(), "page-c".to_owned()])
        );
        assert!(!state.accepted_revisions.contains_key("page-b"));
    }

    #[tokio::test]
    async fn canceling_same_page_waiter_releases_its_gate_reservation() {
        let coordinator = Arc::new(RevisionCoordinator::new(RecordingPreparation::default()));
        let owner = coordinator.acquire_page("page-a").await;
        let waiting_coordinator = Arc::clone(&coordinator);
        let waiter = tokio::spawn(async move { waiting_coordinator.acquire_page("page-a").await });

        loop {
            let users = coordinator
                .state
                .lock()
                .await
                .page_gates
                .get("page-a")
                .map(|gate| gate.users.load(Ordering::SeqCst));
            if users == Some(2) {
                break;
            }
            tokio::task::yield_now().await;
        }

        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        assert_eq!(
            coordinator
                .state
                .lock()
                .await
                .page_gates
                .get("page-a")
                .map(|gate| gate.users.load(Ordering::SeqCst)),
            Some(1)
        );
        owner.release().await;
        assert!(coordinator.state.lock().await.page_gates.is_empty());
    }
}
