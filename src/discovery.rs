use crate::config::{NotionConfig, TaskValues};
use crate::http::EventDispatcher;
use crate::notion::{NotionAdapter, TaskRevision, TaskState};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore, watch};

const REMEMBERED_EVENT_IDS: usize = 4096;
const REMEMBERED_PAGE_REVISIONS: usize = 4096;
pub const MAX_DISPATCH_WORK: usize = 16;
const MAX_EVENT_FIELD_BYTES: usize = 128;
const MAX_TIMESTAMP_BYTES: usize = 64;

pub trait DiscoverySink: Send + Sync + 'static {
    fn task_discovered(&self, task: TaskState) -> Result<(), String>;
}

pub struct PendingDiscoverySink;

impl DiscoverySink for PendingDiscoverySink {
    fn task_discovered(&self, _task: TaskState) -> Result<(), String> {
        Ok(())
    }
}

type EventResult = Result<(), String>;
type CompletionSender = watch::Sender<Option<EventResult>>;

#[derive(Default)]
struct DeliveryState {
    handled_event_ids: HashSet<String>,
    handled_event_order: VecDeque<String>,
    in_flight_events: HashMap<String, CompletionSender>,
    page_revisions: HashMap<String, TaskRevision>,
    page_revision_order: VecDeque<String>,
    page_gates: HashMap<String, Arc<PageGate>>,
}

impl DeliveryState {
    fn remember_event(&mut self, event_id: String, limit: usize) {
        if self.handled_event_ids.insert(event_id.clone()) {
            self.handled_event_order.push_back(event_id);
        }
        while self.handled_event_order.len() > limit {
            if let Some(expired) = self.handled_event_order.pop_front() {
                self.handled_event_ids.remove(&expired);
            }
        }
    }

    fn remember_revision(&mut self, page_id: String, revision: TaskRevision, limit: usize) {
        if self.page_revisions.contains_key(&page_id) {
            self.page_revision_order.retain(|entry| entry != &page_id);
        }
        self.page_revisions.insert(page_id.clone(), revision);
        self.page_revision_order.push_back(page_id);
        while self.page_revision_order.len() > limit {
            if let Some(expired) = self.page_revision_order.pop_front() {
                self.page_revisions.remove(&expired);
            }
        }
    }
}

struct PageGate {
    lock: Arc<Mutex<()>>,
    users: AtomicUsize,
}

#[cfg(test)]
struct AdmissionHook {
    barrier: tokio::sync::Barrier,
    remaining: AtomicUsize,
}

#[cfg(test)]
impl AdmissionHook {
    async fn wait(&self) {
        if self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            self.barrier.wait().await;
        }
    }
}

impl PageGate {
    fn new() -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
            users: AtomicUsize::new(0),
        }
    }
}

struct DispatcherInner<A, S> {
    adapter: Arc<A>,
    sink: Arc<S>,
    task_data_source_id: Arc<str>,
    codex: Arc<str>,
    pending: Arc<str>,
    delivery: Arc<Mutex<DeliveryState>>,
    event_limit: usize,
    revision_limit: usize,
    work_permits: Arc<Semaphore>,
    #[cfg(test)]
    admission_hook: Option<Arc<AdmissionHook>>,
}

enum Admission {
    Handled,
    Subscribe(watch::Receiver<Option<EventResult>>),
    Start {
        completion: watch::Receiver<Option<EventResult>>,
        event: WebhookEvent,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
}

pub struct NotionEventDispatcher<A, S> {
    inner: Arc<DispatcherInner<A, S>>,
}

impl<A, S> NotionEventDispatcher<A, S>
where
    A: NotionAdapter,
    S: DiscoverySink,
{
    pub fn new(adapter: A, sink: S, notion: &NotionConfig, values: &TaskValues) -> Self {
        Self::with_limits(
            adapter,
            sink,
            notion,
            values,
            REMEMBERED_EVENT_IDS,
            REMEMBERED_PAGE_REVISIONS,
            MAX_DISPATCH_WORK,
        )
    }

    fn with_limits(
        adapter: A,
        sink: S,
        notion: &NotionConfig,
        values: &TaskValues,
        event_limit: usize,
        revision_limit: usize,
        work_limit: usize,
    ) -> Self {
        Self {
            inner: Arc::new(DispatcherInner {
                adapter: Arc::new(adapter),
                sink: Arc::new(sink),
                task_data_source_id: Arc::from(notion.task_data_source_id.as_str()),
                codex: Arc::from(values.codex.as_str()),
                pending: Arc::from(values.pending.as_str()),
                delivery: Arc::new(Mutex::new(DeliveryState::default())),
                event_limit,
                revision_limit,
                work_permits: Arc::new(Semaphore::new(work_limit)),
                #[cfg(test)]
                admission_hook: None,
            }),
        }
    }

    async fn process(&self, value: Value) -> EventResult {
        let Ok(event) = serde_json::from_value::<WebhookEvent>(value) else {
            return Ok(());
        };
        if !event.is_task_signal(&self.inner.task_data_source_id) {
            return Ok(());
        }

        let existing = {
            let delivery = self.inner.delivery.lock().await;
            if delivery.handled_event_ids.contains(&event.id) {
                return Ok(());
            }
            delivery
                .in_flight_events
                .get(&event.id)
                .map(watch::Sender::subscribe)
        };
        if let Some(mut completion) = existing {
            return wait_for_completion(&mut completion).await;
        }

        #[cfg(test)]
        if let Some(hook) = &self.inner.admission_hook {
            hook.wait().await;
        }

        // No state is claimed while admission is pending, so cancellation here
        // cannot leave an event, page gate, or cache entry behind.
        let permit = Arc::clone(&self.inner.work_permits)
            .acquire_owned()
            .await
            .map_err(|_| "event dispatcher stopped unexpectedly".to_owned())?;
        let mut permit = Some(permit);
        let admission = {
            let mut delivery = self.inner.delivery.lock().await;
            if delivery.handled_event_ids.contains(&event.id) {
                Admission::Handled
            } else if let Some(completion) = delivery.in_flight_events.get(&event.id) {
                Admission::Subscribe(completion.subscribe())
            } else {
                let (completion, receiver) = watch::channel(None);
                delivery
                    .in_flight_events
                    .insert(event.id.clone(), completion);
                Admission::Start {
                    completion: receiver,
                    event,
                    permit: permit.take().expect("new event owns its admission permit"),
                }
            }
        };
        // A race loser never retains scarce admission while it waits for the
        // registered supervisor. Only Admission::Start moved the permit out.
        drop(permit);

        match admission {
            Admission::Handled => Ok(()),
            Admission::Subscribe(mut completion) => wait_for_completion(&mut completion).await,
            Admission::Start {
                mut completion,
                event,
                permit,
            } => {
                let inner = Arc::clone(&self.inner);
                tokio::spawn(async move {
                    let _permit = permit;
                    let event_id = event.id.clone();
                    let worker = Arc::clone(&inner);
                    let outcome = tokio::spawn(async move { worker.run_event(event).await })
                        .await
                        .unwrap_or_else(
                            |_| Err("event supervisor stopped unexpectedly".to_owned()),
                        );
                    inner.finish_event(event_id, outcome).await;
                });
                wait_for_completion(&mut completion).await
            }
        }
    }
}

async fn wait_for_completion(completion: &mut watch::Receiver<Option<EventResult>>) -> EventResult {
    loop {
        if let Some(result) = completion.borrow().clone() {
            return result;
        }
        completion
            .changed()
            .await
            .map_err(|_| "event supervisor stopped unexpectedly".to_owned())?;
    }
}

impl<A, S> DispatcherInner<A, S>
where
    A: NotionAdapter,
    S: DiscoverySink,
{
    async fn run_event(self: Arc<Self>, event: WebhookEvent) -> EventResult {
        // Event timestamps are deliberately not watermarks. A unique event can
        // arrive late and still signal a newer authoritative page revision.
        let task = self.adapter.refetch_task(&event.entity.id).await?;
        let eligible = task.data_source_id.as_deref() == Some(&self.task_data_source_id)
            && task.executor.as_deref() == Some(&self.codex)
            && task.status.as_deref() == Some(&self.pending)
            && !task.in_trash;
        if !eligible {
            return Ok(());
        }

        let lease = self.acquire_page(&task.page_id).await;
        let result = self.decide_and_discover(task).await;
        lease.release().await;
        result
    }

    async fn decide_and_discover(&self, task: TaskState) -> EventResult {
        {
            let delivery = self.delivery.lock().await;
            if delivery
                .page_revisions
                .get(&task.page_id)
                .is_some_and(|known| known.instant() >= task.revision.instant())
            {
                return Ok(());
            }
        }

        let sink = Arc::clone(&self.sink);
        let sink_task = task.clone();
        let result = tokio::task::spawn_blocking(move || sink.task_discovered(sink_task))
            .await
            .map_err(|_| "discovery boundary stopped unexpectedly".to_owned())?;
        if result.is_ok() {
            let mut delivery = self.delivery.lock().await;
            if !delivery
                .page_revisions
                .get(&task.page_id)
                .is_some_and(|known| known.instant() >= task.revision.instant())
            {
                delivery.remember_revision(task.page_id, task.revision, self.revision_limit);
            }
        }
        result
    }

    async fn acquire_page(&self, page_id: &str) -> PageLease {
        let gate = {
            let mut delivery = self.delivery.lock().await;
            let gate = Arc::clone(
                delivery
                    .page_gates
                    .entry(page_id.to_owned())
                    .or_insert_with(|| Arc::new(PageGate::new())),
            );
            gate.users.fetch_add(1, Ordering::SeqCst);
            gate
        };
        let guard = Arc::clone(&gate.lock).lock_owned().await;
        PageLease {
            delivery: Arc::clone(&self.delivery),
            gate: Some(gate),
            guard: Some(guard),
        }
    }

    async fn finish_event(&self, event_id: String, outcome: EventResult) {
        let mut delivery = self.delivery.lock().await;
        if outcome.is_ok() {
            delivery.remember_event(event_id.clone(), self.event_limit);
        }
        if let Some(completion) = delivery.in_flight_events.remove(&event_id) {
            let _ = completion.send(Some(outcome));
        }
    }
}

struct PageLease {
    delivery: Arc<Mutex<DeliveryState>>,
    gate: Option<Arc<PageGate>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl PageLease {
    async fn release(mut self) {
        self.guard.take();
        let gate = self.gate.take().expect("page lease owns its gate");
        let mut delivery = self.delivery.lock().await;
        release_page(&mut delivery, &gate);
    }
}

impl Drop for PageLease {
    fn drop(&mut self) {
        self.guard.take();
        let Some(gate) = self.gate.take() else {
            return;
        };
        if let Ok(mut delivery) = self.delivery.try_lock() {
            release_page(&mut delivery, &gate);
            return;
        }
        let delivery = Arc::clone(&self.delivery);
        tokio::spawn(async move {
            let mut delivery = delivery.lock().await;
            release_page(&mut delivery, &gate);
        });
    }
}

fn release_page(delivery: &mut DeliveryState, gate: &Arc<PageGate>) {
    if gate.users.fetch_sub(1, Ordering::SeqCst) == 1 {
        delivery
            .page_gates
            .retain(|_, known| !Arc::ptr_eq(known, gate));
    }
}

impl<A, S> EventDispatcher for NotionEventDispatcher<A, S>
where
    A: NotionAdapter,
    S: DiscoverySink,
{
    fn dispatch(&self, event: Value) -> Pin<Box<dyn Future<Output = EventResult> + Send + '_>> {
        Box::pin(self.process(event))
    }
}

#[derive(Deserialize)]
struct WebhookEvent {
    id: String,
    timestamp: String,
    #[serde(rename = "type")]
    kind: String,
    entity: Entity,
    data: EventData,
}

impl WebhookEvent {
    fn is_task_signal(&self, configured_source: &str) -> bool {
        valid_field(&self.id, MAX_EVENT_FIELD_BYTES)
            && valid_timestamp(&self.timestamp)
            && valid_field(&self.entity.id, MAX_EVENT_FIELD_BYTES)
            && self.entity.kind == "page"
            && matches!(
                self.kind.as_str(),
                "page.created" | "page.content_updated" | "page.properties_updated"
            )
            && self
                .data
                .parent
                .data_source_id
                .as_deref()
                .is_some_and(|source| {
                    valid_field(source, MAX_EVENT_FIELD_BYTES) && source == configured_source
                })
    }
}

fn valid_field(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && value.is_ascii()
}

fn valid_timestamp(value: &str) -> bool {
    valid_field(value, MAX_TIMESTAMP_BYTES)
        && time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .is_ok()
}

#[derive(Deserialize)]
struct Entity {
    id: String,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct EventData {
    parent: EventParent,
}

#[derive(Deserialize)]
struct EventParent {
    #[serde(default)]
    data_source_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Condvar, Mutex as StdMutex};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    type FakeResponse = dyn Fn(&str, usize) -> (Result<TaskState, String>, Duration) + Send + Sync;

    struct FakeNotion {
        response: Arc<FakeResponse>,
        calls: AtomicUsize,
    }

    impl NotionAdapter for FakeNotion {
        fn refetch_task<'a>(
            &'a self,
            page_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<TaskState, String>> + Send + 'a>> {
            Box::pin(async move {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                let (task, delay) = (self.response)(page_id, call);
                tokio::time::sleep(delay).await;
                task
            })
        }
    }

    #[derive(Default)]
    struct RecordingSink(StdMutex<Vec<TaskState>>);

    impl DiscoverySink for RecordingSink {
        fn task_discovered(&self, task: TaskState) -> EventResult {
            self.0.lock().unwrap().push(task);
            Ok(())
        }
    }

    fn task(page: &str, source: &str, executor: &str, status: &str, revision: &str) -> TaskState {
        TaskState {
            page_id: page.to_owned(),
            revision: TaskRevision::parse(revision).unwrap(),
            data_source_id: Some(source.to_owned()),
            executor: Some(executor.to_owned()),
            status: Some(status.to_owned()),
            in_trash: false,
        }
    }

    fn eligible(page: &str, revision: &str) -> TaskState {
        task(
            page,
            "task-source-placeholder",
            "Codex",
            "Pending",
            revision,
        )
    }

    fn event(id: &str, page: &str, source: &str) -> Value {
        serde_json::json!({
            "id": id,
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "page.properties_updated",
            "entity": {"id": page, "type": "page"},
            "data": {"parent": {"type": "database", "data_source_id": source}}
        })
    }

    fn configuration() -> (NotionConfig, TaskValues) {
        (
            NotionConfig {
                token: "secret-placeholder".to_owned(),
                task_data_source_id: "task-source-placeholder".to_owned(),
                journal_data_source_id: "journal-placeholder".to_owned(),
            },
            TaskValues {
                codex: "Codex".to_owned(),
                pending: "Pending".to_owned(),
                running: "Running".to_owned(),
                error: "Error".to_owned(),
                done: "Done".to_owned(),
            },
        )
    }

    fn dispatcher(
        response: impl Fn(&str, usize) -> (Result<TaskState, String>, Duration) + Send + Sync + 'static,
    ) -> NotionEventDispatcher<FakeNotion, RecordingSink> {
        let (notion, values) = configuration();
        NotionEventDispatcher::new(
            FakeNotion {
                response: Arc::new(response),
                calls: AtomicUsize::new(0),
            },
            RecordingSink::default(),
            &notion,
            &values,
        )
    }

    #[tokio::test]
    async fn filters_event_source_and_authoritative_state() {
        let dispatcher = dispatcher(|page, call| {
            let state = match call {
                0 => eligible(page, "2026-01-01T00:00:00Z"),
                1 => task(
                    page,
                    "task-source-placeholder",
                    "Other",
                    "Pending",
                    "2026-01-02T00:00:00Z",
                ),
                _ => task(
                    page,
                    "task-source-placeholder",
                    "Codex",
                    "Running",
                    "2026-01-03T00:00:00Z",
                ),
            };
            (Ok(state), Duration::ZERO)
        });
        dispatcher
            .dispatch(event("unrelated", "p", "other"))
            .await
            .unwrap();
        for id in ["eligible", "other-executor", "feedback"] {
            dispatcher
                .dispatch(event(id, "p", "task-source-placeholder"))
                .await
                .unwrap();
        }
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 3);
        assert_eq!(dispatcher.inner.sink.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn same_instant_formats_suppress_and_newer_manual_retry_forwards() {
        let dispatcher = dispatcher(|page, call| {
            let revision = match call {
                0 => "2026-01-01T01:00:00+01:00",
                1 => "2026-01-01T00:00:00Z",
                _ => "2026-01-02T00:00:00Z",
            };
            (Ok(eligible(page, revision)), Duration::ZERO)
        });
        for id in ["first", "same-instant", "manual-retry"] {
            dispatcher
                .dispatch(event(id, "p", "task-source-placeholder"))
                .await
                .unwrap();
        }
        let revisions = dispatcher.inner.sink.0.lock().unwrap();
        assert_eq!(revisions.len(), 2);
        assert!(revisions[0].revision.instant() < revisions[1].revision.instant());
    }

    #[tokio::test]
    async fn concurrent_out_of_order_refetches_reach_sink_monotonically() {
        let dispatcher = Arc::new(dispatcher(|page, call| {
            if call == 0 {
                (
                    Ok(eligible(page, "2026-01-01T00:00:00Z")),
                    Duration::from_millis(80),
                )
            } else {
                (
                    Ok(eligible(page, "2026-01-02T00:00:00Z")),
                    Duration::from_millis(10),
                )
            }
        }));
        let (old, new) = tokio::join!(
            dispatcher.dispatch(event("old", "p", "task-source-placeholder")),
            dispatcher.dispatch(event("new", "p", "task-source-placeholder"))
        );
        old.unwrap();
        new.unwrap();
        let revisions = dispatcher.inner.sink.0.lock().unwrap();
        assert_eq!(revisions.len(), 1);
        assert_eq!(
            revisions[0].revision.instant(),
            TaskRevision::parse("2026-01-02T00:00:00Z")
                .unwrap()
                .instant()
        );
    }

    struct BlockingSink {
        calls: AtomicUsize,
        started: Semaphore,
        released: (StdMutex<bool>, Condvar),
    }

    impl DiscoverySink for BlockingSink {
        fn task_discovered(&self, _task: TaskState) -> EventResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.add_permits(1);
            let (released, changed) = &self.released;
            let mut released = released.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_waiter_does_not_cancel_owned_event_work() {
        let (notion, values) = configuration();
        let dispatcher = Arc::new(NotionEventDispatcher::new(
            FakeNotion {
                response: Arc::new(|page, _| {
                    (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO)
                }),
                calls: AtomicUsize::new(0),
            },
            BlockingSink {
                calls: AtomicUsize::new(0),
                started: Semaphore::new(0),
                released: (StdMutex::new(false), Condvar::new()),
            },
            &notion,
            &values,
        ));
        let signal = event("same", "p", "task-source-placeholder");
        let original_dispatcher = Arc::clone(&dispatcher);
        let original_signal = signal.clone();
        let original =
            tokio::spawn(async move { original_dispatcher.dispatch(original_signal).await });
        dispatcher
            .inner
            .sink
            .started
            .acquire()
            .await
            .unwrap()
            .forget();
        original.abort();

        let retry_dispatcher = Arc::clone(&dispatcher);
        let retry_signal = signal.clone();
        let retry = tokio::spawn(async move { retry_dispatcher.dispatch(retry_signal).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatcher.inner.sink.calls.load(Ordering::SeqCst), 1);

        let (released, changed) = &dispatcher.inner.sink.released;
        *released.lock().unwrap() = true;
        changed.notify_all();
        retry.await.unwrap().unwrap();
        dispatcher.dispatch(signal).await.unwrap();
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatcher.inner.sink.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failure_is_shared_then_later_delivery_retries() {
        let failed = Arc::new(AtomicBool::new(false));
        let failed_for_adapter = Arc::clone(&failed);
        let dispatcher = Arc::new(dispatcher(move |page, _| {
            if !failed_for_adapter.swap(true, Ordering::SeqCst) {
                (
                    Err("temporary fake failure".to_owned()),
                    Duration::from_millis(30),
                )
            } else {
                (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO)
            }
        }));
        let signal = event("same", "p", "task-source-placeholder");
        let (first, second) = tokio::join!(
            dispatcher.dispatch(signal.clone()),
            dispatcher.dispatch(signal.clone())
        );
        assert!(first.is_err());
        assert!(second.is_err());
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 1);
        dispatcher.dispatch(signal).await.unwrap();
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn panicked_worker_releases_event_for_a_later_retry() {
        let dispatcher = dispatcher(|page, call| {
            if call == 0 {
                panic!("fake adapter panic")
            }
            (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO)
        });
        let signal = event("same", "p", "task-source-placeholder");
        assert_eq!(
            dispatcher.dispatch(signal.clone()).await.unwrap_err(),
            "event supervisor stopped unexpectedly"
        );
        dispatcher.dispatch(signal).await.unwrap();
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 2);
        assert_eq!(dispatcher.inner.sink.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn caches_are_bounded_with_deterministic_revision_eviction() {
        let (notion, values) = configuration();
        let dispatcher = NotionEventDispatcher::with_limits(
            FakeNotion {
                response: Arc::new(|page, _| {
                    (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO)
                }),
                calls: AtomicUsize::new(0),
            },
            RecordingSink::default(),
            &notion,
            &values,
            2,
            2,
            2,
        );
        for (id, page) in [("e1", "a"), ("e2", "b"), ("e3", "c"), ("e4", "a")] {
            dispatcher
                .dispatch(event(id, page, "task-source-placeholder"))
                .await
                .unwrap();
        }
        assert_eq!(dispatcher.inner.sink.0.lock().unwrap().len(), 4);
        let state = dispatcher.inner.delivery.lock().await;
        assert_eq!(state.handled_event_ids.len(), 2);
        assert_eq!(state.page_revisions.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatcher_bounds_unique_work_without_blocking_duplicate_subscribers() {
        let (notion, values) = configuration();
        let dispatcher = Arc::new(NotionEventDispatcher::with_limits(
            FakeNotion {
                response: Arc::new(|page, _| {
                    (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO)
                }),
                calls: AtomicUsize::new(0),
            },
            BlockingSink {
                calls: AtomicUsize::new(0),
                started: Semaphore::new(0),
                released: (StdMutex::new(false), Condvar::new()),
            },
            &notion,
            &values,
            8,
            8,
            2,
        ));

        let mut owners = Vec::new();
        for (id, page) in [("a", "page-a"), ("b", "page-b")] {
            let dispatcher = Arc::clone(&dispatcher);
            let signal = event(id, page, "task-source-placeholder");
            owners.push(tokio::spawn(
                async move { dispatcher.dispatch(signal).await },
            ));
        }
        dispatcher
            .inner
            .sink
            .started
            .acquire_many(2)
            .await
            .unwrap()
            .forget();

        let duplicate_dispatcher = Arc::clone(&dispatcher);
        let duplicate = tokio::spawn(async move {
            duplicate_dispatcher
                .dispatch(event("a", "page-a", "task-source-placeholder"))
                .await
        });
        let canceled_dispatcher = Arc::clone(&dispatcher);
        let canceled = tokio::spawn(async move {
            canceled_dispatcher
                .dispatch(event("c", "page-c", "task-source-placeholder"))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());

        {
            let state = dispatcher.inner.delivery.lock().await;
            assert_eq!(state.in_flight_events.len(), 2);
            assert!(!state.in_flight_events.contains_key("c"));
            assert_eq!(state.page_gates.len(), 2);
            assert!(!state.page_gates.contains_key("page-c"));
            assert_eq!(
                state
                    .page_gates
                    .values()
                    .map(|gate| gate.users.load(Ordering::SeqCst))
                    .sum::<usize>(),
                2
            );
        }
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 2);
        assert_eq!(dispatcher.inner.sink.calls.load(Ordering::SeqCst), 2);
        assert_eq!(dispatcher.inner.work_permits.available_permits(), 0);

        let (released, changed) = &dispatcher.inner.sink.released;
        *released.lock().unwrap() = true;
        changed.notify_all();
        for owner in owners {
            owner.await.unwrap().unwrap();
        }
        duplicate.await.unwrap().unwrap();
        assert_eq!(dispatcher.inner.work_permits.available_permits(), 2);

        dispatcher
            .dispatch(event("d", "page-d", "task-source-placeholder"))
            .await
            .unwrap();
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 3);
        assert_eq!(dispatcher.inner.sink.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_event_admission_race_releases_loser_permit_immediately() {
        let (notion, values) = configuration();
        let mut dispatcher = NotionEventDispatcher::with_limits(
            FakeNotion {
                response: Arc::new(|page, _| {
                    (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO)
                }),
                calls: AtomicUsize::new(0),
            },
            BlockingSink {
                calls: AtomicUsize::new(0),
                started: Semaphore::new(0),
                released: (StdMutex::new(false), Condvar::new()),
            },
            &notion,
            &values,
            8,
            8,
            2,
        );
        Arc::get_mut(&mut dispatcher.inner).unwrap().admission_hook =
            Some(Arc::new(AdmissionHook {
                barrier: tokio::sync::Barrier::new(2),
                remaining: AtomicUsize::new(2),
            }));
        let dispatcher = Arc::new(dispatcher);

        let mut duplicates = Vec::new();
        for _ in 0..2 {
            let dispatcher = Arc::clone(&dispatcher);
            duplicates.push(tokio::spawn(async move {
                dispatcher
                    .dispatch(event("same", "page-a", "task-source-placeholder"))
                    .await
            }));
        }
        dispatcher
            .inner
            .sink
            .started
            .acquire()
            .await
            .unwrap()
            .forget();

        let unrelated_dispatcher = Arc::clone(&dispatcher);
        let unrelated = tokio::spawn(async move {
            unrelated_dispatcher
                .dispatch(event("other", "page-b", "task-source-placeholder"))
                .await
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            dispatcher.inner.sink.started.acquire(),
        )
        .await
        .expect("race loser must release admission for unrelated work")
        .unwrap()
        .forget();

        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 2);
        assert_eq!(dispatcher.inner.sink.calls.load(Ordering::SeqCst), 2);
        assert_eq!(dispatcher.inner.work_permits.available_permits(), 0);
        assert_eq!(
            dispatcher
                .inner
                .delivery
                .lock()
                .await
                .in_flight_events
                .len(),
            2
        );

        let (released, changed) = &dispatcher.inner.sink.released;
        *released.lock().unwrap() = true;
        changed.notify_all();
        for duplicate in duplicates {
            duplicate.await.unwrap().unwrap();
        }
        unrelated.await.unwrap().unwrap();
        assert_eq!(dispatcher.inner.work_permits.available_permits(), 2);
    }

    #[tokio::test]
    async fn oversized_or_malformed_identity_fields_are_ignored() {
        let dispatcher =
            dispatcher(|page, _| (Ok(eligible(page, "2026-01-01T00:00:00Z")), Duration::ZERO));
        let mut malformed = event("x", "p", "task-source-placeholder");
        malformed["timestamp"] = Value::String("not-a-time".to_owned());
        dispatcher.dispatch(malformed).await.unwrap();
        dispatcher
            .dispatch(event(&"x".repeat(129), "p", "task-source-placeholder"))
            .await
            .unwrap();
        assert_eq!(dispatcher.inner.adapter.calls.load(Ordering::SeqCst), 0);
    }
}
