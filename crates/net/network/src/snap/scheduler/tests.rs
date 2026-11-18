use super::*;
use crate::snap::tests::{state_with_accounts, Fixture};
use alloy_primitives::B256;
use futures::{future::poll_fn, task::noop_waker_ref};
use reth_primitives_traits::Bytecode;
use reth_storage_api::{errors::provider::ProviderError, BalStoreHandle, StateRangeView};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar, Mutex,
    },
    thread::ThreadId,
};
use tokio::sync::mpsc;

type Scheduler = StateRangeRequests<Arc<Fixture>>;
type Response = oneshot::Receiver<Result<SnapResponse, StateRangeRequestError>>;

fn scheduler() -> Scheduler {
    StateRangeRequests::new(SnapRequestHandler::new(Arc::new(Fixture::new(state_with_accounts()))))
}

fn account_request(scheduler: &Scheduler) -> StateRangeRequest {
    StateRangeRequest::Account(scheduler.handler.provider().account_request())
}

fn admit<C>(
    scheduler: &mut StateRangeRequests<C>,
    peer: PeerId,
    request: StateRangeRequest,
) -> Response
where
    C: BalProvider + BytecodeReader + StateRangeProviderFactory + 'static,
{
    let (tx, rx) = oneshot::channel();
    scheduler.admit(&mut Context::from_waker(noop_waker_ref()), peer, request, tx);
    rx
}

fn occupy(scheduler: &mut Scheduler, peer: PeerId) {
    scheduler.tasks.push((peer, tokio::spawn(futures::future::pending())));
}

async fn receive<C>(scheduler: &mut StateRangeRequests<C>, mut rx: Response) -> SnapResponse
where
    C: BalProvider + BytecodeReader + StateRangeProviderFactory + 'static,
{
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            scheduler.poll(cx);
            Pin::new(&mut rx).poll(cx)
        }),
    )
    .await
    .expect("scheduler must wake its driver")
    .unwrap()
    .unwrap()
}

async fn drain<C>(scheduler: &mut StateRangeRequests<C>)
where
    C: BalProvider + BytecodeReader + StateRangeProviderFactory + 'static,
{
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            scheduler.poll(cx);
            if scheduler.active_len() == 0 && scheduler.queued_len() == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("completed work must release its slots");
}

#[tokio::test(start_paused = true)]
async fn busy_state_range_request_waits_for_a_slot() {
    let mut scheduler = scheduler();
    let first = PeerId::default();
    let busy = PeerId::repeat_byte(1);
    let (release, released) = oneshot::channel::<()>();
    scheduler.tasks.push((
        first,
        tokio::spawn(async move {
            let _ = released.await;
        }),
    ));
    occupy(&mut scheduler, busy);
    let request = account_request(&scheduler);
    let mut passed_over = admit(&mut scheduler, busy, request.clone());
    let rx = admit(&mut scheduler, first, request);
    assert_eq!(scheduler.queued_len(), 2);
    assert_eq!(scheduler.handler.provider().resolutions.load(Ordering::Relaxed), 0);
    release.send(()).unwrap();
    let response = receive(&mut scheduler, rx).await;
    assert!(
        matches!(response, SnapResponse::AccountRange(ref response) if response.accounts.len() == 2)
    );
    // The blocking task sends its result before its JoinHandle becomes ready; keep polling that
    // handle so a wake after sending the response is also required for this assertion.
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            scheduler.poll(cx);
            if scheduler.active_len() < MAX_CONCURRENT_STATE_RANGE_REQUESTS {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    assert_eq!(scheduler.queued_len(), 1);
    assert!(matches!(passed_over.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
    assert_eq!(scheduler.handler.provider().resolutions.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn stale_queued_state_range_requests_are_not_served() {
    let mut scheduler = scheduler();
    for byte in 0..MAX_CONCURRENT_STATE_RANGE_REQUESTS as u8 {
        occupy(&mut scheduler, PeerId::repeat_byte(byte));
    }
    let request = account_request(&scheduler);
    let gone = admit(&mut scheduler, PeerId::repeat_byte(0x80), request.clone());
    drop(gone);
    let (tx, mut expired) = oneshot::channel();
    scheduler.queue.push_back(IncomingStateRangeRequest {
        peer_id: PeerId::repeat_byte(0x81),
        request,
        response: tx,
        received: Instant::now() + Duration::from_millis(50) - STATE_RANGE_REQUEST_QUEUE_TIMEOUT,
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            scheduler.poll(cx);
            Pin::new(&mut expired).poll(cx)
        }),
    )
    .await
    .expect("the queue deadline must wake the driver")
    .unwrap();
    assert!(matches!(result, Err(StateRangeRequestError::QueueTimeout)));
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.handler.provider().resolutions.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn incoming_state_range_request_sees_slots_freed_since_the_last_poll() {
    let mut scheduler = scheduler();
    let peer = PeerId::default();
    let finished = tokio::spawn(async {});
    while !finished.is_finished() {
        tokio::task::yield_now().await;
    }
    scheduler.tasks.push((peer, finished));
    occupy(&mut scheduler, PeerId::repeat_byte(1));
    let request = account_request(&scheduler);
    let (tx, old) = oneshot::channel();
    // The first request was queued before the completed task was observed by the driver.
    scheduler.queue.push_back(IncomingStateRangeRequest {
        peer_id: peer,
        request: request.clone(),
        response: tx,
        received: Instant::now(),
    });
    let new = admit(&mut scheduler, peer, request);
    let first = receive(&mut scheduler, old).await;
    let second = receive(&mut scheduler, new).await;
    assert!(matches!(first, SnapResponse::AccountRange(_)));
    assert!(matches!(second, SnapResponse::AccountRange(_)));
    assert_eq!(scheduler.handler.provider().resolutions.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn overflowing_state_range_requests_report_busy() {
    let mut scheduler = scheduler();
    for byte in 0..MAX_CONCURRENT_STATE_RANGE_REQUESTS as u8 {
        occupy(&mut scheduler, PeerId::repeat_byte(byte));
    }
    let request = account_request(&scheduler);
    let queued_peer = |index| PeerId::repeat_byte(0x80 + index as u8);
    let mut waiting = vec![admit(&mut scheduler, queued_peer(0), request.clone())];
    let duplicate = admit(&mut scheduler, queued_peer(0), request.clone());
    assert!(matches!(duplicate.await.unwrap(), Err(StateRangeRequestError::Busy)));
    for index in 1..MAX_QUEUED_STATE_RANGE_REQUESTS {
        waiting.push(admit(&mut scheduler, queued_peer(index), request.clone()));
    }
    let overflow = admit(&mut scheduler, PeerId::repeat_byte(0xff), request);
    assert!(matches!(overflow.await.unwrap(), Err(StateRangeRequestError::Busy)));
    assert_eq!(scheduler.queued_len(), MAX_QUEUED_STATE_RANGE_REQUESTS);
    assert!(waiting
        .iter_mut()
        .all(|rx| matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty))));
    assert_eq!(scheduler.handler.provider().resolutions.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn state_range_request_from_gone_peer_is_dropped() {
    let mut scheduler = scheduler();
    let request = account_request(&scheduler);
    let (tx, rx) = oneshot::channel();
    drop(rx);
    scheduler.admit(&mut Context::from_waker(noop_waker_ref()), PeerId::default(), request, tx);
    assert_eq!(scheduler.active_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.handler.provider().resolutions.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn finished_state_range_request_frees_its_peer_slot() {
    let mut scheduler = scheduler();
    let request = account_request(&scheduler);
    let rx = admit(&mut scheduler, PeerId::default(), request);
    let response = receive(&mut scheduler, rx).await;
    assert!(matches!(response, SnapResponse::AccountRange(_)));
    drain(&mut scheduler).await;
    assert_eq!(scheduler.active_len(), 0);
}

#[tokio::test(start_paused = true)]
async fn dropping_scheduler_closes_waiting_responses() {
    let mut scheduler = scheduler();
    for byte in 0..MAX_CONCURRENT_STATE_RANGE_REQUESTS as u8 {
        occupy(&mut scheduler, PeerId::repeat_byte(byte));
    }
    let request = account_request(&scheduler);
    let queued = admit(&mut scheduler, PeerId::repeat_byte(0x80), request);
    assert_eq!(scheduler.queued_len(), 1);
    drop(scheduler);
    assert!(queued.await.is_err());
}

#[derive(Debug)]
struct Gate {
    permits: Mutex<(usize, bool)>,
    wake: Condvar,
    started: mpsc::UnboundedSender<()>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    driver: ThreadId,
}

impl Gate {
    fn enter(&self) {
        assert_ne!(
            std::thread::current().id(),
            self.driver,
            "state resolution must leave the async driver thread"
        );
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let _ = self.started.send(());
        let mut state = self.permits.lock().unwrap();
        while state.0 == 0 && !state.1 {
            state = self.wake.wait(state).unwrap();
        }
        if state.0 > 0 {
            state.0 -= 1;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    fn release_one(&self) {
        self.permits.lock().unwrap().0 += 1;
        self.wake.notify_all();
    }

    fn release_all(&self) {
        self.permits.lock().unwrap().1 = true;
        self.wake.notify_all();
    }
}

struct ReleaseOnDrop(Arc<Gate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release_all();
    }
}

#[derive(Debug)]
struct BlockingFixture {
    inner: Arc<Fixture>,
    gate: Arc<Gate>,
}

impl BalProvider for BlockingFixture {
    fn bal_store(&self) -> &BalStoreHandle {
        self.inner.bal_store()
    }
}

impl BytecodeReader for BlockingFixture {
    fn bytecode_by_hash(&self, hash: &B256) -> Result<Option<Bytecode>, ProviderError> {
        self.inner.bytecode_by_hash(hash)
    }
}

impl StateRangeProviderFactory for BlockingFixture {
    fn state_range_provider(&self, root: B256) -> Result<Option<StateRangeView>, ProviderError> {
        self.gate.enter();
        self.inner.state_range_provider(root)
    }
}

#[tokio::test]
async fn real_blocking_factory_preserves_slots_on_cancellation_and_serves_queued_work() {
    let inner = Arc::new(Fixture::new(state_with_accounts()));
    let request = StateRangeRequest::Account(inner.account_request());
    let (started, mut starts) = mpsc::unbounded_channel();
    let gate = Arc::new(Gate {
        permits: Mutex::new((0, false)),
        wake: Condvar::new(),
        started,
        active: AtomicUsize::new(0),
        max_active: AtomicUsize::new(0),
        driver: std::thread::current().id(),
    });
    let _release = ReleaseOnDrop(gate.clone());
    let handler =
        SnapRequestHandler::new(BlockingFixture { inner: inner.clone(), gate: gate.clone() });
    let mut scheduler = StateRangeRequests::new(handler);
    let first = admit(&mut scheduler, PeerId::repeat_byte(1), request.clone());
    let second = admit(&mut scheduler, PeerId::repeat_byte(2), request.clone());
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), starts.recv()).await.unwrap().unwrap();
    }
    assert_eq!(gate.active.load(Ordering::SeqCst), 2);
    assert!(starts.try_recv().is_err());
    drop(first);
    assert_eq!(
        scheduler.active_len(),
        2,
        "canceling an active consumer must not release a running job's slot"
    );
    let third = admit(&mut scheduler, PeerId::repeat_byte(3), request.clone());
    let departed = admit(&mut scheduler, PeerId::repeat_byte(1), request.clone());
    let duplicate = admit(&mut scheduler, PeerId::repeat_byte(1), request);
    assert!(matches!(duplicate.await.unwrap(), Err(StateRangeRequestError::Busy)));
    drop(departed);
    scheduler.poll(&mut Context::from_waker(noop_waker_ref()));
    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(inner.resolutions.load(Ordering::Relaxed), 0);
    gate.release_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            scheduler.poll(cx);
            starts.poll_recv(cx)
        }),
    )
    .await
    .expect("a freed slot must start queued real work")
    .unwrap();
    assert!(gate.max_active.load(Ordering::SeqCst) <= MAX_CONCURRENT_STATE_RANGE_REQUESTS);
    gate.release_all();
    let second = receive(&mut scheduler, second).await;
    let third = receive(&mut scheduler, third).await;
    assert!(
        matches!(second, SnapResponse::AccountRange(ref response) if response.accounts.len() == 2 && !response.proof.is_empty())
    );
    assert!(
        matches!(third, SnapResponse::AccountRange(ref response) if response.accounts.len() == 2 && !response.proof.is_empty())
    );
    drain(&mut scheduler).await;
    assert_eq!(
        inner.resolutions.load(Ordering::Relaxed),
        3,
        "canceled waiting request must never read the backend"
    );
}
