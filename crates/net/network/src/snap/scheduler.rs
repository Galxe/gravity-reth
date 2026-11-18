//! Bounded blocking execution of standalone SNAP state-range requests.

use super::{SnapRequestError, SnapRequestHandler};
use reth_eth_wire_types::snap::{
    GetAccountRangeMessage, GetStorageRangesMessage, SnapProtocolMessage,
};
use reth_metrics::metrics::Counter;
use reth_network_p2p::snap::client::SnapResponse;
use reth_network_peers::PeerId;
use reth_storage_api::{BalProvider, BytecodeReader, StateRangeProviderFactory};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep_until, Instant, Sleep},
};
use tracing::{debug, error, Span};

/// A manually driven scheduler for an explicitly injected SNAP responder.
///
/// At most two blocking jobs run, one per peer. At most sixteen requests wait, with one queued
/// request per peer, for at most 500ms. An active peer may also hold one queued request.
/// [`Self::admit`] polls before and after admission; the owner must keep calling [`Self::poll`]
/// while work remains to drive completions and queue deadlines. No network wiring is installed.
///
/// Dropping a response receiver cancels queued work. Already-running blocking work retains its
/// slot until it finishes; the queue deadline is not an execution timeout. Dropping this scheduler
/// closes queued responses but does not interrupt blocking jobs already running on Tokio's pool.
/// Peer departure is signaled by dropping the corresponding response receivers; this standalone
/// component does not maintain a network peer registry.
#[derive(Debug)]
pub struct StateRangeRequests<C> {
    handler: Arc<SnapRequestHandler<C>>,
    tasks: Vec<(PeerId, JoinHandle<()>)>,
    queue: VecDeque<IncomingStateRangeRequest>,
    queue_timeout: Option<Pin<Box<Sleep>>>,
    unserved: Counter,
}

impl<C> StateRangeRequests<C> {
    /// Creates a scheduler for the given real backend, without starting a driver task.
    pub fn new(handler: SnapRequestHandler<C>) -> Self {
        Self {
            handler: Arc::new(handler),
            tasks: Vec::new(),
            queue: VecDeque::new(),
            queue_timeout: None,
            unserved: reth_metrics::metrics::counter!(
                "network.snap_state_range_requests_unserved_total"
            ),
        }
    }

    /// Returns the number of held slots. Finished jobs release their slot on the next poll.
    pub const fn active_len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the number of requests waiting for a slot.
    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }

    /// Returns the injected responder.
    pub fn handler(&self) -> &SnapRequestHandler<C> {
        &self.handler
    }
}

impl<C> StateRangeRequests<C>
where
    C: BalProvider + BytecodeReader + StateRangeProviderFactory + 'static,
{
    /// Starts or queues a request, registering the driver's waker for completion and expiry.
    ///
    /// Finished work and eligible queued requests are processed before the new admission. Closed
    /// consumers are discarded; full queues and duplicate queued peers receive a typed busy error.
    pub fn admit(
        &mut self,
        cx: &mut Context<'_>,
        peer_id: PeerId,
        request: StateRangeRequest,
        response: oneshot::Sender<Result<SnapResponse, StateRangeRequestError>>,
    ) {
        self.poll(cx);
        self.admit_inner(IncomingStateRangeRequest {
            peer_id,
            request,
            response,
            received: Instant::now(),
        });
        self.poll(cx);
    }

    fn admit_inner(&mut self, incoming: IncomingStateRangeRequest) {
        if incoming.response.is_closed() {
            return
        }
        if self.can_start(incoming.peer_id) {
            return self.spawn(incoming)
        }
        self.remove_stale();
        if self.queue.len() >= MAX_QUEUED_STATE_RANGE_REQUESTS ||
            self.queue.iter().any(|queued| queued.peer_id == incoming.peer_id)
        {
            debug!(target: "net::snap", peer_id=?incoming.peer_id, queued=self.queue.len(), "no room to queue state range request");
            self.unserved.increment(1);
            let _ = incoming.response.send(Err(StateRangeRequestError::Busy));
            return
        }
        self.queue.push_back(incoming);
    }

    fn can_start(&self, peer_id: PeerId) -> bool {
        self.tasks.len() < MAX_CONCURRENT_STATE_RANGE_REQUESTS &&
            self.tasks.iter().all(|(running, _)| *running != peer_id)
    }

    fn spawn(&mut self, incoming: IncomingStateRangeRequest) {
        let IncomingStateRangeRequest { peer_id, request, response, .. } = incoming;
        let handler = self.handler.clone();
        let span = Span::current();
        let task = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let result = handler.serve(request.into()).map_err(StateRangeRequestError::Serve);
            let _ = response.send(result);
        });
        self.tasks.push((peer_id, task));
    }

    fn remove_stale(&mut self) {
        let mut index = 0;
        while index < self.queue.len() {
            let queued = &self.queue[index];
            if queued.response.is_closed() {
                let _ = self.queue.remove(index);
            } else if queued.received.elapsed() < STATE_RANGE_REQUEST_QUEUE_TIMEOUT {
                index += 1;
            } else if let Some(incoming) = self.queue.remove(index) {
                debug!(target: "net::snap", peer_id=?incoming.peer_id, "state range request timed out in queue");
                self.unserved.increment(1);
                let _ = incoming.response.send(Err(StateRangeRequestError::QueueTimeout));
            }
        }
    }

    /// Reaps finished jobs, starts eligible waiting work and arms the oldest queue deadline.
    ///
    /// A queued request whose peer is already active is passed over. The driver's waker is
    /// registered on all held jobs and the queue timer, including jobs started by this poll.
    pub fn poll(&mut self, cx: &mut Context<'_>) {
        loop {
            self.remove_stale();
            self.tasks.retain_mut(|(_, task)| match Pin::new(task).poll(cx) {
                Poll::Pending => true,
                Poll::Ready(result) => {
                    if let Err(error) = result {
                        error!(target: "net::snap", %error, "state range task failed");
                    }
                    false
                }
            });
            let mut spawned = false;
            let mut index = 0;
            while index < self.queue.len() && self.tasks.len() < MAX_CONCURRENT_STATE_RANGE_REQUESTS
            {
                if !self.can_start(self.queue[index].peer_id) {
                    index += 1;
                } else if let Some(incoming) = self.queue.remove(index) {
                    self.spawn(incoming);
                    spawned = true;
                }
            }
            if !spawned && !self.poll_queue_timeout(cx) {
                break
            }
        }
    }

    fn poll_queue_timeout(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(oldest) = self.queue.front() else {
            self.queue_timeout = None;
            return false
        };
        let deadline = oldest.received + STATE_RANGE_REQUEST_QUEUE_TIMEOUT;
        let timeout = self.queue_timeout.get_or_insert_with(|| Box::pin(sleep_until(deadline)));
        timeout.as_mut().reset(deadline);
        timeout.as_mut().poll(cx).is_ready()
    }
}

/// A native SNAP account or storage range request. Bytecode and BAL reads are separate operations.
#[derive(Clone, Debug)]
pub enum StateRangeRequest {
    /// Account range at a pinned state root.
    Account(GetAccountRangeMessage),
    /// Storage ranges at a pinned state root.
    Storage(GetStorageRangesMessage),
}

impl From<StateRangeRequest> for SnapProtocolMessage {
    fn from(request: StateRangeRequest) -> Self {
        match request {
            StateRangeRequest::Account(request) => Self::GetAccountRange(request),
            StateRangeRequest::Storage(request) => Self::GetStorageRanges(request),
        }
    }
}

/// Library scheduling failures, kept distinct from successful empty range responses.
///
/// A future network adapter must map unavailable/busy/expired requests to the native protocol's
/// response semantics; this library does not currently attach to sessions or send wire responses.
#[derive(Debug, thiserror::Error)]
pub enum StateRangeRequestError {
    /// The queue is full or this peer already has a queued request.
    #[error("SNAP state range queue is busy")]
    Busy,
    /// The request waited 500ms without obtaining a slot.
    #[error("SNAP state range request timed out in queue")]
    QueueTimeout,
    /// The injected backend could not produce a response.
    #[error(transparent)]
    Serve(#[from] SnapRequestError),
}

/// Maximum number of simultaneously held blocking task slots.
pub const MAX_CONCURRENT_STATE_RANGE_REQUESTS: usize = 2;
/// Maximum number of queued range requests.
pub const MAX_QUEUED_STATE_RANGE_REQUESTS: usize = 16;
/// Maximum waiting time for a queued request, excluding backend execution time.
pub const STATE_RANGE_REQUEST_QUEUE_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug)]
struct IncomingStateRangeRequest {
    peer_id: PeerId,
    request: StateRangeRequest,
    response: oneshot::Sender<Result<SnapResponse, StateRangeRequestError>>,
    received: Instant,
}

#[cfg(test)]
mod tests;
