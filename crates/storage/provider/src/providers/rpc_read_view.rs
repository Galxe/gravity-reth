//! Published storage views and reader lifetime during destructive maintenance.

use alloy_primitives::B256;
use parking_lot::{Condvar, Mutex};
use reth_db_api::database::DatabaseReadView;
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use std::{fmt, sync::Arc};

/// Ordinary writes replace this view without waiting for RPC readers. Maintenance closes
/// admission first and drains leases before modifying data outside `RocksDB` snapshots.
#[derive(Default)]
pub(crate) struct RpcViewManager {
    state: Mutex<ViewState>,
    changed: Condvar,
}

impl fmt::Debug for RpcViewManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("RpcViewManager")
            .field("status", &state.status)
            .field("readers", &state.readers)
            .field("bounds", &state.current.as_ref().map(|view| view.bounds))
            .finish()
    }
}

impl RpcViewManager {
    pub(crate) fn new(replay_floor: u64) -> Self {
        Self {
            state: Mutex::new(ViewState {
                replay_floor,
                needs_validation: replay_floor > 0,
                ..Default::default()
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn lease(self: &Arc<Self>) -> ProviderResult<Arc<ReadLease>> {
        let mut state = self.state.lock();
        if state.status != Status::Ready || state.maintenance {
            return Err(unavailable());
        }
        state.readers += 1;
        Ok(Arc::new(ReadLease { manager: Arc::clone(self) }))
    }

    pub(crate) fn view(
        self: &Arc<Self>,
        lease: &Arc<ReadLease>,
    ) -> ProviderResult<Arc<PublishedRpcView>> {
        if !Arc::ptr_eq(self, &lease.manager) {
            return Err(ProviderError::other(std::io::Error::other(
                "read lease belongs to another provider factory",
            )));
        }
        // A lease acquired before closure may still open its view while maintenance waits.
        self.state.lock().current.clone().ok_or_else(unavailable)
    }

    pub(crate) fn requires_validation(&self, block_number: u64) -> bool {
        let state = self.state.lock();
        state.needs_validation && block_number >= state.replay_floor
    }

    pub(crate) fn publish(&self, view: PublishedRpcView, verified: bool) {
        let mut state = self.state.lock();
        state.generation += 1;
        state.published_generation = state.generation;
        if !verified || view.bounds.block_number < state.replay_floor {
            let previous = state.current.take();
            if state.status == Status::Ready {
                state.status = Status::Unpublished;
            }
            drop(state);
            drop(previous);
            return;
        }
        let previous = state.current.replace(Arc::new(view));
        // The startup watermark protects interrupted replay only. Later canonical unwinds may
        // publish a lower height once old readers have drained.
        state.replay_floor = 0;
        state.needs_validation = false;
        if state.status == Status::Unpublished {
            state.status = Status::Ready;
        }
        // An ordinary publication cannot reopen maintenance or an interrupted write.
        drop(state);
        drop(previous);
    }

    pub(crate) fn interrupt(&self, replay_floor: u64) {
        let mut state = self.state.lock();
        state.status = Status::Interrupted;
        state.generation += 1;
        state.replay_floor = state.replay_floor.max(replay_floor);
        // Destructive maintenance can already have removed the old body tail before failing.
        // Recheck actual trie contents even when the remaining metadata cannot bound the write.
        state.needs_validation = true;
        // Already-open transactions own their snapshots. A lease without a transaction fails
        // safely instead of pairing its captured memory with an uncertain replacement.
        let previous = state.current.take();
        drop(state);
        drop(previous);
        self.changed.notify_all();
    }

    pub(crate) fn publication_failed(&self) {
        let mut state = self.state.lock();
        state.generation += 1;
        // This is a complete publication of an unavailable view. Successful storage maintenance
        // may end while RPC stays closed, without falsely marking the storage write interrupted.
        // A publication failure cannot certify recovery of an actual interrupted write.
        if state.status != Status::Interrupted {
            state.published_generation = state.generation;
        }
        state.needs_validation = true;
        if state.status == Status::Ready {
            state.status = Status::Unpublished;
        }
        let previous = state.current.take();
        drop(state);
        drop(previous);
        self.changed.notify_all();
    }

    pub(crate) fn maintenance(self: &Arc<Self>, wait: bool) -> Option<MaintenanceGuard> {
        let mut state = self.state.lock();
        while state.maintenance {
            if !wait {
                return None;
            }
            self.changed.wait(&mut state);
        }
        if !wait && state.readers != 0 {
            return None;
        }
        state.maintenance = true;
        state.status = Status::Maintenance;
        self.changed.notify_all();
        while state.readers != 0 {
            self.changed.wait(&mut state);
        }
        Some(MaintenanceGuard {
            manager: Arc::clone(self),
            generation: state.generation,
            complete: false,
        })
    }
}

/// Complete persisted block and exclusive transaction bound of a provider read view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcReadViewBounds {
    /// Last complete persisted block.
    pub block_number: u64,
    /// Canonical hash at the persisted block.
    pub block_hash: B256,
    /// First transaction number outside the view.
    pub next_tx_num: u64,
}

pub(crate) struct PublishedRpcView {
    pub(crate) bounds: RpcReadViewBounds,
    pub(crate) storage: Arc<dyn DatabaseReadView>,
}

#[derive(Default)]
struct ViewState {
    current: Option<Arc<PublishedRpcView>>,
    status: Status,
    readers: usize,
    generation: u64,
    published_generation: u64,
    maintenance: bool,
    replay_floor: u64,
    needs_validation: bool,
}

#[derive(Default, Debug, PartialEq, Eq)]
enum Status {
    #[default]
    Unpublished,
    Ready,
    Maintenance,
    Interrupted,
}

#[derive(Debug)]
pub(crate) struct ReadLease {
    manager: Arc<RpcViewManager>,
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        let mut state = self.manager.state.lock();
        state.readers -= 1;
        if state.readers == 0 {
            self.manager.changed.notify_all();
        }
    }
}

/// Holds external-reader admission closed during destructive storage maintenance.
#[derive(Debug)]
pub struct MaintenanceGuard {
    manager: Arc<RpcViewManager>,
    generation: u64,
    complete: bool,
}

impl MaintenanceGuard {
    /// Maintenance committed and published its replacement view.
    pub fn complete(&mut self) {
        let mut state = self.manager.state.lock();
        // A stage may catch an interrupted write and retry inside this same maintenance range.
        // Only a verified publication after the last interruption can make the range usable.
        if state.maintenance &&
            state.published_generation > self.generation &&
            state.published_generation == state.generation
        {
            state.status =
                if state.current.is_some() { Status::Ready } else { Status::Unpublished };
            self.complete = true;
            self.manager.changed.notify_all();
        }
    }
}

impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        let mut state = self.manager.state.lock();
        state.maintenance = false;
        if !self.complete {
            state.status = Status::Interrupted;
            state.generation += 1;
            state.needs_validation = true;
            let previous = state.current.take();
            drop(state);
            drop(previous);
        }
        self.manager.changed.notify_all();
    }
}

fn unavailable() -> ProviderError {
    ProviderError::other(std::io::Error::other(
        "complete RPC storage view is unavailable during startup or recovery",
    ))
}

#[cfg(test)]
#[path = "rpc_read_view_manager_tests.rs"]
mod tests;
