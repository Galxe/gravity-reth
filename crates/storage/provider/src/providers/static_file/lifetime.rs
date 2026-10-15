use alloy_primitives::BlockNumber;
use parking_lot::{Condvar, Mutex};
use reth_static_file_types::StaticFileSegment;
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
};

/// Keeps static files available to a state snapshot captured after acquiring this lease.
///
/// Clones share a registration. The last clone releases it without performing file I/O.
#[derive(Clone, Debug)]
pub struct StaticFileReadLease {
    _inner: Arc<ReadLeaseInner>,
}

/// Excludes state reads and history collection while storage is being rewritten in place.
///
/// The guard owns no mutex guard and can be held across the pipeline's asynchronous stage work.
/// Call [`Self::complete`] only after every rewrite commits successfully.
/// Otherwise new state reads remain unavailable until restart.
#[derive(Debug)]
pub struct StaticFileMaintenanceGuard {
    lifetime: Arc<StaticFileReadLifetime>,
    completed: bool,
}

impl StaticFileMaintenanceGuard {
    /// Marks all storage rewrites as successfully committed before reopening read admission.
    pub fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for StaticFileMaintenanceGuard {
    fn drop(&mut self) {
        let mut state = self.lifetime.state.lock();
        // A later successful no-op cannot repair data changed by an earlier failed rewrite.
        state.maintenance_failed |= !self.completed;
        state.maintenance = false;
        drop(state);
        self.lifetime.changed.notify_all();
    }
}

#[derive(Debug)]
struct ReadLeaseInner {
    lifetime: Arc<StaticFileReadLifetime>,
    epoch: u64,
}

impl Drop for ReadLeaseInner {
    fn drop(&mut self) {
        let mut state = self.lifetime.state.lock();
        let readers = state.active.get_mut(&self.epoch).expect("registered read epoch");
        *readers -= 1;
        if *readers == 0 {
            state.active.remove(&self.epoch);
        }
        if state.maintenance && state.active.is_empty() {
            self.lifetime.changed.notify_all();
        }
    }
}

/// Serializes short reader registrations independently of database commits and file I/O.
#[derive(Default)]
pub(super) struct StaticFileReadLifetime {
    state: Mutex<LifetimeState>,
    changed: Condvar,
    /// Serializes collectors; maintenance drains it after closing admission. Readers never take
    /// it.
    pub(super) collection: Mutex<()>,
}

impl fmt::Debug for StaticFileReadLifetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticFileReadLifetime").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct LifetimeState {
    epoch: u64,
    active: BTreeMap<u64, usize>,
    retired: VecDeque<RetiredHistory>,
    account_before: Option<BlockNumber>,
    storage_before: Option<BlockNumber>,
    maintenance: bool,
    maintenance_failed: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RetiredHistory {
    epoch: u64,
    pub(super) segment: StaticFileSegment,
    pub(super) before: BlockNumber,
}

impl StaticFileReadLifetime {
    pub(super) fn read_lease(self: &Arc<Self>) -> ProviderResult<StaticFileReadLease> {
        let epoch = {
            let mut state = self.state.lock();
            if state.maintenance {
                return Err(ProviderError::other(std::io::Error::other(
                    "State reads unavailable during storage maintenance",
                )))
            }
            if state.maintenance_failed {
                return Err(ProviderError::other(std::io::Error::other(
                    "State reads unavailable after failed storage maintenance",
                )))
            }
            let epoch = state.epoch;
            *state.active.entry(epoch).or_default() += 1;
            epoch
        };
        Ok(StaticFileReadLease {
            _inner: Arc::new(ReadLeaseInner { lifetime: self.clone(), epoch }),
        })
    }

    pub(super) fn begin_maintenance(self: &Arc<Self>) -> StaticFileMaintenanceGuard {
        {
            let mut state = self.state.lock();
            while state.maintenance {
                self.changed.wait(&mut state);
            }
            state.maintenance = true;
            self.changed.notify_all();
            while !state.active.is_empty() {
                self.changed.wait(&mut state);
            }
        }
        // A collector that started before admission closed may still be doing file I/O.
        // Never wait for it while holding the reader-registration mutex.
        drop(self.collection.lock());
        StaticFileMaintenanceGuard { lifetime: self.clone(), completed: false }
    }

    /// Must run after the database commit that made these prune horizons visible.
    pub(super) fn retire_after_commit(
        &self,
        account_before: Option<BlockNumber>,
        storage_before: Option<BlockNumber>,
    ) {
        {
            let mut state = self.state.lock();
            let mut changed = false;
            for (segment, before) in [
                (StaticFileSegment::AccountChangeSets, account_before),
                (StaticFileSegment::StorageChangeSets, storage_before),
            ] {
                let Some(before) = before.filter(|before| *before != 0) else { continue };
                let previous = match segment {
                    StaticFileSegment::AccountChangeSets => &mut state.account_before,
                    _ => &mut state.storage_before,
                };
                if previous.is_some_and(|previous| previous >= before) {
                    continue
                }
                *previous = Some(before);
                let epoch = state.epoch;
                state.retired.push_back(RetiredHistory { epoch, segment, before });
                changed = true;
            }
            if changed {
                state.epoch = state.epoch.checked_add(1).expect("static file epoch overflow");
            }
        }
    }

    pub(super) fn ready_history(&self) -> Vec<RetiredHistory> {
        let state = self.state.lock();
        if state.maintenance {
            return Vec::new()
        }
        let oldest = state.active.first_key_value().map(|(epoch, _)| *epoch);
        state
            .retired
            .iter()
            .take_while(|retired| oldest.is_none_or(|oldest| oldest > retired.epoch))
            .copied()
            .collect()
    }

    /// A failed collection leaves the retirement queued for the next attempt.
    pub(super) fn collected(&self, count: usize) {
        drop(self.state.lock().retired.drain(..count));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_maintenance_keeps_read_admission_closed() {
        let lifetime = Arc::new(StaticFileReadLifetime::default());
        lifetime.begin_maintenance().complete();
        assert!(lifetime.read_lease().is_ok());

        // Returning early or unwinding a panic drops the guard without declaring success.
        drop(lifetime.begin_maintenance());
        assert!(lifetime.read_lease().is_err());

        lifetime.begin_maintenance().complete();
        assert!(lifetime.read_lease().is_err());
    }
}
