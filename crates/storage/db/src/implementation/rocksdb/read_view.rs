//! Shareable immutable read snapshots of the three database stores.

use super::tx::DbSnapshot;
use reth_db_api::database::DatabaseReadView;
use rocksdb::DB;
use std::{any::Any, fmt, sync::Arc};

pub(super) struct ReadView {
    pub(super) state: Arc<DbSnapshot>,
    pub(super) accounts: Arc<DbSnapshot>,
    pub(super) storages: Arc<DbSnapshot>,
}

impl ReadView {
    pub(super) fn new(state: Arc<DB>, accounts: Arc<DB>, storages: Arc<DB>) -> Self {
        Self {
            state: Arc::new(DbSnapshot::new(state)),
            accounts: Arc::new(DbSnapshot::new(accounts)),
            storages: Arc::new(DbSnapshot::new(storages)),
        }
    }

    pub(super) fn belongs_to(
        &self,
        state: &Arc<DB>,
        accounts: &Arc<DB>,
        storages: &Arc<DB>,
    ) -> bool {
        self.state.belongs_to(state) &&
            self.accounts.belongs_to(accounts) &&
            self.storages.belongs_to(storages)
    }
}

impl fmt::Debug for ReadView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadView").finish_non_exhaustive()
    }
}

impl DatabaseReadView for ReadView {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
#[path = "read_view_tests.rs"]
mod tests;
