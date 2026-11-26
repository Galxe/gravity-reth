//! Explicit verification of legacy trie tables over one shared read snapshot.

use crate::{
    DatabaseAccountTrieCursor, DatabaseHashedAccountCursor, DatabaseHashedCursorFactory,
    DatabaseHashedStorageCursor, DatabaseStorageTrieCursor, DatabaseTrieCursorFactory,
};
use alloy_primitives::B256;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    database::{Database, DatabaseSnapshot},
    tables,
    transaction::DbTx,
    DatabaseError,
};
use reth_execution_errors::StateRootError;
use reth_trie::{
    hashed_cursor::HashedCursorFactory,
    parallel_verify::{Output, StorageJob, StorageWorker, Verifier},
    proof_v2::ProofCursorFactory,
    trie_cursor::TrieCursorFactory,
};
use std::{fmt, sync::Arc};

type TxVerifier<TX> = Verifier<
    DatabaseAccountTrieCursor<<TX as DbTx>::Cursor<tables::AccountsTrie>>,
    DatabaseHashedAccountCursor<<TX as DbTx>::Cursor<tables::HashedAccounts>>,
>;

/// Verifies only `AccountsTrie`/`StoragesTrie` against hashed state on a shared snapshot.
///
/// This checks legacy branch-node caches, not Gravity's nested trie tables. It does not repair
/// tables, configure a node, or select the production state-root implementation.
///
/// Snapshot capture must coordinate with every writer whose logical updates span this read view.
/// The database's snapshot coordinator only excludes writers which participate in that coordinator;
/// this wrapper does not acquire a separate exclusive writer lock. Callers must exclude any
/// uncoordinated writers at capture time. After capture, reads remain pinned while writers
/// continue. All worker cursors and the address iterator reuse that same captured snapshot.
///
/// This type is intentionally separate from the existing serial verifier and its factory API.
pub struct LegacyDatabaseVerifier<TX: DbTx> {
    verifier: TxVerifier<TX>,
    // Cursors and the address scanner must finish before backing transactions are released.
    _transactions: [Arc<TX>; 2],
}

impl<TX: DbTx + 'static> LegacyDatabaseVerifier<TX> {
    /// Captures one coordinated snapshot and starts legacy verification.
    ///
    /// Returns an error if the backend cannot supply a shared snapshot or fixed table views;
    /// independently opened live transactions are never used as a fallback.
    ///
    /// # Panics
    ///
    /// Panics if `storage_workers` or `storage_lookahead` is zero.
    pub fn new(
        db: &impl Database<TX = TX>,
        storage_workers: usize,
        storage_lookahead: usize,
    ) -> Result<Self, DatabaseError> {
        assert!(storage_workers > 0, "at least one storage worker is required");
        assert!(storage_lookahead > 0, "storage lookahead must be positive");
        let snapshot = require_snapshot(db.snapshot(true)?)?;
        Self::from_snapshot(db, snapshot, storage_workers, storage_lookahead)
    }

    /// Starts verification on a previously captured shared snapshot from `db`.
    ///
    /// All transactions are opened on this snapshot, even if writes committed since it was
    /// captured. The caller must have coordinated its capture with writers as described on
    /// [`Self`]. Backend errors, including a snapshot belonging to another database, propagate.
    /// Fixed views are checked for every table read by each transaction.
    ///
    /// # Panics
    ///
    /// Panics if `storage_workers` or `storage_lookahead` is zero.
    pub fn from_snapshot(
        db: &impl Database<TX = TX>,
        snapshot: Arc<dyn DatabaseSnapshot>,
        storage_workers: usize,
        storage_lookahead: usize,
    ) -> Result<Self, DatabaseError> {
        assert!(storage_workers > 0, "at least one storage worker is required");
        assert!(storage_lookahead > 0, "storage lookahead must be positive");
        let open = || {
            let tx = db.tx_with_snapshot(Some(snapshot.clone()))?;
            require_fixed_view(&tx)?;
            Ok::<_, DatabaseError>(Arc::new(tx))
        };
        let account_tx = open()?;
        let address_tx = open()?;
        let workers = (0..storage_workers)
            .map(|_| {
                let tx = open()?;
                let factory = ProofCursorFactory::new(SnapshotCursorFactory(tx.clone()));
                Ok(StorageWorker {
                    trie: factory.storage_trie_cursor(B256::ZERO)?,
                    hashed: factory.hashed_storage_cursor(B256::ZERO)?,
                    tx,
                })
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        let verifier = Verifier::new(
            DatabaseTrieCursorFactory::new(account_tx.as_ref()).account_trie_cursor()?,
            DatabaseHashedCursorFactory::new(account_tx.as_ref()).hashed_account_cursor()?,
            StorageAddresses::<TX> {
                hashed: address_tx.cursor_dup_read::<tables::HashedStorages>()?,
                trie: address_tx.cursor_dup_read::<tables::StoragesTrie>()?,
                next_hashed: None,
                next_trie: None,
                initialized: false,
            },
            workers,
            storage_lookahead,
        )?;
        Ok(Self { verifier, _transactions: [account_tx, address_tx] })
    }
}

impl<TX: DbTx> Iterator for LegacyDatabaseVerifier<TX> {
    type Item = Result<Output, StateRootError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.verifier.next()
    }
}
impl<TX: DbTx> fmt::Debug for LegacyDatabaseVerifier<TX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LegacyDatabaseVerifier").finish_non_exhaustive()
    }
}

fn require_snapshot(
    snapshot: Option<Arc<dyn DatabaseSnapshot>>,
) -> Result<Arc<dyn DatabaseSnapshot>, DatabaseError> {
    snapshot.ok_or_else(|| {
        DatabaseError::Other("legacy trie verification requires a shared database snapshot".into())
    })
}
fn require_fixed_view(tx: &impl DbTx) -> Result<(), DatabaseError> {
    if tx.is_fixed_snapshot::<tables::AccountsTrie>() &&
        tx.is_fixed_snapshot::<tables::StoragesTrie>() &&
        tx.is_fixed_snapshot::<tables::HashedAccounts>() &&
        tx.is_fixed_snapshot::<tables::HashedStorages>()
    {
        Ok(())
    } else {
        Err(DatabaseError::Other("legacy trie verification requires fixed views of all legacy trie and hashed state tables".into()))
    }
}

/// Owns the transaction reused when a worker reopens storage cursors for another address.
#[derive(Debug)]
struct SnapshotCursorFactory<TX>(Arc<TX>);
impl<TX> Clone for SnapshotCursorFactory<TX> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<TX: DbTx> TrieCursorFactory for SnapshotCursorFactory<TX> {
    type AccountTrieCursor = DatabaseAccountTrieCursor<TX::Cursor<tables::AccountsTrie>>;
    type StorageTrieCursor = DatabaseStorageTrieCursor<TX::DupCursor<tables::StoragesTrie>>;
    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        DatabaseTrieCursorFactory::new(self.0.as_ref()).account_trie_cursor()
    }
    fn storage_trie_cursor(&self, address: B256) -> Result<Self::StorageTrieCursor, DatabaseError> {
        DatabaseTrieCursorFactory::new(self.0.as_ref()).storage_trie_cursor(address)
    }
}
impl<TX: DbTx> HashedCursorFactory for SnapshotCursorFactory<TX> {
    type AccountCursor = DatabaseHashedAccountCursor<TX::Cursor<tables::HashedAccounts>>;
    type StorageCursor = DatabaseHashedStorageCursor<TX::DupCursor<tables::HashedStorages>>;
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        DatabaseHashedCursorFactory::new(self.0.as_ref()).hashed_account_cursor()
    }
    fn hashed_storage_cursor(&self, address: B256) -> Result<Self::StorageCursor, DatabaseError> {
        DatabaseHashedCursorFactory::new(self.0.as_ref()).hashed_storage_cursor(address)
    }
}

/// Merges distinct addresses from hashed storage and the legacy stored storage trie table.
struct StorageAddresses<TX: DbTx> {
    hashed: TX::DupCursor<tables::HashedStorages>,
    trie: TX::DupCursor<tables::StoragesTrie>,
    next_hashed: Option<B256>,
    next_trie: Option<B256>,
    initialized: bool,
}
impl<TX: DbTx> StorageAddresses<TX> {
    fn next_address(&mut self) -> Result<Option<StorageJob>, DatabaseError> {
        if !self.initialized {
            self.next_hashed = self.hashed.first()?.map(|(address, _)| address);
            self.next_trie = self.trie.first()?.map(|(address, _)| address);
            self.initialized = true;
        }
        let address = match (self.next_hashed, self.next_trie) {
            (Some(a), Some(b)) => a.min(b),
            (Some(address), None) | (None, Some(address)) => address,
            (None, None) => return Ok(None),
        };
        let root_required = self.next_hashed == Some(address);
        if root_required {
            self.next_hashed = self.hashed.next_no_dup()?.map(|(address, _)| address);
        }
        if self.next_trie == Some(address) {
            self.next_trie = self.trie.next_no_dup()?.map(|(address, _)| address);
        }
        Ok(Some(StorageJob { address, root_required }))
    }
}
impl<TX: DbTx> Iterator for StorageAddresses<TX> {
    type Item = Result<StorageJob, DatabaseError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_address().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_db::test_utils::create_test_rw_db;

    #[test]
    fn missing_shared_snapshot_is_rejected() {
        assert!(
            matches!(require_snapshot(None), Err(DatabaseError::Other(message)) if message == "legacy trie verification requires a shared database snapshot")
        );
    }

    #[test]
    fn live_transactions_are_rejected_and_real_snapshots_are_accepted() {
        let db = create_test_rw_db();
        assert!(
            matches!(require_fixed_view(&db.tx().unwrap()), Err(DatabaseError::Other(message)) if message == "legacy trie verification requires fixed views of all legacy trie and hashed state tables")
        );
        let snapshot = require_snapshot(db.snapshot(true).unwrap()).unwrap();
        require_fixed_view(&db.tx_with_snapshot(Some(snapshot)).unwrap()).unwrap();
    }
}
