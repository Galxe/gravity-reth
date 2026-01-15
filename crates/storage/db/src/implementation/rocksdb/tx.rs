//! Transaction implementation for RocksDB.

use crate::{
    implementation::rocksdb::{cursor, get_cf_handle, read_error, to_error_info},
    DatabaseError,
};
use parking_lot::Mutex;
use reth_db_api::{
    table::{Compress, Decompress, DupSort, Encode, Table, TableImporter},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_storage_errors::db::DatabaseErrorInfo;
use rocksdb::DB;
use std::{sync::Arc, thread};

use crate::set_fail_point;
pub(crate) use cursor::{RO, RW};

/// RocksDB transaction with three-database sharding architecture.
///
/// This transaction type splits data across three separate RocksDB instances to enable
/// parallel writes and commits, improving write throughput and reducing lock contention.
///
/// # Architecture
///
/// The database is partitioned into three independent RocksDB instances:
/// - `state_db`: Stores state tables (accounts, contract storage, receipts) and history indices
/// - `account_db`: Stores the account trie (Merkle Patricia Trie for account state root)
/// - `storage_db`: Stores the storage trie (Merkle Patricia Trie for contract storage)
///
/// Each database instance maintains its own write-ahead log (WAL), memtables, and SST files,
/// allowing writes to different instances to proceed in parallel without blocking each other.
///
/// # Write Batches
///
/// Each DB instance has a corresponding WriteBatch wrapped in Arc<Mutex<_>>:
/// - Arc enables sharing the batch across threads spawned during parallel commits
/// - Mutex provides interior mutability, allowing batch modifications through shared references
/// - Separate batches ensure writes to different DBs can be built concurrently
///
/// During commit, if both account_batch and storage_batch contain data, they are committed
/// in parallel using thread::scope. The state_batch is always committed last to ensure
/// atomicity - if state commit succeeds, the entire transaction is considered successful.
///
/// # Type Parameter
///
/// `K: cursor::TransactionKind` is a phantom type marker that distinguishes read-only (RO)
/// from read-write (RW) transactions at compile time, enabling type-safe cursor APIs.
pub struct Tx<K: cursor::TransactionKind> {
    /// State database instance (accounts, storage, receipts, history indices).
    /// Committed last to act as the commit coordinator.
    state_db: Arc<DB>,

    /// Account trie database instance (Merkle Patricia Trie for account state).
    /// Can be committed in parallel with storage_db.
    account_db: Arc<DB>,

    /// Storage trie database instance (Merkle Patricia Trie for contract storage).
    /// Can be committed in parallel with account_db.
    storage_db: Arc<DB>,

    /// Write batch for state database.
    /// Arc<Mutex<_>> enables shared access across threads during parallel commit.
    state_batch: Arc<Mutex<rocksdb::WriteBatch>>,

    /// Write batch for account trie database.
    /// Arc<Mutex<_>> enables shared access across threads during parallel commit.
    account_batch: Arc<Mutex<rocksdb::WriteBatch>>,

    /// Write batch for storage trie database.
    /// Arc<Mutex<_>> enables shared access across threads during parallel commit.
    storage_batch: Arc<Mutex<rocksdb::WriteBatch>>,

    /// Phantom data to carry the transaction kind (RO or RW) at compile time.
    /// Enables type-safe distinction between read-only and read-write transactions.
    _mode: std::marker::PhantomData<K>,
}

impl<K: cursor::TransactionKind> std::fmt::Debug for Tx<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tx").field("_mode", &self._mode).finish()
    }
}

impl<K: cursor::TransactionKind> Tx<K> {
    pub(crate) fn new(state_db: Arc<DB>, account_db: Arc<DB>, storage_db: Arc<DB>) -> Self {
        Self {
            state_db,
            account_db,
            storage_db,
            state_batch: Arc::new(Mutex::new(rocksdb::WriteBatch::default())),
            account_batch: Arc::new(Mutex::new(rocksdb::WriteBatch::default())),
            storage_batch: Arc::new(Mutex::new(rocksdb::WriteBatch::default())),
            _mode: std::marker::PhantomData,
        }
    }

    fn db_for_table<T: Table>(&self) -> &Arc<DB> {
        match T::NAME {
            tables::AccountsTrieV2::NAME => &self.account_db,
            tables::StoragesTrieV2::NAME => &self.storage_db,
            _ => &self.state_db,
        }
    }

    fn batch_for_table<T: Table>(&self) -> &Arc<Mutex<rocksdb::WriteBatch>> {
        match T::NAME {
            tables::AccountsTrieV2::NAME => &self.account_batch,
            tables::StoragesTrieV2::NAME => &self.storage_batch,
            _ => &self.state_batch,
        }
    }

    fn db_for_name(&self, name: &str) -> &Arc<DB> {
        match name {
            tables::AccountsTrieV2::NAME => &self.account_db,
            tables::StoragesTrieV2::NAME => &self.storage_db,
            _ => &self.state_db,
        }
    }

    pub fn table_entries(&self, name: &str) -> Result<usize, DatabaseError> {
        let db = self.db_for_name(name);
        let cf_handle = db.cf_handle(name).ok_or_else(|| {
            DatabaseError::Open(DatabaseErrorInfo {
                message: format!("Column family '{}' not found", name).into(),
                code: -1,
            })
        })?;

        // Use property_value_cf to get estimated number of keys
        match db.property_value_cf(cf_handle, "rocksdb.estimate-num-keys") {
            Ok(Some(value)) => {
                // Parse the string value to usize
                value.parse::<usize>().map_err(|_| {
                    DatabaseError::Read(DatabaseErrorInfo {
                        message: "Failed to parse estimated number of keys".into(),
                        code: -1,
                    })
                })
            }
            Ok(None) => Ok(0),
            Err(e) => Err(read_error(e)),
        }
    }
}

impl<K: cursor::TransactionKind> DbTx for Tx<K> {
    type Cursor<T: Table> = cursor::Cursor<K, T>;
    type DupCursor<T: DupSort> = cursor::Cursor<K, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let encoded_key = key.encode();
        self.get_by_encoded_key::<T>(&encoded_key)
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let db = self.db_for_table::<T>();
        let cf_handle = get_cf_handle::<T>(db)?;

        match db.get_cf(cf_handle, key) {
            Ok(Some(value)) => {
                T::Value::decompress(&value).map(Some).map_err(|_| DatabaseError::Decode)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(read_error(e)),
        }
    }

    /// Commits all pending writes across the three database instances.
    ///
    /// # Commit Ordering and Correctness Guarantees
    ///
    /// This commit strategy ensures correctness through careful ordering and checkpoint-based
    /// idempotency, even in the face of partial failures:
    ///
    /// 1. **Parallel Trie Commits**: If both account_batch and storage_batch contain data, they are
    ///    committed in parallel to maximize throughput. These commits may succeed or fail
    ///    independently.
    ///
    /// 2. **State Commit as Coordinator**: The state_batch is ALWAYS committed last. This is
    ///    critical because stage checkpoints are stored in state_db. A checkpoint's presence
    ///    indicates that the corresponding stage has been fully completed.
    ///
    /// # Failure Scenarios and Recovery
    ///
    /// Several partial failure scenarios can occur, all of which are handled safely through
    /// checkpoint-based recovery:
    ///
    /// **Scenario 1: Trie commit fails, state commit succeeds**
    /// - If account_batch or storage_batch commit fails, the function returns an error
    /// - The state_batch commit never executes, so no checkpoint is written
    /// - On retry, the entire transaction is re-executed (idempotent trie writes)
    ///
    /// **Scenario 2: Account commit succeeds, storage commit fails**
    /// - When running in parallel, account_db write may succeed while storage_db write fails
    /// - The function returns an error before state_batch is committed
    /// - No checkpoint is written; account trie data is orphaned but harmless
    /// - On retry, account trie writes are idempotent and will overwrite orphaned data
    ///
    /// **Scenario 3: Both trie commits succeed, state commit fails**
    /// - Account and storage trie data is written to disk
    /// - State_batch commit fails, so no checkpoint is recorded
    /// - On retry, the stage is re-executed because the checkpoint is missing
    /// - Trie writes are idempotent: re-writing the same trie nodes produces identical results
    ///
    /// # Why This Works: Idempotency of Trie Operations
    ///
    /// The correctness of this approach relies on a critical property: **account trie and
    /// storage trie writes are idempotent**. Writing the same trie node multiple times with
    /// the same data produces the same on-disk state. This means:
    ///
    /// - Orphaned trie data (from failed state commits) can be safely overwritten
    /// - Retrying a stage will produce consistent results even if partial trie data exists
    /// - The checkpoint in state_db acts as the authoritative "commit point" for the entire
    ///   multi-database transaction
    ///
    /// # Performance vs. Atomicity Trade-off
    ///
    /// This design prioritizes write throughput (via parallel commits) over strict atomicity.
    /// The lack of distributed transaction coordination means we can have temporary
    /// inconsistencies (orphaned trie data), but these are always resolved by the checkpoint
    /// mechanism and idempotent retry logic.
    fn commit(self) -> Result<bool, DatabaseError> {
        // Acquire locks on all batches upfront to prepare for commit
        let mut state_batch = self.state_batch.lock();
        let mut account_batch = self.account_batch.lock();
        let mut storage_batch = self.storage_batch.lock();

        // Phase 1: Commit trie batches (potentially in parallel)
        if account_batch.len() > 0 && storage_batch.len() > 0 {
            // Both trie batches have data - commit them in parallel for maximum throughput.
            // If either fails, we return an error BEFORE committing state_batch, ensuring
            // no checkpoint is written and the stage will be retried.
            thread::scope(|scope| -> Result<(), DatabaseError> {
                let mut handles = vec![];
                handles.push(scope.spawn(|| -> Result<(), DatabaseError> {
                    let account_batch = std::mem::take(&mut *account_batch);
                    self.account_db
                        .write(account_batch)
                        .map_err(|e| DatabaseError::Commit(to_error_info(e)))?;
                    set_fail_point!("db::commit::after_account_trie");
                    Ok(())
                }));
                handles.push(scope.spawn(|| -> Result<(), DatabaseError> {
                    let storage_batch = std::mem::take(&mut *storage_batch);
                    self.storage_db
                        .write(storage_batch)
                        .map_err(|e| DatabaseError::Commit(to_error_info(e)))?;
                    set_fail_point!("db::commit::after_storage_trie");
                    Ok(())
                }));
                for handle in handles {
                    handle.join().unwrap()?;
                }
                Ok(())
            })?;
        } else {
            // Only one trie batch has data - commit sequentially (no parallelism benefit)
            if account_batch.len() > 0 {
                let account_batch = std::mem::take(&mut *account_batch);
                self.account_db
                    .write(account_batch)
                    .map_err(|e| DatabaseError::Commit(to_error_info(e)))?;
                set_fail_point!("db::commit::after_account_trie");
            }
            if storage_batch.len() > 0 {
                let storage_batch = std::mem::take(&mut *storage_batch);
                self.storage_db
                    .write(storage_batch)
                    .map_err(|e| DatabaseError::Commit(to_error_info(e)))?;
                set_fail_point!("db::commit::after_storage_trie");
            }
        }

        // Phase 2: Commit state batch LAST (acts as the commit coordinator)
        // The state_db contains stage checkpoints. Only when this commit succeeds is the
        // checkpoint written, marking the stage as complete. If this commit fails, the
        // checkpoint is absent, triggering a retry that will idempotently re-execute the stage.
        if state_batch.len() > 0 {
            let state_batch = std::mem::take(&mut *state_batch);
            self.state_db
                .write(state_batch)
                .map_err(|e| DatabaseError::Commit(to_error_info(e)))?;
            set_fail_point!("db::commit::after_state");
        }
        Ok(true)
    }

    fn abort(self) {
        // Nothing to abort for RocksDB
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        cursor::Cursor::new(self.db_for_table::<T>().clone(), self.batch_for_table::<T>().clone())
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        cursor::Cursor::new(self.db_for_table::<T>().clone(), self.batch_for_table::<T>().clone())
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        self.table_entries(T::NAME)
    }

    fn disable_long_read_transaction_safety(&mut self) {
        // For RocksDB, this is a no-op as it doesn't have the same long read transaction safety
        // concerns as MDBX RocksDB handles concurrent reads and writes differently
    }
}

impl DbTxMut for Tx<cursor::RW> {
    type CursorMut<T: Table> = cursor::Cursor<cursor::RW, T>;
    type DupCursorMut<T: DupSort> = cursor::Cursor<cursor::RW, T>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let db = self.db_for_table::<T>();
        let cf_handle = get_cf_handle::<T>(db)?;

        let encoded_key = key.encode();
        let encoded_value = value.compress();

        let mut batch = self.batch_for_table::<T>().lock();
        batch.put_cf(cf_handle, &encoded_key, &encoded_value);

        Ok(())
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let db = self.db_for_table::<T>();
        let cf_handle = get_cf_handle::<T>(db)?;

        let encoded_key = key.encode();

        let mut batch = self.batch_for_table::<T>().lock();
        batch.delete_cf(cf_handle, &encoded_key);
        Ok(true)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        let db = self.db_for_table::<T>();
        let cf_handle = get_cf_handle::<T>(db)?;

        // Get first and last keys using separate iterators to avoid borrowing conflicts
        let (first_key, last_key) = {
            let mut iter = db.raw_iterator_cf(cf_handle);
            iter.seek_to_first();
            if !iter.valid() {
                // No data in this column family, nothing to clear
                return Ok(());
            }
            let first_key = iter
                .key()
                .ok_or_else(|| {
                    DatabaseError::Read(DatabaseErrorInfo {
                        message: "Failed to get first key".into(),
                        code: -1,
                    })
                })?
                .to_vec();

            iter.seek_to_last();
            if !iter.valid() {
                // This shouldn't happen if we found a first key, but handle it gracefully
                return Ok(());
            }
            let last_key = iter
                .key()
                .ok_or_else(|| {
                    DatabaseError::Read(DatabaseErrorInfo {
                        message: "Failed to get last key".into(),
                        code: -1,
                    })
                })?
                .to_vec();

            (first_key, last_key)
        };

        let mut batch = self.batch_for_table::<T>().lock();
        // Delete the range [first_key, last_key] - note that delete_range_cf is [start, end)
        // so we need to handle the last key separately
        batch.delete_range_cf(cf_handle, &first_key, &last_key);

        // Delete the last key separately since delete_range_cf is [start, end)
        batch.delete_cf(cf_handle, &last_key);

        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        cursor::Cursor::new(self.db_for_table::<T>().clone(), self.batch_for_table::<T>().clone())
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        cursor::Cursor::new(self.db_for_table::<T>().clone(), self.batch_for_table::<T>().clone())
    }
}

impl TableImporter for Tx<RW> {
    // Default implementation is sufficient for now
}
