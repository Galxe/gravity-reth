//! Transaction implementation for `RocksDB`.

use crate::{
    implementation::rocksdb::{cursor, get_cf_handle, read_error, to_error_info},
    DatabaseError,
};
use parking_lot::Mutex;
use reth_db_api::{
    database::DatabaseSnapshot,
    table::{Compress, Decompress, DupSort, Encode, Table, TableImporter},
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_storage_errors::db::DatabaseErrorInfo;
use rocksdb::{SnapshotWithThreadMode, WriteOptions, DB};
use std::{any::Any, sync::Arc, thread, time::Instant};

use crate::set_fail_point;
pub(crate) use cursor::{RO, RW};

/// `RocksDB` transaction with three-database sharding architecture.
///
/// This transaction type splits data across three separate `RocksDB` instances to enable
/// parallel writes and commits, improving write throughput and reducing lock contention.
///
/// # Architecture
///
/// The database is partitioned into three independent `RocksDB` instances:
/// - `state_db`: Stores state tables (accounts, contract storage, receipts) and history indices
/// - `account_db`: Stores the account trie (Merkle Patricia Trie for account state root)
/// - `storage_db`: Stores the storage trie (Merkle Patricia Trie for contract storage)
///
/// Each database instance maintains its own write-ahead log (WAL), memtables, and SST files,
/// allowing writes to different instances to proceed in parallel without blocking each other.
///
/// # Write Batches
///
/// Read-write transactions give each DB instance a `WriteBatch` wrapped in Arc<Mutex<_>>:
/// - Arc enables sharing the batch across threads spawned during parallel commits
/// - Mutex provides interior mutability, allowing batch modifications through shared references
/// - Separate batches ensure writes to different DBs can be built concurrently
///
/// The state batch is sync-written first, followed by non-empty trie batches in parallel.
/// Each batch is atomic within its database; commits are not atomic across the three databases.
/// Reads use the selected live or snapshot sources and do not include pending batch entries.
///
/// # Type Parameter
///
/// `K: cursor::TransactionKind` is a phantom type marker that distinguishes read-only (RO)
/// from read-write (RW) transactions at compile time, enabling type-safe cursor APIs.
pub struct Tx<K: cursor::TransactionKind> {
    /// State database instance (accounts, storage, receipts, history indices).
    state: DbReadSource,

    /// Account trie database instance (Merkle Patricia Trie for account state).
    /// Can be committed in parallel with the storage trie database.
    accounts: DbReadSource,

    /// Storage trie database instance (Merkle Patricia Trie for contract storage).
    /// Can be committed in parallel with the account trie database.
    storages: DbReadSource,

    /// Allocated only for read-write transactions, and shared with their write cursors.
    write_batches: Option<WriteBatches>,

    /// Phantom data to carry the transaction kind (RO or RW) at compile time.
    /// Enables type-safe distinction between read-only and read-write transactions.
    _mode: std::marker::PhantomData<K>,
}

impl<K: cursor::TransactionKind> std::fmt::Debug for Tx<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tx").field("_mode", &self._mode).finish()
    }
}

#[derive(Default)]
struct WriteBatches {
    state: Arc<Mutex<rocksdb::WriteBatch>>,
    accounts: Arc<Mutex<rocksdb::WriteBatch>>,
    storages: Arc<Mutex<rocksdb::WriteBatch>>,
}

/// Owns the DB behind a `RocksDB` snapshot. Cursors retain this owner independently of their
/// transaction, and field drop order releases the snapshot before its database.
pub(crate) struct DbSnapshot {
    snapshot: SnapshotWithThreadMode<'static, DB>,
    db: Arc<DB>,
}

impl DbSnapshot {
    fn new(db: Arc<DB>) -> Self {
        let snapshot = unsafe {
            // SAFETY: `db` owns the referenced DB for the entire snapshot lifetime. Field drop
            // order releases `snapshot` before `db`, and cursors retain this owner in an Arc.
            std::mem::transmute::<SnapshotWithThreadMode<'_, DB>, SnapshotWithThreadMode<'static, DB>>(
                db.snapshot(),
            )
        };
        Self { snapshot, db }
    }
}

/// Shareable snapshots of the primary store and optional additional stores.
pub(super) struct Snapshot {
    state: Arc<DbSnapshot>,
    tries: Option<(Arc<DbSnapshot>, Arc<DbSnapshot>)>,
}

impl Snapshot {
    pub(super) fn new(state_db: Arc<DB>) -> Self {
        Self { state: Arc::new(DbSnapshot::new(state_db)), tries: None }
    }

    pub(super) fn all_stores(state_db: Arc<DB>, account_db: Arc<DB>, storage_db: Arc<DB>) -> Self {
        Self {
            state: Arc::new(DbSnapshot::new(state_db)),
            tries: Some((
                Arc::new(DbSnapshot::new(account_db)),
                Arc::new(DbSnapshot::new(storage_db)),
            )),
        }
    }

    pub(super) fn belongs_to(&self, state_db: &Arc<DB>) -> bool {
        Arc::ptr_eq(&self.state.db, state_db)
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot").finish_non_exhaustive()
    }
}

impl DatabaseSnapshot for Snapshot {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A live database or a fixed read snapshot owning that same database.
#[derive(Clone)]
pub(crate) enum DbReadSource {
    Live(Arc<DB>),
    Snapshot(Arc<DbSnapshot>),
}

impl DbReadSource {
    pub(crate) fn db(&self) -> &Arc<DB> {
        match self {
            Self::Live(db) => db,
            Self::Snapshot(snapshot) => &snapshot.db,
        }
    }

    pub(crate) fn get_cf(
        &self,
        cf: &rocksdb::ColumnFamily,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        match self {
            Self::Live(db) => db.get_cf(cf, key),
            Self::Snapshot(snapshot) => snapshot.snapshot.get_cf(cf, key),
        }
    }

    pub(crate) fn raw_iterator_cf(&self, cf: &rocksdb::ColumnFamily) -> rocksdb::DBRawIterator<'_> {
        match self {
            Self::Live(db) => db.raw_iterator_cf(cf),
            Self::Snapshot(snapshot) => snapshot.snapshot.raw_iterator_cf(cf),
        }
    }
}

impl<K: cursor::TransactionKind> Tx<K> {
    pub(crate) fn new(state_db: Arc<DB>, account_db: Arc<DB>, storage_db: Arc<DB>) -> Self {
        Self::with_sources(
            DbReadSource::Live(state_db),
            DbReadSource::Live(account_db),
            DbReadSource::Live(storage_db),
        )
    }

    fn with_sources(state: DbReadSource, accounts: DbReadSource, storages: DbReadSource) -> Self {
        Self {
            state,
            accounts,
            storages,
            write_batches: K::IS_READ_WRITE.then(WriteBatches::default),
            _mode: std::marker::PhantomData,
        }
    }

    fn source_for_table<T: Table>(&self) -> &DbReadSource {
        self.source_for_name(T::NAME)
    }

    fn db_for_table<T: Table>(&self) -> &Arc<DB> {
        self.source_for_table::<T>().db()
    }

    fn batch_for_table<T: Table>(&self) -> Option<&Arc<Mutex<rocksdb::WriteBatch>>> {
        let batches = self.write_batches.as_ref()?;
        Some(match T::NAME {
            tables::AccountsTrieV2::NAME => &batches.accounts,
            tables::StoragesTrieV2::NAME => &batches.storages,
            _ => &batches.state,
        })
    }

    fn source_for_name(&self, name: &str) -> &DbReadSource {
        match name {
            tables::AccountsTrieV2::NAME => &self.accounts,
            tables::StoragesTrieV2::NAME => &self.storages,
            _ => &self.state,
        }
    }

    /// Returns `RocksDB`'s live estimate, independent of the transaction's read snapshot.
    pub fn table_entries(&self, name: &str) -> Result<usize, DatabaseError> {
        let db = self.source_for_name(name).db();
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

    fn dupsort_key_value<T: Table>(
        key: T::Key,
        value: T::Value,
    ) -> (Vec<u8>, <T::Value as Compress>::Compressed) {
        let subkey_length = value.subkey_compress_length().expect("DupSort table must have subkey");
        let encoded_key = key.encode();
        let encoded_value = value.compress();
        let encoded_key_ref = encoded_key.as_ref();
        let encoded_value_ref = encoded_value.as_ref();
        let subkey = &encoded_value_ref[..subkey_length];
        let mut composite_key = Vec::with_capacity(encoded_key_ref.len() + subkey.len());
        composite_key.extend_from_slice(encoded_key_ref);
        composite_key.extend_from_slice(subkey);
        (composite_key, encoded_value)
    }
}

impl Tx<RO> {
    pub(crate) fn new_state_snapshot(
        state_db: Arc<DB>,
        account_db: Arc<DB>,
        storage_db: Arc<DB>,
    ) -> Self {
        Self::from_snapshot(&Snapshot::new(state_db), account_db, storage_db)
    }

    pub(super) fn from_snapshot(
        snapshot: &Snapshot,
        account_db: Arc<DB>,
        storage_db: Arc<DB>,
    ) -> Self {
        let (accounts, storages) = match &snapshot.tries {
            Some((accounts, storages)) => {
                (DbReadSource::Snapshot(accounts.clone()), DbReadSource::Snapshot(storages.clone()))
            }
            None => (DbReadSource::Live(account_db), DbReadSource::Live(storage_db)),
        };
        Self::with_sources(DbReadSource::Snapshot(snapshot.state.clone()), accounts, storages)
    }
}

impl Tx<RW> {
    fn write_batch_for_table<T: Table>(&self) -> &Mutex<rocksdb::WriteBatch> {
        self.batch_for_table::<T>().expect("read-write transactions have write batches")
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
        let source = self.source_for_table::<T>();
        let cf_handle = get_cf_handle::<T>(source.db())?;

        if T::DUPSORT {
            // For DupSort tables, we need to seek to the first entry with this key prefix
            // because RocksDB stores composite keys (primary_key + subkey)
            let mut iter = source.raw_iterator_cf(cf_handle);
            let encoded_key_ref = key.as_ref();
            iter.seek(encoded_key_ref);

            if iter.valid() &&
                let (Some(k), Some(v)) = (iter.key(), iter.value())
            {
                // Check if this key has the same prefix (same primary key)
                if k.len() >= encoded_key_ref.len() &&
                    &k[..encoded_key_ref.len()] == encoded_key_ref
                {
                    return T::Value::decompress(v).map(Some).map_err(|_| DatabaseError::Decode);
                }
            }
            Ok(None)
        } else {
            match source.get_cf(cf_handle, key) {
                Ok(Some(value)) => {
                    T::Value::decompress(&value).map(Some).map_err(|_| DatabaseError::Decode)
                }
                Ok(None) => Ok(None),
                Err(e) => Err(read_error(e)),
            }
        }
    }

    /// Flushes pending state writes, then the trie batches, without consuming the transaction.
    /// This does not record a completed persistence unit: callers must write the completion
    /// checkpoint in a separate state commit after both trie batches succeed.
    fn commit_view(&self) -> Result<bool, DatabaseError> {
        let Some(batches) = &self.write_batches else { return Ok(true) };
        let mut sync_opts = WriteOptions::default();
        sync_opts.set_sync(true);
        let write_batch = |db: &DB, batch: rocksdb::WriteBatch, database: &'static str| {
            metrics::histogram!("reth_rocksdb_commit_batch_size_bytes", "database" => database)
                .record(batch.size_in_bytes() as f64);
            let start = Instant::now();
            db.write_opt(batch, &sync_opts).map_err(|e| DatabaseError::Commit(to_error_info(e)))?;
            metrics::counter!("reth_rocksdb_sync_commits_total", "database" => database)
                .increment(1);
            metrics::histogram!("reth_rocksdb_sync_commit_duration_seconds", "database" => database)
                .record(start.elapsed());
            Ok::<(), DatabaseError>(())
        };
        let write_storage = |batch| {
            set_fail_point!("db::commit::before_storage_trie");
            write_batch(self.storages.db(), batch, "storage")
        };
        let mut state_batch = batches.state.lock();
        let mut account_batch = batches.accounts.lock();
        let mut storage_batch = batches.storages.lock();
        let state_data = (!state_batch.is_empty()).then(|| std::mem::take(&mut *state_batch));
        let account_data = (!account_batch.is_empty()).then(|| std::mem::take(&mut *account_batch));
        let storage_data = (!storage_batch.is_empty()).then(|| std::mem::take(&mut *storage_batch));

        // A merged trie must never advance beyond the durable Execution checkpoint.
        if let Some(batch) = state_data {
            set_fail_point!("db::commit::before_state");
            write_batch(self.state.db(), batch, "state")?;
        }

        match (account_data, storage_data) {
            (Some(account), Some(storage)) => thread::scope(|scope| {
                let account_handle =
                    scope.spawn(|| write_batch(self.accounts.db(), account, "account"));
                let storage_result = write_storage(storage);
                // Wait for the worker before propagating either write error.
                account_handle.join().unwrap()?;
                storage_result
            })?,
            (Some(account), None) => write_batch(self.accounts.db(), account, "account")?,
            (None, Some(storage)) => write_storage(storage)?,
            (None, None) => {}
        }
        Ok(true)
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        self.commit_view()
    }

    fn abort(self) {
        // Nothing to abort for RocksDB
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        cursor::Cursor::new(
            self.source_for_table::<T>().clone(),
            self.batch_for_table::<T>().cloned(),
        )
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        cursor::Cursor::new(
            self.source_for_table::<T>().clone(),
            self.batch_for_table::<T>().cloned(),
        )
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
        let mut batch = self.write_batch_for_table::<T>().lock();

        if T::DUPSORT {
            let (composite_key, encoded_value) = Self::dupsort_key_value::<T>(key, value);
            batch.put_cf(cf_handle, &composite_key, &encoded_value);
        } else {
            let encoded_key = key.encode();
            let encoded_value = value.compress();
            batch.put_cf(cf_handle, &encoded_key, &encoded_value);
        }
        Ok(())
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let db = self.db_for_table::<T>();
        let cf_handle = get_cf_handle::<T>(db)?;
        let mut batch = self.write_batch_for_table::<T>().lock();

        if T::DUPSORT {
            if let Some(value) = value {
                // Delete specific key/value pair
                let (composite_key, _) = Self::dupsort_key_value::<T>(key, value);
                batch.delete_cf(cf_handle, &composite_key);
            } else {
                // Delete all values for this key (MDBX semantics: value=None deletes all)
                let encoded_key = key.encode();
                let encoded_key_ref = encoded_key.as_ref();

                // Use iterator to find and delete all entries with this key prefix
                let mut iter = db.raw_iterator_cf(cf_handle);
                iter.seek(encoded_key_ref);

                while iter.valid() {
                    if let Some(k) = iter.key() {
                        // Check if this key still has the same prefix (same primary key)
                        if k.len() >= encoded_key_ref.len() &&
                            &k[..encoded_key_ref.len()] == encoded_key_ref
                        {
                            batch.delete_cf(cf_handle, k);
                            iter.next();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        } else {
            let encoded_key = key.encode();
            batch.delete_cf(cf_handle, &encoded_key);
        }
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

        let mut batch = self.write_batch_for_table::<T>().lock();
        // Delete the range [first_key, last_key] - note that delete_range_cf is [start, end)
        // so we need to handle the last key separately
        batch.delete_range_cf(cf_handle, &first_key, &last_key);

        // Delete the last key separately since delete_range_cf is [start, end)
        batch.delete_cf(cf_handle, &last_key);

        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        cursor::Cursor::new(
            self.source_for_table::<T>().clone(),
            self.batch_for_table::<T>().cloned(),
        )
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        cursor::Cursor::new(
            self.source_for_table::<T>().clone(),
            self.batch_for_table::<T>().cloned(),
        )
    }
}

impl TableImporter for Tx<RW> {
    // Default implementation is sufficient for now
}
