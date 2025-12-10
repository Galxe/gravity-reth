//! Transaction implementation for RocksDB.

use crate::{
    DatabaseError, implementation::rocksdb::{cursor, get_cf_handle, read_error, to_error_info}
};
use reth_db_api::{
    table::{Compress, Decompress, DupSort, Encode, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
};
use reth_storage_errors::db::DatabaseErrorInfo;
use rocksdb::DB;
use std::sync::Arc;
use parking_lot::Mutex;

pub(crate) use cursor::{RO, RW};

/// RocksDB transaction.
pub struct Tx<K: cursor::TransactionKind> {
    db: Arc<DB>,
    batch: Arc<Mutex<rocksdb::WriteBatch>>,
    _mode: std::marker::PhantomData<K>,
}

impl<K: cursor::TransactionKind> std::fmt::Debug for Tx<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tx")
            .field("db", &self.db)
            .field("batch", &"<WriteBatch>")
            .field("_mode", &self._mode)
            .finish()
    }
}

impl<K: cursor::TransactionKind> Tx<K> {
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self { 
            db, 
            batch: Arc::new(Mutex::new(rocksdb::WriteBatch::default())),
            _mode: std::marker::PhantomData 
        }
    }

    pub fn table_entries(&self, name: &str) -> Result<usize, DatabaseError> {
        let cf_handle = self.db.cf_handle(name).ok_or_else(|| {
            DatabaseError::Open(DatabaseErrorInfo {
                message: format!("Column family '{}' not found", name).into(),
                code: -1,
            })
        })?;

        // Use property_value_cf to get estimated number of keys
        match self.db.property_value_cf(cf_handle, "rocksdb.estimate-num-keys") {
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
        let cf_handle = get_cf_handle::<T>(&self.db)?;

        match self.db.get_cf(cf_handle, key) {
            Ok(Some(value)) => {
                T::Value::decompress(&value).map(Some).map_err(|_| DatabaseError::Decode)
            }
            Ok(None) => Ok(None),
            Err(e) => Err(read_error(e)),
        }
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        // Write the batch to RocksDB atomically
        let mut batch_guard = self.batch.lock();
        let batch = std::mem::take(&mut *batch_guard);
        self.db.write(batch).map_err(|e| {
            DatabaseError::Commit(to_error_info(e))
        })?;
        Ok(true)
    }

    fn abort(self) {
        // Nothing to abort for RocksDB
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        cursor::Cursor::new(self.db.clone(), self.batch.clone())
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        cursor::Cursor::new(self.db.clone(), self.batch.clone())
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
        let cf_handle = get_cf_handle::<T>(&self.db)?;

        let encoded_key = key.encode();
        let encoded_value = value.compress();

        let mut batch = self.batch.lock();
        batch.put_cf(cf_handle, &encoded_key, &encoded_value);

        Ok(())
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let cf_handle = get_cf_handle::<T>(&self.db)?;

        let encoded_key = key.encode();

        let mut batch = self.batch.lock();
        batch.delete_cf(cf_handle, &encoded_key);
        Ok(true)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        let cf_handle = get_cf_handle::<T>(&self.db)?;

        // Get first and last keys using separate iterators to avoid borrowing conflicts
        let (first_key, last_key) = {
            let mut iter = self.db.raw_iterator_cf(cf_handle);
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

        let mut batch = self.batch.lock();
        // Delete the range [first_key, last_key] - note that delete_range_cf is [start, end)
        // so we need to handle the last key separately
        batch.delete_range_cf(cf_handle, &first_key, &last_key);

        // Delete the last key separately since delete_range_cf is [start, end)
        batch.delete_cf(cf_handle, &last_key);

        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        cursor::Cursor::new(self.db.clone(), self.batch.clone())
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        cursor::Cursor::new(self.db.clone(), self.batch.clone())
    }
}

impl TableImporter for Tx<RW> {
    // Default implementation is sufficient for now
}
