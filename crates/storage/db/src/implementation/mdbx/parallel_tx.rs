//! A set of read-only transactions that can be used to read data from the database in parallel.

use super::{cursor::Cursor, tx::Tx, Environment, RO};
use crate::{metrics::DatabaseEnvMetrics, DatabaseError};
use reth_db_api::{
    table::{DupSort, Encode, Table},
    transaction::DbTx,
};
use std::sync::{Arc, Mutex};

/// A set of read-only transactions that can be used to read data from the database in parallel.
#[derive(Debug)]
pub struct ParallelTxRO {
    inner: Arc<Mutex<Inner>>,
    env: Environment,
    metrics: Option<Arc<DatabaseEnvMetrics>>,
    max_txs: usize,
}

#[derive(Debug)]
struct Inner {
    txs: Vec<WrappedTx>,
    num_txs: usize,
}

#[derive(Debug)]
struct WrappedTx {
    tx: Arc<Tx<RO>>,
    held_count: usize,
}

impl ParallelTxRO {
    pub(super) fn try_new(
        env: Environment,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Result<Self, DatabaseError> {
        let tx = Self::create_tx(&env, metrics.clone())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                txs: vec![WrappedTx { tx: Arc::new(tx), held_count: 0 }],
                num_txs: 1,
            })),
            max_txs: 8,
            env,
            metrics,
        })
    }

    fn create_tx(
        env: &Environment,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Result<Tx<RO>, DatabaseError> {
        Tx::new_with_metrics(
            env.begin_ro_txn().map_err(|e| DatabaseError::InitTx(e.into()))?,
            metrics,
        )
        .map_err(|e| DatabaseError::InitTx(e.into()))
    }

    fn execute_tx<R>(
        &self,
        f: impl FnOnce(usize, &Tx<RO>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut inner = self.inner.lock().unwrap();
        let (index, tx) = {
            // Find the tx with the lowest held_count
            let num_txs = inner.num_txs;
            let (index, wrapped_tx) =
                inner.txs.iter_mut().enumerate().min_by_key(|(_, tx)| tx.held_count).unwrap();
            if wrapped_tx.held_count > 0 && num_txs < self.max_txs {
                // Create a new tx and add it to the inner txs
                inner.num_txs += 1;
                drop(inner);
                match Self::create_tx(&self.env, self.metrics.clone()) {
                    Ok(tx) => {
                        let tx = Arc::new(tx);
                        let tx_clone = tx.clone();
                        let mut inner = self.inner.lock().unwrap();
                        inner.txs.push(WrappedTx { tx, held_count: 1 });
                        (inner.txs.len() - 1, tx_clone)
                    }
                    Err(e) => {
                        self.inner.lock().unwrap().num_txs -= 1;
                        return Err(e);
                    }
                }
            } else {
                // Use the existing tx
                wrapped_tx.held_count += 1;
                let tx = wrapped_tx.tx.clone();
                drop(inner);
                (index, tx)
            }
        };

        let result = f(index, &tx);
        result
    }

    /// Opens a handle to an MDBX database.
    pub fn open_db(&self, name: Option<&str>) -> reth_libmdbx::Result<reth_libmdbx::Database> {
        self.inner.lock().unwrap().txs[0].tx.inner.open_db(name)
    }

    /// Retrieves database statistics.
    pub fn db_stat(&self, db: &reth_libmdbx::Database) -> reth_libmdbx::Result<reth_libmdbx::Stat> {
        self.inner.lock().unwrap().txs[0].tx.inner.db_stat(db)
    }

    /// Returns a raw pointer to the MDBX environment.
    pub const fn env(&self) -> &Environment {
        &self.env
    }
}

impl DbTx for ParallelTxRO {
    type Cursor<T: Table> = Cursor<RO, T>;
    type DupCursor<T: DupSort> = Cursor<RO, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<<T as Table>::Value>, DatabaseError> {
        self.get_by_encoded_key::<T>(&key.encode())
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        self.execute_tx(|idx, tx| {
            let result = tx.get_by_encoded_key::<T>(key);
            self.inner.lock().unwrap().txs[idx].held_count -= 1;
            result
        })
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        // Do nothing.
        Ok(true)
    }

    fn abort(self) {
        // Do nothing.
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.execute_tx(|idx, tx| {
            let result = tx.cursor_read::<T>();
            match result {
                Ok(mut cursor) => {
                    let inner = self.inner.clone();
                    cursor.with_drop_fn(Box::new(move |_| {
                        inner.lock().unwrap().txs[idx].held_count -= 1;
                    }));
                    Ok(cursor)
                }
                Err(e) => {
                    self.inner.lock().unwrap().txs[idx].held_count -= 1;
                    Err(e)
                }
            }
        })
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.execute_tx(|idx, tx| {
            let result = tx.cursor_dup_read::<T>();
            match result {
                Ok(mut cursor) => {
                    let inner = self.inner.clone();
                    cursor.with_drop_fn(Box::new(move |_| {
                        inner.lock().unwrap().txs[idx].held_count -= 1;
                    }));
                    Ok(cursor)
                }
                Err(e) => {
                    self.inner.lock().unwrap().txs[idx].held_count -= 1;
                    Err(e)
                }
            }
        })
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        self.execute_tx(|idx, tx| {
            let result = tx.entries::<T>();
            self.inner.lock().unwrap().txs[idx].held_count -= 1;
            result
        })
    }

    fn disable_long_read_transaction_safety(&mut self) {
        // Do nothing.
    }
}

#[cfg(test)]
mod tests {
    use crate::{mdbx::DatabaseArguments, tables, DatabaseEnv, DatabaseEnvKind};
    use alloy_consensus::Header;
    use reth_db_api::{
        cursor::DbCursorRO,
        database::Database,
        models::ClientVersion,
        transaction::{DbTx, DbTxMut},
    };
    use tempfile::tempdir;

    #[test]
    fn test_parallel_tx_ro() {
        let dir = tempdir().unwrap();
        let args = DatabaseArguments::new(ClientVersion::default());
        let db = DatabaseEnv::open(dir.path(), DatabaseEnvKind::RW, args).unwrap();
        let tx_rw = db.tx_mut().unwrap();
        tx_rw.put::<tables::Headers>(0, Header::default()).unwrap();
        tx_rw.commit().unwrap();

        let tx_ro = db.tx().unwrap();
        std::thread::scope(|s| {
            for _ in 0..16 {
                s.spawn(|| {
                    for _ in 0..100 {
                        let mut cursor = tx_ro.cursor_read::<tables::Headers>().unwrap();
                        let (block_number, header) = cursor.next().unwrap().unwrap();
                        assert_eq!(block_number, 0);
                        assert_eq!(header.number, 0);
                    }
                });
            }
        });
    }
}
