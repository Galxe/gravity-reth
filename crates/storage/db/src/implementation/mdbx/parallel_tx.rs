//! A set of read-only transactions that can be used to read data from the database in parallel.

use super::{cursor::Cursor, tx::Tx, Environment, RO};
use crate::{metrics::DatabaseEnvMetrics, DatabaseError};
use reth_db_api::{
    table::{DupSort, Encode, Table},
    transaction::DbTx,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, MutexGuard,
};

const FIRST_TX_INDEX: usize = usize::MAX;

/// A set of read-only transactions that can be used to read data from the database in parallel.
#[derive(Debug)]
pub struct ParallelTxRO {
    // Quick path for no-parallelism scenario.
    pub(super) tx: Tx<RO>,
    tx_held_count: Arc<AtomicUsize>,
    // Auxiliary txs for parallelism.
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

fn create_tx(
    env: &Environment,
    metrics: Option<Arc<DatabaseEnvMetrics>>,
) -> Result<Tx<RO>, DatabaseError> {
    Tx::new_with_metrics(env.begin_ro_txn().map_err(|e| DatabaseError::InitTx(e.into()))?, metrics)
        .map_err(|e| DatabaseError::InitTx(e.into()))
}

impl ParallelTxRO {
    pub(super) fn try_new(
        env: Environment,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Result<Self, DatabaseError> {
        let tx = create_tx(&env, metrics.clone())?;
        Ok(Self {
            tx,
            tx_held_count: Arc::new(AtomicUsize::new(0)),
            inner: Arc::new(Mutex::new(Inner { txs: vec![], num_txs: 1 })),
            max_txs: 8,
            env,
            metrics,
        })
    }

    fn create_aux_tx(
        &self,
        mut inner: MutexGuard<'_, Inner>,
    ) -> Result<(usize, Arc<Tx<RO>>), DatabaseError> {
        inner.num_txs += 1;
        drop(inner);
        match create_tx(&self.env, self.metrics.clone()) {
            Ok(tx) => {
                let tx = Arc::new(tx);
                let tx_clone: Arc<Tx<RO>> = tx.clone();
                let mut inner = self.inner.lock().unwrap();
                inner.txs.push(WrappedTx { tx, held_count: 1 });
                Ok((inner.txs.len() - 1, tx_clone))
            }
            Err(e) => {
                self.inner.lock().unwrap().num_txs -= 1;
                Err(e)
            }
        }
    }

    fn execute_tx<R>(
        &self,
        f: impl FnOnce(usize, &Tx<RO>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        // Quick path for no-parallelism scenario.
        if self.tx_held_count.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return f(FIRST_TX_INDEX, &self.tx);
        }

        // Use auxiliary txs.
        let mut inner = self.inner.lock().unwrap();
        let (index, tx) = {
            // Find the tx with the lowest held_count
            let num_txs = inner.num_txs;

            if let Some((index, wrapped_tx)) =
                inner.txs.iter_mut().enumerate().min_by_key(|(_, tx)| tx.held_count)
            {
                if wrapped_tx.held_count > 0 && num_txs < self.max_txs {
                    // Create a new tx and add it to the inner txs
                    self.create_aux_tx(inner)?
                } else if self.tx_held_count.load(Ordering::Acquire) >= wrapped_tx.held_count {
                    // Use the existing auxiliary tx.
                    wrapped_tx.held_count += 1;
                    let tx = wrapped_tx.tx.clone();
                    drop(inner);
                    (index, tx)
                } else {
                    // Use the first tx.
                    drop(inner);
                    self.tx_held_count.fetch_add(1, Ordering::AcqRel);
                    return f(FIRST_TX_INDEX, &self.tx);
                }
            } else if num_txs < self.max_txs {
                // Create a new tx and add it to the inner txs
                self.create_aux_tx(inner)?
            } else {
                // Use the first tx.
                drop(inner);
                self.tx_held_count.fetch_add(1, Ordering::AcqRel);
                return f(FIRST_TX_INDEX, &self.tx);
            }
        };

        f(index, &tx)
    }

    /// Opens a handle to an MDBX database.
    pub fn open_db(&self, name: Option<&str>) -> reth_libmdbx::Result<reth_libmdbx::Database> {
        self.tx.inner.open_db(name)
    }

    /// Retrieves database statistics.
    pub fn db_stat(&self, db: &reth_libmdbx::Database) -> reth_libmdbx::Result<reth_libmdbx::Stat> {
        self.tx.inner.db_stat(db)
    }

    /// Returns a raw pointer to the MDBX environment.
    pub const fn env(&self) -> &Environment {
        &self.env
    }

    fn post_cursor_creation<T: Table>(
        &self,
        idx: usize,
        cursor: Result<Cursor<RO, T>, DatabaseError>,
    ) -> Result<Cursor<RO, T>, DatabaseError> {
        match cursor {
            Ok(mut cursor) => {
                if idx == FIRST_TX_INDEX {
                    let tx_held_count = self.tx_held_count.clone();
                    cursor.with_drop_fn(Box::new(move |_| {
                        tx_held_count.fetch_sub(1, Ordering::AcqRel);
                    }));
                } else {
                    let inner = self.inner.clone();
                    cursor.with_drop_fn(Box::new(move |_| {
                        inner.lock().unwrap().txs[idx].held_count -= 1;
                    }));
                }
                Ok(cursor)
            }
            Err(e) => {
                self.post_tx_execution(idx);
                Err(e)
            }
        }
    }

    fn post_tx_execution(&self, idx: usize) {
        if idx == FIRST_TX_INDEX {
            self.tx_held_count.fetch_sub(1, Ordering::AcqRel);
        } else {
            self.inner.lock().unwrap().txs[idx].held_count -= 1;
        }
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
            self.post_tx_execution(idx);
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
            self.post_cursor_creation(idx, result)
        })
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.execute_tx(|idx, tx| {
            let result = tx.cursor_dup_read::<T>();
            self.post_cursor_creation(idx, result)
        })
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        self.execute_tx(|idx, tx| {
            let result = tx.entries::<T>();
            self.post_tx_execution(idx);
            result
        })
    }

    fn disable_long_read_transaction_safety(&mut self) {
        // Do nothing.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{mdbx::DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
    use reth_db_api::{database::Database, models::ClientVersion};
    use tempfile::tempdir;

    #[test]
    fn test_parallel_tx_ro() {
        let first_tx_access_count = Arc::new(AtomicUsize::new(0));
        let mut aux_tx_access_count = Vec::<AtomicUsize>::new();
        for _ in 0..7 {
            aux_tx_access_count.push(AtomicUsize::new(0));
        }
        let aux_tx_access_count = Arc::new(aux_tx_access_count);

        let dir = tempdir().unwrap();
        let args = DatabaseArguments::new(ClientVersion::default());
        let db = DatabaseEnv::open(dir.path(), DatabaseEnvKind::RW, args).unwrap();
        let tx_ro = db.tx().unwrap();
        std::thread::scope(|s| {
            for _ in 0..16 {
                s.spawn(|| {
                    for _ in 0..10000 {
                        let _ = tx_ro.execute_tx(|idx, _| {
                            std::thread::sleep(std::time::Duration::from_micros(50));
                            if idx == FIRST_TX_INDEX {
                                first_tx_access_count.fetch_add(1, Ordering::Relaxed);
                            } else {
                                aux_tx_access_count[idx].fetch_add(1, Ordering::Relaxed);
                            }
                            tx_ro.post_tx_execution(idx);
                            Ok(())
                        });
                    }
                });
            }
        });

        println!("first_tx_access_count: {}", first_tx_access_count.load(Ordering::Relaxed));
        println!("aux_tx_access_count: {aux_tx_access_count:?}");
        println!("tx: {tx_ro:?}");
    }
}
