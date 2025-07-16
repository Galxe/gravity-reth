//! A set of read-only transactions that can be used to read data from the database in parallel.

use super::{cursor::Cursor, tx::Tx, Environment, RO};
use crate::{metrics::DatabaseEnvMetrics, DatabaseError};
use reth_db_api::{
    table::{DupSort, Encode, Table},
    transaction::DbTx,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};

///
#[derive(Debug)]
pub struct ParallelTxRO {
    is_tx_busy: AtomicBool,
    pub(super) tx: Tx<RO>, // FIXME: Make it private.
    env: Environment,
    metrics: Option<Arc<DatabaseEnvMetrics>>,
    max_txs: usize,
    mut_state: Mutex<MutState>,
    condvar: Condvar,
}

#[derive(Debug)]
struct MutState {
    num_txs: usize,
    idle_txs: Vec<Tx<RO>>,
}

impl ParallelTxRO {
    pub(super) fn try_new(
        env: Environment,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Result<Self, DatabaseError> {
        let tx = Self::create_tx(&env, metrics.clone())?;
        Ok(Self {
            tx,
            is_tx_busy: AtomicBool::new(false),
            max_txs: 8,
            env,
            metrics,
            mut_state: Mutex::new(MutState { num_txs: 1, idle_txs: vec![] }),
            condvar: Condvar::new(),
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

    fn tx_execute<R>(
        &self,
        f: impl FnOnce(&Tx<RO>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        if self
            .is_tx_busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // If the tx is not busy, we can execute the function directly.
            let result = f(&self.tx);
            self.is_tx_busy.store(false, Ordering::Release);
            self.condvar.notify_one();
            return result;
        }

        let mut state = self.mut_state.lock().unwrap();
        loop {
            if !state.idle_txs.is_empty() {
                // If there are idle txs, we can use one of them.
                let tx = state.idle_txs.pop().unwrap();
                drop(state);
                let result = f(&tx);
                self.mut_state.lock().unwrap().idle_txs.push(tx);
                self.condvar.notify_one();
                return result;
            }

            if state.num_txs < self.max_txs {
                // If the number of txs is less than the max number of txs, we can create a new tx.
                state.num_txs += 1;
                drop(state);
                match Self::create_tx(&self.env, self.metrics.clone()) {
                    Ok(tx) => {
                        let result = f(&tx);
                        self.mut_state.lock().unwrap().idle_txs.push(tx);
                        self.condvar.notify_one();
                        return result;
                    }
                    Err(e) => {
                        self.mut_state.lock().unwrap().num_txs -= 1;
                        self.condvar.notify_one();
                        return Err(e);
                    }
                }
            }

            drop(state);
            if self
                .is_tx_busy
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // If the tx is not busy, we can execute the function directly.
                let result = f(&self.tx);
                self.is_tx_busy.store(false, Ordering::Release);
                self.condvar.notify_one();
                return result;
            }

            state = self.condvar.wait(self.mut_state.lock().unwrap()).unwrap();
        }
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
    pub fn env(&self) -> &Environment {
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
        self.tx_execute(|tx| tx.get_by_encoded_key::<T>(key))
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        // Do nothing.
        Ok(true)
    }

    fn abort(self) {
        // Do nothing.
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.tx_execute(|tx| tx.cursor_read::<T>())
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.tx_execute(|tx| tx.cursor_dup_read::<T>())
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        self.tx_execute(|tx| tx.entries::<T>())
    }

    fn disable_long_read_transaction_safety(&mut self) {
        // Do nothing.
    }
}
