use crate::{
    table::TableImporter,
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use std::{fmt::Debug, sync::Arc};

/// Marks a coordinated write range complete. Dropping an unfinished guard tells backends that
/// may have committed part of the range not to serve a mixed read view.
pub trait ConsistentWriteGuard {
    /// The complete logical write range has been committed successfully.
    fn complete(&mut self);

    /// Recovery has repaired any previously interrupted range.
    fn recovered(&mut self) {
        self.complete();
    }
}

impl ConsistentWriteGuard for () {
    fn complete(&mut self) {}
}

/// Main Database trait that can open read-only and read-write transactions.
///
/// Sealed trait which cannot be implemented by 3rd parties, exposed only for consumption.
pub trait Database: Send + Sync + Debug {
    /// Read-Only database transaction
    type TX: DbTx + Send + Sync + Debug + 'static;
    /// Read-Write database transaction
    type TXMut: DbTxMut + DbTx + TableImporter + Send + Sync + Debug + 'static;

    /// Create read only transaction.
    #[track_caller]
    fn tx(&self) -> Result<Self::TX, DatabaseError>;

    /// Create a read-only transaction that observes subsequent database commits.
    ///
    /// Backends without a separate snapshot mode may use their normal read transaction.
    #[track_caller]
    fn tx_live(&self) -> Result<Self::TX, DatabaseError> {
        self.tx()
    }

    /// Create read write transaction only possible if database is open with write access.
    #[track_caller]
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError>;

    /// Coordinate all commits of one logical state/trie update with read snapshot creation.
    fn consistent_write(&self) -> Box<dyn ConsistentWriteGuard + '_> {
        Box::new(())
    }

    /// Takes a function and passes a read-only transaction into it, making sure it's closed in the
    /// end of the execution.
    fn view<T, F>(&self, f: F) -> Result<T, DatabaseError>
    where
        F: FnOnce(&Self::TX) -> T,
    {
        let tx = self.tx()?;

        let res = f(&tx);
        tx.commit()?;

        Ok(res)
    }

    /// Takes a function and passes a write-read transaction into it, making sure it's committed in
    /// the end of the execution.
    fn update<T, F>(&self, f: F) -> Result<T, DatabaseError>
    where
        F: FnOnce(&Self::TXMut) -> T,
    {
        let tx = self.tx_mut()?;

        let res = f(&tx);
        tx.commit()?;

        Ok(res)
    }
}

impl<DB: Database> Database for Arc<DB> {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn tx_live(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx_live(self)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }

    fn consistent_write(&self) -> Box<dyn ConsistentWriteGuard + '_> {
        <DB as Database>::consistent_write(self)
    }
}

impl<DB: Database> Database for &DB {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn tx_live(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx_live(self)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }

    fn consistent_write(&self) -> Box<dyn ConsistentWriteGuard + '_> {
        <DB as Database>::consistent_write(self)
    }
}
