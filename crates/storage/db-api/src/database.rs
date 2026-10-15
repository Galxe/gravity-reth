use crate::{
    table::TableImporter,
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use std::{any::Any, fmt::Debug, sync::Arc};

/// Main Database trait that can open read-only and read-write transactions.
///
/// Sealed trait which cannot be implemented by 3rd parties, exposed only for consumption.
pub trait Database: Send + Sync + Debug {
    /// Read-Only database transaction
    type TX: DbTx + Send + Sync + Debug + 'static;
    /// Read-Write database transaction
    type TXMut: DbTxMut + DbTx + TableImporter + Send + Sync + Debug + 'static;

    /// Create read only transaction.
    /// Read visibility follows the backend. Use [`Self::snapshot`] and [`Self::tx_with_snapshot`]
    /// to reuse a captured database version.
    #[track_caller]
    fn tx(&self) -> Result<Self::TX, DatabaseError>;

    /// Capture a snapshot to reuse across read transactions. When `consistent` is true,
    /// coordinate capture across all physical stores with writes; otherwise capture only the
    /// primary store. Backends without shareable snapshots return `None`.
    fn snapshot(
        &self,
        _consistent: bool,
    ) -> Result<Option<Arc<dyn DatabaseSnapshot>>, DatabaseError> {
        Ok(None)
    }

    /// Open a read transaction on a captured snapshot. `None` captures a fresh primary snapshot.
    ///
    /// Reusing the snapshot keeps separately opened transactions at the same database versions.
    /// Backends with shareable snapshots reject one belonging to another database.
    fn tx_with_snapshot(
        &self,
        _snapshot: Option<Arc<dyn DatabaseSnapshot>>,
    ) -> Result<Self::TX, DatabaseError> {
        self.tx()
    }

    /// Exclude coordinated snapshot capture while committing one logical update across physical
    /// stores. State-only snapshots remain independent of this coordination.
    fn with_write_lock<T>(&self, f: impl FnOnce() -> T) -> T {
        f()
    }

    /// Create read write transaction only possible if database is open with write access.
    #[track_caller]
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError>;

    /// Takes a function and passes a read-only transaction into it, making sure it's closed in the
    /// end of the execution.
    /// Uses [`Self::tx`] and its visibility rules.
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

/// An immutable, backend-specific database snapshot shared by read transactions.
pub trait DatabaseSnapshot: Any + Debug + Send + Sync {
    /// Exposes the backend snapshot type for downcasting.
    fn as_any(&self) -> &dyn Any;
}

impl<DB: Database> Database for Arc<DB> {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn snapshot(
        &self,
        consistent: bool,
    ) -> Result<Option<Arc<dyn DatabaseSnapshot>>, DatabaseError> {
        <DB as Database>::snapshot(self, consistent)
    }

    fn with_write_lock<T>(&self, f: impl FnOnce() -> T) -> T {
        <DB as Database>::with_write_lock(self, f)
    }

    fn tx_with_snapshot(
        &self,
        snapshot: Option<Arc<dyn DatabaseSnapshot>>,
    ) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx_with_snapshot(self, snapshot)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }
}

impl<DB: Database> Database for &DB {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn snapshot(
        &self,
        consistent: bool,
    ) -> Result<Option<Arc<dyn DatabaseSnapshot>>, DatabaseError> {
        <DB as Database>::snapshot(self, consistent)
    }

    fn with_write_lock<T>(&self, f: impl FnOnce() -> T) -> T {
        <DB as Database>::with_write_lock(self, f)
    }

    fn tx_with_snapshot(
        &self,
        snapshot: Option<Arc<dyn DatabaseSnapshot>>,
    ) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx_with_snapshot(self, snapshot)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }
}
