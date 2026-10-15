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
    #[track_caller]
    fn tx(&self) -> Result<Self::TX, DatabaseError>;

    /// Capture a state snapshot that can be shared by multiple read transactions.
    ///
    /// Backends without shareable snapshots return `None`. Separately sharded trie databases
    /// and external static files are not covered by this view.
    fn state_read_view(&self) -> Result<Option<Arc<dyn DatabaseReadView>>, DatabaseError> {
        Ok(None)
    }

    /// Open a read transaction using a captured state snapshot, or capture a new snapshot.
    ///
    /// Backends with shareable views reject a view belonging to another database. Backends whose
    /// ordinary read transactions already provide snapshots can use the default.
    fn tx_with_state_read_view(
        &self,
        _view: Option<Arc<dyn DatabaseReadView>>,
    ) -> Result<Self::TX, DatabaseError> {
        self.tx()
    }

    /// Create read write transaction only possible if database is open with write access.
    #[track_caller]
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError>;

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

/// An immutable, backend-specific state snapshot shared by read transactions.
pub trait DatabaseReadView: Any + Debug + Send + Sync {
    /// Backend-specific view identity, used to reject a view from another database.
    fn as_any(&self) -> &dyn Any;
}

impl<DB: Database> Database for Arc<DB> {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn state_read_view(&self) -> Result<Option<Arc<dyn DatabaseReadView>>, DatabaseError> {
        <DB as Database>::state_read_view(self)
    }

    fn tx_with_state_read_view(
        &self,
        view: Option<Arc<dyn DatabaseReadView>>,
    ) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx_with_state_read_view(self, view)
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

    fn state_read_view(&self) -> Result<Option<Arc<dyn DatabaseReadView>>, DatabaseError> {
        <DB as Database>::state_read_view(self)
    }

    fn tx_with_state_read_view(
        &self,
        view: Option<Arc<dyn DatabaseReadView>>,
    ) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx_with_state_read_view(self, view)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }
}
