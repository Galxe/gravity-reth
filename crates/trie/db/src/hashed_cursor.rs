use alloy_primitives::{B256, U256};
use reth_db::tables;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    transaction::DbTx,
};
use reth_primitives_traits::Account;
use reth_trie::hashed_cursor::{HashedCursor, HashedCursorFactory, HashedStorageCursor};

use crate::{NoopTrieCacheReader, TrieCacheReader};

/// A struct wrapping database transaction that implements [`HashedCursorFactory`].
#[derive(Debug)]
pub struct DatabaseHashedCursorFactory<'a, TX, CR = NoopTrieCacheReader>(&'a TX, Option<CR>);

impl<TX, CR: Clone> Clone for DatabaseHashedCursorFactory<'_, TX, CR> {
    fn clone(&self) -> Self {
        Self(self.0, self.1.clone())
    }
}

impl<'a, TX> DatabaseHashedCursorFactory<'a, TX> {
    /// Create new database hashed cursor factory.
    pub const fn new(tx: &'a TX) -> Self {
        Self(tx, None)
    }
}

impl<'a, TX, CR> DatabaseHashedCursorFactory<'a, TX, CR> {
    /// Create new database hashed cursor factory.
    pub const fn with_cache(tx: &'a TX, cache: Option<CR>) -> Self {
        Self(tx, cache)
    }
}

impl<TX: DbTx, CR: TrieCacheReader> HashedCursorFactory
    for DatabaseHashedCursorFactory<'_, TX, CR>
{
    type AccountCursor =
        DatabaseHashedAccountCursor<<TX as DbTx>::Cursor<tables::HashedAccounts>, CR>;
    type StorageCursor =
        DatabaseHashedStorageCursor<<TX as DbTx>::DupCursor<tables::HashedStorages>, CR>;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, reth_db::DatabaseError> {
        Ok(DatabaseHashedAccountCursor::with_cache(
            self.0.cursor_read::<tables::HashedAccounts>()?,
            self.1.clone(),
        ))
    }

    fn hashed_storage_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageCursor, reth_db::DatabaseError> {
        Ok(DatabaseHashedStorageCursor::with_cache(
            self.0.cursor_dup_read::<tables::HashedStorages>()?,
            self.1.clone(),
            hashed_address,
        ))
    }
}

/// A struct wrapping database cursor over hashed accounts implementing [`HashedCursor`] for
/// iterating over accounts.
#[derive(Debug)]
pub struct DatabaseHashedAccountCursor<C, R = NoopTrieCacheReader>(C, Option<R>);

impl<C> DatabaseHashedAccountCursor<C> {
    /// Create new database hashed account cursor.
    pub const fn new(cursor: C) -> Self {
        Self(cursor, None)
    }
}

impl<C, R> DatabaseHashedAccountCursor<C, R> {
    /// Create new database hashed account cursor.
    pub const fn with_cache(cursor: C, cache: Option<R>) -> Self {
        Self(cursor, cache)
    }
}

impl<C, R> HashedCursor for DatabaseHashedAccountCursor<C, R>
where
    C: DbCursorRO<tables::HashedAccounts>,
    R: TrieCacheReader,
{
    type Value = Account;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        if let Some(cache) = &self.1 {
            if let Some(acount) = cache.hashed_account(key) {
                return Ok(Some((key, acount)));
            }
        }
        self.0.seek(key)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        self.0.next()
    }
}

/// The structure wrapping a database cursor for hashed storage and
/// a target hashed address. Implements [`HashedCursor`] and [`HashedStorageCursor`]
/// for iterating over hashed storage.
#[derive(Debug)]
pub struct DatabaseHashedStorageCursor<C, R = NoopTrieCacheReader> {
    /// Database hashed storage cursor.
    cursor: C,
    cache: Option<R>,
    /// Target hashed address of the account that the storage belongs to.
    hashed_address: B256,
}

impl<C> DatabaseHashedStorageCursor<C> {
    /// Create new [`DatabaseHashedStorageCursor`].
    pub const fn new(cursor: C, hashed_address: B256) -> Self {
        Self { cursor, cache: None, hashed_address }
    }
}

impl<C, R> DatabaseHashedStorageCursor<C, R> {
    /// Create new [`DatabaseHashedStorageCursor`].
    pub const fn with_cache(cursor: C, cache: Option<R>, hashed_address: B256) -> Self {
        Self { cursor, cache, hashed_address }
    }
}

impl<C, R> HashedCursor for DatabaseHashedStorageCursor<C, R>
where
    C: DbCursorRO<tables::HashedStorages> + DbDupCursorRO<tables::HashedStorages>,
    R: TrieCacheReader,
{
    type Value = U256;

    fn seek(
        &mut self,
        subkey: B256,
    ) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.hashed_storage(self.hashed_address, subkey) {
                return Ok(Some((subkey, value)));
            }
        }
        Ok(self.cursor.seek_by_key_subkey(self.hashed_address, subkey)?.map(|e| (e.key, e.value)))
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        Ok(self.cursor.next_dup_val()?.map(|e| (e.key, e.value)))
    }
}

impl<C, R> HashedStorageCursor for DatabaseHashedStorageCursor<C, R>
where
    C: DbCursorRO<tables::HashedStorages> + DbDupCursorRO<tables::HashedStorages>,
    R: TrieCacheReader,
{
    fn is_storage_empty(&mut self) -> Result<bool, reth_db::DatabaseError> {
        Ok(self.cursor.seek_exact(self.hashed_address)?.is_none())
    }
}
