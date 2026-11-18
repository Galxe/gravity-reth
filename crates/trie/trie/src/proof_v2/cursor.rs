//! Read-only factory adapters for reusable proof calculations.

use crate::{
    hashed_cursor::{HashedCursor, HashedCursorFactory, HashedStorageCursor},
    trie_cursor::{TrieCursor, TrieCursorFactory},
    BranchNodeCompact, Nibbles,
};
use alloy_primitives::{B256, U256};
use reth_primitives_traits::Account;
use reth_storage_errors::db::DatabaseError;
use std::fmt;

/// A proof cursor that can restore its original read view before another traversal.
pub trait ResettableTrieCursor: TrieCursor {
    /// Reconstructs the cursor, propagating initialization errors.
    fn reset(&mut self) -> Result<(), DatabaseError>;
}

/// A hashed proof cursor that can restore its original read view.
pub trait ResettableHashedCursor: HashedCursor {
    /// Reconstructs the cursor, propagating initialization errors.
    fn reset(&mut self) -> Result<(), DatabaseError>;
}

/// A proof cursor that can select another account's storage trie.
pub trait TrieStorageCursor: ResettableTrieCursor {
    /// Selects the account, retaining any initialization error for subsequent reads.
    fn set_hashed_address(&mut self, hashed_address: B256) -> Result<(), DatabaseError>;
}

/// A proof cursor that can select another account's hashed storage.
pub trait RebindableHashedStorageCursor: ResettableHashedCursor + HashedStorageCursor {
    /// Selects the account, retaining any initialization error for subsequent reads.
    fn set_hashed_address(&mut self, hashed_address: B256) -> Result<(), DatabaseError>;
}

/// Borrows a hashed factory without adding requirements to the existing factory trait.
pub struct HashedFactoryRef<'a, F>(&'a F);

impl<'a, F> HashedFactoryRef<'a, F> {
    /// Borrows the factory for read-only cursor construction.
    pub const fn new(factory: &'a F) -> Self {
        Self(factory)
    }
}

impl<F> Clone for HashedFactoryRef<'_, F> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

impl<F> fmt::Debug for HashedFactoryRef<'_, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashedFactoryRef").finish_non_exhaustive()
    }
}

impl<F: HashedCursorFactory> HashedCursorFactory for HashedFactoryRef<'_, F> {
    type AccountCursor = F::AccountCursor;
    type StorageCursor = F::StorageCursor;
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        self.0.hashed_account_cursor()
    }
    fn hashed_storage_cursor(&self, address: B256) -> Result<Self::StorageCursor, DatabaseError> {
        self.0.hashed_storage_cursor(address)
    }
}

/// Wraps read-only factories so resets and address changes construct the matching overlay and
/// database cursor together. Failed reinitialization cannot fall back to a cursor at an old
/// address.
#[derive(Clone, Debug)]
pub struct ProofCursorFactory<F>(F);

impl<F> ProofCursorFactory<F> {
    /// Wraps an existing factory without opening cursors.
    pub const fn new(factory: F) -> Self {
        Self(factory)
    }
}

impl<F: TrieCursorFactory + Clone + Send + Sync> TrieCursorFactory for ProofCursorFactory<F> {
    type AccountTrieCursor = FactoryAccountTrieCursor<F>;
    type StorageTrieCursor = FactoryStorageTrieCursor<F>;
    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        let cursor = self.0.account_trie_cursor()?;
        Ok(FactoryAccountTrieCursor { factory: self.0.clone(), cursor: Ok(cursor) })
    }
    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor, DatabaseError> {
        let cursor = self.0.storage_trie_cursor(hashed_address)?;
        Ok(FactoryStorageTrieCursor { factory: self.0.clone(), hashed_address, cursor: Ok(cursor) })
    }
}

impl<F: HashedCursorFactory + Clone> HashedCursorFactory for ProofCursorFactory<F> {
    type AccountCursor = FactoryHashedAccountCursor<F>;
    type StorageCursor = FactoryHashedStorageCursor<F>;
    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        let cursor = self.0.hashed_account_cursor()?;
        Ok(FactoryHashedAccountCursor { factory: self.0.clone(), cursor: Ok(cursor) })
    }
    fn hashed_storage_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageCursor, DatabaseError> {
        let cursor = self.0.hashed_storage_cursor(hashed_address)?;
        Ok(FactoryHashedStorageCursor {
            factory: self.0.clone(),
            hashed_address,
            cursor: Ok(cursor),
        })
    }
}

/// A read-only cursor supplied by a factory: [`FactoryAccountTrieCursor`].
pub struct FactoryAccountTrieCursor<F: TrieCursorFactory> {
    factory: F,
    cursor: Result<F::AccountTrieCursor, DatabaseError>,
}
impl<F: TrieCursorFactory> FactoryAccountTrieCursor<F> {
    fn cursor(&mut self) -> Result<&mut F::AccountTrieCursor, DatabaseError> {
        self.cursor.as_mut().map_err(|error| error.clone())
    }
}
impl<F: TrieCursorFactory> fmt::Debug for FactoryAccountTrieCursor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FactoryAccountTrieCursor").finish_non_exhaustive()
    }
}
impl<F: TrieCursorFactory + Send + Sync> ResettableTrieCursor for FactoryAccountTrieCursor<F> {
    fn reset(&mut self) -> Result<(), DatabaseError> {
        self.cursor = self.factory.account_trie_cursor();
        self.cursor.as_ref().map(|_| ()).map_err(|error| error.clone())
    }
}
impl<F: TrieCursorFactory + Send + Sync> TrieCursor for FactoryAccountTrieCursor<F> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.cursor()?.seek_exact(key)
    }
    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.cursor()?.seek(key)
    }
    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.cursor()?.next()
    }
    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        self.cursor()?.current()
    }
}
/// A read-only cursor supplied by a factory: [`FactoryStorageTrieCursor`].
pub struct FactoryStorageTrieCursor<F: TrieCursorFactory> {
    factory: F,
    hashed_address: B256,
    cursor: Result<F::StorageTrieCursor, DatabaseError>,
}
impl<F: TrieCursorFactory> FactoryStorageTrieCursor<F> {
    fn cursor(&mut self) -> Result<&mut F::StorageTrieCursor, DatabaseError> {
        self.cursor.as_mut().map_err(|error| error.clone())
    }
}
impl<F: TrieCursorFactory> fmt::Debug for FactoryStorageTrieCursor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FactoryStorageTrieCursor").finish_non_exhaustive()
    }
}
impl<F: TrieCursorFactory + Send + Sync> ResettableTrieCursor for FactoryStorageTrieCursor<F> {
    fn reset(&mut self) -> Result<(), DatabaseError> {
        self.cursor = self.factory.storage_trie_cursor(self.hashed_address);
        self.cursor.as_ref().map(|_| ()).map_err(|error| error.clone())
    }
}
impl<F: TrieCursorFactory + Send + Sync> TrieCursor for FactoryStorageTrieCursor<F> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.cursor()?.seek_exact(key)
    }
    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.cursor()?.seek(key)
    }
    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        self.cursor()?.next()
    }
    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        self.cursor()?.current()
    }
}
impl<F: TrieCursorFactory + Send + Sync> TrieStorageCursor for FactoryStorageTrieCursor<F> {
    fn set_hashed_address(&mut self, hashed_address: B256) -> Result<(), DatabaseError> {
        self.hashed_address = hashed_address;
        self.reset()
    }
}
/// A read-only cursor supplied by a factory: [`FactoryHashedAccountCursor`].
pub struct FactoryHashedAccountCursor<F: HashedCursorFactory> {
    factory: F,
    cursor: Result<F::AccountCursor, DatabaseError>,
}
impl<F: HashedCursorFactory> FactoryHashedAccountCursor<F> {
    fn cursor(&mut self) -> Result<&mut F::AccountCursor, DatabaseError> {
        self.cursor.as_mut().map_err(|error| error.clone())
    }
}
impl<F: HashedCursorFactory> fmt::Debug for FactoryHashedAccountCursor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FactoryHashedAccountCursor").finish_non_exhaustive()
    }
}
impl<F: HashedCursorFactory> ResettableHashedCursor for FactoryHashedAccountCursor<F> {
    fn reset(&mut self) -> Result<(), DatabaseError> {
        self.cursor = self.factory.hashed_account_cursor();
        self.cursor.as_ref().map(|_| ()).map_err(|error| error.clone())
    }
}
impl<F: HashedCursorFactory> HashedCursor for FactoryHashedAccountCursor<F> {
    type Value = Account;
    fn seek(&mut self, key: B256) -> Result<Option<(B256, Account)>, DatabaseError> {
        self.cursor()?.seek(key)
    }
    fn next(&mut self) -> Result<Option<(B256, Account)>, DatabaseError> {
        self.cursor()?.next()
    }
}
/// A read-only cursor supplied by a factory: [`FactoryHashedStorageCursor`].
pub struct FactoryHashedStorageCursor<F: HashedCursorFactory> {
    factory: F,
    hashed_address: B256,
    cursor: Result<F::StorageCursor, DatabaseError>,
}
impl<F: HashedCursorFactory> FactoryHashedStorageCursor<F> {
    fn cursor(&mut self) -> Result<&mut F::StorageCursor, DatabaseError> {
        self.cursor.as_mut().map_err(|error| error.clone())
    }
}
impl<F: HashedCursorFactory> fmt::Debug for FactoryHashedStorageCursor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FactoryHashedStorageCursor").finish_non_exhaustive()
    }
}
impl<F: HashedCursorFactory> ResettableHashedCursor for FactoryHashedStorageCursor<F> {
    fn reset(&mut self) -> Result<(), DatabaseError> {
        self.cursor = self.factory.hashed_storage_cursor(self.hashed_address);
        self.cursor.as_ref().map(|_| ()).map_err(|error| error.clone())
    }
}
impl<F: HashedCursorFactory> HashedCursor for FactoryHashedStorageCursor<F> {
    type Value = U256;
    fn seek(&mut self, key: B256) -> Result<Option<(B256, U256)>, DatabaseError> {
        self.cursor()?.seek(key)
    }
    fn next(&mut self) -> Result<Option<(B256, U256)>, DatabaseError> {
        self.cursor()?.next()
    }
}
impl<F: HashedCursorFactory> HashedStorageCursor for FactoryHashedStorageCursor<F> {
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        self.cursor()?.is_storage_empty()
    }
}
impl<F: HashedCursorFactory> RebindableHashedStorageCursor for FactoryHashedStorageCursor<F> {
    fn set_hashed_address(&mut self, hashed_address: B256) -> Result<(), DatabaseError> {
        self.hashed_address = hashed_address;
        self.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hashed_cursor::{
            noop::{NoopHashedAccountCursor, NoopHashedCursorFactory, NoopHashedStorageCursor},
            HashedPostStateCursorFactory,
        },
        proof_v2::{StorageProofCalculator, StorageValueEncoder},
        trie_cursor::noop::{NoopAccountTrieCursor, NoopStorageTrieCursor, NoopTrieCursorFactory},
    };
    use alloy_primitives::map::B256Map;
    use reth_trie_common::{HashedPostState, HashedStorage, EMPTY_ROOT_HASH};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    #[test]
    fn root_node_reuse_with_overlay_and_multiple_addresses() {
        let [a, b, missing] = [1, 2, 3].map(B256::repeat_byte);
        let slots_a =
            [(B256::repeat_byte(0x10), U256::from(1)), (B256::repeat_byte(0x20), U256::from(2))];
        let slots_b = [(B256::repeat_byte(0x30), U256::from(3))];
        let state = HashedPostState {
            storages: B256Map::from_iter([
                (a, HashedStorage::from_iter(true, slots_a)),
                (b, HashedStorage::from_iter(true, slots_b)),
            ]),
            ..Default::default()
        }
        .into_sorted();
        let trie_factory = ProofCursorFactory::new(NoopTrieCursorFactory);
        let hashed_factory = ProofCursorFactory::new(HashedPostStateCursorFactory::new(
            NoopHashedCursorFactory,
            &state,
        ));
        let mut calculator = StorageProofCalculator::new_storage(
            trie_factory.storage_trie_cursor(a).unwrap(),
            hashed_factory.hashed_storage_cursor(a).unwrap(),
        );
        for (address, expected) in [
            (a, crate::test_utils::storage_root_prehashed(slots_a)),
            (b, crate::test_utils::storage_root_prehashed(slots_b)),
            (missing, EMPTY_ROOT_HASH),
            (a, crate::test_utils::storage_root_prehashed(slots_a)),
        ] {
            let root = calculator.storage_root_node(address).unwrap();
            assert_eq!(
                calculator.compute_root_hash(core::slice::from_ref(&root)).unwrap(),
                Some(expected)
            );
            let again = calculator.root_node(&mut StorageValueEncoder).unwrap();
            assert_eq!(root, again);
        }
    }

    #[derive(Clone, Debug, Default)]
    struct FailingFactory(Arc<AtomicBool>);

    impl FailingFactory {
        fn check(&self) -> Result<(), DatabaseError> {
            if self.0.load(Ordering::Relaxed) {
                Err(DatabaseError::Other("factory failed".into()))
            } else {
                Ok(())
            }
        }
    }

    impl TrieCursorFactory for FailingFactory {
        type AccountTrieCursor = NoopAccountTrieCursor;
        type StorageTrieCursor = NoopStorageTrieCursor;
        fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
            self.check()?;
            Ok(Default::default())
        }
        fn storage_trie_cursor(&self, _: B256) -> Result<Self::StorageTrieCursor, DatabaseError> {
            self.check()?;
            Ok(Default::default())
        }
    }

    impl HashedCursorFactory for FailingFactory {
        type AccountCursor = NoopHashedAccountCursor;
        type StorageCursor = NoopHashedStorageCursor;
        fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
            self.check()?;
            Ok(Default::default())
        }
        fn hashed_storage_cursor(&self, _: B256) -> Result<Self::StorageCursor, DatabaseError> {
            self.check()?;
            Ok(Default::default())
        }
    }

    #[test]
    fn failed_reset_and_rebind_remain_read_errors() {
        let factory = FailingFactory::default();
        let proof = ProofCursorFactory::new(factory.clone());
        let mut account_trie = proof.account_trie_cursor().unwrap();
        let mut account_hashed = proof.hashed_account_cursor().unwrap();
        let mut storage_trie = proof.storage_trie_cursor(B256::ZERO).unwrap();
        let mut storage_hashed = proof.hashed_storage_cursor(B256::ZERO).unwrap();
        factory.0.store(true, Ordering::Relaxed);
        assert!(account_trie.reset().is_err());
        assert!(account_hashed.reset().is_err());
        assert!(storage_trie.set_hashed_address(B256::repeat_byte(1)).is_err());
        assert!(storage_hashed.set_hashed_address(B256::repeat_byte(1)).is_err());
        assert!(account_trie.seek(Nibbles::default()).is_err());
        assert!(account_hashed.seek(B256::ZERO).is_err());
        assert!(storage_trie.current().is_err());
        assert!(storage_hashed.is_storage_empty().is_err());
        factory.0.store(false, Ordering::Relaxed);
        account_trie.reset().unwrap();
        account_hashed.reset().unwrap();
        storage_trie.reset().unwrap();
        storage_hashed.reset().unwrap();
        assert!(account_trie.seek(Nibbles::default()).unwrap().is_none());
        assert!(account_hashed.seek(B256::ZERO).unwrap().is_none());
        assert!(storage_trie.seek(Nibbles::default()).unwrap().is_none());
        assert!(storage_hashed.is_storage_empty().unwrap());
    }
}
