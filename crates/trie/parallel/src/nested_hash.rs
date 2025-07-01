use std::sync::Mutex;

use alloy_primitives::{
    map::{B256Map, HashMap},
    B256, KECCAK256_EMPTY,
};
use alloy_rlp::encode_fixed_size;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};

use reth_primitives_traits::Account;
use reth_provider::{DBProvider, PersistBlockCache, ProviderResult};
use reth_storage_errors::db::DatabaseError;
use reth_trie::{
    nested_trie::{Node, Trie, TrieReader},
    updates::StorageTrieUpdates,
    HashedPostState, HashedStorage, Nibbles, StorageTrieUpdatesV2, StoredNibbles,
    StoredNibblesSubKey, EMPTY_ROOT_HASH,
};
use reth_trie_common::updates::{TrieUpdates, TrieUpdatesV2};

/// Storage trie node reader
#[allow(missing_debug_implementations)]
pub struct StorageTrieReader<C> {
    hashed_address: B256,
    cursor: C,
    cache: Option<PersistBlockCache>,
}

impl<C> StorageTrieReader<C> {
    /// Create a new `StorageTrieReader`
    pub fn new(cursor: C, hashed_address: B256, cache: Option<PersistBlockCache>) -> Self {
        Self { cursor, hashed_address, cache }
    }
}

impl<C> TrieReader for StorageTrieReader<C>
where
    C: DbCursorRO<tables::StoragesTrieV2> + DbDupCursorRO<tables::StoragesTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
        if let Some(cache) = &self.cache {
            let value = cache.trie_storage(&self.hashed_address, &path);
            if value.is_some() {
                return Ok(value);
            }
        }
        let path = StoredNibblesSubKey(path.clone());
        Ok(self
            .cursor
            .seek_by_key_subkey(self.hashed_address, path.clone())?
            .filter(|e| e.path == path)
            .map(|e| e.node.into()))
    }
}

/// Account trie node reader
#[allow(missing_debug_implementations)]
pub struct AccountTrieReader<C>(C, Option<PersistBlockCache>);

impl<C> AccountTrieReader<C> {
    /// Create a new `AccountTrieReader`
    pub fn new(cursor: C, cache: Option<PersistBlockCache>) -> Self {
        Self(cursor, cache)
    }
}

impl<C> TrieReader for AccountTrieReader<C>
where
    C: DbCursorRO<tables::AccountsTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
        if let Some(cache) = &self.1 {
            let value = cache.trie_account(path);
            if value.is_some() {
                return Ok(value);
            }
        }
        Ok(self.0.seek_exact(StoredNibbles(path.clone()))?.map(|(_, value)| value.into()))
    }
}

/// Root hash for nested trie
#[derive(Debug)]
pub struct NestedStateRoot<P, F>
where
    P: DBProvider<Tx: DbTx>,
    F: Fn() -> ProviderResult<P>,
{
    provider: F,
    cache: Option<PersistBlockCache>,
}

impl<P, F> NestedStateRoot<P, F>
where
    P: DBProvider<Tx: DbTx>,
    F: Fn() -> ProviderResult<P>,
{
    /// Create a new `NestedStateRoot`
    pub fn new(provider: F, cache: Option<PersistBlockCache>) -> Self {
        Self { provider, cache }
    }

    /// Compatible with origin `BranchNodeCompact` trie
    pub fn read_hashed_state(&self) -> ProviderResult<HashedPostState> {
        let mut accounts = HashMap::default();
        let mut storages: B256Map<HashedStorage> = HashMap::default();

        let mut account_cursor =
            (self.provider)()?.tx_ref().cursor_read::<tables::HashedAccounts>()?;
        let account_walker = account_cursor.walk(None)?;
        for account in account_walker {
            let (hashed_address, account) = account?;
            accounts.insert(hashed_address, Some(account));
        }

        let mut storage_cursor =
            (self.provider)()?.tx_ref().cursor_dup_read::<tables::HashedStorages>()?;
        let storage_walker = storage_cursor.walk_dup(None, None)?;
        for storage in storage_walker {
            let (hashed_address, entry) = storage?;
            storages.entry(hashed_address).or_default().storage.insert(entry.key, entry.value);
        }

        Ok(HashedPostState { accounts, storages })
    }
}

impl<P, F> NestedStateRoot<P, F>
where
    P: DBProvider<Tx: DbTx>,
    F: Fn() -> ProviderResult<P> + Send + Sync,
{
    /// Calculate the root hash of nested trie
    pub fn calculate(
        &self,
        hashed_state: &HashedPostState,
        compatible: bool,
    ) -> ProviderResult<(B256, TrieUpdatesV2, Option<TrieUpdates>)> {
        let trie_update = Mutex::new(TrieUpdatesV2::default());
        let compatible_trie_update = compatible.then_some(Mutex::new(TrieUpdates::default()));
        let updated_account_nodes: [Mutex<Vec<(Nibbles, Option<Node>)>>; 16] = Default::default();
        let mut partitioned_accounts: [Vec<(&B256, &Option<Account>)>; 16] = Default::default();
        let HashedPostState { accounts: hashed_accounts, storages: hashed_storages } = hashed_state;

        for (hashed_address, account) in hashed_accounts {
            let index = (hashed_address[0] >> 4) as usize;
            partitioned_accounts[index].push((hashed_address, account));
        }
        std::thread::scope(|scope| -> ProviderResult<()> {
            let mut handles = Vec::new();
            for partition in partitioned_accounts {
                if partition.is_empty() {
                    continue;
                }
                handles.push(scope.spawn(|| -> ProviderResult<()> {
                    let index = (partition[0].0[0] >> 4) as usize;
                    let mut updated_account_nodes = updated_account_nodes[index].lock().unwrap();
                    for (hashed_address, account) in partition {
                        let hashed_address = hashed_address.clone();
                        let account = account.clone();
                        let path = Nibbles::unpack(hashed_address);
                        if let Some(account) = account {
                            let storage = hashed_storages.get(&hashed_address).cloned();
                            if account.get_bytecode_hash() == KECCAK256_EMPTY &&
                                storage.as_ref().map(|s| s.is_empty()).unwrap_or(true)
                            {
                                let account = account.into_trie_account(EMPTY_ROOT_HASH);
                                let node = Some(Node::ValueNode(alloy_rlp::encode(account)));
                                updated_account_nodes.push((path, node));
                            } else {
                                let mut updated_storage_nodes: [Vec<(Nibbles, Option<Node>)>; 16] =
                                    Default::default();
                                let create_reader = || {
                                    let cursor = (self.provider)()?
                                        .tx_ref()
                                        .cursor_dup_read::<tables::StoragesTrieV2>()?;
                                    Ok(StorageTrieReader::new(
                                        cursor,
                                        hashed_address,
                                        self.cache.clone(),
                                    ))
                                };

                                let cursor = (self.provider)()?
                                    .tx_ref()
                                    .cursor_dup_read::<tables::StoragesTrieV2>()?;
                                let trie_reader = StorageTrieReader::new(
                                    cursor,
                                    hashed_address,
                                    self.cache.clone(),
                                );
                                // only make the large storage trie parallel
                                let parallel = storage
                                    .as_ref()
                                    .map(|s| s.storage.len() > 256)
                                    .unwrap_or(false);
                                let mut storage_trie =
                                    Trie::new(trie_reader, parallel, compatible)?;
                                if let Some(storage) = storage {
                                    for (hashed_slot, value) in storage.storage {
                                        let nibbles = Nibbles::unpack(hashed_slot);
                                        let index = nibbles[0] as usize;
                                        let value = if value.is_zero() {
                                            None
                                        } else {
                                            let value = encode_fixed_size(&value);
                                            Some(Node::ValueNode(value.to_vec()))
                                        };
                                        updated_storage_nodes[index].push((nibbles, value));
                                    }
                                }
                                storage_trie
                                    .parallel_update(updated_storage_nodes, create_reader)?;
                                let account = account.into_trie_account(storage_trie.hash());
                                updated_account_nodes.push((
                                    path,
                                    Some(Node::ValueNode(alloy_rlp::encode(account))),
                                ));

                                let (trie_output, compatible_output) = storage_trie.take_output();
                                if !trie_output.is_empty() {
                                    assert!(trie_update
                                        .lock()
                                        .unwrap()
                                        .storage_tries
                                        .insert(
                                            hashed_address,
                                            StorageTrieUpdatesV2 {
                                                is_deleted: false,
                                                storage_nodes: trie_output.update_nodes,
                                                removed_nodes: trie_output.removed_nodes,
                                            }
                                        )
                                        .is_none());
                                }
                                if let Some(compatible) = compatible_trie_update.as_ref() {
                                    let compatible_output = compatible_output.unwrap();
                                    if !compatible_output.is_empty() {
                                        assert!(compatible
                                            .lock()
                                            .unwrap()
                                            .storage_tries
                                            .insert(
                                                hashed_address,
                                                StorageTrieUpdates {
                                                    is_deleted: false,
                                                    storage_nodes: compatible_output.update_nodes,
                                                    removed_nodes: compatible_output.removed_nodes,
                                                }
                                            )
                                            .is_none());
                                    }
                                }
                            }
                        } else {
                            updated_account_nodes.push((path, None));
                            trie_update
                                .lock()
                                .unwrap()
                                .storage_tries
                                .insert(hashed_address, StorageTrieUpdatesV2::deleted());
                            if let Some(compatible) = compatible_trie_update.as_ref() {
                                compatible
                                    .lock()
                                    .unwrap()
                                    .storage_tries
                                    .insert(hashed_address, StorageTrieUpdates::deleted());
                            }
                        }
                    }
                    Ok(())
                }));
            }
            for handle in handles {
                handle.join().unwrap()?;
            }
            Ok(())
        })?;

        let updated_account_nodes = updated_account_nodes.map(|u| u.into_inner().unwrap());
        let mut trie_update = trie_update.into_inner().unwrap();
        let mut compatible_trie_update = compatible_trie_update.map(|c| c.into_inner().unwrap());
        let create_reader = || {
            let cursor = (self.provider)()?.tx_ref().cursor_read::<tables::AccountsTrieV2>()?;
            Ok(AccountTrieReader(cursor, self.cache.clone()))
        };
        let cursor = (self.provider)()?.tx_ref().cursor_read::<tables::AccountsTrieV2>()?;
        let mut account_trie =
            Trie::new(AccountTrieReader(cursor, self.cache.clone()), true, compatible)?;
        account_trie.parallel_update(updated_account_nodes, create_reader)?;

        let root_hash = account_trie.hash();
        let (output, compatible_output) = account_trie.take_output();
        trie_update.account_nodes = output.update_nodes;
        trie_update.removed_nodes = output.removed_nodes;
        if let Some(compatible) = &mut compatible_trie_update {
            let compatible_output = compatible_output.unwrap();
            compatible.account_nodes = compatible_output.update_nodes;
            compatible.removed_nodes = compatible_output.removed_nodes;
        }

        Ok((root_hash, trie_update, compatible_trie_update))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::Entry,
        sync::{mpsc, Arc, Mutex},
    };

    use super::*;
    use alloy_primitives::{keccak256, map::HashMap, Address, U256};
    use alloy_rlp::encode_fixed_size;
    use rand_08::Rng;
    use reth_primitives_traits::Account;
    use reth_provider::{
        test_utils::create_test_provider_factory, DatabaseProviderFactory, TrieWriterV2,
    };
    use reth_trie::{
        nested_trie::{Node, Trie, TrieReader},
        test_utils, HashedPostState, HashedStorage, EMPTY_ROOT_HASH,
    };

    #[derive(Default)]
    struct InmemoryTrieDB {
        account_trie: Arc<Mutex<HashMap<Nibbles, Vec<u8>>>>,
        storage_trie: Arc<Mutex<HashMap<B256, HashMap<Nibbles, Vec<u8>>>>>,
    }
    struct InmemoryAccountTrieReader(Arc<InmemoryTrieDB>);
    struct InmemoryStorageTrieReader(Arc<InmemoryTrieDB>, B256);

    impl TrieReader for InmemoryAccountTrieReader {
        fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
            Ok(self.0.account_trie.lock().unwrap().get(path).map(|v| v.clone().into()))
        }
    }

    impl TrieReader for InmemoryStorageTrieReader {
        fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
            Ok(self
                .0
                .storage_trie
                .lock()
                .unwrap()
                .get(&self.1)
                .and_then(|storage| storage.get(path))
                .map(|v| v.clone().into()))
        }
    }

    impl TrieWriterV2 for InmemoryTrieDB {
        fn write(&self, input: &TrieUpdatesV2) -> Result<usize, DatabaseError> {
            let mut account_trie = self.account_trie.lock().unwrap();
            let mut storage_trie = self.storage_trie.lock().unwrap();
            let mut num_update = 0;

            for path in &input.removed_nodes {
                if account_trie.remove(path).is_some() {
                    num_update += 1;
                }
            }
            for (path, node) in input.account_nodes.clone() {
                account_trie.insert(path.clone(), node.into());
                num_update += 1;
            }

            for (hashed_address, storage_trie_update) in &input.storage_tries {
                if storage_trie_update.is_deleted {
                    if let Some(destruct_account) = storage_trie.remove(hashed_address) {
                        num_update += destruct_account.len();
                    }
                } else {
                    if let Some(storage) = storage_trie.get_mut(hashed_address) {
                        for path in &storage_trie_update.removed_nodes {
                            if storage.remove(path).is_some() {
                                num_update += 1;
                            }
                        }
                    }
                    let storage = storage_trie.entry(*hashed_address).or_default();
                    for (path, node) in storage_trie_update.storage_nodes.clone() {
                        storage.insert(path.clone(), node.into());
                        num_update += 1;
                    }
                }
            }

            Ok(num_update)
        }
    }

    fn calculate(
        state: HashMap<Address, (Account, HashMap<B256, U256>)>,
        db: Arc<InmemoryTrieDB>,
        is_insert: bool,
    ) -> (B256, TrieUpdatesV2) {
        let (tx, rx) = mpsc::channel();
        let num_task = state.len();
        let is_compatible = false;
        for (address, (account, storage)) in state {
            let db = db.clone();
            let tx = tx.clone();
            rayon::spawn_fifo(move || {
                let hashed_address = keccak256(address);
                let create_reader = || Ok(InmemoryStorageTrieReader(db.clone(), hashed_address));
                let storage_reader = InmemoryStorageTrieReader(db.clone(), hashed_address);
                let mut storage_trie = Trie::new(storage_reader, true, is_compatible).unwrap();
                let mut batches: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
                for (hashed_slot, value) in
                    storage.into_iter().map(|(k, v)| (keccak256(k), encode_fixed_size(&v)))
                {
                    let nibbles = Nibbles::unpack(hashed_slot);
                    let index = nibbles[0] as usize;
                    batches[index]
                        .push((nibbles, is_insert.then_some(Node::ValueNode(value.to_vec()))));
                }
                // parallel update
                storage_trie.parallel_update(batches, create_reader).unwrap();
                let storage_root = storage_trie.hash();
                let account = account.into_trie_account(storage_root);
                let _ = tx.send((
                    hashed_address,
                    alloy_rlp::encode(account),
                    storage_trie.take_output(),
                ));
            });
        }

        // paralle insert
        let mut trie_updates = TrieUpdatesV2::default();
        let mut batches: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
        let create_reader = || Ok(InmemoryAccountTrieReader(db.clone()));
        for _ in 0..num_task {
            let (hashed_address, rlp_account, (trie_output, _)) =
                rx.recv().expect("Failed to receive storage trie");
            let storage_trie_update = if is_insert {
                StorageTrieUpdatesV2 {
                    is_deleted: false,
                    storage_nodes: trie_output.update_nodes,
                    removed_nodes: trie_output.removed_nodes,
                }
            } else {
                StorageTrieUpdatesV2::deleted()
            };
            let nibbles = Nibbles::unpack(hashed_address);
            let index = nibbles[0] as usize;
            batches[index].push((nibbles, is_insert.then_some(Node::ValueNode(rlp_account))));
            assert!(trie_updates
                .storage_tries
                .insert(hashed_address, storage_trie_update)
                .is_none());
        }
        let account_reader = InmemoryAccountTrieReader(db.clone());
        let mut account_trie = Trie::new(account_reader, true, is_compatible).unwrap();

        // parallel update
        account_trie.parallel_update(batches, create_reader).unwrap();

        let root_hash = account_trie.hash();
        let (output, _) = account_trie.take_output();
        trie_updates.account_nodes = output.update_nodes;
        trie_updates.removed_nodes = output.removed_nodes;
        (root_hash, trie_updates)
    }

    fn random_state() -> HashMap<Address, (Account, HashMap<B256, U256>)> {
        let mut rng = rand_08::thread_rng();
        (0..100)
            .map(|_| {
                let address = Address::random();
                let account =
                    Account { balance: U256::from(rng.gen::<u64>()), ..Default::default() };
                let mut storage = HashMap::<B256, U256>::default();
                let has_storage = rng.gen_bool(0.7);
                if has_storage {
                    for _ in 0..100 {
                        storage.insert(
                            B256::from(U256::from(rng.gen::<u64>())),
                            U256::from(rng.gen::<u64>()),
                        );
                    }
                }
                (address, (account, storage))
            })
            .collect::<HashMap<_, _>>()
    }

    fn merge_state(
        mut state1: HashMap<Address, (Account, HashMap<B256, U256>)>,
        state2: HashMap<Address, (Account, HashMap<B256, U256>)>,
    ) -> HashMap<Address, (Account, HashMap<B256, U256>)> {
        for (address, (account, storage)) in state2 {
            match state1.entry(address) {
                Entry::Occupied(mut entry) => {
                    let origin = entry.get_mut();
                    origin.0 = account;
                    origin.1.extend(storage);
                }
                Entry::Vacant(entry) => {
                    entry.insert((account, storage));
                }
            }
        }
        state1
    }

    #[test]
    fn nested_state_root() {
        // create random state
        let state1 = random_state();
        let db = Arc::new(InmemoryTrieDB::default());

        let (state_root1, trie_input1) = calculate(state1.clone(), db.clone(), true);
        // compare state root
        assert_eq!(state_root1, test_utils::state_root(state1.clone()));

        // write into db
        let _ = db.write(&trie_input1).unwrap();
        let state2 = random_state();
        let (state_root2, trie_input2) = calculate(state2.clone(), db.clone(), true);
        let state_merged = merge_state(state1.clone(), state2.clone());
        let _ = db.write(&trie_input2).unwrap();

        // compare state root
        assert_eq!(state_root2, test_utils::state_root(state_merged.clone()));
        let (state_root_merged, ..) =
            calculate(state_merged.clone(), Arc::new(InmemoryTrieDB::default()), true);
        assert_eq!(state_root2, state_root_merged);

        // test delete
        if state_merged.len() == state1.len() + state2.len() {
            let (delete_root1, delete_input1) = calculate(state2.clone(), db.clone(), false);
            assert_eq!(delete_root1, state_root1);
            let _ = db.write(&delete_input1).unwrap();
            let (delete_root2, delete_input2) = calculate(state1.clone(), db.clone(), false);
            // has deleted all data, so the state root is EMPTY_ROOT_HASH
            assert_eq!(delete_root2, EMPTY_ROOT_HASH);
            let _ = db.write(&delete_input2).unwrap();
            assert!(db.account_trie.lock().unwrap().is_empty());
            assert!(db.storage_trie.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn nested_hash_calculate() {
        let state = random_state();
        // test paralle root hash
        let factory = create_test_provider_factory();
        let provider = || factory.database_provider_ro();
        let mut hashed_state = HashedPostState::default();
        for (address, (account, storage)) in state.clone() {
            let hashed_address = keccak256(address);
            hashed_state.accounts.insert(hashed_address, Some(account));
            let mut hashed_storage = HashedStorage::default();
            for (slot, value) in storage {
                hashed_storage.storage.insert(keccak256(slot), value);
            }
            hashed_state.storages.insert(hashed_address, hashed_storage);
        }

        let (parallel_root_hash, ..) =
            NestedStateRoot::new(provider, None).calculate(&hashed_state, true).unwrap();
        assert_eq!(parallel_root_hash, test_utils::state_root(state))
    }
}
