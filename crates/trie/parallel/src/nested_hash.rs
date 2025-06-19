use std::sync::mpsc;

use alloy_primitives::B256;
use alloy_rlp::encode_fixed_size;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_provider::{
    providers::ConsistentDbView, BlockReader, DBProvider, DatabaseProviderFactory,
    PersistBlockCache, ProviderResult, StateCommitmentProvider,
};
use reth_storage_errors::db::DatabaseError;
use reth_trie::{
    nested_trie::{CompatibleTrieOutput, Node, NodeEntry, Trie, TrieOutput, TrieReader},
    updates::StorageTrieUpdates,
    HashedPostState, Nibbles, StorageTrieUpdatesV2, StoredNibbles, StoredNibblesSubKey,
};
use reth_trie_common::updates::{TrieUpdates, TrieUpdatesV2};

struct StorageTrieReader<C> {
    hashed_address: B256,
    cursor: C,
    cache: Option<PersistBlockCache>,
}

impl<C> StorageTrieReader<C> {
    fn new(cursor: C, hashed_address: B256, cache: Option<PersistBlockCache>) -> Self {
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
        Ok(self
            .cursor
            .seek_by_key_subkey(self.hashed_address, StoredNibblesSubKey(path.clone()))?
            .map(|v| NodeEntry::from(v))
            .filter(|e| e.path == *path)
            .map(|e| e.node))
    }
}

struct AccountTrieReader<C>(C, Option<PersistBlockCache>);

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
        Ok(self
            .0
            .seek_exact(StoredNibbles(path.clone()))?
            .map(|(_, value)| NodeEntry::from(value).node))
    }
}

#[derive(Debug)]
pub struct NestedStateRoot<Factory> {
    /// Consistent view of the database.
    view: ConsistentDbView<Factory>,
    cache: Option<PersistBlockCache>,
}

impl<Factory> NestedStateRoot<Factory> {
    pub fn new(view: ConsistentDbView<Factory>, cache: Option<PersistBlockCache>) -> Self {
        Self { view, cache }
    }
}

impl<Factory> NestedStateRoot<Factory>
where
    Factory: DatabaseProviderFactory<Provider: BlockReader>
        + StateCommitmentProvider
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub fn calculate(
        &self,
        hashed_state: &HashedPostState,
        compatible: bool,
    ) -> ProviderResult<(B256, TrieUpdatesV2, Option<TrieUpdates>)> {
        let mut trie_update = TrieUpdatesV2::default();
        let mut compatible_trie_update = compatible.then_some(TrieUpdates::default());
        let mut removed_account_nodes: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
        let (tx, rx) = mpsc::channel();
        let mut num_task = 0;
        let HashedPostState { accounts: hashed_accounts, storages: hashed_storages } = hashed_state;
        for (hashed_address, account) in hashed_accounts.clone() {
            if let Some(account) = account {
                let view = self.view.clone();
                let tx = tx.clone();
                let storage = hashed_storages.get(&hashed_address).cloned();
                let cache = self.cache.clone();
                num_task += 1;
                // calculate storage root in parallel
                rayon::spawn_fifo(move || {
                    let result = (|| -> ProviderResult<(B256, Vec<u8>, (TrieOutput, Option<CompatibleTrieOutput>))> {
                        let provider_ro = view.provider_ro()?;
                        let cursor =
                            provider_ro.tx_ref().cursor_dup_read::<tables::StoragesTrieV2>()?;
                        let trie_reader = StorageTrieReader::new(cursor, hashed_address, cache);
                        let mut storage_trie = Trie::new(trie_reader, false, compatible)?;
                        let mut delete_slots = vec![];
                        if let Some(storage) = storage {
                            for (hashed_slot, value) in storage.storage {
                                if value.is_zero() {
                                    delete_slots.push(hashed_slot);
                                } else {
                                    let value = encode_fixed_size(&value);
                                    storage_trie.insert(
                                        Nibbles::unpack(hashed_slot),
                                        Node::ValueNode(value.to_vec()),
                                    )?;
                                }
                            }
                        }
                        for delete_slot in delete_slots {
                            storage_trie.delete(Nibbles::unpack(delete_slot))?;
                        }
                        let account = account.into_trie_account(storage_trie.hash());
                        Ok((hashed_address, alloy_rlp::encode(account), storage_trie.take_output()))
                    })();
                    let _ = tx.send(result);
                });
            } else {
                let nibbles = Nibbles::unpack(hashed_address);
                let index = nibbles[0] as usize;
                removed_account_nodes[index].push((nibbles.clone(), None));
                trie_update.storage_tries.insert(hashed_address, StorageTrieUpdatesV2::deleted());
                if let Some(compatible) = &mut compatible_trie_update {
                    compatible.storage_tries.insert(hashed_address, StorageTrieUpdates::deleted());
                }
            }
        }
        let provider_ro = self.view.provider_ro()?;
        // Paralle update account trie. Split updated account into 16 groups.
        let mut update_account_nodes: [Vec<(Nibbles, Option<Node>)>; 16] = Default::default();
        let create_reader = || {
            let cursor = provider_ro.tx_ref().cursor_read::<tables::AccountsTrieV2>()?;
            Ok(AccountTrieReader(cursor, self.cache.clone()))
        };
        for _ in 0..num_task {
            let (hashed_address, rlp_account, (trie_output, compatible_output)) =
                rx.recv().expect("Failed to receive storage trie")?;
            assert!(trie_update
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
            if let Some(compatible) = &mut compatible_trie_update {
                let compatible_output = compatible_output.unwrap();
                assert!(compatible
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
            // Each updated path in a group has the same first nibble
            let nibbles = Nibbles::unpack(hashed_address);
            let index = nibbles[0] as usize;
            update_account_nodes[index].push((nibbles, Some(Node::ValueNode(rlp_account))));
        }
        let cursor = provider_ro.tx_ref().cursor_read::<tables::AccountsTrieV2>()?;
        let mut account_trie =
            Trie::new(AccountTrieReader(cursor, self.cache.clone()), true, compatible)?;
        account_trie.parallel_update(update_account_nodes, create_reader)?;
        account_trie.parallel_update(removed_account_nodes, create_reader)?;

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
        sync::{Arc, Mutex},
    };

    use super::*;
    use alloy_primitives::{keccak256, map::HashMap, Address, U256};
    use alloy_rlp::encode_fixed_size;
    use rand::Rng;
    use reth_primitives_traits::Account;
    use reth_provider::{test_utils::create_test_provider_factory, TrieWriterV2};
    use reth_trie::{
        nested_trie::{Node, NodeEntry, StoredNode, Trie, TrieReader},
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
            Ok(self
                .0
                .account_trie
                .lock()
                .unwrap()
                .get(path)
                .map(|v| NodeEntry::from(v.clone()).node))
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
                .map(|v| NodeEntry::from(v.clone()).node))
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
                account_trie.insert(path.clone(), StoredNode::from(NodeEntry { path, node }));
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
                        storage.insert(path.clone(), StoredNode::from(NodeEntry { path, node }));
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
                let storage_reader = InmemoryStorageTrieReader(db, hashed_address);
                let mut storage_trie = Trie::new(storage_reader, false, is_compatible).unwrap();
                for (hashed_slot, value) in
                    storage.into_iter().map(|(k, v)| (keccak256(k), encode_fixed_size(&v)))
                {
                    if is_insert {
                        storage_trie
                            .insert(Nibbles::unpack(hashed_slot), Node::ValueNode(value.to_vec()))
                            .unwrap();
                    } else {
                        storage_trie.delete(Nibbles::unpack(hashed_slot)).unwrap();
                    }
                }
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
            let (hashed_address, rlp_account, (trie_output, compatible_trie_output)) =
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
        let (output, compatible_output) = account_trie.take_output();
        trie_updates.account_nodes = output.update_nodes;
        trie_updates.removed_nodes = output.removed_nodes;
        (root_hash, trie_updates)
    }

    fn random_state() -> HashMap<Address, (Account, HashMap<B256, U256>)> {
        let mut rng = rand::thread_rng();
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
        let consistent_view = ConsistentDbView::new(factory.clone(), None);
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
            NestedStateRoot::new(consistent_view, None).calculate(&hashed_state, true).unwrap();
        assert_eq!(parallel_root_hash, test_utils::state_root(state))
    }
}
