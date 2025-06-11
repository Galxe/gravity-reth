use std::sync::mpsc;

use alloy_primitives::{map::HashSet, B256};
use alloy_rlp::encode_fixed_size;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
};
use reth_provider::{
    providers::ConsistentDbView, BlockReader, DBProvider, DatabaseProviderFactory, ProviderError,
    StateCommitmentProvider,
};
use reth_storage_errors::db::DatabaseError;
use reth_trie::{
    nested_trie::{Node, NodeEntry, Trie, TrieOutput, TrieReader},
    HashedPostState, Nibbles, StoredNibbles, StoredNibblesSubKey, TrieInputV2,
};

struct StorageTrieReader<C> {
    hashed_address: B256,
    cursor: C,
}

impl<C> StorageTrieReader<C> {
    fn new(cursor: C, hashed_address: B256) -> Self {
        Self { cursor, hashed_address }
    }
}

impl<C> TrieReader for StorageTrieReader<C>
where
    C: DbCursorRO<tables::StoragesTrieV2> + DbDupCursorRO<tables::StoragesTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
        Ok(self
            .cursor
            .seek_by_key_subkey(self.hashed_address, StoredNibblesSubKey(path.clone()))?
            .map(|v| NodeEntry::from(v))
            .filter(|e| e.path == *path)
            .map(|e| e.node))
    }
}

struct AccountTrieReader<C>(C);

impl<C> TrieReader for AccountTrieReader<C>
where
    C: DbCursorRO<tables::AccountsTrieV2> + Send + Sync,
{
    fn read(&mut self, path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
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
}

impl<Factory> NestedStateRoot<Factory> {
    pub fn new(view: ConsistentDbView<Factory>) -> Self {
        Self { view }
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
        hashed_state: HashedPostState,
    ) -> Result<(B256, TrieInputV2), ProviderError> {
        let mut removed_account_nodes: HashSet<B256> = HashSet::default();
        let (tx, rx) = mpsc::channel();
        let mut num_task = 0;
        for (hashed_address, account) in hashed_state.accounts.clone() {
            if let Some(account) = account {
                let view = self.view.clone();
                let tx = tx.clone();
                let storage = hashed_state.storages.get(&hashed_address).cloned();
                num_task += 1;
                rayon::spawn_fifo(move || {
                    let result = (|| -> Result<(B256, Vec<u8>, TrieOutput), ProviderError> {
                        let provider_ro = view.provider_ro()?;
                        let cursor =
                            provider_ro.tx_ref().cursor_dup_read::<tables::StoragesTrieV2>()?;
                        let trie_reader = StorageTrieReader::new(cursor, hashed_address);
                        let mut storage_trie = Trie::new(trie_reader, false)?;
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
                removed_account_nodes.insert(hashed_address);
            }
        }
        let mut trie_input = TrieInputV2::default();
        trie_input.state = hashed_state;

        let provider_ro = self.view.provider_ro()?;
        let cursor = provider_ro.tx_ref().cursor_read::<tables::AccountsTrieV2>()?;
        let mut account_trie = Trie::new(AccountTrieReader(cursor), true)?;
        for _ in 0..num_task {
            let (hashed_address, rlp_account, trie_output) =
                rx.recv().expect("Failed to receive storage trie")?;
            account_trie.insert(Nibbles::unpack(hashed_address), Node::ValueNode(rlp_account))?;
            if !trie_output.removed_nodes.is_empty() {
                trie_input.removed_storage_nodes.insert(hashed_address, trie_output.removed_nodes);
            }
            if !trie_output.update_nodes.is_empty() {
                trie_input.update_storage_nodes.insert(hashed_address, trie_output.update_nodes);
            }
        }
        for delete_account in removed_account_nodes {
            let nibbles = Nibbles::unpack(delete_account);
            account_trie.delete(nibbles.clone())?;
            trie_input.removed_account_nodes.insert(nibbles, Some(delete_account));
        }

        let root_hash = account_trie.hash();
        let TrieOutput { removed_nodes, update_nodes } = account_trie.take_output();
        for nibbles in removed_nodes {
            trie_input.removed_account_nodes.insert(nibbles, None);
        }
        trie_input.update_account_nodes.extend(update_nodes);

        Ok((root_hash, trie_input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{keccak256, map::HashMap, Address, U256};
    use alloy_rlp::encode_fixed_size;
    use rand::Rng;
    use reth_primitives::Account;
    use reth_primitives_traits::Account;
    use reth_provider::test_utils::create_test_provider_factory;
    use reth_trie::{
        nested_trie::{Node, NodeEntry, StoredNode, Trie, TrieReader},
        test_utils, HashedPostState, HashedStorage,
    };

    #[derive(Default)]
    struct NoopTrieReader;
    impl TrieReader for NoopTrieReader {
        fn read(&mut self, _path: &Nibbles) -> Result<Option<Node>, DatabaseError> {
            Ok(None)
        }
    }

    #[test]
    fn nested_state_root() {
        let mut rng = rand::thread_rng();
        let state = (0..100)
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
            .collect::<HashMap<_, _>>();

        let encoded_accounts = state.clone().into_iter().map(|(address, (account, storage))| {
            let mut storage_trie = Trie::new(NoopTrieReader::default(), false).unwrap();
            for (hashed_slot, value) in
                storage.into_iter().map(|(k, v)| (keccak256(k), encode_fixed_size(&v)))
            {
                storage_trie
                    .insert(Nibbles::unpack(hashed_slot), Node::ValueNode(value.to_vec()))
                    .unwrap();
            }
            let storage_root = storage_trie.hash();
            let account = account.into_trie_account(storage_root);
            (address, alloy_rlp::encode(account))
        });
        let mut account_trie = Trie::new(NoopTrieReader::default(), false).unwrap();
        for (address, account) in encoded_accounts {
            account_trie
                .insert(Nibbles::unpack(keccak256(address)), Node::ValueNode(account))
                .unwrap();
        }
        let real_root_hash = test_utils::state_root(state.clone());
        assert_eq!(account_trie.hash(), real_root_hash);

        let update_nodes = account_trie.take_output().update_nodes;
        let serialized_nodes: HashMap<Nibbles, Node> = update_nodes
            .clone()
            .into_iter()
            .map(|(k, v)| (k.clone(), StoredNode::from(NodeEntry { path: k, node: v })))
            .map(|(k, v)| (k, NodeEntry::from(v).node))
            .collect();
        assert_eq!(update_nodes.len(), serialized_nodes.len());
        for (k, mut v) in update_nodes {
            v.reset();
            assert_eq!(v, serialized_nodes.get(&k).unwrap().clone())
        }

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

        let (parallel_root_hash, trie_input) =
            NestedStateRoot::new(consistent_view).calculate(hashed_state).unwrap();
        assert_eq!(parallel_root_hash, real_root_hash)
    }
}
