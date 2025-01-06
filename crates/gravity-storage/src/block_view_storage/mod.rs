use core::hash;
use reth_payload_builder::database::CachedReads;
use reth_primitives::{revm_primitives::Bytecode, Address, B256, U256};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{errors::provider::ProviderError, StateProviderBox, StateProviderFactory};
use reth_trie::{updates::TrieUpdates, HashedPostState};
use revm::{db::BundleState, primitives::AccountInfo, DatabaseRef};
use std::{
    clone,
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{GravityStorage, GravityStorageError};

pub struct BlockViewStorage<Client> {
    client: Client,
    inner: Mutex<BlockViewStorageInner>,
}

struct BlockViewStorageInner {
    state_provider_info: (B256, u64), // (block_hash, block_number),
    block_number_to_view: BTreeMap<u64, Arc<CachedReads>>,
    block_number_to_state: BTreeMap<u64, Arc<HashedPostState>>,
    block_number_to_trie_updates: BTreeMap<u64, Arc<TrieUpdates>>,
    block_number_to_id: BTreeMap<u64, B256>,
}

fn get_state_provider<Client: StateProviderFactory + 'static>(
    client: &Client,
    block_hash: B256,
) -> Result<StateProviderBox, GravityStorageError> {
    let state_provider = client.state_by_block_hash(block_hash);

    match state_provider {
        Ok(state_provider) => Ok(state_provider),
        Err(err) => Err(GravityStorageError::StateProviderError((block_hash, err))),
    }
}

impl<Client: StateProviderFactory + 'static> BlockViewStorage<Client> {
    pub fn new(
        client: Client,
        latest_block_number: u64,
        latest_block_hash: B256,
        block_number_to_id: BTreeMap<u64, B256>,
    ) -> Self {
        Self {
            client,
            inner: Mutex::new(BlockViewStorageInner::new(
                latest_block_number,
                latest_block_hash,
                block_number_to_id,
            )),
        }
    }
}

impl BlockViewStorageInner {
    fn new(block_number: u64, block_hash: B256, block_number_to_id: BTreeMap<u64, B256>) -> Self {
        Self {
            state_provider_info: (block_hash, block_number),
            block_number_to_view: BTreeMap::new(),
            block_number_to_state: BTreeMap::new(),
            block_number_to_trie_updates: BTreeMap::new(),
            block_number_to_id,
        }
    }
}

impl<Client: StateProviderFactory + 'static> GravityStorage for BlockViewStorage<Client> {
    type StateView = BlockViewProvider;

    fn get_state_view(
        &self,
        target_block_number: u64,
    ) -> Result<(B256, Self::StateView), GravityStorageError> {
        let storage = self.inner.lock().unwrap();
        let (base_block_hash, base_block_number) = storage.state_provider_info;

        let latest_block_number =
            storage.block_number_to_view.keys().max().cloned().unwrap_or(base_block_number);
        if target_block_number > latest_block_number {
            return Err(GravityStorageError::TooNew(target_block_number));
        }

        let block_id = *storage.block_number_to_id.get(&target_block_number).unwrap();
        let block_number_to_id = storage.block_number_to_id.clone();
        let block_views: Vec<_> = storage
            .block_number_to_view
            .range(base_block_number + 1..target_block_number + 1)
            .rev()
            .map(|(_, view)| view.clone())
            .collect();
        drop(storage);

        // Block number should be continuous
        assert_eq!(block_views.len() as u64, target_block_number - base_block_number);

        Ok((
            block_id,
            BlockViewProvider::new(
                block_views,
                block_number_to_id,
                get_state_provider(&self.client, base_block_hash)?,
            ),
        ))
    }

    fn insert_block_id(&self, block_number: u64, block_id: B256) {
        let mut storage = self.inner.lock().unwrap();
        storage.block_number_to_id.insert(block_number, block_id);
    }

    fn insert_bundle_state(&self, block_number: u64, bundle_state: &BundleState) {
        let mut cached = CachedReads::default();
        for (addr, acc) in bundle_state.state().iter().map(|(a, acc)| (*a, acc)) {
            if let Some(info) = acc.info.clone() {
                // we want pre cache existing accounts and their storage
                // this only includes changed accounts and storage but is better than nothing
                let storage =
                    acc.storage.iter().map(|(key, slot)| (*key, slot.present_value)).collect();
                cached.insert_account(addr, info, storage);
            }
        }
        let hashed_state = Arc::new(HashedPostState::from_bundle_state(&bundle_state.state));
        let mut storage = self.inner.lock().unwrap();
        storage.block_number_to_view.insert(block_number, Arc::new(cached));
        storage.block_number_to_state.insert(block_number, hashed_state);
    }

    fn update_canonical(&self, block_number: u64, block_hash: B256) {
        let mut storage = self.inner.lock().unwrap();
        assert!(block_number > storage.state_provider_info.1);
        let gc_block_number = storage.state_provider_info.1;
        storage.state_provider_info = (block_hash, block_number);
        storage.block_number_to_view.remove(&gc_block_number);
        storage.block_number_to_state.remove(&gc_block_number);
        storage.block_number_to_trie_updates.remove(&gc_block_number);
    }

    fn state_root_with_updates(
        &self,
        block_number: u64,
    ) -> Result<(B256, Arc<HashedPostState>, Arc<TrieUpdates>), GravityStorageError> {
        let storage = self.inner.lock().unwrap();
        let (base_block_hash, base_block_number) = storage.state_provider_info;
        let hashed_state_vec: Vec<_> = storage
            .block_number_to_state
            .range(base_block_number + 1..block_number)
            .map(|(_, hashed_state)| hashed_state.clone())
            .collect();
        let trie_updates_vec: Vec<_> = storage
            .block_number_to_trie_updates
            .range(base_block_number + 1..block_number)
            .map(|(_, trie_updates)| trie_updates.clone())
            .collect();
        let hashed_state = storage.block_number_to_state.get(&block_number).unwrap().clone();
        drop(storage);

        // Block number should be continuous
        assert_eq!(hashed_state_vec.len() as u64, block_number - base_block_number - 1);
        assert_eq!(trie_updates_vec.len() as u64, block_number - base_block_number - 1);

        let state_provider = get_state_provider(&self.client, base_block_hash)?;
        let (state_root, trie_updates) = state_provider
            .state_root_with_updates_v2(
                hashed_state.as_ref().clone(),
                hashed_state_vec,
                trie_updates_vec,
            )
            .unwrap();
        let trie_updates = Arc::new(trie_updates);

        {
            let mut storage = self.inner.lock().unwrap();
            storage.block_number_to_trie_updates.insert(block_number, trie_updates.clone());
        }

        Ok((state_root, hashed_state, trie_updates))
    }
}

pub struct BlockViewProvider {
    block_views: Vec<Arc<CachedReads>>,
    block_number_to_id: BTreeMap<u64, B256>,
    db: StateProviderDatabase<StateProviderBox>,
}

impl BlockViewProvider {
    fn new(
        block_views: Vec<Arc<CachedReads>>,
        block_number_to_id: BTreeMap<u64, B256>,
        state_provider: StateProviderBox,
    ) -> Self {
        Self { block_views, block_number_to_id, db: StateProviderDatabase::new(state_provider) }
    }
}

impl DatabaseRef for BlockViewProvider {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        for block_view in &self.block_views {
            if let Some(account) = block_view.accounts.get(&address) {
                return Ok(account.info.clone());
            }
        }
        Ok(self.db.basic_ref(address)?)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        for block_view in &self.block_views {
            if let Some(bytecode) = block_view.contracts.get(&code_hash) {
                return Ok(bytecode.clone());
            }
        }
        Ok(self.db.code_by_hash_ref(code_hash)?)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        for block_view in &self.block_views {
            if let Some(acc_entry) = block_view.accounts.get(&address) {
                if let Some(value) = acc_entry.storage.get(&index) {
                    return Ok(*value);
                }
            }
        }
        Ok(self.db.storage_ref(address, index)?)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        Ok(*self.block_number_to_id.get(&number).unwrap())
    }
}
