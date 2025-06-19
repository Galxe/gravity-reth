use crate::GravityStorage;
use alloy_primitives::{Address, B256, U256};
use reth_provider::{
    providers::ConsistentDbView, BlockNumReader, BlockReader, DatabaseProviderFactory,
    HeaderProvider, PersistBlockCache, ProviderError, ProviderResult, StateCommitmentProvider,
    StateProviderBox, StateProviderOptions, PERSIST_BLOCK_CACHE,
};
use reth_revm::{
    bytecode::Bytecode, database::StateProviderDatabase, primitives::BLOCK_HASH_HISTORY,
    state::AccountInfo, DatabaseRef,
};
use reth_storage_api::StateProviderFactory;
use reth_trie::{
    updates::{TrieUpdates, TrieUpdatesV2},
    HashedPostState,
};
use reth_trie_parallel::nested_hash::NestedStateRoot;
use std::{collections::BTreeMap, sync::Mutex};

pub struct BlockViewStorage<Client> {
    client: Client,
    cache: PersistBlockCache,
    block_number_to_id: Mutex<BTreeMap<u64, B256>>,
}

impl<Client> BlockViewStorage<Client>
where
    Client: DatabaseProviderFactory<Provider: BlockNumReader + HeaderProvider + BlockReader>
        + StateCommitmentProvider
        + StateProviderFactory
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub fn new(client: Client) -> Self {
        Self { client, cache: PERSIST_BLOCK_CACHE.clone(), block_number_to_id: Default::default() }
    }
}

impl<Client> GravityStorage for BlockViewStorage<Client>
where
    Client: DatabaseProviderFactory<Provider: BlockNumReader + HeaderProvider + BlockReader>
        + StateCommitmentProvider
        + StateProviderFactory
        + Clone
        + Send
        + Sync
        + 'static,
{
    type StateView = BlockViewProvider;

    fn get_state_view(&self, opts: StateProviderOptions) -> ProviderResult<Self::StateView> {
        let state = self.client.latest_with_opts(opts.with_raw_db())?;
        Ok(BlockViewProvider::new(StateProviderDatabase::new(state), Some(self.cache.clone())))
    }

    fn state_root(
        &self,
        hashed_state: &HashedPostState,
        compatible: bool,
    ) -> ProviderResult<(B256, TrieUpdatesV2, Option<TrieUpdates>)> {
        let consistent_view = ConsistentDbView::new_with_best_tip(self.client.clone())?;
        let nested_hash = NestedStateRoot::new(consistent_view, Some(self.cache.clone()));
        nested_hash.calculate(hashed_state, compatible)
    }

    fn insert_block_id(&self, block_number: u64, block_id: B256) {
        self.block_number_to_id.lock().unwrap().insert(block_number, block_id);
    }

    fn get_block_id(&self, block_number: u64) -> Option<B256> {
        self.block_number_to_id.lock().unwrap().get(&block_number).cloned()
    }

    fn update_canonical(&self, block_number: u64, _block_hash: B256) {
        if block_number <= BLOCK_HASH_HISTORY {
            return;
        }

        // Only keep the last BLOCK_HASH_HISTORY block hashes before the canonical block number,
        // including the canonical block number.
        let target_block_number = block_number - BLOCK_HASH_HISTORY;
        let mut block_number_to_id = self.block_number_to_id.lock().unwrap();
        while let Some((&first_key, _)) = block_number_to_id.first_key_value() {
            if first_key <= target_block_number {
                block_number_to_id.pop_first();
            } else {
                break;
            }
        }
    }
}
pub struct BlockViewProvider {
    db: StateProviderDatabase<StateProviderBox>,
    cache: Option<PersistBlockCache>,
}

impl BlockViewProvider {
    pub fn new(
        db: StateProviderDatabase<StateProviderBox>,
        cache: Option<PersistBlockCache>,
    ) -> Self {
        Self { db, cache }
    }
}

impl DatabaseRef for BlockViewProvider {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(cache) = &self.cache {
            let value = cache.basic_account(&address).map(Into::into);
            if value.is_some() {
                return Ok(value);
            }
        }
        self.db.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.bytecode_by_hash(&code_hash) {
                return Ok(value);
            }
        }
        self.db.code_by_hash_ref(code_hash)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.storage(&address, &index) {
                return Ok(value);
            }
        }
        self.db.storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        unimplemented!("not support block_hash_ref in BlockViewProvider")
    }
}
