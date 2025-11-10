use crate::GravityStorage;
use alloy_primitives::{Address, B256, U256};
use reth_db_api::{cursor::DbDupCursorRO, tables, transaction::DbTx, Database};
use reth_provider::{
    BlockNumReader, BlockReader, DBProvider, DatabaseProviderFactory, HeaderProvider,
    PersistBlockCache, ProviderError, ProviderResult, StateProviderBox,
    PERSIST_BLOCK_CACHE,
};
use reth_revm::{
    bytecode::Bytecode, database::StateProviderDatabase, primitives::BLOCK_HASH_HISTORY,
    state::AccountInfo, DatabaseRef,
};
use reth_storage_api::StateProviderFactory;
use reth_trie::{updates::TrieUpdatesV2, HashedPostState};
use reth_trie_parallel::nested_hash::NestedStateRoot;
use std::{collections::BTreeMap, sync::Mutex};

/// Block view for pipeline execution
#[allow(missing_debug_implementations)]
pub struct BlockViewStorage<Client> {
    client: Client,
    cache: PersistBlockCache,
    block_number_to_id: Mutex<BTreeMap<u64, B256>>,
}

impl<Client> BlockViewStorage<Client>
where
    Client: DatabaseProviderFactory<Provider: BlockNumReader + HeaderProvider + BlockReader>
        + StateProviderFactory
        + Clone
        + Send
        + Sync
        + 'static,
{
    /// Create a new `BlockViewStorage`
    pub fn new(client: Client) -> Self {
        Self { client, cache: PERSIST_BLOCK_CACHE.clone(), block_number_to_id: Default::default() }
    }
}

impl<Client> GravityStorage for BlockViewStorage<Client>
where
    Client: DatabaseProviderFactory<Provider: BlockNumReader + HeaderProvider + BlockReader>
        + StateProviderFactory
        + Clone
        + Send
        + Sync
        + 'static,
{
    type StateView = RawBlockViewProvider<<Client::DB as Database>::TX>;

    fn get_state_view(&self) -> ProviderResult<Self::StateView> {
        Ok(RawBlockViewProvider::new(
            self.client.database_provider_ro()?.into_tx(),
            Some(self.cache.clone()),
        ))
    }

    fn state_root(&self, hashed_state: &HashedPostState) -> ProviderResult<(B256, TrieUpdatesV2)> {
        let provider = || self.client.database_provider_ro().map(|db| db.into_tx());
        let nested_hash = NestedStateRoot::new(provider, Some(self.cache.clone()));
        nested_hash.calculate(hashed_state)
    }

    fn insert_block_id(&self, block_number: u64, block_id: B256) {
        self.block_number_to_id.lock().unwrap().insert(block_number, block_id);
    }

    fn get_block_id(&self, block_number: u64) -> Option<B256> {
        self.block_number_to_id.lock().unwrap().get(&block_number).copied()
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

/// Raw Block view provider
#[derive(Debug)]
pub struct RawBlockViewProvider<Tx> {
    tx: Tx,
    cache: Option<PersistBlockCache>,
}

impl<Tx> RawBlockViewProvider<Tx> {
    /// Create `RawBlockViewProvider`
    pub const fn new(tx: Tx, cache: Option<PersistBlockCache>) -> Self {
        Self { tx, cache }
    }
}

impl<Tx: DbTx> DatabaseRef for RawBlockViewProvider<Tx> {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.basic_account(&address) {
                return Ok(value.map(Into::into))
            }
        }
        Ok(self.tx.get_by_encoded_key::<tables::PlainAccountState>(&address)?.map(Into::into))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.bytecode_by_hash(&code_hash) {
                return Ok(value);
            }
        }
        match self.tx.get_by_encoded_key::<tables::Bytecodes>(&code_hash)? {
            Some(byte_code) => {
                let byte_code: Bytecode = byte_code.0;
                if let Some(cache) = &self.cache {
                    cache.cache_byte_code(code_hash, byte_code.clone());
                }
                Ok(byte_code)
            }
            None => Ok(Default::default()),
        }
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.storage(&address, &index) {
                return Ok(value.unwrap_or_default());
            }
        }
        let mut cursor = self.tx.cursor_dup_read::<tables::PlainStorageState>()?;
        let storage_key = B256::new(index.to_be_bytes());
        Ok(cursor
            .seek_by_key_subkey(address, storage_key)?
            .filter(|e| e.key == storage_key)
            .map(|e| e.value)
            .unwrap_or_default())
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        unimplemented!("not support block_hash_ref in BlockViewProvider")
    }
}

/// Block view provider
#[derive(Debug)]
pub struct BlockViewProvider {
    db: StateProviderDatabase<StateProviderBox>,
    cache: Option<PersistBlockCache>,
}

impl BlockViewProvider {
    /// Create a new `BlockViewProvider`
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
            if let Some(value) = cache.basic_account(&address) {
                return Ok(value.map(Into::into))
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
        match self.db.code_by_hash_ref(code_hash) {
            Ok(byte_code) => {
                if let Some(cache) = &self.cache {
                    cache.cache_byte_code(code_hash, byte_code.clone());
                }
                Ok(byte_code)
            }
            Err(e) => Err(e),
        }
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.storage(&address, &index) {
                return Ok(value.unwrap_or_default());
            }
        }
        self.db.storage_ref(address, index)
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        unimplemented!("not support block_hash_ref in BlockViewProvider")
    }
}
