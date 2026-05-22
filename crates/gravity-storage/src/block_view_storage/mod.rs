use crate::GravityStorage;
use alloy_primitives::{Address, B256, U256};
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    tables,
    transaction::DbTx,
    Database,
};
use reth_provider::{
    BlockNumReader, BlockReader, DBProvider, DatabaseProviderFactory, HeaderProvider,
    PersistBlockCache, ProviderError, ProviderResult, StateProviderBox, PERSIST_BLOCK_CACHE,
};
use reth_revm::{
    bytecode::Bytecode, database::StateProviderDatabase, primitives::BLOCK_HASH_HISTORY,
    state::AccountInfo, DatabaseRef,
};
use reth_storage_api::StateProviderFactory;
use reth_trie::{
    updates::TrieUpdatesV2, HashedPostState, Nibbles, StoredNibbles, StoredNibblesSubKey,
};
use reth_trie_parallel::nested_hash::NestedStateRoot;
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};
use tracing::info;

/// Errors returned by `block_hash_ref` when the requested block number is out of range.
#[derive(Debug, Clone)]
pub enum BlockHashError {
    /// The requested block number is greater than the latest known block.
    BlockTooHigh(u64),
    /// The requested block number is older than `BLOCK_HASH_HISTORY` blocks.
    BlockTooOld(u64),
}

impl fmt::Display for BlockHashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockTooHigh(number) => {
                write!(f, "block {number} is too high: exceeds latest known block")
            }
            Self::BlockTooOld(number) => {
                write!(f, "block {number} is too old: only the last {BLOCK_HASH_HISTORY} block hashes are available")
            }
        }
    }
}

impl std::error::Error for BlockHashError {}

/// Block view for pipeline execution
#[allow(missing_debug_implementations)]
pub struct BlockViewStorage<Client> {
    client: Client,
    cache: PersistBlockCache,
    block_number_to_id: Arc<Mutex<BTreeMap<u64, B256>>>,
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
            self.block_number_to_id.clone(),
        ))
    }

    fn state_root(&self, hashed_state: &HashedPostState) -> ProviderResult<(B256, TrieUpdatesV2)> {
        let storage_slots: usize =
            hashed_state.storages.values().map(|storage| storage.storage.len()).sum();
        info!(
            target: "gravity_storage::state_root",
            accounts_len=?hashed_state.accounts.len(),
            storages_len=?hashed_state.storages.len(),
            storage_slots=?storage_slots,
            "lightman0522 gravity storage state_root input"
        );
        for (hashed_address, account) in hashed_state.accounts.iter().take(12) {
            info!(
                target: "gravity_storage::state_root",
                hashed_address=?hashed_address,
                nonce=?account.as_ref().map(|account| account.nonce),
                balance=?account.as_ref().map(|account| account.balance),
                bytecode_hash=?account.as_ref().and_then(|account| account.bytecode_hash),
                destroyed=?account.is_none(),
                "lightman0522 gravity storage state_root account input"
            );
        }
        for (hashed_address, storage) in hashed_state.storages.iter().take(12) {
            info!(
                target: "gravity_storage::state_root",
                hashed_address=?hashed_address,
                wiped=?storage.wiped,
                storage_len=?storage.storage.len(),
                "lightman0522 gravity storage state_root storage input"
            );
        }
        let tx = self.client.database_provider_ro()?.into_tx();
        for (hashed_address, account) in hashed_state.accounts.iter().take(12) {
            let db_account = tx.get::<tables::HashedAccounts>(*hashed_address)?;
            info!(
                target: "gravity_storage::state_root",
                hashed_address=?hashed_address,
                db_nonce=?db_account.as_ref().map(|account| account.nonce),
                db_balance=?db_account.as_ref().map(|account| account.balance),
                db_bytecode_hash=?db_account.as_ref().and_then(|account| account.bytecode_hash),
                post_nonce=?account.as_ref().map(|account| account.nonce),
                post_balance=?account.as_ref().map(|account| account.balance),
                post_bytecode_hash=?account.as_ref().and_then(|account| account.bytecode_hash),
                "lightman0522 gravity storage hashed account table before root"
            );
        }
        let empty_path = Nibbles::new();
        let account_root_node = tx
            .cursor_read::<tables::AccountsTrieV2>()?
            .get(StoredNibbles(empty_path))?
            .map(|(_, node)| node);
        info!(
            target: "gravity_storage::state_root",
            account_root_node=?account_root_node,
            "lightman0522 gravity storage account trie root node before root"
        );
        let mut storage_cursor = tx.cursor_dup_read::<tables::HashedStorages>()?;
        let mut storage_trie_cursor = tx.cursor_dup_read::<tables::StoragesTrieV2>()?;
        for (hashed_address, storage) in hashed_state.storages.iter().take(12) {
            let storage_root_node = storage_trie_cursor
                .get_by_key_subkey(*hashed_address, StoredNibblesSubKey(Nibbles::new()))?
                .map(|entry| entry.node);
            info!(
                target: "gravity_storage::state_root",
                hashed_address=?hashed_address,
                wiped=?storage.wiped,
                storage_root_node=?storage_root_node,
                "lightman0522 gravity storage storage trie root node before root"
            );
            for (hashed_slot, post_value) in storage.storage.iter().take(12) {
                let db_storage = storage_cursor.get_by_key_subkey(*hashed_address, *hashed_slot)?;
                info!(
                    target: "gravity_storage::state_root",
                    hashed_address=?hashed_address,
                    hashed_slot=?hashed_slot,
                    db_value=?db_storage.as_ref().map(|entry| entry.value),
                    post_value=?post_value,
                    "lightman0522 gravity storage hashed storage table before root"
                );
            }
        }
        let nested_hash = NestedStateRoot::new(&tx, Some(self.cache.clone()));
        let (state_root, trie_updates) = nested_hash.calculate(hashed_state)?;
        let trie_storage_nodes: usize =
            trie_updates.storage_tries.values().map(|updates| updates.storage_nodes.len()).sum();
        let trie_storage_removed_nodes: usize =
            trie_updates.storage_tries.values().map(|updates| updates.removed_nodes.len()).sum();
        info!(
            target: "gravity_storage::state_root",
            state_root=?state_root,
            trie_account_nodes=?trie_updates.account_nodes.len(),
            trie_account_removed_nodes=?trie_updates.removed_nodes.len(),
            trie_storage_tries=?trie_updates.storage_tries.len(),
            trie_storage_nodes=?trie_storage_nodes,
            trie_storage_removed_nodes=?trie_storage_removed_nodes,
            "lightman0522 gravity storage state_root output"
        );
        Ok((state_root, trie_updates))
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
    block_number_to_id: Arc<Mutex<BTreeMap<u64, B256>>>,
}

impl<Tx> RawBlockViewProvider<Tx> {
    /// Create `RawBlockViewProvider`
    pub fn new(
        tx: Tx,
        cache: Option<PersistBlockCache>,
        block_number_to_id: Arc<Mutex<BTreeMap<u64, B256>>>,
    ) -> Self {
        Self { tx, cache, block_number_to_id }
    }
}

impl<Tx: DbTx> DatabaseRef for RawBlockViewProvider<Tx> {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.basic_account(&address) {
                return Ok(value.map(Into::into));
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
            .get_by_key_subkey(address, storage_key)?
            .filter(|e| e.key == storage_key)
            .map(|e| e.value)
            .unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let block_number_to_id = self.block_number_to_id.lock().unwrap();
        if let Some(block_id) = block_number_to_id.get(&number) {
            return Ok(*block_id);
        }
        // Check if the requested block is beyond the latest known block.
        if block_number_to_id.last_key_value().is_none_or(|(&max, _)| number > max) {
            return Err(ProviderError::other(BlockHashError::BlockTooHigh(number)));
        }
        // The requested block is older than the maintained history window.
        Err(ProviderError::other(BlockHashError::BlockTooOld(number)))
    }
}

/// Block view provider
#[derive(Debug)]
pub struct BlockViewProvider {
    db: StateProviderDatabase<StateProviderBox>,
    cache: Option<PersistBlockCache>,
    block_number_to_id: Arc<Mutex<BTreeMap<u64, B256>>>,
}

impl BlockViewProvider {
    /// Create a new `BlockViewProvider`
    pub fn new(
        db: StateProviderDatabase<StateProviderBox>,
        cache: Option<PersistBlockCache>,
        block_number_to_id: Arc<Mutex<BTreeMap<u64, B256>>>,
    ) -> Self {
        Self { db, cache, block_number_to_id }
    }
}

impl DatabaseRef for BlockViewProvider {
    type Error = ProviderError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(cache) = &self.cache {
            if let Some(value) = cache.basic_account(&address) {
                return Ok(value.map(Into::into));
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

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let block_number_to_id = self.block_number_to_id.lock().unwrap();
        if let Some(block_id) = block_number_to_id.get(&number) {
            return Ok(*block_id);
        }
        // Check if the requested block is beyond the latest known block.
        if block_number_to_id.last_key_value().is_none_or(|(&max, _)| number > max) {
            return Err(ProviderError::other(BlockHashError::BlockTooHigh(number)));
        }
        // The requested block is older than the maintained history window.
        Err(ProviderError::other(BlockHashError::BlockTooOld(number)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_db_api::mock::TxMock;

    #[test]
    fn test_block_hash_ref() {
        let hash = |v: u8| -> B256 {
            let mut b = [0u8; 32];
            b[31] = v;
            B256::from(b)
        };

        let map: Arc<Mutex<BTreeMap<u64, B256>>> =
            Arc::new(Mutex::new(BTreeMap::from([(100, hash(1)), (101, hash(2)), (102, hash(3))])));

        let provider = RawBlockViewProvider::new(TxMock::default(), None, map);

        // Found
        assert_eq!(provider.block_hash_ref(100).unwrap(), hash(1));
        assert_eq!(provider.block_hash_ref(101).unwrap(), hash(2));
        assert_eq!(provider.block_hash_ref(102).unwrap(), hash(3));

        // Too high
        let err = provider.block_hash_ref(103).unwrap_err();
        assert!(matches!(
            err.downcast_other_ref::<BlockHashError>().unwrap(),
            BlockHashError::BlockTooHigh(103)
        ));

        // Too old
        let err = provider.block_hash_ref(99).unwrap_err();
        assert!(matches!(
            err.downcast_other_ref::<BlockHashError>().unwrap(),
            BlockHashError::BlockTooOld(99)
        ));
    }
}
