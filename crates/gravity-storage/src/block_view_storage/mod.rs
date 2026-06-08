use crate::GravityStorage;
use alloy_primitives::{Address, B256, U256};
use reth_db_api::{cursor::DbDupCursorRO, tables, transaction::DbTx, Database};
use reth_primitives_traits::AlloyBlockHeader;
use reth_provider::{
    BlockNumReader, BlockReader, DBProvider, DatabaseProviderFactory, HeaderProvider,
    PersistBlockCache, ProviderError, ProviderResult, StateProviderBox, PERSIST_BLOCK_CACHE,
};
use reth_revm::{
    bytecode::Bytecode, database::StateProviderDatabase, primitives::BLOCK_HASH_HISTORY,
    state::AccountInfo, DatabaseRef,
};
use reth_storage_api::StateProviderFactory;
use reth_trie::{updates::TrieUpdatesV2, HashedPostState};
use reth_trie_parallel::nested_hash::NestedStateRoot;
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

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
        let tx = self.client.database_provider_ro()?.into_tx();
        let nested_hash = NestedStateRoot::new(&tx, Some(self.cache.clone()));
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

    fn randomness_by_height(&self, block_number: u64) -> ProviderResult<Option<B256>> {
        let provider = self.client.database_provider_ro()?;
        Ok(provider.header_by_number(block_number)?.and_then(|header| header.mix_hash()))
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
    use alloy_consensus::Header;
    use alloy_eips::BlockNumHash;
    use reth_chainspec::ChainInfo;
    use reth_db_api::mock::{DatabaseMock, TxMock};
    use reth_provider::{test_utils::MockEthProvider, BlockHashReader, BlockIdReader};

    #[derive(Clone, Debug)]
    struct TestClient {
        provider: MockEthProvider,
    }

    impl DatabaseProviderFactory for TestClient {
        type DB = DatabaseMock;
        type Provider = MockEthProvider;
        type ProviderRW = MockEthProvider;

        fn database_provider_ro(&self) -> ProviderResult<Self::Provider> {
            Ok(self.provider.clone())
        }

        fn database_provider_rw(&self) -> ProviderResult<Self::ProviderRW> {
            Ok(self.provider.clone())
        }
    }

    impl BlockHashReader for TestClient {
        fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
            self.provider.block_hash(number)
        }

        fn canonical_hashes_range(&self, start: u64, end: u64) -> ProviderResult<Vec<B256>> {
            self.provider.canonical_hashes_range(start, end)
        }
    }

    impl BlockNumReader for TestClient {
        fn chain_info(&self) -> ProviderResult<ChainInfo> {
            self.provider.chain_info()
        }

        fn best_block_number(&self) -> ProviderResult<u64> {
            self.provider.best_block_number()
        }

        fn last_block_number(&self) -> ProviderResult<u64> {
            self.provider.last_block_number()
        }

        fn block_number(&self, hash: B256) -> ProviderResult<Option<u64>> {
            self.provider.block_number(hash)
        }
    }

    impl BlockIdReader for TestClient {
        fn pending_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            self.provider.pending_block_num_hash()
        }

        fn safe_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            self.provider.safe_block_num_hash()
        }

        fn finalized_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            self.provider.finalized_block_num_hash()
        }
    }

    impl StateProviderFactory for TestClient {
        fn latest(&self) -> ProviderResult<StateProviderBox> {
            self.provider.latest()
        }

        fn state_by_block_number_or_tag(
            &self,
            number_or_tag: alloy_eips::BlockNumberOrTag,
        ) -> ProviderResult<StateProviderBox> {
            self.provider.state_by_block_number_or_tag(number_or_tag)
        }

        fn history_by_block_number(&self, block: u64) -> ProviderResult<StateProviderBox> {
            self.provider.history_by_block_number(block)
        }

        fn history_by_block_hash(&self, block: B256) -> ProviderResult<StateProviderBox> {
            self.provider.history_by_block_hash(block)
        }

        fn state_by_block_hash(&self, block: B256) -> ProviderResult<StateProviderBox> {
            self.provider.state_by_block_hash(block)
        }

        fn pending(&self) -> ProviderResult<StateProviderBox> {
            self.provider.pending()
        }

        fn pending_state_by_hash(
            &self,
            block_hash: B256,
        ) -> ProviderResult<Option<StateProviderBox>> {
            self.provider.pending_state_by_hash(block_hash)
        }

        fn maybe_pending(&self) -> ProviderResult<Option<StateProviderBox>> {
            self.provider.maybe_pending()
        }
    }

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

    #[test]
    fn randomness_by_height_returns_header_mix_hash_for_known_block() {
        let provider = MockEthProvider::new();
        let block_hash = B256::repeat_byte(0x11);
        let randomness = B256::repeat_byte(0x22);
        provider.add_header(
            block_hash,
            Header { number: 7, mix_hash: randomness, ..Default::default() },
        );
        let storage = BlockViewStorage::new(TestClient { provider });

        assert_eq!(GravityStorage::randomness_by_height(&storage, 7).unwrap(), Some(randomness));
    }

    #[test]
    fn randomness_by_height_returns_none_for_unknown_block() {
        let provider = MockEthProvider::new();
        provider.add_header(
            B256::repeat_byte(0x11),
            Header { number: 7, mix_hash: B256::repeat_byte(0x22), ..Default::default() },
        );
        let storage = BlockViewStorage::new(TestClient { provider });

        assert_eq!(GravityStorage::randomness_by_height(&storage, 8).unwrap(), None);
    }
}
