use once_cell::sync::Lazy;
use reth_metrics::{metrics::Histogram, Metrics};
use reth_provider::{
    providers::ConsistentDbView, BlockNumReader, BlockReader, DatabaseProviderFactory,
    HeaderProvider, StateCommitmentProvider,
};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{
    errors::provider::ProviderError, StateProviderBox, StateProviderFactory, STATE_PROVIDER_OPTS,
};
use reth_trie::{updates::TrieUpdates, HashedPostState, KeccakKeyHasher, TrieInput};
use reth_trie_parallel::root::ParallelStateRoot;
use revm::{
    db::{
        states::{CacheAccount, PlainAccount},
        BundleState,
    },
    primitives::{AccountInfo, Address, Bytecode, HashMap, B256, BLOCK_HASH_HISTORY, U256},
    DatabaseRef,
};
use std::{
    collections::BTreeMap,
    hash::Hash,
    sync::{Arc, Mutex},
    time::Instant,
};
use tracing::info;

use crate::{GravityStorage, GravityStorageError};

static USE_PARALLEL_STATE_ROOT: Lazy<bool> =
    Lazy::new(|| std::env::var("USE_PARALLEL_STATE_ROOT").is_ok());

#[derive(Metrics)]
#[metrics(scope = "block_view_storage")]
struct BlockViewStorageMetrics {
    /// How long it took for parallel state root calculation
    parallel_state_root_duration: Histogram,
    /// How long it took for parallel state root input calculation
    parallel_state_root_input_duration: Histogram,
}

pub struct BlockViewStorage<Client> {
    client: Client,
    inner: Mutex<BlockViewStorageInner>,
    metrics: BlockViewStorageMetrics,
}

struct BlockViewStorageInner {
    state_provider_info: (B256, u64), // (block_hash, block_number),
    block_number_to_view: BTreeMap<u64, (Arc<BlockView>, Arc<HashedPostState>)>,
    block_number_to_trie_updates: BTreeMap<u64, Arc<TrieUpdates>>,
    block_number_to_id: BTreeMap<u64, B256>,
    trie_input_cache: Option<TrieInputCache>,
}

fn get_state_provider<Client: StateProviderFactory + 'static>(
    client: &Client,
    block_hash: B256,
    parallel: bool,
) -> Result<StateProviderBox, GravityStorageError> {
    let state_provider = if parallel {
        client.state_by_block_hash_with_opts(block_hash, STATE_PROVIDER_OPTS.clone())
    } else {
        client.state_by_block_hash(block_hash)
    };

    match state_provider {
        Ok(state_provider) => Ok(state_provider),
        Err(err) => Err(GravityStorageError::StateProviderError((block_hash, err))),
    }
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
    pub fn new(
        client: Client,
        latest_block_number: u64,
        latest_block_hash: B256,
        block_number_to_id: BTreeMap<u64, B256>,
    ) -> Self {
        info!("USE_PARALLEL_STATE_ROOT is {}", *USE_PARALLEL_STATE_ROOT);
        Self {
            client,
            inner: Mutex::new(BlockViewStorageInner::new(
                latest_block_number,
                latest_block_hash,
                block_number_to_id,
            )),
            metrics: BlockViewStorageMetrics::default(),
        }
    }
}

impl BlockViewStorageInner {
    fn new(block_number: u64, block_hash: B256, block_number_to_id: BTreeMap<u64, B256>) -> Self {
        Self {
            state_provider_info: (block_hash, block_number),
            block_number_to_view: BTreeMap::new(),
            block_number_to_trie_updates: BTreeMap::new(),
            block_number_to_id,
            trie_input_cache: None,
        }
    }

    fn prune_block_id(&mut self, canonical_block_number: u64) {
        if canonical_block_number <= BLOCK_HASH_HISTORY {
            return;
        }

        // Only keep the last BLOCK_HASH_HISTORY block hashes before the canonical block number,
        // including the canonical block number.
        let target_block_number = canonical_block_number - BLOCK_HASH_HISTORY;
        while let Some((&first_key, _)) = self.block_number_to_id.first_key_value() {
            if first_key <= target_block_number {
                self.block_number_to_id.pop_first();
            } else {
                break;
            }
        }
    }
}

/// Get historical states from start block number (inclusive) to end block number (exclusive).
fn get_historical_states(
    storage: &BlockViewStorageInner,
    start: u64,
    end: u64,
) -> (Vec<Arc<HashedPostState>>, Vec<Arc<TrieUpdates>>) {
    let hashed_state_vec: Vec<_> =
        storage.block_number_to_view.range(start..end).map(|(_, view)| view.1.clone()).collect();

    let trie_updates_vec: Vec<_> = storage
        .block_number_to_trie_updates
        .range(start..end)
        .map(|(_, trie_updates)| trie_updates.clone())
        .collect();

    // Block number should be continuous
    assert_eq!(hashed_state_vec.len() as u64, end - start);
    assert_eq!(trie_updates_vec.len() as u64, end - start);

    (hashed_state_vec, trie_updates_vec)
}

#[derive(Default, Debug, Clone)]
struct TrieInputCache {
    trie_input: TrieInput,
    base_block_number: u64,
    last_block_number: u64,
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

        let block_id = *storage
            .block_number_to_id
            .get(&target_block_number)
            .unwrap_or_else(|| panic!("Block number {} not found", target_block_number));
        let block_number_to_id = storage.block_number_to_id.clone();
        let block_views: Vec<_> = storage
            .block_number_to_view
            .range(base_block_number + 1..target_block_number + 1)
            .rev()
            .map(|(_, view)| view.0.clone())
            .collect();
        drop(storage);

        // Block number should be continuous
        assert_eq!(block_views.len() as u64, target_block_number - base_block_number);

        Ok((
            block_id,
            BlockViewProvider::new(
                block_views,
                block_number_to_id,
                get_state_provider(&self.client, base_block_hash, true)?,
            ),
        ))
    }

    fn insert_block_id(&self, block_number: u64, block_id: B256) {
        let mut storage = self.inner.lock().unwrap();
        storage.block_number_to_id.insert(block_number, block_id);
    }

    fn get_block_id(&self, block_number: u64) -> Option<B256> {
        let storage = self.inner.lock().unwrap();
        storage.block_number_to_id.get(&block_number).cloned()
    }

    fn insert_bundle_state(&self, block_number: u64, bundle_state: &BundleState) {
        let block_view = BlockView {
            accounts: bundle_state
                .state()
                .iter()
                .map(|(addr, acc)| {
                    let storage = acc.storage.iter().map(|(k, v)| (*k, v.present_value)).collect();
                    let plain_account =
                        acc.account_info().map(|info| PlainAccount { info, storage });
                    (*addr, CacheAccount { account: plain_account, status: acc.status })
                })
                .collect(),
            contracts: bundle_state.contracts.clone(),
        };
        let hashed_state =
            Arc::new(HashedPostState::from_bundle_state::<KeccakKeyHasher>(&bundle_state.state));
        let mut storage = self.inner.lock().unwrap();
        storage.block_number_to_view.insert(block_number, (Arc::new(block_view), hashed_state));
    }

    fn update_canonical(&self, block_number: u64, block_hash: B256) {
        let mut storage = self.inner.lock().unwrap();
        let gc_block_number = storage.state_provider_info.1;
        if *USE_PARALLEL_STATE_ROOT {
            let provider_ro = self.client.database_provider_ro().unwrap();
            let last_num = provider_ro.best_block_number().unwrap();
            if last_num > gc_block_number {
                storage.state_provider_info = provider_ro
                    .sealed_header(last_num)
                    .unwrap()
                    .map(|h| (h.hash(), last_num))
                    .unwrap();
                for block_number_ in gc_block_number..last_num {
                    storage.block_number_to_view.remove(&block_number_);
                    storage.block_number_to_trie_updates.remove(&block_number_);
                }
            }
        } else {
            assert_eq!(block_number, gc_block_number + 1);
            storage.state_provider_info = (block_hash, block_number);
            storage.block_number_to_view.remove(&gc_block_number);
            storage.block_number_to_trie_updates.remove(&gc_block_number);
        }
        storage.prune_block_id(block_number);
    }

    fn state_root_with_updates(
        &self,
        block_number: u64,
    ) -> Result<(B256, Arc<HashedPostState>, Arc<TrieUpdates>), GravityStorageError> {
        let hashed_state = {
            let storage = self.inner.lock().unwrap();
            storage.block_number_to_view.get(&block_number).unwrap().1.clone()
        };

        let (state_root, trie_updates) = if *USE_PARALLEL_STATE_ROOT {
            let consistent_view = ConsistentDbView::new_with_best_tip(self.client.clone()).unwrap();
            let (_, base_block_number) = consistent_view.tip.unwrap();
            let start_time = Instant::now();
            let input_cache =
                self.inner.lock().unwrap().trie_input_cache.take().and_then(|input_cache| {
                    if input_cache.base_block_number == base_block_number {
                        assert_eq!(input_cache.last_block_number + 2, block_number);
                        Some(input_cache.trie_input)
                    } else {
                        assert!(input_cache.base_block_number < base_block_number);
                        None
                    }
                });
            let mut input = if let Some(mut input) = input_cache {
                let storage = self.inner.lock().unwrap();
                // Extend with contents of the last in-memory block
                let trie_updates = storage
                    .block_number_to_trie_updates
                    .get(&(block_number - 1))
                    .unwrap_or_else(|| panic!("Block number {} not found", block_number))
                    .clone();
                let hashed_state = storage
                    .block_number_to_view
                    .get(&(block_number - 1))
                    .unwrap_or_else(|| panic!("Block number {} not found", block_number))
                    .1
                    .clone();
                drop(storage);
                input.append_cached_ref(trie_updates.as_ref(), hashed_state.as_ref());
                input
            } else {
                let mut input = TrieInput::default();
                let (hashed_state_vec, trie_updates_vec) = {
                    let storage = self.inner.lock().unwrap();
                    get_historical_states(&storage, base_block_number + 1, block_number)
                };
                // Extend with contents of parent in-memory blocks
                for (hashed_state, trie_update) in
                    hashed_state_vec.iter().zip(trie_updates_vec.iter())
                {
                    input.append_cached_ref(trie_update.as_ref(), hashed_state.as_ref());
                }
                input
            };
            self.inner.lock().unwrap().trie_input_cache = Some(TrieInputCache {
                trie_input: input.clone(),
                base_block_number,
                last_block_number: block_number - 1,
            });
            // Extend with block we are validating root for.
            input.append_ref(hashed_state.as_ref());
            self.metrics.parallel_state_root_input_duration.record(start_time.elapsed());

            let start_time = Instant::now();
            let result = ParallelStateRoot::new(consistent_view, input)
                .incremental_root_with_updates()
                .unwrap();
            self.metrics.parallel_state_root_duration.record(start_time.elapsed());
            result
        } else {
            let storage = self.inner.lock().unwrap();
            let (base_block_hash, base_block_number) = storage.state_provider_info;
            let (hashed_state_vec, trie_updates_vec) =
                get_historical_states(&storage, base_block_number + 1, block_number);
            drop(storage);

            let state_provider = get_state_provider(&self.client, base_block_hash, false)?;
            state_provider
                .state_root_with_updates_v2(
                    hashed_state.as_ref().clone(),
                    hashed_state_vec,
                    trie_updates_vec,
                )
                .unwrap()
        };
        let trie_updates = Arc::new(trie_updates);

        {
            let mut storage = self.inner.lock().unwrap();
            storage.block_number_to_trie_updates.insert(block_number, trie_updates.clone());
        }

        Ok((state_root, hashed_state, trie_updates))
    }
}

struct BlockView {
    /// Block state account with account state.
    accounts: HashMap<Address, CacheAccount>,
    /// Created contracts.
    contracts: HashMap<B256, Bytecode>,
}

pub struct BlockViewProvider {
    block_views: Vec<Arc<BlockView>>,
    block_number_to_id: BTreeMap<u64, B256>,
    db: StateProviderDatabase<StateProviderBox>,
}

impl BlockViewProvider {
    fn new(
        block_views: Vec<Arc<BlockView>>,
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
                return Ok(account.account_info());
            }
        }
        self.db.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        for block_view in &self.block_views {
            if let Some(bytecode) = block_view.contracts.get(&code_hash) {
                return Ok(bytecode.clone());
            }
        }
        self.db.code_by_hash_ref(code_hash)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        for block_view in &self.block_views {
            if let Some(entry) = block_view.accounts.get(&address) {
                // if account was destroyed or account is newly built
                // we return zero and don't ask database.
                match &entry.account {
                    Some(account) => {
                        if let Some(value) = account.storage.get(&index) {
                            return Ok(*value);
                        } else if entry.status.is_storage_known() {
                            return Ok(U256::ZERO);
                        } else {
                            continue;
                        }
                    }
                    None => {
                        return Ok(U256::ZERO);
                    }
                }
            }
        }
        self.db.storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        Ok(*self.block_number_to_id.get(&number).unwrap())
    }
}
