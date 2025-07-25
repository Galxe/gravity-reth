use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{Address, B256, U256};
use core::{
    ops::Deref,
    sync::atomic::{AtomicU64, Ordering},
};
use dashmap::DashMap;
use metrics::{Gauge, Histogram};
use metrics_derive::Metrics;
use once_cell::sync::Lazy;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use reth_primitives_traits::Account;
use reth_trie_common::{nested_trie::Node, updates::TrieUpdatesV2, HashedPostState, Nibbles};
use revm_bytecode::Bytecode;
use revm_database::{states::StorageSlot, BundleAccount, OriginalValuesKnown};
use std::{
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const MAX_PERSISTENCE_GAP: u64 = 64;
const CACHE_METRIS_INTERVAL: Duration = Duration::from_secs(15); // 15s
const CACHE_EVICTION_INTERVAL: Duration = Duration::from_secs(300); // 5min
const CACHE_SIZE_THRESHOLD: usize = 2_000_000;
const CACHE_CONTRACTS_THRESHOLD: usize = 2_000;

#[derive(Metrics)]
#[metrics(scope = "storage")]
struct CacheMetrics {
    /// Block cache hit ratio
    block_cache_hit_ratio: Gauge,
    /// Trie cache hit ratio
    trie_cache_hit_ratio: Gauge,
    /// Number of cached items
    cache_num_items: Gauge,
    /// Lastest pre-merged block number
    latest_merged_block_number: Gauge,
    /// Latest stored block number
    latest_persist_block_number: Gauge,
    /// Latest eviction block number
    latest_eviction_block_number: Gauge,
    /// Eviction duration
    eviction_duration: Histogram,
}

#[derive(Default)]
struct CacheMetricsReporter {
    block_cache_hit_record: HitRecorder,
    trie_cache_hit_record: HitRecorder,
    cached_items: AtomicU64,
    merged_block_number: AtomicU64,
    persist_block_number: AtomicU64,
    eviction_block_number: AtomicU64,
    metrics: CacheMetrics,
}

#[derive(Default)]
struct HitRecorder {
    not_hit_cnt: AtomicU64,
    hit_cnt: AtomicU64,
}

impl HitRecorder {
    fn not_hit(&self) {
        self.not_hit_cnt.fetch_add(1, Ordering::Relaxed);
    }

    fn hit(&self) {
        self.hit_cnt.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) -> Option<f64> {
        let not_hit_cnt = self.not_hit_cnt.swap(0, Ordering::Relaxed);
        let hit_cnt = self.hit_cnt.swap(0, Ordering::Relaxed);
        let visit_cnt = not_hit_cnt + hit_cnt;
        (visit_cnt > 0).then(|| hit_cnt as f64 / visit_cnt as f64)
    }
}

impl CacheMetricsReporter {
    fn report(&self) {
        if let Some(hit_ratio) = self.block_cache_hit_record.report() {
            self.metrics.block_cache_hit_ratio.set(hit_ratio);
        }
        if let Some(hit_ratio) = self.trie_cache_hit_record.report() {
            self.metrics.trie_cache_hit_ratio.set(hit_ratio);
        }
        let cached_items = self.cached_items.load(Ordering::Relaxed) as f64;
        self.metrics.cache_num_items.set(cached_items);
        self.metrics
            .latest_merged_block_number
            .set(self.merged_block_number.load(Ordering::Relaxed) as f64);
        self.metrics
            .latest_persist_block_number
            .set(self.persist_block_number.load(Ordering::Relaxed) as f64);
        self.metrics
            .latest_eviction_block_number
            .set(self.eviction_block_number.load(Ordering::Relaxed) as f64);
    }
}

#[allow(dead_code)]
struct ValueWithTip<V> {
    value: V,
    block_number: u64,
}

impl<V> ValueWithTip<V> {
    const fn new(value: V, block_number: u64) -> Self {
        Self { value, block_number }
    }
}

/// Inner of `PersistBlockCache`
#[derive(Default)]
pub struct PersistBlockCacheInner {
    persist_wait: Arc<(Mutex<bool>, Condvar)>,
    accounts: DashMap<Address, ValueWithTip<Account>>,
    storage: DashMap<Address, DashMap<U256, ValueWithTip<U256>>>,
    contracts: DashMap<B256, ValueWithTip<Bytecode>>,
    account_trie: DashMap<Nibbles, ValueWithTip<Node>>,
    storage_trie: DashMap<B256, DashMap<Nibbles, ValueWithTip<Node>>>,
    merged_block_number: Mutex<Option<u64>>,
    persist_block_number: Mutex<Option<u64>>,
    metrics: CacheMetricsReporter,
    daemon_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Cache account state and world trie
#[derive(Clone, Debug)]
pub struct PersistBlockCache(Arc<PersistBlockCacheInner>);

impl Deref for PersistBlockCache {
    type Target = PersistBlockCacheInner;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl std::fmt::Debug for PersistBlockCacheInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistBlockCache")
            .field("num_cached", &self.metrics.cached_items.load(Ordering::Relaxed))
            .finish()
    }
}

impl PersistBlockCacheInner {
    fn entry_count(&self) -> usize {
        let mut num_items = self.accounts.len();
        num_items += self.storage.iter().map(|s| s.len()).sum::<usize>();
        num_items += self.account_trie.len();
        num_items += self.storage_trie.iter().map(|s| s.len()).sum::<usize>();
        num_items
    }
}

/// Single instance of cached state
pub static PERSIST_BLOCK_CACHE: Lazy<PersistBlockCache> = Lazy::new(PersistBlockCache::new);

impl Default for PersistBlockCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistBlockCache {
    /// Create a new `PersistBlockCache`
    pub fn new() -> Self {
        let inner = PersistBlockCacheInner {
            persist_wait: Arc::new((Mutex::new(false), Condvar::new())),
            ..Default::default()
        };
        let inner = Arc::new(inner);

        let weak_inner = Arc::downgrade(&inner);
        let handle = thread::spawn(move || {
            let interval = CACHE_METRIS_INTERVAL; // 15s
            let eviction_interval = CACHE_EVICTION_INTERVAL; // 5min
            let mut last_eviction_time = Instant::now();
            let mut last_eviction_contract = Instant::now();
            let mut last_eviction_height = 0;
            let mut overflow = false;
            loop {
                thread::sleep(interval);
                if let Some(inner) = weak_inner.upgrade() {
                    let start = Instant::now();
                    let num_items = inner.entry_count();
                    inner.metrics.cached_items.store(num_items as u64, Ordering::Release);
                    inner.metrics.report();

                    // check and eviction contracts
                    if inner.contracts.len() > CACHE_CONTRACTS_THRESHOLD &&
                        last_eviction_contract.elapsed() >= eviction_interval
                    {
                        let persist_height =
                            inner.persist_block_number.lock().unwrap().unwrap_or(0);
                        let mut max_height = 512;
                        while inner.contracts.len() > CACHE_CONTRACTS_THRESHOLD {
                            let eviction_height = persist_height.saturating_sub(max_height);
                            if eviction_height == 0 || eviction_height == persist_height {
                                break;
                            }
                            inner.contracts.retain(|_, v| v.block_number > eviction_height);
                            max_height /= 2;
                        }
                        last_eviction_contract = Instant::now();
                    }
                    // check and eviction account and trie state
                    if num_items > CACHE_SIZE_THRESHOLD {
                        let persist_height =
                            inner.persist_block_number.lock().unwrap().unwrap_or(0);
                        if last_eviction_time.elapsed() >= eviction_interval && persist_height > 0 {
                            overflow = true;
                            let eviction_height = if last_eviction_height == 0 {
                                persist_height.saturating_sub(512).max(persist_height / 2)
                            } else {
                                (persist_height + last_eviction_height) / 2
                            };
                            inner.accounts.retain(|_, v| v.block_number > eviction_height);
                            inner.storage.iter().for_each(|s| {
                                s.retain(|_, v| v.block_number > eviction_height);
                            });
                            inner.storage.retain(|_, s| !s.is_empty());
                            inner.account_trie.retain(|_, v| v.block_number > eviction_height);
                            inner.storage_trie.iter().for_each(|s| {
                                s.retain(|_, v| v.block_number > eviction_height);
                            });
                            inner.storage_trie.retain(|_, s| !s.is_empty());
                            last_eviction_height = eviction_height;
                            inner
                                .metrics
                                .eviction_block_number
                                .store(eviction_height, Ordering::Relaxed);
                        }
                    } else if overflow {
                        overflow = false;
                        last_eviction_time = Instant::now();
                    }
                    inner.metrics.metrics.eviction_duration.record(start.elapsed());
                } else {
                    break;
                }
            }
        });
        inner.daemon_handle.lock().unwrap().replace(handle);

        Self(inner)
    }

    /// Wait if there's a large gap between executed block and persist block
    pub fn wait_persist_gap(&self) {
        let (lock, cvar) = self.persist_wait.as_ref();
        let mut large_gap = lock.lock().unwrap();
        while *large_gap {
            large_gap = cvar.wait(large_gap).unwrap();
        }
    }

    /// Get account from cache
    pub fn basic_account(&self, address: &Address) -> Option<Account> {
        if let Some(value) = self.accounts.get(address) {
            self.metrics.block_cache_hit_record.hit();
            Some(value.value)
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    /// Cache latest read account
    pub fn cache_account(&self, address: Address, account: Account) {
        if let dashmap::Entry::Vacant(entry) = self.accounts.entry(address) {
            entry.insert(ValueWithTip::new(
                account,
                self.metrics.persist_block_number.load(Ordering::Relaxed),
            ));
        }
    }

    /// Get byte code from cache
    pub fn bytecode_by_hash(&self, code_hash: &B256) -> Option<Bytecode> {
        if let Some(value) = self.contracts.get(code_hash) {
            self.metrics.block_cache_hit_record.hit();
            Some(value.value.clone())
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    /// Cache latest read bytecode
    pub fn cache_byte_code(&self, code_hash: B256, byte_code: Bytecode) {
        if let dashmap::Entry::Vacant(entry) = self.contracts.entry(code_hash) {
            entry.insert(ValueWithTip::new(
                byte_code,
                self.metrics.persist_block_number.load(Ordering::Relaxed),
            ));
        }
    }

    /// Get storage slot from cache
    pub fn storage(&self, address: &Address, slot: &U256) -> Option<U256> {
        if let Some(storage) = self.storage.get(address) {
            if let Some(value) = storage.get(slot) {
                self.metrics.block_cache_hit_record.hit();
                Some(value.value)
            } else {
                self.metrics.block_cache_hit_record.not_hit();
                None
            }
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    /// Cache latest read storage
    pub fn cache_storage(&self, address: Address, slot: U256, value: U256) {
        if let Some(storage) = self.storage.get(&address) {
            if let dashmap::Entry::Vacant(entry) = storage.entry(slot) {
                entry.insert(ValueWithTip::new(
                    value,
                    self.metrics.persist_block_number.load(Ordering::Relaxed),
                ));
            }
        } else {
            match self.storage.entry(address) {
                dashmap::Entry::Occupied(entry) => {
                    entry.get().insert(
                        slot,
                        ValueWithTip::new(
                            value,
                            self.metrics.persist_block_number.load(Ordering::Relaxed),
                        ),
                    );
                }
                dashmap::Entry::Vacant(entry) => {
                    let data = DashMap::new();
                    data.insert(
                        slot,
                        ValueWithTip::new(
                            value,
                            self.metrics.persist_block_number.load(Ordering::Relaxed),
                        ),
                    );
                    entry.insert(data);
                }
            }
        }
    }

    /// Get account trie node from cache
    pub fn trie_account(&self, nibbles: &Nibbles) -> Option<Node> {
        if let Some(value) = self.account_trie.get(nibbles) {
            self.metrics.trie_cache_hit_record.hit();
            Some(value.value.clone())
        } else {
            self.metrics.trie_cache_hit_record.not_hit();
            None
        }
    }

    /// Get storage trie node from cache
    pub fn trie_storage(&self, hash_address: &B256, nibbles: &Nibbles) -> Option<Node> {
        if let Some(storage) = self.storage_trie.get(hash_address) {
            if let Some(value) = storage.get(nibbles) {
                self.metrics.trie_cache_hit_record.hit();
                Some(value.value.clone())
            } else {
                self.metrics.trie_cache_hit_record.not_hit();
                None
            }
        } else {
            self.metrics.trie_cache_hit_record.not_hit();
            None
        }
    }

    /// Hint for the persist block number
    pub fn persist_tip(&self, block_number: u64) {
        self.metrics.persist_block_number.store(block_number, Ordering::Relaxed);
        let mut guard = self.persist_block_number.lock().unwrap();
        if let Some(ref mut persist_block_number) = *guard {
            assert_eq!(
                block_number,
                *persist_block_number + 1,
                "Persist uncontinuous block, expect: {}, actual: {}",
                *persist_block_number + 1,
                block_number
            );
            *persist_block_number = block_number;
        } else {
            *guard = Some(block_number);
        }
        if let Some(merged_block_number) = *self.merged_block_number.lock().unwrap() {
            let (lock, cvar) = self.persist_wait.as_ref();
            let mut large_gap = lock.lock().unwrap();
            *large_gap = merged_block_number - block_number >= MAX_PERSISTENCE_GAP;
            cvar.notify_all();
        }
    }

    /// Write account state after a block is executed.
    pub fn write_state_changes<'a>(
        &self,
        block_number: u64,
        is_value_known: OriginalValuesKnown,
        state: impl IntoParallelIterator<Item = (&'a Address, &'a BundleAccount)>,
        contracts: impl IntoParallelIterator<Item = (&'a B256, &'a Bytecode)>,
    ) {
        self.metrics.merged_block_number.store(block_number, Ordering::Relaxed);
        {
            let mut guard = self.merged_block_number.lock().unwrap();
            if let Some(ref mut merged_block_number) = *guard {
                assert!(
                    (block_number == *merged_block_number + 1),
                    "Merged uncontinuous block, expect: {}, actual: {}",
                    *merged_block_number + 1,
                    block_number
                );
                *merged_block_number = block_number;
            } else {
                *guard = Some(block_number);
            }
        }

        // Write bytecode
        contracts
            .into_par_iter()
            .filter(|(b, _)| **b != KECCAK_EMPTY)
            .map(|(b, code)| (*b, code.clone()))
            .for_each(|(hash, bytecode)| {
                self.contracts.insert(hash, ValueWithTip::new(bytecode, block_number));
            });

        state.into_par_iter().for_each(|(address, account)| {
            // Append account info if it is changed.
            let was_destroyed = account.was_destroyed();
            if is_value_known.is_not_known() || account.is_info_changed() {
                // write account to database.
                let info = account.info.clone();
                if let Some(info) = info {
                    self.accounts.insert(*address, ValueWithTip::new(info.into(), block_number));
                } else {
                    self.accounts.remove(address);
                }
            }

            if was_destroyed {
                self.storage.remove(address);
            }
            let write_slot = |kv: (U256, StorageSlot)| {
                let (slot, slot_value) = kv;
                // If storage was destroyed that means that storage was wiped.
                // In that case we need to check if present storage value is different then ZERO.
                let destroyed_and_not_zero = was_destroyed && !slot_value.present_value.is_zero();

                // If account is not destroyed check if original values was changed,
                // so we can update it.
                let not_destroyed_and_changed = !was_destroyed && slot_value.is_changed();

                if is_value_known.is_not_known() ||
                    destroyed_and_not_zero ||
                    not_destroyed_and_changed
                {
                    let value = slot_value.present_value;
                    if value.is_zero() {
                        // delete slot
                        if let Some(storage) = self.storage.get(address) {
                            storage.remove(&slot);
                        }
                    } else if let Some(storage) = self.storage.get(address) {
                        storage.insert(slot, ValueWithTip::new(value, block_number));
                    } else {
                        match self.storage.entry(*address) {
                            dashmap::Entry::Occupied(entry) => {
                                entry.get().insert(slot, ValueWithTip::new(value, block_number));
                            }
                            dashmap::Entry::Vacant(entry) => {
                                let data = DashMap::new();
                                data.insert(slot, ValueWithTip::new(value, block_number));
                                entry.insert(data);
                            }
                        }
                    }
                }
            };

            // Append storage changes
            // Note: Assumption is that revert is going to remove whole plain storage from
            // database so we can check if plain state was wiped or not.
            if account.storage.len() > 256 {
                account.storage.par_iter().map(|(k, v)| (*k, *v)).for_each(write_slot);
            } else {
                for kv in account.storage.iter().map(|(k, v)| (*k, *v)) {
                    write_slot(kv);
                }
            }
        })
    }

    /// Write trie updates.
    pub fn write_trie_updates(&self, input: &TrieUpdatesV2, block_number: u64) {
        input.removed_nodes.par_iter().for_each(|path| {
            self.account_trie.remove(path);
        });
        input.account_nodes.par_iter().for_each(|(path, node)| {
            self.account_trie.insert(*path, ValueWithTip::new(node.clone().reset(), block_number));
        });

        let write_slot = |data: &DashMap<Nibbles, ValueWithTip<Node>>, kv: (&Nibbles, &Node)| {
            data.insert(*kv.0, ValueWithTip::new(kv.1.clone().reset(), block_number));
        };
        input.storage_tries.par_iter().for_each(|(hashed_address, storage_trie_update)| {
            if storage_trie_update.is_deleted {
                self.storage_trie.remove(hashed_address);
            } else {
                if let Some(storage) = self.storage_trie.get(hashed_address) {
                    if storage_trie_update.removed_nodes.len() > 256 {
                        storage_trie_update.removed_nodes.par_iter().for_each(|path| {
                            storage.remove(path);
                        });
                    } else {
                        for path in &storage_trie_update.removed_nodes {
                            storage.remove(path);
                        }
                    }
                }

                if let Some(storage) = self.storage_trie.get(hashed_address) {
                    if storage_trie_update.storage_nodes.len() > 256 {
                        storage_trie_update.storage_nodes.par_iter().for_each(|kv| {
                            write_slot(storage.value(), kv);
                        });
                    } else {
                        for kv in &storage_trie_update.storage_nodes {
                            write_slot(storage.value(), kv);
                        }
                    }
                } else {
                    match self.storage_trie.entry(*hashed_address) {
                        dashmap::Entry::Occupied(entry) => {
                            if storage_trie_update.storage_nodes.len() > 256 {
                                storage_trie_update.storage_nodes.par_iter().for_each(|kv| {
                                    write_slot(entry.get(), kv);
                                });
                            } else {
                                for kv in &storage_trie_update.storage_nodes {
                                    write_slot(entry.get(), kv);
                                }
                            }
                        }
                        dashmap::Entry::Vacant(entry) => {
                            let data = DashMap::new();
                            if storage_trie_update.storage_nodes.len() > 256 {
                                storage_trie_update.storage_nodes.par_iter().for_each(|kv| {
                                    write_slot(&data, kv);
                                });
                            } else {
                                for kv in &storage_trie_update.storage_nodes {
                                    write_slot(&data, kv);
                                }
                            }
                            entry.insert(data);
                        }
                    }
                }
            }
        });
    }
}

/// Stage Data channel that from execution to merkle stage
#[derive(Clone, Debug, Default)]
pub struct ExecutionMerkleChannel {
    data: Arc<Mutex<Option<(u64, HashedPostState)>>>,
}

impl ExecutionMerkleChannel {
    /// send data to merkle stage
    pub fn send(&self, block_number: u64, hashed_state: HashedPostState) {
        let mut data = self.data.lock().unwrap();
        assert!(data.is_none());
        *data = Some((block_number, hashed_state));
    }

    /// consume data in merkle stage
    pub fn consume(&self, block_number: u64) -> HashedPostState {
        let data = self.data.lock().unwrap().take().unwrap();
        assert_eq!(data.0, block_number);
        data.1
    }
}

/// Single instance of `ExecutionMerkleChannel`
pub static EXECUTION_MERKLE_CHANNEL: Lazy<ExecutionMerkleChannel> =
    Lazy::new(ExecutionMerkleChannel::default);
