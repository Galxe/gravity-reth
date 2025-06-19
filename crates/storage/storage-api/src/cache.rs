use alloy_primitives::{Address, B256, U256};
use core::{
    ops::Deref,
    sync::atomic::{AtomicU64, Ordering},
};
use dashmap::DashMap;
use metrics::Gauge;
use metrics_derive::Metrics;
use once_cell::sync::Lazy;
use reth_primitives_traits::Account;
use reth_trie_common::{nested_trie::Node, updates::TrieUpdatesV2, Nibbles};
use revm_bytecode::Bytecode;
use revm_database::states::{PlainStorageChangeset, StateChangeset};
use std::{
    sync::{Arc, Mutex},
    thread,
    thread::JoinHandle,
    time::Duration,
};

#[derive(Metrics)]
#[metrics(scope = "storage")]
struct CacheMetrics {
    /// Block cache hit ratio
    block_cache_hit_ratio: Gauge,
    /// Trie cache hit ratio
    trie_cache_hit_ratio: Gauge,
    /// Number of cached items
    cache_num_items: Gauge,
}

#[derive(Default)]
struct CacheMetricsReporter {
    block_cache_hit_record: HitRecorder,
    trie_cache_hit_record: HitRecorder,
    cached_items: AtomicU64,
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
        if visit_cnt > 0 {
            Some(hit_cnt as f64 / visit_cnt as f64)
        } else {
            None
        }
    }
}

impl CacheMetricsReporter {
    fn cached_items(&self, num: u64) {
        self.cached_items.store(num, Ordering::Relaxed);
    }

    fn report(&self) {
        if let Some(hit_ratio) = self.block_cache_hit_record.report() {
            self.metrics.block_cache_hit_ratio.set(hit_ratio);
        }
        if let Some(hit_ratio) = self.trie_cache_hit_record.report() {
            self.metrics.trie_cache_hit_ratio.set(hit_ratio);
        }
        let cached_items = self.cached_items.load(Ordering::Relaxed) as f64;
        self.metrics.cache_num_items.set(cached_items);
    }
}

struct ValueWithTip<V> {
    value: V,
    block_number: u64,
}

impl<V> ValueWithTip<V> {
    fn new(value: V, block_number: u64) -> Self {
        Self { value, block_number }
    }
}

#[derive(Default)]
pub struct PersistBlockCacheInner {
    accounts: DashMap<Address, ValueWithTip<Account>>,
    storage: DashMap<Address, DashMap<U256, ValueWithTip<U256>>>,
    contracts: DashMap<B256, Bytecode>,
    account_trie: DashMap<Nibbles, ValueWithTip<Node>>,
    storage_trie: DashMap<B256, DashMap<Nibbles, ValueWithTip<Node>>>,
    persist_block_number: Mutex<Option<u64>>,
    metrics: CacheMetricsReporter,
    daemon_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug)]
pub struct PersistBlockCache(Arc<PersistBlockCacheInner>);

impl Deref for PersistBlockCache {
    type Target = PersistBlockCacheInner;

    fn deref(&self) -> &Self::Target {
        &self.0.as_ref()
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

pub static PERSIST_BLOCK_CACHE: Lazy<PersistBlockCache> = Lazy::new(|| PersistBlockCache::new());

impl PersistBlockCache {
    pub fn new() -> Self {
        let inner = Arc::new(PersistBlockCacheInner::default());

        let weak_inner = Arc::downgrade(&inner);
        let handle = thread::spawn(move || {
            let interval = Duration::from_secs(5); // 5s
            loop {
                thread::sleep(interval);
                if let Some(inner) = weak_inner.upgrade() {
                    let num_items = inner.entry_count();
                    inner.metrics.cached_items.store(num_items as u64, Ordering::Release);
                    inner.metrics.report();
                } else {
                    break;
                }
            }
        });
        inner.daemon_handle.lock().unwrap().replace(handle);

        Self(inner)
    }

    pub fn basic_account(&self, address: &Address) -> Option<Account> {
        if let Some(value) = self.accounts.get(address) {
            self.metrics.block_cache_hit_record.hit();
            Some(value.value.clone())
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    pub fn bytecode_by_hash(&self, code_hash: &B256) -> Option<Bytecode> {
        if let Some(value) = self.contracts.get(code_hash) {
            self.metrics.block_cache_hit_record.hit();
            Some(value.clone())
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    pub fn storage(&self, address: &Address, slot: &U256) -> Option<U256> {
        if let Some(storage) = self.storage.get(address) {
            if let Some(value) = storage.get(slot) {
                self.metrics.block_cache_hit_record.hit();
                Some(value.value.clone())
            } else {
                self.metrics.block_cache_hit_record.not_hit();
                None
            }
        } else {
            self.metrics.block_cache_hit_record.not_hit();
            None
        }
    }

    pub fn trie_account(&self, nibbles: &Nibbles) -> Option<Node> {
        if let Some(value) = self.account_trie.get(nibbles) {
            self.metrics.trie_cache_hit_record.hit();
            Some(value.value.clone())
        } else {
            self.metrics.trie_cache_hit_record.not_hit();
            None
        }
    }

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

    pub fn persist_tip(&self, block_number: u64) {
        let mut guard = self.persist_block_number.lock().unwrap();
        if let Some(ref mut persist_block_number) = *guard {
            if block_number != *persist_block_number + 1 {
                panic!(
                    "Persist uncontinuous block, expect: {}, actual: {}",
                    *persist_block_number + 1,
                    block_number
                );
            }
            *persist_block_number = block_number;
        } else {
            *guard = Some(block_number);
        }
    }

    pub fn write_state_changes(&self, block_number: u64, changes: StateChangeset) {
        // write account to database.
        for (address, account) in changes.accounts {
            if let Some(account) = account {
                self.accounts.insert(address, ValueWithTip::new(account.into(), block_number));
            } else {
                self.accounts.remove(&address);
            }
        }

        // Write bytecode
        for (hash, bytecode) in changes.contracts {
            self.contracts.insert(hash, bytecode);
        }

        // Write new storage state and wipe storage if needed.
        for PlainStorageChangeset { address, wipe_storage, storage } in changes.storage {
            // Wiping of storage.
            if wipe_storage {
                self.storage.remove(&address);
            }
            for (slot, value) in storage {
                if value.is_zero() {
                    // delete slot
                    let mut clean = false;
                    if let Some(storage) = self.storage.get(&address) {
                        storage.remove(&slot);
                        clean = storage.is_empty();
                    }
                    if clean {
                        match self.storage.entry(address) {
                            dashmap::Entry::Occupied(entry) => {
                                if entry.get().is_empty() {
                                    entry.remove();
                                }
                            }
                            _ => {}
                        }
                    }
                } else {
                    if let Some(storage) = self.storage.get(&address) {
                        storage.insert(slot, ValueWithTip::new(value, block_number));
                    } else {
                        match self.storage.entry(address) {
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
            }
        }
    }

    pub fn write_trie_updates(&self, input: &TrieUpdatesV2, block_number: u64) {
        for path in &input.removed_nodes {
            self.account_trie.remove(path);
        }
        for (path, node) in &input.account_nodes {
            self.account_trie.insert(path.clone(), ValueWithTip::new(node.clone(), block_number));
        }

        for (hashed_address, storage_trie_update) in &input.storage_tries {
            if storage_trie_update.is_deleted {
                self.storage_trie.remove(hashed_address);
            } else {
                if let Some(storage) = self.storage_trie.get(hashed_address) {
                    for path in &storage_trie_update.removed_nodes {
                        storage.remove(path);
                    }
                }
                if let Some(storage) = self.storage_trie.get(hashed_address) {
                    for (path, node) in &storage_trie_update.storage_nodes {
                        storage.insert(path.clone(), ValueWithTip::new(node.clone(), block_number));
                    }
                } else {
                    match self.storage_trie.entry(*hashed_address) {
                        dashmap::Entry::Occupied(entry) => {
                            let data = entry.get();
                            for (path, node) in &storage_trie_update.storage_nodes {
                                data.insert(
                                    path.clone(),
                                    ValueWithTip::new(node.clone(), block_number),
                                );
                            }
                        }
                        dashmap::Entry::Vacant(entry) => {
                            let data = DashMap::new();
                            for (path, node) in &storage_trie_update.storage_nodes {
                                data.insert(
                                    path.clone(),
                                    ValueWithTip::new(node.clone(), block_number),
                                );
                            }
                            entry.insert(data);
                        }
                    }
                }
            }
        }
    }
}
