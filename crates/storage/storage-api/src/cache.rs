use alloy_primitives::{Address, B256, U256};
use core::sync::atomic::{AtomicU64, Ordering};
use dashmap::{DashMap, DashSet};
use metrics::Histogram;
use metrics_derive::Metrics;
use moka::sync::Cache;
use reth_primitives::{Account, Bytecode};
use reth_trie::{
    updates::{StorageTrieUpdates, TrieUpdates},
    BranchNodeCompact, HashedPostState, Nibbles,
};
use reth_trie_db::TrieCacheReader;
use revm::db::states::{PlainStorageChangeset, StateChangeset};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Metrics)]
#[metrics(scope = "storage")]
struct CacheMetrics {
    /// Block cache hit ratio
    block_cache_hit_ratio: Histogram,
    /// Trie cache hit ratio
    trie_cache_hit_ratio: Histogram,
    /// Number of cached items
    cache_num_items: Histogram,
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
    visit_cnt: AtomicU64,
    hit_cnt: AtomicU64,
}

impl HitRecorder {
    fn visit(&self) {
        self.visit_cnt.fetch_add(1, Ordering::Relaxed);
    }

    fn hit(&self) {
        self.hit_cnt.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) -> f64 {
        let visit_cnt = self.visit_cnt.swap(0, Ordering::Relaxed);
        let hit_cnt = self.hit_cnt.swap(0, Ordering::Relaxed);
        let hit_ratio = if visit_cnt > 0 { hit_cnt as f64 / visit_cnt as f64 } else { 1.0 };
        hit_ratio
    }
}

impl CacheMetricsReporter {
    fn cached_items(&self, num: u64) {
        self.cached_items.store(num, Ordering::Relaxed);
    }

    fn report(&self) {
        self.metrics.block_cache_hit_ratio.record(self.block_cache_hit_record.report());
        self.metrics.trie_cache_hit_ratio.record(self.trie_cache_hit_record.report());
        let cached_items = self.cached_items.load(Ordering::Relaxed) as f64;
        self.metrics.cache_num_items.record(cached_items);
    }
}

#[derive(Hash, PartialEq, Eq, Clone)]
enum CacheKey {
    StateAccount(Address),
    StateContract(B256),
    StateStorage(Address, B256),
    HashAccount(B256),
    HashStorage(B256, B256),
    TrieAccout(Nibbles),
    TrieStorage(B256, Nibbles),
}

#[derive(Clone)]
enum CacheValue {
    StateAccount(Account),
    StateContract(Bytecode),
    StateStorage(U256),
    HashAccount(Account),
    HashStorage(U256),
    TrieAccout(BranchNodeCompact),
    TrieStorage(BranchNodeCompact),
}

#[derive(Default)]
struct PersistBlockCacheInner {
    account_state: DashMap<Address, DashSet<B256>>,
    hashed_state: DashMap<B256, DashSet<B256>>,
    trie_updates: DashMap<B256, DashSet<Nibbles>>,
    block_number: Mutex<(Option<u64>, bool)>,
    metrics: CacheMetricsReporter,
}

#[derive(Clone)]
pub struct PersistBlockCache {
    inner: Arc<PersistBlockCacheInner>,
    cache: Arc<Cache<CacheKey, CacheValue>>,
}

impl std::fmt::Debug for PersistBlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistBlockCache").field("num_cached", &self.cache.entry_count()).finish()
    }
}

impl Default for PersistBlockCache {
    fn default() -> Self {
        let inner = Arc::new(PersistBlockCacheInner::default());
        let eviction_storages = inner.clone();
        let cache = Cache::builder()
            .max_capacity(1000_000)
            .time_to_live(Duration::from_secs(60 * 60))
            .time_to_idle(Duration::from_secs(10 * 60))
            .eviction_listener(move |k: Arc<CacheKey>, _, _| match k.as_ref() {
                CacheKey::StateStorage(address, slot) => {
                    let mut remove = false;
                    if let Some(cached_slots) = eviction_storages.account_state.get(address) {
                        cached_slots.remove(slot);
                        remove = cached_slots.is_empty();
                    }
                    if remove {
                        match eviction_storages.account_state.entry(*address) {
                            dashmap::Entry::Occupied(entry) => {
                                if entry.get().is_empty() {
                                    entry.remove();
                                }
                            }
                            dashmap::Entry::Vacant(_) => {}
                        }
                    }
                }
                CacheKey::HashStorage(hash_address, hash_slot) => {
                    let mut remove = false;
                    if let Some(cached_slots) = eviction_storages.hashed_state.get(hash_address) {
                        cached_slots.remove(hash_slot);
                        remove = cached_slots.is_empty();
                    }
                    if remove {
                        match eviction_storages.hashed_state.entry(*hash_address) {
                            dashmap::Entry::Occupied(entry) => {
                                if entry.get().is_empty() {
                                    entry.remove();
                                }
                            }
                            dashmap::Entry::Vacant(_) => {}
                        }
                    }
                }
                CacheKey::TrieStorage(hash_address, nibbles) => {
                    let mut remove = false;
                    if let Some(cached_slots) = eviction_storages.trie_updates.get(hash_address) {
                        cached_slots.remove(nibbles);
                        remove = cached_slots.is_empty();
                    }
                    if remove {
                        match eviction_storages.trie_updates.entry(*hash_address) {
                            dashmap::Entry::Occupied(entry) => {
                                if entry.get().is_empty() {
                                    entry.remove();
                                }
                            }
                            dashmap::Entry::Vacant(_) => {}
                        }
                    }
                }
                _ => {}
            })
            .build();
        Self { inner, cache: Arc::new(cache) }
    }
}

impl TrieCacheReader for PersistBlockCache {
    fn hashed_account(&self, hash_address: B256) -> Option<Account> {
        self.inner.metrics.trie_cache_hit_record.visit();
        self.cache.get(&CacheKey::HashAccount(hash_address)).and_then(|v| match v {
            CacheValue::HashAccount(account) => {
                self.inner.metrics.trie_cache_hit_record.hit();
                Some(account)
            }
            _ => None,
        })
    }

    fn hashed_storage(&self, hash_address: B256, hash_slot: B256) -> Option<U256> {
        self.inner.metrics.trie_cache_hit_record.visit();
        self.cache.get(&CacheKey::HashStorage(hash_address, hash_slot)).and_then(|v| match v {
            CacheValue::HashStorage(value) => {
                self.inner.metrics.trie_cache_hit_record.hit();
                Some(value)
            }
            _ => None,
        })
    }

    fn trie_account(&self, nibbles: Nibbles) -> Option<BranchNodeCompact> {
        self.inner.metrics.trie_cache_hit_record.visit();
        self.cache.get(&CacheKey::TrieAccout(nibbles)).and_then(|v| match v {
            CacheValue::TrieAccout(branch) => {
                self.inner.metrics.trie_cache_hit_record.hit();
                Some(branch)
            }
            _ => None,
        })
    }

    fn trie_storage(&self, hash_address: B256, nibbles: Nibbles) -> Option<BranchNodeCompact> {
        self.inner.metrics.trie_cache_hit_record.visit();
        self.cache.get(&CacheKey::TrieStorage(hash_address, nibbles)).and_then(|v| match v {
            CacheValue::TrieStorage(branch) => {
                self.inner.metrics.trie_cache_hit_record.hit();
                Some(branch)
            }
            _ => None,
        })
    }
}

impl PersistBlockCache {
    pub fn basic_account(&self, address: &Address) -> Option<Account> {
        self.inner.metrics.block_cache_hit_record.visit();
        self.cache.get(&CacheKey::StateAccount(*address)).and_then(|v| match v {
            CacheValue::StateAccount(account) => {
                self.inner.metrics.block_cache_hit_record.hit();
                Some(account)
            }
            _ => None,
        })
    }

    pub fn bytecode_by_hash(&self, code_hash: &B256) -> Option<Bytecode> {
        self.inner.metrics.block_cache_hit_record.visit();
        self.cache.get(&CacheKey::StateContract(*code_hash)).and_then(|v| match v {
            CacheValue::StateContract(code) => {
                self.inner.metrics.block_cache_hit_record.hit();
                Some(code)
            }
            _ => None,
        })
    }

    pub fn storage(&self, address: Address, slot: B256) -> Option<U256> {
        self.inner.metrics.block_cache_hit_record.visit();
        self.cache.get(&CacheKey::StateStorage(address, slot)).and_then(|v| match v {
            CacheValue::StateStorage(value) => {
                self.inner.metrics.block_cache_hit_record.hit();
                Some(value)
            }
            _ => None,
        })
    }

    pub fn clear(&self) {
        let mut check = self.inner.block_number.lock().unwrap();
        if !check.1 {
            panic!("Cannot clear cache while writing");
        }
        self.inner.account_state.clear();
        self.inner.hashed_state.clear();
        self.inner.trie_updates.clear();
        self.cache.invalidate_all();
        check.0 = None;
    }

    pub fn update_lock(&self, block_number: u64) {
        let mut check = self.inner.block_number.lock().unwrap();
        if let Some(last_block_number) = check.0 {
            let expect = last_block_number + 1;
            if block_number != expect {
                panic!(
                    "Write discontinuous block number, expect {}, actual {}",
                    expect, block_number
                );
            }
        }
        if check.1 {
            panic!("Cannot write two blocks simultaneously");
        }
        check.1 = true;
    }

    pub fn commit(&self, block_number: u64) {
        self.inner.metrics.cached_items(self.cache.entry_count());
        let mut check = self.inner.block_number.lock().unwrap();
        if !check.1 {
            panic!("Should check continuous before commit");
        }
        check.1 = false;

        if let Some(last_block_number) = check.0 {
            let expect = last_block_number + 1;
            if block_number != expect {
                panic!(
                    "Commit discontinuous block number, expect {}, actual {}",
                    expect, block_number
                );
            }
        }
        check.0 = Some(block_number);
        self.inner.metrics.report();
    }

    pub fn write_state_changes(&self, changes: StateChangeset) {
        // write account to database.
        for (address, account) in changes.accounts {
            if let Some(account) = account {
                self.cache.insert(
                    CacheKey::StateAccount(address),
                    CacheValue::StateAccount(account.into()),
                );
            } else {
                self.cache.remove(&CacheKey::StateAccount(address));
            }
        }

        // Write bytecode
        for (hash, bytecode) in changes.contracts {
            self.cache.insert(
                CacheKey::StateContract(hash),
                CacheValue::StateContract(Bytecode(bytecode)),
            );
        }

        // Write new storage state and wipe storage if needed.
        for PlainStorageChangeset { address, wipe_storage, storage } in changes.storage {
            // Wiping of storage.
            if wipe_storage {
                if let Some((_, storage_slots)) = self.inner.account_state.remove(&address) {
                    for slot in storage_slots {
                        self.cache.remove(&CacheKey::StateStorage(address, slot));
                    }
                }
            }
            for (slot, value) in storage {
                let slot: B256 = slot.into();
                if value.is_zero() {
                    // delete slot
                    self.cache.remove(&CacheKey::StateStorage(address, slot));
                    let mut clean = false;
                    if let Some(cached_slots) = self.inner.account_state.get(&address) {
                        cached_slots.remove(&slot);
                        clean = cached_slots.is_empty();
                    }
                    if clean {
                        match self.inner.account_state.entry(address) {
                            dashmap::Entry::Occupied(entry) => {
                                if entry.get().is_empty() {
                                    entry.remove();
                                }
                            }
                            dashmap::Entry::Vacant(_) => {}
                        }
                    }
                } else {
                    self.cache.insert(
                        CacheKey::StateStorage(address, slot),
                        CacheValue::StateStorage(value),
                    );
                    if let Some(cached_slots) = self.inner.account_state.get(&address) {
                        cached_slots.insert(slot);
                    } else {
                        self.inner.account_state.entry(address).or_default().insert(slot);
                    }
                }
            }
        }
    }

    pub fn write_hashed_state(&self, hashed_state: HashedPostState) {
        // Write hashed account updates.
        for (hashed_address, account) in hashed_state.accounts {
            if let Some(account) = account {
                self.cache.insert(
                    CacheKey::HashAccount(hashed_address),
                    CacheValue::HashAccount(account),
                );
            } else {
                self.cache.remove(&CacheKey::HashAccount(hashed_address));
            }
        }

        // Write hashed storage changes.
        for (hashed_address, storage) in hashed_state.storages {
            if storage.wiped {
                if let Some((_, storage_slots)) = self.inner.hashed_state.remove(&hashed_address) {
                    for slot in storage_slots {
                        self.cache.remove(&CacheKey::HashStorage(hashed_address, slot));
                    }
                }

                for (hashed_slot, value) in storage.storage {
                    if value.is_zero() {
                        // delete slot
                        self.cache.remove(&CacheKey::HashStorage(hashed_address, hashed_slot));
                        let mut clean = false;
                        if let Some(cached_slots) = self.inner.hashed_state.get(&hashed_address) {
                            cached_slots.remove(&hashed_slot);
                            clean = cached_slots.is_empty();
                        }
                        if clean {
                            match self.inner.hashed_state.entry(hashed_address) {
                                dashmap::Entry::Occupied(entry) => {
                                    if entry.get().is_empty() {
                                        entry.remove();
                                    }
                                }
                                dashmap::Entry::Vacant(_) => {}
                            }
                        }
                    } else {
                        self.cache.insert(
                            CacheKey::HashStorage(hashed_address, hashed_slot),
                            CacheValue::HashStorage(value),
                        );
                        if let Some(cached_slots) = self.inner.hashed_state.get(&hashed_address) {
                            cached_slots.insert(hashed_slot);
                        } else {
                            self.inner
                                .hashed_state
                                .entry(hashed_address)
                                .or_default()
                                .insert(hashed_slot);
                        }
                    }
                }
            }
        }
    }

    pub fn write_trie_updates(&self, trie_updates: TrieUpdates) {
        if trie_updates.is_empty() {
            return;
        }

        // Merge updated and removed nodes. Updated nodes must take precedence.
        let TrieUpdates { account_nodes, removed_nodes, storage_tries } = trie_updates;
        for removed_node in removed_nodes {
            if !account_nodes.contains_key(&removed_node) {
                self.cache.remove(&CacheKey::TrieAccout(removed_node));
            }
        }
        for (nibbles, node) in account_nodes {
            if !nibbles.is_empty() {
                self.cache.insert(CacheKey::TrieAccout(nibbles), CacheValue::TrieAccout(node));
            }
        }

        for (hashed_address, StorageTrieUpdates { is_deleted, storage_nodes, removed_nodes }) in
            storage_tries
        {
            if is_deleted {
                if let Some((_, nibble_slots)) = self.inner.trie_updates.remove(&hashed_address) {
                    for nibbles in nibble_slots {
                        self.cache.remove(&CacheKey::TrieStorage(hashed_address, nibbles));
                    }
                }
            }
            for removed_node in removed_nodes {
                if !storage_nodes.contains_key(&removed_node) {
                    self.cache.remove(&CacheKey::TrieStorage(hashed_address, removed_node.clone()));
                    let mut clean = false;
                    if let Some(cached_slots) = self.inner.trie_updates.get(&hashed_address) {
                        cached_slots.remove(&removed_node);
                        clean = cached_slots.is_empty();
                    }
                    if clean {
                        match self.inner.trie_updates.entry(hashed_address) {
                            dashmap::Entry::Occupied(entry) => {
                                if entry.get().is_empty() {
                                    entry.remove();
                                }
                            }
                            dashmap::Entry::Vacant(_) => {}
                        }
                    }
                }
            }
            for (nibbles, node) in storage_nodes {
                if !nibbles.is_empty() {
                    self.cache.insert(
                        CacheKey::TrieStorage(hashed_address, nibbles.clone()),
                        CacheValue::TrieStorage(node),
                    );
                    if let Some(cached_slots) = self.inner.trie_updates.get(&hashed_address) {
                        cached_slots.insert(nibbles);
                    } else {
                        self.inner.trie_updates.entry(hashed_address).or_default().insert(nibbles);
                    }
                }
            }
        }
    }
}
