use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{Address, B256, U256};
use core::{
    hash::Hash,
    ops::Deref,
    sync::atomic::{AtomicU64, Ordering},
};
use dashmap::DashMap;
use gravity_primitives::get_gravity_config;
use metrics::{Gauge, Histogram};
use metrics_derive::Metrics;
use once_cell::sync::Lazy;
use rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use reth_primitives_traits::Account;
use reth_trie_common::{
    nested_trie::{Node, StoredNode},
    updates::TrieUpdatesV2,
    Nibbles,
};
use revm_bytecode::Bytecode;
use revm_database::{BundleAccount, OriginalValuesKnown};
use std::{
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const CACHE_METRICS_INTERVAL: Duration = Duration::from_secs(15); // 15s
const CACHE_CONTRACTS_THRESHOLD: usize = 2_000;

/// Interval at which the cache daemon reports metrics and runs eviction.
///
/// Defaults to [`CACHE_METRICS_INTERVAL`] (15s). Set `GRETH_CACHE_METRICS_INTERVAL_MS`
/// (milliseconds, must be > 0) to shorten it — intended for tests that need eviction to fire
/// within a short run rather than waiting for the production cadence.
fn cache_metrics_interval() -> Duration {
    std::env::var("GRETH_CACHE_METRICS_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map_or(CACHE_METRICS_INTERVAL, Duration::from_millis)
}

#[derive(Metrics)]
#[metrics(scope = "storage")]
struct CacheMetrics {
    /// Block cache hit ratio
    block_cache_hit_ratio: Gauge,
    /// Trie cache hit ratio
    trie_cache_hit_ratio: Gauge,
    /// Number of cached items
    cache_num_items: Gauge,
    /// Latest pre-merged block number
    latest_merged_block_number: Gauge,
    /// Latest stored block number
    latest_persist_block_number: Gauge,
    /// Latest eviction block number
    latest_eviction_block_number: Gauge,
    /// Eviction duration
    eviction_duration: Histogram,
    /// Wait persist duration
    wait_persist_duration: Histogram,
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
    /// Record whether a lookup was answered authoritatively by the cache.
    fn record(&self, hit: bool) {
        if hit {
            self.hit_cnt.fetch_add(1, Ordering::Relaxed);
        } else {
            self.not_hit_cnt.fetch_add(1, Ordering::Relaxed);
        }
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

/// A cached value tagged with the block number it was written at.
///
/// The block tag is the single knob both correctness and eviction hinge on: an entry may only be
/// evicted once it is persisted (`block <= persist_height`), so a not-yet-persisted entry is never
/// dropped and never falls through to a stale DB.
struct Tip<V> {
    value: V,
    block: u64,
}

impl<V> Tip<V> {
    const fn new(value: V, block: u64) -> Self {
        Self { value, block }
    }

    /// Whether this entry may be dropped at the given eviction watermark.
    ///
    /// Callers must guarantee `eviction_height <= persist_height`, so anything evictable is
    /// already in the DB.
    const fn evictable(&self, eviction_height: u64) -> bool {
        self.block <= eviction_height
    }
}

/// A single-level cache with tombstones.
///
/// The inner `Option` is the tombstone: `Some` is a live value, `None` records a *deletion* so a
/// read resolves to "absent" instead of falling through to a not-yet-pruned DB. Used for accounts
/// and account-trie nodes (keys that are read directly, never wiped wholesale).
struct Layer<K, V>(DashMap<K, Tip<Option<V>>>);

impl<K: Eq + Hash, V> Default for Layer<K, V> {
    fn default() -> Self {
        Self(DashMap::new())
    }
}

impl<K: Eq + Hash, V: Clone> Layer<K, V> {
    /// Read: `None` = miss (fall through to DB); `Some(None)` = tombstone; `Some(Some(v))` = live.
    fn get(&self, key: &K) -> Option<Option<V>> {
        self.0.get(key).map(|t| t.value.clone())
    }

    /// Overwrite the entry with `value` (`None` writes a tombstone).
    fn set(&self, key: K, value: Option<V>, block: u64) {
        self.0.insert(key, Tip::new(value, block));
    }

    fn put(&self, key: K, value: V, block: u64) {
        self.set(key, Some(value), block);
    }

    fn tombstone(&self, key: K, block: u64) {
        self.set(key, None, block);
    }

    /// Populate from a DB read without clobbering an existing (merged) entry.
    fn cache_read(&self, key: K, value: V, block: u64) {
        if let dashmap::Entry::Vacant(entry) = self.0.entry(key) {
            entry.insert(Tip::new(Some(value), block));
        }
    }

    fn evict(&self, eviction_height: u64) {
        self.0.retain(|_, t| !t.evictable(eviction_height));
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

/// The cached sub-tree for one wipeable key (an account / hashed address).
struct Sub<K2, V> {
    /// Block at which the whole sub-tree was wiped, if any.
    ///
    /// While `wiped_at > persist_height` the wipe is not yet reflected in the DB, so a sub-key
    /// *miss* must resolve to "absent" rather than reading the stale DB. Once persisted the marker
    /// is irrelevant (and cleared by eviction).
    wiped_at: Option<u64>,
    map: DashMap<K2, Tip<Option<V>>>,
}

impl<K2: Eq + Hash, V> Default for Sub<K2, V> {
    fn default() -> Self {
        Self { wiped_at: None, map: DashMap::new() }
    }
}

/// A two-level cache whose top-level keys can be wiped wholesale (self-destruct).
///
/// Tombstones (per sub-key) plus a top-level `wiped_at` marker together guarantee that a deletion
/// — whether of a single sub-key or of an entire key — shadows a not-yet-pruned DB. Used for plain
/// storage and storage-trie nodes (sub-keys read directly by `(key, sub-key)`).
struct WipeLayer<K1, K2, V>(DashMap<K1, Sub<K2, V>>);

impl<K1: Eq + Hash, K2: Eq + Hash, V> Default for WipeLayer<K1, K2, V> {
    fn default() -> Self {
        Self(DashMap::new())
    }
}

impl<K1: Eq + Hash, K2: Eq + Hash, V: Clone> WipeLayer<K1, K2, V> {
    /// Read a sub-key. `None` = miss (fall through to DB); `Some(_)` = answered by the cache,
    /// either a live value, a tombstone, or "absent because the key was wiped and the wipe is not
    /// yet persisted".
    fn get(&self, key: &K1, sub_key: &K2, persist_height: u64) -> Option<Option<V>> {
        let sub = self.0.get(key)?;
        if let Some(t) = sub.map.get(sub_key) {
            return Some(t.value.clone());
        }
        match sub.wiped_at {
            Some(wiped) if wiped > persist_height => Some(None),
            _ => None,
        }
    }

    fn set(&self, key: K1, sub_key: K2, value: Option<V>, block: u64) {
        self.0.entry(key).or_default().map.insert(sub_key, Tip::new(value, block));
    }

    fn put(&self, key: K1, sub_key: K2, value: V, block: u64) {
        self.set(key, sub_key, Some(value), block);
    }

    fn tombstone(&self, key: K1, sub_key: K2, block: u64) {
        self.set(key, sub_key, None, block);
    }

    /// Wipe the whole sub-tree (self-destruct). Replaces the entry atomically with an empty,
    /// `wiped_at`-marked sub-tree; the recreated sub-keys are written afterwards via [`Self::put`].
    fn wipe(&self, key: K1, block: u64) {
        self.0.insert(key, Sub { wiped_at: Some(block), map: DashMap::new() });
    }

    /// Populate from a DB read without clobbering an existing (merged) entry.
    fn cache_read(&self, key: K1, sub_key: K2, value: V, block: u64) {
        if let dashmap::Entry::Vacant(entry) = self.0.entry(key).or_default().map.entry(sub_key) {
            entry.insert(Tip::new(Some(value), block));
        }
    }

    fn evict(&self, eviction_height: u64) {
        self.0.iter_mut().for_each(|mut sub| {
            sub.map.retain(|_, t| !t.evictable(eviction_height));
            // The wipe is now in the DB, so the marker no longer needs to shadow it.
            if sub.wiped_at.is_some_and(|wiped| wiped <= eviction_height) {
                sub.wiped_at = None;
            }
        });
        // Drop a key only once it has no live sub-keys *and* no active wipe marker.
        self.0.retain(|_, sub| !sub.map.is_empty() || sub.wiped_at.is_some());
    }

    fn len(&self) -> usize {
        self.0.iter().map(|sub| sub.map.len()).sum()
    }
}

/// Controls the eviction watermark for one group of tables.
///
/// The watermark is the block height at and below which entries may be dropped. Two rules:
/// * **Correctness (hard):** it is computed as `persist_height - window`, so it never exceeds
///   `persist_height` — not-yet-persisted entries are never evicted.
/// * **Policy (soft):** while over capacity, [`Self::tighten`] halves the retained window each tick
///   (gradual); while under capacity, [`Self::relax`] releases it so the window grows back.
#[derive(Default)]
struct EvictionWatermark {
    height: u64,
}

impl EvictionWatermark {
    /// Under capacity: release the watermark. Nothing is evicted, and the retained window grows as
    /// `persist_height` rises, so the next pressure episode starts from a wide window again.
    const fn relax(&mut self) {
        self.height = 0;
    }

    /// Over capacity: shrink the retained (already-persisted) window by half and return the new
    /// watermark. Derived as `persist_height - window`, so it is always `<= persist_height`.
    const fn tighten(&mut self, persist_height: u64) -> u64 {
        let retained = persist_height.saturating_sub(self.height);
        self.height = persist_height.saturating_sub(retained / 2);
        self.height
    }
}

/// Inner of `PersistBlockCache`
#[derive(Default)]
pub struct PersistBlockCacheInner {
    persist_wait: Arc<(Mutex<bool>, Condvar)>,
    accounts: Layer<Address, Account>,
    contracts: DashMap<B256, Tip<Bytecode>>,
    storage: WipeLayer<Address, U256, U256>,
    account_trie: Layer<Nibbles, StoredNode>,
    storage_trie: WipeLayer<B256, Nibbles, StoredNode>,
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
        self.accounts.len() +
            self.contracts.len() +
            self.storage.len() +
            self.account_trie.len() +
            self.storage_trie.len()
    }

    /// The highest persisted block. Entries at or below this are safe to evict (the DB owns them).
    fn persist_height(&self) -> u64 {
        self.metrics.persist_block_number.load(Ordering::Relaxed)
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
            let interval = cache_metrics_interval();
            let cache_capacity = get_gravity_config().cache_capacity as usize;
            let mut state_wm = EvictionWatermark::default();
            let mut contract_wm = EvictionWatermark::default();
            loop {
                thread::sleep(interval);
                let Some(inner) = weak_inner.upgrade() else { break };
                let start = Instant::now();
                let num_items = inner.entry_count();
                inner.metrics.cached_items.store(num_items as u64, Ordering::Release);
                inner.metrics.report();

                // `persist_height` is the hard ceiling on eviction: entries above it are not yet
                // in the DB and must never be dropped.
                let persist_height = inner.persist_height();

                // Contracts are content-addressed and immutable, so they carry no persist
                // constraint; they are bounded purely by count.
                if inner.contracts.len() > CACHE_CONTRACTS_THRESHOLD {
                    let height = contract_wm.tighten(persist_height);
                    inner.contracts.retain(|_, t| !t.evictable(height));
                } else {
                    contract_wm.relax();
                }

                // State / trie tables: a cache — nothing is evicted under capacity; over capacity
                // the watermark is tightened gradually until back under.
                if num_items > cache_capacity {
                    let height = state_wm.tighten(persist_height);
                    inner.accounts.evict(height);
                    inner.storage.evict(height);
                    inner.account_trie.evict(height);
                    inner.storage_trie.evict(height);
                    inner.metrics.eviction_block_number.store(height, Ordering::Relaxed);
                } else {
                    state_wm.relax();
                }
                inner.metrics.metrics.eviction_duration.record(start.elapsed());
            }
        });
        inner.daemon_handle.lock().unwrap().replace(handle);

        Self(inner)
    }

    /// For test
    pub fn reset(&self) {
        self.persist_block_number.lock().unwrap().take();
    }

    /// Wait if there's a large gap between executed block and persist block
    ///
    /// # Arguments
    /// * `timeout_ms` - Optional timeout in milliseconds. If None, waits indefinitely.
    pub fn wait_persist_gap(&self, timeout_ms: Option<u64>) {
        let (lock, cvar) = self.persist_wait.as_ref();
        let mut large_gap = lock.lock().unwrap();
        let mut wait_duration = None;
        let start_time = Instant::now();

        while *large_gap {
            if wait_duration.is_none() {
                wait_duration = Some(start_time);
            }
            if let Some(timeout_ms) = timeout_ms {
                let elapsed = start_time.elapsed();
                let timeout_duration = Duration::from_millis(timeout_ms);
                if elapsed >= timeout_duration {
                    break;
                }
                let remaining = timeout_duration - elapsed;
                let result = cvar.wait_timeout(large_gap, remaining).unwrap();
                large_gap = result.0;
                if result.1.timed_out() {
                    break;
                }
            } else {
                large_gap = cvar.wait(large_gap).unwrap();
            }
        }

        if let Some(wait_duration) = wait_duration {
            self.metrics.metrics.wait_persist_duration.record(wait_duration.elapsed());
        }
    }

    /// Get account from cache
    pub fn basic_account(&self, address: &Address) -> Option<Option<Account>> {
        let value = self.accounts.get(address);
        self.metrics.block_cache_hit_record.record(value.is_some());
        value
    }

    /// Cache latest read account
    pub fn cache_account(&self, address: Address, account: Account) {
        self.accounts.cache_read(address, account, self.persist_height());
    }

    /// Get byte code from cache
    pub fn bytecode_by_hash(&self, code_hash: &B256) -> Option<Bytecode> {
        if let Some(value) = self.contracts.get(code_hash) {
            self.metrics.block_cache_hit_record.record(true);
            Some(value.value.clone())
        } else {
            self.metrics.block_cache_hit_record.record(false);
            None
        }
    }

    /// Cache bytecode loaded from persistent storage.
    ///
    /// Bytecode is content-addressed and immutable, so read-through caching keeps old hot
    /// contracts warm after their creation block. Capacity pressure is handled asynchronously to
    /// keep eviction out of this DB-read hot path.
    pub fn cache_byte_code(&self, code_hash: B256, byte_code: Bytecode) {
        if let dashmap::Entry::Vacant(entry) = self.contracts.entry(code_hash) {
            entry.insert(Tip::new(byte_code, self.persist_height()));
        }
    }

    /// Get storage slot from cache
    pub fn storage(&self, address: &Address, slot: &U256) -> Option<Option<U256>> {
        // A destroyed account's storage is wiped, so any sub-key resolves to absent via the
        // `wiped_at` marker — no special account-level handling needed here.
        let value = self.storage.get(address, slot, self.persist_height());
        self.metrics.block_cache_hit_record.record(value.is_some());
        value
    }

    /// Cache latest read storage
    pub fn cache_storage(&self, address: Address, slot: U256, value: U256) {
        self.storage.cache_read(address, slot, value, self.persist_height());
    }

    /// Get account trie node from cache.
    ///
    /// The outer `Option` distinguishes a cache miss (`None`) from a cache hit; the inner `Option`
    /// distinguishes a live node (`Some(Some(node))`) from a tombstone (`Some(None)`). A tombstone
    /// short-circuits the DB read.
    pub fn trie_account(&self, nibbles: &Nibbles) -> Option<Option<Node>> {
        let value = self.account_trie.get(nibbles).map(|node| node.map(Into::into));
        self.metrics.trie_cache_hit_record.record(value.is_some());
        value
    }

    /// Get storage trie node from cache.
    ///
    /// See [`Self::trie_account`] for the meaning of the nested `Option`.
    pub fn trie_storage(&self, hash_address: &B256, nibbles: &Nibbles) -> Option<Option<Node>> {
        let value = self
            .storage_trie
            .get(hash_address, nibbles, self.persist_height())
            .map(|node| node.map(Into::into));
        self.metrics.trie_cache_hit_record.record(value.is_some());
        value
    }

    /// Hint for the persist block number
    pub fn persist_tip(&self, block_number: u64) {
        self.metrics.persist_block_number.store(block_number, Ordering::Relaxed);
        let mut guard = self.persist_block_number.lock().unwrap();
        if let Some(ref mut persist_block_number) = *guard {
            assert!(
                block_number > *persist_block_number,
                "Pesist block {} should be greater than: {}",
                block_number,
                *persist_block_number
            );
            *persist_block_number = block_number;
        } else {
            *guard = Some(block_number);
        }
        if let Some(merged_block_number) = *self.merged_block_number.lock().unwrap() {
            let (lock, cvar) = self.persist_wait.as_ref();
            let mut large_gap = lock.lock().unwrap();
            *large_gap =
                merged_block_number - block_number >= get_gravity_config().cache_max_persist_gap;
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
                assert_eq!(
                    block_number,
                    *merged_block_number + 1,
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
                self.contracts.insert(hash, Tip::new(bytecode, block_number));
            });

        state.into_par_iter().for_each(|(address, account)| {
            // Append account info if it is changed.
            let was_destroyed = account.was_destroyed();
            if is_value_known.is_not_known() || account.is_info_changed() {
                self.accounts.set(*address, account.info.clone().map(Into::into), block_number);
            }

            // A self-destruct wipes the whole storage trie. Mark it before writing the recreated
            // slots so that any *other* slot resolves to absent instead of a stale DB read.
            if was_destroyed {
                self.storage.wipe(*address, block_number);
            }

            // Append storage changes
            // Note: Assumption is that revert is going to remove whole plain storage from
            // database so we can check if plain state was wiped or not.
            for (slot, slot_value) in account.storage.iter().map(|(k, v)| (*k, *v)) {
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
                        self.storage.tombstone(*address, slot, block_number);
                    } else {
                        self.storage.put(*address, slot, value, block_number);
                    }
                }
            }
        })
    }

    /// Write trie updates.
    ///
    /// Removed nodes become tombstones; a deleted (wiped) storage trie is marked via
    /// [`WipeLayer::wipe`] and then repopulated with the rebuilt nodes. Both shadow a
    /// not-yet-pruned DB until the change is persisted.
    pub fn write_trie_updates(&self, input: &TrieUpdatesV2, block_number: u64) {
        input.removed_nodes.par_iter().for_each(|path| {
            self.account_trie.tombstone(*path, block_number);
        });
        input.account_nodes.par_iter().for_each(|(path, node)| {
            self.account_trie.put(*path, node.clone().into(), block_number);
        });

        input.storage_tries.par_iter().for_each(|(hashed_address, storage_trie_update)| {
            if storage_trie_update.is_deleted {
                self.storage_trie.wipe(*hashed_address, block_number);
            }
            for path in &storage_trie_update.removed_nodes {
                self.storage_trie.tombstone(*hashed_address, *path, block_number);
            }
            for (path, node) in &storage_trie_update.storage_nodes {
                self.storage_trie.put(*hashed_address, *path, node.clone().into(), block_number);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_trie_common::{nested_trie::NodeFlag, updates::StorageTrieUpdatesV2};

    /// Build a cache without spawning the metrics/eviction daemon thread.
    fn test_cache() -> PersistBlockCache {
        PersistBlockCache(Arc::new(PersistBlockCacheInner::default()))
    }

    /// A leaf node that round-trips cleanly through `Node <-> StoredNode`.
    fn leaf(value: u64) -> Node {
        Node::ShortNode {
            key: Nibbles::from_nibbles_unchecked([0x1, 0x2]),
            value: Box::new(Node::ValueNode(value.to_be_bytes().to_vec())),
            flags: NodeFlag::new(None),
        }
    }

    fn nib(nibbles: impl AsRef<[u8]>) -> Nibbles {
        Nibbles::from_nibbles_unchecked(nibbles)
    }

    fn addr(n: u64) -> B256 {
        B256::from(U256::from(n))
    }

    // --- Layer ---------------------------------------------------------------------------------

    #[test]
    fn layer_miss_present_tombstone() {
        let layer = Layer::<u64, u64>::default();
        assert_eq!(layer.get(&1), None); // miss

        layer.put(1, 100, 5);
        assert_eq!(layer.get(&1), Some(Some(100))); // live

        layer.tombstone(1, 6);
        assert_eq!(layer.get(&1), Some(None)); // tombstone, not a miss
    }

    #[test]
    fn layer_evict_only_below_watermark() {
        let layer = Layer::<u64, u64>::default();
        layer.put(1, 10, 5); // persisted, old
        layer.put(2, 20, 9); // not yet persisted

        layer.evict(7); // watermark 7 (<= persist_height by contract)
        assert_eq!(layer.get(&1), None); // block 5 <= 7 -> evicted
        assert_eq!(layer.get(&2), Some(Some(20))); // block 9 > 7 -> kept
    }

    #[test]
    fn layer_cache_read_does_not_clobber() {
        let layer = Layer::<u64, u64>::default();
        layer.put(1, 100, 9);
        layer.cache_read(1, 999, 1); // existing entry must win
        assert_eq!(layer.get(&1), Some(Some(100)));
    }

    // --- WipeLayer -----------------------------------------------------------------------------

    #[test]
    fn wipe_layer_miss_present_tombstone() {
        let layer = WipeLayer::<u64, u64, u64>::default();
        assert_eq!(layer.get(&1, &2, 0), None);

        layer.put(1, 2, 100, 5);
        assert_eq!(layer.get(&1, &2, 0), Some(Some(100)));

        layer.tombstone(1, 2, 6);
        assert_eq!(layer.get(&1, &2, 0), Some(None));
    }

    #[test]
    fn wipe_layer_wipe_shadows_db_until_persisted() {
        let layer = WipeLayer::<u64, u64, u64>::default();
        // Slot X=5 lives in the DB and the cache (block 1).
        layer.put(1, /* X */ 10, 5, 1);

        // Block 2: destroy + recreate, only slot Y is rewritten.
        layer.wipe(1, 2);
        layer.put(1, /* Y */ 20, 9, 2);

        // While the wipe (block 2) is unpersisted (persist=0), the un-rewritten X must read as
        // absent — NOT a miss that would fall through to the stale DB value 5.
        assert_eq!(layer.get(&1, &10, 0), Some(None));
        assert_eq!(layer.get(&1, &20, 0), Some(Some(9)));

        // Once the wipe block is persisted (persist>=2) the marker steps aside: a miss becomes a
        // real miss again so the (now-wiped) DB is authoritative.
        assert_eq!(layer.get(&1, &10, 2), None);
        // A live, persisted sub-key is still served from cache regardless.
        assert_eq!(layer.get(&1, &20, 2), Some(Some(9)));
    }

    #[test]
    fn wipe_layer_evict_clears_marker_and_drops_empty() {
        let layer = WipeLayer::<u64, u64, u64>::default();
        layer.wipe(1, 2); // wipe-to-empty at block 2

        // Not yet persisted: the empty-but-wiped key must survive eviction and keep shadowing.
        layer.evict(1);
        assert_eq!(layer.get(&1, &10, 0), Some(None));

        // Persisted: the marker is cleared and the now-empty key is dropped.
        layer.evict(2);
        assert_eq!(layer.get(&1, &10, 0), None);
    }

    // --- EvictionWatermark ---------------------------------------------------------------------

    #[test]
    fn eviction_watermark_tighten_gradual_and_bounded() {
        let mut wm = EvictionWatermark::default();
        // From a released state, the window halves toward persist each tick, never exceeding it.
        assert_eq!(wm.tighten(100), 50); // window 100 -> 50
        assert_eq!(wm.tighten(100), 75); // window 50 -> 25
        assert_eq!(wm.tighten(100), 88); // window 25 -> 12
        assert!(wm.height <= 100);

        wm.relax();
        assert_eq!(wm.height, 0);
        assert_eq!(wm.tighten(100), 50); // fresh episode starts wide again
    }

    // --- integration through PersistBlockCache -------------------------------------------------

    #[test]
    fn write_trie_updates_account_tombstone() {
        let cache = test_cache();
        let mut update = TrieUpdatesV2::default();
        update.account_nodes.insert(nib([0x3]), leaf(7));
        cache.write_trie_updates(&update, 1);
        assert_eq!(cache.trie_account(&nib([0x3])), Some(Some(leaf(7))));
        assert_eq!(cache.trie_account(&nib([0x4])), None);

        let mut update = TrieUpdatesV2::default();
        update.removed_nodes.insert(nib([0x3]));
        cache.write_trie_updates(&update, 2);
        assert_eq!(cache.trie_account(&nib([0x3])), Some(None));
    }

    #[test]
    fn write_trie_updates_wipe_shadows_root() {
        let cache = test_cache();
        let address = addr(1);

        let mut update = TrieUpdatesV2::default();
        let mut s = StorageTrieUpdatesV2::default();
        s.storage_nodes.insert(Nibbles::new(), leaf(1));
        s.storage_nodes.insert(nib([0x1]), leaf(2));
        update.storage_tries.insert(address, s);
        cache.write_trie_updates(&update, 1);

        // Wipe to empty: the storage root (read directly by `Trie::new`) must resolve to absent,
        // and the destroyed incarnation's node must not leak through.
        let mut update = TrieUpdatesV2::default();
        update.storage_tries.insert(address, StorageTrieUpdatesV2::deleted());
        cache.write_trie_updates(&update, 2);
        assert_eq!(cache.trie_storage(&address, &Nibbles::new()), Some(None));
        assert_eq!(cache.trie_storage(&address, &nib([0x1])), Some(None));
    }

    #[test]
    fn storage_destroy_recreate_shadows_db() {
        let cache = test_cache();
        let a = Address::with_last_byte(1);
        let (x, y) = (U256::from(1), U256::from(2));

        // X=5 persisted + cached at block 1.
        cache.storage.put(a, x, U256::from(5), 1);
        // Block 2: destroy + recreate, only Y rewritten.
        cache.storage.wipe(a, 2);
        cache.storage.put(a, y, U256::from(9), 2);

        // Through the public read path (persist_height defaults to 0): X is absent, not a miss.
        assert_eq!(cache.storage(&a, &x), Some(None));
        assert_eq!(cache.storage(&a, &y), Some(Some(U256::from(9))));
    }

    #[test]
    fn storage_zero_write_to_uncached_account_is_tombstoned() {
        // A zero-write (slot deleted) to an account with no other cached storage must still leave
        // a tombstone, otherwise a read would fall through to a stale non-zero DB value.
        let cache = test_cache();
        let a = Address::with_last_byte(2);
        let x = U256::from(7);
        cache.storage.tombstone(a, x, 1);
        assert_eq!(cache.storage(&a, &x), Some(None));
    }

    #[test]
    fn wipe_layer_cache_read_does_not_clobber() {
        let layer = WipeLayer::<u64, u64, u64>::default();
        layer.put(1, 2, 100, 9);
        layer.cache_read(1, 2, 999, 1); // existing (merged) value must win
        assert_eq!(layer.get(&1, &2, 0), Some(Some(100)));
        // a fresh sub-key can be populated from a DB read
        layer.cache_read(1, 3, 7, 1);
        assert_eq!(layer.get(&1, &3, 0), Some(Some(7)));
    }

    #[test]
    fn eviction_watermark_persist_zero_is_a_noop() {
        // Before anything is persisted, the watermark cannot rise above 0, so nothing is ever
        // evictable (no entry has block <= 0 except genesis).
        let mut wm = EvictionWatermark::default();
        assert_eq!(wm.tighten(0), 0);
        assert_eq!(wm.tighten(0), 0);
        wm.relax();
        assert_eq!(wm.height, 0);
    }

    #[test]
    fn wipe_layer_evict_across_keys() {
        let layer = WipeLayer::<u64, u64, u64>::default();
        // key 1: an old persisted node + a fresh one.
        layer.put(1, 10, 100, 3);
        layer.put(1, 11, 101, 9);
        // key 2: wiped at block 4, no recreated nodes.
        layer.wipe(2, 4);

        // Evict at height 5 (>= both block 3 and the wipe at 4, all persisted by contract).
        layer.evict(5);
        // key 1: stale node (block 3) gone, fresh node (block 9) kept.
        assert_eq!(layer.get(&1, &10, 5), None);
        assert_eq!(layer.get(&1, &11, 5), Some(Some(101)));
        // key 2: marker cleared and the empty sub-tree dropped -> a miss falls through to DB.
        assert_eq!(layer.get(&2, &10, 5), None);
    }

    #[test]
    fn layer_cache_read_populates_when_absent() {
        let layer = Layer::<u64, u64>::default();
        assert_eq!(layer.get(&1), None);
        layer.cache_read(1, 42, 7);
        assert_eq!(layer.get(&1), Some(Some(42)));
    }
}
