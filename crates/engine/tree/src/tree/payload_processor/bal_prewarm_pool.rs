//! Public pool for warming block read sets without enabling a production execution path.

use alloy_primitives::{Address, StorageKey};
use reth_execution_cache::{CachedStateProvider, ExecutionCache};
use reth_provider::{AccountReader, ProviderResult, StateProvider, StateProviderBox};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
};
use tracing::trace;

/// Builds a fresh `StateProviderBox` over the block's parent state. Type-erased so the pool is not
/// generic over the provider factory; each worker builds its own per block.
pub type BuildProviderFn = dyn Fn() -> ProviderResult<StateProviderBox> + Send + Sync;

/// A warm request for an account and a bounded batch of its storage slots.
enum PrewarmTarget {
    Account(Address, Box<[StorageKey]>),
    Storage(Address, Box<[StorageKey]>),
}

/// A message in a worker's queue. The per-block lifecycle is explicit and ordered (the queue is
/// FIFO): one `BeginBlock`, then the worker's share of `Warm`s, then one `EndBlock`.
enum PrewarmMsg {
    /// Open a read txn for the new block: build a provider over the parent state and hold it.
    BeginBlock { build: Arc<BuildProviderFn>, caches: ExecutionCache },
    /// Warm one target into the held provider's cache. Ignored if no provider is held.
    Warm(PrewarmTarget),
    /// Drop the held provider (and its read txn).
    EndBlock(std::sync::mpsc::Sender<()>),
}

/// Long-lived pool of blocking threads that warm the BAL read-set into the shared execution cache.
#[derive(Debug)]
pub struct BalPrewarmPool {
    /// One queue per worker. `BeginBlock`/`EndBlock` are broadcast to all; `Warm`s round-robin.
    workers: Vec<crossbeam_channel::Sender<PrewarmMsg>>,
    /// Round-robin cursor for distributing warm requests across workers.
    next: AtomicUsize,
    _handles: Vec<JoinHandle<()>>,
}

impl BalPrewarmPool {
    /// Spawns `num_threads` long-lived blocking worker threads. Owned by the
    /// [`PayloadProcessor`](super::PayloadProcessor); the threads exit when the pool is dropped.
    pub fn new(num_threads: usize) -> Arc<Self> {
        assert!(num_threads > 0, "at least one prewarm worker is required");
        let mut workers = Vec::with_capacity(num_threads);
        let mut handles = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let (tx, rx) = crossbeam_channel::unbounded::<PrewarmMsg>();
            workers.push(tx);
            handles.push(
                std::thread::Builder::new()
                    .name(format!("bal-prewarm-{i:03}"))
                    .spawn(move || prewarm_loop(rx))
                    .expect("spawn bal-prewarm thread"),
            );
        }
        trace!(target: "engine::tree::bal_prewarm_pool", num_threads, "BalPrewarmPool spawned");
        Arc::new(Self { workers, next: AtomicUsize::new(0), _handles: handles })
    }

    /// Begins a block: hands every worker the provider builder and shared cache so each opens its
    /// own read txn over the parent state. Pair with [`end_block`](Self::end_block).
    pub fn begin_block(&self, build: Arc<BuildProviderFn>, caches: ExecutionCache) {
        for worker in &self.workers {
            let _ = worker
                .send(PrewarmMsg::BeginBlock { build: build.clone(), caches: caches.clone() });
        }
    }

    /// Fire-and-forget: warm an account on some worker. Bytecode is loaded on demand.
    pub fn warm_account(&self, addr: Address) {
        self.warm_account_with_storage(addr, core::iter::empty());
    }

    /// Warms an account and its slots, spreading bounded batches across workers.
    ///
    /// Batching amortizes queue sends without serializing a large account's read set on one
    /// worker. [`end_block`](Self::end_block) waits for every queued batch.
    pub fn warm_account_with_storage(
        &self,
        addr: Address,
        slots: impl IntoIterator<Item = StorageKey>,
    ) {
        let mut slots = slots.into_iter();
        let batch = slots.by_ref().take(WARM_BATCH_SIZE).collect();
        self.send_warm(PrewarmTarget::Account(addr, batch));
        loop {
            let batch: Box<[StorageKey]> = slots.by_ref().take(WARM_BATCH_SIZE).collect();
            if batch.is_empty() {
                break
            }
            self.send_warm(PrewarmTarget::Storage(addr, batch));
        }
    }

    /// Fire-and-forget: warm one storage slot on some worker.
    pub fn warm_storage(&self, addr: Address, slot: StorageKey) {
        self.send_warm(PrewarmTarget::Storage(addr, Box::new([slot])));
    }

    /// Waits until every worker drains queued warm requests and drops its provider/read view.
    pub fn end_block(&self) {
        let (finished, receiver) = std::sync::mpsc::channel();
        for worker in &self.workers {
            let _ = worker.send(PrewarmMsg::EndBlock(finished.clone()));
        }
        drop(finished);
        // Every successfully queued message owns a sender, dropped after acknowledgement.
        while receiver.recv().is_ok() {}
    }

    fn send_warm(&self, target: PrewarmTarget) {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let _ = self.workers[i].send(PrewarmMsg::Warm(target));
    }
}

/// Number of warming threads.
///
/// The work performed on those threads boils down mostly to MDBX reads. An MDBX read consists of
/// a tree traversal and major page faults causing I/O.
///
/// In order to utilize the parallelism of `NVMe` we have to give it enough work, or equally,
/// maintain a high queue depth. Modern `NVMe` devices require in between 64-128 requests in-flight
/// to achieve its peak performance. Ideally we don't grow past that but it's OK to do so, it just
/// means that a request is going to wait in the `NVMe` queue rather than in memory.
///
/// MDBX piggy-backs on the OS page cache for its buffers. Oftentimes, the hit rate reaches 90-99%
/// hit rate. At that point, the workload can be classified as CPU-bound. In that case, having
/// a high number of threads is counterproductive due to the effects of context switching, core
/// migration, contention, etc.
///
/// However, that overhead is considered negligible compared to the benefits of fully utilizing
/// `NVMe` resources. For example, with request latency of 100µs, 100k IO requests the expected
/// time to finish is 312.5ms at QD=32 and 156.26ms at QD=64.
///
/// This should explain why this particular value is picked.
pub const DEFAULT_BAL_PREWARM_THREADS: usize = 128;

/// Number of slots per queue message, bounded to keep work spread across workers.
const WARM_BATCH_SIZE: usize = 8;

fn prewarm_loop(rx: crossbeam_channel::Receiver<PrewarmMsg>) {
    // The provider (and its MDBX read txn) held for the current block, between `BeginBlock` and
    // `EndBlock`. `None` while idle, so no read txn is pinned across the inter-block gap.
    let mut provider: Option<CachedStateProvider<StateProviderBox>> = None;

    // Blocks when idle; the channel disconnects (and the loop ends) when the pool is dropped.
    while let Ok(msg) = rx.recv() {
        match msg {
            PrewarmMsg::BeginBlock { build, caches } => {
                provider = match (build)() {
                    Ok(inner) => Some(CachedStateProvider::new_prewarm(inner, caches)),
                    Err(err) => {
                        trace!(target: "engine::tree::bal_prewarm_pool", %err, "failed to build provider");
                        None
                    }
                };
            }
            PrewarmMsg::Warm(target) => {
                let Some(provider) = provider.as_ref() else { continue };
                match target {
                    PrewarmTarget::Account(addr, slots) => {
                        let _ = provider.basic_account(&addr);
                        for &slot in &slots {
                            let _ = provider.storage(addr, slot);
                        }
                    }
                    PrewarmTarget::Storage(addr, slots) => {
                        for &slot in &slots {
                            let _ = provider.storage(addr, slot);
                        }
                    }
                }
            }
            PrewarmMsg::EndBlock(finished) => {
                provider = None;
                let _ = finished.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_provider::test_utils::MockEthProvider;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn batched_warming_populates_all_slots_without_prefetching_bytecode() {
        use alloy_primitives::{Bytes, U256};
        use reth_execution_cache::CachedStatus;
        use reth_provider::test_utils::ExtendedAccount;
        for count in [0usize, 1, 8, 9, 17] {
            let addr = Address::repeat_byte(1);
            let caches = ExecutionCache::new(1000);
            // Fixed-cache is direct-mapped: collisions may evict a previously warmed slot.
            // Choose a collision-free read set against this cache's actual hash seed, then clear
            // the probe values so every real warm below must read the provider.
            let missing_key = StorageKey::from(U256::from(99));
            let mut keys = vec![missing_key];
            for candidate in 0..65_536u64 {
                if keys.len() == count + 1 {
                    break
                }
                let candidate = StorageKey::from(U256::from(candidate));
                if candidate == missing_key {
                    continue
                }
                caches.clear();
                for &key in keys.iter().chain(core::iter::once(&candidate)) {
                    caches.insert_storage(addr, key, Some(U256::MAX));
                }
                if keys.iter().chain(core::iter::once(&candidate)).all(|&key| {
                    caches.get_or_try_insert_storage_with::<()>(addr, key, || Err(())).is_ok()
                }) {
                    keys.push(candidate);
                }
                if keys.len() == count + 1 {
                    break
                }
            }
            assert_eq!(keys.len(), count + 1, "could not construct a collision-free fixture");
            caches.clear();
            let slots = &keys[1..];
            let account = ExtendedAccount::new(7, U256::from(100))
                .with_bytecode(Bytes::from_static(&[0x00]))
                .extend_storage(slots.iter().enumerate().map(|(i, &key)| (key, U256::from(i + 1))));
            let provider = MockEthProvider::default();
            provider.extend_accounts(vec![(addr, account)]);
            let expected_account = provider.basic_account(&addr).unwrap().unwrap();
            let code_hash = expected_account.bytecode_hash.unwrap();
            // Warming is best-effort under bucket contention. This fixture verifies exact cache
            // contents without competing writers; the separate two-worker test checks draining.
            let pool = BalPrewarmPool::new(1);
            pool.begin_block(Arc::new(move || Ok(Box::new(provider.clone()))), caches.clone());
            pool.warm_account_with_storage(addr, slots.iter().copied());
            // Keep the old public entry points usable alongside the batch API.
            pool.warm_account(addr);
            pool.warm_storage(addr, missing_key);
            pool.end_block();
            assert_eq!(
                caches.get_or_try_insert_account_with::<()>(addr, || Err(())).unwrap(),
                CachedStatus::Cached(Some(expected_account))
            );
            for (i, &key) in slots.iter().enumerate() {
                assert_eq!(
                    caches.get_or_try_insert_storage_with::<()>(addr, key, || Err(())).unwrap(),
                    CachedStatus::Cached(U256::from(i + 1))
                );
            }
            assert_eq!(
                caches.get_or_try_insert_storage_with::<()>(addr, missing_key, || Err(())).unwrap(),
                CachedStatus::Cached(U256::ZERO)
            );
            assert!(
                caches.get_or_try_insert_code_with::<()>(code_hash, || Err(())).is_err(),
                "bytecode must remain demand-loaded"
            );
        }
    }

    #[test]
    fn end_block_waits_for_all_queued_provider_builds() {
        let pool = BalPrewarmPool::new(2);
        let opened = Arc::new(AtomicUsize::new(0));
        let count = opened.clone();
        pool.begin_block(
            Arc::new(move || {
                std::thread::yield_now();
                count.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new(MockEthProvider::default()))
            }),
            ExecutionCache::new(100),
        );
        pool.warm_account(Address::ZERO);
        pool.end_block();
        assert_eq!(opened.load(Ordering::Relaxed), 2);
    }
}
