# GRETH MEDIUM Fixes Design (Phase 3)

Date: 2026-03-05

## GRETH-044: Fire-and-Forget tokio::spawn — Panic Propagation Gap

**Problem:** In `PipeExecService::run()`, the `JoinHandle` returned by `tokio::spawn` at line 269 is discarded. If the spawned `core.process(block)` task panics, the panic is silently swallowed and the block is permanently lost with no error surfaced to the operator.

**Fix:** Store the `JoinHandle` and monitor it for panics. Use a `JoinSet` to track all spawned process tasks. After each loop iteration (or via a select! branch), check for completed tasks and propagate any panics by unwrapping the `JoinHandle` result.

**Reference Code:**
```rust
// before (lib.rs:244-276)
impl<Storage: GravityStorage> PipeExecService<Storage> {
    async fn run(mut self) {
        self.core.init_storage(self.execution_args_rx.await.unwrap());
        loop {
            let start_time = Instant::now();
            let block = match self.ordered_block_rx.recv().await {
                Some(block) => block,
                None => {
                    self.core.execute_block_barrier.close();
                    self.core.merklize_barrier.close();
                    self.core.make_canonical_barrier.close();
                    return;
                }
            };
            // ...
            let core = self.core.clone();
            tokio::spawn(async move {
                let start_time = Instant::now();
                core.process(block).await;
                core.metrics.process_block_duration.record(start_time.elapsed());
            });
        }
    }
}

// after
impl<Storage: GravityStorage> PipeExecService<Storage> {
    async fn run(mut self) {
        self.core.init_storage(self.execution_args_rx.await.unwrap());
        let mut task_set = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                block_opt = self.ordered_block_rx.recv() => {
                    let block = match block_opt {
                        Some(block) => block,
                        None => {
                            self.core.execute_block_barrier.close();
                            self.core.merklize_barrier.close();
                            self.core.make_canonical_barrier.close();
                            // Drain remaining tasks before returning
                            while let Some(result) = task_set.join_next().await {
                                if let Err(e) = result {
                                    if e.is_panic() {
                                        error!(target: "PipeExecService.run",
                                            error=?e,
                                            "process() task panicked during shutdown drain"
                                        );
                                        std::panic::resume_unwind(e.into_panic());
                                    }
                                }
                            }
                            return;
                        }
                    };
                    let elapsed = start_time.elapsed();
                    self.core.metrics.recv_block_time_diff.record(elapsed);
                    // ... logging ...

                    let core = self.core.clone();
                    task_set.spawn(async move {
                        let start_time = Instant::now();
                        core.process(block).await;
                        core.metrics.process_block_duration.record(start_time.elapsed());
                    });
                }
                Some(result) = task_set.join_next() => {
                    if let Err(e) = result {
                        if e.is_panic() {
                            error!(target: "PipeExecService.run",
                                error=?e,
                                "process() task panicked"
                            );
                            // Propagate the panic to crash the node deterministically
                            std::panic::resume_unwind(e.into_panic());
                        }
                    }
                }
            }
        }
    }
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-045: Unbounded Task Accumulation in PipeExecService::run()

**Problem:** `PipeExecService::run()` spawns a new `tokio::spawn` for every ordered block received (line 269) with no concurrency limit. Under sustained high block throughput, this can lead to unbounded task accumulation, memory exhaustion, and out-of-order execution pressure.

**Fix:** Add a concurrency limit using a `tokio::sync::Semaphore` or limit the `JoinSet` size. Since block processing must maintain ordering guarantees via barriers, the concurrency limit acts as backpressure rather than a strict parallelism cap. This fix naturally combines with GRETH-044's JoinSet approach.

**Reference Code:**
```rust
// before (lib.rs:268-273)
let core = self.core.clone();
tokio::spawn(async move {
    let start_time = Instant::now();
    core.process(block).await;
    core.metrics.process_block_duration.record(start_time.elapsed());
});

// after — add semaphore-based concurrency limit
use tokio::sync::Semaphore;

// In PipeExecService or Core, add a field:
// max_concurrent_blocks: Arc<Semaphore>,  // e.g., Semaphore::new(16)

const MAX_CONCURRENT_PROCESS_TASKS: usize = 16;

impl<Storage: GravityStorage> PipeExecService<Storage> {
    async fn run(mut self) {
        self.core.init_storage(self.execution_args_rx.await.unwrap());
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PROCESS_TASKS));
        let mut task_set = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                block_opt = self.ordered_block_rx.recv() => {
                    let block = match block_opt {
                        Some(block) => block,
                        None => { /* shutdown logic */ return; }
                    };
                    // ... logging ...

                    // Acquire permit before spawning — provides backpressure
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let core = self.core.clone();
                    task_set.spawn(async move {
                        let start_time = Instant::now();
                        core.process(block).await;
                        core.metrics.process_block_duration.record(start_time.elapsed());
                        drop(permit); // release concurrency slot
                    });
                }
                Some(result) = task_set.join_next() => {
                    if let Err(e) = result {
                        if e.is_panic() {
                            std::panic::resume_unwind(e.into_panic());
                        }
                    }
                }
            }
        }
    }
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: accepted; comments: Semaphore-based concurrency limit is a clean approach. Combines well with GRETH-044's JoinSet. MAX_CONCURRENT_PROCESS_TASKS = 16 is reasonable given the barrier-based ordering guarantees.

## GRETH-046: Non-Atomic Trie Cache Writes

**Problem:** `write_trie_updates()` in `cache.rs:525-579` performs individual `DashMap` insert/remove operations in parallel (`par_iter`) without transactional semantics. A concurrent reader could observe a partially-applied trie update: some nodes removed but their replacements not yet inserted.

**Fix:** The severity is partially overstated because the pipeline architecture ensures sequential block processing through barriers (merklize_barrier). However, document the invariant and consider grouping related remove-then-insert sequences. At minimum, ensure removes and inserts for the same account trie path are not reordered.

**Reference Code:**
```rust
// before (cache.rs:524-531)
pub fn write_trie_updates(&self, input: &TrieUpdatesV2, block_number: u64) {
    input.removed_nodes.par_iter().for_each(|path| {
        self.account_trie.remove(path);
    });
    input.account_nodes.par_iter().for_each(|(path, node)| {
        self.account_trie.insert(*path, ValueWithTip::new(node.clone().into(), block_number));
    });
    // ...
}

// after — insert before remove to prevent transient "missing node" windows,
// and add a doc comment explaining the safety invariant
/// Write trie updates to the cache.
///
/// # Safety Invariant
///
/// This method is called sequentially per block number, ordered by the
/// `merklize_barrier`. Concurrent reads may observe a partially-applied
/// update within a single block, but the barrier ensures no two blocks'
/// trie updates overlap. The insert-before-remove ordering prevents
/// transient "missing node" windows for paths that are both removed and
/// re-inserted.
pub fn write_trie_updates(&self, input: &TrieUpdatesV2, block_number: u64) {
    // Insert new nodes first so readers never see a gap
    input.account_nodes.par_iter().for_each(|(path, node)| {
        self.account_trie.insert(*path, ValueWithTip::new(node.clone().into(), block_number));
    });
    // Then remove nodes that are no longer needed
    input.removed_nodes.par_iter().for_each(|path| {
        // Only remove if the node wasn't just re-inserted for this block
        if let Some(entry) = self.account_trie.get(path) {
            if entry.tip < block_number {
                drop(entry);
                self.account_trie.remove(path);
            }
        }
    });

    input.storage_tries.par_iter().for_each(|(hashed_address, storage_trie_update)| {
        if storage_trie_update.is_deleted {
            self.storage_trie.remove(hashed_address);
        } else {
            // Insert new storage nodes first
            let storage = self.storage_trie
                .entry(*hashed_address)
                .or_insert_with(DashMap::new);
            for (nibbles, node) in &storage_trie_update.storage_nodes {
                storage.insert(*nibbles, ValueWithTip::new(node.clone().into(), block_number));
            }
            // Then remove stale nodes
            for path in &storage_trie_update.removed_nodes {
                if let Some(entry) = storage.get(path) {
                    if entry.tip < block_number {
                        drop(entry);
                        storage.remove(path);
                    }
                }
            }
            if storage.is_empty() {
                drop(storage);
                self.storage_trie.remove(hashed_address);
            }
        }
    });
}
```

**Files:** `crates/storage/storage-api/src/cache.rs`

## GRETH-049: No Total Supply Cap on Minting

**Problem:** The mint precompile (`mint_precompile.rs:118-132`) allows minting any amount of tokens up to `u128::MAX` per call with no cumulative supply cap. A compromised or buggy JWK Manager system contract could mint unbounded tokens, inflating supply without limit.

**Fix:** Add an on-chain total supply tracking mechanism. Track cumulative minted amount either via a dedicated storage slot in the precompile state or via a constant cap enforced in the precompile logic. The cap should be configurable via the gravity config.

**Reference Code:**
```rust
// before (mint_precompile.rs — no supply tracking)
let amount: u128 = amount_u256.try_into().map_err(|_| { /* ... */ })?;
if amount == 0 {
    return Err(PrecompileError::Other("Zero amount not allowed".into()));
}
// 6. Execute mint operation
let mut state_guard = state.lock();
if let Err(e) = state_guard.increment_balances(HashMap::from([(recipient, amount)])) {
    // ...
}

// after — add per-block and total supply cap enforcement
use std::sync::atomic::{AtomicU128, Ordering};

/// Maximum total tokens that can be minted across all time (configurable)
const MAX_TOTAL_SUPPLY: u128 = 1_000_000_000 * 10u128.pow(18); // 1 billion tokens in wei

/// Maximum tokens that can be minted in a single block
const MAX_PER_BLOCK_MINT: u128 = 10_000_000 * 10u128.pow(18); // 10M tokens in wei

pub fn create_mint_token_precompile<DB: ParallelDatabase + Send + Sync + 'static>(
    state: Arc<Mutex<ParallelState<DB>>>,
    total_minted: Arc<AtomicU128>,       // cumulative tracker, persisted across blocks
    block_minted: Arc<AtomicU128>,       // reset per block
) -> DynPrecompile {
    // ... pass total_minted and block_minted into closure ...
}

fn mint_token_handler<DB: ParallelDatabase + Send + Sync>(
    input: PrecompileInput<'_>,
    state: Arc<Mutex<ParallelState<DB>>>,
    total_minted: Arc<AtomicU128>,
    block_minted: Arc<AtomicU128>,
) -> PrecompileResult {
    // ... existing validation ...

    let amount: u128 = amount_u256.try_into().map_err(|_| { /* ... */ })?;
    if amount == 0 {
        return Err(PrecompileError::Other("Zero amount not allowed".into()));
    }

    // Supply cap checks
    let new_block_total = block_minted.fetch_add(amount, Ordering::SeqCst) + amount;
    if new_block_total > MAX_PER_BLOCK_MINT {
        block_minted.fetch_sub(amount, Ordering::SeqCst); // rollback
        warn!(target: "evm::precompile::mint_token",
            ?recipient, amount, new_block_total,
            "Per-block mint cap exceeded"
        );
        return Err(PrecompileError::Other("Per-block mint cap exceeded".into()));
    }

    let new_total = total_minted.fetch_add(amount, Ordering::SeqCst) + amount;
    if new_total > MAX_TOTAL_SUPPLY {
        total_minted.fetch_sub(amount, Ordering::SeqCst); // rollback
        block_minted.fetch_sub(amount, Ordering::SeqCst); // rollback
        warn!(target: "evm::precompile::mint_token",
            ?recipient, amount, new_total,
            "Total supply cap exceeded"
        );
        return Err(PrecompileError::Other("Total supply cap exceeded".into()));
    }

    // 6. Execute mint operation
    let mut state_guard = state.lock();
    if let Err(e) = state_guard.increment_balances(HashMap::from([(recipient, amount)])) {
        // rollback counters on failure
        total_minted.fetch_sub(amount, Ordering::SeqCst);
        block_minted.fetch_sub(amount, Ordering::SeqCst);
        return Err(PrecompileError::Other("Failed to mint tokens".into()));
    }
    // ...
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs`, `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs` (precompile initialization)

**Review Comments** reviewer: neko; state: pending; comments: @lightman The concept of adding supply caps is sound, but the proposed implementation raises questions: (1) MAX_TOTAL_SUPPLY and MAX_PER_BLOCK_MINT are hardcoded constants — should these be configurable via gravity_config or on-chain governance? (2) The AtomicU128 total_minted tracker is in-memory only and resets on restart — how do we persist and recover the cumulative minted amount? (3) The fetch_add + check + fetch_sub pattern is not truly atomic and could race under concurrent precompile invocations within parallel EVM execution. Please clarify the intended supply cap parameters and persistence strategy.

## GRETH-050: Precompile State Merge Loses Original Storage Values

**Problem:** When merging precompile `BundleState` into `accumulated_state_changes` at line 818-825, storage slots are constructed with `EvmStorageSlot::new(v.present_value, 0)`, discarding the `original_value`. This means if the precompile modifies an existing storage slot, the original value (needed for gas refund calculations and state revert) is lost, replaced with a zero.

**Fix:** Preserve the `original_value` from the precompile's `StorageSlot` when converting to `EvmStorageSlot`. The `StorageSlot` in `BundleState` already carries both `previous_or_original_value` and `present_value`.

**Reference Code:**
```rust
// before (lib.rs:814-832)
for (address, account) in precompile_bundle.state {
    if let Some(info) = account.info {
        use revm::state::{Account, AccountStatus, EvmStorageSlot};
        accumulated_state_changes.insert(
            address,
            Account {
                info,
                storage: account
                    .storage
                    .into_iter()
                    .map(|(k, v)| (k, EvmStorageSlot::new(v.present_value, 0)))
                    .collect(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            },
        );
    }
}

// after — preserve original_value from the bundle's StorageSlot
for (address, account) in precompile_bundle.state {
    if let Some(info) = account.info {
        use revm::state::{Account, AccountStatus, EvmStorageSlot};
        accumulated_state_changes.insert(
            address,
            Account {
                info,
                storage: account
                    .storage
                    .into_iter()
                    .map(|(k, v)| {
                        (k, EvmStorageSlot::new_changed(
                            v.previous_or_original_value,
                            v.present_value,
                        ))
                    })
                    .collect(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            },
        );
    }
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-051: System Transaction State Merge Overwrites via HashMap::insert

**Problem:** At line 766-768, when merging validator transaction state changes into `accumulated_state_changes`, a plain `HashMap::insert` is used. This replaces the entire `Account` entry for a given address, potentially discarding storage slots written by a prior system transaction targeting the same address.

**Fix:** Use deep-merge semantics: when the address already exists in `accumulated_state_changes`, merge the storage maps and update the account info, rather than replacing the entire entry.

**Reference Code:**
```rust
// before (lib.rs:765-768)
// Merge state changes into accumulated changes
for (addr, account) in validator_state_changes {
    accumulated_state_changes.insert(addr, account);
}

// after — deep-merge storage slots instead of overwriting
for (addr, new_account) in validator_state_changes {
    match accumulated_state_changes.entry(addr) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let existing = entry.get_mut();
            // Update account info to latest
            existing.info = new_account.info;
            // Merge status flags
            existing.status |= new_account.status;
            // Deep-merge storage: new slots override existing, but existing
            // slots not touched by this txn are preserved
            for (slot, value) in new_account.storage {
                existing.storage.insert(slot, value);
            }
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(new_account);
        }
    }
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: rejected; comments: Same root cause as GRETH-026 (shallow `HashMap::insert` overwrites instead of deep-merging). GRETH-026 covers the precompile→accumulated merge at L823-840; this covers the validator_txn→accumulated merge at L766-768. Rejected on the same rationale as lightman's rejection of GRETH-026: overlapping addresses between different validator system txns won't occur in practice.

## GRETH-052: Block Hash Verification Bypassed with None

**Problem:** In `verify_executed_block_hash()` at line 533-535, when `block_hash` is `None`, the `assert_eq!` verification is skipped entirely with only a warning log. For post-genesis blocks, this means consensus can silently skip block hash verification, weakening the integrity guarantee that execution and consensus agree on block contents.

**Fix:** Add a guard that requires verification hash for all post-genesis blocks. Only allow `None` for the genesis block (block_number == 0) or during initial sync where consensus verification is not yet available.

**Reference Code:**
```rust
// before (lib.rs:526-547)
async fn verify_executed_block_hash(&self, execution_result: ExecutionResult) -> Option<()> {
    let start_time = Instant::now();
    let block_id = execution_result.block_id;
    let block_number = execution_result.block_number;
    let executed_block_hash = execution_result.block_hash;
    self.execution_result_tx.send(execution_result).ok()?;
    let block_hash = self.verified_block_hash_rx.wait(block_id).await?;
    match block_hash {
        Some(verified_hash) => {
            assert_eq!(executed_block_hash, verified_hash, "Block hash mismatch");
        }
        None => {
            warn!(
                target: "PipeExecService.process",
                block_number = ?block_number,
                block_id = ?block_id,
                block_hash = ?executed_block_hash,
                "consensus did not provide a verification hash for this block"
            );
        }
    }
    // ...
}

// after — require verification hash for post-genesis blocks
async fn verify_executed_block_hash(&self, execution_result: ExecutionResult) -> Option<()> {
    let start_time = Instant::now();
    let block_id = execution_result.block_id;
    let block_number = execution_result.block_number;
    let executed_block_hash = execution_result.block_hash;
    self.execution_result_tx.send(execution_result).ok()?;
    let block_hash = self.verified_block_hash_rx.wait(block_id).await?;
    match block_hash {
        Some(verified_hash) => {
            if executed_block_hash != verified_hash {
                error!(
                    target: "PipeExecService.process",
                    block_number = ?block_number,
                    block_id = ?block_id,
                    executed_hash = ?executed_block_hash,
                    verified_hash = ?verified_hash,
                    "CRITICAL: Block hash mismatch between execution and consensus"
                );
                // See GRETH-053: return error instead of panic
                panic!(
                    "Block hash mismatch at block {}: executed={:?}, verified={:?}",
                    block_number, executed_block_hash, verified_hash
                );
            }
        }
        None => {
            if block_number > 1 {
                error!(
                    target: "PipeExecService.process",
                    block_number = ?block_number,
                    block_id = ?block_id,
                    block_hash = ?executed_block_hash,
                    "CRITICAL: Consensus did not provide verification hash for post-genesis block"
                );
                // For safety, treat missing verification as a critical error
                // This prevents silent bypass of hash verification
                panic!(
                    "Missing consensus verification hash for block {}",
                    block_number
                );
            } else {
                info!(
                    target: "PipeExecService.process",
                    block_number = ?block_number,
                    "Skipping hash verification for genesis/initial block"
                );
            }
        }
    }
    // ...
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-053: Block Hash Mismatch Causes Deliberate Node Panic

**Problem:** At line 535, `assert_eq!(executed_block_hash, verified_hash, "Block hash mismatch")` causes an immediate, unstructured panic on hash mismatch. This does not log diagnostics (block number, both hashes) before crashing, makes it impossible for monitoring to catch the event, and bypasses any graceful shutdown logic.

**Fix:** Replace `assert_eq!` with structured error logging followed by a graceful shutdown trigger. The node should log full diagnostic context, flush metrics, and then terminate.

**Reference Code:**
```rust
// before (lib.rs:534-535)
Some(verified_hash) => {
    assert_eq!(executed_block_hash, verified_hash, "Block hash mismatch");
}

// after — structured error + graceful shutdown
Some(verified_hash) => {
    if executed_block_hash != verified_hash {
        error!(
            target: "PipeExecService.process",
            block_number = block_number,
            block_id = ?block_id,
            executed_hash = ?executed_block_hash,
            consensus_hash = ?verified_hash,
            "CRITICAL: Block hash mismatch detected — \
             execution layer produced a different block than consensus expects. \
             This indicates a determinism bug or state corruption. \
             Node will shut down."
        );

        // Give metrics/logs time to flush
        // In production, trigger graceful shutdown via a shutdown channel
        // rather than panic. For now, use a descriptive panic after logging.
        panic!(
            "Block hash mismatch at block {block_number}: \
             executed={executed_block_hash:?}, consensus={verified_hash:?}. \
             See logs for details."
        );
    }
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: accepted; comments:

## GRETH-054: wait_for_block_persistence Blocks Indefinitely

**Problem:** `wait_for_block_persistence()` at line 1355-1364 uses `rx.await.ok()` on a oneshot channel with no timeout. If the persistence subsystem hangs or the sender is dropped without sending (due to a bug), the caller blocks indefinitely. This can stall consensus progress.

**Fix:** Add a timeout using `tokio::time::timeout`. Return `None` on timeout to signal the failure to the caller.

**Reference Code:**
```rust
// before (lib.rs:1355-1364)
pub async fn wait_for_block_persistence(&self, block_number: u64) -> Option<()> {
    let (tx, rx) = oneshot::channel();
    self.event_tx
        .send(PipeExecLayerEvent::WaitForPersistence(WaitForPersistenceEvent {
            block_number,
            tx,
        }))
        .ok()?;
    rx.await.ok()
}

// after — add timeout
pub async fn wait_for_block_persistence(&self, block_number: u64) -> Option<()> {
    use std::time::Duration;
    use tokio::time::timeout;

    /// Maximum time to wait for a block to be persisted before giving up.
    /// If persistence takes longer than this, something is seriously wrong.
    const PERSISTENCE_TIMEOUT: Duration = Duration::from_secs(120);

    let (tx, rx) = oneshot::channel();
    self.event_tx
        .send(PipeExecLayerEvent::WaitForPersistence(WaitForPersistenceEvent {
            block_number,
            tx,
        }))
        .ok()?;

    match timeout(PERSISTENCE_TIMEOUT, rx).await {
        Ok(Ok(())) => Some(()),
        Ok(Err(_recv_err)) => {
            error!(
                target: "PipeExecLayerApi",
                block_number = block_number,
                "Persistence sender dropped without confirming block"
            );
            None
        }
        Err(_timeout) => {
            error!(
                target: "PipeExecLayerApi",
                block_number = block_number,
                timeout_secs = PERSISTENCE_TIMEOUT.as_secs(),
                "Timed out waiting for block persistence"
            );
            None
        }
    }
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-057: Atomic Ordering Gap Between Epoch and Execute Height

**Problem:** Two independent `AtomicU64` fields (`epoch` at line 238 and `execute_height` at line 239) are updated at lines 455-457 with `Ordering::Release`, but there is no cross-variable ordering guarantee. A concurrent reader using `Ordering::Acquire` could observe the new `execute_height` but the old `epoch`, creating an inconsistent view of which epoch a given block height belongs to.

**Fix:** Either (a) use `SeqCst` ordering for both atomics to enforce a total order, or (b) combine both values into a single `AtomicU128` (epoch in high 64 bits, height in low 64 bits) so they are updated atomically together.

**Reference Code:**
```rust
// before (lib.rs:238-239, 455-457)
// Field declarations:
epoch: AtomicU64,
execute_height: AtomicU64,

// Updates:
assert_eq!(self.epoch.fetch_max(epoch, Ordering::Release), block_epoch);
assert_eq!(self.execute_height.fetch_add(1, Ordering::Release), block_number - 1);

// after — Option A: combined AtomicU128
use std::sync::atomic::AtomicU128;

// Field declaration:
epoch_and_height: AtomicU128,  // high 64 bits = epoch, low 64 bits = execute_height

// Helper methods:
fn pack_epoch_height(epoch: u64, height: u64) -> u128 {
    ((epoch as u128) << 64) | (height as u128)
}

fn unpack_epoch_height(packed: u128) -> (u64, u64) {
    ((packed >> 64) as u64, packed as u64)
}

// Update (atomic CAS loop):
loop {
    let current = self.epoch_and_height.load(Ordering::Acquire);
    let (current_epoch, current_height) = unpack_epoch_height(current);
    assert_eq!(current_height, block_number - 1);
    let new_epoch = current_epoch.max(epoch);
    let new_packed = pack_epoch_height(new_epoch, block_number);
    match self.epoch_and_height.compare_exchange(
        current, new_packed, Ordering::Release, Ordering::Acquire,
    ) {
        Ok(_) => break,
        Err(_) => continue, // retry on contention
    }
}

// after — Option B (simpler): use SeqCst ordering
assert_eq!(self.epoch.fetch_max(epoch, Ordering::SeqCst), block_epoch);
assert_eq!(self.execute_height.fetch_add(1, Ordering::SeqCst), block_number - 1);
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: rejected; comments: The original code does not have a cross-variable ordering problem. The `execute_block_barrier` Channel (backed by `Mutex<Inner>`) enforces strict serial execution: block N's `process()` cannot proceed past `wait((epoch, N-1))` until block N-1 has completed and called `notify((epoch, N-1), ...)`. Since the `epoch` and `execute_height` stores at L492-495 sit inside this serialized critical section (between `wait` returning and `notify` being called), there is only ever a single writer — no concurrent writer can interleave. The only concurrent readers are in the timeout branch at L411 (`self.epoch()` / `self.execute_height()`), which merely check for stale/duplicate blocks; a slightly stale read there causes a harmless retry of the wait loop, not an inconsistent state. The Atomics are purely an optimization to avoid wrapping these two scalars in their own Mutex; a Mutex would itself only provide acquire-release semantics, identical to the original `Ordering::Release` stores + `Ordering::Acquire` loads. Upgrading to `SeqCst` is harmless but unnecessary — it does not fix a real bug.

## GRETH-058: No TLS Certificate Pinning for RPC Connections

**Problem:** `EthHttpCli::new()` at `eth_client.rs:58` uses `ClientBuilder::new().no_proxy().use_rustls_tls()` without any certificate pinning. The HTTP client trusts the system CA store, meaning a compromised CA could MITM the connection between the relayer and external RPC endpoints, injecting forged oracle data.

**Fix:** Add certificate pinning or, at minimum, document the risk. For production deployments, support configuring custom CA certificates or certificate fingerprint pinning. Consider adding an optional `tls_ca_cert` configuration parameter.

**Reference Code:**
```rust
// before (eth_client.rs:52-59)
pub fn new(rpc_url: &str) -> Result<Self> {
    debug!("Creating EthHttpCli for URL: {}", rpc_url);
    let url =
        Url::parse(rpc_url).with_context(|| format!("Failed to parse RPC URL: {}", rpc_url))?;
    let client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();
    let client = client_builder.build().with_context(|| "Failed to build HTTP client")?;
    // ...
}

// after — support optional custom CA certificate for pinning
use reqwest::tls::Certificate;
use std::path::Path;

pub fn new(rpc_url: &str) -> Result<Self> {
    Self::new_with_tls_config(rpc_url, None)
}

/// Create a new EthHttpCli with optional TLS certificate pinning.
///
/// # Arguments
/// * `rpc_url` - The RPC endpoint URL
/// * `ca_cert_path` - Optional path to a PEM-encoded CA certificate for pinning.
///   When provided, ONLY this CA will be trusted (system CAs are excluded).
pub fn new_with_tls_config(
    rpc_url: &str,
    ca_cert_path: Option<&Path>,
) -> Result<Self> {
    debug!("Creating EthHttpCli for URL: {}", rpc_url);
    let url =
        Url::parse(rpc_url).with_context(|| format!("Failed to parse RPC URL: {}", rpc_url))?;

    let mut client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();

    if let Some(cert_path) = ca_cert_path {
        let cert_pem = std::fs::read(cert_path)
            .with_context(|| format!("Failed to read CA certificate: {:?}", cert_path))?;
        let cert = Certificate::from_pem(&cert_pem)
            .with_context(|| "Failed to parse CA certificate PEM")?;
        // Pin to this specific CA — disable system CA store
        client_builder = client_builder
            .tls_built_in_root_certs(false)
            .add_root_certificate(cert);
        info!("TLS certificate pinned to: {:?}", cert_path);
    } else {
        warn!(
            target: "EthHttpCli",
            rpc_url = rpc_url,
            "No TLS certificate pinning configured — using system CA store. \
             Consider configuring certificate pinning for production deployments."
        );
    }

    let client = client_builder.build().with_context(|| "Failed to build HTTP client")?;
    let provider: RootProvider<Ethereum> =
        ProviderBuilder::default().connect_reqwest(client, url.clone());
    Ok(Self { provider, retry_config: RetryConfig::default() })
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/eth_client.rs`

## GRETH-059: is_unsupported_jwk() Uses Type Name Parsing

**Problem:** `is_unsupported_jwk()` at `jwk_oracle.rs:62-66` determines whether a JWK is "unsupported" by checking if `jwk.type_name` parses as a `u32`. This is brittle — any numeric type name (even a legitimate one) would be misidentified, and the approach relies on a naming convention rather than a type system invariant.

**Fix:** Use an explicit type name string match (e.g., `"0x1::jwks::UNSUPPORTED_JWK"`) or an enum discriminant. The existing TODO comment at line 64 already flags this.

**Reference Code:**
```rust
// before (jwk_oracle.rs:60-66)
/// Check if a JWKStruct is an UnsupportedJWK (blockchain/other oracle data)
/// Checks for sourceType string (0, 1, 2, etc.) instead of fixed type_name
fn is_unsupported_jwk(jwk: &JWKStruct) -> bool {
    // Check if type_name is a numeric string (sourceType)
    // TODO(gravity): check if it should be "0x1::jwks::UNSUPPORTED_JWK"
    jwk.type_name.parse::<u32>().is_ok()
}

// after — use explicit type name constant or allowlist
/// Known type name for unsupported/oracle JWK entries
const UNSUPPORTED_JWK_TYPE_NAME: &str = "0x1::jwks::UnsupportedJWK";

/// Known source type identifiers used for blockchain oracle data.
/// These are numeric strings representing the oracle source type.
const KNOWN_SOURCE_TYPES: &[&str] = &["0", "1", "2"];

/// Check if a JWKStruct is an UnsupportedJWK (blockchain/other oracle data).
///
/// Matches either the canonical Move type name or known numeric source type
/// identifiers used by the oracle system.
fn is_unsupported_jwk(jwk: &JWKStruct) -> bool {
    jwk.type_name == UNSUPPORTED_JWK_TYPE_NAME
        || KNOWN_SOURCE_TYPES.contains(&jwk.type_name.as_str())
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/jwk_oracle.rs`

**Review Comments** reviewer: neko; state: pending; comments: @AlexYue

## GRETH-060: expect()/unwrap() on ABI Decode in Config Fetchers

**Problem:** Multiple on-chain config fetchers use `expect()` on ABI decode results: `epoch.rs:65`, `dkg.rs:134`, `validator_set.rs:54-55,72-73,92-93`. If the on-chain contract returns malformed data (due to contract upgrade, misconfiguration, or RPC corruption), the node panics instead of gracefully handling the error.

**Fix:** Replace `expect()` with `?` error propagation. Change the `fetch()` return type to propagate decode errors, or map decode failures to `None` with a warning log (consistent with the existing pattern of returning `Option<Bytes>`).

**Reference Code:**
```rust
// before — epoch.rs:64-65
let epoch = Reconfiguration::currentEpochCall::abi_decode_returns(&result)
    .expect("Failed to decode currentEpoch return value");

// after — epoch.rs
let epoch = match Reconfiguration::currentEpochCall::abi_decode_returns(&result) {
    Ok(epoch) => epoch,
    Err(e) => {
        tracing::error!(
            "Failed to decode currentEpoch return value at block {}: {:?}",
            block_id, e
        );
        return None;
    }
};

// before — dkg.rs:134-135
let solidity_dkg_state = getDKGStateCall::abi_decode_returns(&result)
    .expect("Failed to decode getDKGState return value");

// after — dkg.rs
let solidity_dkg_state = match getDKGStateCall::abi_decode_returns(&result) {
    Ok(state) => state,
    Err(e) => {
        tracing::error!(
            "Failed to decode getDKGState return value at block {}: {:?}",
            block_id, e
        );
        return None;
    }
};

// before — validator_set.rs:54-55 (repeated for 3 calls)
getActiveValidatorsCall::abi_decode_returns(&result)
    .expect("Failed to decode getActiveValidators return value")

// after — validator_set.rs (apply same pattern to all 3 calls)
let active_validators = match getActiveValidatorsCall::abi_decode_returns(&result) {
    Ok(v) => v,
    Err(e) => {
        tracing::error!(
            "Failed to decode getActiveValidators at block {}: {:?}",
            block_id, e
        );
        return None;
    }
};

let pending_active = match getPendingActiveValidatorsCall::abi_decode_returns(&result) {
    Ok(v) => v,
    Err(e) => {
        tracing::error!(
            "Failed to decode getPendingActiveValidators at block {}: {:?}",
            block_id, e
        );
        return None;
    }
};

let pending_inactive = match getPendingInactiveValidatorsCall::abi_decode_returns(&result) {
    Ok(v) => v,
    Err(e) => {
        tracing::error!(
            "Failed to decode getPendingInactiveValidators at block {}: {:?}",
            block_id, e
        );
        return None;
    }
};
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/epoch.rs`
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/dkg.rs`
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/validator_set.rs`

## GRETH-062: Relaxed Memory Ordering on Cursor Atomics

**Problem:** The `cursor` field in `BlockchainEventSource` uses `Ordering::Relaxed` for all loads (line 157) and stores (line 206). Relaxed ordering provides no happens-before guarantee across threads. If one thread updates the cursor and another reads it, the reader may see a stale value, potentially causing duplicate event processing or skipped blocks.

**Fix:** Use `Acquire` ordering for loads and `Release` ordering for stores. This ensures that when a reader observes the updated cursor value, it also sees all memory writes that happened before the store.

**Reference Code:**
```rust
// before (blockchain_source.rs:156-157)
pub fn cursor(&self) -> u64 {
    self.cursor.load(Ordering::Relaxed)
}

// before (blockchain_source.rs:205-207)
pub fn set_cursor(&self, block: u64) {
    self.cursor.store(block, Ordering::Relaxed);
}

// after
pub fn cursor(&self) -> u64 {
    self.cursor.load(Ordering::Acquire)
}

pub fn set_cursor(&self, block: u64) {
    self.cursor.store(block, Ordering::Release);
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs`

## GRETH-063: fromBlock Defaults to 0 When Parameter Missing

**Problem:** `from_block()` in `uri_parser.rs:52-54` returns `0` when the `fromBlock` parameter is missing from the oracle URI. This causes the relayer to scan events starting from block 0 on the source chain, which is extremely expensive for mature chains (millions of blocks) and could overwhelm the RPC endpoint.

**Fix:** Either require `fromBlock` as a mandatory parameter (return `Result` with error), or default to `latest` (current block number at startup). For oracle data sources, scanning from block 0 is almost never the correct behavior.

**Reference Code:**
```rust
// before (uri_parser.rs:51-54)
/// Get fromBlock parameter
pub fn from_block(&self) -> u64 {
    self.params.get("fromBlock").and_then(|s| s.parse().ok()).unwrap_or(0)
}

// after — make fromBlock required, return Result
/// Get fromBlock parameter.
///
/// Returns an error if `fromBlock` is not specified or is not a valid u64.
/// Defaulting to 0 is dangerous as it scans the entire chain history.
pub fn from_block(&self) -> Result<u64> {
    let value_str = self
        .params
        .get("fromBlock")
        .ok_or_else(|| anyhow!(
            "Missing required 'fromBlock' parameter in oracle URI"
        ))?;
    value_str
        .parse::<u64>()
        .map_err(|e| anyhow!("Invalid 'fromBlock' value '{}': {}", value_str, e))
}
```

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/uri_parser.rs` and all call sites of `from_block()`

**Review Comments** reviewer: neko; state: accepted; comments: Fix is correct — making `fromBlock` a required parameter avoids the dangerous default-to-0 scan. Simplified the error message to a concise `"Missing required 'fromBlock' parameter in oracle URI"` (applied to both source code and reference code above).

## GRETH-064: Voting Power Conversion Precision Loss

**Problem:** Voting power is converted from wei (U256) to ether by dividing by 10^18, then truncated to `u64`. At `dkg.rs:265-268`, the conversion uses `(votingPower / 10^18).try_into().unwrap_or(u64::MAX)`. In `types.rs:98,105`, `wei_to_ether()` does the same division. Validators with less than 1 ETH of voting power get rounded to 0, effectively losing all voting rights. The `unwrap_or(u64::MAX)` fallback also means extremely large stakes silently clip to max.

**Fix:** Use checked conversion with a minimum threshold. Validators with sub-1-ETH stake should either be rejected or given a minimum voting power of 1. Log a warning when clipping occurs.

**Reference Code:**
```rust
// before — dkg.rs:264-268
// Convert wei to tokens by dividing by 10^18
voting_power: (validator.votingPower /
    alloy_primitives::U256::from(10).pow(alloy_primitives::U256::from(18)))
.try_into()
.unwrap_or(u64::MAX),

// after — dkg.rs
voting_power: {
    let wei = validator.votingPower;
    let ether = wei / alloy_primitives::U256::from(10).pow(alloy_primitives::U256::from(18));
    let power: u64 = ether.try_into().unwrap_or_else(|_| {
        tracing::warn!(
            validator = ?validator.validator,
            voting_power_wei = ?wei,
            "Voting power exceeds u64::MAX after conversion, clamping"
        );
        u64::MAX
    });
    if power == 0 && !wei.is_zero() {
        tracing::warn!(
            validator = ?validator.validator,
            voting_power_wei = ?wei,
            "Voting power rounds to 0 after wei-to-ether conversion, \
             setting minimum power of 1"
        );
        1u64  // minimum voting power to prevent disenfranchisement
    } else {
        power
    }
},

// before — types.rs:14-16
fn wei_to_ether(wei: U256) -> U256 {
    wei / WEI_PER_ETHER
}

// types.rs:98
let power_ether = wei_to_ether(info.votingPower);

// types.rs:105
power_ether.to::<u64>(),

// after — types.rs
/// Convert voting power from wei to ether, with minimum threshold.
///
/// Returns at least 1 if the input is non-zero, to prevent validators
/// with sub-1-ETH stake from being silently disenfranchised.
fn wei_to_ether_voting_power(wei: U256) -> u64 {
    let ether = wei / WEI_PER_ETHER;
    let power: u64 = ether.try_into().unwrap_or_else(|_| {
        tracing::warn!(
            voting_power_wei = ?wei,
            "Voting power exceeds u64::MAX, clamping"
        );
        u64::MAX
    });
    if power == 0 && !wei.is_zero() {
        tracing::warn!(
            voting_power_wei = ?wei,
            "Voting power rounds to 0 after conversion, using minimum of 1"
        );
        1u64
    } else {
        power
    }
}

// Usage in convert_validator_consensus_info:
let power = wei_to_ether_voting_power(info.votingPower);
// ...
GravityValidatorInfo::new(
    account_address,
    power,
    // ...
)
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/dkg.rs`
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/types.rs`
