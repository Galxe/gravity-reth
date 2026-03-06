# Phase 3 Security Audit — CRITICAL & HIGH Findings Fix Design

**Date:** 2026-03-05
**Scope:** gravity-reth (greth) Phase 3 audit, CRITICAL and HIGH severity findings
**Findings:** 2 CRITICAL, 13 HIGH

---

## GRETH-029: Pipeline Permanent Deadlock via Barrier Timeout Gap

**Problem:** The pipeline uses 4 barriers (`execute_block_barrier`, `merklize_barrier`, `seal_barrier`, `make_canonical_barrier`), but only `execute_block_barrier` uses `wait_timeout(2s)`. The other 3 barriers use `.wait()` which blocks indefinitely. If `process()` panics inside a `tokio::spawn` task, downstream barriers never receive their notify signal and the entire pipeline deadlocks permanently.

**Fix:** (1) Add `wait_timeout` with reasonable deadlines to all barrier `.wait()` calls, with retry loops and stall detection. (2) Capture the `JoinHandle` from `tokio::spawn` and monitor it for panics — if a task panics, close all barriers to unblock the pipeline and log the error.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/lib.rs ===

// BEFORE (lines ~472, 492, 1115): bare .wait() with no timeout
self.merklize_barrier.wait(block_number - 1).await.unwrap();
// ...
let parent_hash = self.seal_barrier.wait(block_number - 1).await.unwrap();
// ...
let prev_finish_commit_time =
    self.make_canonical_barrier.wait(block_number - 1).await.unwrap();

// AFTER: Use wait_timeout with stall detection on all barriers
const BARRIER_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BARRIER_RETRIES: u32 = 60; // 5 minutes total before giving up

// Helper: wait on a barrier with timeout and stall detection
async fn wait_barrier_with_timeout<K, V>(
    barrier: &Channel<K, V>,
    key: K,
    barrier_name: &str,
    block_number: u64,
) -> Result<V, PipelineError>
where
    K: Eq + Clone + std::fmt::Debug + std::hash::Hash,
{
    for attempt in 0..MAX_BARRIER_RETRIES {
        match barrier.wait_timeout(key.clone(), BARRIER_TIMEOUT).await {
            Some(v) => return Ok(v),
            None => {
                warn!(
                    target: "PipeExecService.process",
                    block_number,
                    barrier_name,
                    attempt,
                    "barrier timeout — retrying"
                );
            }
        }
    }
    error!(
        target: "PipeExecService.process",
        block_number,
        barrier_name,
        "barrier stalled after {} retries — aborting pipeline",
        MAX_BARRIER_RETRIES,
    );
    Err(PipelineError::BarrierStall(barrier_name.to_string()))
}

// Usage in process():
let () = wait_barrier_with_timeout(
    &self.merklize_barrier, block_number - 1, "merklize", block_number
).await?;

let parent_hash = wait_barrier_with_timeout(
    &self.seal_barrier, block_number - 1, "seal", block_number
).await?;

let prev_finish_commit_time = wait_barrier_with_timeout(
    &self.make_canonical_barrier, block_number - 1, "make_canonical", block_number
).await?;
```

```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/lib.rs ===
// In PipeExecService::run() — monitor JoinHandle for panics

// BEFORE (line ~269):
let core = self.core.clone();
tokio::spawn(async move {
    let start_time = Instant::now();
    core.process(block).await;
    core.metrics.process_block_duration.record(start_time.elapsed());
});

// AFTER: Track JoinHandle, detect panics, close barriers on failure
let core = self.core.clone();
let handle = tokio::spawn(async move {
    let start_time = Instant::now();
    core.process(block).await;
    core.metrics.process_block_duration.record(start_time.elapsed());
});

// Spawn a lightweight watcher that detects panics
let core_ref = self.core.clone();
tokio::spawn(async move {
    if let Err(join_err) = handle.await {
        error!(
            target: "PipeExecService.run",
            error = %join_err,
            "process() task panicked — closing all barriers to prevent deadlock"
        );
        core_ref.execute_block_barrier.close();
        core_ref.merklize_barrier.close();
        core_ref.seal_barrier.close();
        core_ref.make_canonical_barrier.close();
    }
});
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs` (lines ~269, 472, 492, 1115)
- `crates/pipe-exec-layer-ext-v2/execute/src/channel.rs` (no changes needed; `wait_timeout` already exists)

**Review Comments** reviewer: neko; state: rejected; comments: The problem statement is invalid. greth is used as a crate by gravity_node, which calls `gaptos::aptos_crash_handler::setup_panic_handler()` at startup — any panic (including inside a `tokio::spawn` task) triggers process exit, so the "task panics → downstream barriers never notified → permanent deadlock" scenario cannot occur. Furthermore, the proposed `wait_timeout` + retry loop + early-return fix introduces logic errors: if a barrier times out and `process()` returns early, it never calls `notify(block_number, ...)` on downstream barriers (merklize, seal, make_canonical), causing all subsequent blocks to cascade-timeout and silently stop processing — a worse failure mode than the original code.

---

## GRETH-030: Cache Eviction of Unpersisted Trie Nodes

**Problem:** The cache eviction daemon can compute an `eviction_height` that exceeds `persist_height` (via the formula `(persist_height + last_state_eviction_height) / 2`). When `last_state_eviction_height > persist_height`, the midpoint exceeds `persist_height`, causing trie nodes that have not yet been persisted to disk to be evicted from the cache. Subsequent merklization reads will miss the cache and fall back to stale on-disk data, producing incorrect state roots.

**Fix:** Cap `eviction_height` at `persist_height` in both the contract eviction and the state/trie eviction paths. This ensures that only data already persisted to disk can be evicted from the cache.

**Reference Code:**
```rust
// === File: crates/storage/storage-api/src/cache.rs ===

// BEFORE (lines ~200-225): eviction_height can exceed persist_height
let eviction_height = if last_state_eviction_height == 0 {
    persist_height.saturating_sub(512).max(persist_height / 2)
} else {
    (persist_height + last_state_eviction_height) / 2
};

// AFTER: Cap eviction_height at persist_height
// Contract eviction (line ~200):
let eviction_height = if last_contract_eviction_height == 0 {
    persist_height.saturating_sub(512).max(persist_height / 2)
} else {
    (persist_height + last_contract_eviction_height) / 2
};
// CRITICAL FIX: Never evict above persist_height
let eviction_height = eviction_height.min(persist_height);
inner.contracts.retain(|_, v| v.block_number > eviction_height);
last_contract_eviction_height = eviction_height;

// State/trie eviction (line ~210):
let eviction_height = if last_state_eviction_height == 0 {
    persist_height.saturating_sub(512).max(persist_height / 2)
} else {
    (persist_height + last_state_eviction_height) / 2
};
// CRITICAL FIX: Never evict above persist_height
let eviction_height = eviction_height.min(persist_height);
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
last_state_eviction_height = eviction_height;
```

**Files:**
- `crates/storage/storage-api/src/cache.rs` (lines ~200-230)

---

## GRETH-031: BLOCKHASH Opcode Unimplemented — Validator DoS

**Problem:** Both `RawBlockViewProvider::block_hash_ref()` and `BlockViewProvider::block_hash_ref()` call `unimplemented!()`, which panics at runtime. Any smart contract using the `BLOCKHASH` opcode will crash the validator node, enabling a trivial denial-of-service attack.

**Fix:** Implement `block_hash_ref()` using the existing `block_number_to_id` BTreeMap (which already maintains a ring buffer of the last 256 block hashes via `update_canonical()`). For `RawBlockViewProvider`, look up the canonical block hash from the DB transaction. For `BlockViewProvider`, use the DB fallback.

**Reference Code:**
```rust
// === File: crates/gravity-storage/src/block_view_storage/mod.rs ===

// BEFORE (line 150-152): panics at runtime
fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
    unimplemented!("not support block_hash_ref in BlockViewProvider")
}

// AFTER: Look up block hash from the HeaderByNumber table in the DB
// For RawBlockViewProvider<Tx: DbTx>:
fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
    // Look up the canonical block header by number to retrieve the block hash.
    // The Ethereum BLOCKHASH opcode only looks back 256 blocks, so only
    // recent blocks will be queried here.
    match self.tx.get_by_encoded_key::<tables::CanonicalHeaders>(&number)? {
        Some(hash) => Ok(hash),
        None => Ok(B256::ZERO),
    }
}

// For BlockViewProvider (wraps StateProviderDatabase):
fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
    self.db.block_hash_ref(number)
}
```

**Files:**
- `crates/gravity-storage/src/block_view_storage/mod.rs` (lines 150-152, 210-212)

---

## GRETH-032: Token Loss During Epoch Transitions

**Problem:** When `execute_system_transactions()` returns early due to an epoch change (triggered by either the metadata txn at line 694 or a DKG txn at line 772), the `state_for_precompile` (containing any mint operations performed by the precompile during execution) is dropped without being merged into the returned `BundleState`. Any native token mints that occurred before the epoch-change signal are silently lost.

**Fix:** Extract and merge the precompile state into `accumulated_state_changes` before every early-return path. Refactor the precompile state extraction into a helper function and call it before constructing the `EpochChanged` result.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/lib.rs ===

// Add a helper function to extract precompile state into accumulated changes:
fn merge_precompile_state(
    state_for_precompile: &Arc<parking_lot::Mutex<ParallelState>>,
    accumulated_state_changes: &mut EvmState,
) {
    let mut precompile_state = state_for_precompile.lock();
    precompile_state.merge_transitions(BundleRetention::Reverts);
    let precompile_bundle = precompile_state.take_bundle();

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
}

// BEFORE (line ~694-713): epoch change from metadata txn — precompile state dropped
if let Some((new_epoch, validators)) = metadata_txn_result.emit_new_epoch() {
    drop(evm);
    assert_eq!(new_epoch, epoch + 1);
    // ... log ...
    inner_state.merge_transitions(BundleRetention::Reverts);
    return SystemTxnExecutionOutcome::EpochChanged(
        metadata_txn_result.into_executed_ordered_block_result(
            chain_spec, ordered_block, base_fee,
            inner_state.take_bundle(), validators,
        ),
    );
}

// AFTER: merge precompile state before returning
if let Some((new_epoch, validators)) = metadata_txn_result.emit_new_epoch() {
    drop(evm);
    assert_eq!(new_epoch, epoch + 1);
    // ... log ...

    // FIX: Merge any precompile state (e.g., native mints) before returning
    merge_precompile_state(&state_for_precompile_ref, &mut accumulated_state_changes);

    inner_state.merge_transitions(BundleRetention::Reverts);
    let mut bundle = inner_state.take_bundle();
    // Apply precompile changes to the bundle
    bundle.apply_transitions_and_extend_reverts(accumulated_state_changes);
    return SystemTxnExecutionOutcome::EpochChanged(
        metadata_txn_result.into_executed_ordered_block_result(
            chain_spec, ordered_block, base_fee,
            bundle, validators,
        ),
    );
}

// Same fix for DKG epoch change path (line ~772-791):
if is_dkg {
    if let Some((new_epoch, validators)) = validator_result.emit_new_epoch() {
        drop(evm);
        assert_eq!(new_epoch, epoch + 1);
        // ... log ...

        // FIX: Merge any precompile state before returning
        merge_precompile_state(&state_for_precompile_ref, &mut accumulated_state_changes);

        inner_state.merge_transitions(BundleRetention::Reverts);
        let mut bundle = inner_state.take_bundle();
        bundle.apply_transitions_and_extend_reverts(accumulated_state_changes);
        return SystemTxnExecutionOutcome::EpochChanged(
            validator_result.into_executed_ordered_block_result(
                chain_spec, ordered_block, base_fee,
                bundle, validators,
            ),
        );
    }
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs` (lines ~639-840)

---

## GRETH-033: Mint Precompile Parallel State Divergence

**Problem:** During system transaction execution, three distinct state objects exist: (1) `inner_state` — the EVM state that system txns commit to, (2) `state_for_precompile` — a separate `ParallelState` created for the mint precompile, and (3) the `Arc<Storage::StateView>` used as the DB backend for both. When the mint precompile writes to `state_for_precompile`, those balance changes are invisible to subsequent EVM reads from `inner_state`. If a system transaction reads a balance that was just minted by the precompile, it sees the old (pre-mint) value.

**Fix:** After each system transaction that invokes the mint precompile, merge the precompile state changes into the EVM state before the next transaction executes. Alternatively, restructure so the mint precompile writes directly to the EVM's state (preferred long-term approach).

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/lib.rs ===

// Short-term fix: After each transact_system_txn call, flush precompile
// state into the EVM state so subsequent txns see minted balances.

// After metadata_txn execution (line ~688):
let (metadata_txn_result, metadata_state_changes) =
    transact_system_txn(&mut evm, metadata_txn);
evm.db_mut().commit(metadata_state_changes.clone());

// FIX: Flush precompile state into EVM state after each system txn
{
    let mut precompile_state = state_for_precompile_ref.lock();
    precompile_state.merge_transitions(BundleRetention::Reverts);
    let precompile_bundle = precompile_state.take_bundle();
    for (address, account) in precompile_bundle.state {
        if let Some(info) = account.info {
            use revm::state::{Account, AccountStatus, EvmStorageSlot};
            let evm_account = Account {
                info,
                storage: account
                    .storage
                    .into_iter()
                    .map(|(k, v)| (k, EvmStorageSlot::new(v.present_value, 0)))
                    .collect(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            };
            evm.db_mut().commit(
                std::collections::HashMap::from([(address, evm_account)])
            );
        }
    }
}
// Accumulate state changes
let mut accumulated_state_changes = metadata_state_changes;

// Same pattern after each validator txn execution in the loop (line ~760):
let (validator_result, validator_state_changes) = transact_system_txn(&mut evm, txn);
evm.db_mut().commit(validator_state_changes.clone());
// FIX: Flush precompile state after each validator txn too
{
    // ... same precompile merge logic as above ...
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs` (lines ~639-840)
- `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs` (no code change, but verify the precompile writes to the shared `ParallelState`)

**Review Comments** reviewer: neko; state: rejected; comments: Duplicate of GRETH-026 (Precompile State Merge Overwrites Instead of Deep Merging) in the LOW fixes design doc. Both describe the same root cause — the mint precompile writes to a separate `ParallelState` invisible to the EVM's `inner_state`, and the merge uses shallow `HashMap::insert`. GRETH-026 already has a rejection from lightman stating overlapping addresses won't occur in practice. Consolidate discussion under GRETH-026.

---

## GRETH-034: Cache-DB Consistency Gap During Merklization

**Problem:** The `state_root()` method acquires a new `database_provider_ro()` (which creates a RocksDB read transaction) at the time of the call, not at the time the block was executed. Between execution and merklization, the persistence layer may have written newer state to RocksDB. When a cache miss occurs during merklization, the fallback read hits the DB and gets newer state rather than the state that was current at execution time. This causes state root mismatches.

**Fix:** Use a RocksDB snapshot captured at execution time for read-only transactions during merklization, ensuring cache misses fall back to consistent point-in-time state.

**Reference Code:**
```rust
// === File: crates/gravity-storage/src/block_view_storage/mod.rs ===

// BEFORE (line 58-61): No snapshot — DB reads may see writes from later blocks
fn state_root(&self, hashed_state: &HashedPostState) -> ProviderResult<(B256, TrieUpdatesV2)> {
    let tx = self.client.database_provider_ro()?.into_tx();
    let nested_hash = NestedStateRoot::new(&tx, Some(self.cache.clone()));
    nested_hash.calculate(hashed_state)
}

// AFTER: Capture a RocksDB snapshot to ensure point-in-time consistency
fn state_root(&self, hashed_state: &HashedPostState) -> ProviderResult<(B256, TrieUpdatesV2)> {
    // Use a snapshot-backed read transaction to ensure cache misses
    // fall back to the same DB state that was current at execution time,
    // not a potentially newer state written by the persistence layer.
    let tx = self.client.database_provider_ro_with_snapshot()?.into_tx();
    let nested_hash = NestedStateRoot::new(&tx, Some(self.cache.clone()));
    nested_hash.calculate(hashed_state)
}
```

```rust
// === File: crates/storage/db/src/implementation/rocksdb/tx.rs ===

// Add snapshot support to the RocksDB Tx type.
// The Tx struct needs an optional snapshot field:

use rocksdb::SnapshotWithThreadMode;

pub struct Tx<K: cursor::TransactionKind> {
    state_db: Arc<DB>,
    account_db: Arc<DB>,
    storage_db: Arc<DB>,
    // ... existing fields ...

    // NEW: Optional snapshots for read-only transactions to provide
    // point-in-time consistency across all three DB instances.
    state_snapshot: Option<SnapshotWithThreadMode<'static, DB>>,
    account_snapshot: Option<SnapshotWithThreadMode<'static, DB>>,
    storage_snapshot: Option<SnapshotWithThreadMode<'static, DB>>,
}

// When creating an RO transaction with snapshots, capture them from all 3 DBs:
impl Tx<RO> {
    pub fn new_with_snapshots(
        state_db: Arc<DB>,
        account_db: Arc<DB>,
        storage_db: Arc<DB>,
    ) -> Self {
        // Safety: snapshots are valid as long as DB is alive (Arc keeps it alive)
        let state_snapshot = unsafe { state_db.snapshot() };
        let account_snapshot = unsafe { account_db.snapshot() };
        let storage_snapshot = unsafe { storage_db.snapshot() };
        Self {
            state_db,
            account_db,
            storage_db,
            state_snapshot: Some(state_snapshot),
            account_snapshot: Some(account_snapshot),
            storage_snapshot: Some(storage_snapshot),
            // ... other fields ...
        }
    }
}

// Read operations check for snapshot first:
// In cursor reads, use snapshot.get() instead of db.get() when snapshot is present.
```

**Files:**
- `crates/gravity-storage/src/block_view_storage/mod.rs` (line 58-61)
- `crates/storage/db/src/implementation/rocksdb/tx.rs` (add snapshot support)
- `crates/storage/db/src/implementation/rocksdb/mod.rs` (add `database_provider_ro_with_snapshot()`)
- `crates/trie/parallel/src/nested_hash.rs` (no changes needed; uses the Tx it receives)

---

## GRETH-035: Read-Only Transactions See Inconsistent Cross-DB State

**Problem:** The three-database RocksDB architecture (`state_db`, `account_db`, `storage_db`) means that a `Tx<RO>` reads from 3 independent DB instances. There is no coordinated snapshot across the 3 DBs. If `account_db` commits trie updates from block N+1 before `state_db` commits the corresponding state, a read-only transaction can observe the new trie but the old state, leading to inconsistencies during verification or merklization.

**Fix:** Coordinate RocksDB snapshots across all 3 DB instances. When creating a read-only transaction, atomically capture snapshots from all 3 DBs under a shared mutex to ensure consistency. This is closely related to GRETH-034; the snapshot mechanism described there addresses both findings.

**Reference Code:**
```rust
// === File: crates/storage/db/src/implementation/rocksdb/tx.rs ===

// The fix is the same snapshot mechanism as GRETH-034.
// Additionally, add a lock to coordinate snapshot creation:

use std::sync::Mutex as StdMutex;

/// Global lock to ensure snapshots across all 3 DBs are taken atomically.
/// This prevents the window where one DB has committed new data but another
/// has not yet committed, which would produce an inconsistent cross-DB view.
static SNAPSHOT_LOCK: Lazy<StdMutex<()>> = Lazy::new(|| StdMutex::new(()));

impl Tx<RO> {
    pub fn new_consistent_snapshot(
        state_db: Arc<DB>,
        account_db: Arc<DB>,
        storage_db: Arc<DB>,
    ) -> Self {
        // Hold the lock while taking all 3 snapshots to ensure
        // no commit can interleave between them.
        let _guard = SNAPSHOT_LOCK.lock().unwrap();
        let state_snapshot = state_db.snapshot();
        let account_snapshot = account_db.snapshot();
        let storage_snapshot = storage_db.snapshot();
        drop(_guard);

        Self {
            state_db,
            account_db,
            storage_db,
            state_snapshot: Some(state_snapshot),
            account_snapshot: Some(account_snapshot),
            storage_snapshot: Some(storage_snapshot),
            ..Default::default()
        }
    }
}

// The commit path must also acquire the lock:
fn commit_view(&self) -> Result<bool, DatabaseError> {
    let _guard = SNAPSHOT_LOCK.lock().unwrap();
    // ... existing commit logic (phases 1 and 2) ...
}
```

**Files:**
- `crates/storage/db/src/implementation/rocksdb/tx.rs` (lines ~50-78, 264-326)

---

## GRETH-036: Unbounded Channels Between Consensus and Execution

**Problem:** All 3 inter-layer channels (`ordered_block_tx/rx`, `execution_result_tx/rx`, `discard_txs_tx/rx`) are created with `tokio::sync::mpsc::unbounded_channel()`. If the execution layer falls behind the consensus layer, ordered blocks accumulate without limit, causing unbounded memory growth and eventual OOM.

**Fix:** Replace `unbounded_channel()` with `channel(capacity)` using a reasonable bound (e.g., 32-64). The consensus layer will apply natural backpressure via `.send().await` when the execution layer is saturated.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/lib.rs ===

// BEFORE (lines ~1392-1396):
let (ordered_block_tx, ordered_block_rx) = tokio::sync::mpsc::unbounded_channel();
let (execution_result_tx, execution_result_rx) = tokio::sync::mpsc::unbounded_channel();
let (discard_txs_tx, discard_txs_rx) = tokio::sync::mpsc::unbounded_channel();

// AFTER: Use bounded channels with backpressure
const ORDERED_BLOCK_CHANNEL_CAP: usize = 64;
const EXECUTION_RESULT_CHANNEL_CAP: usize = 64;
const DISCARD_TXS_CHANNEL_CAP: usize = 128;

let (ordered_block_tx, ordered_block_rx) =
    tokio::sync::mpsc::channel(ORDERED_BLOCK_CHANNEL_CAP);
let (execution_result_tx, execution_result_rx) =
    tokio::sync::mpsc::channel(EXECUTION_RESULT_CHANNEL_CAP);
let (discard_txs_tx, discard_txs_rx) =
    tokio::sync::mpsc::channel(DISCARD_TXS_CHANNEL_CAP);
```

```rust
// All send sites must change from .send() to .send().await (or .try_send() with
// overflow handling). For example, in PipeExecLayerApi methods that send ordered blocks:

// BEFORE:
self.ordered_block_tx.send(block).unwrap();

// AFTER (if in async context):
if self.ordered_block_tx.send(block).await.is_err() {
    error!(target: "PipeExecLayerApi", "execution layer channel closed");
}

// AFTER (if in sync context, use try_send with backpressure logging):
match self.ordered_block_tx.try_send(block) {
    Ok(()) => {}
    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
        warn!(target: "PipeExecLayerApi", "execution channel full — applying backpressure");
        // Block or retry with backoff
    }
    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
        error!(target: "PipeExecLayerApi", "execution layer channel closed");
    }
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs` (lines ~1392-1396, plus all send sites)
- Update sender types from `UnboundedSender<T>` to `Sender<T>` in relevant structs

---

## GRETH-037: Type-Erased Event Bus Singleton with Panicking Downcast

**Problem:** `PIPE_EXEC_LAYER_EVENT_BUS` is stored as `OnceCell<Box<dyn Any + Send + Sync>>`, and `get_pipe_exec_layer_event_bus()` calls `.downcast_ref::<T>().unwrap()`. If the generic type parameter `N: NodePrimitives` at the call site does not match the type used when the event bus was initialized, the downcast fails and the node panics. This is a type-safety hole that cannot be caught at compile time.

**Fix:** Replace the `Box<dyn Any>` approach with a typed `OnceLock<PipeExecLayerEventBus<EthPrimitives>>`, since gravity-reth exclusively uses `EthPrimitives`. This eliminates the dynamic downcast entirely and makes type mismatches a compile-time error.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs ===

// BEFORE (lines 12-29):
pub static PIPE_EXEC_LAYER_EVENT_BUS: OnceCell<Box<dyn Any + Send + Sync>> = OnceCell::new();

pub fn get_pipe_exec_layer_event_bus<N: NodePrimitives>() -> &'static PipeExecLayerEventBus<N> {
    let mut wait_time = 0;
    loop {
        let event_bus = PIPE_EXEC_LAYER_EVENT_BUS
            .get()
            .map(|ext| ext.downcast_ref::<PipeExecLayerEventBus<N>>().unwrap());
        if let Some(event_bus) = event_bus {
            break event_bus;
        } else if wait_time % 5 == 0 {
            info!("Wait PipeExecLayerEventBus ready...");
        }
        sleep(Duration::from_secs(1));
        wait_time += 1;
    }
}

// AFTER: Use typed OnceLock — no dynamic downcast, no type parameter needed
use std::sync::OnceLock;
use reth_ethereum_primitives::EthPrimitives;

pub static PIPE_EXEC_LAYER_EVENT_BUS: OnceLock<PipeExecLayerEventBus<EthPrimitives>> =
    OnceLock::new();

/// Get a reference to the global `PipeExecLayerEventBus` instance.
/// Blocks until the event bus is initialized, with a maximum timeout.
pub fn get_pipe_exec_layer_event_bus() -> &'static PipeExecLayerEventBus<EthPrimitives> {
    const MAX_WAIT_SECS: u64 = 120;
    let start = std::time::Instant::now();
    loop {
        if let Some(event_bus) = PIPE_EXEC_LAYER_EVENT_BUS.get() {
            return event_bus;
        }
        if start.elapsed().as_secs() >= MAX_WAIT_SECS {
            panic!(
                "PipeExecLayerEventBus not initialized after {}s — \
                 likely a startup ordering bug",
                MAX_WAIT_SECS
            );
        }
        if start.elapsed().as_secs() % 5 == 0 {
            info!("Wait PipeExecLayerEventBus ready...");
        }
        sleep(Duration::from_secs(1));
    }
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs` (lines 12-29)
- All call sites of `get_pipe_exec_layer_event_bus::<N>()` must remove the generic parameter

**Review Comments** reviewer: neko; state: accepted; comments: The typed `OnceLock<PipeExecLayerEventBus<EthPrimitives>>` fix in event-bus is correct and eliminates the downcast. However, the call site in `tree/mod.rs` L593-596 uses `std::mem::transmute` to bridge `Receiver<PipeExecLayerEvent<EthPrimitives>>` to `Receiver<PipeExecLayerEvent<N>>`, which trades a deterministic panic (from the old Any downcast) for potential silent UB if N ever differs from EthPrimitives. Recommend adding a `TypeId` assertion before the transmute as a runtime safety guard: `assert_eq!(std::any::TypeId::of::<N>(), std::any::TypeId::of::<EthPrimitives>(), "GRETH-037: pipe_run_inner requires N = EthPrimitives")`. This is feasible because `NodePrimitives` already has a `'static` bound (see `crates/primitives-traits/src/node.rs` L13), and it does not pollute the generic signature chain, keeping the reth upstream diff minimal.

---

## GRETH-038: Thread-Blocking Busy-Wait in Event Bus Access

**Problem:** `get_pipe_exec_layer_event_bus()` uses `std::thread::sleep(1s)` in an infinite loop with no maximum retry count. If the event bus is never initialized (e.g., due to a startup failure), the calling thread blocks forever. This also wastes an OS thread on pure busy-waiting.

**Fix:** Add a maximum wait timeout (e.g., 120 seconds) and panic with a clear error message if the timeout is exceeded. This is combined with the GRETH-037 fix above (using `OnceLock`). Long-term, consider using a condition variable or `tokio::sync::watch` for zero-cost notification instead of polling.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs ===
// (See GRETH-037 reference code above — the combined fix addresses both issues)

// The key addition is the MAX_WAIT_SECS timeout:
const MAX_WAIT_SECS: u64 = 120;
let start = std::time::Instant::now();
loop {
    if let Some(event_bus) = PIPE_EXEC_LAYER_EVENT_BUS.get() {
        return event_bus;
    }
    if start.elapsed().as_secs() >= MAX_WAIT_SECS {
        panic!(
            "PipeExecLayerEventBus not initialized after {}s — \
             likely a startup ordering bug",
            MAX_WAIT_SECS
        );
    }
    // Log every 5 seconds
    if start.elapsed().as_secs() % 5 == 0 {
        info!("Wait PipeExecLayerEventBus ready...");
    }
    sleep(Duration::from_secs(1));
}

// ALTERNATIVE (preferred long-term): Use a Notify for zero-cost waiting
use std::sync::OnceLock;
use tokio::sync::Notify;

static EVENT_BUS_NOTIFY: Lazy<Notify> = Lazy::new(Notify::new);

pub async fn get_pipe_exec_layer_event_bus_async()
    -> &'static PipeExecLayerEventBus<EthPrimitives>
{
    if let Some(bus) = PIPE_EXEC_LAYER_EVENT_BUS.get() {
        return bus;
    }
    // Wait until notified by the initializer
    tokio::time::timeout(Duration::from_secs(120), EVENT_BUS_NOTIFY.notified())
        .await
        .expect("PipeExecLayerEventBus not initialized within 120s");
    PIPE_EXEC_LAYER_EVENT_BUS.get().expect("event bus should be set after notify")
}

// In the initialization path:
pub fn init_pipe_exec_layer_event_bus(bus: PipeExecLayerEventBus<EthPrimitives>) {
    PIPE_EXEC_LAYER_EVENT_BUS.set(bus).expect("event bus already initialized");
    EVENT_BUS_NOTIFY.notify_waiters();
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs` (lines 14-29)

---

## GRETH-039: No Cryptographic Verification for Cross-Chain Oracle Data

**Problem:** The oracle relayer in `blockchain_source.rs` calls `self.rpc_client.get_logs(&filter)` and trusts the RPC response with zero cryptographic proof. A compromised or malicious RPC endpoint can inject fabricated `MessageSent` events, causing the oracle to relay fraudulent cross-chain data that gets committed on-chain.

**Fix:** Implement multi-RPC cross-checking as a defense-in-depth measure: query at least 2 independent RPC endpoints and only accept events that are confirmed by a quorum. Long-term, implement Merkle proof verification (verify the event log's receipt trie inclusion proof against a finalized block hash).

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs ===

// BEFORE (line ~243): Single RPC, no verification
let logs = self.rpc_client.get_logs(&filter).await?;

// AFTER: Multi-RPC cross-validation
// Add a secondary (or tertiary) RPC client to BlockchainSource:
pub struct BlockchainSource {
    rpc_client: EthHttpCli,
    // NEW: Additional RPC endpoints for cross-validation
    verification_clients: Vec<EthHttpCli>,
    // ... existing fields ...
}

async fn poll_events(&self, /* ... */) -> Result<Vec<OracleData>> {
    // ... build filter ...

    // Query primary RPC
    let primary_logs = self.rpc_client.get_logs(&filter).await?;

    // Cross-validate with at least one verification endpoint
    if !self.verification_clients.is_empty() {
        let verification_client = &self.verification_clients[0];
        let verification_logs = verification_client.get_logs(&filter).await
            .map_err(|e| {
                warn!(
                    target: "blockchain_source",
                    error = %e,
                    "Verification RPC failed — using primary only (degraded mode)"
                );
                e
            });

        if let Ok(ref v_logs) = verification_logs {
            // Compare log sets: same count and same transaction hashes
            if v_logs.len() != primary_logs.len() {
                error!(
                    target: "blockchain_source",
                    primary_count = primary_logs.len(),
                    verification_count = v_logs.len(),
                    "RPC MISMATCH: log count differs — rejecting batch"
                );
                return Err(anyhow!("Cross-chain oracle RPC mismatch"));
            }
            for (p, v) in primary_logs.iter().zip(v_logs.iter()) {
                if p.transaction_hash != v.transaction_hash
                    || p.data() != v.data()
                {
                    error!(
                        target: "blockchain_source",
                        "RPC MISMATCH: log data differs — rejecting batch"
                    );
                    return Err(anyhow!("Cross-chain oracle RPC data mismatch"));
                }
            }
        }
    }

    // ... process primary_logs as before ...
}

// Long-term: Add Merkle proof verification
// For each log, request eth_getProof or eth_getTransactionReceipt with proof,
// verify the receipt trie inclusion against a trusted finalized block hash.
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs` (line ~243, struct definition)
- `crates/pipe-exec-layer-ext-v2/relayer/src/oracle_manager.rs` (pass additional RPC clients when constructing `BlockchainSource`)

---

## GRETH-040: Zero-Signature System Transactions Bypass All Validation

**Problem:** System transactions are constructed with `Signature::new(U256::ZERO, U256::ZERO, false)` and sender `SYSTEM_CALLER`. There is no allowlist restricting which contract addresses these transactions can call. If the construction logic is compromised or if a code path mistakenly passes an arbitrary `contract` address to `new_system_call_txn()`, the system caller can invoke any contract with unlimited authority.

**Fix:** Add an explicit allowlist of valid target contract addresses in the `new_system_call_txn()` function. Panic (or return an error) if the target is not in the allowlist.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs ===
// (After consolidation with GRETH-041, this will be the single definition)

use std::collections::HashSet;
use once_cell::sync::Lazy;

/// Allowlist of contract addresses that system transactions are permitted to call.
/// Any attempt to create a system txn targeting a non-allowlisted address will panic.
static SYSTEM_TXN_TARGET_ALLOWLIST: Lazy<HashSet<Address>> = Lazy::new(|| {
    HashSet::from([
        BLOCK_ADDR,            // Blocker.onBlockStart
        DKG_ADDR,              // DKG transcript submission
        JWK_MANAGER_ADDR,      // JWK updates
        NATIVE_ORACLE_ADDR,    // Oracle data recording
    ])
});

/// Create a new system call transaction.
///
/// Validates that the target contract is in the allowlist before construction.
/// Panics if the target is not an approved system contract.
pub(crate) fn new_system_call_txn(
    contract: Address,
    nonce: u64,
    gas_price: u128,
    input: Bytes,
) -> TransactionSigned {
    assert!(
        SYSTEM_TXN_TARGET_ALLOWLIST.contains(&contract),
        "system transaction target {contract} is not in the allowlist"
    );

    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce,
            gas_price,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        Signature::new(U256::ZERO, U256::ZERO, false),
    )
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs` (lines 210-229)
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/mod.rs` (lines 175-193, remove duplicate after GRETH-041)

---

## GRETH-041: Duplicate new_system_call_txn() Definitions

**Problem:** The function `new_system_call_txn()` is defined identically in two files: `metadata_txn.rs` (line 211, private `fn`) and `mod.rs` (line 175, `pub(crate)`). This duplication risks divergence — if one copy is updated (e.g., with the GRETH-040 allowlist fix) but the other is not, the security fix is partially bypassed.

**Fix:** Remove the duplicate in `metadata_txn.rs` and have all call sites use the single `pub(crate)` definition in `mod.rs`. Alternatively, keep the definition in one file and re-export.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs ===

// BEFORE (lines 210-229): Private duplicate definition
fn new_system_call_txn(
    contract: Address,
    nonce: u64,
    gas_price: u128,
    input: Bytes,
) -> TransactionSigned {
    // ... identical body ...
}

// AFTER: Remove the function entirely. Import from parent module instead.
// At the top of metadata_txn.rs, add:
use super::new_system_call_txn;

// And update construct_metadata_txn to use the imported function:
pub fn construct_metadata_txn(
    nonce: u64,
    gas_price: u128,
    timestamp_us: u64,
    proposer_index: Option<u64>,
) -> TransactionSigned {
    let proposer_idx = proposer_index.unwrap_or(NIL_PROPOSER_INDEX);
    let call = onBlockStartCall {
        proposerIndex: proposer_idx,
        failedProposerIndices: vec![],
        timestampMicros: timestamp_us,
    };
    let input: Bytes = call.abi_encode().into();
    // Uses the single canonical definition from mod.rs
    new_system_call_txn(BLOCK_ADDR, nonce, gas_price, input)
}
```

```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/mod.rs ===
// Keep the single definition here (lines 175-193), enhanced with the GRETH-040 allowlist.
// This is already pub(crate), so all submodules can import it via `use super::new_system_call_txn`.
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs` (remove lines 210-229)
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/mod.rs` (keep lines 175-193, add allowlist from GRETH-040)

---

## GRETH-042: failedProposerIndices Always Empty

**Problem:** The `construct_metadata_txn()` function always passes `failedProposerIndices: vec![]` to the `onBlockStart` contract call. This means the on-chain slashing/penalty logic for failed proposers never receives the actual indices of validators who failed to propose blocks, effectively disabling proposer accountability.

**Fix:** Thread the actual failed proposer indices from the consensus layer's `OrderedBlock` data through to `construct_metadata_txn()`. The `OrderedBlock` from the consensus layer should carry information about which proposers failed in the gap between blocks.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs ===

// BEFORE (lines 241-258): Always passes empty vec
pub fn construct_metadata_txn(
    nonce: u64,
    gas_price: u128,
    timestamp_us: u64,
    proposer_index: Option<u64>,
) -> TransactionSigned {
    let proposer_idx = proposer_index.unwrap_or(NIL_PROPOSER_INDEX);
    let call = onBlockStartCall {
        proposerIndex: proposer_idx,
        failedProposerIndices: vec![],       // <-- always empty
        timestampMicros: timestamp_us,
    };
    let input: Bytes = call.abi_encode().into();
    new_system_call_txn(BLOCK_ADDR, nonce, gas_price, input)
}

// AFTER: Accept failed proposer indices as a parameter
pub fn construct_metadata_txn(
    nonce: u64,
    gas_price: u128,
    timestamp_us: u64,
    proposer_index: Option<u64>,
    failed_proposer_indices: Vec<u64>,
) -> TransactionSigned {
    let proposer_idx = proposer_index.unwrap_or(NIL_PROPOSER_INDEX);
    let call = onBlockStartCall {
        proposerIndex: proposer_idx,
        failedProposerIndices: failed_proposer_indices,
        timestampMicros: timestamp_us,
    };
    let input: Bytes = call.abi_encode().into();
    new_system_call_txn(BLOCK_ADDR, nonce, gas_price, input)
}
```

```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/lib.rs ===

// At the call site in execute_system_transactions() (line ~676):
// BEFORE:
let metadata_txn = construct_metadata_txn(
    current_nonce,
    gas_price,
    ordered_block.timestamp_us,
    ordered_block.proposer_index,
);

// AFTER: Pass failed proposer indices from ordered_block
let metadata_txn = construct_metadata_txn(
    current_nonce,
    gas_price,
    ordered_block.timestamp_us,
    ordered_block.proposer_index,
    ordered_block.failed_proposer_indices.clone(),
);
```

```rust
// The OrderedBlock type (in gravity-sdk) needs a new field:
pub struct OrderedBlock {
    // ... existing fields ...
    /// Indices of validators who failed to propose in the gap since the last block.
    /// Populated by the consensus layer (AptosBFT) based on missed rounds.
    pub failed_proposer_indices: Vec<u64>,
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs` (lines 241-258)
- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs` (call site ~line 676)
- gravity-sdk: `OrderedBlock` type definition (add `failed_proposer_indices` field)

---

## GRETH-043: Nonce Truncation u128 to u64

**Problem:** Multiple locations truncate `u128` nonces to `u64` using `as u64`, which silently discards the upper 64 bits. If a cross-chain source produces nonces exceeding `u64::MAX` (2^64), the truncated nonce wraps to a small value, potentially causing duplicate nonce collisions and re-processing of old oracle data, or missing new data.

**Fix:** Replace `as u64` casts with checked conversions (`u64::try_from()`) that return an error or log a warning on overflow. Alternatively, use `u128` consistently throughout the nonce pipeline.

**Reference Code:**
```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/oracle_state.rs ===

// BEFORE (line 148): Silent truncation
results.push(OracleSourceState {
    source_type: SOURCE_TYPE_BLOCKCHAIN,
    source_id: source_id.try_into().unwrap_or(0),
    latest_nonce: latest_nonce as u64,
    latest_record,
});

// AFTER: Checked conversion with error on overflow
let nonce_u64 = u64::try_from(latest_nonce).unwrap_or_else(|_| {
    error!(
        target: "oracle_state",
        source_id = source_id.to_string(),
        latest_nonce,
        "Nonce exceeds u64::MAX — clamping to u64::MAX"
    );
    u64::MAX
});
results.push(OracleSourceState {
    source_type: SOURCE_TYPE_BLOCKCHAIN,
    source_id: source_id.try_into().unwrap_or(0),
    latest_nonce: nonce_u64,
    latest_record,
});

// Same pattern for line 174:
latest_nonce: u64::try_from(latest_nonce).unwrap_or_else(|_| {
    error!(target: "oracle_state", latest_nonce, "Nonce exceeds u64::MAX");
    u64::MAX
}),
```

```rust
// === File: crates/pipe-exec-layer-ext-v2/relayer/src/oracle_manager.rs ===

// BEFORE (line 263): Silent truncation
s.last_nonce().await.map(|n| n as u64),

// AFTER: Checked conversion
s.last_nonce().await.map(|n| {
    u64::try_from(n).unwrap_or_else(|_| {
        error!(target: "oracle_manager", nonce = n, "Nonce exceeds u64::MAX");
        u64::MAX
    })
}),
```

```rust
// === File: crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/jwk_consensus_config.rs ===

// BEFORE (line 110): Silent truncation
onchain_nonce: Some(nonce as u64),

// AFTER: Checked conversion
onchain_nonce: Some(u64::try_from(nonce).unwrap_or_else(|_| {
    error!(target: "jwk_consensus_config", nonce, "Nonce exceeds u64::MAX");
    u64::MAX
})),
```

```rust
// PREFERRED LONG-TERM: Change OracleSourceState.latest_nonce and
// OIDCProvider.onchain_nonce to u128 to avoid any truncation.
// This requires updating the gravity_api_types crate:
pub struct OracleSourceState {
    pub source_type: u32,
    pub source_id: u64,
    pub latest_nonce: u128,  // was u64
    pub latest_record: Option<LatestDataRecord>,
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/oracle_state.rs` (lines 148, 174)
- `crates/pipe-exec-layer-ext-v2/relayer/src/oracle_manager.rs` (line 263)
- `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/jwk_consensus_config.rs` (line 110)
- Long-term: `gravity_api_types` crate (`OracleSourceState`, `OIDCProvider` type definitions)

---

## Summary Table

| ID | Severity | Title | Complexity | Key Files |
|----|----------|-------|-----------|-----------|
| GRETH-029 | CRITICAL | Pipeline Deadlock via Barrier Timeout Gap | Medium | `execute/src/lib.rs`, `channel.rs` |
| GRETH-030 | CRITICAL | Cache Eviction of Unpersisted Trie Nodes | Low | `storage-api/src/cache.rs` |
| GRETH-031 | HIGH | BLOCKHASH Opcode Unimplemented | Low | `block_view_storage/mod.rs` |
| GRETH-032 | HIGH | Token Loss During Epoch Transitions | Medium | `execute/src/lib.rs` |
| GRETH-033 | HIGH | Mint Precompile State Divergence | Medium | `execute/src/lib.rs`, `mint_precompile.rs` |
| GRETH-034 | HIGH | Cache-DB Consistency Gap During Merklization | High | `block_view_storage/mod.rs`, `rocksdb/tx.rs` |
| GRETH-035 | HIGH | Inconsistent Cross-DB Read Snapshots | High | `rocksdb/tx.rs` |
| GRETH-036 | HIGH | Unbounded Channels — No Backpressure | Medium | `execute/src/lib.rs` |
| GRETH-037 | HIGH | Type-Erased Event Bus with Panicking Downcast | Low | `event-bus/src/lib.rs` |
| GRETH-038 | HIGH | Thread-Blocking Busy-Wait in Event Bus | Low | `event-bus/src/lib.rs` |
| GRETH-039 | HIGH | No Crypto Verification for Oracle Data | High | `relayer/src/blockchain_source.rs` |
| GRETH-040 | HIGH | Zero-Sig System Txns — No Target Allowlist | Low | `onchain_config/metadata_txn.rs`, `mod.rs` |
| GRETH-041 | HIGH | Duplicate new_system_call_txn() | Low | `onchain_config/metadata_txn.rs`, `mod.rs` |
| GRETH-042 | HIGH | failedProposerIndices Always Empty | Medium | `onchain_config/metadata_txn.rs`, `lib.rs` |
| GRETH-043 | HIGH | Nonce Truncation u128 to u64 | Low | `oracle_state.rs`, `oracle_manager.rs`, `jwk_consensus_config.rs` |

### Recommended Implementation Order

1. **GRETH-030** (CRITICAL, Low complexity) — one-line `.min(persist_height)` cap
2. **GRETH-031** (HIGH, Low complexity) — replace `unimplemented!()` with DB lookup
3. **GRETH-041** (HIGH, Low complexity) — delete duplicate function, add import
4. **GRETH-040** (HIGH, Low complexity) — add allowlist to consolidated function
5. **GRETH-037 + GRETH-038** (HIGH, Low complexity) — combined event bus rewrite
6. **GRETH-043** (HIGH, Low complexity) — replace `as u64` with `try_from`
7. **GRETH-029** (CRITICAL, Medium complexity) — barrier timeout + panic monitoring
8. **GRETH-032** (HIGH, Medium complexity) — merge precompile state on early return
9. **GRETH-033** (HIGH, Medium complexity) — flush precompile state after each system txn
10. **GRETH-036** (HIGH, Medium complexity) — bounded channels + send site updates
11. **GRETH-042** (HIGH, Medium complexity) — thread failed proposer data from SDK
12. **GRETH-034 + GRETH-035** (HIGH, High complexity) — RocksDB snapshot coordination
13. **GRETH-039** (HIGH, High complexity) — multi-RPC verification + Merkle proofs
