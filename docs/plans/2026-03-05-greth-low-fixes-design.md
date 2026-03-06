# gravity-reth Phase 3 Security Audit — LOW Severity Fixes

Date: 2026-03-05
Scope: GRETH-065 through GRETH-075 (LOW severity, excluding GRETH-067 which is invalid)

---

## GRETH-065: BLS Precompile Gas Underpricing

**Problem:** `POP_VERIFY_GAS` is set to 45,000, which is only ~23% of the EIP-2537 equivalent cost for two bilinear pairings plus hash-to-curve. This allows an attacker to spam BLS PoP verification calls at a fraction of their true computational cost, potentially causing DoS on validators.

**Fix:** Increase `POP_VERIFY_GAS` to 150,000 gas. This aligns with EIP-2537 pricing where `BLS12_PAIRING_CHECK` for 2 pairings costs ~131,000 gas, plus ~32,000 for `BLS12_MAP_FP2_TO_G2` (hash-to-curve), totaling ~163,000. A value of 150,000 provides a conservative lower bound.

**Reference Code:**
```rust
// before (crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs:22)
/// Gas cost for PoP verification (2 bilinear pairings + hash-to-curve)
const POP_VERIFY_GAS: u64 = 45_000;

// after
/// Gas cost for PoP verification (2 bilinear pairings + hash-to-curve)
/// Aligned with EIP-2537: ~131k (2 pairings) + ~32k (hash-to-curve) ≈ 163k.
/// Set to 150,000 as a conservative lower bound.
const POP_VERIFY_GAS: u64 = 150_000;
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs`

**Review Comments** reviewer: neko; state: pending; comments: Gas repricing is a hardfork-level consensus-breaking change and the exact value needs careful justification. The proposed 150,000 is derived from EIP-2537 component costs (~176,600 total for 2-pairing + hash-to-curve + cofactor clearing), but the native `blst` implementation shares final exponentiation across pairings and fuses subgroup checks, making it ~60-70% of the EIP-2537 equivalent cost. Need to run a `criterion` benchmark of `verify_pop` on target hardware and convert to gas using the Ethereum gas-to-time ratio (~1 gas ≈ 1ns, calibrated against EcRecover: 3000 gas ≈ ~3μs) before committing to a final value.

---

## GRETH-066: Mint Precompile Gas Underpricing

**Problem:** `MINT_BASE_GAS` is set to 21,000, the same cost as a simple ETH transfer. However, the mint precompile performs additional work: caller authorization, input parsing, acquiring a mutex lock, modifying state (incrementing balances), and logging. This underpricing could be exploited by the authorized caller to perform many state modifications cheaply.

**Fix:** Increase `MINT_BASE_GAS` to 50,000 gas. This accounts for the authorization check, state mutation (comparable to `SSTORE` at 20,000 gas), balance increment via HashMap, and lock acquisition overhead.

**Reference Code:**
```rust
// before (crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs:26-28)
/// Base gas cost for mint operation
const MINT_BASE_GAS: u64 = 21000;

// after
/// Base gas cost for mint operation.
/// Higher than a simple transfer (21,000) to account for authorization check,
/// state mutation (balance increment), and mutex lock acquisition.
const MINT_BASE_GAS: u64 = 50_000;
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs`

---

## GRETH-068: Panic on make_canonical Failure

**Problem:** In `make_executed_block_canonical()`, the call to `self.make_canonical(block_hash)` uses `unwrap_or_else(|err| panic!(...))`. If `make_canonical` returns an error (e.g., database corruption, storage I/O failure), the entire node process terminates immediately with no opportunity for retry, cleanup, or graceful shutdown. Under BFT consensus, the block is already consensus-committed, so a transient error should be retried rather than causing a crash.

**Fix:** Replace the `panic!` with error propagation. Change `make_executed_block_canonical` to return `ProviderResult<()>` and propagate the error to the caller (`on_pipe_exec_event`), which can log the error and trigger a graceful shutdown or retry. If changing the return type is too invasive, at minimum replace the panic with a bounded retry loop with exponential backoff.

**Reference Code:**
```rust
// before (crates/engine/tree/src/tree/mod.rs:517-549)
fn make_executed_block_canonical(&mut self, block: ExecutedBlockWithTrieUpdates<N>) {
    let block_number = block.recovered_block.number();
    let block_hash = block.recovered_block.hash();
    let sealed_header = block.recovered_block.clone_sealed_header();

    self.state.tree_state.insert_executed(block);

    self.state.forkchoice_state_tracker.set_latest(
        ForkchoiceState {
            head_block_hash: block_hash,
            safe_block_hash: block_hash,
            finalized_block_hash: block_hash,
        },
        ForkchoiceStatus::Valid,
    );

    self.make_canonical(block_hash).unwrap_or_else(|err| {
        panic!(
            "Failed to make canonical, block_number={block_number} block_hash={block_hash}: {err}",
        )
    });

    self.canonical_in_memory_state.set_safe(sealed_header.clone());
    self.canonical_in_memory_state.set_finalized(sealed_header);
}

// after
fn make_executed_block_canonical(
    &mut self,
    block: ExecutedBlockWithTrieUpdates<N>,
) -> ProviderResult<()> {
    let block_number = block.recovered_block.number();
    let block_hash = block.recovered_block.hash();
    let sealed_header = block.recovered_block.clone_sealed_header();

    self.state.tree_state.insert_executed(block);

    self.state.forkchoice_state_tracker.set_latest(
        ForkchoiceState {
            head_block_hash: block_hash,
            safe_block_hash: block_hash,
            finalized_block_hash: block_hash,
        },
        ForkchoiceStatus::Valid,
    );

    if let Err(err) = self.make_canonical(block_hash) {
        error!(
            target: "engine::tree",
            block_number=%block_number,
            block_hash=%block_hash,
            error=%err,
            "Failed to make block canonical — triggering graceful shutdown"
        );
        return Err(err);
    }

    self.canonical_in_memory_state.set_safe(sealed_header.clone());
    self.canonical_in_memory_state.set_finalized(sealed_header);
    Ok(())
}
```

Update the call site in `on_pipe_exec_event`:
```rust
// before (crates/engine/tree/src/tree/mod.rs:490-498)
PipeExecLayerEvent::MakeCanonical(MakeCanonicalEvent { executed_block, tx }) => {
    let block_number = executed_block.recovered_block.number();
    // ...
    self.make_executed_block_canonical(executed_block);
    tx.send(()).expect("Failed to send make canonical event");
}

// after
PipeExecLayerEvent::MakeCanonical(MakeCanonicalEvent { executed_block, tx }) => {
    let block_number = executed_block.recovered_block.number();
    // ...
    if let Err(err) = self.make_executed_block_canonical(executed_block) {
        error!(
            target: "on_pipe_exec_event",
            block_number=%block_number,
            error=%err,
            "make_executed_block_canonical failed, node should shut down"
        );
        // Do NOT send on tx — the caller will detect the failure via channel drop.
        // Optionally: trigger a shutdown signal here.
        return;
    }
    tx.send(()).expect("Failed to send make canonical event");
}
```

**Files:**
- `crates/engine/tree/src/tree/mod.rs`

---

## GRETH-069: Parallel State/Trie Persistence Ordering

**Problem:** State writes (execution, hashed state, history indices) and trie writes run in separate `thread::scope` handles that commit independently. There is no ordering barrier between the two threads. If the state thread commits first and the trie thread crashes, or vice versa, the system relies entirely on checkpoint-based idempotent recovery. While the existing checkpoint design handles this correctly, the lack of explicit ordering documentation makes the window between commits a potential source of confusion and subtle bugs.

**Fix:** The existing checkpoint-based recovery (GRETH-005 comment at line 344) already handles this correctly. Add explicit documentation on the ordering invariant and the recovery guarantee. Optionally, add a metric to track the timing gap between state and trie commits.

**Reference Code:**
```rust
// before (crates/engine/tree/src/persistence.rs:225)
thread::scope(|scope| -> Result<(), PersistenceError> {
    let state_handle = scope.spawn(|| -> Result<(), PersistenceError> {
        // ... state writes ...
    });
    let trie_handle = scope.spawn(|| -> Result<(), PersistenceError> {
        // ... trie writes ...
    });

// after — add timing metric and enhanced documentation
thread::scope(|scope| -> Result<(), PersistenceError> {
    // ORDERING NOTE (GRETH-069): State and trie writes execute concurrently
    // in separate threads with independent commits. This is safe because:
    //
    // 1. Each stage uses per-stage checkpoints (StageId::Execution,
    //    StageId::MerkleExecute, etc.) stored atomically with its data.
    // 2. On crash, StorageRecoveryHelper re-executes only incomplete stages
    //    by comparing checkpoint block numbers.
    // 3. All stage writes are idempotent — re-executing produces identical data.
    //
    // The acceptable inconsistency window is: between the first thread's commit
    // and the second thread's commit (~milliseconds). During this window, a crash
    // would leave one stage ahead, which recovery handles correctly.
    let state_commit_time = Arc::new(std::sync::Mutex::new(None));
    let trie_commit_time = Arc::new(std::sync::Mutex::new(None));
    let state_commit_time_clone = state_commit_time.clone();
    let trie_commit_time_clone = trie_commit_time.clone();

    let state_handle = scope.spawn(move || -> Result<(), PersistenceError> {
        // ... existing state writes ...
        // After final commit:
        *state_commit_time_clone.lock().unwrap() = Some(Instant::now());
        Ok(())
    });
    let trie_handle = scope.spawn(move || -> Result<(), PersistenceError> {
        // ... existing trie writes ...
        // After commit:
        *trie_commit_time_clone.lock().unwrap() = Some(Instant::now());
        Ok(())
    });
    // ... existing error handling ...

    // Track the commit ordering gap
    if let (Some(s), Some(t)) = (
        *state_commit_time.lock().unwrap(),
        *trie_commit_time.lock().unwrap(),
    ) {
        let gap = if s > t { s - t } else { t - s };
        metrics::histogram!(
            "save_blocks_time",
            &[("process", "state_trie_commit_gap")]
        )
        .record(gap);
    }
```

**Files:**
- `crates/engine/tree/src/persistence.rs`

---

## GRETH-070: Recovery Does Not Handle Execution Checkpoint Corruption

**Problem:** `StorageRecoveryHelper::check_and_recover()` trusts the `StageId::Execution` checkpoint as ground truth via `recover_block_number()`, which simply reads the checkpoint and returns its `block_number` (defaulting to 0 if absent). If the checkpoint is corrupted or points to a block whose data was only partially written, recovery will treat that block as the recovery target and rebuild other stages on top of potentially inconsistent state.

**Fix:** After reading `recover_block_number`, validate that the block at that number actually exists in the database (header + body present) and that the execution state is consistent. If validation fails, fall back to the previous block number.

**Reference Code:**
```rust
// before (crates/engine/tree/src/recovery.rs:116-125)
pub fn check_and_recover(&self) -> ProviderResult<()> {
    let provider_ro = self.factory.database_provider_ro()?;
    let recover_block_number = provider_ro.recover_block_number()?;
    let best_block_number = provider_ro.best_block_number()?;
    drop(provider_ro);

    if recover_block_number == best_block_number {
        info!(target: "engine::recovery", block_number = ?recover_block_number,
              "No recovery needed, checkpoints are consistent");
        return Ok(());
    }

// after
pub fn check_and_recover(&self) -> ProviderResult<()> {
    let provider_ro = self.factory.database_provider_ro()?;
    let recover_block_number = provider_ro.recover_block_number()?;
    let best_block_number = provider_ro.best_block_number()?;

    // GRETH-070: Validate that the execution checkpoint points to a block
    // that actually exists and has consistent data.
    if recover_block_number > 0 {
        let header_exists = provider_ro
            .header_by_number(recover_block_number)?
            .is_some();
        if !header_exists {
            warn!(
                target: "engine::recovery",
                recover_block_number = ?recover_block_number,
                "Execution checkpoint references non-existent block header, \
                 falling back to previous block"
            );
            // Fall back: treat recover_block_number - 1 as the safe point
            let recover_block_number = recover_block_number.saturating_sub(1);
            drop(provider_ro);
            // Continue recovery from the safe block
            return self.recover_from(recover_block_number, best_block_number);
        }
    }
    drop(provider_ro);

    if recover_block_number == best_block_number {
        info!(target: "engine::recovery", block_number = ?recover_block_number,
              "No recovery needed, checkpoints are consistent");
        return Ok(());
    }

    self.recover_from(recover_block_number, best_block_number)
}

/// Inner recovery logic extracted for reuse.
fn recover_from(
    &self,
    recover_block_number: BlockNumber,
    best_block_number: BlockNumber,
) -> ProviderResult<()> {
    info!(target: "engine::recovery",
          recover_block = ?recover_block_number,
          best_block = ?best_block_number,
          "Starting recovery");

    self.recover_hashing(recover_block_number)?;
    self.recover_merkle(recover_block_number)?;
    if !get_gravity_config().validator_node_only {
        self.recover_history_indices(recover_block_number)?;
    }

    let provider_rw = self.factory.database_provider_rw()?;
    provider_rw.update_pipeline_stages(recover_block_number, false)?;
    provider_rw.commit()?;
    info!(target: "engine::recovery",
          recover_block_number = ?recover_block_number,
          "Recovery completed successfully");
    Ok(())
}
```

**Files:**
- `crates/engine/tree/src/recovery.rs`

---

## GRETH-071: Static File Pruning Assumes Block Body Indices Exist

**Problem:** In `prune_static_file_segment`, when `provider.block_body_indices(target_block)?` returns `None` (possible after a crash or incomplete write), the pruning silently does nothing — only the header branch and the `else` branch with indices are handled. No warning is emitted if indices are missing.

**Fix:** Add an explicit `else` branch that logs a warning when `block_body_indices` returns `None`, making the silent skip visible in logs.

**Reference Code:**
```rust
// before (crates/storage/provider/src/providers/static_file/manager.rs:1092-1102)
let mut writer = self.latest_writer(segment)?;
if segment.is_headers() {
    writer.prune_headers(highest_static_file_block - target_block)?;
} else if let Some(block) = provider.block_body_indices(target_block)? {
    let highest_tx = self.get_highest_static_file_tx(segment).unwrap_or_default();
    let to_delete = highest_tx - block.last_tx_num();
    if segment.is_receipts() {
        writer.prune_receipts(to_delete, target_block)?;
    } else {
        writer.prune_transactions(to_delete, target_block)?;
    }
}
writer.commit()?;

// after
let mut writer = self.latest_writer(segment)?;
if segment.is_headers() {
    writer.prune_headers(highest_static_file_block - target_block)?;
} else if let Some(block) = provider.block_body_indices(target_block)? {
    let highest_tx = self.get_highest_static_file_tx(segment).unwrap_or_default();
    let to_delete = highest_tx
        .checked_sub(block.last_tx_num())
        .unwrap_or_else(|| {
            warn!(
                target: "reth::providers",
                ?segment,
                highest_tx,
                last_tx_num = block.last_tx_num(),
                "highest_tx < last_tx_num during static file pruning, skipping"
            );
            0
        });
    if to_delete > 0 {
        if segment.is_receipts() {
            writer.prune_receipts(to_delete, target_block)?;
        } else {
            writer.prune_transactions(to_delete, target_block)?;
        }
    }
} else {
    warn!(
        target: "reth::providers",
        ?segment,
        target_block,
        "Block body indices not found for target block during static file pruning — \
         skipping segment pruning. This may indicate a crash during block write."
    );
}
writer.commit()?;
```

**Files:**
- `crates/storage/provider/src/providers/static_file/manager.rs`

---

## GRETH-072: validator_node_only Skips History But Doesn't Prevent Queries

**Problem:** When `validator_node_only` is set, history index writes are skipped in `persistence.rs` (line 284). However, the `StateProviderFactory` implementation in `BlockchainProvider` still accepts `history_by_block_number` and `state_by_block_number_or_tag(Number(n))` calls (line 556), which will query the (empty) history indices and return stale or missing data without any error indication.

**Fix:** In the `BlockchainProvider` methods that serve historical state queries, check `validator_node_only` and return an explicit error when historical state is not available.

**Reference Code:**
```rust
// before (crates/storage/provider/src/providers/blockchain_provider.rs:565-576)
fn history_by_block_number(
    &self,
    block_number: BlockNumber,
) -> ProviderResult<StateProviderBox> {
    trace!(target: "providers::blockchain", ?block_number, "Getting history by block number");
    let provider = self.consistent_provider()?;
    provider.ensure_canonical_block(block_number)?;
    let hash = provider
        .block_hash(block_number)?
        .ok_or_else(|| ProviderError::HeaderNotFound(block_number.into()))?;
    provider.into_state_provider_at_block_hash(hash)
}

// after
fn history_by_block_number(
    &self,
    block_number: BlockNumber,
) -> ProviderResult<StateProviderBox> {
    trace!(target: "providers::blockchain", ?block_number, "Getting history by block number");

    // GRETH-072: Validator-only nodes do not write history indices.
    // Reject historical queries to prevent returning stale/empty data.
    if get_gravity_config().validator_node_only {
        let best = self.database.best_block_number()?;
        if block_number < best {
            return Err(ProviderError::StateForNumberNotFound(block_number));
        }
    }

    let provider = self.consistent_provider()?;
    provider.ensure_canonical_block(block_number)?;
    let hash = provider
        .block_hash(block_number)?
        .ok_or_else(|| ProviderError::HeaderNotFound(block_number.into()))?;
    provider.into_state_provider_at_block_hash(hash)
}
```

**Files:**
- `crates/storage/provider/src/providers/blockchain_provider.rs`

---

## GRETH-073: Potential Arithmetic Underflow in Static File Pruning

**Problem:** At line 1096, `highest_tx - block.last_tx_num()` can underflow if `highest_tx` is less than `block.last_tx_num()`. This can happen after a crash when the static file tx count is behind the database tx indices. On debug builds this panics; on release builds it wraps to `u64::MAX`, causing an attempt to delete an enormous number of entries.

**Fix:** Use `checked_sub` and skip pruning (with a warning) if underflow would occur. This is already addressed in the combined fix in GRETH-071 above. Shown separately for clarity.

**Reference Code:**
```rust
// before (crates/storage/provider/src/providers/static_file/manager.rs:1095-1096)
let highest_tx = self.get_highest_static_file_tx(segment).unwrap_or_default();
let to_delete = highest_tx - block.last_tx_num();

// after
let highest_tx = self.get_highest_static_file_tx(segment).unwrap_or_default();
let to_delete = match highest_tx.checked_sub(block.last_tx_num()) {
    Some(n) => n,
    None => {
        warn!(
            target: "reth::providers",
            ?segment,
            highest_tx,
            last_tx_num = block.last_tx_num(),
            "Arithmetic underflow in static file pruning: highest_tx < last_tx_num. \
             Skipping pruning for this segment."
        );
        0
    }
};
```

**Files:**
- `crates/storage/provider/src/providers/static_file/manager.rs`

**Review Comments** reviewer: neko; state: accepted; comments: @Ashin Gau Straightforward checked_sub fix to prevent arithmetic underflow after crash recovery. Already covered by the combined GRETH-071 fix but worth tracking separately for clarity.

---

## GRETH-074: filter_invalid_txs Uses Effective Gas Price Instead of Max Fee

**Problem:** The `is_tx_valid` closure in `filter_invalid_txs` computes `gas_spent` using `tx.effective_gas_price(Some(base_fee_per_gas))`, which for EIP-1559 transactions yields `min(max_fee_per_gas, base_fee_per_gas + max_priority_fee_per_gas)`. This is less than or equal to `max_fee_per_gas`. The EVM actually reserves `max_fee_per_gas * gas_limit` from the sender's balance during execution, not the effective price. Using the effective price here means the pre-filter may pass a transaction that the EVM will later reject for insufficient balance, defeating the purpose of the filter.

**Fix:** Use `max_fee_per_gas` (falling back to `gas_price` for legacy transactions) for the balance check, matching what the EVM actually reserves.

**Reference Code:**
```rust
// before (crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:1250-1252)
let gas_spent = U256::from(tx.effective_gas_price(Some(base_fee_per_gas)))
    .saturating_mul(U256::from(tx.gas_limit()));
let total_spent = gas_spent.saturating_add(tx.value());

// after
// GRETH-074: Use max_fee_per_gas for the balance reservation check.
// The EVM reserves max_fee_per_gas * gas_limit, not effective_gas_price * gas_limit.
// For legacy transactions, max_fee_per_gas == gas_price.
let max_fee = tx.max_fee_per_gas().unwrap_or_else(|| tx.gas_price());
let gas_reserved = U256::from(max_fee)
    .saturating_mul(U256::from(tx.gas_limit()));
let total_spent = gas_reserved.saturating_add(tx.value());
```

Also update the warning message variable name for clarity:
```rust
// before
if account.balance < total_spent {
    warn!(target: "filter_invalid_txs",
        tx_hash=?tx.hash(),
        sender=?sender,
        balance=?account.balance,
        gas_spent=?gas_spent,
        transfer_value=?tx.value(),
        "insufficient balance"
    );

// after
if account.balance < total_spent {
    warn!(target: "filter_invalid_txs",
        tx_hash=?tx.hash(),
        sender=?sender,
        balance=?account.balance,
        gas_reserved=?gas_reserved,
        transfer_value=?tx.value(),
        "insufficient balance"
    );
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

---

## GRETH-075: Schema Version Warning Without Action

**Problem:** In `RelayerState::load()`, when the persisted schema version does not match `SCHEMA_VERSION`, only a `warn!` is emitted. The function then proceeds to return the deserialized state, which may have an incompatible format. If the schema changes in an incompatible way (e.g., renamed fields, changed types), this could silently corrupt the relayer's understanding of its state.

**Fix:** Fail on major version mismatch (different major version). For minor version differences (future-proofing), allow loading with a warning. Since we are currently at version 1 and the version is a simple `u32`, treat any mismatch as a hard error that resets state.

**Reference Code:**
```rust
// before (crates/pipe-exec-layer-ext-v2/relayer/src/persistence.rs:53-65)
pub fn load(path: &Path) -> Result<Self> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read state file: {}", path.display()))?;

    let state: Self = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse state file: {}", path.display()))?;

    if state.version != SCHEMA_VERSION {
        warn!(
            "State file version {} differs from current version {}",
            state.version, SCHEMA_VERSION
        );
    }

    debug!("Loaded relayer state with {} sources from {}",
           state.sources.len(), path.display());

    Ok(state)
}

// after
pub fn load(path: &Path) -> Result<Self> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read state file: {}", path.display()))?;

    let state: Self = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse state file: {}", path.display()))?;

    if state.version != SCHEMA_VERSION {
        warn!(
            "State file version {} differs from current version {}. \
             Resetting relayer state to avoid loading incompatible data.",
            state.version, SCHEMA_VERSION
        );
        // GRETH-075: Return a fresh state instead of loading incompatible data.
        // The relayer will re-sync from its configured starting points.
        return Ok(Self::new());
    }

    debug!("Loaded relayer state with {} sources from {}",
           state.sources.len(), path.display());

    Ok(state)
}
```

**Files:**
- `crates/pipe-exec-layer-ext-v2/relayer/src/persistence.rs`

**Review Comments** reviewer: neko; state: pending; comments: @AlexYue Please review — resetting state on version mismatch will cause the relayer to re-sync from its configured starting points. Need to confirm this is acceptable for production deployments and that no critical cursor state would be irreversibly lost.
