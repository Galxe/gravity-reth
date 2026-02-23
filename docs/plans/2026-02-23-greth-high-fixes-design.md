# GRETH HIGH Fixes Design

Date: 2026-02-23

## GRETH-005: Parallel Persistence Non-Atomic Multi-DB Commit

**Problem:** Persistence writes to three independent RocksDB instances (`state_db`, `account_trie_db`, `storage_trie_db`) in parallel. If one write fails, state is partially committed with no rollback mechanism.

**Fix (Mitigation):** Add explicit error logging on partial write failure. Ensure checkpoint advancement is atomic — only written after all three DB writes succeed. Design is idempotent: recovery re-applies all writes from the checkpoint. Full 2-phase commit deferred to future refactor.

**Files:** `crates/engine/tree/src/persistence.rs`

## GRETH-006: Path Traversal in RocksDB Shard Directory Config

**Problem:** `--db.sharding-directories` accepts arbitrary paths including `../../etc/cron.daily`. No normalization or bounds checking.

**Fix:** Validate shard paths at CLI parse time. Require absolute paths. Reject any path component that is `..` (ParentDir). Log validated paths at startup for operator visibility.

**Files:** `crates/node/core/src/args/database.rs`, `crates/storage/db/src/implementation/rocksdb/mod.rs`

## GRETH-007: Grevm State Root Unverified After Parallel Execution

**Problem:** After Grevm parallel execution, the computed post-execution state root is not compared against the block header's `state_root`. If Grevm has a parallel dependency detection bug, the node silently diverges.

**Fix:** Add `warn!` logging when state root is `None` (not computed). Improve assertion messages for mismatch cases so operators can detect divergence.

**Files:** `crates/ethereum/evm/src/parallel_execute.rs`

## GRETH-008: Crash Recovery Trusts Unverified State (Design Decision)

**Problem:** Recovery re-hashes and rebuilds trie from persisted state without verifying against canonical block header state root.

**Decision:** Documented as acceptable under BFT consensus model. AptosBFT guarantees that all blocks delivered to execution layer are final and valid. Adding state root verification on recovery would require full state root computation which is expensive. Added documentation explaining the trust model.

**Files:** `crates/engine/tree/src/recovery.rs`

## GRETH-009: Immediate Finalization Without Separate Signal (Design Decision)

**Problem:** Every block is immediately marked `safe` AND `finalized` with no separate finalization signal from consensus.

**Decision:** Documented as intentional. AptosBFT provides deterministic finality — a block delivered via `push_ordered_block()` has already achieved 2f+1 consensus votes. Separating canonical/finalized would add complexity without security benefit under this consensus model.

**Files:** `crates/engine/tree/src/tree/mod.rs`

## GRETH-010: Oracle Events Extracted from User Transaction Receipts

**Problem:** Oracle events extracted from ALL execution receipts including user transactions. If NativeOracle's `SYSTEM_CALLER` access control is bypassed, user contracts could inject false oracle data.

**Fix:** Changed event extraction to process only system transaction receipts (receipts at the end of the receipt list, corresponding to system transactions appended after user transactions).

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-011: Relayer Trusts RPC Log Filtering Without Verification

**Problem:** Relayer uses `eth_getLogs` with address/topic filters but does not re-verify the returned logs locally. A compromised RPC endpoint could inject fake bridge events.

**Fix (Two-part):**
1. Added local re-verification of `topic[0]` (event signature) and `address` on every returned log.
2. Added receipt proof cross-verification: for each log, fetch `eth_getBlockReceipts` and verify the log exists in the receipts by matching address, topics, and data. Logs that fail verification are dropped (fail-closed).

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs`, `crates/pipe-exec-layer-ext-v2/relayer/src/eth_client.rs`

## GRETH-012: Relayer Has No Reorg Detection

**Problem:** Relayer polls up to "finalized" block from RPC but does not verify the block hash remains stable across polls. A source chain reorg past finality could cause already-relayed events to be undone.

**Fix:** Cache block hash on each poll. On next poll, verify the cached block hash still matches. If mismatch detected, halt relayer and emit critical error alert.

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs`

## GRETH-013: Transaction Pool Discard Unbounded

**Problem:** `discard_txs` event bus can remove unlimited transactions in a single message. Any crate with event bus access can drain the entire mempool.

**Fix:** Added `MAX_DISCARD_PER_BATCH = 1000` limit. Batches exceeding the limit are truncated with `warn!` logging. Each discard operation logged at warn level with transaction count and sample hashes.

**Files:** `crates/transaction-pool/src/maintain.rs`
