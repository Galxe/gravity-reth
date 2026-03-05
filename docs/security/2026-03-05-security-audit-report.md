# Security Audit Report — gravity-reth (Phase 2)

**Date:** 2026-03-05
**Scope:** Gravity-specific execution pipeline, parallel EVM, state integrity, oracle relayer, GCEI protocol bridge
**Repository:** https://github.com/Galxe/gravity-reth
**Methodology:** Multi-agent parallel audit (6 specialist sub-agents + manager cross-review)
**Auditor:** Claude Opus 4.6
**Previous Audit:** [2026-02-23 Report](2026-02-23-security-audit-report.md) — 19 findings (GRETH-001 to GRETH-019), all addressed

---

## Summary

| Severity | Findings | Status |
|----------|----------|--------|
| CRITICAL | 2 | Open |
| HIGH | 14 | Open |
| MEDIUM | 21 | Open |
| LOW | 11 | Open |
| **Total** | **48** | **All open** |

This report covers findings **not** addressed in the 2026-02-23 audit. Cross-references to prior findings are noted where applicable.

---

## CRITICAL Severity (2)

### GRETH-020: Pipeline Permanent Deadlock via Barrier Timeout Gap

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:472,492,1101`, `channel.rs:94`
**Issue:** Three of four pipeline barriers (`merklize_barrier`, `seal_barrier`, `make_canonical_barrier`) use `Channel::wait()` with **no timeout**. Only `execute_block_barrier` uses `wait_timeout(2s)`. If any block's `process()` task panics (via `assert!`, `unwrap()`, or `unimplemented!()`), downstream barriers wait forever on a oneshot channel that will never receive. The `JoinHandle` from `tokio::spawn` (line 269) is discarded, so panics are silently swallowed.
**Panic sources in process():** `assert!(epoch == ...)` (line 397), `assert_eq!(execute_height...)` (line 457), `panic!("failed to execute block")` (line 1000), `unwrap()` on channel (line 518).
**Impact:** A single panic in any block permanently freezes the entire node. All subsequent blocks hang on merklize/seal/make_canonical barriers indefinitely. This is a persistent DoS that survives until manual restart.
**Recommendation:** Add `wait_timeout` to all barriers. Monitor `JoinHandle` for panics. Implement circuit breaker to trigger graceful shutdown on task failure.

### GRETH-021: Cache Eviction of Unpersisted Trie Nodes

**Files:** `crates/storage/storage-api/src/cache.rs:209-230`, `crates/trie/parallel/src/nested_hash.rs:80-88`
**Issue:** The `PersistBlockCache` eviction daemon computes `eviction_height = (persist_height + last_state_eviction_height) / 2`. When `last_state_eviction_height > persist_height` (from a previous cycle), `eviction_height > persist_height`. Trie cache entries (account_trie, storage_trie) at block numbers between `persist_height` and `eviction_height` are evicted despite not being persisted. Merklization falls back to DB which only has data up to `persist_height`, producing incorrect state roots.
**Impact:** Silent state root divergence between validators (if eviction timing differs) or between the node and the network, causing consensus failure. No error or panic — wrong state root is computed silently.
**Recommendation:** Cap `eviction_height` at `persist_height`: `eviction_height = min(midpoint, persist_height)`.

---

## HIGH Severity (14)

### GRETH-022: BLOCKHASH Opcode Unimplemented — Validator DoS

**File:** `crates/gravity-storage/src/block_view_storage/mod.rs:150-152,210-212`
**Issue:** `block_hash_ref()` calls `unimplemented!()` on both `RawBlockViewProvider` and `BlockViewProvider`. Any user transaction using the BLOCKHASH opcode (0x40) triggers a panic. No `catch_unwind` in the execution path. Combined with GRETH-020, this causes permanent pipeline deadlock.
**Impact:** Trivially exploitable DoS — any user can submit a contract calling BLOCKHASH. Cost: one transaction fee.
**Recommendation:** Implement `block_hash_ref()` using a ring buffer of the last 256 block hashes.

### GRETH-023: Token Loss During Epoch Transitions

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:682-701,758-779,797-821`
**Issue:** `execute_system_transactions()` creates a separate `ParallelState` (`state_for_precompile`) for the mint precompile. On epoch-change early return paths (lines 682 and 758), `inner_state.take_bundle()` is returned **without** extracting precompile state. The precompile merge block (lines 797-821) is only reached on the normal (non-epoch-change) path. Minted tokens in the dropped `state_for_precompile` are permanently lost.
**Impact:** Native tokens minted in the epoch-change block are silently lost. Low probability (requires mint + epoch change in same block) but no recovery mechanism.
**Recommendation:** Extract precompile state before every early return path.

### GRETH-024: Mint Precompile Parallel State Divergence

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:639-648`, `mint_precompile.rs:136`
**Issue:** Three distinct state objects exist during system transaction execution: `inner_state` (EVM), `state_for_precompile` (mint), and later the Grevm `ParallelState`. The mint precompile writes to `state_for_precompile`, invisible to the EVM's `inner_state`. If the system transaction that triggers minting also reads the recipient's balance, it sees the pre-mint value.
**Impact:** Incorrect balance visibility during system transaction execution. Potential state inconsistency if system contract logic depends on post-mint balance.

### GRETH-025: Cache-DB Consistency Gap During Merklization

**Files:** `crates/gravity-storage/src/block_view_storage/mod.rs:58-61`, `crates/trie/parallel/src/nested_hash.rs:80-88`
**Issue:** `state_root()` obtains a read-only DB transaction via `database_provider_ro().into_tx()` without a RocksDB snapshot. The DB state reflects the persist frontier (height P), while the cache contains data up to merge height M > P. Cache misses fall back to DB at height P. If GRETH-021's eviction removes cache entries, the fallback returns stale data from a different block height.
**Impact:** Incorrect state root computation when cache entries are evicted before persistence catches up.

### GRETH-026: Read-Only Transactions See Inconsistent Cross-DB State

**Files:** `crates/storage/db/src/implementation/rocksdb/tx.rs:264-326`, `block_view_storage/mod.rs:58-61`
**Issue:** The three-database RocksDB design (state_db, account_db, storage_db) means a `Tx<RO>` reads from three independent DB instances without coordinated snapshots. During persistence, account_db might be at height H while storage_db is at H-1. A cache miss during merklization could return trie nodes from different heights.
**Note:** Extends GRETH-005 (non-atomic parallel DB writes) with the read-path consequence.
**Impact:** Inconsistent DB view during merklization if cache misses occur. Mitigated by cache overlay in normal operation.

### GRETH-027: Unbounded Channels Between Consensus and Execution

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:1369-1373`
**Issue:** All three inter-layer channels (`ordered_block_tx/rx`, `execution_result_tx/rx`, `discard_txs_tx/rx`) are `tokio::sync::mpsc::unbounded_channel()`. No backpressure from execution to consensus. Under sustained load or during catch-up, blocks accumulate without bound in the ordered_block channel. Each queued block holds a `Vec<TransactionSigned>` potentially containing thousands of transactions.
**Impact:** OOM risk under sustained high load or adversarial block conditions. The 1 Gigagas block limit amplifies memory pressure.
**Recommendation:** Replace with `bounded_channel(32-64)`.

### GRETH-028: Type-Erased Event Bus Singleton with Panicking Downcast

**File:** `crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs:15-29`
**Issue:** `PIPE_EXEC_LAYER_EVENT_BUS` stores `Box<dyn Any + Send + Sync>`. Retrieval via `downcast_ref::<PipeExecLayerEventBus<N>>().unwrap()` panics if the generic type `N` at initialization doesn't match retrieval. No compile-time guarantee of type matching.
**Impact:** Runtime panic if a refactor changes the node primitives type on either side.

### GRETH-029: Thread-Blocking Busy-Wait in Event Bus Access

**File:** `crates/pipe-exec-layer-ext-v2/event-bus/src/lib.rs:17-29`
**Issue:** `get_pipe_exec_layer_event_bus()` uses `std::thread::sleep(Duration::from_secs(1))` in a loop with no maximum retry count or timeout. Blocks the OS thread entirely. If the event bus is never initialized, this loops forever.
**Impact:** Permanent thread hang if initialization fails. If called from a tokio async context (currently not, but no guard), would block a worker thread.

### GRETH-030: No Cryptographic Verification for Cross-Chain Oracle Data

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs:243`
**Issue:** The oracle relayer trusts `eth_getLogs` responses from the configured RPC endpoint with zero cryptographic proof that the logs exist on the source chain. No Merkle proof verification of log inclusion in blocks. No block header validation against source chain consensus. The fix for GRETH-011 added local topic/address filtering and receipt cross-verification, but this only verifies internal RPC consistency — a compromised RPC can serve internally-consistent fake data.
**Note:** Extends GRETH-011 fix scope — receipt proof verifies the RPC's own data, not the source chain's data.
**Impact:** If validators share an RPC provider and it is compromised, fake cross-chain messages could be finalized by quorum.

### GRETH-031: Zero-Signature System Transactions Bypass All Validation

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs:209-227`, `mod.rs:175-193`
**Issue:** System transactions use `Signature::new(U256::ZERO, U256::ZERO, false)` with no chain_id and are attributed to `SYSTEM_CALLER` via `Recovered::new_unchecked()`. Security relies entirely on on-chain `SystemAccessControl` modifier. No allowlist of target contract addresses at the transaction construction layer.
**Impact:** Full system compromise if system transaction construction can be influenced by external input. The `SYSTEM_CALLER` can update oracle data, trigger epoch transitions, finish DKG, and modify validator sets.

### GRETH-032: Duplicate `new_system_call_txn()` Definitions

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs:209-227`, `mod.rs:175-193`
**Issue:** Identical function defined in two locations. If one is updated without the other (e.g., gas limit change), system transactions would behave inconsistently, causing consensus failures between validators running different code versions.

### GRETH-033: `failedProposerIndices` Always Empty

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs:249-250`
**Issue:** The `onBlockStart` call always passes `failedProposerIndices: vec![]`. If the `Blocker.sol` contract uses this for slashing or reward distribution, the functionality is completely non-operative.
**Impact:** No proposer slashing — validators that skip proposal slots face no consequences.

### GRETH-034: Nonce Truncation u128 to u64

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/oracle_state.rs:148`, `oracle_manager.rs:303`, `jwk_consensus_config.rs:111`
**Issue:** Oracle nonces are `u128` internally but silently truncated to `u64` via `as u64` cast. Values above `u64::MAX` wrap around silently, potentially causing the relayer to re-process already-committed events.

---

## MEDIUM Severity (21)

### GRETH-035: Fire-and-Forget tokio::spawn — Panic Propagation Gap

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:269-273`
**Issue:** `JoinHandle` from `tokio::spawn` discarded. Panics in `process()` are silently swallowed by tokio runtime.
**Note:** Root cause of GRETH-020. Listed separately as it is independently fixable.

### GRETH-036: Unbounded Task Accumulation in PipeExecService::run()

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:244-276`
**Issue:** No concurrency limit on spawned `process()` tasks. Each waiting task holds its `ReceivedBlock` (with full transaction list) in memory while waiting on barriers.

### GRETH-037: Non-Atomic Trie Cache Writes

**File:** `crates/storage/storage-api/src/cache.rs:525-579`
**Issue:** `write_trie_updates()` performs individual `DashMap` operations without a transaction. Concurrent readers could observe partial trie updates.

### GRETH-038: wait_persist_gap Timeout Allows Unbounded Cache Growth

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:402`, `crates/storage/storage-api/src/cache.rs:251-281`
**Issue:** The 2-second timeout in `wait_persist_gap(Some(2000))` allows execution to proceed when persistence is behind. Cache grows unboundedly, amplifying GRETH-021's eviction risk.

### GRETH-039: Mint Precompile DELEGATECALL Risk

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs:20,77`
**Issue:** Caller validation uses `input.caller`. In DELEGATECALL context, `input.caller` reflects the original `msg.sender`, not the executing contract. If the authorized caller contract has a DELEGATECALL vulnerability, an attacker could mint through it.
**Note:** Different from GRETH-003 (wrong address). The address was fixed; this is about the DELEGATECALL semantic.

### GRETH-040: No Total Supply Cap on Minting

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs:118-132`
**Issue:** Individual mints capped at `u128::MAX` but no cumulative total supply cap. Compromised authorized caller could mint unlimited tokens.

### GRETH-041: Precompile State Merge Loses Original Storage Values

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:802-821`
**Issue:** Storage slots from precompile `BundleState` are converted using `present_value` only; original values discarded. This can cause incorrect gas refunds for `SSTORE` and incorrect state diffs for trie updates.

### GRETH-042: System Transaction State Merge Overwrites via HashMap::insert

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:753-756,802-821`
**Issue:** `HashMap::insert` replaces entire account entry. If mint precompile modifies the balance of an account also touched by a metadata/validator transaction, the storage updates from the system transaction are lost.
**Recommendation:** Use deep-merge semantics (merge storage maps, take latest AccountInfo).

### GRETH-043: Block Hash Verification Bypassed with None

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:533-535`
**Issue:** When `block_hash` is `None`, the `assert_eq!` verification is skipped entirely. The `None` path exists for genesis/bootstrap but has no guard preventing it for post-genesis blocks.

### GRETH-044: Block Hash Mismatch Causes Deliberate Node Panic

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:534`
**Issue:** `assert_eq!(executed_block_hash, block_hash)` — deliberate panic on hash mismatch. Correct for consensus safety but means any non-determinism bug causes all nodes to crash simultaneously.

### GRETH-045: wait_for_block_persistence Blocks Indefinitely

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:1332-1341`
**Issue:** `rx.await` with no timeout. If persistence never completes (disk full, I/O error), the commit loop stalls permanently. The epoch change does not complete. Consensus hangs.

### GRETH-046: Unsafe Send Impl for MutexGuard Wrapper

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/channel.rs:54-58`
**Issue:** `unsafe impl Send for SendMutexGuard`. Safety depends on a code invariant (guard dropped before `.await`) not enforced by the type system. A future edit introducing `.await` while holding the guard causes undefined behavior.

### GRETH-047: Channel Timeout-Notify Race Leaves Orphaned Entries

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/channel.rs:77-96,108-116`
**Issue:** When a task times out and abandons processing, the `State::Notified` entry remains in the channel's `HashMap` — a slow memory leak proportional to the number of abandoned blocks.

### GRETH-048: Atomic Ordering Gap Between Epoch and Execute Height

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:238-239,455-457`
**Issue:** Two independent `AtomicU64` values (`epoch`, `execute_height`) lack cross-variable ordering guarantees. Currently safe because barriers provide true ordering, but fragile under refactoring.

### GRETH-049: No TLS Certificate Pinning for RPC Connections

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/eth_client.rs:58`
**Issue:** HTTP client uses `use_rustls_tls()` without certificate pinning. Vulnerable to TLS interception by compromised CA.

### GRETH-050: is_unsupported_jwk() Uses Type Name Parsing

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/jwk_oracle.rs:62-66`
**Issue:** Determines JWK type by checking if `type_name` parses as `u32`. Any new oracle source with a numeric type name would be misrouted.

### GRETH-051: expect()/unwrap() on ABI Decode in Config Fetchers

**Files:** `onchain_config/epoch.rs:65-66`, `dkg.rs:134-135`, `validator_set.rs:54-55,72-73,92-93`
**Issue:** `expect()` on ABI decode operations. Malformed contract response crashes the node instead of allowing graceful error handling.

### GRETH-052: OracleRelayerManager::new() Panics on None

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/oracle_manager.rs:134`
**Issue:** Constructor accepts `Option<PathBuf>` but unconditionally `unwrap()`s it. `OracleRelayerManager::default()` always panics.

### GRETH-053: Relaxed Memory Ordering on Cursor Atomics

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs:157,206`
**Issue:** `cursor` AtomicU64 uses `Ordering::Relaxed` for all loads/stores. No visibility ordering guarantee with respect to the `last_processed` Mutex-protected state.

### GRETH-054: fromBlock Defaults to 0 When Parameter Missing

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/uri_parser.rs:52-54`
**Issue:** Missing `fromBlock` parameter causes the relayer to scan from block 0, potentially millions of blocks. Extreme RPC load and delayed startup.

### GRETH-055: Voting Power Conversion Precision Loss

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/dkg.rs:265-268`, `types.rs:98,131`
**Issue:** Voting power divided by 10^18 (wei→ether) then truncated to u64. Validators with sub-ether voting power are silently zeroed out. If total exceeds `u64::MAX` ethers, `.to::<u64>()` panics.

---

## LOW Severity (11)

### GRETH-056: BLS Precompile Gas Underpricing

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs:22`
**Issue:** `POP_VERIFY_GAS = 45,000` — roughly 23% of EIP-2537 equivalent cost (~193,000 gas). Enables computational DoS.

### GRETH-057: Mint Precompile Gas Underpricing

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs:26`
**Issue:** `MINT_BASE_GAS = 21,000` — same as a simple transfer but performs additional work (mutex, state load, balance modification).

### GRETH-058: WrapDatabaseRef Pre-Execution State Loss

**File:** `crates/ethereum/evm/src/parallel_execute.rs:73-86`
**Issue:** `WrapDatabaseRef(state)` provides read-only access. State changes from `apply_pre_execution_changes` (e.g., beacon root update) cached in EVM's `JournaledState` may not be committed back to `ParallelState`.

### GRETH-059: Panic on make_canonical Failure

**File:** `crates/engine/tree/src/tree/mod.rs:541-545`
**Issue:** Any error from `make_canonical` (including transient RocksDB I/O errors) causes immediate panic. No retry mechanism.

### GRETH-060: Parallel State/Trie Persistence Ordering

**File:** `crates/engine/tree/src/persistence.rs:225-346`
**Issue:** State and trie writes happen in separate threads with separate commits. If trie commits before state, another thread could see new trie nodes but old state data. Mitigated by cache overlay.

### GRETH-061: Recovery Does Not Handle Execution Checkpoint Corruption

**File:** `crates/engine/tree/src/recovery.rs:113-142`
**Issue:** Recovery uses `StageId::Execution` checkpoint as ground truth. If corrupted, recovery operates at wrong block number.

### GRETH-062: Static File Pruning Assumes Block Body Indices Exist

**File:** `crates/storage/provider/src/providers/static_file/manager.rs:1094-1096`
**Issue:** If `block_body_indices(target_block)` returns `None` in crash scenarios, pruning for transaction-based segments is silently skipped.

### GRETH-063: validator_node_only Skips History But Doesn't Prevent Queries

**Files:** `crates/engine/tree/src/persistence.rs:284`, `crates/storage/provider/src/providers/blockchain_provider.rs:510-531`
**Issue:** Validator nodes skip history index writes but don't reject historical queries. RPC responses may be empty/incorrect without an explicit error.

### GRETH-064: Potential Arithmetic Underflow in Static File Pruning

**File:** `crates/storage/provider/src/providers/static_file/manager.rs:1093-1096`
**Issue:** `highest_tx - block.last_tx_num()` could underflow on inconsistent data, producing astronomically large prune count.

### GRETH-065: filter_invalid_txs Uses Effective Gas Price Instead of Max Fee

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:1227-1241`
**Issue:** Balance check uses `effective_gas_price` (min of max_fee and base_fee + priority_fee) instead of `max_fee_per_gas`. Filter is less strict than the EVM's actual validation, allowing some insufficient-balance transactions through to parallel execution.

### GRETH-066: Schema Version Warning Without Action

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/persistence.rs:60-63`
**Issue:** Mismatched state file schema version only produces a warning. Incompatible state data loaded regardless, potentially causing incorrect cursor/nonce restoration.

---

## Architectural Recommendations

### R-01: Add Timeouts to ALL Pipeline Barriers (addresses GRETH-020)

Apply `wait_timeout(Duration::from_secs(30))` to `merklize_barrier`, `seal_barrier`, and `make_canonical_barrier`. On timeout, trigger graceful shutdown rather than infinite hang.

### R-02: Protect Cache from Premature Eviction (addresses GRETH-021, GRETH-025)

Cap eviction height: `eviction_height = min(midpoint, persist_height)`. Never evict trie entries above the persist frontier.

### R-03: Implement block_hash_ref (addresses GRETH-022)

Maintain a ring buffer of the last 256 block hashes in `BlockViewStorage`. Populate from sealed blocks.

### R-04: Extract Precompile State Before Epoch Change Returns (addresses GRETH-023)

Add precompile state extraction before every early return in `execute_system_transactions()`.

### R-05: Deep-Merge System Transaction State (addresses GRETH-042)

Replace `HashMap::insert` with entry-based deep merge: merge storage maps, take latest `AccountInfo`.

### R-06: Add Cryptographic Verification for Oracle Data (addresses GRETH-030)

Options: (1) Multi-RPC cross-checking, (2) Merkle proof verification via `eth_getProof`, (3) Source chain light client.

### R-07: Replace Unbounded Channels with Bounded (addresses GRETH-027)

Use `bounded_channel(32-64)` for `ordered_block_tx/rx` to provide backpressure to consensus.

### R-08: Remove Duplicate new_system_call_txn() (addresses GRETH-032)

Consolidate to single definition in `mod.rs`. Have `metadata_txn.rs` use `super::new_system_call_txn()`.

---

## Cross-Reference to Prior Audit

| Prior Finding | Status | Related New Finding |
|---|---|---|
| GRETH-005 (Non-atomic DB writes) | Mitigated | GRETH-026 (read-path consequence) |
| GRETH-011 (Relayer log parsing) | Fixed | GRETH-030 (broader: no cryptographic proof) |
| GRETH-014 (Read-your-writes) | Documented | GRETH-026 (cross-DB inconsistency) |
| GRETH-017 (State file integrity) | Fixed | GRETH-066 (schema version handling) |

---

*Report generated by Claude Opus 4.6 multi-agent audit framework.*
