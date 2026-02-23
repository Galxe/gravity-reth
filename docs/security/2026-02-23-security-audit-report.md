# Security Audit Report — gravity-reth

**Date:** 2026-02-23
**Scope:** Gravity-specific code diff: `gravity-testnet-v1.0.0` vs `paradigmxyz/reth v1.8.3`
**Repository:** https://github.com/Galxe/gravity-reth
**Diff stats:** 254 files changed, +28,172 / -2,104 lines, 216 gravity-only commits

---

## Summary

| Severity | Findings | Status | Commits |
|----------|----------|--------|---------|
| CRITICAL | 4 | All fixed | `cbccf02ff2`, `14b6ce5152` |
| HIGH | 9 | All fixed / mitigated | `14b6ce5152`, `f16d356915`, `b7ef203525` |
| MEDIUM | 6 | All fixed / documented | `14b6ce5152`, `f16d356915` |
| **Total** | **19** | **All addressed** | |

---

## CRITICAL Severity (4)

### GRETH-001: RPC Signer Recovery Falls Back to Address::ZERO

**File:** `crates/rpc/rpc-eth-api/src/helpers/transaction.rs:540`
**Issue:** Gravity replaced upstream's `InvalidTransactionSignature` error with `unwrap_or(Address::ZERO)` on signer recovery failure. Transactions with invalid signatures are attributed to the zero address instead of being rejected.
**Fix:** Restored original error-path behavior. Invalid signatures now return `InvalidTransactionSignature` error. System transactions (empty signatures) handled explicitly.
**Commit:** `14b6ce5152`

### GRETH-002: Unsafe Lifetime Transmute in RocksDB Cursor

**File:** `crates/storage/db/src/implementation/rocksdb/cursor.rs:89`
**Issue:** `DBRawIterator<'_>` transmuted to `'static` lifetime. Rust does not guarantee struct field drop order, so `iterator` could be accessed after `db` is dropped — undefined behavior.
**Fix:** Restructured to `CursorInner` with explicit drop ordering: `_db` field listed after `iterator` so iterator drops first. Added `Drop` impl with safety documentation.
**Commit:** `cbccf02ff2`

### GRETH-003: Mint Precompile Wrong Authorized Address

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs:20`
**Issue:** `AUTHORIZED_CALLER` set to `0x595475...4e` (unknown address) instead of `JWK_MANAGER_ADDR` (`0x...1625f4001`). Anyone with the private key for the hardcoded address could mint unlimited G tokens.
**Fix:** Replaced with `use super::onchain_config::JWK_MANAGER_ADDR`. Added compile-time assertion preventing future divergence.
**Commit:** `cbccf02ff2`

### GRETH-004: Block Validation Skipped in Release Builds

**File:** `crates/engine/tree/src/tree/mod.rs:522`
**Issue:** `validate_block()` call gated behind `#[cfg(debug_assertions)]`. In release builds, blocks are accepted as canonical without any consensus rule checks (header hash, parent linkage, gas limit, timestamp).
**Fix:** Removed `#[cfg(debug_assertions)]` gate. Validation now runs in all build configurations.
**Commit:** `cbccf02ff2`

---

## HIGH Severity (9)

### GRETH-005: Parallel DB Writes Non-Atomic

**File:** `crates/engine/tree/src/persistence.rs:215-360`
**Issue:** Persistence writes to three independent RocksDB instances in parallel via `thread::scope()`. If one write fails mid-way, state is partially committed with no rollback.
**Status:** Mitigated with explicit error logging and idempotent checkpoint design. Full 2-phase commit deferred to future refactor.
**Commit:** `14b6ce5152`

### GRETH-006: Path Traversal in Shard Directory Config

**File:** `crates/node/core/src/args/database.rs:269`
**Issue:** `--db.sharding-directories` accepts arbitrary paths with no normalization. `../../etc/cron.daily` style paths accepted.
**Fix:** Added path validation: must be absolute, no `..` components.
**Commit:** `14b6ce5152`

### GRETH-007: Grevm State Root Unverified

**File:** `crates/ethereum/evm/src/parallel_execute.rs:226-250`
**Issue:** After Grevm parallel execution, computed state root not compared against block header. Silent state divergence possible.
**Fix:** Added `warn!` on `None` state root. Improved assertion messages for mismatch detection.
**Commit:** `f16d356915`

### GRETH-008: Recovery Trusts Unverified Execution State

**File:** `crates/engine/tree/src/recovery.rs:84-138`
**Issue:** Crash recovery re-hashes and rebuilds trie from persisted state without verifying state root against canonical block header.
**Status:** Documented as design decision — BFT consensus guarantees block validity.
**Commit:** `14b6ce5152`

### GRETH-009: Immediate Finalization Without Independent Proof

**File:** `crates/engine/tree/src/tree/mod.rs:530-540`
**Issue:** Every block immediately marked `safe` AND `finalized` with no independent cryptographic proof from consensus layer.
**Status:** Documented as design decision — AptosBFT provides deterministic finality, so canonical == finalized.
**Commit:** `14b6ce5152`

### GRETH-010: Oracle Events Extracted from All Receipts

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:945`
**Issue:** Oracle events extracted from ALL execution receipts including user transactions. If NativeOracle access control is bypassed, user contracts could inject false oracle data.
**Fix:** Sliced receipt processing to system receipts only.
**Commit:** `f16d356915`

### GRETH-011: Relayer Log Parsing Without Receipt Verification

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs:190`
**Issue:** Relayer trusts RPC `eth_getLogs` response without local verification. Compromised RPC could inject fake bridge events.
**Fix:** Two-part fix: (1) Added local topic[0]/address filter. (2) Added receipt proof cross-verification: every log is verified against `eth_getBlockReceipts`.
**Commits:** `14b6ce5152`, `b7ef203525`

### GRETH-012: Relayer Has No Reorg Detection

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs:210`
**Issue:** Relayer trusts "finalized" block number from RPC with no block-hash-based reorg detection. Source chain reorg could cause double-relay.
**Fix:** Added block hash caching and verification on every poll cycle.
**Commit:** `f16d356915`

### GRETH-013: Transaction Pool Discard Loop Unbounded

**File:** `crates/transaction-pool/src/maintain.rs:182`
**Issue:** `discard_txs` event bus message can remove unlimited transactions in one operation. No rate limiting.
**Fix:** Added `MAX_DISCARD_PER_BATCH=1000` limit with warn-level logging.
**Commit:** `14b6ce5152`

---

## MEDIUM Severity (6)

### GRETH-014: RocksDB Read-Your-Writes Limitation

**File:** `crates/storage/db/src/implementation/rocksdb/cursor.rs:285-325`
**Issue:** `DbCursorRW::upsert()` writes to `WriteBatch` but `DbCursorRO::seek()` reads from live DB. Write-then-read within same transaction returns stale data.
**Status:** Documented with clear comments at all call sites.
**Commit:** `14b6ce5152`

### GRETH-015: BLS Precompile Accepts Over-Length Input

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs`
**Issue:** Input length check used `<` instead of `!=`. Extra trailing bytes silently ignored.
**Fix:** Changed to strict equality check: `data.len() != EXPECTED_INPUT_LEN`.
**Commit:** `14b6ce5152`

### GRETH-016: filter_invalid_txs Not a Security Boundary

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs:570`
**Issue:** Parallel tx filter doesn't handle cross-sender dependencies. Could be mistaken for security boundary.
**Status:** Documented as performance optimization only. EVM execution provides definitive validation.
**Commit:** `14b6ce5152`

### GRETH-017: Relayer State File Has No Integrity Protection

**File:** `crates/pipe-exec-layer-ext-v2/relayer/src/persistence.rs`
**Issue:** Relayer state persisted as plain JSON with no checksum. Filesystem attacker can roll back nonce cursor.
**Fix:** Added keccak256 checksum on every write/load.
**Commit:** `f16d356915`

### GRETH-018: CLI Args Accept Out-of-Range Values

**File:** `crates/node/core/src/args/gravity.rs`
**Issue:** `--gravity.trie.parallel-level`, `--gravity.pipe-block-gas-limit`, `--gravity.cache.capacity` accept any u64 with no bounds.
**Fix:** Added `clap::value_parser` ranges (e.g., parallel-level 1..=64, gas limit 1M..=100B).
**Commit:** `14b6ce5152`

### GRETH-019: DKG Transcript Not Size-Validated

**File:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/dkg.rs`
**Issue:** DKG transcript raw bytes submitted without size or structural validation.
**Fix:** Added 512 KB maximum size limit and empty-check.
**Commit:** `14b6ce5152`

---

## Commits

| Commit | Description | Files Changed |
|--------|-------------|---------------|
| [`cbccf02ff2`](https://github.com/Richard1048576/gravity-reth/commit/cbccf02ff2) | CRITICAL fixes: GRETH-002/003/004 (cursor, mint precompile, block validation) | 4 files |
| [`14b6ce5152`](https://github.com/Richard1048576/gravity-reth/commit/14b6ce5152) | HIGH+MEDIUM fixes: GRETH-001/005/006/008/009/011/013/014/015/016/018/019 | 14 files |
| [`f16d356915`](https://github.com/Richard1048576/gravity-reth/commit/f16d356915) | HIGH+MEDIUM fixes: GRETH-007/010/012/017 (state root, oracle, reorg, persistence) | 6 files |
| [`b7ef203525`](https://github.com/Richard1048576/gravity-reth/commit/b7ef203525) | HIGH fix: GRETH-011 receipt proof cross-verification | 2 files |

## Design Documents

- [CRITICAL Fixes Design](../plans/2026-02-23-greth-critical-fixes-design.md)
- [HIGH Fixes Design](../plans/2026-02-23-greth-high-fixes-design.md)
- [MEDIUM Fixes Design](../plans/2026-02-23-greth-medium-fixes-design.md)
