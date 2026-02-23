# GRETH MEDIUM Fixes Design

Date: 2026-02-23

## GRETH-014: RocksDB Read-Your-Writes Limitation (Documentation)

**Problem:** `DbCursorRW::upsert()` appends writes to a `WriteBatch` buffer. `DbCursorRO::seek()` reads directly from the live RocksDB DB. A write-then-read sequence within the same transaction returns stale data, violating standard database semantics.

**Fix:** Documented the limitation clearly at the cursor implementation and all call sites. Added comments explaining that `WriteBatch` writes are only visible after `commit()`. Audited all call sites to ensure none rely on read-your-writes within uncommitted transactions.

**Files:** `crates/storage/db/src/implementation/rocksdb/cursor.rs`

## GRETH-015: BLS Precompile Over-Length Input Accepted

**Problem:** BLS PoP verification precompile checks `data.len() < EXPECTED_INPUT_LEN` (144 bytes) but allows arbitrarily long input. Extra trailing bytes silently ignored. Could serve as covert channel.

**Fix:** Changed to strict equality: `data.len() != EXPECTED_INPUT_LEN`. Returns `PrecompileError::Other` with descriptive message including expected and actual lengths.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs`

## GRETH-016: filter_invalid_txs Parallel Pre-Filter (Documentation)

**Problem:** `filter_invalid_txs` processes transactions per-sender in parallel using pre-block state. Cross-sender dependencies (balance transfers, shared contract state) are not visible during validation.

**Fix:** Added documentation comment clarifying this is a performance optimization filter, not a security boundary. The EVM execution definitively validates all transactions regardless of filter results. Ensured no downstream code treats the filter output as authoritative.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-017: Relayer State File No Integrity Protection

**Problem:** Relayer state (last nonce, cursor position) persisted as plain JSON with no checksum. A filesystem-level attacker can roll back `last_nonce` to cause duplicate oracle writes or advance it to skip events.

**Fix:** Added keccak256 checksum. On save, compute `keccak256(content)` and append as hex string field. On load, verify checksum matches content. Reject state file if checksum is missing or invalid, forcing fresh sync.

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/persistence.rs`

## GRETH-018: Gravity CLI Args Accept Out-of-Range Values

**Problem:** `--gravity.pipe-block-gas-limit`, `--gravity.cache.capacity`, `--gravity.trie.parallel-level` accept any u64 value. Zero gas limit causes all executions to revert. Extremely large parallel level spawns millions of threads.

**Fix:** Added `clap::value_parser` range validators:
- `pipe-block-gas-limit`: 1,000,000..=100,000,000,000
- `cache.capacity`: 1,000..=100,000,000
- `trie.parallel-level`: 1..=64

**Files:** `crates/node/core/src/args/gravity.rs`

## GRETH-019: DKG Transcript Not Size-Validated

**Problem:** DKG transcript bytes from consensus layer are passed directly to system transaction construction without size or structural validation. Oversized transcript could cause excessive gas consumption or OOM.

**Fix:** Added size validation: reject if empty or exceeds 512 KB (`MAX_DKG_TRANSCRIPT_BYTES`). Returns descriptive error message with actual vs maximum size.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/dkg.rs`
