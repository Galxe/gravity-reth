# GRETH MEDIUM Fixes Design

Date: 2026-02-23

## GRETH-014: RocksDB Read-Your-Writes Limitation (Documentation)

**Problem:** `DbCursorRW::upsert()` appends writes to a `WriteBatch` buffer. `DbCursorRO::seek()` reads directly from the live RocksDB DB. A write-then-read sequence within the same transaction returns stale data, violating standard database semantics.

**Fix:** Documented the limitation clearly at the cursor implementation and all call sites. Added comments explaining that `WriteBatch` writes are only visible after `commit()`. Audited all call sites to ensure none rely on read-your-writes within uncommitted transactions.

**Files:** `crates/storage/db/src/implementation/rocksdb/cursor.rs`

**Review Comments** reviewer: neko; state: accepted; comments: Accepted.

## GRETH-015: BLS Precompile Over-Length Input Accepted

**Problem:** BLS PoP verification precompile checks `data.len() < EXPECTED_INPUT_LEN` (144 bytes) but allows arbitrarily long input. Extra trailing bytes silently ignored. Could serve as covert channel.

**Fix:** Changed to strict equality: `data.len() != EXPECTED_INPUT_LEN`. Returns `PrecompileError::Other` with descriptive message including expected and actual lengths.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/bls_precompile.rs`

**Review Comments** reviewer: neko; state: accepted; comments: Accepted.

## GRETH-016: filter_invalid_txs Parallel Pre-Filter (Documentation)

**Problem:** `filter_invalid_txs` processes transactions per-sender in parallel using pre-block state. Cross-sender dependencies (balance transfers, shared contract state) are not visible during validation.

**Fix:** Added documentation comment clarifying this is a performance optimization filter, not a security boundary. The EVM execution definitively validates all transactions regardless of filter results. Ensured no downstream code treats the filter output as authoritative.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: rejected; comments: The statement "each sender's account state is a local copy of the pre-block state" does not lead to invalid transactions slipping through. On the contrary, it may cause false positives — transactions that would actually be valid during EVM execution (due to cross-sender incoming transfers) are incorrectly marked as invalid by this pre-filter. The documentation comment should be corrected to: "Cross-sender dependencies (e.g. incoming transfers from other senders in the same block) are not visible during parallel per-sender validation because each sender's account state is a local copy of the pre-block state. This means the filter may produce false positives (marking valid transactions as invalid) but will not produce false negatives (letting truly invalid transactions through)."

## GRETH-017: Relayer State File No Integrity Protection

**Problem:** Relayer state (last nonce, cursor position) persisted as plain JSON with no checksum. A filesystem-level attacker can roll back `last_nonce` to cause duplicate oracle writes or advance it to skip events.

**Fix:** Added keccak256 checksum. On save, compute `keccak256(content)` and append as hex string field. On load, verify checksum matches content. Reject state file if checksum is missing or invalid, forcing fresh sync.

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/persistence.rs`

**Review Comments** reviewer: neko; state: rejected; comments: (1) Defense against attackers is invalid — keccak256(content) is a keyless public algorithm and the code is open source. Any attacker with filesystem write access can modify last_nonce and recompute a valid checksum. Defending against a real attacker requires at minimum HMAC with a node-exclusive secret key. (2) Defense against process crashes is redundant — save() already uses a write-temp-then-rename atomic write pattern, so abnormal process exit will not produce half-written files. The checksum adds no incremental value in this scenario. (3) Defense against disk bit rot is theoretically valid but practically negligible — modern hardware ECC, mainstream filesystems (ZFS/Btrfs have data checksums; even ext4 without them), and cloud block storage end-to-end verification make the probability of "a bit flip that changes a numeric value while keeping the JSON structurally valid" approach zero.

## GRETH-018: Gravity CLI Args Accept Out-of-Range Values

**Problem:** `--gravity.pipe-block-gas-limit`, `--gravity.cache.capacity`, `--gravity.trie.parallel-level` accept any u64 value. Zero gas limit causes all executions to revert. Extremely large parallel level spawns millions of threads.

**Fix:** Added `clap::value_parser` range validators:
- `pipe-block-gas-limit`: 1,000,000..=100,000,000,000
- `cache.capacity`: 1,000..=100,000,000
- `trie.parallel-level`: 1..=64

**Files:** `crates/node/core/src/args/gravity.rs`

**Review Comments** reviewer: neko; state: accepted; comments: Accepted.

## GRETH-019: DKG Transcript Not Size-Validated

**Problem:** DKG transcript bytes from consensus layer are passed directly to system transaction construction without size or structural validation. Oversized transcript could cause excessive gas consumption or OOM.

**Fix:** Added size validation: reject if empty or exceeds 512 KB (`MAX_DKG_TRANSCRIPT_BYTES`). Returns descriptive error message with actual vs maximum size.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/dkg.rs`
