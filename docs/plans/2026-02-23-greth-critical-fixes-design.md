# GRETH CRITICAL Fixes Design

Date: 2026-02-23

## GRETH-001: RPC Signer Recovery Address::ZERO Fallback

**Problem:** Gravity replaced upstream's `InvalidTransactionSignature` error with `unwrap_or(Address::ZERO)`. Any transaction with an invalid signature is silently attributed to the zero address. Block explorers, indexers, and wallets display wrong sender. Both `transaction.rs` and `receipt.rs` affected.

**Fix:** Restore original error path: `try_into_recovered_unchecked().map_err(|_| EthApiError::InvalidTransactionSignature)?`. For meta-transactions (system transactions with empty signatures), add explicit type-check that returns `SYSTEM_CALLER` instead of zero address.

**Files:** `crates/rpc/rpc-eth-api/src/helpers/transaction.rs`, `crates/rpc/rpc-eth-api/src/helpers/receipt.rs`

## GRETH-002: RocksDB Cursor Unsafe Lifetime Transmute

**Problem:** `DBRawIterator<'_>` transmuted to `'static` to store in struct alongside `Arc<DB>`. The SAFETY comment claims Arc<DB> outlives iterator, but Rust doesn't guarantee field drop order. If `db` drops before `iterator`, use-after-free occurs.

**Fix:** Restructure cursor to use `CursorInner` that holds `_db: Arc<DB>` and `iterator: DBRawIterator<'static>` together with explicit field ordering. The `iterator` field is listed before `_db` so Rust drops it first (fields drop in declaration order). Add documentation explaining the safety invariant.

**Files:** `crates/storage/db/src/implementation/rocksdb/cursor.rs`

## GRETH-003: Mint Precompile Authorized Address Mismatch

**Problem:** `AUTHORIZED_CALLER` is `0x595475934ed7d9faa7fca28341c2ce583904a44e` — an unknown EOA. The actual JWK Manager system address is `0x00000000000000000000000000000001625f4001` (defined in `onchain_config/mod.rs` as `JWK_MANAGER_ADDR`). Neither matches the comment's "0x2018". Anyone with the private key to the hardcoded address can mint unlimited G tokens.

**Fix:** Replace the hardcoded constant with `use super::onchain_config::JWK_MANAGER_ADDR; pub const AUTHORIZED_CALLER: Address = JWK_MANAGER_ADDR;`. Add compile-time assertion: `const _: () = assert!(AUTHORIZED_CALLER.0 == JWK_MANAGER_ADDR.0);` to prevent future divergence.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs`

## GRETH-004: Block Validation Gated on debug_assertions

**Problem:** `validate_block()` in `make_executed_block_canonical()` is wrapped in `#[cfg(debug_assertions)]`. In release builds (`--release`), the entire validation call is compiled out. Blocks are accepted as canonical without header hash verification, parent hash linkage, timestamp monotonicity, or gas limit checks.

**Fix:** Remove the `#[cfg(debug_assertions)]` attribute. Validation runs in all build configurations. Change `panic!` on failure to `error!` log + proper error propagation to avoid crashing the node on validation failure.

**Files:** `crates/engine/tree/src/tree/mod.rs`
