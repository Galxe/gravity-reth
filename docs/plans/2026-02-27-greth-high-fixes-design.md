# GRETH HIGH Fixes Design (Round 2)

Date: 2026-02-27

## GRETH-021: validate_execution_output Only Runs in Debug Builds

**Problem:** Critical block validation logic in `execute/src/lib.rs` (L304–L336) and its call site (L420–L423) are gated behind `#[cfg(debug_assertions)]`. In production (`--release`) builds, none of these sanity checks run: (1) `gas_limit < gas_used` — block claims more gas than allowed, (2) `gas_used != last receipt cumulative_gas_used` — inconsistent gas accounting, (3) `timestamp > now_secs * 2` — timestamp in microseconds instead of seconds. This is the same class of issue as GRETH-004 (block validation bypassed in production) but in the execution layer rather than the engine tree.

**Fix:** Remove the `#[cfg(debug_assertions)]` guard. Replace `panic!` with `error!` logging + metric counter so the node signals the anomaly without crashing in production. Alternatively, keep `unwrap_or_else` panic in debug and `error!` log in release using a runtime check.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

## GRETH-025: System Transaction Failure Silently Continues Block Execution

**Problem:** In `transact_system_txn` (`metadata_txn.rs:L198–L203`), when a system transaction execution fails (reverts), only `log_execution_error()` is called — execution continues normally. The caller in `lib.rs:L692–L696` proceeds to commit state changes from the failed transaction via `evm.db_mut().commit(metadata_state_changes.clone())` and checks its logs for epoch-change events. A reverted metadata transaction would have empty/invalid logs, causing the epoch-change check to silently skip. Additionally, `into_executed_ordered_block_result` (`metadata_txn.rs:L123–L128`) hardcodes `success: true` in the receipt regardless of actual execution outcome, making failed system transactions indistinguishable from successful ones on-chain.

**Fix:** Either (a) assert that system transactions always succeed — `assert!(result.result.is_success(), "system txn must succeed: {result:?}")` — or (b) propagate the failure as an `Err` and halt block processing. Set `receipt.success` from `result.is_success()` instead of hardcoding `true`.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs`, `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`
