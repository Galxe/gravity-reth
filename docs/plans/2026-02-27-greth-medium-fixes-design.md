# GRETH MEDIUM Fixes Design (Round 2)

Date: 2026-02-27

## GRETH-020: Mint Precompile Accepts Trailing Bytes

**Problem:** `mint_precompile.rs:L91` uses `input.data.len() < EXPECTED_LEN` instead of strict equality `!=`. This allows extra trailing bytes to be silently ignored. Inconsistent with the BLS precompile which was fixed to use strict equality in GRETH-015. Trailing bytes could serve as a covert data channel or cause confusion during forensic analysis of on-chain transactions.

**Fix:** Change the length check from `<` to `!=`, matching the BLS precompile fix pattern: `if input.data.len() != EXPECTED_LEN { return Err(...) }`. Update the error message to include both expected and actual lengths.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs`

## GRETH-022: Unsafe `Send` Impl on MutexGuard Wrapper in Channel

**Problem:** `channel.rs:L57–L58` defines `struct SendMutexGuard<'a, T>(MutexGuard<'a, T>)` with `unsafe impl<'a, T> Send for SendMutexGuard<'a, T> {}`. The safety comment claims `.await` will not occur within the critical zone, but this invariant is not compiler-enforced. `MutexGuard` is deliberately `!Send` because holding a lock across `.await` points can cause deadlocks (the tokio runtime may resume the task on a different thread). A future refactor that adds `.await` while the guard is held would silently compile but introduce undefined behavior or deadlocks.

**Fix:** Replace `std::sync::Mutex` with `tokio::sync::Mutex` for `Channel.inner`, which yields a `Send`-safe guard natively. Alternatively, restructure the code so the `MutexGuard` is always dropped before any `.await` point without needing the unsafe workaround — the current code already does this, but the unsafe shim is fragile against future changes.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/channel.rs`

## GRETH-023: OracleRelayerManager::new Panics on None Datadir

**Problem:** `oracle_manager.rs:L134` calls `datadir.unwrap()` on an `Option<PathBuf>` parameter. The function signature `fn new(datadir: Option<PathBuf>) -> Self` advertises that `datadir` is optional, but the implementation panics if `None` is passed. This can crash the node at startup if the relayer is configured without a data directory. The `Default` impl at L122–L126 calls `Self::new(None)`, guaranteeing a panic.

**Fix:** Either (a) change the parameter type to `PathBuf` (removing the `Option`) and fix all call sites including `Default`, or (b) handle `None` gracefully by defaulting to a temporary directory or disabling persistence.

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/oracle_manager.rs`
