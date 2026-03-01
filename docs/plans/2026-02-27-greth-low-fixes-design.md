# GRETH LOW Fixes Design (Round 2)

Date: 2026-02-27

## GRETH-024: Nonce Truncation u128→u64 in OracleRelayerManager

**Problem:** In `oracle_manager.rs:L270`, `BlockchainEventSource::last_nonce()` returns `Option<u128>`, but `PollResult::nonce` is typed as `Option<u64>`. The implicit `as u64` cast at L270 silently truncates nonce values exceeding `u64::MAX`. At L303, the truncated `u64` is cast back to `u128` for `update_and_save_state`, potentially persisting a wrong value. While the Solidity `MessageSent` event uses `uint128` for nonce, current deployments are unlikely to reach u64::MAX, but the type mismatch may hide bugs.

**Fix:** Either (a) ensure the oracle nonce domain is guaranteed ≤ u64::MAX and document this invariant with a compile-time or runtime assertion, or (b) widen `PollResult::nonce` to `Option<u128>` throughout the API.

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/oracle_manager.rs`

**Review Comments** reviewer: neko; state: accepted; comments: The nonce originates from `blockchain_source.rs:L396-401` where it is correctly parsed as `u128` from the Solidity `MessageSent` event's indexed topic (`nonce_topic[16..32]`). It stays `u128` throughout `BlockchainEventSource` (`LastProcessedEvent.nonce`, `OracleData.nonce`) and is persisted correctly as `u128` in `SourceState.last_nonce`. The truncation occurs **solely** at the `PollResult` boundary in `oracle_manager.rs:L270` (`n as u64`) because the upstream `gravity_api_types::relayer::PollResult.nonce` field is typed `Option<u64>`. This truncated `u64` is then cast back to `u128` at L303 and fed into `update_and_save_state`, which **contaminates** the persistence layer — `SourceState.last_nonce` will hold the truncated value. On restart, `StartupScenario::determine` (L44/L47) reads this corrupted `u128` value, propagating the truncation across restarts. Fix option (b) — widening `PollResult.nonce` to `Option<u128>` — is the correct fix since the upstream `gravity-aptos` `api-types` crate owns the `PollResult` struct and needs a coordinated change.

## GRETH-026: Precompile State Merge Overwrites Instead of Deep Merging

**Problem:** In `execute/src/lib.rs:L823–L840`, precompile state changes are merged into `accumulated_state_changes` using `HashMap::insert`. If both a prior system transaction (e.g., metadata) and the mint precompile modify the same account address, `insert` replaces the entire `Account` entry. This means changes from the earlier transaction (nonce increments, storage writes) are lost for overlapping addresses. In practice, the mint precompile operates on `ParallelState` which is a separate DB snapshot, so overlapping addresses are unlikely but not impossible (e.g., if a mint targets `SYSTEM_CALLER` or a validator address that also had system txn changes).

**Fix:** Use `entry(...).and_modify(|existing| { /* merge storage slots + update info */ }).or_insert(...)` to properly deep-merge account state. Alternatively, document that the mint precompile must not modify accounts that are also touched by system transactions.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: pending; comments: @lightman The current implementation maintains two separate state tracks (EVM `inner_state` + precompile `ParallelState`) and merges them with `HashMap::insert`, which is fragile. The root cause is that the `DynPrecompile` interface cannot access the EVM's internal state, forcing the mint precompile to use a side-channel `ParallelState`. This dual-state design is too tricky and the shallow merge is a latent bug. Needs further discussion on whether we can unify the state path (e.g., via `StatefulPrecompile` or restructuring the mint logic outside the precompile interface).

## GRETH-027: GRETH-011 Cross-Verification Uses Same RPC Endpoint (Informational)

**Problem:** This is a reiteration of reviewer neko's existing rejection on GRETH-011. Both `eth_getLogs` and `eth_getBlockReceipts` calls in `blockchain_source.rs` go through the same `self.rpc_client` (same RPC endpoint). A fully compromised RPC can forge both responses to be mutually consistent, making the cross-verification ineffective against a compromised-RPC threat model.

**Recommendation:** Use a separate, independent RPC endpoint for receipt verification, or implement Merkle proof verification against a locally-validated block header. This is an architectural limitation, not a code bug.

**Files:** `crates/pipe-exec-layer-ext-v2/relayer/src/blockchain_source.rs`

**Review Comments** reviewer: neko; state: rejected; comments: Acknowledged as an architectural limitation. No adjustment planned at this time.

## GRETH-028: Block Timestamp Sanity Check Debug-Only (Subset of GRETH-021)

**Problem:** The timestamp sanity check at `lib.rs:L327–L334` detects when a block timestamp is in milliseconds or microseconds instead of seconds (by checking `timestamp > now_secs * 2`). This is critical for catching bugs in the `timestamp_us / 1_000_000` conversion (L579, L958), but is compiled out in release builds as part of the `#[cfg(debug_assertions)]` block described in GRETH-021.

**Fix:** Addressed as part of GRETH-021. When the `#[cfg(debug_assertions)]` guard is removed, this check will automatically run in production.

**Files:** `crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`

**Review Comments** reviewer: neko; state: rejected; comments: Same rationale as GRETH-021 — the checks cover critical logic bugs that should only be detected in the test environment. There is no need to enable them in production builds.
