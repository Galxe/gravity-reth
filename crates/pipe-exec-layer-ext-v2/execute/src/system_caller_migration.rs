//! Gravity Alpha hardfork — one-shot `SYSTEM_CALLER` balance migration.
//!
//! On the Alpha activation block we zero the `SYSTEM_CALLER` account balance
//! (the historical sentinel ~1.158×10⁵⁸ G allocated in genesis to cover the
//! per-block system-tx base-fee bill). With the gas-exempt design active from
//! Alpha onwards (see L1+L2 wiring in
//! `crates/ethereum/evm/src/lib.rs::transact_system_txn` and
//! `pipe-exec-layer-ext-v2/.../lib.rs::execute_system_transactions`), the
//! sentinel balance is no longer needed and would otherwise pollute total-supply
//! accounting.
//!
//! Pattern lifted verbatim from `eip_2935.rs`:
//!   - Idempotency gated by `transitions_at_timestamp(current_ts, parent_ts)` — fires exactly on
//!     the activation block, reorg-safe (Gravity has immediate finality but the predicate is robust
//!     anyway).
//!   - Routes the diff through the executor's `apply_state_change` channel, which has symmetric
//!     impls in both the serial (`WrapExecutor` → `BasicBlockExecutor`) and grevm
//!     (`GrevmExecutor::apply_state_change`) backends, so serial == grevm by construction.
//!
//! Crucially:
//!   - **nonce is preserved** (SYSTEM_CALLER auto-increments per block — clearing it would break
//!     the per-block construction sequence post-Alpha).
//!   - **code / code_hash are preserved** (defensive symmetry — the historical SYSTEM_CALLER alloc
//!     has no code, but treat the read result as ground truth so future migrations that touch a
//!     coded variant stay correct).
//!   - Only `balance` is set to `U256::ZERO`.
//!
//! With `nonce > 0`, the EIP-161 `is_empty` predicate stays false post-migration
//! and the account is never pruned by state-clear.

use alloy_primitives::U256;
use reth_chainspec::{ChainSpec, EthChainSpec, GravityHardfork, SYSTEM_CALLER};
use reth_evm::{execute::BlockExecutionError, parallel_execute::ParallelExecutor};
use reth_primitives::EthPrimitives;
use revm::state::{Account, AccountInfo, AccountStatus, EvmState};
use tracing::info;

type Executor<'a> =
    &'a mut dyn ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>;

/// Apply Gravity Alpha boundary state changes for `block_number`.
///
/// On the Alpha activation block (the unique block whose timestamp transitions
/// across `alphaTime`), zero the `SYSTEM_CALLER` balance while preserving its
/// nonce and code. No-op on every other block.
///
/// The hook reads SYSTEM_CALLER's current `AccountInfo` via the executor's
/// `ParallelExecutor::basic` accessor, so callers stay decoupled from the
/// hook's data needs and non-activation blocks pay nothing beyond the gating
/// check.
///
/// Panics on `apply_state_change` failure: in the gravity-sdk integration the
/// panic handler aborts the process, preventing partial-state corruption.
pub(crate) fn apply_state_changes_for_block(
    executor: Executor<'_>,
    chain_spec: &ChainSpec,
    current_ts: u64,
    parent_ts: u64,
    block_number: u64,
) {
    if !chain_spec
        .gravity_hardforks()
        .fork(GravityHardfork::Alpha)
        .transitions_at_timestamp(current_ts, parent_ts)
    {
        return;
    }

    // `unwrap_or_default` covers degenerate test fixtures where the genesis alloc
    // omits SYSTEM_CALLER — we still wind up writing balance=0 with nonce=0 and
    // no code, which is the natural "empty" terminal state.
    let prev = executor
        .basic(SYSTEM_CALLER)
        .expect("Alpha migration: failed to read SYSTEM_CALLER account")
        .unwrap_or_default();
    let prev_balance = prev.balance;
    let prev_nonce = prev.nonce;

    let new_info = AccountInfo {
        balance: U256::ZERO,
        nonce: prev.nonce,
        code_hash: prev.code_hash,
        code: prev.code,
    };

    let mut state_diff = EvmState::default();
    state_diff.insert(
        SYSTEM_CALLER,
        Account {
            info: new_info,
            storage: Default::default(),
            status: AccountStatus::Touched,
            transaction_id: 0,
        },
    );

    executor
        .apply_state_change(state_diff)
        .unwrap_or_else(|e| panic!("Alpha migration: SYSTEM_CALLER balance zeroing failed: {e:?}"));

    info!(target: "execute_ordered_block",
        number = block_number,
        ?prev_balance,
        prev_nonce,
        "Gravity Alpha: zeroed SYSTEM_CALLER balance (nonce/code preserved)"
    );
}

#[cfg(test)]
mod tests {
    //! Unit tests for the Gravity Alpha one-shot `SYSTEM_CALLER` balance migration
    //! (acceptance-tests-2026-06-26.md §1.4 — **must-pass**).
    //!
    //! Pins the load-bearing invariants of `apply_state_changes_for_block`:
    //!   - On the activation block (`transitions_at_timestamp(current, parent) == true`), zero
    //!     balance while preserving nonce and code.
    //!   - Re-applying on the same block is idempotent.
    //!   - Pre-/post-activation blocks are no-ops.
    //!   - With `nonce > 0`, the resulting account is **not** empty under EIP-161 and therefore not
    //!     pruned by state-clear (defends R5 verify).
    //!
    //! Backend: `WrapExecutor<BasicBlockExecutor<EthEvmConfig, CacheDB<EmptyDB>>>`.
    //! The serial path is sufficient because `apply_state_change` is the same
    //! channel both serial and grevm route through; the byte-equivalence
    //! invariant between the two backends is pinned by U-2 (existing) and U-6
    //! (gas-exempt twin) — see `crates/ethereum/evm/src/parallel_execute.rs`.
    //!
    //! Naming follows the acceptance matrix verbatim so a `rg` over the test
    //! names lines up with the §1.4 checklist row in the doc.
    use super::*;
    use alloy_consensus::constants::KECCAK_EMPTY;
    use alloy_primitives::{address, b256, Bytes, U256};
    use reth_chainspec::{ChainHardforks, ChainSpecBuilder, ForkCondition, MAINNET};
    use reth_evm::{
        execute::BasicBlockExecutor,
        parallel_execute::{ParallelExecutor, WrapExecutor},
    };
    use reth_evm_ethereum::EthEvmConfig;
    use reth_primitives::EthPrimitives;
    use revm::{
        bytecode::Bytecode,
        database::{CacheDB, EmptyDB},
        state::AccountInfo,
    };
    use std::sync::Arc;

    /// Activation timestamp used across these tests. Picked arbitrarily; the
    /// transition logic only cares about the `parent_ts < T <= current_ts`
    /// triangle.
    const ALPHA_TS: u64 = 100;

    /// Sentinel balance: ~1.158×10⁵⁸ G — the genesis-allocated number the
    /// migration is responsible for zeroing. Picked to be obviously non-zero
    /// so a regression that fails to migrate is visible at a glance.
    fn sentinel_balance() -> U256 {
        U256::from_be_bytes(
            b256!("0x1999999999999999999999999999999999999999999999999999999999999999").0,
        )
    }

    /// Non-empty bytecode used to verify that the migration preserves the
    /// existing code/code_hash on SYSTEM_CALLER. The historical alloc is
    /// codeless, but the hook reads the previous account info as ground
    /// truth — so a future coded SYSTEM_CALLER would stay coded.
    fn nonempty_code() -> Bytecode {
        Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xfd]))
    }

    /// Build a chainspec with Alpha = `Timestamp(ALPHA_TS)`.
    fn alpha_chainspec() -> Arc<reth_chainspec::ChainSpec> {
        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        spec.gravity_hardforks =
            ChainHardforks::from([(GravityHardfork::Alpha, ForkCondition::Timestamp(ALPHA_TS))]);
        Arc::new(spec)
    }

    /// Build a `WrapExecutor` with a pre-seeded SYSTEM_CALLER account.
    ///
    /// Returns the executor; the caller drives it through
    /// `apply_state_changes_for_block` directly via `&mut dyn ParallelExecutor`.
    #[allow(clippy::type_complexity)]
    fn fresh_executor(
        chain_spec: Arc<reth_chainspec::ChainSpec>,
        seed: AccountInfo,
    ) -> WrapExecutor<CacheDB<EmptyDB>, BasicBlockExecutor<EthEvmConfig, CacheDB<EmptyDB>>> {
        let evm_config = EthEvmConfig::new(chain_spec);
        let mut db = CacheDB::new(EmptyDB::default());
        db.insert_account_info(SYSTEM_CALLER, seed);
        WrapExecutor::new(BasicBlockExecutor::new(evm_config, db))
    }

    /// Helper: drive the migration hook against the current state and take
    /// the resulting bundle. Returns the bundle so the caller can inspect.
    fn run_migration_and_take(
        executor: &mut (impl ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>
                  + 'static),
        chain_spec: &ChainSpec,
        current_ts: u64,
        parent_ts: u64,
        block_number: u64,
    ) -> revm::database::BundleState {
        apply_state_changes_for_block(
            executor
                as &mut dyn ParallelExecutor<
                    Primitives = EthPrimitives,
                    Error = BlockExecutionError,
                >,
            chain_spec,
            current_ts,
            parent_ts,
            block_number,
        );
        executor.take_bundle()
    }

    // --- §1.4 / u9_a: activation block zeros balance, preserves nonce + code

    #[test]
    fn test_migration_at_activation_block_zeros_balance_preserves_rest() {
        let chain_spec = alpha_chainspec();
        let code = nonempty_code();
        let code_hash = code.hash_slow();
        let seed = AccountInfo {
            balance: sentinel_balance(),
            nonce: 5,
            code_hash,
            code: Some(code.clone()),
        };
        let mut executor = fresh_executor(chain_spec.clone(), seed);

        // parent_ts = ALPHA_TS - 1, current_ts = ALPHA_TS  →  transitions.
        let bundle =
            run_migration_and_take(&mut executor, chain_spec.as_ref(), ALPHA_TS, ALPHA_TS - 1, 42);

        let acc = bundle
            .state
            .get(&SYSTEM_CALLER)
            .expect("SYSTEM_CALLER must be present in bundle after activation migration");
        let info = acc
            .info
            .as_ref()
            .expect("SYSTEM_CALLER bundle info must be present (Touched diff applied)");
        assert_eq!(info.balance, U256::ZERO, "balance must be zeroed by migration");
        assert_eq!(info.nonce, 5, "nonce must be preserved across migration");
        assert_eq!(info.code_hash, code_hash, "code_hash must be preserved across migration");
        assert!(acc.storage.is_empty(), "migration must not touch storage (balance-only diff)");
    }

    // --- §1.4 / u9_b: idempotent on re-execution at the same activation block

    #[test]
    fn test_migration_idempotent_on_reexecution() {
        let chain_spec = alpha_chainspec();
        let code = nonempty_code();
        let code_hash = code.hash_slow();
        let seed =
            AccountInfo { balance: sentinel_balance(), nonce: 5, code_hash, code: Some(code) };
        let mut executor = fresh_executor(chain_spec.clone(), seed);

        // First application — should zero balance and produce a bundle.
        apply_state_changes_for_block(
            &mut executor
                as &mut dyn ParallelExecutor<
                    Primitives = EthPrimitives,
                    Error = BlockExecutionError,
                >,
            chain_spec.as_ref(),
            ALPHA_TS,
            ALPHA_TS - 1,
            42,
        );
        // Second application — gate still fires at the activation block, but
        // the previously zeroed balance means the resulting diff is a no-op
        // in observable state (balance was 0; setting to 0 again is the
        // identity). After the second `apply_state_change`, the bundle
        // should still terminate at balance=0, nonce=preserved.
        apply_state_changes_for_block(
            &mut executor
                as &mut dyn ParallelExecutor<
                    Primitives = EthPrimitives,
                    Error = BlockExecutionError,
                >,
            chain_spec.as_ref(),
            ALPHA_TS,
            ALPHA_TS - 1,
            42,
        );

        let bundle = executor.take_bundle();
        let acc = bundle
            .state
            .get(&SYSTEM_CALLER)
            .expect("SYSTEM_CALLER must be present after second application");
        let info = acc.info.as_ref().expect("info present");
        assert_eq!(info.balance, U256::ZERO, "balance stays zero after re-application");
        assert_eq!(info.nonce, 5, "nonce still preserved after re-application");
        assert_eq!(info.code_hash, code_hash, "code_hash still preserved after re-application");
    }

    // --- §1.4 / u9_c: post-activation block is a no-op

    #[test]
    fn test_migration_no_op_on_post_activation_blocks() {
        let chain_spec = alpha_chainspec();
        let seed = AccountInfo {
            balance: sentinel_balance(),
            nonce: 5,
            code_hash: KECCAK_EMPTY,
            code: None,
        };
        let mut executor = fresh_executor(chain_spec.clone(), seed);

        // parent_ts >= ALPHA_TS, current_ts > ALPHA_TS  →  transitions_at_timestamp
        // returns false, hook returns early, no apply_state_change call.
        let bundle =
            run_migration_and_take(&mut executor, chain_spec.as_ref(), ALPHA_TS + 1, ALPHA_TS, 43);

        assert!(
            bundle.state.get(&SYSTEM_CALLER).is_none(),
            "post-activation block: hook must NOT touch SYSTEM_CALLER (no apply_state_change)"
        );
        assert!(
            bundle.state.is_empty(),
            "post-activation block: hook must leave bundle empty (gate guards the early-return)"
        );
    }

    // --- §1.4: pre-activation block is a no-op (defensive — same gating)

    #[test]
    fn test_migration_no_op_on_pre_activation_blocks() {
        let chain_spec = alpha_chainspec();
        let seed = AccountInfo {
            balance: sentinel_balance(),
            nonce: 5,
            code_hash: KECCAK_EMPTY,
            code: None,
        };
        let mut executor = fresh_executor(chain_spec.clone(), seed);

        // parent_ts < ALPHA_TS, current_ts < ALPHA_TS  →  transitions_at_timestamp
        // returns false, hook returns early.
        let bundle = run_migration_and_take(
            &mut executor,
            chain_spec.as_ref(),
            ALPHA_TS - 1,
            ALPHA_TS - 2,
            41,
        );

        assert!(
            bundle.state.get(&SYSTEM_CALLER).is_none(),
            "pre-activation block: hook must NOT touch SYSTEM_CALLER"
        );
        assert!(bundle.state.is_empty(), "pre-activation block: hook must leave bundle empty");
    }

    // --- §1.4 / u9_d: EIP-161 not pruned (nonce > 0 keeps account alive)

    #[test]
    fn test_migration_account_not_pruned_by_eip161() {
        let chain_spec = alpha_chainspec();
        let seed = AccountInfo {
            balance: sentinel_balance(),
            nonce: 5,
            code_hash: KECCAK_EMPTY,
            code: None,
        };
        let mut executor = fresh_executor(chain_spec.clone(), seed);

        let bundle =
            run_migration_and_take(&mut executor, chain_spec.as_ref(), ALPHA_TS, ALPHA_TS - 1, 42);

        let acc = bundle
            .state
            .get(&SYSTEM_CALLER)
            .expect("SYSTEM_CALLER must be present after migration");
        let info = acc.info.as_ref().expect("info present");
        assert!(
            !info.is_empty(),
            "post-migration SYSTEM_CALLER must NOT satisfy EIP-161 `is_empty` (nonce>0 keeps it alive)"
        );
        // Belt-and-braces: nonce > 0 means EIP-161 will never strip the
        // account on state-clear, regardless of how the post-block state
        // hook walks it.
        assert!(info.nonce > 0, "nonce must remain non-zero post-migration");
    }

    // --- Defensive: hook is robust against a missing pre-state SYSTEM_CALLER

    #[test]
    fn test_migration_defensive_when_system_caller_absent() {
        let chain_spec = alpha_chainspec();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        // No `insert_account_info` for SYSTEM_CALLER — the underlying
        // EmptyDB returns Ok(None), and the hook's `unwrap_or_default()`
        // covers this degenerate test fixture without panicking.
        let mut executor = WrapExecutor::new(BasicBlockExecutor::new(
            evm_config,
            CacheDB::new(EmptyDB::default()),
        ));

        let bundle =
            run_migration_and_take(&mut executor, chain_spec.as_ref(), ALPHA_TS, ALPHA_TS - 1, 42);

        // The hook still constructs a diff (balance=0, nonce=0, no code) and
        // calls apply_state_change. That diff is observable in the bundle as
        // a Touched SYSTEM_CALLER with balance=0 / nonce=0.
        let acc = bundle.state.get(&SYSTEM_CALLER).expect(
            "even with absent pre-state, hook writes a balance=0 diff and SYSTEM_CALLER lands in bundle",
        );
        let info = acc.info.as_ref().expect("info present");
        assert_eq!(info.balance, U256::ZERO);
        assert_eq!(info.nonce, 0);
    }

    // --- Address-literal sanity (defends §6.1 grep #2)

    #[test]
    fn test_system_caller_address_literal_matches_canonical() {
        // If this fails, somebody redeclared the SYSTEM_CALLER literal — and
        // `reth_chainspec::is_gravity_system_caller` plus the grep checklist
        // §6.1 #2 should also fail. The unit test is a fast-feedback canary.
        assert_eq!(SYSTEM_CALLER, address!("0x00000000000000000000000000000001625f0000"));
    }

    // ================================================================
    // U-6b / U-6c — extensions to the U-6 dual-backend equivalence tests
    // (defined in `crates/ethereum/evm/src/parallel_execute.rs`).
    //
    // These tests live here rather than beside U-6 because
    // `apply_state_changes_for_block` is `pub(crate)` to this crate and
    // pipe-exec-layer is downstream of `reth-evm-ethereum`. Colocating the
    // tests with the hook they exercise avoids either promoting the hook
    // to `pub` (widens the API surface for a test-only concern) or
    // reaching into pipe-layer runtime from the U-6 module.
    // U-6d does not need the hook and lives with U-6.
    // ================================================================

    /// Build a grevm-backed executor seeded with a `SYSTEM_CALLER` account.
    ///
    /// Mirrors [`fresh_executor`] but constructs
    /// [`reth_evm_ethereum::parallel_execute::GrevmExecutor`] so U-6b/U-6c can
    /// compare the migration hook / cross-block bundle against the serial
    /// (`WrapExecutor`) path. Uses `CacheDB<EmptyDB>` on both sides so the
    /// pre-hook state is byte-identical between backends.
    fn fresh_grevm_executor(
        chain_spec: Arc<reth_chainspec::ChainSpec>,
        seed: AccountInfo,
    ) -> reth_evm_ethereum::parallel_execute::GrevmExecutor<
        CacheDB<EmptyDB>,
        EthEvmConfig,
        reth_chainspec::ChainSpec,
    > {
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let mut db = CacheDB::new(EmptyDB::default());
        db.insert_account_info(SYSTEM_CALLER, seed);
        reth_evm_ethereum::parallel_execute::GrevmExecutor::new(chain_spec, &evm_config, db)
    }

    // --- U-6b: migration hook symmetry across serial vs grevm backends ---

    /// `u6b`: the Alpha `SYSTEM_CALLER` migration hook must produce
    /// byte-identical `BundleState` on both backends. Serial routes through
    /// `WrapExecutor::apply_state_change`; grevm routes through
    /// `GrevmExecutor::apply_state_change`. Both consume the same diff shape
    /// emitted by [`apply_state_changes_for_block`], so any drift is a
    /// backend-side commit-semantics bug and would fork state root on the
    /// activation block.
    ///
    /// Complements the existing U-6 (`transact_system_txn` equivalence) —
    /// U-6 never triggers migration (its seed is already balance=0) and U-6b
    /// never runs a system tx, so together they cover the two orthogonal
    /// diff sources that land in an Alpha activation block.
    #[test]
    fn u6b_test_migration_hook_symmetry() {
        let chain_spec = alpha_chainspec();
        let code = nonempty_code();
        let code_hash = code.hash_slow();
        let seed = AccountInfo {
            balance: sentinel_balance(),
            nonce: 5,
            code_hash,
            code: Some(code.clone()),
        };

        // Serial: WrapExecutor over CacheDB<EmptyDB>, seeded identically.
        let mut serial = fresh_executor(chain_spec.clone(), seed.clone());
        apply_state_changes_for_block(
            &mut serial
                as &mut dyn ParallelExecutor<
                    Primitives = EthPrimitives,
                    Error = BlockExecutionError,
                >,
            chain_spec.as_ref(),
            ALPHA_TS,
            ALPHA_TS - 1,
            42,
        );
        let bundle_serial = serial.take_bundle();

        // Grevm: GrevmExecutor over CacheDB<EmptyDB>, seeded identically.
        let mut grevm = fresh_grevm_executor(chain_spec.clone(), seed);
        apply_state_changes_for_block(
            &mut grevm
                as &mut dyn ParallelExecutor<
                    Primitives = EthPrimitives,
                    Error = BlockExecutionError,
                >,
            chain_spec.as_ref(),
            ALPHA_TS,
            ALPHA_TS - 1,
            42,
        );
        let bundle_grevm = grevm.take_bundle();

        // Load-bearing byte-equivalence. Same field selection as U-6 —
        // `reverts_size` skipped for the same reason (grevm's
        // `parallel_apply_transitions_and_create_reverts` does not update
        // `reverts_size`; not consensus-affecting).
        assert_eq!(
            bundle_serial.state, bundle_grevm.state,
            "migration hook state map drift between serial and grevm"
        );
        assert_eq!(
            bundle_serial.contracts, bundle_grevm.contracts,
            "migration hook contracts drift between serial and grevm"
        );
        assert_eq!(
            bundle_serial.state_size, bundle_grevm.state_size,
            "migration hook state_size drift between serial and grevm"
        );
        assert_eq!(
            bundle_serial.reverts, bundle_grevm.reverts,
            "migration hook reverts drift between serial and grevm"
        );

        // Sanity: the migration actually did its job on the serial side
        // (mirrored on grevm via the byte-equality above).
        let acc = bundle_serial
            .state
            .get(&SYSTEM_CALLER)
            .expect("SYSTEM_CALLER must be present after migration");
        let info = acc.info.as_ref().expect("info present");
        assert_eq!(info.balance, U256::ZERO, "balance must be zeroed by migration");
        assert_eq!(info.nonce, 5, "nonce must be preserved by migration");
        assert_eq!(info.code_hash, code_hash, "code_hash must be preserved by migration");
        assert!(!info.is_empty(), "post-migration SYSTEM_CALLER must survive EIP-161 (nonce > 0)");
    }

    // --- U-6c: cross-block bundle carryover (activation + 5 post-Alpha) ---

    /// `u6c`: run a sequence of 6 blocks through both backends
    /// (activation block T + five post-Alpha blocks T+1..T+5), asserting
    /// per-block `BundleState` byte-equivalence at every step. Pins that
    /// backend state carryover across blocks does not drift — a bug that
    /// only surfaces cumulatively (e.g. grevm forgetting a nonce bump
    /// between blocks) would show up here as a drift in block N ≥ 2, but
    /// not in the single-block U-6 or the single-hook U-6b.
    ///
    /// Uses "方案 A" from the design doc: the executor is reused across
    /// blocks, `take_bundle` drains the bundle_state but preserves the
    /// underlying cache, so block N+1 runs against the accumulated state
    /// from blocks 1..N without any explicit re-seeding.
    ///
    /// The activation block also exercises the load-bearing "migration hook
    /// diff + system-tx diff on the same block" combination — a serial /
    /// grevm ordering mismatch there (migration written after tx instead of
    /// before, or interleaved with the wrong state view) would fail at
    /// block 1 of the sequence.
    #[test]
    fn u6c_test_cross_block_bundle_carryover() {
        // Alpha = 1000. First block ts = 1000 → migration hook fires.
        // Subsequent 5 blocks (ts 1001..=1005) → hook is a no-op, only
        // system txs execute.
        const ALPHA_C: u64 = 1000;
        let mut spec = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated()
            .build();
        spec.gravity_hardforks =
            ChainHardforks::from([(GravityHardfork::Alpha, ForkCondition::Timestamp(ALPHA_C))]);
        let chain_spec = Arc::new(spec);
        let chain_id = chain_spec.chain().id();
        let evm_config = EthEvmConfig::new(chain_spec.clone());

        // Both backends: seed SYSTEM_CALLER with the sentinel balance (mirror
        // production genesis pre-Alpha) and a non-zero nonce. Nonce is set
        // to 1 rather than 0 so the post-migration `AccountInfo` is
        // non-empty even before any tx has bumped the nonce — the migration
        // hook's `apply_state_change` runs with `state_clear_flag = false`
        // (only `transact_system_txn` flips that flag on the underlying
        // `State`), and revm's pre-EIP-161 path (`touch_create_pre_eip161`
        // → `on_touched_created_pre_eip161`) panics with "Wrong state
        // transition, touch crate is not possible from Loaded" when the
        // touched account is empty. Nonce ≥ 1 avoids that trap and matches
        // production reality (SYSTEM_CALLER always has non-zero nonce by
        // the time Alpha activates — it has been executing per-block system
        // txs since block 1).
        let seed = AccountInfo {
            balance: sentinel_balance(),
            nonce: 1,
            code_hash: KECCAK_EMPTY,
            code: None,
        };
        let mut serial = fresh_executor(chain_spec.clone(), seed.clone());
        let mut grevm = fresh_grevm_executor(chain_spec.clone(), seed);

        // Blocks: (block_number, timestamp, parent_ts)
        //   block 1 = activation (ts 1000, parent 999) — migration fires
        //   blocks 2..=6 = post-Alpha (ts 1001..=1005) — migration no-op
        let sequence: [(u64, u64, u64); 6] = [
            (1, 1000, 999),
            (2, 1001, 1000),
            (3, 1002, 1001),
            (4, 1003, 1002),
            (5, 1004, 1003),
            (6, 1005, 1004),
        ];

        // Build a Prague-shaped header for each block. Only `timestamp` and
        // `number` matter for the `EthEvmConfig::evm_env` shape used by
        // `transact_system_txn`; `parent_hash` / `parent_beacon_block_root`
        // stay defaulted since we do not chain them through consensus here.
        let build_header = |number: u64, timestamp: u64| alloy_consensus::Header {
            parent_hash: alloy_primitives::B256::ZERO,
            timestamp,
            number,
            requests_hash: Some(alloy_eips::eip7685::EMPTY_REQUESTS_HASH),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(alloy_primitives::B256::ZERO),
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1_000_000_000),
            ..alloy_consensus::Header::default()
        };

        // Metadata-shaped system tx (gas_price=0, gas-exempt under Alpha).
        let build_tx = |nonce: u64| revm::context::TxEnv {
            caller: SYSTEM_CALLER,
            gas_limit: 1_000_000,
            gas_price: 0,
            kind: revm::primitives::TxKind::Call(SYSTEM_CALLER),
            value: U256::ZERO,
            data: Bytes::new(),
            nonce,
            chain_id: Some(chain_id),
            ..revm::context::TxEnv::default()
        };

        // Sanity: gate is ON at ts 1000 (activation), post-Alpha blocks too.
        assert!(
            reth_chainspec::is_system_tx_gas_exempt(chain_spec.as_ref(), 1000),
            "U-6c fixture: gate must be ON at activation ts"
        );
        assert!(
            reth_chainspec::is_system_tx_gas_exempt(chain_spec.as_ref(), 1005),
            "U-6c fixture: gate must remain ON post-Alpha"
        );

        for (block_num, ts, prev_ts) in sequence {
            let header = build_header(block_num, ts);
            let evm_env = <EthEvmConfig as reth_evm::ConfigureEvm>::evm_env(&evm_config, &header)
                .expect("evm_env must build");

            // 1) migration hook (fires exactly on block 1, no-op on 2..=6)
            apply_state_changes_for_block(
                &mut serial
                    as &mut dyn ParallelExecutor<
                        Primitives = EthPrimitives,
                        Error = BlockExecutionError,
                    >,
                chain_spec.as_ref(),
                ts,
                prev_ts,
                block_num,
            );
            apply_state_changes_for_block(
                &mut grevm
                    as &mut dyn ParallelExecutor<
                        Primitives = EthPrimitives,
                        Error = BlockExecutionError,
                    >,
                chain_spec.as_ref(),
                ts,
                prev_ts,
                block_num,
            );

            // 2) two system txs per block. Seed nonce = 1, so tx nonces
            // start at 1 and grow monotonically: block 1 uses (1, 2),
            // block 2 uses (3, 4), ..., block N uses (2*(N-1)+1, 2*(N-1)+2).
            let n_meta = 2 * (block_num - 1) + 1;
            let n_val = 2 * (block_num - 1) + 2;
            serial
                .transact_system_txn(evm_env.clone(), Vec::new(), build_tx(n_meta))
                .unwrap_or_else(|e| panic!("serial block {block_num} metadata tx failed: {e:?}"));
            serial
                .transact_system_txn(evm_env.clone(), Vec::new(), build_tx(n_val))
                .unwrap_or_else(|e| panic!("serial block {block_num} validator tx failed: {e:?}"));
            grevm
                .transact_system_txn(evm_env.clone(), Vec::new(), build_tx(n_meta))
                .unwrap_or_else(|e| panic!("grevm block {block_num} metadata tx failed: {e:?}"));
            grevm
                .transact_system_txn(evm_env, Vec::new(), build_tx(n_val))
                .unwrap_or_else(|e| panic!("grevm block {block_num} validator tx failed: {e:?}"));

            // 3) drain bundle at end-of-block and assert byte equivalence.
            // Draining resets bundle_state on both backends but preserves
            // the underlying cache, so the next iteration continues from
            // the accumulated state (方案 A from the design doc).
            let bs = serial.take_bundle();
            let bg = grevm.take_bundle();
            assert_eq!(
                bs.state, bg.state,
                "block {block_num}: state map drift between serial and grevm"
            );
            assert_eq!(
                bs.contracts, bg.contracts,
                "block {block_num}: contracts drift between serial and grevm"
            );
            assert_eq!(
                bs.state_size, bg.state_size,
                "block {block_num}: state_size drift between serial and grevm"
            );
            assert_eq!(
                bs.reverts, bg.reverts,
                "block {block_num}: reverts drift between serial and grevm"
            );

            // Sanity per block: after end-of-block drain, SYSTEM_CALLER's
            // nonce in the just-drained serial bundle equals the highest
            // tx nonce we issued this block + 1 (revm bumps nonce on each
            // successful tx).
            if let Some(info) = bs.state.get(&SYSTEM_CALLER).and_then(|a| a.info.as_ref()) {
                assert_eq!(
                    info.nonce,
                    n_val + 1,
                    "block {block_num}: SYSTEM_CALLER nonce must reflect all txs so far"
                );
                // At and after activation, migration + subsequent txs keep
                // balance at zero (gas-exempt under Alpha ⇒ no fee debit).
                assert_eq!(
                    info.balance,
                    U256::ZERO,
                    "block {block_num}: SYSTEM_CALLER balance must be zero post-migration"
                );
            }
        }

        // Final: after 6 blocks × 2 txs = 12 txs, SYSTEM_CALLER nonce
        // observable via the last block's bundle equals 12. Already checked
        // per-iteration above; this is a summary anchor for the sequence.
        // No additional read here — the per-block assertions already pin
        // the cumulative invariant.
    }
}
