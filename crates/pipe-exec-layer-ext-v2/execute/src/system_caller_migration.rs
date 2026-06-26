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
}
