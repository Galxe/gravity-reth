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
//! and the account is never pruned by state-clear (design §3.3, R5 verify).

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
