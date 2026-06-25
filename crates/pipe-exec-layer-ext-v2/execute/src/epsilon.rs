//! Epsilon (Gravity) — zero `SYSTEM_CALLER`'s sentinel balance.
//!
//! `SYSTEM_CALLER` (`0x…01625f0000`) is pre-funded in genesis with a sentinel "infinite" balance
//! so it could pay the base fee of the per-block system transactions. Once the Epsilon hardfork
//! makes system transactions gas-exempt (`EthEvmConfig::transact_system_txn` /
//! `GrevmExecutor::transact_system_txn`), that balance is unused — and the sentinel is a
//! supply-accounting wart plus an unbounded-mint footgun. Zero it on the Epsilon activation block.
//!
//! Mirrors `eip_2935`: a one-time irregular state change applied via the executor's
//! backend-agnostic [`ParallelExecutor::apply_state_change`], gated by `transitions_at_timestamp`
//! so it fires exactly once and is reorg-safe. See gravity-reth#364 / gravity-audit#720.

use crate::onchain_config::SYSTEM_CALLER;
use alloy_primitives::U256;
use reth_chainspec::{ChainSpec, EthChainSpec, GravityHardfork, Hardforks};
use reth_evm::{execute::BlockExecutionError, parallel_execute::ParallelExecutor};
use reth_primitives::EthPrimitives;
use revm::state::{Account, AccountInfo, AccountStatus, EvmState};
use tracing::info;

type Executor<'a> =
    &'a mut dyn ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>;

/// Zero `SYSTEM_CALLER`'s balance on the Epsilon activation block.
///
/// `system_caller_nonce` is `SYSTEM_CALLER`'s nonce at the start of this block (before the system
/// transactions run); it is preserved so the account stays non-empty (nonce > 0 → not pruned by
/// EIP-161 state-clear) and its transaction sequence continues unbroken. The account has no code
/// and no storage, so a full replacement with `balance = 0` is exact.
///
/// Idempotency/reorg-safety comes from `transitions_at_timestamp` (a parent below the fork time is
/// history-immutable), so the zeroing fires exactly on the activation block.
pub(crate) fn apply_state_changes_for_block(
    executor: Executor<'_>,
    chain_spec: &ChainSpec,
    current_ts: u64,
    parent_ts: u64,
    system_caller_nonce: u64,
    block_number: u64,
) {
    if chain_spec
        .gravity_hardforks()
        .fork(GravityHardfork::Epsilon)
        .transitions_at_timestamp(current_ts, parent_ts)
    {
        let mut state_diff = EvmState::default();
        state_diff.insert(
            SYSTEM_CALLER,
            Account {
                info: AccountInfo {
                    nonce: system_caller_nonce,
                    balance: U256::ZERO,
                    ..Default::default()
                },
                storage: Default::default(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            },
        );
        executor.apply_state_change(state_diff).unwrap_or_else(|e| {
            panic!("zeroing SYSTEM_CALLER balance failed at Epsilon activation: {e:?}")
        });
        info!(target: "execute_ordered_block",
            number=?block_number,
            "zeroed SYSTEM_CALLER balance on Epsilon activation block"
        );
    }
}
