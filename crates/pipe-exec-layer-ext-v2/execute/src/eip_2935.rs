//! EIP-2935 (Prague) — `HISTORY_STORAGE` activation.
//!
//! Carved out of `lib.rs` so the pipe-layer's `execute_ordered_block` only sees a
//! single call site for "irregular state changes at the Prague boundary".
//!
//! EIP-2935 `HISTORY_STORAGE_ADDRESS` deployment fires exactly on the Prague activation block.
//! Mainnet-aligned alloc: nonce=1, balance=0, `code=HISTORY_STORAGE_CODE`, no storage prefill.
//! The upstream `SystemCaller` then drives the per-block SSTORE inside the executor's
//! `apply_pre_execution_changes`.

use alloy_eips::eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE};
use alloy_primitives::{keccak256, Address, Bytes, U256};
use reth_chainspec::{ChainSpec, EthereumHardfork, Hardforks};
use reth_evm::{execute::BlockExecutionError, parallel_execute::ParallelExecutor};
use reth_primitives::EthPrimitives;
use revm::{
    bytecode::Bytecode,
    state::{Account, AccountInfo, AccountStatus, EvmState},
};
use tracing::info;

type Executor<'a> =
    &'a mut dyn ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>;

/// Apply EIP-2935 boundary state changes for `block_number`.
///
/// Idempotency comes from `transitions_at_timestamp` — `parent_ts < pragueTime`
/// is history-immutable, so the deployment branch fires exactly on the
/// activation block and is naturally reorg-safe.
///
/// Panics on deployment failure: in the gravity-sdk integration the panic
/// handler aborts the process, preventing partial-state corruption.
pub(crate) fn apply_state_changes_for_block(
    executor: Executor<'_>,
    chain_spec: &ChainSpec,
    current_ts: u64,
    parent_ts: u64,
    block_number: u64,
) {
    if chain_spec.fork(EthereumHardfork::Prague).transitions_at_timestamp(current_ts, parent_ts) {
        deploy_contract(executor, HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE.clone())
            .unwrap_or_else(|e| {
                panic!("HISTORY_STORAGE deployment failed at Prague activation: {e:?}")
            });
        info!(target: "execute_ordered_block",
            number=?block_number,
            "deployed EIP-2935 HISTORY_STORAGE contract on Prague activation block"
        );
    }
}

/// Deploy a contract at `address` with `code` via the executor's
/// [`ParallelExecutor::apply_state_change`] irregular-state-change channel.
///
/// Mainnet-aligned alloc shape: nonce=1, balance=0, given code, no storage prefill.
/// The `Created | Touched` status routes the diff through ParallelState's
/// `newly_created` path so the contract code is recorded and a proper transition
/// lands in the bundle.
fn deploy_contract(
    executor: Executor<'_>,
    address: Address,
    code: Bytes,
) -> Result<(), BlockExecutionError> {
    let code_hash = keccak256(&code);
    let info = AccountInfo {
        nonce: 1,
        balance: U256::ZERO,
        code_hash,
        code: Some(Bytecode::new_raw(code)),
    };

    let mut state_diff = EvmState::default();
    state_diff.insert(
        address,
        Account {
            info,
            storage: Default::default(),
            status: AccountStatus::Created | AccountStatus::Touched,
            transaction_id: 0,
        },
    );
    executor.apply_state_change(state_diff)
}
