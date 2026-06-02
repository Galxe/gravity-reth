//! EIP-2935 (Prague) — `HISTORY_STORAGE` activation + test reader.
//!
//! Carved out of `lib.rs` so the pipe-layer's `execute_ordered_block` only sees a
//! single call site for "irregular state changes at the Prague boundary". The two
//! state changes share the same `apply_state_change` channel and the same
//! transition gating shape — keeping them together documents that pairing and
//! avoids further bloat in the main file.
//!
//! Two state changes live here:
//!
//! 1. EIP-2935 `HISTORY_STORAGE_ADDRESS` deployment — fires exactly on the Prague activation block.
//!    Mainnet-aligned alloc: nonce=1, balance=0, `code=HISTORY_STORAGE_CODE`, no storage prefill.
//!    The upstream `SystemCaller` then drives the per-block SSTORE inside the executor's
//!    `apply_pre_execution_changes`.
//! 2. A 52-byte test reader at `0x...C0DE1`, gated on the chainspec
//!    `extra_fields["gravityTestReaderDeployTime"]` key. Production chainspecs omit the field, so
//!    the branch is dormant.

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

/// Address of the test-only HISTORY_STORAGE reader. Mirrored literally in
/// `tests/gravity_eip2935_test.rs::READER_ADDRESS` so any drift between the
/// pipe-side hook and the test driver fails loudly.
///
/// Bytecode (52 bytes, hand-written EVM):
///   - `mem[0:32] := calldata[0:32]`        (n)
///   - `staticcall(gas, HISTORY_STORAGE, mem[0:32], mem[0:32])`
///   - on success → `return mem[0:32]`
///   - on revert  → `revert(mem[0:32])` (32 zero bytes; EIP-2935 itself returns no revert data, so
///     the bubble-up has no payload anyway)
///
/// Invocation: callers send exactly 32 bytes of calldata holding the
/// block number `n`. The reader is therefore directly callable via
/// `eth_call({ to, data: B256::from(U256::from(n)).as_slice() })` —
/// no Solidity ABI selector to manage, no `solc` dependency.
const TEST_HISTORY_QUERY_READER_ADDRESS: Address =
    alloy_primitives::address!("0x00000000000000000000000000000000000C0DE1");

/// Runtime bytecode for [`TEST_HISTORY_QUERY_READER_ADDRESS`].
const TEST_HISTORY_QUERY_READER_BYTECODE: [u8; 52] = alloy_primitives::hex!(
    "602060006000376020600060206000730000f90827f1c53a10cb7a02335b1753200029355afa602e5760206000fd5b60206000f3"
);

type Executor<'a> =
    &'a mut dyn ParallelExecutor<Primitives = EthPrimitives, Error = BlockExecutionError>;

/// Apply EIP-2935 boundary state changes for `block_number`.
///
/// Idempotency comes from `transitions_at_timestamp` — `parent_ts < pragueTime`
/// is history-immutable, so the deployment branch fires exactly on the
/// activation block and is naturally reorg-safe.
///
/// The test-reader branch uses the same shape against a custom extra field; it
/// is dormant unless the chainspec opts in.
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

    // Test-only: deploy a thin HISTORY_STORAGE reader at the activation
    // timestamp configured in `genesis.config.extra_fields.gravityTestReaderDeployTime`.
    // The gate is custom to the test field and not registered in `EthereumHardfork`,
    // so we re-derive `transitions_at_timestamp` inline. Production chainspecs omit
    // the field, so `.and_then(|v| v.as_u64())` returns `None`.
    if let Some(reader_deploy_ts) = chain_spec
        .genesis()
        .config
        .extra_fields
        .get("gravityTestReaderDeployTime")
        .and_then(|v| v.as_u64())
    {
        if parent_ts < reader_deploy_ts && current_ts >= reader_deploy_ts {
            deploy_contract(
                executor,
                TEST_HISTORY_QUERY_READER_ADDRESS,
                Bytes::from_static(&TEST_HISTORY_QUERY_READER_BYTECODE),
            )
            .unwrap_or_else(|e| panic!("test HISTORY_STORAGE reader deployment failed: {e:?}"));
            info!(target: "execute_ordered_block",
                number=?block_number,
                address=?TEST_HISTORY_QUERY_READER_ADDRESS,
                "deployed test HISTORY_STORAGE reader contract"
            );
        }
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
