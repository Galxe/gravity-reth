//! Metadata transaction execution

use super::{
    types::{convert_active_validators_to_bcs, onBlockStartCall, NewEpochEvent},
    SYSTEM_CALLER,
};
use crate::{onchain_config::BLOCK_ADDR, ExecuteOrderedBlockResult, OrderedBlock};
use alloy_consensus::{constants::EMPTY_WITHDRAWALS, Header, TxLegacy, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::{eip4895::Withdrawals, merge::BEACON_NONCE};
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
use alloy_sol_types::{SolCall, SolEvent};
use gravity_api_types::events::contract_event::GravityEvent;
use gravity_primitives::get_gravity_config;
use reth_chainspec::{ChainSpec, EthereumHardforks};
use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};
use reth_evm::{Evm, IntoTxEnv};
use reth_execution_types::BlockExecutionOutput;
use reth_primitives::{Receipt, Recovered};
use reth_provider::BlockExecutionResult;
use revm::{
    context::TxEnv,
    context_interface::result::{ExecutionResult, HaltReason},
    database::BundleState,
    state::EvmState,
    Database,
};
use std::fmt::Debug;

/// NIL proposer index constant (from Blocker.sol)
/// NIL blocks occur when consensus cannot produce a block with transactions
pub const NIL_PROPOSER_INDEX: u64 = u64::MAX;

/// Result of a metadata transaction execution
/// Merge new state changes into accumulated state changes
///
/// This is a helper function to accumulate state changes from multiple
/// sequential transaction executions.
pub fn merge_state_changes(accumulated: &mut EvmState, new_changes: EvmState) {
    for (addr, account) in new_changes {
        accumulated.insert(addr, account);
    }
}

/// Result of a system transaction execution (metadata, DKG, or JWK)
/// This is a unified structure for all system-level transactions that are executed before
/// the parallel executor.
#[derive(Debug)]
pub struct SystemTxnResult {
    /// Result of the system transaction execution
    pub result: ExecutionResult,
    /// The system transaction
    pub txn: TransactionSigned,
}

impl SystemTxnResult {
    /// Check if the transaction emitted a `NewEpoch` event
    pub fn emit_new_epoch(&self) -> Option<(u64, Bytes)> {
        for log in self.result.logs() {
            match NewEpochEvent::decode_log(log) {
                Ok(event) => {
                    // Convert ValidatorConsensusInfo[] to BCS-encoded ValidatorSet
                    let validator_bytes = convert_active_validators_to_bcs(&event.validatorSet);
                    return Some((event.newEpoch, validator_bytes));
                }
                Err(_) => {}
            }
        }
        None
    }

    /// Convert the system transaction result into a full executed block result
    /// Used when new epoch is triggered and the block needs to be discarded.
    pub(crate) fn into_executed_ordered_block_result(
        self,
        chain_spec: &ChainSpec,
        ordered_block: &OrderedBlock,
        base_fee: u64,
        state: BundleState,
        validators: Bytes,
    ) -> ExecuteOrderedBlockResult {
        let tx_type = self.txn.tx_type();
        let gas_used = self.result.gas_used();
        let mut block = Block {
            header: Header {
                beneficiary: ordered_block.coinbase,
                timestamp: ordered_block.timestamp_us / 1_000_000, // convert to seconds
                mix_hash: ordered_block.prev_randao,
                base_fee_per_gas: Some(base_fee),
                number: ordered_block.number,
                gas_limit: get_gravity_config().pipe_block_gas_limit,
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                nonce: BEACON_NONCE.into(),
                gas_used,
                ..Default::default()
            },
            body: BlockBody { transactions: vec![self.txn], ..Default::default() },
        };

        // Shanghai fork fields
        if chain_spec.is_shanghai_active_at_timestamp(block.timestamp) {
            block.header.withdrawals_root = Some(EMPTY_WITHDRAWALS);
            block.body.withdrawals = Some(Withdrawals::default());
        }

        // Cancun fork fields
        if chain_spec.is_cancun_active_at_timestamp(block.timestamp) {
            // FIXME: Is it OK to use the parent's block id as `parent_beacon_block_root` before
            // execution?
            block.header.parent_beacon_block_root = Some(ordered_block.parent_id);

            // TODO(nekomoto): fill `excess_blob_gas` and `blob_gas_used` fields
            block.header.excess_blob_gas = Some(0);
            block.header.blob_gas_used = Some(0);
        }

        let new_epoch = ordered_block.epoch + 1;
        ExecuteOrderedBlockResult {
            block,
            senders: vec![SYSTEM_CALLER],
            execution_output: BlockExecutionOutput {
                state,
                result: BlockExecutionResult {
                    receipts: vec![Receipt {
                        tx_type,
                        success: true,
                        cumulative_gas_used: gas_used,
                        logs: self.result.into_logs(),
                    }],
                    requests: Default::default(),
                    gas_used,
                },
            },
            txs_info: vec![],
            gravity_events: vec![GravityEvent::NewEpoch(new_epoch, validators.into())],
            epoch: new_epoch,
        }
    }

    /// Insert this system transaction into an existing executed block result at the specified
    /// position Position 0 is reserved for metadata tx, positions 1+ are for validator
    /// transactions
    pub(crate) fn insert_to_executed_ordered_block_result(
        self,
        result: &mut crate::ExecuteOrderedBlockResult,
        insert_position: usize,
    ) {
        let gas_used = self.result.gas_used();
        result.block.header.gas_used += gas_used;
        result.execution_output.gas_used += gas_used;

        // Calculate cumulative_gas_used for this system transaction:
        // It should be the cumulative gas of the previous transaction (at insert_position - 1)
        // plus this transaction's gas_used
        let cumulative_gas_used = if insert_position == 0 {
            // First transaction, cumulative equals its own gas_used
            gas_used
        } else {
            // Get cumulative from the previous receipt and add this tx's gas
            result
                .execution_output
                .receipts
                .get(insert_position - 1)
                .map(|prev| prev.cumulative_gas_used + gas_used)
                .unwrap_or(gas_used)
        };

        // Update all receipts AFTER insert_position to add this tx's gas
        for receipt in result.execution_output.receipts.iter_mut().skip(insert_position) {
            receipt.cumulative_gas_used += gas_used;
        }

        result.execution_output.receipts.insert(
            insert_position,
            Receipt {
                tx_type: self.txn.tx_type(),
                success: true,
                cumulative_gas_used,
                logs: self.result.into_logs(),
            },
        );
        result.block.body.transactions.insert(insert_position, self.txn);
        result.senders.insert(insert_position, SYSTEM_CALLER);
    }
}

/// Execute a single system transaction (metadata, DKG, or JWK)
///
/// This is the unified entry point for executing all system-level transactions.
/// These transactions are executed one by one before the parallel executor.
pub fn transact_system_txn(
    evm: &mut impl Evm<DB = impl Database, Error: Debug, Tx = TxEnv, HaltReason = HaltReason>,
    txn: TransactionSigned,
) -> (SystemTxnResult, EvmState) {
    use reth_evm::IntoTxEnv;
    use reth_primitives::Recovered;

    let tx_env = Recovered::new_unchecked(txn.clone(), SYSTEM_CALLER).into_tx_env();
    let result = evm.transact_raw(tx_env).unwrap();

    // Log any execution errors with appropriate severity
    if !result.result.is_success() {
        super::errors::log_execution_error(&result.result);
    }

    (SystemTxnResult { result: result.result, txn }, result.state)
}

/// Create a new system call transaction
fn new_system_call_txn(
    contract: Address,
    nonce: u64,
    gas_price: u128,
    input: Bytes,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce,
            gas_price,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        Signature::new(U256::ZERO, U256::ZERO, false),
    )
}

/// Execute a metadata contract call (onBlockStart from Blocker.sol)
///
/// Calls Blocker.onBlockStart(proposerIndex, failedProposerIndices, timestampMicros)
/// to perform block prologue operations including:
/// - Resolving proposer address from index
/// - Updating global timestamp
/// - Checking and potentially starting epoch transition
///
/// @param proposer_index Index of the proposer in the active validator set,
///        or None for NIL blocks (will use NIL_PROPOSER_INDEX = u64::MAX)
pub fn construct_metadata_txn(
    nonce: u64,
    gas_price: u128,
    timestamp_us: u64,
    proposer_index: Option<u64>,
) -> TransactionSigned {
    // For NIL blocks, use NIL_PROPOSER_INDEX (type(uint64).max in Solidity)
    let proposer_idx = proposer_index.unwrap_or(NIL_PROPOSER_INDEX);

    let call = onBlockStartCall {
        proposerIndex: proposer_idx,
        failedProposerIndices: vec![],
        timestampMicros: timestamp_us,
    };
    let input: Bytes = call.abi_encode().into();

    new_system_call_txn(BLOCK_ADDR, nonce, gas_price, input)
}
