//! Metadata transaction execution

use super::{
    types::{blockPrologueCall, convert_validator_set_to_bcs, AllValidatorsUpdated},
    SYSTEM_CALLER,
};
use crate::{onchain_config::BLOCK_ADDR, ExecuteOrderedBlockResult, OrderedBlock};
use alloy_consensus::{constants::EMPTY_WITHDRAWALS, Header, TxLegacy, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::{eip4895::Withdrawals, merge::BEACON_NONCE};
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
use alloy_sol_types::{SolCall, SolEvent};
use gravity_api_types::events::contract_event::GravityEvent;
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
};
use std::fmt::Debug;

/// Result of a metadata transaction execution
#[derive(Debug)]
pub struct MetadataTxnResult {
    /// Result of the metadata transaction execution
    pub result: ExecutionResult,
    /// The metadata transaction
    pub txn: TransactionSigned,
}

impl MetadataTxnResult {
    /// Check if the transaction emitted a `NewEpoch` event
    pub fn emit_new_epoch(&self) -> Option<(u64, Bytes)> {
        for log in self.result.logs() {
            match AllValidatorsUpdated::decode_log(log) {
                Ok(event) => {
                    let solidity_validator_set = &event.validatorSet;
                    // Convert to Gravity validator set
                    let validator_bytes = convert_validator_set_to_bcs(solidity_validator_set);
                    return Some((event.newEpoch.to::<u64>(), validator_bytes));
                }
                Err(_) => {}
            }
        }
        None
    }

    /// Convert the metadata transaction result into a full executed block result
    pub(crate) fn into_executed_ordered_block_result(
        self,
        ordered_block: &OrderedBlock,
        state: BundleState,
        validators: Bytes,
    ) -> ExecuteOrderedBlockResult {
        let tx_type = self.txn.tx_type();
        let mut block = Block {
            header: Header {
                beneficiary: ordered_block.coinbase,
                timestamp: ordered_block.timestamp,
                mix_hash: ordered_block.prev_randao,
                base_fee_per_gas: Some(0),
                number: ordered_block.number,
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                nonce: BEACON_NONCE.into(),
                ..Default::default()
            },
            body: BlockBody { transactions: vec![self.txn], ..Default::default() },
        };

        // Shanghai fork fields
        block.header.withdrawals_root = Some(EMPTY_WITHDRAWALS);
        block.body.withdrawals = Some(Withdrawals::default());

        // Cancun fork fields
        // FIXME: Is it OK to use the parent's block id as `parent_beacon_block_root` before
        // execution?
        block.header.parent_beacon_block_root = Some(ordered_block.parent_id);
        block.header.excess_blob_gas = Some(0);
        block.header.blob_gas_used = Some(0);

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
                        cumulative_gas_used: 0,
                        logs: self.result.into_logs(),
                    }],
                    requests: Default::default(),
                    gas_used: 0,
                },
            },
            txs_info: vec![],
            gravity_events: vec![GravityEvent::NewEpoch(new_epoch, validators.into())],
            epoch: new_epoch,
        }
    }

    /// Insert this metadata transaction into an existing executed block result
    pub(crate) fn insert_to_executed_ordered_block_result(
        self,
        result: &mut ExecuteOrderedBlockResult,
    ) {
        result.execution_output.receipts.insert(
            0,
            Receipt {
                tx_type: self.txn.tx_type(),
                success: true,
                cumulative_gas_used: 0,
                logs: self.result.into_logs(),
            },
        );
        result.block.body.transactions.insert(0, self.txn);
        result.senders.insert(0, SYSTEM_CALLER);
    }
}

/// Create a new system call transaction
fn new_system_call_txn(contract: Address, input: Bytes) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce: 0,
            gas_price: 0,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        Signature::test_signature(),
    )
}

/// Execute a metadata contract call (blockPrologue)
pub fn transact_metadata_contract_call(
    evm: &mut impl Evm<Error: Debug, Tx = TxEnv, HaltReason = HaltReason>,
    timestamp_us: u64,
    proposer: Option<Address>,
) -> (MetadataTxnResult, EvmState) {
    let call = blockPrologueCall {
        proposer: proposer.unwrap_or(SYSTEM_CALLER),
        failedProposerIndices: vec![],
        timestampMicros: U256::from(timestamp_us),
    };
    let input: Bytes = call.abi_encode().into();
    let txn = new_system_call_txn(BLOCK_ADDR, input);
    let tx_env = Recovered::new_unchecked(txn.clone(), SYSTEM_CALLER).into_tx_env();
    let mut result = evm.transact_raw(tx_env).unwrap();
    assert!(result.result.is_success(), "Failed to execute blockPrologue: {:?}", result.result);
    result.state.remove(&SYSTEM_CALLER);
    result.state.remove(&evm.block().beneficiary);
    (MetadataTxnResult { result: result.result, txn }, result.state)
}
