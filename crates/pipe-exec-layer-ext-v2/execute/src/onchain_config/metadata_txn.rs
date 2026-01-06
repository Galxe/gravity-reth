//! Metadata transaction execution

use super::{
    types::{
        blockPrologueCall, blockPrologueExtCall, convert_validator_set_to_bcs, AllValidatorsUpdated,
    },
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
                timestamp: ordered_block.timestamp_us,
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

    /// Insert this metadata transaction into an existing executed block result
    pub(crate) fn insert_to_executed_ordered_block_result(
        self,
        result: &mut ExecuteOrderedBlockResult,
    ) {
        let gas_used = self.result.gas_used();
        result.block.header.gas_used += gas_used;
        result.execution_output.gas_used += gas_used;
        result.execution_output.receipts.iter_mut().for_each(|receipt| {
            receipt.cumulative_gas_used += gas_used;
        });
        result.execution_output.receipts.insert(
            0,
            Receipt {
                tx_type: self.txn.tx_type(),
                success: true,
                cumulative_gas_used: gas_used,
                logs: self.result.into_logs(),
            },
        );
        result.block.body.transactions.insert(0, self.txn);
        result.senders.insert(0, SYSTEM_CALLER);
    }
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

/// Execute a metadata contract call (blockPrologue or blockPrologueExt)
///
/// If `enable_randomness` is true, calls blockPrologueExt.
/// Otherwise, calls the legacy blockPrologue function.
///
/// Note: blockPrologueExt retrieves randomness from block.difficulty (set before this call),
/// so it doesn't need to be passed as a parameter.
pub fn transact_metadata_contract_call(
    evm: &mut impl Evm<DB = impl Database, Error: Debug, Tx = TxEnv, HaltReason = HaltReason>,
    timestamp_us: u64,
    proposer: Option<[u8; 32]>,
    enable_randomness: bool,
) -> (MetadataTxnResult, EvmState) {
    let input: Bytes = if enable_randomness {
        // Use blockPrologueExt - randomness is read from block.difficulty by the contract
        let call = blockPrologueExtCall {
            proposer: proposer.map(|p| Bytes::from(p)).unwrap_or(Bytes::from([0u8; 32])),
            failedProposerIndices: vec![],
            timestampMicros: timestamp_us,
        };
        call.abi_encode().into()
    } else {
        // Use legacy blockPrologue without randomness
        let call = blockPrologueCall {
            proposer: proposer.map(|p| Bytes::from(p)).unwrap_or(Bytes::from([0u8; 32])),
            failedProposerIndices: vec![],
            timestampMicros: timestamp_us,
        };
        call.abi_encode().into()
    };

    let system_call_account =
        evm.db_mut().basic(SYSTEM_CALLER).unwrap().expect("SYSTEM_CALLER not exists");
    let txn = new_system_call_txn(
        BLOCK_ADDR,
        system_call_account.nonce,
        evm.block().basefee as u128,
        input,
    );
    let tx_env = Recovered::new_unchecked(txn.clone(), SYSTEM_CALLER).into_tx_env();
    let result = evm.transact_raw(tx_env).unwrap();

    let function_name = if enable_randomness { "blockPrologueExt" } else { "blockPrologue" };
    assert!(result.result.is_success(), "Failed to execute {}: {:?}", function_name, result.result);

    (MetadataTxnResult { result: result.result, txn }, result.state)
}
