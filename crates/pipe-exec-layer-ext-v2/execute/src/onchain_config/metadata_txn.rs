use crate::{ExecuteOrderedBlockResult, OrderedBlock};
use alloy_consensus::{constants::EMPTY_WITHDRAWALS, Header, TxLegacy, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::{eip4895::Withdrawals, merge::BEACON_NONCE};
use alloy_primitives::{Bytes, PrimitiveSignature, TxKind, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent};
use gravity_api_types::events::contract_event::GravityEvent;
use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};
use reth_evm::Evm;
use reth_execution_types::BlockExecutionOutput;
use reth_primitives::Receipt;
use revm::db::BundleState;
use revm_primitives::{EvmState, ExecutionResult};
use std::fmt::Debug;
use super::{GRAVITY_FRAMEWORK_ADDRESS, BLOCK_MODULE_ADDRESS};
use alloy_primitives::Address;

sol! {
    event NewEpoch(uint64 indexed epoch, bytes validators);
    function blockPrologue(uint64 _timestamp_microseconds) external onlyVm whenInitialized;
}

/// Result of a metadata transaction execution
pub struct MetadataTxnResult {
    pub result: ExecutionResult,
    pub txn: TransactionSigned,
}

impl MetadataTxnResult {
    /// Check if the transaction emitted a NewEpoch event
    pub fn emit_new_epoch(&self) -> Option<(u64, Bytes)> {
        for log in self.result.logs() {
            match NewEpoch::decode_log(log, false) {
                Ok(event) => return Some((event.epoch, event.validators.clone().into())),
                Err(_) => continue,
            }
        }
        None
    }

    /// Convert the metadata transaction result into a full executed block result
    pub fn into_executed_ordered_block_result(
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
            senders: vec![GRAVITY_FRAMEWORK_ADDRESS],
            execution_output: BlockExecutionOutput {
                state,
                receipts: vec![Receipt {
                    tx_type,
                    success: true,
                    cumulative_gas_used: 0,
                    logs: self.result.into_logs(),
                }],
                requests: Default::default(),
                gas_used: 0,
            },
            txs_info: vec![],
            gravity_events: vec![GravityEvent::NewEpoch(new_epoch, validators.clone().into())],
            epoch: new_epoch,
        }
    }

    /// Insert this metadata transaction into an existing executed block result
    pub fn insert_to_executed_ordered_block_result(
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
        result.senders.insert(0, GRAVITY_FRAMEWORK_ADDRESS);
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
        PrimitiveSignature::test_signature(),
    )
}

/// Execute a metadata contract call (blockPrologue)
pub fn transact_metadata_contract_call(
    evm: &mut impl Evm<Error: Debug>,
    timestamp_us: u64,
) -> (MetadataTxnResult, EvmState) {
    let call = blockPrologueCall { _timestamp_microseconds: timestamp_us };
    let input: Bytes = call.abi_encode().into();
    let mut result = evm
        .transact_system_call(GRAVITY_FRAMEWORK_ADDRESS, BLOCK_MODULE_ADDRESS, input.clone())
        .unwrap();
    assert!(result.result.is_success(), "Failed to execute blockPrologue: {:?}", result.result);
    result.state.remove(&GRAVITY_FRAMEWORK_ADDRESS);
    result.state.remove(&evm.block().coinbase);
    (
        MetadataTxnResult {
            result: result.result,
            txn: new_system_call_txn(BLOCK_MODULE_ADDRESS, input),
        },
        result.state,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address as AlloyAddress, b256};
    use revm_primitives::{Log as RevmLog, LogData};

    fn create_test_log_with_new_epoch(epoch: u64, validators: &[u8]) -> RevmLog {
        let event_signature = NewEpoch::SIGNATURE_HASH;
        let encoded_epoch = alloy_primitives::U256::from(epoch);
        let validators_bytes = Bytes::from(validators.to_vec());
        
        RevmLog {
            address: AlloyAddress::from([1u8; 20]),
            data: LogData::new(
                vec![event_signature, encoded_epoch.into()],
                validators_bytes.into(),
            ).unwrap(),
        }
    }

    #[test]
    fn test_emit_new_epoch_detection() {
        // Create a fake execution result with NewEpoch event
        let test_log = create_test_log_with_new_epoch(42, b"test_validators");
        
        let execution_result = ExecutionResult::Success {
            reason: revm_primitives::SuccessReason::Stop,
            gas_used: 21000,
            gas_refunded: 0,
            logs: vec![test_log],
            output: revm_primitives::Output::default(),
        };

        let metadata_result = MetadataTxnResult {
            result: execution_result,
            txn: new_system_call_txn(BLOCK_MODULE_ADDRESS, Bytes::new()),
        };

        let new_epoch_data = metadata_result.emit_new_epoch();
        assert!(new_epoch_data.is_some());
        
        let (epoch, validators) = new_epoch_data.unwrap();
        assert_eq!(epoch, 42);
        assert_eq!(validators.as_ref(), b"test_validators");
    }

    #[test]
    fn test_emit_new_epoch_no_event() {
        // Create an execution result without NewEpoch event
        let execution_result = ExecutionResult::Success {
            reason: revm_primitives::SuccessReason::Stop,
            gas_used: 21000,
            gas_refunded: 0,
            logs: vec![], // No logs
            output: revm_primitives::Output::default(),
        };

        let metadata_result = MetadataTxnResult {
            result: execution_result,
            txn: new_system_call_txn(BLOCK_MODULE_ADDRESS, Bytes::new()),
        };

        let new_epoch_data = metadata_result.emit_new_epoch();
        assert!(new_epoch_data.is_none());
    }

    #[test]
    fn test_new_system_call_txn() {
        let contract_addr = AlloyAddress::from([5u8; 20]);
        let input_data = Bytes::from(vec![1, 2, 3, 4]);
        
        let txn = new_system_call_txn(contract_addr, input_data.clone());
        
        // Verify transaction properties
        match txn.into_transaction() {
            Transaction::Legacy(ref legacy) => {
                assert_eq!(legacy.chain_id, None);
                assert_eq!(legacy.nonce, 0);
                assert_eq!(legacy.gas_price, 0);
                assert_eq!(legacy.gas_limit, 30_000_000);
                assert_eq!(legacy.to, TxKind::Call(contract_addr));
                assert_eq!(legacy.value, U256::ZERO);
                assert_eq!(legacy.input, input_data);
            }
            _ => panic!("Expected legacy transaction"),
        }
    }

    #[test]
    fn test_block_prologue_call_encoding() {
        let timestamp = 1234567890u64;
        let call = blockPrologueCall { _timestamp_microseconds: timestamp };
        let encoded = call.abi_encode();
        
        // Verify the encoding is not empty and contains the function selector
        assert!(!encoded.is_empty());
        assert!(encoded.len() >= 4); // At least function selector
    }
} 