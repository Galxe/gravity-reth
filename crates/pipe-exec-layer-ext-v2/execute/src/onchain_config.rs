use crate::{ExecuteOrderedBlockResult, OrderedBlock};
use alloy_consensus::{constants::EMPTY_WITHDRAWALS, Header, TxLegacy, EMPTY_OMMER_ROOT_HASH};
use alloy_eips::{eip4895::Withdrawals, merge::BEACON_NONCE, BlockId};
use alloy_primitives::{address, Address, Bytes, PrimitiveSignature, TxKind, U256};
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent};
use gravity_api_types::config_storage::OnChainConfig;
use reth_ethereum_primitives::{Block, BlockBody, Transaction, TransactionSigned};
use reth_evm::Evm;
use reth_execution_types::BlockExecutionOutput;
use reth_primitives::Receipt;
use reth_rpc_eth_api::{
    EthApiServer, EthApiTypes, RpcBlock, RpcHeader, RpcReceipt, RpcTransaction,
};
use revm::db::BundleState;
use revm_primitives::{EvmState, ExecutionResult};
use std::{fmt::Debug, sync::OnceLock};
use tokio::runtime::Runtime;

const SYSTEM_ADDRESS: Address = address!("0000000000000000000000000000000000000000");
const BLOCK_MODULE_ADDRESS: Address = address!("00000000000000000000000000000000000000f0");
const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000000000000f1");
const CONSENSUS_CONFIG_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f2");

#[derive(Debug)]
pub(crate) struct OnchainConfigFetcher<EthApi> {
    eth_api: EthApi,
    runtime: OnceLock<Runtime>,
}

impl<EthApi> OnchainConfigFetcher<EthApi>
where
    EthApi: EthApiServer<
            RpcTransaction<EthApi::NetworkTypes>,
            RpcBlock<EthApi::NetworkTypes>,
            RpcReceipt<EthApi::NetworkTypes>,
            RpcHeader<EthApi::NetworkTypes>,
        > + EthApiTypes,
{
    pub(crate) fn new(eth_api: EthApi) -> Self {
        Self { eth_api, runtime: OnceLock::new() }
    }

    /// Simulate the call to the contract at block number and return the result.
    /// Return None if the block is not found.
    fn eth_call(
        &self,
        from: Address,
        to: Address,
        input: Bytes,
        block_number: u64,
    ) -> Option<Bytes> {
        let rt_handle = self
            .runtime
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4.min(std::thread::available_parallelism().unwrap().get()))
                    .thread_name("OnchainConfigFetcher")
                    .enable_all()
                    .build()
                    .expect("Failed to create Tokio runtime")
            })
            .handle();
        // TODO(nekomoto): Handle the case where the block is not found.
        rt_handle
            .block_on(self.eth_api.call(
                TransactionRequest {
                    from: Some(from),
                    to: Some(TxKind::Call(to)),
                    input: TransactionInput::new(input),
                    ..Default::default()
                },
                Some(BlockId::from(block_number)),
                None,
                None,
            ))
            .ok()
    }

    pub(crate) fn fetch_epoch(&self, block_number: u64) -> u64 {
        sol! {
            function getCurrentEpoch() external view returns (uint64);
        }

        let call = getCurrentEpochCall {};
        let input: Bytes = call.abi_encode().into();
        let result = self
            .eth_call(SYSTEM_ADDRESS, RECONFIGURATION_ADDRESS, input, block_number)
            .expect("Failed to call getCurrentEpoch");
        getCurrentEpochCall::abi_decode_returns(&result, false)
            .expect("Failed to decode getCurrentEpoch return value")
            ._0
    }

    pub(crate) fn fetch_config_bytes(
        &self,
        config_name: OnChainConfig,
        block_number: u64,
    ) -> bytes::Bytes {
        match config_name {
            OnChainConfig::ConsensusConfig => {
                sol! {
                    function getCurrentConfig() external view returns (bytes memory);
                }
                let call = getCurrentConfigCall {};
                let input: Bytes = call.abi_encode().into();
                let result = self
                    .eth_call(
                        SYSTEM_ADDRESS,
                        CONSENSUS_CONFIG_CONTRACT_ADDRESS,
                        input,
                        block_number,
                    )
                    .expect("Failed to call getCurrentConfig");
                getCurrentConfigCall::abi_decode_returns(&result, false)
                    .expect("Failed to decode getCurrentConfig return value")
                    ._0
                    .0
            }
            _ => todo!("Implement fetching for other config types"),
        }
    }
}

pub(crate) struct MetadataTxnResult {
    pub result: ExecutionResult,
    pub txn: TransactionSigned,
}

impl MetadataTxnResult {
    pub(crate) fn emit_new_epoch(&self) -> bool {
        sol! {
            event NewEpoch(uint64 indexed epoch);
        }

        for log in self.result.logs() {
            match NewEpoch::decode_log(log, false) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
        false
    }

    pub(crate) fn into_executed_ordered_block_result(
        self,
        ordered_block: &OrderedBlock,
        state: BundleState,
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

        ExecuteOrderedBlockResult {
            block,
            senders: vec![SYSTEM_ADDRESS],
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
            epoch: ordered_block.epoch + 1,
        }
    }

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
        result.senders.insert(0, SYSTEM_ADDRESS);
    }
}

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

pub(crate) fn transact_metadata_contract_call(
    evm: &mut impl Evm<Error: Debug>,
    timestamp_us: u64,
) -> (MetadataTxnResult, EvmState) {
    sol! {
        function blockPrologue(uint64 _timestamp_microseconds) external onlyVm whenInitialized;
    }

    let call = blockPrologueCall { _timestamp_microseconds: timestamp_us };
    let input: Bytes = call.abi_encode().into();
    let result =
        evm.transact_system_call(SYSTEM_ADDRESS, BLOCK_MODULE_ADDRESS, input.clone()).unwrap();
    assert!(result.result.is_success(), "Failed to execute blockPrologue: {:?}", result.result);
    (
        MetadataTxnResult {
            result: result.result,
            txn: new_system_call_txn(BLOCK_MODULE_ADDRESS, input),
        },
        result.state,
    )
}
