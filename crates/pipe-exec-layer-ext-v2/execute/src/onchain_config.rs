use std::ops::Deref;

use crate::{ExecuteOrderedBlockResult, OrderedBlock};
use alloy_eips::BlockId;
use alloy_primitives::{address, Address, Bytes, TxKind};
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use gravity_api_types::config_storage::OnChainConfig;
use reth_evm::Evm;
use reth_rpc_eth_api::{
    EthApiServer, EthApiTypes, RpcBlock, RpcHeader, RpcReceipt, RpcTransaction,
};
use revm_primitives::{EvmState, ExecutionResult};

const SYSTEM_ADDRESS: Address = address!("00000000000000000000000000000000000000f0");

#[derive(Debug)]
pub(crate) struct OnchainConfigFetcher<EthApi> {
    eth_api: EthApi,
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
        Self { eth_api }
    }

    /// Simulate the call to the contract at block number and return the result.
    /// Return None if the block is not found.
    async fn eth_call(
        &self,
        from: Address,
        to: Address,
        input: Bytes,
        block_number: u64,
    ) -> Option<Bytes> {
        // TODO(nekomoto): Handle the case where the block is not found.
        self.eth_api
            .call(
                TransactionRequest {
                    from: Some(from),
                    to: Some(TxKind::Call(to)),
                    input: TransactionInput::new(input),
                    ..Default::default()
                },
                Some(BlockId::from(block_number)),
                None,
                None,
            )
            .await
            .ok()
    }

    pub(crate) fn fetch_epoch(&self, block_number: u64) -> u64 {
        todo!()
    }

    pub(crate) fn fetch_config_bytes(
        &self,
        config_name: OnChainConfig,
        block_number: u64,
    ) -> bytes::Bytes {
        todo!()
    }
}

pub(crate) struct MetadataTxnResultAndState {
    pub result: MetadataTxnResult,
    pub state: EvmState,
}

impl MetadataTxnResultAndState {
    pub fn into_executed_ordered_block_result(
        self,
        ordered_block: &OrderedBlock,
    ) -> ExecuteOrderedBlockResult {
        todo!()
    }
}

pub(crate) struct MetadataTxnResult(pub ExecutionResult);

impl Deref for MetadataTxnResult {
    type Target = ExecutionResult;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl MetadataTxnResult {
    pub(crate) fn emit_new_epoch(&self) -> bool {
        todo!()
    }

    pub(crate) fn insert_to_executed_ordered_block_result(
        self,
        result: &mut ExecuteOrderedBlockResult,
    ) {
        todo!()
    }
}

pub(crate) fn transact_metadata_contract_call(
    evm: &mut impl Evm,
    timestamp_us: u64,
) -> MetadataTxnResultAndState {
    todo!();
}
