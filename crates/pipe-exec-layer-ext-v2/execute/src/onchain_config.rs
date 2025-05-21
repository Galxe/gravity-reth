use crate::PipeExecLayerApi;
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes, TxKind};
use alloy_rpc_types_eth::{TransactionInput, TransactionRequest};
use gravity_api_types::config_storage::{ConfigStorage, OnChainConfig};
use reth_rpc_eth_api::{
    EthApiServer, EthApiTypes, RpcBlock, RpcHeader, RpcReceipt, RpcTransaction,
};

pub struct OnchainConfigFetcher<EthApi> {
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
    pub fn new(eth_api: EthApi) -> Self {
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

    pub fn fetch_epoch(&self, block_number: u64) -> u64 {
        todo!()
    }

    pub fn fetch_config_bytes(&self, config_name: OnChainConfig, block_number: u64) -> Bytes {
        todo!()
    }
}
