//! Fetcher for consensus configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    CONSENSUS_CONFIG_CONTRACT_ADDRESS, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

sol! {
    function getCurrentConfig() external view returns (bytes memory);
}

/// Fetcher for consensus configuration
#[derive(Debug)]
pub struct ConsensusConfigFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new consensus config fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for ConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let call = getCurrentConfigCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
            .map_err(|e| {
                tracing::warn!("Failed to fetch consensus config at block {}: {:?}", block_id, e);
            })
            .ok()?;

        Some(
            getCurrentConfigCall::abi_decode_returns(&result)
                .expect("Failed to decode getCurrentConfig return value"),
        )
    }

    fn contract_address() -> Address {
        CONSENSUS_CONFIG_CONTRACT_ADDRESS
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
