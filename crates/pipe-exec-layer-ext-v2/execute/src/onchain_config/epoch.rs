//! Fetcher for epoch information

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    EPOCH_MANAGER_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

sol! {
    contract EpochManager {
        function getCurrentEpochInfo() external view returns (uint256 epoch, uint256 lastTransitionTime, uint256 interval);
    }
}

/// Fetcher for epoch information
#[derive(Debug)]
pub struct EpochFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> EpochFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new epoch fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for EpochFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        #[cfg(feature = "pipe_test")]
        {
            // For testing, return epoch 0
            Some(Bytes::from(0u64.to_le_bytes().to_vec()))
        }

        #[cfg(not(feature = "pipe_test"))]
        {
            let call = EpochManager::getCurrentEpochInfoCall {};
            let input: Bytes = call.abi_encode().into();

            // uint64 currentEpoch = uint64(IEpochManager(EPOCH_MANAGER_ADDR).currentEpoch());
            let result = self
                .base_fetcher
                .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
                .map_err(|e| {
                    tracing::warn!("Failed to fetch epoch info at block {}: {:?}", block_id, e);
                })
                .ok()?;

            let epoch_info = EpochManager::getCurrentEpochInfoCall::abi_decode_returns(&result)
                .expect("Failed to decode getCurrentEpoch return value");

            // Convert epoch to bytes
            let epoch: u64 = epoch_info.epoch.to::<u64>();
            Some(Bytes::from(epoch.to_le_bytes().to_vec()))
        }
    }

    fn contract_address() -> Address {
        EPOCH_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
