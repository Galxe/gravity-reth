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
use std::time::Instant;

// New Reconfiguration contract ABI (aligned with
// gravity_chain_core_contracts/src/blocker/IReconfiguration.sol)
sol! {
    contract Reconfiguration {
        function currentEpoch() external view returns (uint64);
        function lastReconfigurationTime() external view returns (uint64);
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
            let start = Instant::now();
            let call = Reconfiguration::currentEpochCall {};
            let input: Bytes = call.abi_encode().into();
            tracing::info!(
                "epoch fetch enter: block_id={}, contract={}, caller={}",
                block_id,
                Self::contract_address(),
                Self::caller_address(),
            );

            let result = self
                .base_fetcher
                .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
                .map_err(|e| {
                    tracing::warn!(
                        "epoch fetch error: block_id={}, elapsed_ms={}, error={:?}",
                        block_id,
                        start.elapsed().as_millis(),
                        e,
                    );
                })
                .ok()?;

            let epoch = Reconfiguration::currentEpochCall::abi_decode_returns(&result)
                .expect("Failed to decode currentEpoch return value");
            tracing::info!(
                "epoch fetch exit: block_id={}, epoch={}, elapsed_ms={}",
                block_id,
                epoch,
                start.elapsed().as_millis(),
            );

            // Convert epoch to bytes
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
