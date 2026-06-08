//! Fetcher for validator performance tracking
//! Reads performance data from the ValidatorPerformanceTracker EVM contract

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    types::{convert_performances_to_bcs, getAllPerformancesCall},
    PERFORMANCE_TRACKER_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

/// Fetcher for validator performance data
#[derive(Debug)]
pub struct ValidatorPerformanceFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ValidatorPerformanceFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new validator performance fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for ValidatorPerformanceFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        // Fetch all performances from the EVM contract
        let performances = {
            let call = getAllPerformancesCall {};
            let input: Bytes = call.abi_encode().into();

            let call_result = self.base_fetcher.eth_call(
                Self::caller_address(),
                Self::contract_address(),
                input,
                block_id,
            );

            let result = match call_result {
                Ok(res) => res,
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch validator performances at block {}: {:?}",
                        block_id,
                        e
                    );
                    return None;
                }
            };

            match getAllPerformancesCall::abi_decode_returns(&result) {
                Ok(decoded) => decoded,
                Err(e) => {
                    // Case A: empty result means contract has no data yet, this is normal
                    if result.is_empty() {
                        tracing::debug!(target: "validator_performance", ?block_id, "no performance data yet");
                        return None;
                    }
                    // Case B: non-empty result but decode failed → ABI mismatch, must not be silent
                    panic!(
                        "ValidatorPerformanceTracker ABI mismatch at block {:?}: {:?}, raw bytes = {:?}",
                        block_id, e, result
                    );
                }
            }
        };

        // Convert the IndividualPerformance array into BCS-encoded ValidatorPerformances
        Some(convert_performances_to_bcs(&performances))
    }

    fn contract_address() -> Address {
        PERFORMANCE_TRACKER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
