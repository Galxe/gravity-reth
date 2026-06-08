//! Fetcher for validator set information

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    types::{
        convert_validators_to_bcs, getActiveValidatorsCall, getPendingActiveValidatorsCall,
        getPendingInactiveValidatorsCall,
    },
    SYSTEM_CALLER, VALIDATOR_MANAGER_ADDR,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

/// Fetcher for validator set information
#[derive(Debug)]
pub struct ValidatorSetFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ValidatorSetFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new validator set fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for ValidatorSetFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        // 1. Fetch active validators
        let active_validators = {
            let call = getActiveValidatorsCall {};
            let input: Bytes = call.abi_encode().into();
            let result = self
                .base_fetcher
                .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
                .map_err(|e| {
                    tracing::warn!(
                        "Failed to fetch active validators at block {}: {:?}",
                        block_id,
                        e
                    );
                })
                .ok()?;
            getActiveValidatorsCall::abi_decode_returns(&result)
                .expect("Failed to decode getActiveValidators return value")
        };

        // 2. Fetch pending active validators
        let pending_active = {
            let call = getPendingActiveValidatorsCall {};
            let input: Bytes = call.abi_encode().into();
            let result = self
                .base_fetcher
                .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
                .map_err(|e| {
                    tracing::warn!(
                        "Failed to fetch pending active validators at block {}: {:?}",
                        block_id,
                        e
                    );
                })
                .ok()?;
            getPendingActiveValidatorsCall::abi_decode_returns(&result)
                .expect("Failed to decode getPendingActiveValidators return value")
        };

        // 3. Fetch pending inactive validators
        let pending_inactive = {
            let call = getPendingInactiveValidatorsCall {};
            let input: Bytes = call.abi_encode().into();
            let result = self
                .base_fetcher
                .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
                .map_err(|e| {
                    tracing::warn!(
                        "Failed to fetch pending inactive validators at block {}: {:?}",
                        block_id,
                        e
                    );
                })
                .ok()?;
            getPendingInactiveValidatorsCall::abi_decode_returns(&result)
                .expect("Failed to decode getPendingInactiveValidators return value")
        };

        // Convert to BCS-encoded ValidatorSet format with all validator lists
        Some(convert_validators_to_bcs(&active_validators, &pending_active, &pending_inactive))
    }

    fn contract_address() -> Address {
        VALIDATOR_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
