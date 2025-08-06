//! Fetcher for validator set information

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    types::{convert_validator_set_to_bcs, getValidatorSetCall},
    SYSTEM_CALLER, VALIDATOR_MANAGER_ADDR,
};
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

// BCS for serialization

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
    fn fetch(&self, block_number: u64) -> Bytes {
        let call = getValidatorSetCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self.base_fetcher.eth_call(
            Self::caller_address(),
            Self::contract_address(),
            input,
            block_number,
        );

        // Decode the Solidity validator set
        let solidity_validator_set = getValidatorSetCall::abi_decode_returns(&result)
            .expect("Failed to decode getValidatorSet return value");

        convert_validator_set_to_bcs(&solidity_validator_set)
    }

    fn contract_address() -> Address {
        VALIDATOR_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
