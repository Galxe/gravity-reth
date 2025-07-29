use super::base::{ConfigFetcher, OnchainConfigFetcher};
use super::{SYSTEM_CALLER, VALIDATOR_MANAGER_ADDR};
use super::types::{getValidatorSetCall, convert_validator_set_to_bcs};
use alloy_primitives::{Address, Bytes};
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::helpers::EthCall;

// BCS for serialization

/// Fetcher for validator set information
pub struct ValidatorSetFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ValidatorSetFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    pub fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher<EthApi> for ValidatorSetFetcher<'a, EthApi>
where
    EthApi: EthCall,
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