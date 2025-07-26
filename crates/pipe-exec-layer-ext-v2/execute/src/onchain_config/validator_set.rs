use super::base::{ConfigFetcher, OnchainConfigFetcher};
use super::{SYSTEM_CALLER, VALIDATOR_MANAGER_ADDR};
use super::types::{getValidatorSetCall, convert_validator_info};
use alloy_primitives::{Address, Bytes};
use alloy_sol_types::SolCall;
use gravity_api_types::{
    on_chain_config::validator_set::ValidatorSet as GravityValidatorSet,
};
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
        let solidity_validator_set = getValidatorSetCall::abi_decode_returns(&result, false)
            .expect("Failed to decode getValidatorSet return value");

        // Convert to Gravity validator set
        let gravity_validator_set = GravityValidatorSet {
            active_validators: solidity_validator_set._0.activeValidators
                .iter()
                .map(convert_validator_info)
                .collect(),
            pending_inactive: solidity_validator_set._0.pendingInactive
                .iter()
                .map(convert_validator_info)
                .collect(),
            pending_active: solidity_validator_set._0.pendingActive
                .iter()
                .map(convert_validator_info)
                .collect(),
            total_voting_power: solidity_validator_set._0.totalVotingPower.to::<u128>(),
            total_joining_power: solidity_validator_set._0.totalJoiningPower.to::<u128>(),
        };
        
        // Serialize to BCS format (gravity-aptos standard)
        bcs::to_bytes(&gravity_validator_set)
            .expect("Failed to serialize validator set")
            .into()
    }

    fn contract_address() -> Address {
        VALIDATOR_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use crate::onchain_config::base::test_utils::*;
    // use alloy_primitives::{Address as AlloyAddress, U256};

    // fn create_test_validator_info(
    //     voting_power: u64,
    //     validator_index: u64,
    //     fee_address: AlloyAddress,
    // ) -> ValidatorInfo {
    //     ValidatorInfo {
    //         consensusPublicKey: Bytes::from(vec![1, 2, 3, 4]),
    //         commission: Commission {
    //             rate: 1000,
    //             maxRate: 5000,
    //             maxChangeRate: 100,
    //         },
    //         moniker: "test_validator".to_string(),
    //         registered: true,
    //         stakeCreditAddress: AlloyAddress::from([1u8; 20]),
    //         status: ValidatorStatus::ACTIVE,
    //         votingPower: U256::from(voting_power),
    //         validatorIndex: U256::from(validator_index),
    //         updateTime: U256::from(2000),
    //         operator: AlloyAddress::from([2u8; 20]),
    //         validatorNetworkAddresses: todo!(),
    //         fullnodeNetworkAddresses: todo!(),
    //     }
    // }

    // fn create_test_validator_set() -> ValidatorSet {
    //     let validator1 = create_test_validator_info(100, 0, AlloyAddress::from([10u8; 20]));
    //     let validator2 = create_test_validator_info(200, 1, AlloyAddress::from([20u8; 20]));
        
    //     ValidatorSet {
    //         activeValidators: vec![validator1, validator2],
    //         pendingInactive: vec![],
    //         pendingActive: vec![],
    //         totalVotingPower: U256::from(300),
    //         totalJoiningPower: U256::from(0),
    //     }
    // }

    // #[test]
    // fn test_validator_set_fetch() {
    //     let mock_eth_call = MockEthCall::new();
    //     let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
    //     let fetcher = ValidatorSetFetcher::new(&base_fetcher);

    //     // Setup mock response
    //     let test_validator_set = create_test_validator_set();
    //     let call = getValidatorSetCall {};
        
    //     mock_eth_call.set_sol_response(
    //         ValidatorSetFetcher::<MockEthCall>::caller_address(),
    //         ValidatorSetFetcher::<MockEthCall>::contract_address(),
    //         call,
    //         100,
    //         test_validator_set,
    //     );

    //     // Test fetch
    //     let result = fetcher.fetch(100);
        
    //     // Verify that we get valid BCS-encoded data
    //     assert!(!result.is_empty());
        
    //     // Try to decode the result back to verify it's valid BCS
    //     let decoded: Result<GravityValidatorSet, _> = bcs::from_bytes(&result);
    //     assert!(decoded.is_ok());
        
    //     let validator_set = decoded.unwrap();
    //     assert_eq!(validator_set.active_validators.len(), 2);
    //     assert_eq!(validator_set.total_voting_power, 300);
    //     assert_eq!(validator_set.total_joining_power, 0);
    // }

    // #[test]
    // fn test_validator_set_addresses() {
    //     assert_eq!(
    //         ValidatorSetFetcher::<MockEthCall>::contract_address(),
    //         VALIDATOR_MANAGER_ADDR
    //     );
    //     assert_eq!(
    //         ValidatorSetFetcher::<MockEthCall>::caller_address(),
    //         SYSTEM_CALLER
    //     );
    // }

    // #[test]
    // fn test_validator_info_conversion() {
    //     let test_address = AlloyAddress::from([15u8; 20]);
    //     let validator_info = create_test_validator_info(500, 42, test_address);
        
    //     let gravity_info = ValidatorSetFetcher::<MockEthCall>::convert_validator_info(&validator_info);
        
    //     assert_eq!(gravity_info.consensus_voting_power(), 500);
    //     assert_eq!(gravity_info.config().validator_index, 42);
    //     assert_eq!(gravity_info.consensus_public_key(), &vec![1, 2, 3, 4]);
    // }

    // #[test]
    // fn test_empty_validator_set() {
    //     let mock_eth_call = MockEthCall::new();
    //     let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
    //     let fetcher = ValidatorSetFetcher::new(&base_fetcher);

    //     // Setup empty validator set
    //     let empty_validator_set = ValidatorSet {
    //         activeValidators: vec![],
    //         pendingInactive: vec![],
    //         pendingActive: vec![],
    //         totalVotingPower: U256::from(0),
    //         totalJoiningPower: U256::from(0),
    //     };
        
    //     let call = getValidatorSetCall {};
    //     mock_eth_call.set_sol_response(
    //         ValidatorSetFetcher::<MockEthCall>::caller_address(),
    //         ValidatorSetFetcher::<MockEthCall>::contract_address(),
    //         call,
    //         100,
    //         empty_validator_set,
    //     );

    //     let result = fetcher.fetch(100);
    //     let decoded: GravityValidatorSet = bcs::from_bytes(&result).unwrap();
        
    //     assert_eq!(decoded.active_validators.len(), 0);
    //     assert_eq!(decoded.total_voting_power, 0);
    // }

    // #[test]
    // fn test_validator_set_with_pending_validators() {
    //     let mock_eth_call = MockEthCall::new();
    //     let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
    //     let fetcher = ValidatorSetFetcher::new(&base_fetcher);

    //     // Create validator set with pending validators
    //     let active_validator = create_test_validator_info(100, 0, AlloyAddress::from([10u8; 20]));
    //     let pending_active = create_test_validator_info(50, 1, AlloyAddress::from([20u8; 20]));
    //     let pending_inactive = create_test_validator_info(75, 2, AlloyAddress::from([30u8; 20]));
        
    //     let validator_set = ValidatorSet {
    //         activeValidators: vec![active_validator],
    //         pendingInactive: vec![pending_inactive],
    //         pendingActive: vec![pending_active],
    //         totalVotingPower: U256::from(175), // active + pending_inactive
    //         totalJoiningPower: U256::from(50), // pending_active
    //     };
        
    //     let call = getValidatorSetCall {};
    //     mock_eth_call.set_sol_response(
    //         ValidatorSetFetcher::<MockEthCall>::caller_address(),
    //         ValidatorSetFetcher::<MockEthCall>::contract_address(),
    //         call,
    //         100,
    //         validator_set,
    //     );

    //     let result = fetcher.fetch(100);
    //     let decoded: GravityValidatorSet = bcs::from_bytes(&result).unwrap();
        
    //     assert_eq!(decoded.active_validators.len(), 1);
    //     assert_eq!(decoded.pending_inactive.len(), 1);
    //     assert_eq!(decoded.pending_active.len(), 1);
    //     assert_eq!(decoded.total_voting_power, 175);
    //     assert_eq!(decoded.total_joining_power, 50);
    // }
} 