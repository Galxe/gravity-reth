use super::base::test_utils::{MockEthCall, ConfigFetcherTestFramework};
use super::{
    consensus_config::ConsensusConfigFetcher,
    epoch::EpochFetcher,
    validator_set::ValidatorSetFetcher,
    base::OnchainConfigFetcher,
};
use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_types::{SolCall, SolValue};
use std::collections::HashMap;
use gravity_api_types;

/// Comprehensive test framework for all onchain config fetchers
pub struct OnchainConfigTestFramework {
    pub mock_eth_call: MockEthCall,
    pub base_fetcher: OnchainConfigFetcher<MockEthCall>,
    pub consensus_config_fetcher: ConsensusConfigFetcher<'static, MockEthCall>,
    pub epoch_fetcher: EpochFetcher<'static, MockEthCall>,
    pub validator_set_fetcher: ValidatorSetFetcher<'static, MockEthCall>,
    pub block_number: u64,
}

// impl OnchainConfigTestFramework {
//     /// Create a new test framework with all fetchers initialized
//     pub fn new() -> Self {
//         let mock_eth_call = MockEthCall::new();
//         let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
        
//         // We need to use Box::leak to make the base_fetcher have 'static lifetime for the test
//         let base_fetcher_static = Box::leak(Box::new(base_fetcher));
        
//         Self {
//             mock_eth_call: mock_eth_call.clone(),
//             base_fetcher: OnchainConfigFetcher::new(mock_eth_call.clone()),
//             consensus_config_fetcher: ConsensusConfigFetcher::new(base_fetcher_static),
//             epoch_fetcher: EpochFetcher::new(base_fetcher_static),
//             validator_set_fetcher: ValidatorSetFetcher::new(base_fetcher_static),
//             block_number: 100,
//         }
//     }

//     /// Set the block number for tests
//     pub fn with_block_number(mut self, block_number: u64) -> Self {
//         self.block_number = block_number;
//         self
//     }

//     /// Setup a consensus config response
//     pub fn setup_consensus_config(&self, config_data: &[u8]) {
//         use super::consensus_config::getCurrentConfigCall;
        
//         let call = getCurrentConfigCall {};
//         let response = Bytes::from(config_data.to_vec());
        
//         self.mock_eth_call.set_sol_response(
//             ConsensusConfigFetcher::<MockEthCall>::caller_address(),
//             ConsensusConfigFetcher::<MockEthCall>::contract_address(),
//             call,
//             self.block_number,
//             response,
//         );
//     }

//     /// Setup an epoch response
//     pub fn setup_epoch(&self, epoch: u64, last_transition_time: u64, interval: u64) {
//         use super::epoch::getCurrentEpochCall;
        
//         let call = getCurrentEpochCall {};
//         let response = (U256::from(epoch), U256::from(last_transition_time), U256::from(interval));
        
//         self.mock_eth_call.set_sol_response(
//             EpochFetcher::<MockEthCall>::caller_address(),
//             EpochFetcher::<MockEthCall>::contract_address(),
//             call,
//             self.block_number,
//             response,
//         );
//     }

//     /// Setup a validator set response
//     pub fn setup_validator_set(&self, validator_set: super::validator_set::ValidatorSet) {
//         use super::validator_set::getValidatorSetCall;
        
//         let call = getValidatorSetCall {};
        
//         self.mock_eth_call.set_sol_response(
//             ValidatorSetFetcher::<MockEthCall>::caller_address(),
//             ValidatorSetFetcher::<MockEthCall>::contract_address(),
//             call,
//             self.block_number,
//             validator_set,
//         );
//     }

//     /// Create a test validator for use in validator set tests
//     pub fn create_test_validator(
//         &self,
//         voting_power: u64,
//         validator_index: u64,
//         fee_address: Address,
//     ) -> super::validator_set::ValidatorInfo {
//         use super::validator_set::{ValidatorInfo, Commission, ValidatorStatus};
        
//         ValidatorInfo {
//             consensusPublicKey: Bytes::from(format!("consensus_key_{}", validator_index).into_bytes()),
//             commission: Commission {
//                 rate: 1000, // 10%
//                 maxRate: 5000, // 50%
//                 maxChangeRate: 100, // 1%
//             },
//             moniker: format!("validator_{}", validator_index),
//             registered: true,
//             stakeCreditAddress: Address::from([validator_index as u8; 20]),
//             status: ValidatorStatus::ACTIVE,
//             votingPower: U256::from(voting_power),
//             validatorIndex: U256::from(validator_index),
//             updateTime: U256::from(2000 + validator_index),
//             operator: Address::from([(validator_index + 100) as u8; 20]),
//             validatorNetworkAddresses: todo!(),
//             fullnodeNetworkAddresses: todo!(),
//         }
//     }

//     /// Create a complete test scenario with multiple validators
//     pub fn setup_complete_validator_set_scenario(&self) -> super::validator_set::ValidatorSet {
//         use super::validator_set::ValidatorSet;
        
//         let validator1 = self.create_test_validator(100, 0, Address::from([10u8; 20]));
//         let validator2 = self.create_test_validator(200, 1, Address::from([20u8; 20]));
//         let validator3 = self.create_test_validator(150, 2, Address::from([30u8; 20]));
//         let pending_validator = self.create_test_validator(75, 3, Address::from([40u8; 20]));
        
//         ValidatorSet {
//             activeValidators: vec![validator1, validator2],
//             pendingInactive: vec![validator3],
//             pendingActive: vec![pending_validator],
//             totalVotingPower: U256::from(450), // 100 + 200 + 150
//             totalJoiningPower: U256::from(75), // pending_validator
//         }
//     }

//     /// Run a complete integration test scenario
//     pub fn run_integration_test(&self) -> IntegrationTestResults {
//         // Setup test data
//         self.setup_consensus_config(b"test_consensus_config_v1");
//         self.setup_epoch(42, 1000, 100);
//         let validator_set = self.setup_complete_validator_set_scenario();
//         self.setup_validator_set(validator_set);

//         // Fetch all configs
//         let consensus_result = self.consensus_config_fetcher.fetch(self.block_number);
//         let epoch_result = self.epoch_fetcher.fetch(self.block_number);
//         let validator_set_result = self.validator_set_fetcher.fetch(self.block_number);

//         IntegrationTestResults {
//             consensus_config: consensus_result,
//             epoch: epoch_result,
//             validator_set: validator_set_result,
//             block_number: self.block_number,
//         }
//     }
// }

// /// Results from an integration test
// pub struct IntegrationTestResults {
//     pub consensus_config: Bytes,
//     pub epoch: Bytes,
//     pub validator_set: Bytes,
//     pub block_number: u64,
// }

// impl IntegrationTestResults {
//     /// Validate that all results are non-empty and properly formatted
//     pub fn validate(&self) -> Result<(), String> {
//         if self.consensus_config.is_empty() {
//             return Err("Consensus config is empty".to_string());
//         }

//         if self.epoch.is_empty() {
//             return Err("Epoch is empty".to_string());
//         }

//         if self.validator_set.is_empty() {
//             return Err("Validator set is empty".to_string());
//         }

//         // Try to decode epoch
//         if self.epoch.len() != 8 {
//             return Err("Epoch should be 8 bytes".to_string());
//         }

//         let epoch_value = u64::from_le_bytes(
//             self.epoch.as_ref().try_into()
//                 .map_err(|_| "Failed to convert epoch bytes")?
//         );

//         if epoch_value != 42 {
//             return Err(format!("Expected epoch 42, got {}", epoch_value));
//         }

//         // Try to decode validator set
//         let validator_set: Result<gravity_api_types::on_chain_config::validator_set::ValidatorSet, _> = 
//             bcs::from_bytes(&self.validator_set);
        
//         if validator_set.is_err() {
//             return Err("Failed to decode validator set as BCS".to_string());
//         }

//         let vs = validator_set.unwrap();
//         if vs.active_validators.len() != 2 {
//             return Err(format!("Expected 2 active validators, got {}", vs.active_validators.len()));
//         }

//         Ok(())
//     }

//     /// Get the decoded epoch value
//     pub fn get_epoch(&self) -> u64 {
//         u64::from_le_bytes(self.epoch.as_ref().try_into().unwrap_or([0; 8]))
//     }

//     /// Get the decoded validator set
//     pub fn get_validator_set(&self) -> Result<gravity_api_types::on_chain_config::validator_set::ValidatorSet, bcs::Error> {
//         bcs::from_bytes(&self.validator_set)
//     }
// }

// /// Macro to create a comprehensive test for all fetchers
// #[macro_export]
// macro_rules! create_integration_test {
//     ($test_name:ident) => {
//         #[test]
//         fn $test_name() {
//             use crate::onchain_config::test_framework::OnchainConfigTestFramework;
            
//             let framework = OnchainConfigTestFramework::new();
//             let results = framework.run_integration_test();
            
//             // Validate results
//             results.validate().expect("Integration test validation failed");
            
//             // Additional assertions
//             assert_eq!(results.get_epoch(), 42);
//             assert_eq!(results.consensus_config.as_ref(), b"test_consensus_config_v1");
            
//             let validator_set = results.get_validator_set().expect("Failed to decode validator set");
//             assert_eq!(validator_set.active_validators.len(), 2);
//             assert_eq!(validator_set.pending_inactive.len(), 1);
//             assert_eq!(validator_set.pending_active.len(), 1);
//             assert_eq!(validator_set.total_voting_power, 450);
//             assert_eq!(validator_set.total_joining_power, 75);
//         }
//     };
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_framework_creation() {
//         let framework = OnchainConfigTestFramework::new();
//         assert_eq!(framework.block_number, 100);
//     }

//     #[test]
//     fn test_framework_with_custom_block_number() {
//         let framework = OnchainConfigTestFramework::new().with_block_number(200);
//         assert_eq!(framework.block_number, 200);
//     }

//     #[test]
//     fn test_create_test_validator() {
//         let framework = OnchainConfigTestFramework::new();
//         let validator = framework.create_test_validator(500, 42, Address::from([15u8; 20]));
        
//         assert_eq!(validator.votingPower, U256::from(500));
//         assert_eq!(validator.validatorIndex, U256::from(42));
//         assert_eq!(validator.feeAddress, Address::from([15u8; 20]));
//         assert_eq!(validator.moniker, "validator_42");
//     }

//     create_integration_test!(test_full_integration);

//     #[test]
//     fn test_integration_results_validation() {
//         let framework = OnchainConfigTestFramework::new();
//         let results = framework.run_integration_test();
        
//         // Test validation passes
//         assert!(results.validate().is_ok());
        
//         // Test individual getters
//         assert_eq!(results.get_epoch(), 42);
        
//         let validator_set = results.get_validator_set().unwrap();
//         assert_eq!(validator_set.total_voting_power, 450);
//     }

//     #[test]
//     fn test_multiple_block_numbers() {
//         let framework = OnchainConfigTestFramework::new();
        
//         // Setup different data for different blocks
//         framework.setup_consensus_config(b"config_block_100");
        
//         let framework_200 = OnchainConfigTestFramework::new().with_block_number(200);
//         framework_200.setup_consensus_config(b"config_block_200");
        
//         // Fetch from different blocks
//         let result_100 = framework.consensus_config_fetcher.fetch(100);
//         let result_200 = framework_200.consensus_config_fetcher.fetch(200);
        
//         // Results should be different
//         assert_ne!(result_100, result_200);
//     }
// } 