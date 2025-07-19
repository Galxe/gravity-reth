/// Examples of how to use the onchain config fetchers
/// 
/// This module provides practical examples of how to:
/// 1. Set up and use individual config fetchers
/// 2. Mock contract responses for testing
/// 3. Create integration tests
/// 4. Handle different scenarios and edge cases

#[cfg(test)]
mod examples {
    use crate::onchain_config::{
        base::{OnchainConfigFetcher, ConfigFetcher, test_utils::MockEthCall},
        consensus_config::ConsensusConfigFetcher,
        epoch::EpochFetcher,
        validator_set::{ValidatorSetFetcher, ValidatorSet, ValidatorInfo, Commission, ValidatorStatus},
        test_framework::OnchainConfigTestFramework,
    };
    use alloy_primitives::{Address, Bytes, U256};

    /// Example 1: Basic usage of individual config fetchers
    #[test]
    fn example_basic_fetcher_usage() {
        // Create a mock eth call implementation
        let mock_eth_call = MockEthCall::new();
        
        // Create the base fetcher
        let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
        
        // Create specific fetchers
        let consensus_fetcher = ConsensusConfigFetcher::new(&base_fetcher);
        let epoch_fetcher = EpochFetcher::new(&base_fetcher);
        let validator_set_fetcher = ValidatorSetFetcher::new(&base_fetcher);

        // Setup mock responses (you would set these up based on your contract data)
        // ... setup code would go here ...
        
        // Usage example (would actually call the mocked contract):
        // let consensus_config = consensus_fetcher.fetch(100);
        // let epoch = epoch_fetcher.fetch(100);  
        // let validator_set = validator_set_fetcher.fetch(100);
        
        println!("Example demonstrates basic fetcher setup and usage patterns");
    }

    /// Example 2: Setting up mock contract responses for testing
    #[test]
    fn example_mock_contract_setup() {
        use crate::onchain_config::consensus_config::getCurrentConfigCall;
        use crate::onchain_config::epoch::getCurrentEpochCall;
        use crate::onchain_config::validator_set::getValidatorSetCall;

        let mock_eth_call = MockEthCall::new();
        let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());

        // Example: Mock a consensus config response
        let consensus_config_data = b"example_consensus_config_bytes";
        let consensus_call = getCurrentConfigCall {};
        mock_eth_call.set_sol_response(
            ConsensusConfigFetcher::<MockEthCall>::caller_address(),
            ConsensusConfigFetcher::<MockEthCall>::contract_address(),
            consensus_call,
            100, // block number
            Bytes::from(consensus_config_data.to_vec()),
        );

        // Example: Mock an epoch response
        let epoch_call = getCurrentEpochCall {};
        let epoch_response = (U256::from(15), U256::from(1000), U256::from(100)); // epoch, last_transition, interval
        mock_eth_call.set_sol_response(
            EpochFetcher::<MockEthCall>::caller_address(),
            EpochFetcher::<MockEthCall>::contract_address(),
            epoch_call,
            100,
            epoch_response,
        );

        // Example: Mock a validator set response
        let test_validator = ValidatorInfo {
            consensusPublicKey: Bytes::from(b"test_consensus_key".to_vec()),
            feeAddress: Address::from([1u8; 20]),
            voteAddress: Bytes::from(b"test_vote_addr".to_vec()),
            commission: Commission {
                rate: 1000,      // 10%
                maxRate: 5000,   // 50%
                maxChangeRate: 100, // 1%
            },
            moniker: "example_validator".to_string(),
            createdTime: U256::from(1000),
            registered: true,
            stakeCreditAddress: Address::from([2u8; 20]),
            status: ValidatorStatus::ACTIVE,
            votingPower: U256::from(1000),
            validatorIndex: U256::from(0),
            lastEpochActive: U256::from(14),
            updateTime: U256::from(2000),
            operator: Address::from([3u8; 20]),
        };

        let validator_set = ValidatorSet {
            consensusScheme: 0,
            activeValidators: vec![test_validator],
            pendingInactive: vec![],
            pendingActive: vec![],
            totalVotingPower: U256::from(1000),
            totalJoiningPower: U256::from(0),
        };

        let validator_set_call = getValidatorSetCall {};
        mock_eth_call.set_sol_response(
            ValidatorSetFetcher::<MockEthCall>::caller_address(),
            ValidatorSetFetcher::<MockEthCall>::contract_address(),
            validator_set_call,
            100,
            validator_set,
        );

        // Now you can test the fetchers with these mocked responses
        let consensus_fetcher = ConsensusConfigFetcher::new(&base_fetcher);
        let result = consensus_fetcher.fetch(100);
        assert_eq!(result.as_ref(), consensus_config_data);

        println!("Example demonstrates how to set up mock contract responses");
    }

    /// Example 3: Using the comprehensive test framework
    #[test] 
    fn example_comprehensive_test_framework() {
        // Create the test framework - this sets up everything automatically
        let framework = OnchainConfigTestFramework::new()
            .with_block_number(200);

        // The framework provides helper methods to set up common test scenarios
        framework.setup_consensus_config(b"production_config_v2");
        framework.setup_epoch(25, 5000, 200);
        
        // Create a realistic validator set scenario
        let validator_set = framework.setup_complete_validator_set_scenario();
        framework.setup_validator_set(validator_set);

        // Run a complete integration test
        let results = framework.run_integration_test();
        
        // Validate results
        assert!(results.validate().is_ok());
        assert_eq!(results.get_epoch(), 42); // Note: run_integration_test uses hardcoded test values
        
        let decoded_validator_set = results.get_validator_set().unwrap();
        assert_eq!(decoded_validator_set.active_validators.len(), 2);

        println!("Example demonstrates using the comprehensive test framework");
    }

    /// Example 4: Testing different contract upgrade scenarios
    #[test]
    fn example_contract_upgrade_scenarios() {
        let framework = OnchainConfigTestFramework::new();

        // Simulate config changes across different blocks (contract upgrades)
        let blocks_and_configs = vec![
            (100, b"config_v1"),
            (200, b"config_v2_after_upgrade"),
            (300, b"config_v3_final"),
        ];

        for (block_number, config_data) in blocks_and_configs {
            let framework_for_block = OnchainConfigTestFramework::new()
                .with_block_number(block_number);
            
            framework_for_block.setup_consensus_config(config_data);
            
            let result = framework_for_block.consensus_config_fetcher.fetch(block_number);
            assert_eq!(result.as_ref(), config_data);
        }

        println!("Example demonstrates testing contract upgrade scenarios");
    }

    /// Example 5: Testing edge cases and error scenarios
    #[test]
    fn example_edge_cases() {
        let framework = OnchainConfigTestFramework::new();

        // Test with empty validator set
        let empty_validator_set = ValidatorSet {
            consensusScheme: 0,
            activeValidators: vec![],
            pendingInactive: vec![],
            pendingActive: vec![],
            totalVotingPower: U256::from(0),
            totalJoiningPower: U256::from(0),
        };
        
        framework.setup_validator_set(empty_validator_set);
        
        let result = framework.validator_set_fetcher.fetch(framework.block_number);
        let decoded: super::super::gravity_api_types::on_chain_config::validator_set::ValidatorSet = 
            bcs::from_bytes(&result).expect("Should decode empty validator set");
        
        assert_eq!(decoded.active_validators.len(), 0);
        assert_eq!(decoded.total_voting_power, 0);

        // Test with maximum values
        let max_epoch = u64::MAX;
        framework.setup_epoch(max_epoch, u64::MAX, u64::MAX);
        
        let epoch_result = framework.epoch_fetcher.fetch(framework.block_number);
        let decoded_epoch = u64::from_le_bytes(epoch_result.as_ref().try_into().unwrap());
        assert_eq!(decoded_epoch, max_epoch);

        println!("Example demonstrates testing edge cases and boundary conditions");
    }

    /// Example 6: Custom validator creation for specific test scenarios
    #[test]
    fn example_custom_validator_scenarios() {
        let framework = OnchainConfigTestFramework::new();

        // Create validators with different characteristics
        let high_power_validator = framework.create_test_validator(
            10000, // High voting power
            0,
            Address::from([0xAAu8; 20]),
        );

        let low_power_validator = framework.create_test_validator(
            100, // Low voting power
            1,
            Address::from([0xBBu8; 20]),
        );

        let new_validator = framework.create_test_validator(
            500, // Medium voting power
            2,
            Address::from([0xCCu8; 20]),
        );

        // Create a custom validator set scenario
        let custom_validator_set = ValidatorSet {
            consensusScheme: 0,
            activeValidators: vec![high_power_validator, low_power_validator],
            pendingInactive: vec![],
            pendingActive: vec![new_validator],
            totalVotingPower: U256::from(10100), // 10000 + 100
            totalJoiningPower: U256::from(500),   // new validator
        };

        framework.setup_validator_set(custom_validator_set);
        
        let result = framework.validator_set_fetcher.fetch(framework.block_number);
        let decoded: super::super::gravity_api_types::on_chain_config::validator_set::ValidatorSet = 
            bcs::from_bytes(&result).unwrap();
        
        assert_eq!(decoded.active_validators.len(), 2);
        assert_eq!(decoded.pending_active.len(), 1);
        assert_eq!(decoded.total_voting_power, 10100);
        assert_eq!(decoded.total_joining_power, 500);

        println!("Example demonstrates creating custom validator scenarios");
    }

    /// Example 7: Using the macro for quick integration tests
    #[test]
    fn example_macro_usage() {
        // The create_integration_test! macro provides a quick way to create comprehensive tests
        // Here's how you would use it in your own test files:
        
        /*
        use crate::create_integration_test;
        
        create_integration_test!(my_integration_test);
        
        // This creates a test function that:
        // 1. Sets up the test framework
        // 2. Runs a complete integration test
        // 3. Validates all results
        // 4. Checks expected values
        */

        // For demonstration, we'll manually do what the macro does:
        let framework = OnchainConfigTestFramework::new();
        let results = framework.run_integration_test();
        
        results.validate().expect("Integration test validation failed");
        
        assert_eq!(results.get_epoch(), 42);
        assert_eq!(results.consensus_config.as_ref(), b"test_consensus_config_v1");
        
        let validator_set = results.get_validator_set().expect("Failed to decode validator set");
        assert_eq!(validator_set.active_validators.len(), 2);
        assert_eq!(validator_set.pending_inactive.len(), 1);
        assert_eq!(validator_set.pending_active.len(), 1);
        assert_eq!(validator_set.total_voting_power, 450);
        assert_eq!(validator_set.total_joining_power, 75);

        println!("Example demonstrates integration test patterns");
    }
} 