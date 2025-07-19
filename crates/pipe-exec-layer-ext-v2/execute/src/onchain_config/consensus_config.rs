use super::base::{ConfigFetcher, OnchainConfigFetcher};
use super::{GRAVITY_FRAMEWORK_ADDRESS, CONSENSUS_CONFIG_CONTRACT_ADDRESS};
use alloy_primitives::{Address, Bytes};
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::helpers::EthCall;

sol! {
    function getCurrentConfig() external view returns (bytes memory);
}

/// Fetcher for consensus configuration
pub struct ConsensusConfigFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    pub fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher<EthApi> for ConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    fn fetch(&self, block_number: u64) -> Bytes {
        let call = getCurrentConfigCall {};
        let input: Bytes = call.abi_encode().into();
        
        let result = self.base_fetcher.eth_call(
            Self::caller_address(),
            Self::contract_address(),
            input,
            block_number,
        );
        
        getCurrentConfigCall::abi_decode_returns(&result, false)
            .expect("Failed to decode getCurrentConfig return value")
            ._0
    }

    fn contract_address() -> Address {
        CONSENSUS_CONFIG_CONTRACT_ADDRESS
    }

    fn caller_address() -> Address {
        GRAVITY_FRAMEWORK_ADDRESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onchain_config::base::test_utils::*;
    use alloy_primitives::Bytes;

    #[test]
    fn test_consensus_config_fetch() {
        let mock_eth_call = MockEthCall::new();
        let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
        let fetcher = ConsensusConfigFetcher::new(&base_fetcher);

        // Setup mock response
        let expected_config = Bytes::from(b"test_consensus_config".to_vec());
        let call = getCurrentConfigCall {};
        
        mock_eth_call.set_sol_response(
            ConsensusConfigFetcher::<MockEthCall>::caller_address(),
            ConsensusConfigFetcher::<MockEthCall>::contract_address(),
            call,
            100,
            expected_config.clone(),
        );

        // Test fetch
        let result = fetcher.fetch(100);
        assert_eq!(result, expected_config);
    }

    #[test]
    fn test_consensus_config_addresses() {
        assert_eq!(
            ConsensusConfigFetcher::<MockEthCall>::contract_address(),
            CONSENSUS_CONFIG_CONTRACT_ADDRESS
        );
        assert_eq!(
            ConsensusConfigFetcher::<MockEthCall>::caller_address(),
            GRAVITY_FRAMEWORK_ADDRESS
        );
    }

    #[test]
    fn test_consensus_config_with_different_block_numbers() {
        let mock_eth_call = MockEthCall::new();
        let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
        let fetcher = ConsensusConfigFetcher::new(&base_fetcher);

        // Setup different responses for different block numbers
        let config_v1 = Bytes::from(b"config_v1".to_vec());
        let config_v2 = Bytes::from(b"config_v2".to_vec());
        let call = getCurrentConfigCall {};
        
        mock_eth_call.set_sol_response(
            ConsensusConfigFetcher::<MockEthCall>::caller_address(),
            ConsensusConfigFetcher::<MockEthCall>::contract_address(),
            call.clone(),
            100,
            config_v1.clone(),
        );
        
        mock_eth_call.set_sol_response(
            ConsensusConfigFetcher::<MockEthCall>::caller_address(),
            ConsensusConfigFetcher::<MockEthCall>::contract_address(),
            call,
            200,
            config_v2.clone(),
        );

        // Test different block numbers return different configs
        assert_eq!(fetcher.fetch(100), config_v1);
        assert_eq!(fetcher.fetch(200), config_v2);
    }
} 