use super::base::{ConfigFetcher, OnchainConfigFetcher};
use super::{EPOCH_MANAGER_ADDR, SYSTEM_CALLER};
use alloy_primitives::{Address, Bytes, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::helpers::EthCall;

sol! {
    function getCurrentEpochInfo() external view returns (uint256 epoch, uint256 lastTransitionTime, uint256 interval);
}

/// Fetcher for epoch information
pub struct EpochFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> EpochFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    pub fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher<EthApi> for EpochFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    fn fetch(&self, block_number: u64) -> Bytes {
        #[cfg(feature = "pipe_test")]
        {
            // For testing, return epoch 0
            return Bytes::from(0u64.to_le_bytes().to_vec());
        }

        #[cfg(not(feature = "pipe_test"))]
        {
            let call = getCurrentEpochInfoCall {};
            let input: Bytes = call.abi_encode().into();

            // uint64 currentEpoch = uint64(IEpochManager(EPOCH_MANAGER_ADDR).currentEpoch());
            let result = self.base_fetcher.eth_call(
                Self::caller_address(),
                Self::contract_address(),
                input,
                block_number,
            );

            let epoch_info = getCurrentEpochInfoCall::abi_decode_returns(&result, false)
                .expect("Failed to decode getCurrentEpoch return value");

            // Convert epoch to bytes
            let epoch: u64 = epoch_info.epoch.to::<u64>();
            Bytes::from(epoch.to_le_bytes().to_vec())
        }
    }

    fn contract_address() -> Address {
        EPOCH_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::onchain_config::base::test_utils::*;
//     use alloy_primitives::U256;

//     #[test]
//     fn test_epoch_fetch() {
//         let mock_eth_call = MockEthCall::new();
//         let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
//         let fetcher = EpochFetcher::new(&base_fetcher);

//         // Setup mock response
//         let expected_epoch = 42u64;
//         let epoch_response = (U256::from(expected_epoch), U256::from(1000), U256::from(100));
//         let call = getCurrentEpochCall {};

//         mock_eth_call.set_sol_response(
//             EpochFetcher::<MockEthCall>::caller_address(),
//             EpochFetcher::<MockEthCall>::contract_address(),
//             call,
//             100,
//             epoch_response,
//         );

//         // Test fetch
//         let result = fetcher.fetch(100);
//         let epoch_bytes = expected_epoch.to_le_bytes();
//         assert_eq!(result.as_ref(), &epoch_bytes);
//     }

//     #[test]
//     fn test_epoch_addresses() {
//         assert_eq!(
//             EpochFetcher::<MockEthCall>::contract_address(),
//             EPOCH_MANAGER_ADDR
//         );
//         assert_eq!(
//             EpochFetcher::<MockEthCall>::caller_address(),
//             SYSTEM_CALLER
//         );
//     }

//     #[test]
//     fn test_epoch_progression() {
//         let mock_eth_call = MockEthCall::new();
//         let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
//         let fetcher = EpochFetcher::new(&base_fetcher);

//         // Setup responses for epoch progression
//         let epochs = [1u64, 2u64, 3u64];
//         let call = getCurrentEpochCall {};

//         for (block_num, epoch) in epochs.iter().enumerate() {
//             let epoch_response = (U256::from(*epoch), U256::from(1000), U256::from(100));
//             mock_eth_call.set_sol_response(
//                 EpochFetcher::<MockEthCall>::caller_address(),
//                 EpochFetcher::<MockEthCall>::contract_address(),
//                 call.clone(),
//                 (block_num + 1) as u64 * 100,
//                 epoch_response,
//             );
//         }

//         // Test epoch progression
//         for (block_num, expected_epoch) in epochs.iter().enumerate() {
//             let result = fetcher.fetch((block_num + 1) as u64 * 100);
//             let epoch_bytes = expected_epoch.to_le_bytes();
//             assert_eq!(result.as_ref(), &epoch_bytes);
//         }
//     }

//     #[test]
//     fn test_epoch_edge_cases() {
//         let mock_eth_call = MockEthCall::new();
//         let base_fetcher = OnchainConfigFetcher::new(mock_eth_call.clone());
//         let fetcher = EpochFetcher::new(&base_fetcher);

//         // Test with maximum epoch value
//         let max_epoch = u64::MAX;
//         let epoch_response = (U256::from(max_epoch), U256::from(1000), U256::from(100));
//         let call = getCurrentEpochCall {};

//         mock_eth_call.set_sol_response(
//             EpochFetcher::<MockEthCall>::caller_address(),
//             EpochFetcher::<MockEthCall>::contract_address(),
//             call,
//             100,
//             epoch_response,
//         );

//         let result = fetcher.fetch(100);
//         let epoch_bytes = max_epoch.to_le_bytes();
//         assert_eq!(result.as_ref(), &epoch_bytes);
//     }
// }
