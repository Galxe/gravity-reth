//! Base trait and implementation for onchain config fetchers

use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::{state::EvmOverrides, TransactionInput, TransactionRequest};

use alloy_primitives::TxKind;
use gravity_api_types::config_storage::{OnChainConfig, OnChainConfigResType};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use std::{fmt::Debug, sync::OnceLock};
use tokio::runtime::Runtime;

static ETH_CALL_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Base trait for all config fetchers
pub trait ConfigFetcher {
    /// Fetch configuration data for a specific block
    fn fetch(&self, block_number: u64) -> Bytes;

    /// Get the contract address for this fetcher
    fn contract_address() -> Address;

    /// Get the caller address for this fetcher
    fn caller_address() -> Address;
}

/// Main onchain config fetcher that coordinates different fetchers
#[derive(Debug)]
pub struct OnchainConfigFetcher<EthApi> {
    eth_api: EthApi,
}

impl<EthApi> OnchainConfigFetcher<EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    /// Create a new onchain config fetcher
    pub const fn new(eth_api: EthApi) -> Self {
        Self { eth_api }
    }

    /// Execute an `eth_call` with retry logic
    pub fn eth_call(&self, from: Address, to: Address, input: Bytes, block_number: u64) -> Bytes {
        let rt_handle = ETH_CALL_RUNTIME
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4.min(std::thread::available_parallelism().unwrap().get()))
                    .thread_name("OnchainConfigFetcher")
                    .enable_all()
                    .build()
                    .expect("Failed to create Tokio runtime")
            })
            .handle();

        tokio::task::block_in_place(|| {
            rt_handle.block_on(async {
                const RETRY: u64 = 5;
                let mut count = 0;
                loop {
                    match self
                        .eth_api
                        .call(
                            TransactionRequest {
                                from: Some(from),
                                to: Some(TxKind::Call(to)),
                                input: TransactionInput::new(input.clone()),
                                ..Default::default()
                            },
                            Some(BlockId::from(block_number)),
                            EvmOverrides::new(None, None),
                        )
                        .await
                    {
                        Ok(result) => return result,
                        Err(err) => {
                            tracing::warn!(
                                "Failed to execute eth_call at {block_number}, retrying... (attempt {count}/{RETRY}): {err}"
                            );
                            count += 1;
                            assert!(count <= RETRY, "Failed to execute eth_call: {err}");
                            tokio::time::sleep(std::time::Duration::from_millis(10 * count)).await;
                        }
                    }
                }
            })
        })
    }

    /// Fetch epoch directly
    pub fn fetch_epoch(&self, block_number: u64) -> u64 {
        let fetcher = super::epoch::EpochFetcher::new(self);
        let epoch_bytes = fetcher.fetch(block_number);
        u64::from_le_bytes(epoch_bytes.as_ref().try_into().unwrap_or([0; 8]))
    }

    /// Fetch consensus config directly
    pub fn fetch_consensus_config(&self, block_number: u64) -> Bytes {
        let fetcher = super::consensus_config::ConsensusConfigFetcher::new(self);
        fetcher.fetch(block_number)
    }

    /// Fetch validator set directly
    pub fn fetch_validator_set(&self, block_number: u64) -> Bytes {
        let fetcher = super::validator_set::ValidatorSetFetcher::new(self);
        fetcher.fetch(block_number)
    }

    /// Generic method to fetch config bytes
    pub fn fetch_config_bytes(
        &self,
        config_name: OnChainConfig,
        block_number: u64,
    ) -> OnChainConfigResType {
        use crate::onchain_config::{
            consensus_config::ConsensusConfigFetcher, dkg::DKGStateFetcher, epoch::EpochFetcher,
            jwk_consensus_config::JwkConsensusConfigFetcher, observed_jwk::ObservedJwkFetcher,
            validator_set::ValidatorSetFetcher,
        };

        match config_name {
            OnChainConfig::ConsensusConfig => {
                let fetcher = ConsensusConfigFetcher::new(self);
                fetcher.fetch(block_number).0.into()
            }
            OnChainConfig::Epoch => {
                let fetcher = EpochFetcher::new(self);
                let epoch_bytes = fetcher.fetch(block_number);
                // Convert bytes back to u64 for the epoch
                u64::from_le_bytes(epoch_bytes.as_ref().try_into().unwrap_or([0; 8])).into()
            }
            OnChainConfig::ValidatorSet => {
                let fetcher = ValidatorSetFetcher::new(self);
                fetcher.fetch(block_number).0.into()
            }
            OnChainConfig::ObservedJWKs => {
                let fetcher = ObservedJwkFetcher::new(self);
                fetcher.fetch(block_number).0.into()
            }
            OnChainConfig::JWKConsensusConfig => {
                let fetcher = JwkConsensusConfigFetcher::new(self);
                fetcher.fetch(block_number).0.into()
            }
            OnChainConfig::DKGState => {
                let fetcher = DKGStateFetcher::new(self);
                fetcher.fetch(block_number).0.into()
            }
            _ => todo!("Implement fetching for other config types"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::{SolCall, SolValue};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    /// Mock `EthCall` implementation for testing
    #[derive(Clone)]
    struct MockEthCall {
        responses: Arc<Mutex<HashMap<(Address, Address, Bytes, u64), Bytes>>>,
    }

    impl MockEthCall {
        fn new() -> Self {
            Self { responses: Arc::new(Mutex::new(HashMap::new())) }
        }

        /// Set a mock response for a specific call
        fn set_response(
            &self,
            from: Address,
            to: Address,
            input: Bytes,
            block_number: u64,
            response: Bytes,
        ) {
            self.responses.lock().unwrap().insert((from, to, input, block_number), response);
        }

        /// Helper to set response for a Solidity function call
        fn set_sol_response<T, R>(
            &self,
            from: Address,
            to: Address,
            call: T,
            block_number: u64,
            response: R,
        ) where
            T: SolCall,
            R: SolValue,
        {
            let input: Bytes = call.abi_encode().into();
            let response_bytes: Bytes = response.abi_encode().into();
            self.set_response(from, to, input, block_number, response_bytes);
        }
    }

    // Simple mock implementation for testing
    impl MockEthCall {
        async fn call(
            &self,
            request: TransactionRequest,
            block_id: Option<BlockId>,
            _overrides: EvmOverrides,
        ) -> Result<Bytes, String> {
            let from = request.from.unwrap_or_default();
            let to = match request.to {
                Some(TxKind::Call(addr)) => addr,
                _ => return Err("Invalid transaction type".to_string()),
            };
            let input = request.input.into_input().unwrap_or_default();
            let block_number = match block_id {
                Some(BlockId::Number(n)) => n.as_number().unwrap_or(0),
                _ => 0,
            };

            let responses = self.responses.lock().unwrap();
            responses
                .get(&(from, to, input, block_number))
                .cloned()
                .ok_or_else(|| format!("No mock response for call to {to} at block {block_number}"))
        }
    }

    /// Base test framework for config fetchers
    struct ConfigFetcherTestFramework<T> {
        mock_eth_call: MockEthCall,
        fetcher: T,
        block_number: u64,
    }

    impl<T> ConfigFetcherTestFramework<T> {
        fn new(fetcher: T) -> Self {
            Self { mock_eth_call: MockEthCall::new(), fetcher, block_number: 100 }
        }

        /// Set the block number for tests
        fn with_block_number(mut self, block_number: u64) -> Self {
            self.block_number = block_number;
            self
        }
    }

    /// Macro to create test cases for config fetchers
    #[macro_export]
    macro_rules! create_config_test {
        ($test_name:ident, $fetcher_type:ty, $setup:expr, $validation:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let framework =
                    ConfigFetcherTestFramework::new(<$fetcher_type>::new(&MockEthCall::new()));
                $setup(&framework);
                let result = framework.fetcher.fetch(framework.block_number);
                $validation(result);
            }
        };
    }

    #[test]
    fn test_mock_eth_call_basic() {
        let mock = MockEthCall::new();
        let test_address = Address::from([1u8; 20]);
        let test_input = Bytes::from(vec![1, 2, 3]);
        let test_response = Bytes::from(vec![4, 5, 6]);

        mock.set_response(
            Address::ZERO,
            test_address,
            test_input.clone(),
            100,
            test_response.clone(),
        );

        // Create runtime for async test
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = mock
                .call(
                    TransactionRequest {
                        from: Some(Address::ZERO),
                        to: Some(TxKind::Call(test_address)),
                        input: TransactionInput::new(test_input),
                        ..Default::default()
                    },
                    Some(BlockId::Number(100.into())),
                    EvmOverrides::new(None, None),
                )
                .await
                .unwrap();

            assert_eq!(result, test_response);
        });
    }
}
