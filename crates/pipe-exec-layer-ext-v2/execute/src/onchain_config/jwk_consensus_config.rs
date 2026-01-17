//! Fetcher for JWK consensus configuration from OracleTaskConfig
//!
//! In the new Oracle architecture, provider configuration is stored in OracleTaskConfig
//! instead of JWKManager. The task is retrieved using:
//!   OracleTaskConfig.getTask(sourceType=1, sourceId=0, taskName=keccak256("oidc_providers"))

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    ORACLE_TASK_CONFIG_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

/// Source type for JWK in the Oracle system
const SOURCE_TYPE_JWK: u32 = 1;

/// Task name for OIDC providers configuration
/// keccak256("oidc_providers")
fn oidc_providers_task_name() -> B256 {
    keccak256("oidc_providers")
}

// OracleTaskConfig contract ABI (aligned with
// gravity_chain_core_contracts/src/oracle/IOracleTaskConfig.sol)
sol! {
    /// @notice Configuration for a continuous oracle task
    struct OracleTask {
        /// Task configuration bytes (ABI-encoded OIDCProvider[])
        bytes config;
        /// Timestamp when this task was last updated
        uint64 updatedAt;
    }

    /// @notice OIDC Provider stored in the task config
    /// Note: This is what's ABI-encoded inside OracleTask.config
    struct OIDCProvider {
        bytes name;      // Provider name, e.g., "https://accounts.google.com"
        bytes configUrl; // OpenID configuration URL
        uint64 blockNumber; // Onchain block number
    }

    /// @notice Get an oracle task by its key tuple
    function getTask(
        uint32 sourceType,
        uint256 sourceId,
        bytes32 taskName
    ) external view returns (OracleTask memory task);
}

/// Convert OracleTaskConfig providers to BCS-encoded JWKConsensusConfig
fn convert_task_config_to_bcs(task: OracleTask) -> Option<Bytes> {
    if task.config.is_empty() {
        tracing::warn!("OracleTaskConfig: oidc_providers task has empty config");
        let jwk_consensus_config = gravity_api_types::on_chain_config::jwks::JWKConsensusConfig {
            enabled: false,
            oidc_providers: Vec::new(),
        };

        return Some(
            bcs::to_bytes(&jwk_consensus_config)
                .expect("Failed to serialize JwkConsensusConfig")
                .into(),
        );
    }

    // ABI decode the config bytes to get OIDCProvider[]
    // The config is ABI-encoded as a dynamic array of OIDCProvider structs
    let providers = match <alloy_sol_types::sol_data::Array<OIDCProvider> as alloy_sol_types::SolType>::abi_decode(
        &task.config,
    ) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Failed to ABI decode OIDCProvider[] from task config: {:?}", e);
            return None;
        }
    };

    let active_providers = providers
        .iter()
        .map(|provider| gravity_api_types::on_chain_config::jwks::OIDCProvider {
            name: String::from_utf8_lossy(&provider.name).to_string(),
            config_url: String::from_utf8_lossy(&provider.configUrl).to_string(),
            onchain_block_number: Some(provider.blockNumber),
        })
        .collect::<Vec<_>>();

    let jwk_consensus_config = gravity_api_types::on_chain_config::jwks::JWKConsensusConfig {
        enabled: false,
        oidc_providers: active_providers,
    };

    Some(
        bcs::to_bytes(&jwk_consensus_config)
            .expect("Failed to serialize JwkConsensusConfig")
            .into(),
    )
}

/// Fetcher for JWK consensus configuration from OracleTaskConfig
#[derive(Debug)]
pub struct JwkConsensusConfigFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> JwkConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new consensus config fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for JwkConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        // Call OracleTaskConfig.getTask(SOURCE_TYPE_JWK, 0, keccak256("oidc_providers"))
        let call = getTaskCall {
            sourceType: SOURCE_TYPE_JWK,
            sourceId: U256::ZERO,
            taskName: oidc_providers_task_name(),
        };
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
            .map_err(|e| {
                tracing::warn!(
                    "Failed to fetch JWK consensus config from OracleTaskConfig at block {}: {:?}",
                    block_id,
                    e
                );
            })
            .ok()?;

        let task = getTaskCall::abi_decode_returns(&result)
            .expect("Failed to decode getTask return value");

        convert_task_config_to_bcs(task)
    }

    fn contract_address() -> Address {
        ORACLE_TASK_CONFIG_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
