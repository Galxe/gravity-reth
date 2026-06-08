//! Fetcher for JWK consensus configuration from OracleTaskConfig
//!
//! In the new Oracle architecture, provider configuration is stored in OracleTaskConfig.
//! This fetcher enumerates ALL oracle tasks (JWK, blockchain, etc.) and formats them
//! as OIDCProviders for the gravity-aptos JWK consensus system.

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    oracle_task_helpers::{OracleTaskClient, SOURCE_TYPE_JWK},
    ORACLE_TASK_CONFIG_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::info;

// Well-known task names
fn oidc_providers_task_name() -> B256 {
    keccak256("oidc_providers")
}

// =============================================================================
// JWK-specific ABI (not shared with observed_jwk)
// =============================================================================

sol! {
    /// OIDC Provider stored in task config (for JWK sourceType=1)
    struct TaskOIDCProvider {
        bytes name;
        bytes configUrl;
        uint64 blockNumber;
    }
}

// =============================================================================
// JwkConsensusConfigFetcher
// =============================================================================

/// Fetcher for JWK consensus configuration
///
/// Enumerates ALL oracle tasks from OracleTaskConfig and formats them as OIDCProviders
/// for gravity-aptos JWK consensus.
#[derive(Debug)]
pub struct JwkConsensusConfigFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> JwkConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }

    /// Get shared oracle task client
    fn oracle_client(&self) -> OracleTaskClient<'_, EthApi> {
        OracleTaskClient::new(self.base_fetcher)
    }

    /// Fetch and parse JWK providers (sourceType=1)
    fn fetch_jwk_providers(
        &self,
        block_id: BlockId,
    ) -> Vec<gravity_api_types::on_chain_config::jwks::OIDCProvider> {
        let mut providers = Vec::new();

        // Try to get the "oidc_providers" task for JWK
        if let Some(task) = self.oracle_client().call_get_task(
            SOURCE_TYPE_JWK,
            U256::ZERO,
            oidc_providers_task_name(),
            block_id,
        ) {
            if !task.config.is_empty() {
                // ABI decode TaskOIDCProvider[]
                if let Ok(oidc_providers) = <alloy_sol_types::sol_data::Array<TaskOIDCProvider> as alloy_sol_types::SolType>::abi_decode(&task.config) {
                    for provider in oidc_providers {
                        providers.push(gravity_api_types::on_chain_config::jwks::OIDCProvider {
                            name: String::from_utf8_lossy(&provider.name).to_string(),
                            config_url: String::from_utf8_lossy(&provider.configUrl).to_string(),
                            onchain_nonce: None, // JWK providers don't use nonce
                        });
                    }
                }
            }
        }

        providers
    }

    /// Fetch and parse blockchain providers (sourceType=0)
    /// Uses shared OracleTaskClient for task enumeration
    fn fetch_blockchain_providers(
        &self,
        block_id: BlockId,
    ) -> Vec<gravity_api_types::on_chain_config::jwks::OIDCProvider> {
        let task_uris = self.oracle_client().fetch_blockchain_task_uris(block_id);

        task_uris
            .into_iter()
            .map(|(uri, nonce)| {
                info!(uri = %uri, nonce = nonce, "Found blockchain monitoring task");
                gravity_api_types::on_chain_config::jwks::OIDCProvider {
                    name: uri.clone(),
                    config_url: uri,
                    onchain_nonce: Some(nonce as u64),
                }
            })
            .collect()
    }
}

impl<'a, EthApi> ConfigFetcher for JwkConsensusConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let mut all_providers = Vec::new();

        // 1. Fetch JWK providers (sourceType=1)
        all_providers.extend(self.fetch_jwk_providers(block_id));

        // 2. Fetch blockchain providers (sourceType=0)
        all_providers.extend(self.fetch_blockchain_providers(block_id));

        info!(provider_count = all_providers.len(), "Fetched oracle task providers");

        // Build JWKConsensusConfig
        // TODO(gravity): should read the onchain config
        let jwk_consensus_config = gravity_api_types::on_chain_config::jwks::JWKConsensusConfig {
            enabled: !all_providers.is_empty(),
            oidc_providers: all_providers,
        };

        Some(
            bcs::to_bytes(&jwk_consensus_config)
                .expect("Failed to serialize JwkConsensusConfig")
                .into(),
        )
    }

    fn contract_address() -> Address {
        ORACLE_TASK_CONFIG_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
