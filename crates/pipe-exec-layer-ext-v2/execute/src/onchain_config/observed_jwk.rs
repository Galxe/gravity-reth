//! Fetcher for Oracle on-chain data
//!
//! This module handles the READ path for ALL oracle data from the chain:
//! - RSA JWKs from JWKManager contract
//! - Blockchain events from NativeOracle (sourced from OracleTaskConfig)
//!
//! Both are packaged as AllProvidersJWKs for gravity-aptos JWK consensus.
//! The data format matches what the relayer sends for byte-exact comparison.

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    oracle_task_helpers::OracleTaskClient,
    types::{convert_oracle_rsa_to_api_jwk, getObservedJWKsCall, OracleProviderJWKs},
    JWK_MANAGER_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::jwks::ProviderJWKs;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use std::fmt::Debug;
use tracing::{debug, info};

// =============================================================================
// Conversion Functions
// =============================================================================

/// Convert Oracle ProviderJWKs to api-types ProviderJWKs
fn convert_oracle_provider_jwks(provider_jwks: OracleProviderJWKs) -> ProviderJWKs {
    ProviderJWKs {
        issuer: provider_jwks.issuer.to_vec(),
        version: provider_jwks.version,
        jwks: provider_jwks.jwks.into_iter().map(convert_oracle_rsa_to_api_jwk).collect(),
    }
}

/// Convert Oracle ProviderJWKs to api-types ProviderJWKs (pub for lib.rs event parsing)
pub fn convert_into_api_provider_jwks(provider_jwks: OracleProviderJWKs) -> ProviderJWKs {
    convert_oracle_provider_jwks(provider_jwks)
}

// =============================================================================
// ObservedJwkFetcher - Unified READ Path
// =============================================================================

/// Fetcher for all oracle data (JWKs + blockchain events)
#[derive(Debug)]
pub struct ObservedJwkFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ObservedJwkFetcher<'a, EthApi>
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

    /// Fetch JWKs from JWKManager contract
    fn fetch_jwk_manager_providers(&self, block_id: BlockId) -> Option<Vec<ProviderJWKs>> {
        let call = getObservedJWKsCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(SYSTEM_CALLER, JWK_MANAGER_ADDR, input, block_id)
            .map_err(|e| {
                debug!("Failed to fetch JWKs from JWKManager: {:?}", e);
            })
            .ok()?;

        let oracle_jwks = getObservedJWKsCall::abi_decode_returns(&result).ok()?;

        Some(oracle_jwks.entries.into_iter().map(convert_oracle_provider_jwks).collect())
    }

    /// Fetch blockchain event providers using shared OracleTaskClient
    fn fetch_blockchain_providers(&self, block_id: BlockId) -> Vec<ProviderJWKs> {
        let task_uris = self.oracle_client().fetch_blockchain_task_uris(block_id);

        task_uris
            .into_iter()
            .map(|(uri, nonce)| ProviderJWKs {
                issuer: uri.into_bytes(),
                version: nonce as u64,
                jwks: vec![], // Empty - only use version for comparison
            })
            .collect()
    }
}

impl<'a, EthApi> ConfigFetcher for ObservedJwkFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    /// Fetch ALL oracle data (JWKs + blockchain events) and return as BCS-encoded ObservedJWKs
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let mut all_entries: Vec<ProviderJWKs> = Vec::new();

        // 1. Fetch JWKs from JWKManager
        if let Some(jwk_entries) = self.fetch_jwk_manager_providers(block_id) {
            all_entries.extend(jwk_entries);
        }

        // 2. Fetch blockchain events from NativeOracle (for configured chains from
        //    OracleTaskConfig)
        let blockchain_entries = self.fetch_blockchain_providers(block_id);
        all_entries.extend(blockchain_entries);

        info!(
            jwk_count = all_entries.iter().filter(|e| e.issuer.starts_with(b"https://")).count(),
            blockchain_count =
                all_entries.iter().filter(|e| e.issuer.starts_with(b"gravity://")).count(),
            "Fetched all oracle providers"
        );

        // 3. Build and BCS-encode ObservedJWKs
        let api_all_providers =
            gravity_api_types::on_chain_config::jwks::AllProvidersJWKs { entries: all_entries };

        let observed_jwks =
            gravity_api_types::on_chain_config::jwks::ObservedJWKs { jwks: api_all_providers };

        Some(bcs::to_bytes(&observed_jwks).expect("Failed to BCS serialize ObservedJWKs").into())
    }

    fn contract_address() -> Address {
        JWK_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
