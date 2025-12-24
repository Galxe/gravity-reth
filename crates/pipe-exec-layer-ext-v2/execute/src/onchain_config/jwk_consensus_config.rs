//! Fetcher for consensus configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    JWK_MANAGER_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

sol! {
    struct OIDCProvider {
        string name; // Provider name, e.g., "https://accounts.google.com"
        string configUrl; // OpenID configuration URL
        bool active; // Whether the provider is active
        uint64 onchainBlockNumber; // Onchain block number
    }
    function getActiveProviders() external view returns (OIDCProvider[] memory);
}

fn convert_into_bcs_active_providers(oidc_providers: Vec<OIDCProvider>) -> Bytes {
    let active_providers = oidc_providers
        .iter()
        .filter(|provider| provider.active)
        .map(|provider| gravity_api_types::on_chain_config::jwks::OIDCProvider {
            name: provider.name.clone(),
            config_url: provider.configUrl.clone(),
            onchain_block_number: Some(provider.onchainBlockNumber),
        })
        .collect::<Vec<_>>();
    let jwk_consensus_config = gravity_api_types::on_chain_config::jwks::JWKConsensusConfig {
        enabled: true,
        oidc_providers: active_providers,
    };
    bcs::to_bytes(&jwk_consensus_config).expect("Failed to serialize JwkConsensusConfig").into()
}

/// Fetcher for consensus configuration
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
        let call = getActiveProvidersCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
            .map_err(|e| {
                tracing::warn!(
                    "Failed to fetch JWK consensus config at block {}: {:?}",
                    block_id,
                    e
                );
            })
            .ok()?;

        let solidity_active_providers = getActiveProvidersCall::abi_decode_returns(&result)
            .expect("Failed to decode getActiveProviders return value");
        Some(convert_into_bcs_active_providers(solidity_active_providers))
    }

    fn contract_address() -> Address {
        JWK_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}
