//! Fetcher for consensus configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    GRAVITY_FRAMEWORK_ADDRESS, JWK_MANAGER_ADDR,
};
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

sol! {
    struct JWK {
        uint8 variant; // 0: RSA_JWK, 1: UnsupportedJWK
        bytes data; // Encoded JWK data
    }

    /// @dev Provider's JWK collection
    struct ProviderJWKs {
        string issuer; // Issuer
        uint64 version; // Version number
        JWK[] jwks; // JWK array, sorted by kid
    }

    /// @dev All providers' JWK collection
    struct AllProvidersJWKs {
        ProviderJWKs[] entries; // Provider array sorted by issuer
    }
    function getObservedJWKs() external view returns (AllProvidersJWKs memory);
}

fn convert_into_api_jwk(jwk: JWK) -> JWKStruct {
    JWKStruct { type_name: "JWK".to_string(), data: jwk.data.into() }
}

fn convert_into_api_provider_jwks(
    provider_jwks: ProviderJWKs,
) -> gravity_api_types::on_chain_config::jwks::ProviderJWKs {
    gravity_api_types::on_chain_config::jwks::ProviderJWKs {
        issuer: provider_jwks.issuer.into(),
        version: provider_jwks.version,
        jwks: provider_jwks
            .jwks
            .iter()
            .map(|jwk: &JWK| convert_into_api_jwk(jwk.clone()))
            .collect::<Vec<_>>(),
    }
}

fn convert_into_api_all_providers_jwks(
    all_providers_jwks: AllProvidersJWKs,
) -> gravity_api_types::on_chain_config::jwks::AllProvidersJWKs {
    gravity_api_types::on_chain_config::jwks::AllProvidersJWKs {
        entries: all_providers_jwks
            .entries
            .iter()
            .map(|provider_jwks: &ProviderJWKs| {
                convert_into_api_provider_jwks(provider_jwks.clone())
            })
            .collect::<Vec<_>>(),
    }
}

fn convert_into_observed_jwks(
    all_providers_jwks: AllProvidersJWKs,
) -> gravity_api_types::on_chain_config::jwks::ObservedJWKs {
    gravity_api_types::on_chain_config::jwks::ObservedJWKs {
        jwks: convert_into_api_all_providers_jwks(all_providers_jwks),
    }
}

fn convert_into_bcs_all_providers_jwks(all_providers_jwks: AllProvidersJWKs) -> Bytes {
    let all_providers = convert_into_observed_jwks(all_providers_jwks);
    bcs::to_bytes(&all_providers).expect("Failed to serialize AllProvidersJWKs").into()
}

/// Fetcher for consensus configuration
#[derive(Debug)]
pub struct ObservedJwkFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> ObservedJwkFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new consensus config fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for ObservedJwkFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_number: u64) -> Bytes {
        let call = getObservedJWKsCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self.base_fetcher.eth_call(
            Self::caller_address(),
            Self::contract_address(),
            input,
            block_number,
        );

        let solidity_all_providers_jwks = getObservedJWKsCall::abi_decode_returns(&result)
            .expect("Failed to decode getObservedJWKs return value");
        convert_into_bcs_all_providers_jwks(solidity_all_providers_jwks)
    }

    fn contract_address() -> Address {
        JWK_MANAGER_ADDR
    }

    fn caller_address() -> Address {
        GRAVITY_FRAMEWORK_ADDRESS
    }
}
