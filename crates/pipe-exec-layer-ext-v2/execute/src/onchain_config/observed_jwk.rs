//! Fetcher for consensus configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    GRAVITY_FRAMEWORK_ADDRESS, JWK_MANAGER_ADDR,
};
use crate::onchain_config::{BLOCK_ADDR, SYSTEM_CALLER};
use alloy_consensus::{EthereumTxEnvelope, TxEip4844, TxLegacy};
use alloy_primitives::{Address, Bytes, Signature, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use reth_evm::{Evm, IntoTxEnv};
use reth_primitives::Recovered;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use revm::{
    context::TxEnv,
    context_interface::result::HaltReason,
    database::{states::bundle_state::BundleRetention, State},
    state::EvmState,
    Database,
};
use revm_primitives::TxKind;
use std::fmt::Debug;
use tracing::info;

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

    function upsertObservedJWKs(
        ProviderJWKs[] calldata providerJWKsArray
    ) external;

    event ObservedJWKsUpdated(uint256 indexed epoch, ProviderJWKs[] jwks);
}

fn convert_into_api_jwk(jwk: JWK) -> JWKStruct {
    JWKStruct { type_name: "JWK".to_string(), data: jwk.data.into() }
}

pub fn convert_into_api_provider_jwks(
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

fn convert_into_sol_provider_jwks(
    provider_jwks: gravity_api_types::on_chain_config::jwks::ProviderJWKs,
) -> ProviderJWKs {
    ProviderJWKs {
        issuer: String::from_utf8(provider_jwks.issuer)
            .expect("Failed to convert issuer to string"),
        version: provider_jwks.version,
        jwks: provider_jwks
            .jwks
            .into_iter()
            .map(|jwk| JWK { variant: jwk.type_name.as_bytes()[0], data: jwk.data.into() })
            .collect(),
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

/// Create a new system call transaction
fn new_system_call_txn(
    contract: Address,
    nonce: u64,
    gas_price: u128,
    input: Bytes,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce,
            gas_price,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        Signature::new(U256::ZERO, U256::ZERO, false),
    )
}

pub fn construct_observed_jwks_txns_envelope(
    provider_jwks_array_bytes: &Vec<Vec<u8>>,
    system_caller_nonce: u64,
    gas_price: u128,
) -> Vec<EthereumTxEnvelope<TxEip4844>> {
    let system_caller_nonce = system_caller_nonce + 1;
    let txns = provider_jwks_array_bytes
        .iter()
        .enumerate()
        .map(|(index, provider_jwks_bytes)| {
            let provider_jwks = bcs::from_bytes::<
                gravity_api_types::on_chain_config::jwks::ProviderJWKs,
            >(&provider_jwks_bytes)
            .expect("Failed to deserialize provider JWKS");
            let provider_jwks = convert_into_sol_provider_jwks(provider_jwks);

            let call = upsertObservedJWKsCall { providerJWKsArray: vec![provider_jwks] };
            let input: Bytes = call.abi_encode().into();
            let current_nonce = system_caller_nonce + index as u64;
            new_system_call_txn(JWK_MANAGER_ADDR, current_nonce, gas_price, input)
        })
        .collect();
    txns
}
