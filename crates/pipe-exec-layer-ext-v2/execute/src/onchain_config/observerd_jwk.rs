//! Fetcher for consensus configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    GRAVITY_FRAMEWORK_ADDRESS, JWK_MANAGER_ADDR,
};
use crate::onchain_config::{BLOCK_ADDR, SYSTEM_CALLER};
use alloy_consensus::{EthereumTxEnvelope, TxEip4844, TxLegacy};
use alloy_primitives::{Address, Bytes};
use alloy_primitives::{Signature, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use reth_evm::{Evm, IntoTxEnv};
use reth_primitives::Recovered;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use revm::database::{states::bundle_state::BundleRetention, State};
use revm::{context::TxEnv, context_interface::result::HaltReason, state::EvmState, Database};
use revm_primitives::TxKind;
use std::fmt::Debug;

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
fn new_system_call_txn(contract: Address, input: Bytes) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce: 0,
            gas_price: 0,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        Signature::new(U256::ZERO, U256::ZERO, false),
    )
}

pub fn constrct_observed_jwks_txns_envelope(
    provider_jwks_array_bytes: Vec<Vec<u8>>,
) -> Vec<EthereumTxEnvelope<TxEip4844>> {
    let txns = provider_jwks_array_bytes
        .into_iter()
        .map(|provider_jwks_bytes| {
            let provider_jwks = bcs::from_bytes::<
                gravity_api_types::on_chain_config::jwks::ProviderJWKs,
            >(&provider_jwks_bytes)
            .expect("Failed to deserialize provider JWKS");
            let provider_jwks = convert_into_sol_provider_jwks(provider_jwks);

            let call = upsertObservedJWKsCall { providerJWKsArray: vec![provider_jwks] };
            let input: Bytes = call.abi_encode().into();
            let txn = new_system_call_txn(JWK_MANAGER_ADDR, input);
            txn
        })
        .collect();
    txns
}

// fn constrct_observed_jwks_txns(provider_jwks_array_bytes: Vec<Vec<u8>>) -> Vec<TxEnv> {
//     let txns = provider_jwks_array_bytes
//         .into_iter()
//         .map(|provider_jwks_bytes| {
//             let all_provider_jwks = bcs::from_bytes::<
//                 gravity_api_types::on_chain_config::jwks::AllProvidersJWKs,
//             >(&provider_jwks_bytes)
//             .expect("Failed to deserialize provider JWKS");

//             let call = upsertObservedJWKsCall {
//                 providerJWKsArray: all_provider_jwks
//                     .entries
//                     .into_iter()
//                     .map(|provider_jwks| ProviderJWKs {
//                         issuer: String::from_utf8(provider_jwks.issuer)
//                             .expect("Failed to convert issuer to string"),
//                         version: provider_jwks.version,
//                         jwks: provider_jwks
//                             .jwks
//                             .into_iter()
//                             .map(|jwk| JWK {
//                                 variant: jwk.type_name.as_bytes()[0],
//                                 data: jwk.data.into(),
//                             })
//                             .collect(),
//                     })
//                     .collect(),
//             };
//             let input: Bytes = call.abi_encode().into();
//             let txn = new_system_call_txn(BLOCK_ADDR, input);
//             Recovered::new_unchecked(txn, SYSTEM_CALLER).into_tx_env()
//         })
//         .collect();
//     txns
// }

// /// Execute a observed jwk contract call (upsertObservedJWKs)
// pub fn transact_observed_jwk_contract_call<'a, D: Database + 'a>(
//     evm: &mut impl Evm<Error: Debug, Tx = TxEnv, HaltReason = HaltReason, DB = &'a mut State<D>>,
//     provider_jwks_array_bytes: Vec<Vec<u8>>,
// ) -> EvmState {
//     let txns = constrct_observed_jwks_txns(provider_jwks_array_bytes);
//     let mut results = vec![];
//     for txn in txns {
//         let result = evm.transact_raw(txn).unwrap();
//         assert!(result.result.is_success(), "Failed to execute blockPrologue: {:?}", result.result);
//         results.push(result.state);
//     }
//     evm.db_mut().merge_transitions(BundleRetention::Reverts);
//     let state = evm.db_mut().take_bundle();
//     state.state;
//     // state.
//     todo!()
// }
