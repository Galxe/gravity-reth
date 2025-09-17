//! Fetcher for consensus configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    GRAVITY_FRAMEWORK_ADDRESS, JWK_MANAGER_ADDR,
};
use alloy_consensus::{EthereumTxEnvelope, TxEip4844, TxLegacy};
use alloy_primitives::{Address, Bytes, Signature, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent, SolType};
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use revm_primitives::{keccak256, TxKind};
use std::fmt::Debug;
use tracing::debug;

// 全局常量：事件类型的keccak256哈希值
const EVENT_TYPE_1_HASH: [u8; 32] = [
    0xc8, 0x9e, 0xfd, 0xaa, 0x54, 0xc0, 0xf2, 0x0c, 0x7a, 0xdf, 0x61, 0x28, 0x82, 0xdf, 0x09, 0x50,
    0xf5, 0xa9, 0x51, 0x63, 0x7e, 0x03, 0x07, 0xcd, 0xcb, 0x4c, 0x67, 0x2f, 0x29, 0x8b, 0x8b, 0xc6,
];
const EVENT_TYPE_2_HASH: [u8; 32] = [
    0xad, 0x7c, 0x5b, 0xef, 0x02, 0x78, 0x16, 0xa8, 0x00, 0xda, 0x17, 0x36, 0x44, 0x4f, 0xb5, 0x8a,
    0x80, 0x7e, 0xf4, 0xc9, 0x60, 0x3b, 0x78, 0x48, 0x67, 0x3f, 0x7e, 0x3a, 0x68, 0xeb, 0x14, 0xa5,
];
const EVENT_TYPE_3_HASH: [u8; 32] = [
    0x2a, 0x80, 0xe1, 0xef, 0x1d, 0x78, 0x42, 0xf2, 0x7f, 0x2e, 0x6b, 0xe0, 0x97, 0x2b, 0xb7, 0x08,
    0xb9, 0xa1, 0x35, 0xc3, 0x88, 0x60, 0xdb, 0xe7, 0x3c, 0x27, 0xc3, 0x48, 0x6c, 0x34, 0xf4, 0xde,
];
const EVENT_TYPE_4_HASH: [u8; 32] = [
    0x13, 0x60, 0x0b, 0x29, 0x41, 0x91, 0xfc, 0x92, 0x92, 0x4b, 0xb3, 0xce, 0x4b, 0x96, 0x9c, 0x1e,
    0x7e, 0x2b, 0xab, 0x8f, 0x4c, 0x93, 0xc3, 0xfc, 0x6d, 0x0a, 0x51, 0x73, 0x3d, 0xf3, 0xc0, 0x60,
];

// 全局常量：默认的ValidatorRegistrationParams
const DEFAULT_VALIDATOR_PARAMS: ValidatorRegistrationParams = ValidatorRegistrationParams {
    consensusPublicKey: Bytes::new(),
    blsProof: Bytes::new(),
    commission: Commission { rate: 0, maxRate: 0, maxChangeRate: 0 },
    moniker: String::new(),
    initialOperator: Address::ZERO,
    initialBeneficiary: Address::ZERO,
    validatorNetworkAddresses: Bytes::new(),
    fullnodeNetworkAddresses: Bytes::new(),
    aptosAddress: Bytes::new(),
};

sol! {
    event StakeRegisterValidatorEvent(
        address user,
        uint256 amount,
        bytes params,
        uint256 blockNumber
    );

    event StakeEvent(
        address user,
        uint256 amount,
        address targetValidator,
        uint256 blockNumber
    );

    event ValidatorExitEvent(
        address user,
        uint256 amount,
        address targetValidator,
        uint256 blockNumber
    );

    event UnstakeEvent(
        address user,
        uint256 amount,
        address targetValidator,
        uint256 blockNumber
    );
}

sol! {
    // Commission structure
    struct Commission {
        uint64 rate; // the commission rate charged to delegators(10000 is 100%)
        uint64 maxRate; // maximum commission rate which validator can ever charge
        uint64 maxChangeRate; // maximum daily increase of the validator commission
    }
    struct ValidatorRegistrationParams {
        bytes consensusPublicKey;
        bytes blsProof; // BLS proof
        Commission commission; // Changed from uint64 commissionRate to Commission struct
        string moniker;
        address initialOperator;
        address initialBeneficiary; // Passed directly to StakeCredit
        // Network addresses for Aptos compatibility
        bytes validatorNetworkAddresses; // BCS serialized Vec<NetworkAddress>
        bytes fullnodeNetworkAddresses; // BCS serialized Vec<NetworkAddress>
        bytes aptosAddress; // Aptos validator address
    }

    struct CrossChainParams {
        // 1 => StakeRegisterValidatorEvent
        // 2 => DelegationEvent
        // 3 => LeaveValidatorSetEvent
        // 4 => UndelegationEvent
        bytes id;
        ValidatorRegistrationParams validatorParams;
        address targetValidator;
        uint256 shares;
        uint256 blockNumber;
        string issuer;
    }

    // 0 => Raw,
    // 1 => StakeRegisterValidatorEvent,
    // 2 => StakeEvent,
    // 3 => ValidatorExitEvent,
    // 4 => UnstakeEvent,
    struct UnsupportedJWK {
        bytes id;
        bytes payload;
    }
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
        ProviderJWKs[] calldata providerJWKsArray,
        CrossChainParams[] calldata crossChainParamsArray
    ) external;

    event ObservedJWKsUpdated(uint256 indexed epoch, ProviderJWKs[] jwks);
}

fn convert_into_api_jwk(jwk: JWK) -> JWKStruct {
    if jwk.variant == 0 {
        // Note: Gravity relayer does not fetch RSA JWKs directly. RSA JWKs are fetched in Aptos code
        JWKStruct { type_name: "0x1::jwks::RSA_JWK".to_string(), data: jwk.data.into() }
    } else {
        // All data fetched by gravity relayer is contained within UnsupportedJWK in the data field
        JWKStruct { type_name: "0x1::jwks::UnsupportedJWK".to_string(), data: jwk.data.into() }
    }
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
            .map(|jwk| {
                let variant = match jwk.type_name.as_str() {
                    "0x1::jwks::RSA_JWK" => 0,
                    _ => 1,
                };
                JWK { variant, data: jwk.data.into() }
            })
            .collect(),
    }
}

fn construct_params_string(crosschain_params: &CrossChainParams) -> String {
    format!(
        "CrossChainParams {{\n  id: {:?},\n  validatorParams: ValidatorRegistrationParams {{\n    consensusPublicKey: {:?},\n    blsProof: {:?},\n    commission: Commission {{ rate: {}, maxRate: {}, maxChangeRate: {} }},\n    moniker: {:?},\n    initialOperator: {:?},\n    initialBeneficiary: {:?},\n    validatorNetworkAddresses: {:?},\n    fullnodeNetworkAddresses: {:?},\n    aptosAddress: {:?}\n  }},\n  targetValidator: {:?},\n  shares: {:?},\n  blockNumber: {:?},\n  issuer: {:?}\n}}",
        crosschain_params.id,
        crosschain_params.validatorParams.consensusPublicKey,
        crosschain_params.validatorParams.blsProof,
        crosschain_params.validatorParams.commission.rate,
        crosschain_params.validatorParams.commission.maxRate,
        crosschain_params.validatorParams.commission.maxChangeRate,
        crosschain_params.validatorParams.moniker,
        crosschain_params.validatorParams.initialOperator,
        crosschain_params.validatorParams.initialBeneficiary,
        crosschain_params.validatorParams.validatorNetworkAddresses,
        crosschain_params.validatorParams.fullnodeNetworkAddresses,
        crosschain_params.validatorParams.aptosAddress,
        crosschain_params.targetValidator,
        crosschain_params.shares,
        crosschain_params.blockNumber,
        crosschain_params.issuer
    )
}

fn print_crosschain_params(crosschain_params: &CrossChainParams) {
    let params_string = construct_params_string(crosschain_params);
    debug!(
        target: "gravity-relayer",
        "CrossChainParams created:\n{}",
        params_string
    );
}

fn convert_into_sol_crosschain_params(jwks: &Vec<JWK>, issuer: String) -> Vec<CrossChainParams> {
    jwks.iter()
        .filter(|jwk| jwk.variant == 1)
        .map(|jwk| {
            let crosschain_params = process_unsupported_jwk(jwk, &issuer);
            print_crosschain_params(&crosschain_params);
            crosschain_params
        })
        .collect()
}

fn process_unsupported_jwk(jwk: &JWK, issuer: &str) -> CrossChainParams {
    let unsupported_jwk = UnsupportedJWK::abi_decode(&jwk.data).unwrap();
    let id_hash = keccak256(&unsupported_jwk.id);

    match id_hash {
        hash if hash == EVENT_TYPE_1_HASH => {
            // StakeRegisterValidatorEvent
            let event =
                StakeRegisterValidatorEvent::abi_decode_data(&unsupported_jwk.payload).unwrap();
            let validator_params = ValidatorRegistrationParams::abi_decode(&event.2).unwrap();

            CrossChainParams {
                id: unsupported_jwk.id,
                validatorParams: validator_params,
                targetValidator: event.0,
                shares: event.1,
                blockNumber: event.3,
                issuer: issuer.to_string(),
            }
        }
        hash if hash == EVENT_TYPE_2_HASH => {
            // StakeEvent
            let event = StakeEvent::abi_decode_data(&unsupported_jwk.payload).unwrap();

            CrossChainParams {
                id: unsupported_jwk.id,
                validatorParams: DEFAULT_VALIDATOR_PARAMS.clone(),
                targetValidator: event.0,
                shares: U256::from(0),
                blockNumber: event.3,
                issuer: issuer.to_string(),
            }
        }
        hash if hash == EVENT_TYPE_3_HASH => {
            // ValidatorExitEvent
            let event = ValidatorExitEvent::abi_decode_data(&unsupported_jwk.payload).unwrap();

            CrossChainParams {
                id: unsupported_jwk.id,
                validatorParams: DEFAULT_VALIDATOR_PARAMS.clone(),
                targetValidator: event.0,
                shares: U256::from(0),
                blockNumber: event.3,
                issuer: issuer.to_string(),
            }
        }
        hash if hash == EVENT_TYPE_4_HASH => {
            // UnstakeEvent
            let event = UnstakeEvent::abi_decode_data(&unsupported_jwk.payload).unwrap();

            CrossChainParams {
                id: unsupported_jwk.id,
                validatorParams: DEFAULT_VALIDATOR_PARAMS.clone(),
                targetValidator: event.0,
                shares: event.1,
                blockNumber: event.3,
                issuer: issuer.to_string(),
            }
        }
        _ => panic!("Unsupported event type: {:?}", id_hash),
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
            let cross_chain_params = convert_into_sol_crosschain_params(
                &provider_jwks.jwks,
                provider_jwks.issuer.clone(),
            );

            let call = upsertObservedJWKsCall {
                providerJWKsArray: vec![provider_jwks],
                crossChainParamsArray: cross_chain_params,
            };
            let input: Bytes = call.abi_encode().into();
            let current_nonce = system_caller_nonce + index as u64;
            new_system_call_txn(JWK_MANAGER_ADDR, current_nonce, gas_price, input)
        })
        .collect();
    txns
}
