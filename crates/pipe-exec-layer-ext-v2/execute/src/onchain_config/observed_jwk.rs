//! Fetcher for JWK (JSON Web Key) on-chain configuration

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    JWK_MANAGER_ADDR, SYSTEM_CALLER,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent, SolType};
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use reth_ethereum_primitives::TransactionSigned;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use std::fmt::Debug;
use tracing::info;

sol! {
    event DepositGravityEvent(
        address user,
        uint256 amount,
        address targetAddress,
        uint256 blockNumber
    );

    event ChangeRecord(
        bytes32 key,
        bytes32 value,
        uint256 blockNumber,
        address updater,
        uint256 sequenceNumber
    );
}

sol! {
    struct CrossChainParams {
        // 1 => CrossChainDepositEvent
        bytes id;
        address sender;
        address targetAddress;
        uint256 amount;
        uint256 blockNumber;
        string issuer;
        bytes data; // 额外数据（用于哈希记录等）
    }

    // 0 => Raw,
    // 1 => CrossChainDepositEvent
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
        // Note: Gravity relayer does not fetch RSA JWKs directly. RSA JWKs are fetched in Aptos
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

/// Parse chain_id from issuer URI
/// Issuer format: gravity://{chain_id}/event?address={contract_address}&topic0={topic0}&fromBlock={from_block}
fn parse_chain_id_from_issuer(issuer: &str) -> Option<u32> {
    if issuer.starts_with("gravity://") {
        // Extract the part after "gravity://" and before the next "/"
        let after_protocol = &issuer[10..]; // Skip "gravity://"
        if let Some(slash_pos) = after_protocol.find('/') {
            let chain_id_str = &after_protocol[..slash_pos];
            return chain_id_str.parse().ok();
        }
    }
    None
}

fn convert_into_sol_crosschain_params(jwks: &Vec<JWK>, issuer: &str) -> Vec<CrossChainParams> {
    jwks.iter()
        .filter(|jwk| jwk.variant == 1)
        .map(|jwk| process_unsupported_jwk(jwk, &issuer))
        .collect()
}

fn process_unsupported_jwk(jwk: &JWK, issuer: &str) -> CrossChainParams {
    let unsupported_jwk = UnsupportedJWK::abi_decode(&jwk.data).unwrap();
    let id_string = String::from_utf8(unsupported_jwk.id.to_vec())
        .expect("Failed to convert id bytes to string");
    let data_type: u8 = id_string.parse().expect("Failed to parse data_type from string");

    match data_type {
        hash if hash == 1 => {
            // DepositGravityEvent
            let event = DepositGravityEvent::abi_decode_data(&unsupported_jwk.payload).unwrap();

            info!(target: "observed_jwk stake event",
                user=?event.0,
                amount=?event.1,
                target_address=?event.2,
                block_number=?event.3,
                "observed_jwk stake event created"
            );
            CrossChainParams {
                id: unsupported_jwk.id,
                sender: event.0,
                targetAddress: event.2,
                amount: event.1,
                blockNumber: event.3,
                issuer: issuer.to_string(),
                data: Bytes::new(), // deposit模式为空
            }
        }
        hash if hash == 2 => {
            // ChangeRecord
            // All parameters are non-indexed, so all fields are in the data part
            let event = ChangeRecord::abi_decode_data(&unsupported_jwk.payload).unwrap();
            let hash_value: B256 = event.1;
            let block_number = event.2;
            let sequence_number = event.4;

            //bytes memory data = crossChainParam.data;
            // require(data.length == 76, "Invalid hash record data length");

            // bytes32 hash;
            // uint64 sourceBlockNumber;
            // uint32 sourceChainId;
            // uint256 sequenceNumber;

            // assembly {
            //     hash := mload(add(data, 32))
            //     sourceBlockNumber := mload(add(data, 64))
            //     sourceChainId := mload(add(data, 72))
            //     sequenceNumber := mload(add(data, 104))
            // }
            // Build 76-byte data according to Solidity contract expectations:
            // - hash (bytes32): 32 bytes at offset 0 (from event.1)
            // - sourceBlockNumber (uint64): 8 bytes at offset 32 (from event.2, converted to u64)
            // - sourceChainId (uint32): 4 bytes at offset 40 (parsed from issuer)
            // - sequenceNumber (uint256): 32 bytes at offset 44 (from event.4)
            // Total: 76 bytes
            let mut data_bytes = vec![0u8; 76];

            // hash (bytes32) - 32 bytes at offset 0 (event.1)
            data_bytes[0..32].copy_from_slice(hash_value.as_slice());

            // sourceBlockNumber (uint64) - 8 bytes at offset 32 (event.2 converted to u64)
            let source_block_number = block_number.to::<u64>();
            data_bytes[32..40].copy_from_slice(&source_block_number.to_be_bytes());

            // sourceChainId (uint32) - 4 bytes at offset 40 (parsed from issuer)
            let source_chain_id = parse_chain_id_from_issuer(issuer).unwrap_or(0u32);

            info!(target: "observed_jwk change record event",
                key=?event.0,
                hash=?hash_value,
                block_number=?block_number,
                updater=?event.3,
                sequence_number=?sequence_number,
                source_chain_id=?source_chain_id,
                "observed_jwk change record event created"
            );

            data_bytes[40..44].copy_from_slice(&source_chain_id.to_be_bytes());

            // sequenceNumber (uint256) - 32 bytes at offset 44 (event.4)
            let sequence_bytes: [u8; 32] = sequence_number.to_be_bytes();
            data_bytes[44..76].copy_from_slice(&sequence_bytes);

            CrossChainParams {
                id: unsupported_jwk.id,
                sender: event.3,              // updater is in event.3
                targetAddress: Address::ZERO, // ChangeRecord doesn't have targetAddress
                amount: U256::ZERO,           // ChangeRecord doesn't have amount
                blockNumber: block_number,
                issuer: issuer.to_string(),
                data: Bytes::from(data_bytes), // Store 76-byte data as expected by contract
            }
        }
        _ => panic!("Unsupported event type: {:?}, id: {:?}", data_type, unsupported_jwk.id),
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

/// Construct JWK transaction from ProviderJWKs
///
/// This function is called by the validator transactions construction logic in mod.rs
pub(crate) fn construct_jwk_transaction(
    provider_jwks: gravity_api_types::on_chain_config::jwks::ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let sol_provider_jwks = convert_into_sol_provider_jwks(provider_jwks);
    let cross_chain_params = convert_into_sol_crosschain_params(
        &sol_provider_jwks.jwks,
        sol_provider_jwks.issuer.as_str(),
    );

    let call = upsertObservedJWKsCall {
        providerJWKsArray: vec![sol_provider_jwks],
        crossChainParamsArray: cross_chain_params,
    };
    let input: Bytes = call.abi_encode().into();
    Ok(super::new_system_call_txn(JWK_MANAGER_ADDR, nonce, gas_price, input))
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
        SYSTEM_CALLER
    }
}
