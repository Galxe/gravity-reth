//! JWK Oracle write path for consensus-approved relayer payloads.
//!
//! UnsupportedJWK entries carry a canonical ABI wrapper containing
//! `(nonce, source_position, callback_payload)`. The NativeOracle ABI retains
//! the legacy `blockNumber` name, but the value is a source-defined restart
//! position and payload history is no longer stored by NativeOracle.

use super::{new_system_call_txn, NATIVE_ORACLE_ADDR};
use alloy_primitives::{Bytes, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolValue};
use gravity_api_types::on_chain_config::jwks::{JWKStruct, ProviderJWKs};
use reth_ethereum_primitives::TransactionSigned;
use reth_pipe_exec_layer_relayer::{parse_oracle_uri, source_types};
use tracing::{debug, info, warn};

const CALLBACK_GAS_LIMIT: u64 = 500_000;

sol! {
    function record(
        uint32 sourceType,
        uint256 sourceId,
        uint128 nonce,
        uint256 blockNumber,
        bytes calldata payload,
        uint256 callbackGasLimit
    ) external;

    function recordBatch(
        uint32 sourceType,
        uint256 sourceId,
        uint128[] calldata nonces,
        uint256[] calldata blockNumbers,
        bytes[] calldata payloads,
        uint256[] calldata callbackGasLimits
    ) external;
}

fn is_rsa_jwk(jwk: &JWKStruct) -> bool {
    jwk.type_name == "0x1::jwks::RSA_JWK"
}

fn is_unsupported_jwk(jwk: &JWKStruct) -> bool {
    jwk.type_name == "0x1::jwks::Unsupported_JWK"
}

fn parse_source_from_issuer(issuer: &[u8]) -> Option<(u32, u64)> {
    let issuer = std::str::from_utf8(issuer).ok()?;
    let task = parse_oracle_uri(issuer).ok()?;
    Some((task.source_type, task.source_id))
}

fn callback_gas_limit(source_type: u32) -> Result<u64, String> {
    match source_type {
        source_types::BLOCKCHAIN | source_types::PRICE_FEED => Ok(CALLBACK_GAS_LIMIT),
        _ => Err(format!("Unsupported oracle source type: {source_type}")),
    }
}

fn extract_canonical_wrapper(data: &[u8]) -> Result<(u128, U256, Vec<u8>), String> {
    let decoded = <(u128, U256, Bytes)>::abi_decode(data)
        .map_err(|error| format!("Failed to decode oracle payload wrapper: {error}"))?;
    if decoded.abi_encode() != data {
        return Err("Oracle payload wrapper is not canonically encoded".to_string());
    }
    if decoded.1 > U256::from(u128::MAX) {
        return Err("Oracle source position exceeds NativeOracle uint128 range".to_string());
    }

    Ok((decoded.0, decoded.1, decoded.2.to_vec()))
}

pub fn construct_oracle_record_transaction(
    provider_jwks: ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let issuer = &provider_jwks.issuer;
    let issuer_str = String::from_utf8_lossy(issuer);

    let first_jwk = provider_jwks
        .jwks
        .first()
        .ok_or_else(|| format!("No JWKs found for issuer: {issuer_str}"))?;

    if is_rsa_jwk(first_jwk) {
        warn!(
            target: "gravity::onchain_config::jwk_oracle",
            issuer = %issuer_str,
            jwk_count = provider_jwks.jwks.len(),
            "RSA JWK path entered unexpectedly; rejecting unsupported execution path"
        );
        Err(format!(
            "RSA JWK oracle record path is not supported: issuer={issuer_str}, jwk_count={}",
            provider_jwks.jwks.len()
        ))
    } else if is_unsupported_jwk(first_jwk) {
        construct_unsupported_oracle_batch_transaction(provider_jwks, nonce, gas_price)
    } else {
        Err(format!("Unknown JWK type '{}' for issuer: {issuer_str}", first_jwk.type_name))
    }
}

fn construct_unsupported_oracle_batch_transaction(
    provider_jwks: ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let issuer = &provider_jwks.issuer;
    let jwks = &provider_jwks.jwks;

    let (source_type, source_id) = parse_source_from_issuer(issuer)
        .ok_or_else(|| format!("Failed to parse source coordinates from issuer: {issuer:?}"))?;
    let callback_gas_limit = callback_gas_limit(source_type)?;

    if jwks.is_empty() {
        return Err("No oracle entries found".to_string());
    }

    let mut nonces = Vec::with_capacity(jwks.len());
    let mut source_positions = Vec::with_capacity(jwks.len());
    let mut payloads = Vec::with_capacity(jwks.len());
    let mut gas_limits = Vec::with_capacity(jwks.len());
    let mut previous_nonce: Option<u128> = None;

    for (index, jwk) in jwks.iter().enumerate() {
        if !is_unsupported_jwk(jwk) {
            return Err(format!("Mixed JWK types in unsupported oracle batch at index {index}"));
        }

        let (event_nonce, source_position, callback_payload) = extract_canonical_wrapper(&jwk.data)
            .map_err(|error| {
                warn!(
                    target: "gravity::onchain_config::jwk_oracle",
                    index,
                    payload_len = jwk.data.len(),
                    %error,
                    "Rejected oracle payload wrapper"
                );
                format!("Invalid oracle payload wrapper at index {index}: {error}")
            })?;

        if let Some(previous) = previous_nonce {
            let expected =
                previous.checked_add(1).ok_or_else(|| "Oracle batch nonce overflow".to_string())?;
            if event_nonce != expected {
                return Err(format!(
                    "Oracle batch nonces are not sequential at index {index}: expected {expected}, got {event_nonce}"
                ));
            }
        }
        previous_nonce = Some(event_nonce);

        nonces.push(event_nonce);
        source_positions.push(source_position);
        payloads.push(Bytes::from(callback_payload));
        gas_limits.push(U256::from(callback_gas_limit));

        debug!(
            target: "gravity::onchain_config::jwk_oracle",
            index,
            event_nonce,
            ?source_position,
            "Added canonical oracle entry to batch"
        );
    }

    info!(
        target: "gravity::onchain_config::jwk_oracle",
        issuer = %String::from_utf8_lossy(issuer),
        source_type,
        source_id,
        item_count = nonces.len(),
        "Constructing NativeOracle recordBatch transaction"
    );

    let call = recordBatchCall {
        sourceType: source_type,
        sourceId: U256::from(source_id),
        nonces,
        blockNumbers: source_positions,
        payloads,
        callbackGasLimits: gas_limits,
    };

    Ok(new_system_call_txn(NATIVE_ORACLE_ADDR, nonce, gas_price, call.abi_encode().into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Transaction;
    use alloy_primitives::I256;

    sol! {
        struct PricePayloadForTest {
            uint256 feedId;
            uint64 roundId;
            uint64 resolvedAt;
            uint8 decimals;
            int256 price;
        }
    }

    fn wrapped_jwk(nonce: u128, position: U256, payload: &[u8]) -> JWKStruct {
        JWKStruct {
            type_name: "0x1::jwks::Unsupported_JWK".to_string(),
            data: (nonce, position, payload).abi_encode(),
        }
    }

    fn provider(uri: &[u8], jwks: Vec<JWKStruct>) -> ProviderJWKs {
        ProviderJWKs { issuer: uri.to_vec(), version: 1, jwks }
    }

    #[test]
    fn parses_source_coordinates_from_issuer() {
        let issuer = b"gravity://0/1/events?fromBlock=22000000";
        assert_eq!(parse_source_from_issuer(issuer), Some((0, 1)));
    }

    #[test]
    fn extracts_canonical_wrapper() {
        let encoded = (7u128, U256::from(3020), b"oracle-payload".as_slice()).abi_encode();
        let (nonce, position, payload) = extract_canonical_wrapper(&encoded).unwrap();
        assert_eq!(nonce, 7);
        assert_eq!(position, U256::from(3020));
        assert_eq!(payload, b"oracle-payload");
    }

    #[test]
    fn rejects_noncanonical_wrapper() {
        let mut encoded = (7u128, U256::from(3020), b"oracle-payload".as_slice()).abi_encode();
        encoded.push(0);
        assert!(extract_canonical_wrapper(&encoded).is_err());
    }

    #[test]
    fn preserves_source_zero_coordinates_and_callback_gas() {
        let payload = b"bridge-event-payload";
        let provider = provider(
            b"gravity://0/1/events?fromBlock=22000000",
            vec![wrapped_jwk(7, U256::from(22_000_123u64), payload)],
        );

        let tx = construct_oracle_record_transaction(provider, 0, 0).unwrap();
        let call = recordBatchCall::abi_decode(tx.input()).unwrap();
        assert_eq!(call.sourceType, source_types::BLOCKCHAIN);
        assert_eq!(call.sourceId, U256::from(1));
        assert_eq!(call.nonces, vec![7]);
        assert_eq!(call.blockNumbers, vec![U256::from(22_000_123u64)]);
        assert_eq!(call.payloads, vec![Bytes::copy_from_slice(payload)]);
        assert_eq!(call.callbackGasLimits, vec![U256::from(CALLBACK_GAS_LIMIT)]);
    }

    #[test]
    fn preserves_price_feed_coordinates_payload_and_callback_gas() {
        let payload = PricePayloadForTest {
            feedId: U256::from(2001),
            roundId: 28_500_000,
            resolvedAt: 1_710_000_059_999,
            decimals: 8,
            price: "40067545000".parse::<I256>().unwrap(),
        }
        .abi_encode();
        let provider = provider(
            b"gravity://3/2001/price_feed?provider=binance_index_kline_v1",
            vec![wrapped_jwk(1, U256::from(1_710_000_059_999u64), &payload)],
        );

        let tx = construct_oracle_record_transaction(provider, 0, 0).unwrap();
        let call = recordBatchCall::abi_decode(tx.input()).unwrap();
        assert_eq!(call.sourceType, source_types::PRICE_FEED);
        assert_eq!(call.sourceId, U256::from(2001));
        assert_eq!(call.nonces, vec![1]);
        assert_eq!(call.blockNumbers, vec![U256::from(1_710_000_059_999u64)]);
        assert_eq!(call.payloads, vec![Bytes::from(payload)]);
        assert_eq!(call.callbackGasLimits, vec![U256::from(CALLBACK_GAS_LIMIT)]);
    }

    #[test]
    fn rejects_provider_source_types_not_implemented_by_core() {
        let provider = provider(
            b"gravity://6/1001/settlement",
            vec![wrapped_jwk(1, U256::from(60_000), b"settlement")],
        );
        let error = construct_oracle_record_transaction(provider, 0, 0).unwrap_err();
        assert_eq!(error, "Unsupported oracle source type: 6");
    }

    #[test]
    fn rejects_nonsequential_batch_before_execution() {
        let provider = provider(
            b"gravity://0/1/events",
            vec![
                wrapped_jwk(7, U256::from(100), b"first"),
                wrapped_jwk(9, U256::from(101), b"gap"),
            ],
        );
        let error = construct_oracle_record_transaction(provider, 0, 0).unwrap_err();
        assert!(error.contains("expected 8, got 9"));
    }

    #[test]
    fn rejects_mixed_jwk_types() {
        let mut mixed = wrapped_jwk(8, U256::from(101), b"mixed");
        mixed.type_name = "0x1::jwks::RSA_JWK".to_string();
        let provider = provider(
            b"gravity://0/1/events",
            vec![wrapped_jwk(7, U256::from(100), b"first"), mixed],
        );
        let error = construct_oracle_record_transaction(provider, 0, 0).unwrap_err();
        assert!(error.contains("Mixed JWK types"));
    }

    #[test]
    fn rejects_source_position_outside_contract_range() {
        let position = U256::from(u128::MAX) + U256::from(1);
        let provider =
            provider(b"gravity://0/1/events", vec![wrapped_jwk(1, position, b"payload")]);
        let error = construct_oracle_record_transaction(provider, 0, 0).unwrap_err();
        assert!(error.contains("uint128 range"));
    }
}
