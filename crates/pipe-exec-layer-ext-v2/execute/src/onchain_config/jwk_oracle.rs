//! JWK Oracle module for writing oracle updates via NativeOracle.record()
//!
//! This module handles the WRITE path for ALL oracle updates in the new Oracle architecture:
//! - RSA JWKs: rejected here because the active execution path uses UnsupportedJWK payloads
//! - UnsupportedJWK: NativeOracle.recordBatch() for oracle payloads
//!
//! For blockchain events, the payload from relayer is ABI-encoded and passed through unchanged.
//! This ensures byte-exact match between relayer, on-chain storage, and read-back for comparison.

use super::{new_system_call_txn, NATIVE_ORACLE_ADDR};
use alloy_primitives::{Bytes, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolValue};
use gravity_api_types::on_chain_config::jwks::{JWKStruct, ProviderJWKs};
use reth_ethereum_primitives::TransactionSigned;
use reth_pipe_exec_layer_relayer::{parse_oracle_uri, source_types};
use tracing::{debug, info, warn};

/// Existing bridge callbacks and price resolvers fit within the legacy budget.
const STANDARD_CALLBACK_GAS_LIMIT: u64 = 500_000;

/// Polymarket payouts may contain up to 32 outcome slots.
const POLYMARKET_CALLBACK_GAS_LIMIT: u64 = 2_000_000;

// =============================================================================
// Solidity Types (NativeOracle function signatures)
// =============================================================================

sol! {
    /// NativeOracle.record() function signature
    function record(
        uint32 sourceType,
        uint256 sourceId,
        uint128 nonce,
        uint256 blockNumber,
        bytes calldata payload,
        uint256 callbackGasLimit
    ) external;

    /// NativeOracle.recordBatch() function signature for multiple events
    function recordBatch(
        uint32 sourceType,
        uint256 sourceId,
        uint128[] calldata nonces,
        uint256[] calldata blockNumbers,
        bytes[] calldata payloads,
        uint256[] calldata callbackGasLimits
    ) external;
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Check if a JWKStruct is an RSA JWK
fn is_rsa_jwk(jwk: &JWKStruct) -> bool {
    jwk.type_name == "0x1::jwks::RSA_JWK"
}

/// Check if a JWKStruct is an UnsupportedJWK (blockchain/other oracle data)
fn is_unsupported_jwk(jwk: &JWKStruct) -> bool {
    jwk.type_name == "0x1::jwks::Unsupported_JWK"
}

/// Parse source type and source id from issuer URI.
/// Format: gravity://{source_type}/{source_id}/{task_type}?...
fn parse_source_from_issuer(issuer: &[u8]) -> Option<(u32, u64)> {
    let issuer_str = std::str::from_utf8(issuer).ok()?;
    let task = parse_oracle_uri(issuer_str).ok()?;
    Some((task.source_type, task.source_id))
}

fn callback_gas_limit(source_type: u32) -> Result<u64, String> {
    match source_type {
        source_types::BLOCKCHAIN | source_types::PRICE_FEED => Ok(STANDARD_CALLBACK_GAS_LIMIT),
        source_types::POLYMARKET_SETTLEMENT => Ok(POLYMARKET_CALLBACK_GAS_LIMIT),
        _ => Err(format!("Unsupported oracle source type: {source_type}")),
    }
}

/// Extract a canonical ABI `(uint128 nonce, uint256 blockNumber, bytes payload)` tuple.
fn extract_nonce_block_and_payload(data: &[u8]) -> Option<(u128, U256, Vec<u8>)> {
    let decoded = match <(u128, U256, Bytes)>::abi_decode(data) {
        Ok(decoded) => decoded,
        Err(error) => {
            warn!(
                target: "gravity::onchain_config::jwk_oracle",
                data_len = data.len(),
                ?error,
                "Failed to decode oracle payload wrapper"
            );
            return None;
        }
    };
    if decoded.abi_encode() != data {
        warn!(
            target: "gravity::onchain_config::jwk_oracle",
            data_len = data.len(),
            "Rejected non-canonical oracle payload wrapper"
        );
        return None;
    }

    Some((decoded.0, decoded.1, decoded.2.to_vec()))
}

// =============================================================================
// Public API
// =============================================================================

/// Construct transaction for oracle update via NativeOracle.record()
///
/// This is the unified entry point for ALL oracle updates. It routes based on JWK type:
/// - RSA_JWK → explicit error; this execution path is not enabled
/// - UnsupportedJWK → Uses recordBatch for ALL logs (payload passed through unchanged)
///
/// Note: All JWKs in provider_jwks.jwks are guaranteed to be of the same type
/// (either all RSA or all unsupported), so we only check the first element.
pub fn construct_oracle_record_transaction(
    provider_jwks: ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let issuer = &provider_jwks.issuer;
    let issuer_str = String::from_utf8_lossy(issuer);

    // All JWKs are homogeneous, check the first one to determine the type
    let first_jwk = provider_jwks
        .jwks
        .first()
        .ok_or_else(|| format!("No JWKs found for issuer: {}", issuer_str))?;

    if is_rsa_jwk(first_jwk) {
        // RSA JWK path is not exercised in production today — all JWK data flows through the
        // UnsupportedJWK (blockchain event) path, and the RSA record construction has never
        // been audited/exercised. Fail CLOSED: return a recoverable `Err` rather than run
        // unverified construction logic. It must NEVER panic — this runs on the deterministic
        // execute_ordered_block system-tx path (over consensus `extra_data`), whose `Err` the
        // caller logs + skips (lib.rs), so a panic here would deterministically halt every
        // validator on this ordered block (gravity-audit#822 class).
        warn!(
            target: "gravity::onchain_config::jwk_oracle",
            issuer = %issuer_str,
            jwk_count = provider_jwks.jwks.len(),
            "RSA JWK path entered unexpectedly — rejecting (unsupported in production)"
        );
        Err(format!(
            "RSA JWK oracle record path is not enabled: issuer={}, jwk_count={}",
            issuer_str,
            provider_jwks.jwks.len()
        ))
    } else if is_unsupported_jwk(first_jwk) {
        // Generic oracle records - use recordBatch for ALL entries
        construct_unsupported_oracle_batch_transaction(provider_jwks, nonce, gas_price)
    } else {
        warn!(target: "gravity::onchain_config::jwk_oracle", "Unknown JWK type '{}' for issuer: {}", first_jwk.type_name, issuer_str);
        Err(format!("Unknown JWK type '{}' for issuer: {}", first_jwk.type_name, issuer_str))
    }
}

/// Construct transaction for unsupported JWK oracle data using recordBatch()
///
/// This handles ALL UnsupportedJWK entries (each represents one event).
/// The payload is passed through UNCHANGED from relayer - this ensures
/// byte-exact match between what relayer sends and what gets stored on-chain.
fn construct_unsupported_oracle_batch_transaction(
    provider_jwks: ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let issuer = &provider_jwks.issuer;
    let jwks = &provider_jwks.jwks;

    // Parse NativeOracle coordinates from issuer
    let (source_type, source_id) = parse_source_from_issuer(issuer)
        .ok_or_else(|| format!("Failed to parse source coordinates from issuer: {:?}", issuer))?;
    let callback_gas_limit = callback_gas_limit(source_type)?;
    info!(
        target: "gravity::onchain_config::jwk_oracle",
        source_type,
        source_id,
        len = jwks.len(),
        "unsupported JWK oracle batch"
    );

    // All JWKs are guaranteed to be unsupported type when entering this function
    if jwks.is_empty() {
        return Err("No blockchain event JWKs found".to_string());
    }

    // Build batch arrays
    let mut nonces: Vec<u128> = Vec::with_capacity(jwks.len());
    let mut block_numbers: Vec<U256> = Vec::with_capacity(jwks.len());
    let mut payloads: Vec<Bytes> = Vec::with_capacity(jwks.len());
    let mut gas_limits: Vec<U256> = Vec::with_capacity(jwks.len());

    for (idx, jwk) in jwks.iter().enumerate() {
        let (event_nonce, block_number, inner_payload) =
            match extract_nonce_block_and_payload(&jwk.data) {
                Some((nonce, block_num, payload)) => (nonce, block_num, payload),
                None => {
                    warn!(
                        target: "gravity::onchain_config::jwk_oracle",
                        idx = idx,
                        payload_len = jwk.data.len(),
                        payload_hex = %hex::encode(&jwk.data),
                        "Failed to extract nonce, block_number, and payload"
                    );
                    return Err(format!(
                        "Failed to extract nonce, block_number, and payload at index {}",
                        idx
                    ));
                }
            };

        nonces.push(event_nonce);
        block_numbers.push(block_number);
        // Use the inner payload (the original resolver payload)
        // This is what the user put in and what gets passed to the callback
        payloads.push(inner_payload.into());
        gas_limits.push(U256::from(callback_gas_limit));

        debug!(
            idx = idx,
            event_nonce = event_nonce,
            ?block_number,
            inner_payload_len = payloads.last().map(|p: &Bytes| p.len()).unwrap_or(0),
            "Added event to batch"
        );
    }

    info!(
        issuer = %String::from_utf8_lossy(issuer),
        source_type = source_type,
        source_id = source_id,
        item_count = nonces.len(),
        "Constructing oracle recordBatch transaction (pass-through payload)"
    );

    // Use recordBatch for multiple events
    let call = recordBatchCall {
        sourceType: source_type,
        sourceId: U256::from(source_id),
        nonces,
        blockNumbers: block_numbers,
        payloads,
        callbackGasLimits: gas_limits,
    };

    let input: Bytes = call.abi_encode().into();
    Ok(new_system_call_txn(NATIVE_ORACLE_ADDR, nonce, gas_price, input))
}

// convert_oracle_rsa_to_api_jwk is now provided by super::types

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Transaction;
    use alloy_sol_macro::sol;
    use alloy_sol_types::SolValue;
    use reth_pipe_exec_layer_relayer::{OracleDataSource, PriceFeedSource};

    sol! {
        struct PriceObservationForTest {
            bytes32 dataSourceId;
            uint64 observedAt;
            int256 price;
            uint256 weight;
        }

        struct PricePayloadForTest {
            uint256 feedId;
            uint64 roundId;
            uint64 resolvedAt;
            uint8 decimals;
            uint8 aggregationMode;
            uint256 minSourceCount;
            uint256 minTotalWeight;
            uint64 maxStaleness;
            PriceObservationForTest[] observations;
        }
    }

    #[test]
    fn test_parse_source_from_issuer() {
        let issuer = b"gravity://3/1001/price_feed?provider=binance_index_kline_v1";
        assert_eq!(parse_source_from_issuer(issuer), Some((3, 1001)));
    }

    #[test]
    fn test_extract_nonce_block_and_payload() {
        let payload = b"oracle-payload";
        let encoded = SolValue::abi_encode(&(7u128, U256::from(3020u64), payload.as_slice()));

        let (nonce, block_number, inner_payload) =
            extract_nonce_block_and_payload(&encoded).expect("extract wrapper");

        assert_eq!(nonce, 7);
        assert_eq!(block_number, U256::from(3020u64));
        assert_eq!(inner_payload, payload);
    }

    #[test]
    fn test_extract_rejects_noncanonical_wrapper() {
        let mut encoded =
            SolValue::abi_encode(&(7u128, U256::from(3020u64), b"oracle-payload".as_slice()));
        encoded.push(0);

        assert!(extract_nonce_block_and_payload(&encoded).is_none());
    }

    #[test]
    fn test_rsa_jwk_path_returns_error_without_panicking() {
        let provider = ProviderJWKs {
            issuer: b"https://issuer.example".to_vec(),
            version: 1,
            jwks: vec![JWKStruct { type_name: "0x1::jwks::RSA_JWK".to_string(), data: vec![] }],
        };

        let err = construct_oracle_record_transaction(provider, 0, 0).unwrap_err();
        assert!(err.contains("not enabled"));
    }

    #[test]
    fn test_construct_unsupported_batch_uses_source_from_issuer() {
        let resolver_payload = b"price-feed-resolver-payload";
        let wrapped_payload =
            SolValue::abi_encode(&(1u128, U256::from(3020u64), resolver_payload.as_slice()));
        let provider = ProviderJWKs {
            issuer: b"gravity://3/1001/price_feed?provider=inline_fixture_v1&round=1".to_vec(),
            version: 1,
            jwks: vec![JWKStruct {
                type_name: "0x1::jwks::Unsupported_JWK".to_string(),
                data: wrapped_payload,
            }],
        };

        let tx = construct_oracle_record_transaction(provider, 0, 0).expect("construct tx");
        let call = recordBatchCall::abi_decode(tx.input()).expect("decode recordBatch call");

        assert_eq!(call.sourceType, 3);
        assert_eq!(call.sourceId, U256::from(1001u64));
        assert_eq!(call.nonces, vec![1]);
        assert_eq!(call.blockNumbers, vec![U256::from(3020u64)]);
        assert_eq!(call.payloads, vec![Bytes::copy_from_slice(resolver_payload)]);
        assert_eq!(call.callbackGasLimits, vec![U256::from(STANDARD_CALLBACK_GAS_LIMIT)]);
    }

    #[test]
    fn test_construct_unsupported_batch_preserves_legacy_blockchain_coordinates_and_gas() {
        let resolver_payload = b"bridge-event-payload";
        let wrapped_payload =
            SolValue::abi_encode(&(7u128, U256::from(22_000_123u64), resolver_payload.as_slice()));
        let provider = ProviderJWKs {
            issuer: b"gravity://0/1/events?fromBlock=22000000".to_vec(),
            version: 1,
            jwks: vec![JWKStruct {
                type_name: "0x1::jwks::Unsupported_JWK".to_string(),
                data: wrapped_payload,
            }],
        };

        let tx = construct_oracle_record_transaction(provider, 0, 0).expect("construct tx");
        let call = recordBatchCall::abi_decode(tx.input()).expect("decode recordBatch call");

        assert_eq!(call.sourceType, source_types::BLOCKCHAIN);
        assert_eq!(call.sourceId, U256::from(1));
        assert_eq!(call.nonces, vec![7]);
        assert_eq!(call.blockNumbers, vec![U256::from(22_000_123u64)]);
        assert_eq!(call.payloads, vec![Bytes::copy_from_slice(resolver_payload)]);
        assert_eq!(call.callbackGasLimits, vec![U256::from(STANDARD_CALLBACK_GAS_LIMIT)]);
    }

    #[test]
    fn test_construct_unsupported_batch_supports_polymarket_source_type() {
        let resolver_payload = b"polymarket-settlement-resolver-payload";
        let wrapped_payload =
            SolValue::abi_encode(&(1u128, U256::from(89_222_209u64), resolver_payload.as_slice()));
        let provider = ProviderJWKs {
            issuer: b"gravity://6/1897398/polymarket_settlement?fromBlock=89222200".to_vec(),
            version: 1,
            jwks: vec![JWKStruct {
                type_name: "0x1::jwks::Unsupported_JWK".to_string(),
                data: wrapped_payload,
            }],
        };

        let tx = construct_oracle_record_transaction(provider, 0, 0).expect("construct tx");
        let call = recordBatchCall::abi_decode(tx.input()).expect("decode recordBatch call");

        assert_eq!(call.sourceType, 6);
        assert_eq!(call.sourceId, U256::from(1_897_398u64));
        assert_eq!(call.nonces, vec![1]);
        assert_eq!(call.blockNumbers, vec![U256::from(89_222_209u64)]);
        assert_eq!(call.payloads, vec![Bytes::copy_from_slice(resolver_payload)]);
        assert_eq!(call.callbackGasLimits, vec![U256::from(POLYMARKET_CALLBACK_GAS_LIMIT)]);
    }

    #[test]
    fn test_construct_unsupported_batch_rejects_unknown_source_type() {
        let wrapped_payload =
            SolValue::abi_encode(&(1u128, U256::from(1u64), b"payload".as_slice()));
        let provider = ProviderJWKs {
            issuer: b"gravity://99/1/custom".to_vec(),
            version: 1,
            jwks: vec![JWKStruct {
                type_name: "0x1::jwks::Unsupported_JWK".to_string(),
                data: wrapped_payload,
            }],
        };

        let err = construct_oracle_record_transaction(provider, 0, 0).unwrap_err();
        assert_eq!(err, "Unsupported oracle source type: 99");
    }

    #[tokio::test]
    async fn test_price_feed_source_payload_reaches_record_batch() {
        let uri = "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:2000:10000000000:1,source-b:2000:10200000000:2,source-c:2000:9800000000:1";
        let task = parse_oracle_uri(uri).expect("parse price feed uri");
        let source = PriceFeedSource::from_task(&task, 0).expect("create price feed source");
        let data = source.poll().await.expect("poll price feed source");
        assert_eq!(data.len(), 1);

        let provider = ProviderJWKs {
            issuer: uri.as_bytes().to_vec(),
            version: 1,
            jwks: vec![JWKStruct {
                type_name: "0x1::jwks::Unsupported_JWK".to_string(),
                data: data[0].payload.to_vec(),
            }],
        };

        let tx = construct_oracle_record_transaction(provider, 0, 0).expect("construct tx");
        let call = recordBatchCall::abi_decode(tx.input()).expect("decode recordBatch call");

        assert_eq!(call.sourceType, 3);
        assert_eq!(call.sourceId, U256::from(1));
        assert_eq!(call.nonces, vec![1]);
        assert_eq!(call.blockNumbers, vec![U256::from(2010u64)]);
        assert_eq!(call.payloads.len(), 1);

        let payload =
            PricePayloadForTest::abi_decode(&call.payloads[0]).expect("decode resolver payload");
        assert_eq!(payload.feedId, U256::from(1));
        assert_eq!(payload.roundId, 1);
        assert_eq!(payload.resolvedAt, 2010);
        assert_eq!(payload.decimals, 8);
        assert_eq!(payload.aggregationMode, 1);
        assert_eq!(payload.observations.len(), 3);
    }
}
