//! JWK Oracle module for writing oracle updates via NativeOracle.record()
//!
//! This module handles the WRITE path for ALL oracle updates in the new Oracle architecture:
//! - RSA JWKs: NativeOracle.record(sourceType=1, sourceId=keccak256(issuer))
//! - UnsupportedJWK (blockchain events): NativeOracle.recordBatch() for multiple logs
//!
//! For blockchain events, the payload from relayer is ABI-encoded and passed through unchanged.
//! This ensures byte-exact match between relayer, on-chain storage, and read-back for comparison.

use super::{
    new_system_call_txn,
    types::{GaptosRsaJwk, OracleRSA_JWK, SOURCE_TYPE_JWK},
    NATIVE_ORACLE_ADDR,
};
use alloy_primitives::{keccak256, Bytes, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::jwks::{JWKStruct, ProviderJWKs};
use reth_ethereum_primitives::TransactionSigned;
use tracing::{debug, info, warn};

/// Default callback gas limit for oracle updates
const CALLBACK_GAS_LIMIT: u64 = 500_000;

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
/// Checks for sourceType string (0, 1, 2, etc.) instead of fixed type_name
fn is_unsupported_jwk(jwk: &JWKStruct) -> bool {
    // Check if type_name is a numeric string (sourceType)
    // TODO(gravity): check if it should be "0x1::jwks::UNSUPPORTED_JWK"
    jwk.type_name.parse::<u32>().is_ok()
}

/// Parse RSA JWK from BCS-encoded data
fn parse_rsa_jwk_from_bcs(data: &[u8]) -> Option<OracleRSA_JWK> {
    let gaptos_jwk: GaptosRsaJwk = bcs::from_bytes(data).ok()?;
    Some(OracleRSA_JWK {
        kid: gaptos_jwk.kid,
        kty: gaptos_jwk.kty,
        alg: gaptos_jwk.alg,
        e: gaptos_jwk.e,
        n: gaptos_jwk.n,
    })
}

/// Parse chain_id from issuer URI
/// Format: gravity://{source_type}/{chain_id}/{task_type}?...
fn parse_chain_id_from_issuer(issuer: &[u8]) -> Option<u64> {
    let issuer_str = String::from_utf8_lossy(issuer);
    if issuer_str.starts_with("gravity://") {
        let after_protocol = &issuer_str[10..];
        // Skip source_type (first segment), get chain_id (second segment)
        // Format: {source_type}/{chain_id}/{task_type}?...
        let mut parts = after_protocol.split('/');
        let _source_type = parts.next()?; // Skip source_type
        let chain_id_str = parts.next()?;
        return chain_id_str.parse().ok();
    }
    None
}

/// Extract nonce, block_number, and inner payload from ABI-encoded event data
/// Payload format: alloy's abi_encode(&(u128, U256, &[u8]))
///
/// alloy encodes (u128, U256, &[u8]) as a dynamic tuple with structure:
/// - bytes 0-31:   offset to tuple data (always 32 = 0x20)
/// - bytes 32-63:  nonce (uint128, right-aligned, so nonce is at bytes 48-63)
/// - bytes 64-95:  block_number (uint256)
/// - bytes 96-127: offset to bytes data (relative to tuple start at byte 32)
/// - bytes 128-159: payload length
/// - bytes 160+:   payload data
///
/// Returns (nonce, block_number, inner_payload)
fn extract_nonce_block_and_payload(data: &[u8]) -> Option<(u128, U256, Vec<u8>)> {
    if data.len() < 160 {
        warn!(
            target: "gravity::onchain_config::jwk_oracle",
            data_len = data.len(),
            "Data too short for ABI decoding"
        );
        return None;
    }

    // nonce is at bytes 32-63, right-aligned u128 so actual value is at bytes 48-63
    let nonce_bytes = &data[48..64];
    let nonce = u128::from_be_bytes(nonce_bytes.try_into().ok()?);

    // block_number is at bytes 64-95 (full U256)
    let block_number = U256::from_be_slice(&data[64..96]);

    // Payload length is at bytes 128-159 (right-aligned u256)
    let length_bytes = &data[128..160];
    let payload_len = u64::from_be_bytes(length_bytes[24..32].try_into().ok()?) as usize;

    // Check we have enough data for the payload
    if data.len() < 160 + payload_len {
        warn!(
            target: "gravity::onchain_config::jwk_oracle",
            data_len = data.len(),
            payload_len = payload_len,
            "Not enough data for payload"
        );
        return None;
    }

    // Extract the inner payload starting at byte 160
    let inner_payload = data[160..160 + payload_len].to_vec();

    Some((nonce, block_number, inner_payload))
}

// =============================================================================
// Public API
// =============================================================================

/// Construct transaction for oracle update via NativeOracle.record()
///
/// This is the unified entry point for ALL oracle updates. It routes based on JWK type:
/// - RSA_JWK → sourceType=1 (JWK), payload=ABI(issuer, version, jwks[])
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
        // RSA JWK update
        construct_jwk_record_transaction(provider_jwks, nonce, gas_price)
    } else if is_unsupported_jwk(first_jwk) {
        // Blockchain/oracle events - use recordBatch for ALL logs
        construct_blockchain_batch_transaction(provider_jwks, nonce, gas_price)
    } else {
        warn!(target: "gravity::onchain_config::jwk_oracle", "Unknown JWK type '{}' for issuer: {}", first_jwk.type_name, issuer_str);
        Err(format!("Unknown JWK type '{}' for issuer: {}", first_jwk.type_name, issuer_str))
    }
}

/// Construct transaction for JWK update (sourceType=1)
fn construct_jwk_record_transaction(
    provider_jwks: ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let issuer = &provider_jwks.issuer;
    let version = provider_jwks.version;

    // All JWKs are guaranteed to be RSA type when entering this function
    let rsa_jwks: Vec<OracleRSA_JWK> =
        provider_jwks.jwks.iter().filter_map(|jwk| parse_rsa_jwk_from_bcs(&jwk.data)).collect();

    info!(
        issuer = %String::from_utf8_lossy(issuer),
        version = version,
        jwk_count = rsa_jwks.len(),
        "Constructing JWK record transaction"
    );

    // sourceId = keccak256(issuer)
    let issuer_hash = keccak256(issuer);
    let source_id = U256::from_be_bytes(issuer_hash.0);

    // Encode payload: (bytes issuer, uint64 version, OracleRSA_JWK[] jwks)
    let payload = alloy_sol_types::SolValue::abi_encode(&(issuer.as_slice(), version, rsa_jwks));

    let call = recordCall {
        sourceType: SOURCE_TYPE_JWK,
        sourceId: source_id,
        nonce: nonce as u128,
        blockNumber: U256::ZERO, // JWK records don't have a source block number
        payload: payload.into(),
        callbackGasLimit: U256::from(CALLBACK_GAS_LIMIT),
    };

    let input: Bytes = call.abi_encode().into();
    Ok(new_system_call_txn(NATIVE_ORACLE_ADDR, nonce, gas_price, input))
}

/// Construct transaction for blockchain events using recordBatch()
///
/// This handles ALL UnsupportedJWK entries (each represents one event).
/// The payload is passed through UNCHANGED from relayer - this ensures
/// byte-exact match between what relayer sends and what gets stored on-chain.
fn construct_blockchain_batch_transaction(
    provider_jwks: ProviderJWKs,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    let issuer = &provider_jwks.issuer;
    let jwks = &provider_jwks.jwks;

    // Parse chain_id from issuer
    let chain_id = parse_chain_id_from_issuer(issuer)
        .ok_or_else(|| format!("Failed to parse chain_id from issuer: {:?}", issuer))?;
    info!(target: "gravity::onchain_config::jwk_oracle", "jwk chain_id: {}, len {:?}", chain_id, jwks.len());

    // All JWKs are guaranteed to be unsupported type when entering this function
    if jwks.is_empty() {
        return Err("No blockchain event JWKs found".to_string());
    }

    // Parse sourceType from first JWK's type_name (all have same type)
    let source_type: u32 = 0;

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
        // Use the inner payload (the original MessageSent.payload)
        // This is what the user put in and what gets passed to the callback
        payloads.push(inner_payload.into());
        gas_limits.push(U256::from(CALLBACK_GAS_LIMIT));

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
        chain_id = chain_id,
        source_type = source_type,
        event_count = nonces.len(),
        "Constructing blockchain recordBatch transaction (pass-through payload)"
    );

    // Use recordBatch for multiple events
    let call = recordBatchCall {
        sourceType: source_type,
        sourceId: U256::from(chain_id),
        nonces,
        blockNumbers: block_numbers,
        payloads,
        callbackGasLimits: gas_limits,
    };

    let input: Bytes = call.abi_encode().into();
    Ok(new_system_call_txn(NATIVE_ORACLE_ADDR, nonce, gas_price, input))
}

// convert_oracle_rsa_to_api_jwk is now provided by super::types
