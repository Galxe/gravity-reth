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
    types::{convert_oracle_rsa_to_api_jwk, GaptosRsaJwk, OracleRSA_JWK, SOURCE_TYPE_JWK},
    NATIVE_ORACLE_ADDR,
};
use alloy_primitives::{keccak256, Bytes, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::jwks::{JWKStruct, ProviderJWKs};
use reth_ethereum_primitives::TransactionSigned;
use tracing::{debug, info};

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
        bytes calldata payload,
        uint256 callbackGasLimit
    ) external;

    /// NativeOracle.recordBatch() function signature for multiple events
    function recordBatch(
        uint32 sourceType,
        uint256 sourceId,
        uint128[] calldata nonces,
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
/// Format: gravity://{chain_id}/events?...
fn parse_chain_id_from_issuer(issuer: &[u8]) -> Option<u64> {
    let issuer_str = String::from_utf8_lossy(issuer);
    if issuer_str.starts_with("gravity://") {
        let after_protocol = &issuer_str[10..];
        if let Some(slash_pos) = after_protocol.find('/') {
            let chain_id_str = &after_protocol[..slash_pos];
            return chain_id_str.parse().ok();
        }
    }
    None
}

/// Extract nonce from ABI-encoded event payload
/// Payload format: abi.encode(address, bytes32[], bytes, uint64 block_number, uint64 log_index)
/// Returns block_number * 1000 + log_index as unique nonce
fn extract_nonce_from_payload(payload: &[u8]) -> Option<u128> {
    // ABI-encoded payload structure:
    // - address (32 bytes padded)
    // - offset to topics array (32 bytes)
    // - offset to data bytes (32 bytes)
    // - block_number (32 bytes as uint64)
    // - log_index (32 bytes as uint64)
    // Then dynamic data...

    if payload.len() < 160 {
        // Minimum: 5 * 32 bytes for fixed parts
        return None;
    }

    // block_number is at offset 96 (3 * 32)
    let block_number_bytes = &payload[96..128];
    let block_number = U256::from_be_slice(&block_number_bytes[..32]);

    // log_index is at offset 128 (4 * 32)
    let log_index_bytes = &payload[128..160];
    let log_index = U256::from_be_slice(&log_index_bytes[..32]);

    let nonce = block_number.saturating_to::<u128>() * 1000 + log_index.saturating_to::<u128>();
    Some(nonce)
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

    // All JWKs are guaranteed to be unsupported type when entering this function
    if jwks.is_empty() {
        return Err("No blockchain event JWKs found".to_string());
    }

    // Parse sourceType from first JWK's type_name (all have same type)
    let source_type: u32 = jwks[0]
        .type_name
        .parse()
        .map_err(|_| format!("Invalid sourceType: {}", jwks[0].type_name))?;

    // Build batch arrays
    let mut nonces: Vec<u128> = Vec::with_capacity(jwks.len());
    let mut payloads: Vec<Bytes> = Vec::with_capacity(jwks.len());
    let mut gas_limits: Vec<U256> = Vec::with_capacity(jwks.len());

    for (idx, jwk) in jwks.iter().enumerate() {
        // Extract nonce from the ABI-encoded payload
        let event_nonce = extract_nonce_from_payload(&jwk.data)
            .ok_or_else(|| format!("Failed to extract nonce from payload at index {}", idx))?;

        nonces.push(event_nonce);
        // Pass payload through UNCHANGED - this is critical for comparison matching
        // The relayer already ABI-encoded the data, we just store it as-is
        payloads.push(jwk.data.clone().into());
        gas_limits.push(U256::from(CALLBACK_GAS_LIMIT));

        debug!(
            idx = idx,
            event_nonce = event_nonce,
            payload_len = jwk.data.len(),
            "Added event to batch (pass-through)"
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
        payloads,
        callbackGasLimits: gas_limits,
    };

    let input: Bytes = call.abi_encode().into();
    Ok(new_system_call_txn(NATIVE_ORACLE_ADDR, nonce, gas_price, input))
}

// convert_oracle_rsa_to_api_jwk is now provided by super::types
