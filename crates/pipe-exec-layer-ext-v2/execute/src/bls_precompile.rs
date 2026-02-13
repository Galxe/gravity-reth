//! BLS12-381 Consensus Key Proof-of-Possession Verification Precompile
//!
//! This precompile verifies that a BLS12-381 consensus public key has a valid
//! proof-of-possession (PoP), preventing rogue-key attacks. This mirrors the
//! Aptos `bls12381::public_key_from_bytes_with_pop` logic used in `stake.move`.

use alloy_primitives::Bytes;
use reth_evm::precompiles::{DynPrecompile, PrecompileInput};
use revm::precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult};
use tracing::warn;

/// BLS12-381 public key size in bytes (G1 point, compressed)
const BLS_PUBKEY_LEN: usize = 48;

/// BLS12-381 proof-of-possession size in bytes (G2 point, compressed)
const BLS_POP_LEN: usize = 96;

/// Expected input length: pubkey (48) + pop (96) = 144 bytes
const EXPECTED_INPUT_LEN: usize = BLS_PUBKEY_LEN + BLS_POP_LEN;

/// Gas cost for PoP verification (2 bilinear pairings + hash-to-curve)
const POP_VERIFY_GAS: u64 = 45_000;

/// Domain separation tag for BLS PoP verification
/// Matches the IETF standard for BLS12-381 PoP
const POP_DST: &[u8] = b"BLS_POP_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

/// Creates a BLS12-381 proof-of-possession verification precompile.
///
/// This is a stateless precompile that verifies a consensus public key
/// has a valid PoP. Any address can call it.
///
/// # Input format (144 bytes)
///
/// | Offset | Size | Description                      |
/// |--------|------|----------------------------------|
/// | 0      | 48   | BLS12-381 public key (G1, compressed) |
/// | 48     | 96   | Proof of possession (G2, compressed)  |
///
/// # Output
///
/// ABI-encoded `bool` (32 bytes): `0x01` if valid, `0x00` if invalid.
pub fn create_bls_pop_verify_precompile() -> DynPrecompile {
    let precompile_id = PrecompileId::custom("bls_pop_verify");

    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        bls_pop_verify_handler(input)
    })
        .into()
}

/// BLS PoP verification handler
fn bls_pop_verify_handler(input: PrecompileInput<'_>) -> PrecompileResult {
    bls_pop_verify_handler_raw(input.data)
}

/// Core BLS PoP verification logic operating on raw input bytes.
///
/// Separated from the `PrecompileInput` wrapper to facilitate unit testing.
fn bls_pop_verify_handler_raw(data: &[u8]) -> PrecompileResult {
    // 1. Validate input length
    if data.len() < EXPECTED_INPUT_LEN {
        warn!(
            target: "evm::precompile::bls_pop_verify",
            input_len = data.len(),
            expected = EXPECTED_INPUT_LEN,
            "Invalid input length"
        );
        return Err(PrecompileError::Other(
            format!("Invalid input length: {}, expected {}", data.len(), EXPECTED_INPUT_LEN).into(),
        ));
    }

    // 2. Parse pubkey (bytes 0..48) and PoP (bytes 48..144)
    let pubkey_bytes = &data[..BLS_PUBKEY_LEN];
    let pop_bytes = &data[BLS_PUBKEY_LEN..EXPECTED_INPUT_LEN];

    // 3. Verify using blst
    let valid = verify_pop(pubkey_bytes, pop_bytes);

    // 4. Encode result as ABI bool (32 bytes, right-padded)
    let mut output = [0u8; 32];
    if valid {
        output[31] = 1;
    }

    Ok(PrecompileOutput {
        gas_used: POP_VERIFY_GAS,
        bytes: Bytes::copy_from_slice(&output),
        reverted: false,
    })
}

/// Verify BLS12-381 proof-of-possession using the `blst` crate.
///
/// This uses the min-pk variant (public keys in G1, signatures/PoP in G2),
/// matching the Aptos BLS12-381 scheme.
fn verify_pop(pubkey_bytes: &[u8], pop_bytes: &[u8]) -> bool {
    use blst::min_pk::{PublicKey, Signature};

    // Deserialize public key
    let pk = match PublicKey::from_bytes(pubkey_bytes) {
        Ok(pk) => pk,
        Err(e) => {
            warn!(
                target: "evm::precompile::bls_pop_verify",
                error = ?e,
                "Failed to deserialize BLS public key"
            );
            return false;
        }
    };

    // Validate public key is in the correct subgroup and not identity
    if pk.validate().is_err() {
        warn!(
            target: "evm::precompile::bls_pop_verify",
            "BLS public key failed subgroup check"
        );
        return false;
    }

    // Deserialize PoP signature
    let pop = match Signature::from_bytes(pop_bytes) {
        Ok(sig) => sig,
        Err(e) => {
            warn!(
                target: "evm::precompile::bls_pop_verify",
                error = ?e,
                "Failed to deserialize BLS proof of possession"
            );
            return false;
        }
    };

    // Verify PoP: the PoP is a signature over the public key bytes using the POP DST
    let result = pop.verify(true, pubkey_bytes, POP_DST, &[], &pk, true);
    result == blst::BLST_ERROR::BLST_SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Generate a BLS12-381 keypair and PoP for testing
    fn generate_test_keypair() -> (Vec<u8>, Vec<u8>) {
        use blst::min_pk::{PublicKey, SecretKey, Signature};

        // Use a deterministic secret key for testing
        let ikm = [42u8; 32];
        let sk = SecretKey::key_gen(&ikm, &[]).unwrap();
        let pk = sk.sk_to_pk();

        // Create PoP: sign the public key bytes with POP DST
        let pk_bytes = pk.to_bytes().to_vec();
        let pop = sk.sign(&pk_bytes, POP_DST, &[]);

        (pk_bytes, pop.to_bytes().to_vec())
    }

    #[test]
    fn test_valid_pop_verification() {
        let (pubkey, pop) = generate_test_keypair();
        assert!(verify_pop(&pubkey, &pop), "Valid PoP should verify successfully");
    }

    #[test]
    fn test_invalid_pop() {
        let (pubkey, mut pop) = generate_test_keypair();
        // Maul the PoP
        pop[0] ^= 0xff;
        assert!(!verify_pop(&pubkey, &pop), "Mauled PoP should fail verification");
    }

    #[test]
    fn test_invalid_pubkey() {
        let (mut pubkey, pop) = generate_test_keypair();
        // Maul the pubkey
        pubkey[0] ^= 0xff;
        assert!(!verify_pop(&pubkey, &pop), "Mauled pubkey should fail verification");
    }

    #[test]
    fn test_precompile_valid_input() {
        let (pubkey, pop) = generate_test_keypair();
        let mut input_data = Vec::with_capacity(EXPECTED_INPUT_LEN);
        input_data.extend_from_slice(&pubkey);
        input_data.extend_from_slice(&pop);

        let result = bls_pop_verify_handler_raw(&input_data).unwrap();
        assert_eq!(result.gas_used, POP_VERIFY_GAS);
        assert_eq!(result.bytes.len(), 32);
        assert_eq!(result.bytes[31], 1, "Valid PoP should return true");
    }

    #[test]
    fn test_precompile_invalid_input_length() {
        let input_data = vec![0u8; 10]; // Too short
        let result = bls_pop_verify_handler_raw(&input_data);
        assert!(result.is_err(), "Short input should return error");
    }

    #[test]
    fn test_precompile_invalid_pop_returns_false() {
        let (pubkey, mut pop) = generate_test_keypair();
        pop[0] ^= 0xff;

        let mut input_data = Vec::with_capacity(EXPECTED_INPUT_LEN);
        input_data.extend_from_slice(&pubkey);
        input_data.extend_from_slice(&pop);

        let result = bls_pop_verify_handler_raw(&input_data).unwrap();
        assert_eq!(result.bytes[31], 0, "Invalid PoP should return false");
    }
}
