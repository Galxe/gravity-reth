#![allow(missing_docs)]

//! REPRO: `construct_oracle_record_transaction` PANICS (chain halt) when the
//! first JWK in a consensus-carried `ProviderJWKs` is RSA-typed.
//!
//! Finding id: jwk-rsa-path-unconditional-panic (severity: high)
//!
//! ## The bug
//!
//! `construct_oracle_record_transaction`
//! (crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/jwk_oracle.rs:155)
//! routes on the `type_name` string of the FIRST JWK of the provider's `jwks`
//! vector. `is_rsa_jwk` (jwk_oracle.rs:56-58) matches
//! `type_name == "0x1::jwks::RSA_JWK"`. When that branch is taken, the function
//! executes an UNCONDITIONAL `panic!()` (jwk_oracle.rs:179-183) instead of
//! returning `Err`.
//!
//! ## Why this is a full-chain halt
//!
//! This function is invoked from
//! `construct_validator_txn_from_extra_data` (onchain_config/mod.rs:160), which
//! `execute_ordered_block` (lib.rs:773-789) calls for every
//! `ExtraDataType::JWK` entry in the consensus-ordered `OrderedBlock.extra_data`.
//! lib.rs:778-788 is written to handle a returned `Err` gracefully (it logs and
//! `continue`s, consuming no nonce) -- but the `panic!()` fires INSIDE the call,
//! BEFORE any `Err` can be returned, so the graceful path is never reached.
//!
//! Because `extra_data` is consensus-ordered and byte-identical on every
//! validator, an RSA-typed JWK entry panics EVERY validator simultaneously =
//! full chain halt. The node's panic hook (see e.g. the sibling 7702 repro)
//! turns the panic into `process::exit(1)`.
//!
//! `type_name` is a free-form `String` field on `JWKStruct`
//! (gravity-aptos crates/api-types/src/on_chain_config/jwks.rs:12) carried
//! verbatim through consensus; nothing rejects an RSA `type_name` before it
//! reaches this routing code. The consensus JWK observer's OIDC path produces
//! `JWK::RSA` for any standard RSA OIDC key, whose Move struct `type_name` is
//! exactly `"0x1::jwks::RSA_JWK"`, so the RSA branch is genuinely reachable
//! under a normal (OIDC-JWK-enabled) feature configuration.
//!
//! ## What this test does
//!
//! It calls the public `construct_oracle_record_transaction` directly with a
//! `ProviderJWKs` whose first (and only) JWK is RSA-typed -- i.e. exactly the
//! value the validator-txn construction path receives for a consensus-ordered
//! `ObservedJWKUpdate` carrying an RSA JWK.
//!
//! EXPECTED (correct / post-fix) behaviour, which this test asserts:
//!   - the function returns `Err(..)` (the caller at lib.rs:778-788 already
//!     logs + `continue`s on `Err`, so a returned `Err` is a deterministic
//!     skipped entry, NOT a halt).
//!
//! ACTUAL behaviour on buggy HEAD f39cdf39a:
//!   - the function executes `panic!("RSA JWK oracle record path is
//!     unreachable: ...")` (jwk_oracle.rs:179-183), which unwinds into this
//!     test and FAILS it. In the live node this same panic crosses the
//!     panic hook -> `process::exit(1)` -> all-validator halt.

use gravity_api_types::on_chain_config::jwks::{JWKStruct, ProviderJWKs};
use reth_pipe_exec_layer_ext_v2::onchain_config::jwk_oracle::construct_oracle_record_transaction;

/// Exact match string of `is_rsa_jwk` (jwk_oracle.rs:57) and of
/// `RSA_JWK::MOVE_TYPE_NAME` in gravity-aptos.
const RSA_JWK_TYPE_NAME: &str = "0x1::jwks::RSA_JWK";

/// Build a `ProviderJWKs` whose first JWK is RSA-typed, mirroring what the
/// consensus observer emits for a standard RSA OIDC key. The `data` payload is
/// intentionally not a valid BCS RSA JWK -- it is irrelevant because the bug
/// fires on the `type_name` routing decision, BEFORE any payload parsing.
fn rsa_first_provider_jwks() -> ProviderJWKs {
    ProviderJWKs {
        issuer: b"https://accounts.google.com".to_vec(),
        version: 1,
        jwks: vec![JWKStruct {
            type_name: RSA_JWK_TYPE_NAME.to_string(),
            // Arbitrary bytes; never parsed before the panic.
            data: vec![0u8; 8],
        }],
    }
}

/// CORRECT behaviour: an RSA-typed first JWK must yield `Err`, never panic.
///
/// This FAILS on HEAD because `construct_oracle_record_transaction` calls
/// `panic!()` on the RSA branch instead of returning `Err`.
#[test]
fn repro_jwk_rsa_path_unconditional_panic() {
    let provider_jwks = rsa_first_provider_jwks();

    let result = construct_oracle_record_transaction(
        provider_jwks,
        /* nonce = */ 0,
        /* gas_price = */ 1_000_000_000,
    );

    // On HEAD the line above panics and we never get here. After the suggested
    // fix (return Err instead of panic!), the RSA branch yields Err and the
    // caller at lib.rs:778-788 logs + continues -> deterministic skip, no halt.
    assert!(
        result.is_err(),
        "RSA-typed first JWK must return Err (so execute_ordered_block can log + \
         continue), but got Ok -- and on buggy HEAD it instead panics, halting \
         every validator that executes this consensus-ordered extra_data entry"
    );
}
