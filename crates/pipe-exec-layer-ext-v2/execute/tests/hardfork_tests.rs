//! Tests for Gravity hardfork (DevnetV0_5) functionality
//!
//! These tests verify the hardfork-related behavior:
//! - JWK Manager address switching based on timestamp
//! - Timestamp precision handling (microseconds vs seconds)
//!
//! The DevnetV0_5 hardfork affects:
//! 1. `get_jwk_manager_addr()` - switches from JWK_MANAGER_ADDR to JWK_MANAGER_ADDR_V2
//! 2. Timestamp handling in block execution - preserves microsecond precision after hardfork

use alloy_genesis::Genesis;
use alloy_primitives::address;
use reth_chainspec::ChainSpec;
use reth_pipe_exec_layer_ext_v2::onchain_config::{
    get_jwk_manager_addr, JWK_MANAGER_ADDR, JWK_MANAGER_ADDR_V2,
};

// ============================================================================
// get_jwk_manager_addr Tests
// ============================================================================

/// Test that JWK Manager address returns V1 when hardfork is not configured
#[test]
fn test_jwk_manager_addr_no_hardfork() {
    let spec = ChainSpec::default();

    // Without hardfork configured, should always return V1 address
    assert_eq!(get_jwk_manager_addr(&spec, 0), JWK_MANAGER_ADDR);
    assert_eq!(get_jwk_manager_addr(&spec, 1000), JWK_MANAGER_ADDR);
    assert_eq!(get_jwk_manager_addr(&spec, u64::MAX), JWK_MANAGER_ADDR);
}

/// Test JWK Manager address switching at hardfork activation
#[test]
fn test_jwk_manager_addr_with_hardfork() {
    // Genesis with devnetV0_5Time set to 1000
    let genesis_json = r#"{
        "config": {
            "chainId": 1625,
            "homesteadBlock": 0,
            "eip150Block": 0,
            "eip155Block": 0,
            "eip158Block": 0,
            "byzantiumBlock": 0,
            "constantinopleBlock": 0,
            "petersburgBlock": 0,
            "istanbulBlock": 0,
            "berlinBlock": 0,
            "londonBlock": 0,
            "terminalTotalDifficulty": 0,
            "shanghaiTime": 0,
            "cancunTime": 0,
            "devnetV0_5Time": 1000
        },
        "alloc": {}
    }"#;
    let genesis: Genesis = serde_json::from_str(genesis_json).unwrap();
    let spec = ChainSpec::from(genesis);

    // Before hardfork: V1 address
    assert_eq!(get_jwk_manager_addr(&spec, 0), JWK_MANAGER_ADDR);
    assert_eq!(get_jwk_manager_addr(&spec, 999), JWK_MANAGER_ADDR);

    // At and after hardfork: V2 address
    assert_eq!(get_jwk_manager_addr(&spec, 1000), JWK_MANAGER_ADDR_V2);
    assert_eq!(get_jwk_manager_addr(&spec, 1001), JWK_MANAGER_ADDR_V2);
    assert_eq!(get_jwk_manager_addr(&spec, u64::MAX), JWK_MANAGER_ADDR_V2);
}

/// Test JWK Manager address values are correct
#[test]
fn test_jwk_manager_addr_values() {
    // V1: Fixed precompile address
    assert_eq!(JWK_MANAGER_ADDR, address!("0000000000000000000000000000000000002018"));

    // V2: Deployed contract address
    assert_eq!(JWK_MANAGER_ADDR_V2, address!("919F5f9EeA137382099707Ed8cf135f8A43f0205"));
}

// ============================================================================
// ChainSpec Hardfork Detection Tests
// ============================================================================

/// Test is_devnet_v0_5_active_at_timestamp with hardfork at genesis (timestamp 0)
#[test]
fn test_hardfork_active_at_genesis() {
    let genesis_json = r#"{
        "config": {
            "chainId": 1625,
            "homesteadBlock": 0,
            "terminalTotalDifficulty": 0,
            "shanghaiTime": 0,
            "devnetV0_5Time": 0
        },
        "alloc": {}
    }"#;
    let genesis: Genesis = serde_json::from_str(genesis_json).unwrap();
    let spec = ChainSpec::from(genesis);

    // Hardfork active from genesis
    assert!(spec.is_devnet_v0_5_active_at_timestamp(0));
    assert!(spec.is_devnet_v0_5_active_at_timestamp(1));
    assert!(spec.is_devnet_v0_5_active_at_timestamp(u64::MAX));

    // JWK Manager should use V2 from genesis
    assert_eq!(get_jwk_manager_addr(&spec, 0), JWK_MANAGER_ADDR_V2);
}

/// Test hardfork with far-future activation time
#[test]
fn test_hardfork_future_activation() {
    let farfuture_timestamp = 1893456000u64; // 2030-01-01

    let genesis_json = format!(
        r#"{{
        "config": {{
            "chainId": 1625,
            "homesteadBlock": 0,
            "terminalTotalDifficulty": 0,
            "shanghaiTime": 0,
            "devnetV0_5Time": {}
        }},
        "alloc": {{}}
    }}"#,
        farfuture_timestamp
    );
    let genesis: Genesis = serde_json::from_str(&genesis_json).unwrap();
    let spec = ChainSpec::from(genesis);

    // Before activation: hardfork not active, V1 address
    assert!(!spec.is_devnet_v0_5_active_at_timestamp(0));
    assert!(!spec.is_devnet_v0_5_active_at_timestamp(farfuture_timestamp - 1));
    assert_eq!(get_jwk_manager_addr(&spec, farfuture_timestamp - 1), JWK_MANAGER_ADDR);

    // At activation: hardfork active, V2 address
    assert!(spec.is_devnet_v0_5_active_at_timestamp(farfuture_timestamp));
    assert_eq!(get_jwk_manager_addr(&spec, farfuture_timestamp), JWK_MANAGER_ADDR_V2);
}

// ============================================================================
// Timestamp Precision Tests
// ============================================================================

/// Test the timestamp conversion logic that is affected by the hardfork
///
/// Before hardfork: timestamp_us is truncated to seconds, then converted back
///   i.e., microseconds are lost: timestamp_us => (timestamp_us / 1_000_000) * 1_000_000
///
/// After hardfork: timestamp_us is preserved exactly
#[test]
fn test_timestamp_precision_logic() {
    let hardfork_time = 1000u64;

    let genesis_json = format!(
        r#"{{
        "config": {{
            "chainId": 1625,
            "homesteadBlock": 0,
            "terminalTotalDifficulty": 0,
            "shanghaiTime": 0,
            "devnetV0_5Time": {}
        }},
        "alloc": {{}}
    }}"#,
        hardfork_time
    );
    let genesis: Genesis = serde_json::from_str(&genesis_json).unwrap();
    let spec = ChainSpec::from(genesis);

    // Simulate the timestamp conversion logic from lib.rs execute_ordered_block
    let convert_timestamp = |timestamp_us: u64| -> u64 {
        let timestamp_sec = timestamp_us / 1_000_000;
        if spec.is_devnet_v0_5_active_at_timestamp(timestamp_sec) {
            // After hardfork: preserve microseconds
            timestamp_us
        } else {
            // Before hardfork: truncate to seconds precision
            timestamp_sec * 1_000_000
        }
    };

    // Before hardfork (999 seconds = before 1000 hardfork time)
    // Input: 999_123_456 us (999.123456 seconds)
    // Output: 999_000_000 us (microseconds lost)
    let before_hardfork_us = 999_123_456u64;
    assert_eq!(convert_timestamp(before_hardfork_us), 999_000_000);

    // At hardfork (1000 seconds)
    // Input: 1000_123_456 us (1000.123456 seconds)
    // Output: 1000_123_456 us (microseconds preserved)
    let at_hardfork_us = 1000_123_456u64;
    assert_eq!(convert_timestamp(at_hardfork_us), 1000_123_456);

    // After hardfork (1001 seconds)
    let after_hardfork_us = 1001_999_999u64;
    assert_eq!(convert_timestamp(after_hardfork_us), 1001_999_999);
}

/// Test edge case: timestamp exactly at second boundary
#[test]
fn test_timestamp_at_second_boundary() {
    let hardfork_time = 1000u64;

    let genesis_json = format!(
        r#"{{
        "config": {{
            "chainId": 1625,
            "homesteadBlock": 0,
            "terminalTotalDifficulty": 0,
            "devnetV0_5Time": {}
        }},
        "alloc": {{}}
    }}"#,
        hardfork_time
    );
    let genesis: Genesis = serde_json::from_str(&genesis_json).unwrap();
    let spec = ChainSpec::from(genesis);

    let convert_timestamp = |timestamp_us: u64| -> u64 {
        let timestamp_sec = timestamp_us / 1_000_000;
        if spec.is_devnet_v0_5_active_at_timestamp(timestamp_sec) {
            timestamp_us
        } else {
            timestamp_sec * 1_000_000
        }
    };

    // Exactly 999 seconds (no fractional part)
    assert_eq!(convert_timestamp(999_000_000), 999_000_000);

    // Exactly 1000 seconds (hardfork active)
    assert_eq!(convert_timestamp(1000_000_000), 1000_000_000);
}
