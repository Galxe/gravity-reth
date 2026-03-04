//! Gravity-specific hardfork state changes.
//!
//! Contains constants and logic for irregular state changes during Gravity hardforks,
//! similar to how Ethereum handles the DAO fork.

use alloy_primitives::{address, Address};

/// Staking contract address (SystemAddresses.STAKING)
pub const STAKING_ADDRESS: Address = address!("00000000000000000000000000000001625F2000");

/// New Staking contract runtime bytecode for TestNetV1_1 hardfork.
///
/// TODO: Replace with actual compiled bytecode from `forge build`.
/// This is mock bytecode for development purposes.
pub const STAKING_V1_1_RUNTIME_BYTECODE: &[u8] = &[
    0x60, 0x80, 0x60, 0x40, 0x52, 0x34, 0x80, 0x15, 0x60, 0x0f, 0x57, 0x60, 0x00, 0x80, 0xfd, 0x5b,
    0x50, 0x60, 0x01, 0x60, 0x00, 0x81, 0x90, 0x55, 0x50, 0xfe,
];
