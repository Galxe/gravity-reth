//! Gamma hardfork: stub (bytecodes removed).
//!
//! The actual Gamma hardfork bytecodes and `.bin` files have been removed
//! from this branch. This stub preserves the `HardforkUpgrades` trait
//! implementation and the public constants referenced by tests, so that
//! the hardfork dispatch infrastructure compiles without changes.

use super::common::{BytecodeUpgrade, HardforkUpgrades, StoragePatch};
use alloy_primitives::{address, Address, B256, U256};

/// Gamma hardfork descriptor.
pub struct GammaHardfork;

impl HardforkUpgrades for GammaHardfork {
    fn name(&self) -> &'static str {
        "Gamma"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
    fn storage_patches(&self) -> &'static [StoragePatch] {
        &[]
    }
}

// ── Public constants preserved for test compatibility ────────────────────────

/// All system contract upgrades for Gamma hardfork (stubbed — empty).
pub const GAMMA_SYSTEM_UPGRADES: &[(Address, &[u8])] = &[];

/// ERC-7201 namespaced storage slot for `ReentrancyGuard` (from `OpenZeppelin` v5)
pub const REENTRANCY_GUARD_SLOT: [u8; 32] = [
    0x9b, 0x77, 0x9b, 0x17, 0x42, 0x2d, 0x0d, 0xf9, 0x22, 0x23, 0x01, 0x8b, 0x32, 0xb4, 0xd1, 0xfa,
    0x46, 0xe0, 0x71, 0x72, 0x3d, 0x68, 0x17, 0xe2, 0x48, 0x6d, 0x00, 0x3b, 0xec, 0xc5, 0x5f, 0x00,
];

/// `NOT_ENTERED` value for `ReentrancyGuard`
pub const REENTRANCY_GUARD_NOT_ENTERED: u8 = 1;

/// `StakePool` contract addresses (kept for test reference).
pub const STAKEPOOL_ADDRESSES: &[Address] = &[address!("33f4ee289578b2ff35ac3ffa46ea2e97557da32c")];

/// `StakePool` bytecode stub (empty — actual bytecode removed).
pub const STAKEPOOL_BYTECODE: &[u8] = &[];
