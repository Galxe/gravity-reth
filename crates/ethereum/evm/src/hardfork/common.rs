//! Common types and traits for Gravity hardfork state changes.
//!
//! Each hardfork module (alpha, beta, gamma, ...) defines a set of
//! system contract bytecode upgrades. This module provides a trait
//! that standardizes how upgrade tables are exposed, enabling generic
//! verification logic in tests.

use alloy_primitives::{Address, B256, U256};

/// A single bytecode replacement: target address → new runtime bytecode.
pub type BytecodeUpgrade = (Address, &'static [u8]);

/// A single storage patch: target address, slot, value.
pub type StoragePatch = (Address, B256, U256);

/// Trait implemented by each hardfork module to describe its upgrades.
///
/// This enables generic test helpers that work across any hardfork:
/// ```ignore
/// fn verify_hardfork_applied<H: HardforkUpgrades>(provider: &P, block: u64) { ... }
/// ```
pub trait HardforkUpgrades {
    /// The human-readable name of this hardfork (e.g. "Gamma").
    fn name(&self) -> &'static str;

    /// System contract bytecode upgrades: `(address, new_bytecode)` pairs.
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade];

    /// Additional non-system contract bytecode upgrades (e.g. `StakePool` instances).
    /// Returns `(address, new_bytecode)` pairs.
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }

    /// Storage patches to apply alongside bytecode replacements
    /// (e.g. `ReentrancyGuard` initialization).
    fn storage_patches(&self) -> &'static [StoragePatch] {
        &[]
    }
}
