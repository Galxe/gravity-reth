//! Gamma hardfork: stub (implementation removed).
//!
//! The actual Gamma hardfork bytecodes and storage patches have been removed
//! from this branch. This stub preserves the `HardforkUpgrades` trait
//! implementation so that the hardfork dispatch infrastructure in
//! `parallel_execute.rs` compiles without changes.

use super::common::{BytecodeUpgrade, HardforkUpgrades};

/// Gamma hardfork descriptor.
#[derive(Debug)]
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
}
