//! Alpha hardfork: stub (bytecodes removed).
//!
//! The actual Alpha hardfork bytecodes have been removed from this branch.
//! This stub preserves the `HardforkUpgrades` trait implementation so that
//! the hardfork dispatch infrastructure in `parallel_execute.rs` compiles
//! without changes.

use super::common::{BytecodeUpgrade, HardforkUpgrades};

/// Alpha hardfork descriptor.
#[derive(Debug)]
pub struct AlphaHardfork;

impl HardforkUpgrades for AlphaHardfork {
    fn name(&self) -> &'static str {
        "Alpha"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
}
