//! Beta hardfork: stub (bytecodes removed).
//!
//! The actual Beta hardfork bytecodes have been removed from this branch.
//! This stub preserves the `HardforkUpgrades` trait implementation so that
//! the hardfork dispatch infrastructure in `parallel_execute.rs` compiles
//! without changes.

use super::common::{BytecodeUpgrade, HardforkUpgrades};

/// Beta hardfork descriptor.
#[derive(Debug)]
pub struct BetaHardfork;

impl HardforkUpgrades for BetaHardfork {
    fn name(&self) -> &'static str {
        "Beta"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
}
