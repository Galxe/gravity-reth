//! Delta hardfork: stub (implementation removed).
//!
//! The actual Delta hardfork storage patches (Governance owner, `GovernanceConfig`
//! E2E overrides) have been removed from this branch. This stub preserves the
//! `HardforkUpgrades` trait implementation so that the hardfork dispatch
//! infrastructure in `parallel_execute.rs` compiles without changes.

use super::common::{BytecodeUpgrade, HardforkUpgrades};

/// Delta hardfork descriptor.
#[derive(Debug)]
pub struct DeltaHardfork;

impl HardforkUpgrades for DeltaHardfork {
    fn name(&self) -> &'static str {
        "Delta"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        &[]
    }
}
