//! Gravity-specific hardforks

use alloy_primitives::{address, Address};
use reth_ethereum_forks::hardfork;

hardfork!(
    /// Gravity hardforks.
    GravityHardfork {
        /// Alpha hardfork: upgrade Staking/StakePool contracts and disable PoW rewards
        Alpha,
        /// Beta hardfork: upgrade StakePool contracts with correct FACTORY immutable
        Beta,
        /// Gamma hardfork: audit fixes, precompile changes, 12 contract bytecode upgrades
        Gamma,
        /// Delta hardfork: activate Governance contract by setting Ownable._owner
        Delta,
    }
);

/// Canonical sender address of every Gravity protocol-injected system transaction
/// (metadata `onBlockStart` + DKG/JWK validator txns).
///
/// Single source of truth for the literal `0x00000000000000000000000000000001625f0000`.
/// Downstream consumers (pipe-exec-layer, eth RPC, EVM gas-exempt gating) MUST reuse
/// this constant rather than redeclaring the literal — see `system-tx-gas-exempt`
/// design §2.5 ("地址字面量收口").
pub const SYSTEM_CALLER: Address = address!("00000000000000000000000000000001625f0000");

/// Returns `true` iff `addr` is the canonical Gravity [`SYSTEM_CALLER`].
///
/// Prefer this helper over direct comparison so future address-shape changes have a
/// single migration point.
#[inline]
pub fn is_gravity_system_caller(addr: Address) -> bool {
    addr == SYSTEM_CALLER
}
