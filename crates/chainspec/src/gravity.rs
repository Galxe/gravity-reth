//! Gravity-specific hardforks

use crate::EthChainSpec;
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

/// Returns `true` when `block_ts` falls on or after the activation of the
/// Gravity Alpha hardfork, which bundles:
///   - randomness precompile registration,
///   - system-tx gas-exemption (this gate),
///   - one-shot [`SYSTEM_CALLER`] balance migration to zero.
///
/// Single source of truth for the L1 (cfg-side) and L2 (construction-side)
/// gas-exempt gating; all callsites — serial executor, grevm executor, pipe
/// system-tx construction, RPC trace replay, RPC precompile registration —
/// MUST route their fork check through this helper to keep the predicate
/// uniform. Centralizing the predicate here (in `reth-chainspec`, the crate
/// `reth-rpc-eth-api` / `reth-evm-ethereum` / `reth-pipe-exec-layer-ext-v2`
/// all already depend on) lets every replay path reuse it without pulling in
/// extra crate edges; any future tweak (timestamp+block dual-gate, etc.)
/// propagates uniformly. See `system-tx-gas-exempt` design §3.4.
#[inline]
pub fn is_system_tx_gas_exempt<S: EthChainSpec>(chain_spec: &S, block_ts: u64) -> bool {
    chain_spec.gravity_hardforks().is_fork_active_at_timestamp(GravityHardfork::Alpha, block_ts)
}
