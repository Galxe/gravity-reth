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

/// Protocol-reserved synthetic emitter for `GravityTxSkipped` receipt logs.
///
/// This is not an EVM account or precompile. It is reserved in the Gravity system-address
/// namespace so indexers can query skipped transactions through a single stable emitter without
/// risking a future system contract reusing the same address.
pub const GRAVITY_TX_SKIPPED_LOG_ADDRESS: Address =
    address!("00000000000000000000000000000001625f50ff");

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

/// Verifies the protocol invariant that every [`SYSTEM_CALLER`]-signed
/// transaction in a block occupies a contiguous head prefix (positions
/// `0..k`), with all non-system txs appearing only at positions `k..`.
///
/// The pipe layer pins protocol-injected system txs (metadata `onBlockStart`
/// plus DKG/JWK validator txns) to the front of the block at construction time
/// (`pipe-exec-layer-ext-v2/execute/src/onchain_config/metadata_txn.rs:120` /
/// `:185`). Both RPC replay paths — block-family
/// `trace_block_until_with_inspector` and single-tx
/// `replay_transactions_until` — build their EVM-cfg rebuild optimization
/// ("at most one rebuild per block, on the system→user boundary") on top of
/// this invariant. A violation would silently degrade the rebuild count in
/// release and almost certainly indicate either a pipe-layer regression or
/// (in theory) a [`SYSTEM_CALLER`] signature forgery. Each replay loop runs
/// the matching inline check inside `debug_assert!` so debug builds catch a
/// regression loudly; this function documents the predicate and is the
/// unit-tested algorithm of record.
///
/// Returns `Ok(())` when the invariant holds, or `Err(idx)` for the index of
/// the first [`SYSTEM_CALLER`]-signed transaction that appears AFTER a
/// non-[`SYSTEM_CALLER`] transaction.
#[inline]
pub fn system_txs_form_head_prefix<I>(senders: I) -> Result<(), usize>
where
    I: IntoIterator<Item = Address>,
{
    let mut saw_non_system = false;
    for (idx, addr) in senders.into_iter().enumerate() {
        if is_gravity_system_caller(addr) {
            if saw_non_system {
                return Err(idx);
            }
        } else {
            saw_non_system = true;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(seed: u8) -> Address {
        let mut bytes = [0u8; 20];
        // Non-zero leading byte distinguishes from SYSTEM_CALLER's all-zero prefix.
        bytes[0] = 0xAA;
        bytes[19] = seed;
        Address::from(bytes)
    }

    #[test]
    fn empty_block_holds() {
        assert!(system_txs_form_head_prefix(std::iter::empty()).is_ok());
    }

    #[test]
    fn user_only_block_holds() {
        let senders = vec![user(1), user(2), user(3)];
        assert!(system_txs_form_head_prefix(senders).is_ok());
    }

    #[test]
    fn system_only_block_holds() {
        // Epoch-transition shape with no user-tx tail: metadata + N validator txs.
        let senders = vec![SYSTEM_CALLER, SYSTEM_CALLER, SYSTEM_CALLER];
        assert!(system_txs_form_head_prefix(senders).is_ok());
    }

    #[test]
    fn normal_block_holds() {
        // Typical post-Alpha block: 1 metadata system tx + user-tx tail.
        let senders = vec![SYSTEM_CALLER, user(1), user(2), user(3)];
        assert!(system_txs_form_head_prefix(senders).is_ok());
    }

    #[test]
    fn epoch_switch_block_holds() {
        // Epoch-switch shape: metadata + multiple DKG/JWK validator txs + user tail.
        let senders = vec![SYSTEM_CALLER, SYSTEM_CALLER, SYSTEM_CALLER, user(1), user(2)];
        assert!(system_txs_form_head_prefix(senders).is_ok());
    }

    #[test]
    fn system_after_user_is_detected() {
        // Pipe-layer regression or signature forgery: system tx mid-block.
        let senders = vec![SYSTEM_CALLER, user(1), SYSTEM_CALLER];
        assert_eq!(system_txs_form_head_prefix(senders), Err(2));
    }

    #[test]
    fn user_first_then_system_is_detected() {
        // Degenerate shape: very first tx is a user tx, then a system tx surfaces.
        let senders = vec![user(1), SYSTEM_CALLER];
        assert_eq!(system_txs_form_head_prefix(senders), Err(1));
    }
}
