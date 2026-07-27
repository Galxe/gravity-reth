//! Ethereum protocol-related constants

/// Gas units, for example [`GIGAGAS`].
pub mod gas_units;
pub use gas_units::{GIGAGAS, KILOGAS, MEGAGAS};

/// The client version: `reth/v{major}.{minor}.{patch}`
pub const RETH_CLIENT_VERSION: &str = concat!("reth/v", env!("CARGO_PKG_VERSION"));

/// Minimum gas limit allowed for transactions.
pub const MINIMUM_GAS_LIMIT: u64 = 5000;

/// Maximum gas limit allowed for block.
/// In hex this number is `0x7fffffffffffffff`
pub const MAXIMUM_GAS_LIMIT_BLOCK: u64 = 2u64.pow(63) - 1;

/// The bound divisor of the gas limit, used in update calculations.
pub const GAS_LIMIT_BOUND_DIVISOR: u64 = 1024;

/// Maximum transaction gas limit as defined by [EIP-7825](https://eips.ethereum.org/EIPS/eip-7825) activated in `Osaka` hardfork.
pub const MAX_TX_GAS_LIMIT_OSAKA: u64 = 2u64.pow(24);

/// Gravity's per-transaction gas limit cap, enforced once the `Osaka` hardfork is active.
///
/// Gravity replaces the EIP-7825 cap ([`MAX_TX_GAS_LIMIT_OSAKA`], `2^24`) with Monad's flat
/// per-tx cap (`TFM_MAX_GAS_LIMIT` = 30M). The 30M system transactions built by
/// `new_system_call_txn` are constructed at exactly this value and clear the cap because the
/// check is a strict `gas_limit > cap`; the `2^24` cap would reject them.
///
/// Enforced in lockstep at three sites, all gated on `Osaka`: the executor cfg
/// (`reth-evm-ethereum`, `tx_gas_limit_cap`), the consensus block check
/// (`reth-consensus-common`), and the pipe `tx_filter` guard. Keep this equal to the
/// system-transaction `gas_limit`: lowering it below 30M would reject system transactions.
pub const GRAVITY_TX_GAS_LIMIT_CAP: u64 = 30_000_000;

/// The number of blocks to unwind during a reorg that already became a part of canonical chain.
///
/// In reality, the node can end up in this particular situation very rarely. It would happen only
/// if the node process is abruptly terminated during ongoing reorg and doesn't boot back up for
/// long period of time.
///
/// Unwind depth of `3` blocks significantly reduces the chance that the reorged block is kept in
/// the database.
pub const BEACON_CONSENSUS_REORG_UNWIND_DEPTH: u64 = 3;
