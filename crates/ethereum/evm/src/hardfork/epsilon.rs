//! Epsilon hardfork: D3-2 underbonded eviction + GBridgeReceiver dedup
//!
//! Replaces 4 contract bytecodes (gravity-testnet-v1.3 → v1.4):
//!
//! - `ValidatorManagement` (PR #56): D3-2 Phase 1 underbonded eviction +
//!   percentage-based Phase 2 performance threshold + skip-epoch-1 rule
//! - `Reconfiguration` (PR #63): `evictUnderperformingValidators()` call site
//!   moved from `_applyReconfiguration` into `checkAndStartTransition` and
//!   `governanceReconfigure` (fixes DKG synchronization)
//! - `ValidatorConfig` (PR #63): new `autoEvictThresholdPct (uint64)` field,
//!   declared between `autoEvictEnabled` and `__deprecated_autoEvictThreshold`
//!   so it packs into the same slot as `autoEvictEnabled` and the storage
//!   layout stays byte-identical to v1.3
//! - `GBridgeReceiver` (PR #66): `_processedNonces` mapping replaced with
//!   `__deprecated_processedNonces` storage gap (slot 0); `isProcessed()` and
//!   `AlreadyProcessed` removed. Also adds the `trustedSourceId` immutable
//!   that did not exist in the originally-deployed v1.0 receiver.
//!
//! **No storage patches** are needed — every byte of pre-Epsilon storage stays
//! at the same slot/offset under the new bytecode. This is a pure bytecode-
//! replacement hardfork.

use super::common::{BytecodeUpgrade, HardforkUpgrades};
use alloy_primitives::{address, Address};

// ── Compiled runtime bytecodes ──────────────────────────────────────────────────
static VALIDATOR_MANAGEMENT_BYTECODE: &[u8] =
    include_bytes!("bytecodes/epsilon/ValidatorManagement.bin");
static RECONFIGURATION_BYTECODE: &[u8] = include_bytes!("bytecodes/epsilon/Reconfiguration.bin");
static VALIDATOR_CONFIG_BYTECODE: &[u8] = include_bytes!("bytecodes/epsilon/ValidatorConfig.bin");
/// `GBridgeReceiver` bytecode with the testnet immutables (`trustedBridge`,
/// `trustedSourceId`) pre-patched. Regenerate via
/// `scripts/build_epsilon_bytecodes.sh` in the contracts repo with the right
/// per-environment values when deploying to a non-testnet environment.
static GBRIDGE_RECEIVER_BYTECODE: &[u8] = include_bytes!("bytecodes/epsilon/GBridgeReceiver.bin");

// ── System addresses ────────────────────────────────────────────────────────────

/// `ValidatorManagement` contract system address
pub const VALIDATOR_MANAGEMENT_ADDRESS: Address =
    address!("00000000000000000000000000000001625F2001");

/// `Reconfiguration` contract system address
pub const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000001625F2003");

/// `ValidatorConfig` contract system address
pub const VALIDATOR_CONFIG_ADDRESS: Address = address!("00000000000000000000000000000001625F1002");

/// Testnet `GBridgeReceiver` instance address.
///
/// Unlike the other three contracts in this hardfork, `GBridgeReceiver` is not
/// deployed at a fixed `0x...1625F` slot — it is created dynamically by
/// `Genesis.sol` at chain bring-up. This constant is the address it landed at
/// on the gravity testnet. For other environments, override this constant and
/// regenerate `bytecodes/epsilon/GBridgeReceiver.bin` with the matching
/// immutables via `scripts/build_epsilon_bytecodes.sh` in the contracts repo.
pub const GBRIDGE_RECEIVER_ADDRESS: Address =
    address!("595475934ed7d9faa7fca28341c2ce583904a44e");

// ── Bytecode upgrade tables ─────────────────────────────────────────────────────

/// Three fixed-system-address contract upgrades.
pub static EPSILON_SYSTEM_UPGRADES: &[BytecodeUpgrade] = &[
    (VALIDATOR_MANAGEMENT_ADDRESS, VALIDATOR_MANAGEMENT_BYTECODE),
    (RECONFIGURATION_ADDRESS, RECONFIGURATION_BYTECODE),
    (VALIDATOR_CONFIG_ADDRESS, VALIDATOR_CONFIG_BYTECODE),
];

/// Dynamic-address contract upgrades: just `GBridgeReceiver`.
/// Mirrors the way Gamma handles its `StakePool` instances via `extra_upgrades()`.
pub static EPSILON_EXTRA_UPGRADES: &[BytecodeUpgrade] =
    &[(GBRIDGE_RECEIVER_ADDRESS, GBRIDGE_RECEIVER_BYTECODE)];

// ── HardforkUpgrades impl ───────────────────────────────────────────────────────

/// Epsilon hardfork descriptor.
#[derive(Debug)]
pub struct EpsilonHardfork;

impl HardforkUpgrades for EpsilonHardfork {
    fn name(&self) -> &'static str {
        "Epsilon"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        EPSILON_SYSTEM_UPGRADES
    }
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        EPSILON_EXTRA_UPGRADES
    }
    // storage_patches() and batch_storage_patches() use the trait defaults — empty.
}
