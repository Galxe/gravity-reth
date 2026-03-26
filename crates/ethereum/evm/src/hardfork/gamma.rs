//! Gamma hardfork: contract bytecode upgrades
//!
//! Upgrades 11 system contracts at fixed addresses + all `StakePool` instances.
//! Also initializes `ReentrancyGuard` storage for `StakePool` contracts.
//!
//! Bytecodes are stored as external `.bin` files under `bytecodes/gamma/`
//! and loaded at compile time via `include_bytes!()`.

use alloy_primitives::{address, Address, B256, U256};
use super::common::{BytecodeUpgrade, HardforkUpgrades, StoragePatch};

/// Gamma hardfork descriptor.
pub struct GammaHardfork;

impl HardforkUpgrades for GammaHardfork {
    fn name(&self) -> &'static str { "Gamma" }

    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        GAMMA_SYSTEM_UPGRADES
    }

    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        GAMMA_EXTRA_UPGRADES
    }

    fn storage_patches(&self) -> &'static [StoragePatch] {
        GAMMA_STORAGE_PATCHES
    }
}

// ─── System contract bytecodes (loaded from external .bin files) ─────────────

/// `StakingConfig` runtime bytecode (1608 bytes)
pub const STAKING_CONFIG_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/staking_config.bin");
/// `StakingConfig` system address
pub const STAKING_CONFIG_ADDRESS: Address = address!("00000000000000000000000000000001625F1001");

/// `ValidatorConfig` runtime bytecode (2266 bytes)
pub const VALIDATOR_CONFIG_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/validator_config.bin");
/// `ValidatorConfig` system address
pub const VALIDATOR_CONFIG_ADDRESS: Address = address!("00000000000000000000000000000001625F1002");

/// `GovernanceConfig` runtime bytecode (1575 bytes)
pub const GOVERNANCE_CONFIG_BYTECODE: &[u8] =
    include_bytes!("bytecodes/gamma/governance_config.bin");
/// `GovernanceConfig` system address
pub const GOVERNANCE_CONFIG_ADDRESS: Address = address!("00000000000000000000000000000001625f1004");

/// `Staking` runtime bytecode (11054 bytes)
pub const STAKING_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/staking.bin");
/// `Staking` system address
pub const STAKING_ADDRESS: Address = address!("00000000000000000000000000000001625F2000");

/// `ValidatorManagement` runtime bytecode (16427 bytes)
pub const VALIDATOR_MANAGEMENT_BYTECODE: &[u8] =
    include_bytes!("bytecodes/gamma/validator_management.bin");
/// `ValidatorManagement` system address
pub const VALIDATOR_MANAGEMENT_ADDRESS: Address =
    address!("00000000000000000000000000000001625F2001");

/// `Reconfiguration` runtime bytecode (5721 bytes)
pub const RECONFIGURATION_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/reconfiguration.bin");
/// `Reconfiguration` system address
pub const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000001625F2003");

/// `Blocker` runtime bytecode (1858 bytes)
pub const BLOCKER_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/blocker.bin");
/// `Blocker` system address
pub const BLOCKER_ADDRESS: Address = address!("00000000000000000000000000000001625F2004");

/// `PerformanceTracker` runtime bytecode (1742 bytes)
pub const PERFORMANCE_TRACKER_BYTECODE: &[u8] =
    include_bytes!("bytecodes/gamma/performance_tracker.bin");
/// `PerformanceTracker` system address
pub const PERFORMANCE_TRACKER_ADDRESS: Address =
    address!("00000000000000000000000000000001625F2005");

/// `Governance` runtime bytecode (8552 bytes)
pub const GOVERNANCE_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/governance.bin");
/// `Governance` system address
pub const GOVERNANCE_ADDRESS: Address = address!("00000000000000000000000000000001625F3000");

/// `NativeOracle` runtime bytecode (4578 bytes)
pub const NATIVE_ORACLE_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/native_oracle.bin");
/// `NativeOracle` system address
pub const NATIVE_ORACLE_ADDRESS: Address = address!("00000000000000000000000000000001625f4000");

/// `OracleRequestQueue` runtime bytecode (3522 bytes)
pub const ORACLE_REQUEST_QUEUE_BYTECODE: &[u8] =
    include_bytes!("bytecodes/gamma/oracle_request_queue.bin");
/// `OracleRequestQueue` system address
pub const ORACLE_REQUEST_QUEUE_ADDRESS: Address =
    address!("00000000000000000000000000000001625F4002");

// ─── StakePool ───────────────────────────────────────────────────────────────

/// `StakePool` runtime bytecode (6909 bytes)
pub const STAKEPOOL_BYTECODE: &[u8] = include_bytes!("bytecodes/gamma/stakepool.bin");

/// `StakePool` contract addresses to upgrade during Gamma hardfork.
/// These must be updated with all pool addresses from the live chain before deployment.
pub const STAKEPOOL_ADDRESSES: &[Address] = &[
    // StakePool created by Genesis.initialize for the initial validator
    address!("33f4ee289578b2ff35ac3ffa46ea2e97557da32c"),
];

// ─── Upgrade table & ReentrancyGuard ─────────────────────────────────────────

/// All system contract upgrades for Gamma hardfork: (address, `new_bytecode`).
pub const GAMMA_SYSTEM_UPGRADES: &[(Address, &[u8])] = &[
    (STAKING_CONFIG_ADDRESS, STAKING_CONFIG_BYTECODE),
    (VALIDATOR_CONFIG_ADDRESS, VALIDATOR_CONFIG_BYTECODE),
    (GOVERNANCE_CONFIG_ADDRESS, GOVERNANCE_CONFIG_BYTECODE),
    (STAKING_ADDRESS, STAKING_BYTECODE),
    (VALIDATOR_MANAGEMENT_ADDRESS, VALIDATOR_MANAGEMENT_BYTECODE),
    (RECONFIGURATION_ADDRESS, RECONFIGURATION_BYTECODE),
    (BLOCKER_ADDRESS, BLOCKER_BYTECODE),
    (PERFORMANCE_TRACKER_ADDRESS, PERFORMANCE_TRACKER_BYTECODE),
    (GOVERNANCE_ADDRESS, GOVERNANCE_BYTECODE),
    (NATIVE_ORACLE_ADDRESS, NATIVE_ORACLE_BYTECODE),
    (ORACLE_REQUEST_QUEUE_ADDRESS, ORACLE_REQUEST_QUEUE_BYTECODE),
];

/// ERC-7201 namespaced storage slot for `ReentrancyGuard` (from `OpenZeppelin` v5)
/// keccak256(abi.encode(uint256(keccak256("openzeppelin.storage.ReentrancyGuard")) - 1)) &
/// ~bytes32(uint256(0xff))
pub const REENTRANCY_GUARD_SLOT: [u8; 32] = [
    0x9b, 0x77, 0x9b, 0x17, 0x42, 0x2d, 0x0d, 0xf9, 0x22, 0x23, 0x01, 0x8b, 0x32, 0xb4, 0xd1, 0xfa,
    0x46, 0xe0, 0x71, 0x72, 0x3d, 0x68, 0x17, 0xe2, 0x48, 0x6d, 0x00, 0x3b, 0xec, 0xc5, 0x5f, 0x00,
];

/// `NOT_ENTERED` value for `ReentrancyGuard` (must be written after bytecode replacement)
pub const REENTRANCY_GUARD_NOT_ENTERED: u8 = 1;

// ─── Trait-driven upgrade tables ─────────────────────────────────────────────

/// StakePool extra upgrades for Gamma (dynamic addresses → same bytecode).
static GAMMA_EXTRA_UPGRADES: &[BytecodeUpgrade] = &[
    (STAKEPOOL_ADDRESSES_ARRAY[0], STAKEPOOL_BYTECODE),
];

/// Addresses as a fixed array for static table indexing.
const STAKEPOOL_ADDRESSES_ARRAY: [Address; 1] = [
    address!("33f4ee289578b2ff35ac3ffa46ea2e97557da32c"),
];

/// ReentrancyGuard storage patches for all StakePool contracts.
static GAMMA_STORAGE_PATCHES: &[StoragePatch] = &[
    (
        STAKEPOOL_ADDRESSES_ARRAY[0],
        B256::new(REENTRANCY_GUARD_SLOT),
        U256::from_limbs([REENTRANCY_GUARD_NOT_ENTERED as u64, 0, 0, 0]),
    ),
];
