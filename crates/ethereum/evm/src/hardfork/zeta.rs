//! Zeta hardfork: upgrade `Governance`, `StakingConfig`, `ValidatorManagement`,
//! `Reconfiguration`, `JWKManager` and every existing `StakePool`.
//!
//! This hardfork ships the v1.4 → v1.5 contract changes and the matching
//! storage-side bootstrapping that the new Solidity code requires.
//!
//! **Bytecode Upgrades** (from `gravity_chain_core_contracts` main, i.e.
//! `gravity-testnet-v1.5` contracts branch):
//! - `Governance` (PR #83): new `initialize(address)` gated entrypoint plus `_initialized` slot;
//!   companion storage patch below seeds both slots.
//! - `StakingConfig` (PR #85): three single-field governance setters (`setMinimumStakeForNextEpoch`
//!   / `setLockupDurationForNextEpoch` / `setUnbondingDelayForNextEpoch`) overlaying pending
//!   config.
//! - `ValidatorManagement` (PR #85): per-pool whitelist + permissionless-join flag gating
//!   `registerValidator` / `joinValidatorSet`. Active pools are seeded into `_allowedPools` via the
//!   batch storage patch below.
//! - `Reconfiguration` (PR #82): apply `ValidatorConfig.applyPendingConfig()` before
//!   `_startDkgSession()` so the DKG snapshot matches the post-apply validator set. ABI unchanged.
//! - `JWKManager` (PR #79): stricter non-empty JWK field validation on `setPatches`. ABI unchanged.
//! - `StakePool` (PR #73): 2-step timelock for staker/operator/voter role changes. The three
//!   `*ChangeDelay` slots read 0 on upgraded pools (no constructor rerun); `_effectiveDelay(0) ==
//!   MIN_ROLE_CHANGE_DELAY` handles that lazily, so no storage patch is needed on `StakePool`.
//!
//! **Storage Patches**:
//! - `Governance._owner` (slot 0) ← configured admin address.
//! - `Governance._initialized` (slot 8) ← 1. Once set, `initialize(address)` permanently reverts,
//!   so `addExecutor` / `removeExecutor` / ownership transfer become reachable for the patched
//!   owner.
//!
//! **Batch Storage Patches**:
//! - Per-pool `ValidatorManagement._allowedPools[pool] = true` for every existing active pool.
//!   `_allowedPools` is a `mapping(address => bool)` at storage slot 7, so the real slot for each
//!   pool is `keccak256(pool_address_padded || slot7_padded)`. This is not a "same slot on many
//!   addresses" shape so the built-in `batch_storage_patches` helper (which shares one slot across
//!   multiple accounts) does not fit. The per-pool slot hashes are precomputed below and emitted as
//!   ordinary `storage_patches`.
//!
//! The `_permissionlessJoinEnabled` flag (slot 8, one byte) stays at 0
//! intentionally — the network launches permissioned, and governance flips
//! it later when it's time to open up.

use super::common::{BytecodeUpgrade, HardforkUpgrades, StoragePatch};
use alloy_primitives::{address, Address, B256, U256};

// ── Compiled runtime bytecodes ──────────────────────────────────────────────────

static GOVERNANCE_BYTECODE: &[u8] = include_bytes!("bytecodes/zeta/Governance.bin");
static STAKING_CONFIG_BYTECODE: &[u8] = include_bytes!("bytecodes/zeta/StakingConfig.bin");
static VALIDATOR_MANAGEMENT_BYTECODE: &[u8] =
    include_bytes!("bytecodes/zeta/ValidatorManagement.bin");
static RECONFIGURATION_BYTECODE: &[u8] = include_bytes!("bytecodes/zeta/Reconfiguration.bin");
static JWK_MANAGER_BYTECODE: &[u8] = include_bytes!("bytecodes/zeta/JWKManager.bin");
static STAKEPOOL_BYTECODE: &[u8] = include_bytes!("bytecodes/zeta/StakePool.bin");
// PR #73 changes the StakePool runtime: it removes setStaker/setOperator/setVoter
// and adds the propose/accept/cancel + per-role-delay surface plus the
// MIN_ROLE_CHANGE_DELAY constant. Function selectors and code section both
// change, so the Gamma template would no-op the upgrade and leave the new ABI
// missing on chain (verify_hardfork/zeta.sh would fail at proposeStaker(),
// acceptStaker(), MIN_ROLE_CHANGE_DELAY, *ChangeDelay()).
//
// Regenerated from `out/StakePool.sol/StakePool.json` (deployedBytecode.object,
// 9223 bytes) with the FACTORY immutable patched to the Staking system address
// (0x…01625F2000) at the two immutableReferences offsets. All four pools share
// the same FACTORY (singleton Staking), so a single template applies to every
// entry in STAKEPOOL_ADDRESSES, matching the Gamma deployment pattern.

// ── System addresses ────────────────────────────────────────────────────────────

/// `Governance` contract system address (0x…1625F3000)
pub const GOVERNANCE_ADDRESS: Address = address!("00000000000000000000000000000001625F3000");

/// `StakingConfig` contract system address (0x…1625F1001)
pub const STAKING_CONFIG_ADDRESS: Address = address!("00000000000000000000000000000001625F1001");

/// `ValidatorManagement` contract system address (0x…1625F2001)
pub const VALIDATOR_MANAGEMENT_ADDRESS: Address =
    address!("00000000000000000000000000000001625F2001");

/// `Reconfiguration` contract system address (0x…1625F2003)
pub const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000001625F2003");

/// `JWKManager` contract system address (0x…1625F4001)
pub const JWK_MANAGER_ADDRESS: Address = address!("00000000000000000000000000000001625F4001");

/// Testnet `StakePool` instance addresses (match Gamma / Delta / Epsilon).
/// Queried from `Staking.getAllPools()` on the gravity testnet.
pub const STAKEPOOL_ADDRESSES: &[Address] = &[
    address!("ce128222bd84d67672f863424a03d114cd1253c5"),
    address!("78f595fb25d03a742338fb32acfd544bdc63d814"),
    address!("891299fe364088ead65aba911ea17dd5d968cd81"),
    address!("b99aa922eb5cae399b79adc87621e72f66d5a976"),
];

// ── Bytecode upgrade tables ─────────────────────────────────────────────────────

/// Five fixed-system-address contract upgrades.
pub static ZETA_SYSTEM_UPGRADES: &[BytecodeUpgrade] = &[
    (GOVERNANCE_ADDRESS, GOVERNANCE_BYTECODE),
    (STAKING_CONFIG_ADDRESS, STAKING_CONFIG_BYTECODE),
    (VALIDATOR_MANAGEMENT_ADDRESS, VALIDATOR_MANAGEMENT_BYTECODE),
    (RECONFIGURATION_ADDRESS, RECONFIGURATION_BYTECODE),
    (JWK_MANAGER_ADDRESS, JWK_MANAGER_BYTECODE),
];

/// Dynamic-address upgrades: `StakePool` bytecode for each existing pool.
pub static ZETA_EXTRA_UPGRADES: &[BytecodeUpgrade] = &[
    (STAKEPOOL_ADDRESSES[0], STAKEPOOL_BYTECODE),
    (STAKEPOOL_ADDRESSES[1], STAKEPOOL_BYTECODE),
    (STAKEPOOL_ADDRESSES[2], STAKEPOOL_BYTECODE),
    (STAKEPOOL_ADDRESSES[3], STAKEPOOL_BYTECODE),
];

// ── Governance storage patches ──────────────────────────────────────────────────
//
// Governance storage layout (from `forge inspect Governance storageLayout`):
//   slot 0: `_owner`          (address)
//   slot 1: `_pendingOwner`   (address, bytes 0..19) | `nextProposalId` (uint64, bytes 20..27)
//   slot 2: `_proposals`      (mapping base)
//   slot 3: `usedVotingPower` (mapping base)
//   slot 4: `executed`        (mapping base)
//   slot 5: `lastVoteTime`    (mapping base)
//   slot 6: `_executors.values`  (EnumerableSet length)
//   slot 7: `_executors._indexes` (EnumerableSet mapping base)
//   slot 8: `_initialized`    (bool)
//
// Note: `nextProposalId` is already initialised to 1 by the Governance
// constructor for deployer-path instances. For system-predeployed instances
// the constructor never runs; Delta already wrote `nextProposalId = 1` at
// slot 1 bytes 20..27, so Zeta only needs to write `_owner` and `_initialized`.

/// Slot 0: `Governance._owner` (standard Ownable address storage).
pub const GOVERNANCE_OWNER_SLOT: [u8; 32] = [0u8; 32];

/// Slot 8: `Governance._initialized`.
pub const GOVERNANCE_INITIALIZED_SLOT: [u8; 32] = {
    let mut s = [0u8; 32];
    s[31] = 8;
    s
};

/// Owner address to stamp into `Governance._owner`.
///
/// Defaults to the testnet faucet (hardhat #0). Override via env / config
/// before deploying a hardfork genesis to a non-testnet network.
pub const GOVERNANCE_OWNER: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

/// Precomputed `GOVERNANCE_OWNER` as a 32-byte big-endian `U256`.
pub const GOVERNANCE_OWNER_U256: U256 = {
    let bytes = GOVERNANCE_OWNER.0 .0; // [u8; 20]
    let mut word = [0u8; 32];
    let mut i = 0;
    while i < 20 {
        word[12 + i] = bytes[i];
        i += 1;
    }
    U256::from_be_bytes(word)
};

/// Value 1, used for `_initialized = true`.
pub const ONE_U256: U256 = U256::from_limbs([1, 0, 0, 0]);

// ── ValidatorManagement whitelist seeding ───────────────────────────────────────
//
// ValidatorManagement storage layout (from `forge inspect ... storageLayout`):
//   slot 0: `_validators`        (mapping base)
//   slot 1: `_activeValidators`  (address[])
//   slot 2: `_pendingActive`     (address[])
//   slot 3: `_pendingInactive`   (address[])
//   slot 4: `totalVotingPower`   (uint256)
//   slot 5: `_initialized`       (bool)
//   slot 6: `_pubkeyToValidator` (mapping base)
//   slot 7: `_allowedPools`      (mapping address => bool)    ← NEW in Zeta
//   slot 8: `_permissionlessJoinEnabled` (bool)               ← NEW in Zeta
//
// For each active `stakePool`, the real storage slot of `_allowedPools[stakePool]`
// is `keccak256(pool_address_padded_32 || slot7_padded_32)`. Rust's `no_std`
// environment doesn't let us compute keccak256 inside a `const` context, so the
// slot hashes are precomputed offline and baked in as `B256` literals. The
// source inputs are each element of `STAKEPOOL_ADDRESSES` with mapping base 7.
//
// If `STAKEPOOL_ADDRESSES` is ever changed, regenerate these with:
//     python3 -c "from Crypto.Hash import keccak; \
//       k=keccak.new(digest_bits=256); \
//       k.update(bytes(12)+bytes.fromhex('<pool>') + (7).to_bytes(32,'big')); \
//       print(k.hexdigest())"

/// Precomputed `keccak256(pool_padded || slot7_padded)` for each pool in
/// `STAKEPOOL_ADDRESSES` (index-aligned).
const ALLOWED_POOLS_SLOTS: [B256; 4] = [
    // ce128222bd84d67672f863424a03d114cd1253c5
    B256::new(hex_to_bytes32("f0e7cd36827fa13711ddad3d7068a25b49281673eaf95afa383c82c2a3aa3b36")),
    // 78f595fb25d03a742338fb32acfd544bdc63d814
    B256::new(hex_to_bytes32("127031e46799c53f1421740658d3168f5a15495d5c31383f8e8de158ad8e7674")),
    // 891299fe364088ead65aba911ea17dd5d968cd81
    B256::new(hex_to_bytes32("ce7ff84eddcbb51b207ab1ac22cc2e13d4f8f5d3b67b091092ea514186bb963c")),
    // b99aa922eb5cae399b79adc87621e72f66d5a976
    B256::new(hex_to_bytes32("53aa3d4aaf7033d5b7219de41ba52ce5b1fbd2c29d483026006724b3b2230a1b")),
];

/// `const`-friendly hex-string → `[u8; 32]` converter. Panics at compile time
/// if the input is not exactly 64 lowercase hex characters.
const fn hex_to_bytes32(s: &str) -> [u8; 32] {
    let bytes = s.as_bytes();
    assert!(bytes.len() == 64, "hex string must be 64 chars");
    let mut out = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        out[i] = (hex_nibble(bytes[2 * i]) << 4) | hex_nibble(bytes[2 * i + 1]);
        i += 1;
    }
    out
}

const fn hex_nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => panic!("invalid hex nibble"),
    }
}

// ── Storage patch table ─────────────────────────────────────────────────────────

static ZETA_STORAGE_PATCHES: &[StoragePatch] = &[
    // Governance._owner ← configured admin
    (GOVERNANCE_ADDRESS, B256::new(GOVERNANCE_OWNER_SLOT), GOVERNANCE_OWNER_U256),
    // Governance._initialized = 1
    (GOVERNANCE_ADDRESS, B256::new(GOVERNANCE_INITIALIZED_SLOT), ONE_U256),
    // ValidatorManagement._allowedPools[pool] = true (one per pool)
    (VALIDATOR_MANAGEMENT_ADDRESS, ALLOWED_POOLS_SLOTS[0], ONE_U256),
    (VALIDATOR_MANAGEMENT_ADDRESS, ALLOWED_POOLS_SLOTS[1], ONE_U256),
    (VALIDATOR_MANAGEMENT_ADDRESS, ALLOWED_POOLS_SLOTS[2], ONE_U256),
    (VALIDATOR_MANAGEMENT_ADDRESS, ALLOWED_POOLS_SLOTS[3], ONE_U256),
];

// ── HardforkUpgrades impl ───────────────────────────────────────────────────────

/// Zeta hardfork descriptor.
#[derive(Debug)]
pub struct ZetaHardfork;

impl HardforkUpgrades for ZetaHardfork {
    fn name(&self) -> &'static str {
        "Zeta"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        ZETA_SYSTEM_UPGRADES
    }
    fn extra_upgrades(&self) -> &'static [BytecodeUpgrade] {
        ZETA_EXTRA_UPGRADES
    }
    fn storage_patches(&self) -> &'static [StoragePatch] {
        ZETA_STORAGE_PATCHES
    }
}
