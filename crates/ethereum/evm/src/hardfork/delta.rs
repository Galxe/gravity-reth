//! Delta hardfork: activate Governance contract
//!
//! The Governance contract was deployed via BSC-style bytecode placement during genesis,
//! which skips the Solidity constructor. As a result, the `Ownable(initialOwner)` constructor
//! never ran, leaving `_owner` as `address(0)`. This prevents the owner from calling
//! `addExecutor()` / `removeExecutor()`, which in turn makes `execute()` permanently
//! unreachable.
//!
//! This hardfork writes the correct owner address to storage slot 0 of the Governance
//! contract, restoring the full proposal execution lifecycle.
//!
//! Additionally, for E2E testing, it overrides GovernanceConfig storage to enable
//! fast governance proposals (10-second voting, minimal thresholds).

use alloy_primitives::{address, Address, B256, U256};
use super::common::{BytecodeUpgrade, HardforkUpgrades, StoragePatch};

/// Delta hardfork descriptor.
pub struct DeltaHardfork;

/// Storage patches for Delta hardfork: Governance owner + GovernanceConfig E2E overrides.
static DELTA_STORAGE_PATCHES: &[StoragePatch] = &[
    // Set Governance._owner = GOVERNANCE_OWNER
    (
        GOVERNANCE_ADDRESS,
        B256::new(GOVERNANCE_OWNER_SLOT),
        // GOVERNANCE_OWNER as left-padded U256: the address occupies the lower 20 bytes
        GOVERNANCE_OWNER_U256,
    ),
    // Set Governance.nextProposalId = 1 (packed in slot 1 with _pendingOwner)
    (
        GOVERNANCE_ADDRESS,
        B256::new(GOVERNANCE_NEXT_PROPOSAL_ID_SLOT),
        U256::from_be_bytes(GOVERNANCE_NEXT_PROPOSAL_ID_VALUE),
    ),
    // GovernanceConfig: minVotingThreshold = 1
    (
        GOVERNANCE_CONFIG_ADDRESS,
        B256::new(GOV_CONFIG_SLOT_MIN_THRESHOLD),
        U256::from_limbs([GOV_CONFIG_MIN_THRESHOLD as u64, 0, 0, 0]),
    ),
    // GovernanceConfig: requiredProposerStake = 1
    (
        GOVERNANCE_CONFIG_ADDRESS,
        B256::new(GOV_CONFIG_SLOT_PROPOSER_STAKE),
        U256::from_limbs([GOV_CONFIG_PROPOSER_STAKE as u64, 0, 0, 0]),
    ),
    // GovernanceConfig: votingDurationMicros = 10_000_000 (10s)
    (
        GOVERNANCE_CONFIG_ADDRESS,
        B256::new(GOV_CONFIG_SLOT_VOTING_DURATION),
        U256::from_limbs([GOV_CONFIG_VOTING_DURATION, 0, 0, 0]),
    ),
];

impl HardforkUpgrades for DeltaHardfork {
    fn name(&self) -> &'static str { "Delta" }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] { &[] }
    fn storage_patches(&self) -> &'static [StoragePatch] { DELTA_STORAGE_PATCHES }
}

/// Governance contract system address
pub const GOVERNANCE_ADDRESS: Address = address!("00000000000000000000000000000001625F3000");

/// Storage slot for `Ownable._owner` (slot 0 in standard Solidity layout)
///
/// Storage layout (from `forge inspect Governance storage-layout`):
///   - slot 0: `_owner` (address, 20 bytes, offset 0)
///   - slot 1: `_pendingOwner` (address, 20 bytes, offset 0) + `nextProposalId` (uint64, 8 bytes, offset 20)
///   - slot 2: `_proposals` mapping base
pub const GOVERNANCE_OWNER_SLOT: [u8; 32] = [0u8; 32];

/// Storage slot for `nextProposalId` — packed in slot 1 at byte offset 20.
/// `_pendingOwner` occupies bytes 0-19 of slot 1 (initially address(0)).
/// `nextProposalId` occupies bytes 20-27 of slot 1 (uint64).
/// To set nextProposalId=1 with _pendingOwner=0, the slot value = 1 << 160.
pub const GOVERNANCE_NEXT_PROPOSAL_ID_SLOT: [u8; 32] = {
    let mut s = [0u8; 32];
    s[31] = 1; // slot 1
    s
};
/// nextProposalId=1, shifted left by 160 bits (20 bytes offset in packed storage).
/// As [u8; 32]: 0x0000000000000001_000000000000000000000000_00000000
///              ^^^^^^^^^^^^^^^^^^ nextProposalId=1 at bytes 20-27
pub const GOVERNANCE_NEXT_PROPOSAL_ID_VALUE: [u8; 32] = {
    let mut v = [0u8; 32];
    // nextProposalId = 1 at offset 20 bytes from LSB
    // In big-endian 32-byte representation: byte index = 32 - 20 - 8 = 4
    // So v[4..12] should be 0x0000000000000001
    v[11] = 1;
    v
};

/// The address to set as Governance owner (faucet / hardhat #0 for E2E testing).
///
/// TODO: Replace with the actual multisig / admin address before mainnet deployment.
pub const GOVERNANCE_OWNER: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

/// Precomputed `GOVERNANCE_OWNER` as U256 (left-padded 20-byte address in 32-byte word).
/// Used in static table since `Address::into_word()` is not const.
pub const GOVERNANCE_OWNER_U256: U256 = {
    let bytes = GOVERNANCE_OWNER.0 .0; // [u8; 20]
    // Left-pad to 32 bytes in big-endian
    let mut word = [0u8; 32];
    word[12] = bytes[0]; word[13] = bytes[1]; word[14] = bytes[2]; word[15] = bytes[3];
    word[16] = bytes[4]; word[17] = bytes[5]; word[18] = bytes[6]; word[19] = bytes[7];
    word[20] = bytes[8]; word[21] = bytes[9]; word[22] = bytes[10]; word[23] = bytes[11];
    word[24] = bytes[12]; word[25] = bytes[13]; word[26] = bytes[14]; word[27] = bytes[15];
    word[28] = bytes[16]; word[29] = bytes[17]; word[30] = bytes[18]; word[31] = bytes[19];
    U256::from_be_bytes(word)
};

// ── GovernanceConfig overrides for E2E testing ──────────────────────────

/// GovernanceConfig contract system address
pub const GOVERNANCE_CONFIG_ADDRESS: Address =
    address!("00000000000000000000000000000001625F1004");

/// GovernanceConfig storage layout (Solidity sequential packing):
///   slot 0: minVotingThreshold    (uint128)
///   slot 1: requiredProposerStake (uint256)
///   slot 2: votingDurationMicros  (uint64)
pub const GOV_CONFIG_SLOT_MIN_THRESHOLD: [u8; 32] = [0u8; 32];
pub const GOV_CONFIG_SLOT_PROPOSER_STAKE: [u8; 32] = {
    let mut s = [0u8; 32];
    s[31] = 1;
    s
};
pub const GOV_CONFIG_SLOT_VOTING_DURATION: [u8; 32] = {
    let mut s = [0u8; 32];
    s[31] = 2;
    s
};

/// Test values: 1 vote quorum, 1 wei proposer stake, 10-second voting period.
pub const GOV_CONFIG_MIN_THRESHOLD: u128 = 1;
pub const GOV_CONFIG_PROPOSER_STAKE: u128 = 1;
/// 10 seconds in microseconds
pub const GOV_CONFIG_VOTING_DURATION: u64 = 10_000_000;
