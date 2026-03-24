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

use alloy_primitives::{address, Address};

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
