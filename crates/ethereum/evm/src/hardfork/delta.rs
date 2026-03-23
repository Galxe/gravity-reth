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

use alloy_primitives::{address, Address};

/// Governance contract system address
pub const GOVERNANCE_ADDRESS: Address = address!("00000000000000000000000000000001625F3000");

/// Storage slot for `Ownable._owner` (slot 0 in standard Solidity layout)
///
/// Inheritance: `Governance is IGovernance, Ownable2Step`
///   - `Ownable` (from `Context`): `address private _owner` → **slot 0**
///   - `Ownable2Step`:             `address private _pendingOwner` → slot 1
///   - `Governance`:               `uint64 nextProposalId` → slot 2, ...
pub const GOVERNANCE_OWNER_SLOT: [u8; 32] = [0u8; 32];

/// The address to set as Governance owner.
///
/// TODO: Replace with the actual multisig / admin address before mainnet deployment.
pub const GOVERNANCE_OWNER: Address = address!("6e2021ee24e2430da0f5bb9c2ae6c586bf3e0a0f");
