//! Common Solidity type definitions for onchain config modules
//! This module contains shared sol! macro definitions to avoid duplication

use alloy_primitives::Address;
use alloy_sol_macro::sol;

sol! {
    enum ValidatorStatus {
        PENDING_ACTIVE, // 0
        ACTIVE, // 1
        PENDING_INACTIVE, // 2
        INACTIVE // 3
    }

    // Commission structure
    struct Commission {
        uint64 rate; // the commission rate charged to delegators(10000 is 100%)
        uint64 maxRate; // maximum commission rate which validator can ever charge
        uint64 maxChangeRate; // maximum daily increase of the validator commission
    }

    /// Complete validator information (merged from multiple contracts)
    struct ValidatorInfo {
        // Basic information (from ValidatorManager)
        bytes consensusPublicKey;
        Commission commission;
        string moniker;
        bool registered;
        address stakeCreditAddress;
        ValidatorStatus status;
        uint256 votingPower; // Changed from uint64 to uint256 to prevent overflow
        uint256 validatorIndex;
        uint256 updateTime;
        address operator;
        bytes validatorNetworkAddresses; // BCS serialized Vec<NetworkAddress>
        bytes fullnodeNetworkAddresses; // BCS serialized Vec<NetworkAddress>
    }

    struct ValidatorSet {
        ValidatorInfo[] activeValidators; // Active validators for the current epoch
        ValidatorInfo[] pendingInactive; // Pending validators to leave in next epoch (still active)
        ValidatorInfo[] pendingActive; // Pending validators to join in next epoch
        uint256 totalVotingPower; // Current total voting power
        uint256 totalJoiningPower; // Total voting power waiting to join in the next epoch
    }

    // event NewEpoch(uint64 indexed epoch, bytes validators);
    event AllValidatorsUpdated(uint256 indexed newEpoch, ValidatorSet validatorSet);

    // function blockPrologue(uint64 _timestamp_microseconds) external onlyVm whenInitialized;
    function blockPrologue(
        address proposer,
        uint64[] calldata failedProposerIndices,
        uint256 timestampMicros
    );

    function getValidatorSet() external view returns (ValidatorSet memory);
}

/// Helper function to convert Ethereum address to AccountAddress format
/// Ethereum addresses are 20 bytes, need to pad to 32 bytes for AccountAddress
pub fn convert_account(acc: &Address) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(acc.as_slice());
    bytes
}

/// Convert Solidity ValidatorInfo to Gravity API ValidatorInfo
pub fn convert_validator_info(solidity_info: &ValidatorInfo) -> gravity_api_types::on_chain_config::validator_info::ValidatorInfo {
    use gravity_api_types::{
        on_chain_config::validator_config::ValidatorConfig,
        on_chain_config::validator_info::ValidatorInfo as GravityValidatorInfo,
    };

    // Convert Address to AccountAddress (20 bytes -> AccountAddress)
    let account_address = gravity_api_types::u256_define::AccountAddress::from_bytes(
        &convert_account(&solidity_info.operator)
    );

    GravityValidatorInfo::new(
        account_address,
        solidity_info.votingPower.to::<u64>(),
        ValidatorConfig::new(
            solidity_info.consensusPublicKey.clone().into(),
            solidity_info.validatorNetworkAddresses.clone().into(),
            solidity_info.fullnodeNetworkAddresses.clone().into(),
            solidity_info.validatorIndex.to::<u64>(),
        ),
    )
} 