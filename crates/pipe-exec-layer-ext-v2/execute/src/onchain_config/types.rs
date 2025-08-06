//! Common Solidity type definitions for onchain config modules
//! This module contains shared sol! macro definitions to avoid duplication

#![allow(missing_docs)]

use alloy_primitives::{Address, Bytes};
use alloy_sol_macro::sol;
use gravity_api_types::on_chain_config::validator_set::ValidatorSet as GravityValidatorSet;

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
        bytes aptosAddress; // [u8; 32]
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

    function getValidatorSet() external view returns (ValidatorSet memory);
}

sol! {
        function blockPrologue(
            address proposer,
            uint64[] calldata failedProposerIndices,
            uint256 timestampMicros
        );
}

/// Helper function to convert Ethereum address to `AccountAddress` format
/// Ethereum addresses are 20 bytes, need to pad to 32 bytes for `AccountAddress`
pub fn convert_account(acc: &Address) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(acc.as_slice());
    bytes
}

/// Convert Solidity `ValidatorInfo` to Gravity API `ValidatorInfo`
pub fn convert_validator_info(
    solidity_info: &ValidatorInfo,
) -> gravity_api_types::on_chain_config::validator_info::ValidatorInfo {
    use gravity_api_types::on_chain_config::{
        validator_config::ValidatorConfig, validator_info::ValidatorInfo as GravityValidatorInfo,
    };

    // Convert Address to AccountAddress (20 bytes -> AccountAddress)
    let account_address =
        gravity_api_types::u256_define::AccountAddress::from_bytes(&solidity_info.aptosAddress);

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

pub fn convert_validator_set_to_bcs(solidity_validator_set: &ValidatorSet) -> Bytes {
    let gravity_validator_set = GravityValidatorSet {
        active_validators: solidity_validator_set
            .activeValidators
            .iter()
            .map(convert_validator_info)
            .collect(),
        pending_inactive: solidity_validator_set
            .pendingInactive
            .iter()
            .map(convert_validator_info)
            .collect(),
        pending_active: solidity_validator_set
            .pendingActive
            .iter()
            .map(convert_validator_info)
            .collect(),
        total_voting_power: solidity_validator_set.totalVotingPower.to::<u128>(),
        total_joining_power: solidity_validator_set.totalJoiningPower.to::<u128>(),
    };

    // Serialize to BCS format (gravity-aptos standard)
    bcs::to_bytes(&gravity_validator_set).expect("Failed to serialize validator set").into()
}
