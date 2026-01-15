//! Common Solidity type definitions for onchain config modules
//! This module contains shared sol! macro definitions to avoid duplication

#![allow(missing_docs)]

use alloy_primitives::{Bytes, U256};
use alloy_sol_macro::sol;
use gravity_api_types::on_chain_config::validator_set::ValidatorSet as GravityValidatorSet;

// Wei to Ether conversion constant (10^18)
const WEI_PER_ETHER: U256 = U256::from_limbs([1000000000000000000, 0, 0, 0]);

/// Convert voting power from wei to ether
fn wei_to_ether(wei: U256) -> U256 {
    wei / WEI_PER_ETHER
}

sol! {
    // Validator lifecycle status enum (from Types.sol)
    enum ValidatorStatus {
        INACTIVE, // 0
        PENDING_ACTIVE, // 1
        ACTIVE, // 2
        PENDING_INACTIVE // 3
    }

    /// Validator consensus info (from Types.sol in gravity_chain_core_contracts)
    /// Returned by ValidatorManagement.getActiveValidators()
    struct ValidatorConsensusInfo {
        address validator;           // Validator identity address
        bytes consensusPubkey;       // BLS public key for consensus
        bytes consensusPop;          // Proof of possession for BLS key
        uint256 votingPower;         // Voting power derived from bond
        uint64 validatorIndex;       // Index in active validator array
        bytes networkAddresses;      // Network addresses for P2P communication
        bytes fullnodeAddresses;     // Fullnode addresses for sync
    }

    // Function from ValidatorManagement contract
    function getActiveValidators() external view returns (ValidatorConsensusInfo[] memory);

    /// NewEpochEvent from Reconfiguration.sol
    /// Emitted when epoch transition completes with full validator set
    event NewEpochEvent(
        uint64 indexed newEpoch,
        ValidatorConsensusInfo[] validatorSet,
        uint256 totalVotingPower,
        uint64 transitionTime
    );
}

sol! {
    /// onBlockStart from Blocker.sol
    /// Called by blockchain runtime at the start of each block
    /// @param proposerIndex Index of the block proposer in the active validator set
    /// @param failedProposerIndices Indices of validators who failed to propose
    /// @param timestampMicros Block timestamp in microseconds
    function onBlockStart(
        uint64 proposerIndex,
        uint64[] calldata failedProposerIndices,
        uint64 timestampMicros
    );
}

/// Derive 32-byte AccountAddress from BLS consensus public key using SHA3-256
/// This matches the derivation used in gravity-sdk for validator identity
pub fn derive_account_address_from_consensus_pubkey(consensus_pubkey: &[u8]) -> [u8; 32] {
    use tiny_keccak::{Hasher, Sha3};

    let mut hasher = Sha3::v256();
    hasher.update(consensus_pubkey);
    let mut output = [0u8; 32];
    hasher.finalize(&mut output);
    output
}

/// Convert Solidity `ValidatorConsensusInfo` to Gravity API `ValidatorInfo`
pub fn convert_validator_consensus_info(
    info: &ValidatorConsensusInfo,
) -> gravity_api_types::on_chain_config::validator_info::ValidatorInfo {
    use gravity_api_types::on_chain_config::{
        validator_config::ValidatorConfig, validator_info::ValidatorInfo as GravityValidatorInfo,
    };

    // Derive AccountAddress from consensus public key using SHA3-256
    let account_address_bytes = derive_account_address_from_consensus_pubkey(&info.consensusPubkey);
    let account_address =
        gravity_api_types::u256_define::AccountAddress::from_bytes(&account_address_bytes);

    // Convert voting power from wei to ether
    let power_ether = wei_to_ether(info.votingPower);

    // Original Ethereum address (20 bytes) as reth_account_address
    let reth_account_address = info.validator.as_slice().to_vec();

    GravityValidatorInfo::new(
        account_address,
        power_ether.to::<u64>(),
        ValidatorConfig::new(
            info.consensusPubkey.clone().into(),
            info.networkAddresses.to_vec(),
            info.fullnodeAddresses.to_vec(),
            info.validatorIndex,
        ),
        reth_account_address,
    )
}

/// Convert array of ValidatorConsensusInfo to BCS-encoded ValidatorSet
/// Used by ValidatorSetFetcher to convert getActiveValidators() response
pub fn convert_active_validators_to_bcs(validators: &[ValidatorConsensusInfo]) -> Bytes {
    let total_voting_power: u128 =
        validators.iter().map(|v| wei_to_ether(v.votingPower).to::<u128>()).sum();

    let gravity_validator_set = GravityValidatorSet {
        active_validators: validators.iter().map(convert_validator_consensus_info).collect(),
        pending_inactive: vec![], // Not returned by getActiveValidators()
        pending_active: vec![],   // Not returned by getActiveValidators()
        total_voting_power,
        total_joining_power: 0, // Not returned by getActiveValidators()
    };

    // Serialize to BCS format (gravity-aptos standard)
    bcs::to_bytes(&gravity_validator_set).expect("Failed to serialize validator set").into()
}
