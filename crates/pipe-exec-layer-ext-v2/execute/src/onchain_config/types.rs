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

    // Functions to get pending validators (now return full ValidatorConsensusInfo[])
    function getPendingActiveValidators() external view returns (ValidatorConsensusInfo[] memory);
    function getPendingInactiveValidators() external view returns (ValidatorConsensusInfo[] memory);

    // Function to get total voting power directly from contract
    function getTotalVotingPower() external view returns (uint256);

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

/// Convert arrays of ValidatorConsensusInfo to BCS-encoded ValidatorSet
/// Used by ValidatorSetFetcher to convert getActiveValidators() response
///
/// # Arguments
/// * `active_validators` - Active validators from getActiveValidators()
/// * `pending_active` - Validators pending activation (optional, from getCurValidatorConsensusInfos
///   style query)
/// * `pending_inactive` - Validators pending deactivation (still in active set for this epoch)
pub fn convert_validators_to_bcs(
    active_validators: &[ValidatorConsensusInfo],
    pending_active: &[ValidatorConsensusInfo],
    pending_inactive: &[ValidatorConsensusInfo],
) -> Bytes {
    // Calculate total voting power from active validators (in Ether units)
    let total_voting_power: u128 =
        active_validators.iter().map(|v| wei_to_ether(v.votingPower).to::<u128>()).sum();

    // Calculate total joining power from pending_active validators
    let total_joining_power: u128 =
        pending_active.iter().map(|v| wei_to_ether(v.votingPower).to::<u128>()).sum();

    let gravity_validator_set = GravityValidatorSet {
        active_validators: active_validators.iter().map(convert_validator_consensus_info).collect(),
        pending_inactive: pending_inactive.iter().map(convert_validator_consensus_info).collect(),
        pending_active: pending_active.iter().map(convert_validator_consensus_info).collect(),
        total_voting_power,
        total_joining_power,
    };

    // Serialize to BCS format (gravity-aptos standard)
    bcs::to_bytes(&gravity_validator_set).expect("Failed to serialize validator set").into()
}

/// Legacy function for backward compatibility - only active validators
/// Used when we don't have pending validator info available
pub fn convert_active_validators_to_bcs(validators: &[ValidatorConsensusInfo]) -> Bytes {
    convert_validators_to_bcs(validators, &[], &[])
}

// =============================================================================
// Oracle JWK Types (shared by jwk_oracle.rs and observed_jwk.rs)
// =============================================================================

/// Source type for JWK in NativeOracle
pub const SOURCE_TYPE_JWK: u32 = 1;

sol! {
    /// RSA JWK structure from JWKManager contract
    struct OracleRSA_JWK {
        string kid;
        string kty;
        string alg;
        string e;
        string n;
    }

    /// Provider's JWK collection from JWKManager
    struct OracleProviderJWKs {
        bytes issuer;
        uint64 version;
        OracleRSA_JWK[] jwks;
    }

    /// All providers' JWK collection from JWKManager
    struct OracleAllProvidersJWKs {
        OracleProviderJWKs[] entries;
    }

    /// JWKManager.getObservedJWKs()
    function getObservedJWKs() external view returns (OracleAllProvidersJWKs memory);

    /// Event emitted when JWKs are updated
    event ObservedJWKsUpdated(uint256 indexed epoch, OracleProviderJWKs[] jwks);
}

sol! {
    /// DataRecorded event from NativeOracle contract
    /// Emitted when data is recorded by the consensus engine via SYSTEM_CALLER
    /// @param sourceType The source type (0 = BLOCKCHAIN, 1 = JWK, etc.)
    /// @param sourceId The source identifier (e.g., chain ID for blockchains)
    /// @param nonce The nonce (block height, timestamp, etc.)
    /// @param dataLength Length of the stored data
    event DataRecorded(
        uint32 indexed sourceType,
        uint256 indexed sourceId,
        uint128 nonce,
        uint256 dataLength
    );
}

/// RSA JWK fields for BCS serialization - matches gravity-aptos struct order
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GaptosRsaJwk {
    pub kid: String,
    pub kty: String,
    pub alg: String,
    pub e: String,
    pub n: String,
}

/// Convert Oracle RSA_JWK to api-types JWKStruct
pub fn convert_oracle_rsa_to_api_jwk(
    rsa_jwk: OracleRSA_JWK,
) -> gravity_api_types::on_chain_config::jwks::JWKStruct {
    let gaptos_rsa = GaptosRsaJwk {
        kid: rsa_jwk.kid,
        kty: rsa_jwk.kty,
        alg: rsa_jwk.alg,
        e: rsa_jwk.e,
        n: rsa_jwk.n,
    };

    gravity_api_types::on_chain_config::jwks::JWKStruct {
        type_name: "0x1::jwks::RSA_JWK".to_string(),
        data: bcs::to_bytes(&gaptos_rsa).expect("Failed to BCS serialize RSA_JWK"),
    }
}
