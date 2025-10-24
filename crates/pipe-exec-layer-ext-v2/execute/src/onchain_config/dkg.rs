//! DKG state and event handling module
//! 
//! This module contains all DKG-related functionality including:
//! - Solidity type definitions for DKG structures
//! - DKG state fetching from contracts
//! - DKG event conversion to API types
//! - DKG transcript processing

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    SYSTEM_CALLER, DKG_ADDR
};
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::dkg::DKGState as GravityDKGState;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::*;
use hex;

// ============================================================================
// Solidity Type Definitions
// ============================================================================

sol! {
    struct FixedPoint64 {
        uint128 value;
    }

    // Configuration variant enum
    enum ConfigVariant {
        V1,     // Basic configuration
        V2      // Configuration with fast path
    }

    // Basic configuration struct
    struct ConfigV1 {
        FixedPoint64 secrecyThreshold;
        FixedPoint64 reconstructionThreshold;
    }

    // Configuration with fast path struct
    struct ConfigV2 {
        FixedPoint64 secrecyThreshold;
        FixedPoint64 reconstructionThreshold;
        FixedPoint64 fastPathSecrecyThreshold;
    }

    // Main configuration struct
    struct RandomnessConfigData {
        ConfigVariant variant;
        ConfigV1 configV1;
        ConfigV2 configV2;
    }

    // Struct for validator consensus information
    struct ValidatorConsensusInfo {
        bytes aptosAddress;
        bytes pkBytes;
        uint64 votingPower;
    }

    // DKG session metadata - can be considered as the public input of DKG
    struct DKGSessionMetadata {
        uint64 dealerEpoch;
        RandomnessConfigData randomnessConfig;
        ValidatorConsensusInfo[] dealerValidatorSet;
        ValidatorConsensusInfo[] targetValidatorSet;
    }

    // DKG session state
    struct DKGSessionState {
        DKGSessionMetadata metadata;
        uint64 startTimeUs;
        bytes transcript;
    }

    // DKG state containing last completed and in progress sessions
    struct DKGState {
        DKGSessionState lastCompleted;
        bool hasLastCompleted;
        DKGSessionState inProgress;
        bool hasInProgress;
    }

    // Function to get DKG state
    function getDKGState() external view returns (DKGState memory);
    
    // Function to finish DKG with result
    function finishWithDkgResult(
        bytes calldata dkg_result
    ) external;
    
    // DKG start event
    event DKGStartEvent(DKGSessionMetadata metadata, uint64 startTimeUs);
}

// ============================================================================
// DKG State Fetcher
// ============================================================================

/// Fetcher for DKG state information
#[derive(Debug)]
pub struct DKGStateFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> DKGStateFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new DKG state fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for DKGStateFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_number: u64) -> Bytes {
        let call = getDKGStateCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self.base_fetcher.eth_call(
            Self::caller_address(),
            Self::contract_address(),
            input,
            block_number,
        );

        // Decode the Solidity DKG state
        let solidity_dkg_state = getDKGStateCall::abi_decode_returns(&result)
            .expect("Failed to decode getDKGState return value");
        convert_dkg_state_to_bcs(&solidity_dkg_state)
    }

    fn contract_address() -> Address {
        DKG_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}

/// Helper function to convert FixedPoint64
fn convert_fixed_point64(fp: &FixedPoint64) -> gravity_api_types::on_chain_config::dkg::FixedPoint64 {
    gravity_api_types::on_chain_config::dkg::FixedPoint64 {
        value: fp.value,
    }
}

/// Helper function to convert ConfigV1
fn convert_config_v1(config: &ConfigV1) -> gravity_api_types::on_chain_config::dkg::ConfigV1 {
    gravity_api_types::on_chain_config::dkg::ConfigV1 {
        secrecyThreshold: convert_fixed_point64(&config.secrecyThreshold),
        reconstructionThreshold: convert_fixed_point64(&config.reconstructionThreshold),
    }
}

/// Helper function to convert ConfigV2
fn convert_config_v2(config: &ConfigV2) -> gravity_api_types::on_chain_config::dkg::ConfigV2 {
    gravity_api_types::on_chain_config::dkg::ConfigV2 {
        secrecyThreshold: convert_fixed_point64(&config.secrecyThreshold),
        reconstructionThreshold: convert_fixed_point64(&config.reconstructionThreshold),
        fastPathSecrecyThreshold: convert_fixed_point64(&config.fastPathSecrecyThreshold),
    }
}

/// Helper function to convert RandomnessConfigData
fn convert_randomness_config(config: &RandomnessConfigData) -> gravity_api_types::on_chain_config::dkg::RandomnessConfigData {
    // Convert enum variant
    let variant = match config.variant {
        ConfigVariant::V1 => gravity_api_types::on_chain_config::dkg::ConfigVariant::V1,
        ConfigVariant::V2 => gravity_api_types::on_chain_config::dkg::ConfigVariant::V2,
        ConfigVariant::__Invalid => panic!("Invalid ConfigVariant"),
    };
    
    gravity_api_types::on_chain_config::dkg::RandomnessConfigData {
        variant,
        configV1: convert_config_v1(&config.configV1),
        configV2: convert_config_v2(&config.configV2),
    }
}

/// Helper function to convert ValidatorConsensusInfo
fn convert_validator(validator: &ValidatorConsensusInfo) -> gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
    gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
        addr: gravity_api_types::account::ExternalAccountAddress::new(
            validator.aptosAddress.to_vec().try_into().unwrap(),
        ),
        pk_bytes: hex::decode(&validator.pkBytes).unwrap(),
        voting_power: validator.votingPower,
    }
}

/// Helper function to convert DKGSessionMetadata
fn convert_dkg_session_metadata(metadata: &DKGSessionMetadata) -> gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
    gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
        dealer_epoch: metadata.dealerEpoch,
        randomness_config: convert_randomness_config(&metadata.randomnessConfig),
        dealer_validator_set: metadata.dealerValidatorSet.iter().map(convert_validator).collect(),
        target_validator_set: metadata.targetValidatorSet.iter().map(convert_validator).collect(),
    }
}

// ============================================================================
// DKG Event Conversion (for events, using to_vec() instead of hex::decode)
// ============================================================================

/// Helper function to convert ValidatorConsensusInfo for events (uses to_vec())
fn convert_validator_for_event(validator: &ValidatorConsensusInfo) -> gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
    gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
        addr: gravity_api_types::account::ExternalAccountAddress::new(
            validator.aptosAddress.to_vec().try_into().unwrap(),
        ),
        pk_bytes: validator.pkBytes.to_vec(),
        voting_power: validator.votingPower,
    }
}

/// Helper function to convert DKGSessionMetadata for events
fn convert_dkg_session_metadata_for_event(metadata: &DKGSessionMetadata) -> gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
    gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
        dealer_epoch: metadata.dealerEpoch,
        randomness_config: convert_randomness_config(&metadata.randomnessConfig),
        dealer_validator_set: metadata.dealerValidatorSet.iter().map(convert_validator_for_event).collect(),
        target_validator_set: metadata.targetValidatorSet.iter().map(convert_validator_for_event).collect(),
    }
}

/// Convert DKGStartEvent to API type (public interface for external use)
pub fn convert_dkg_start_event_to_api(event: &DKGStartEvent) -> gravity_api_types::on_chain_config::dkg::DKGStartEvent {
    gravity_api_types::on_chain_config::dkg::DKGStartEvent {
        session_metadata: convert_dkg_session_metadata_for_event(&event.metadata),
        start_time_us: event.startTimeUs,
    }
}

// ============================================================================
// DKG State Conversion
// ============================================================================

/// Convert Solidity DKG state to BCS-encoded bytes
fn convert_dkg_state_to_bcs(solidity_state: &DKGState) -> Bytes {
    let gravity_state = GravityDKGState {
        last_completed: if !solidity_state.hasLastCompleted {
            None
        } else {
            Some(gravity_api_types::on_chain_config::dkg::DKGSessionState {
                metadata: convert_dkg_session_metadata(&solidity_state.lastCompleted.metadata),
                start_time_us: solidity_state.lastCompleted.startTimeUs,
                transcript: solidity_state.lastCompleted.transcript.to_vec(),
            })
        },
        in_progress: if !solidity_state.hasInProgress {
            None
        } else {
            Some(gravity_api_types::on_chain_config::dkg::DKGSessionState {
                metadata: convert_dkg_session_metadata(&solidity_state.inProgress.metadata),
                start_time_us: solidity_state.inProgress.startTimeUs,
                transcript: solidity_state.inProgress.transcript.to_vec(),
            })
        },
    };

    // Serialize to BCS
    let bcs_bytes = bcs::to_bytes(&gravity_state)
        .expect("Failed to serialize DKG state to BCS");

    Bytes::from(bcs_bytes)
}

/// Construct DKG transaction from DKGTranscript
/// 
/// This function is called by the validator transactions construction logic in mod.rs
pub(crate) fn construct_dkg_transaction(
    dkg_transcript: gravity_api_types::on_chain_config::dkg::DKGTranscript,
    nonce: u64,
    gas_price: u128,
) -> Result<reth_ethereum_primitives::TransactionSigned, String> {
    use super::RECONFIGURATION_WITH_DKG_ADDR;
    use alloy_primitives::Bytes;
    use alloy_sol_types::SolCall;
    
    let call = finishWithDkgResultCall {
        dkg_result: dkg_transcript.transcript_bytes.into(),
    };
    let input: Bytes = call.abi_encode().into();
    
    Ok(super::new_system_call_txn(RECONFIGURATION_WITH_DKG_ADDR, nonce, gas_price, input))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dkg_conversion() {
        // Basic test to ensure conversion functions compile
        // TODO: Add comprehensive tests
    }
}
