//! DKG state and event handling module
//!
//! This module contains all DKG-related functionality including:
//! - Solidity type definitions for DKG structures
//! - DKG state fetching from contracts
//! - DKG event conversion to API types
//! - DKG transcript processing

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    DKG_ADDR, RANDOMNESS_CONFIG_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::dkg::DKGState as GravityDKGState;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};

// ============================================================================
// Solidity Type Definitions
// ============================================================================

sol! {
    // Configuration variant enum - matches RandomnessConfig.sol
    enum ConfigVariant {
        Off,    // Randomness disabled
        V2      // Configuration with fast path
    }

    // V2 configuration data with DKG thresholds - matches RandomnessConfig.ConfigV2Data
    // Thresholds are fixed-point values (value / 2^64), stored as uint128
    struct ConfigV2Data {
        uint128 secrecyThreshold;
        uint128 reconstructionThreshold;
        uint128 fastPathSecrecyThreshold;
    }

    // Main configuration struct - matches RandomnessConfig.RandomnessConfigData
    struct RandomnessConfigData {
        ConfigVariant variant;
        ConfigV2Data configV2;
    }

    // Struct for validator consensus information - matches Types.ValidatorConsensusInfo
    struct ValidatorConsensusInfo {
        address validator;
        bytes consensusPubkey;
        bytes consensusPop;
        uint256 votingPower;
        uint64 validatorIndex;
        bytes networkAddresses;
        bytes fullnodeAddresses;
    }

    // DKG session metadata - matches IDKG.DKGSessionMetadata
    struct DKGSessionMetadata {
        uint64 dealerEpoch;
        RandomnessConfigData randomnessConfig;
        ValidatorConsensusInfo[] dealerValidatorSet;
        ValidatorConsensusInfo[] targetValidatorSet;
    }

    // DKG session info - matches IDKG.DKGSessionInfo
    struct DKGSessionInfo {
        DKGSessionMetadata metadata;
        uint64 startTimeUs;
        bytes transcript;
    }

    // Function to get DKG state - multi-value return matching DKG.getDKGState()
    function getDKGState() external view returns (
        DKGSessionInfo memory lastCompleted,
        bool hasLastCompleted,
        DKGSessionInfo memory inProgress,
        bool hasInProgress
    );

    // Function to finish DKG with result - matches IReconfiguration.finishTransition
    function finishTransition(
        bytes calldata dkgResult
    ) external;

    // Function to get current randomness configuration - matches RandomnessConfig.getCurrentConfig()
    function getCurrentConfig() external view returns (RandomnessConfigData memory);

    // DKG start event - matches DKG.DKGStartEvent
    event DKGStartEvent(
        uint64 indexed dealerEpoch,
        uint64 startTimeUs,
        DKGSessionMetadata metadata
    );
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
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let call = getDKGStateCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
            .map_err(|e| {
                tracing::warn!("Failed to fetch DKG state at block {}: {:?}", block_id, e);
            })
            .ok()?;

        // Decode the Solidity DKG state
        let solidity_dkg_state = getDKGStateCall::abi_decode_returns(&result)
            .expect("Failed to decode getDKGState return value");
        Some(convert_dkg_state_to_bcs(&solidity_dkg_state))
    }

    fn contract_address() -> Address {
        DKG_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}

// ============================================================================
// Randomness Config Fetcher
// ============================================================================

/// Fetcher for Randomness configuration information
#[derive(Debug)]
pub struct RandomnessConfigFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> RandomnessConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
{
    /// Create a new randomness config fetcher
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }
}

impl<'a, EthApi> ConfigFetcher for RandomnessConfigFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let call = getCurrentConfigCall {};
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(Self::caller_address(), Self::contract_address(), input, block_id)
            .map_err(|e| {
                tracing::warn!("Failed to fetch RandomnessConfig at block {}: {:?}", block_id, e);
            })
            .ok()?;

        // Decode the Solidity RandomnessConfig
        let solidity_config = getCurrentConfigCall::abi_decode_returns(&result)
            .expect("Failed to decode getCurrentConfig return value");
        Some(convert_randomness_config_to_bcs(&solidity_config))
    }

    fn contract_address() -> Address {
        RANDOMNESS_CONFIG_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}

/// Convert RandomnessConfigData to BCS-encoded bytes
fn convert_randomness_config_to_bcs(config: &RandomnessConfigData) -> Bytes {
    let gravity_config = convert_randomness_config(config);

    // Serialize to BCS
    let bcs_bytes =
        bcs::to_bytes(&gravity_config).expect("Failed to serialize RandomnessConfig to BCS");

    Bytes::from(bcs_bytes)
}

/// Helper function to convert ConfigV2Data
fn convert_config_v2_data(
    config: &ConfigV2Data,
) -> gravity_api_types::on_chain_config::dkg::ConfigV2 {
    gravity_api_types::on_chain_config::dkg::ConfigV2 {
        secrecyThreshold: gravity_api_types::on_chain_config::dkg::FixedPoint64 {
            value: config.secrecyThreshold,
        },
        reconstructionThreshold: gravity_api_types::on_chain_config::dkg::FixedPoint64 {
            value: config.reconstructionThreshold,
        },
        fastPathSecrecyThreshold: gravity_api_types::on_chain_config::dkg::FixedPoint64 {
            value: config.fastPathSecrecyThreshold,
        },
    }
}

/// Helper function to convert RandomnessConfigData
fn convert_randomness_config(
    config: &RandomnessConfigData,
) -> gravity_api_types::on_chain_config::dkg::RandomnessConfigData {
    // Convert enum variant (Off -> V1, V2 -> V2 in API types)
    let variant = match config.variant {
        ConfigVariant::Off => gravity_api_types::on_chain_config::dkg::ConfigVariant::V1,
        ConfigVariant::V2 => gravity_api_types::on_chain_config::dkg::ConfigVariant::V2,
        ConfigVariant::__Invalid => panic!("Invalid ConfigVariant"),
    };

    // For Off variant, configV1 should be default/empty
    let config_v1 = gravity_api_types::on_chain_config::dkg::ConfigV1 {
        secrecyThreshold: gravity_api_types::on_chain_config::dkg::FixedPoint64 { value: 0 },
        reconstructionThreshold: gravity_api_types::on_chain_config::dkg::FixedPoint64 { value: 0 },
    };

    gravity_api_types::on_chain_config::dkg::RandomnessConfigData {
        variant,
        configV1: config_v1,
        configV2: convert_config_v2_data(&config.configV2),
    }
}

/// Helper function to convert ValidatorConsensusInfo
fn convert_validator(
    validator: &ValidatorConsensusInfo,
) -> gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
    // Convert address to 32-byte array (pad with zeros if needed)
    let mut addr_bytes = [0u8; 32];
    let validator_bytes = validator.validator.as_slice();
    addr_bytes[32 - validator_bytes.len()..].copy_from_slice(validator_bytes);

    gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
        addr: gravity_api_types::account::ExternalAccountAddress::new(addr_bytes),
        pk_bytes: validator.consensusPubkey.to_vec(),
        // Convert wei to tokens by dividing by 10^18
        voting_power: (validator.votingPower /
            alloy_primitives::U256::from(10).pow(alloy_primitives::U256::from(18)))
        .try_into()
        .unwrap_or(u64::MAX),
    }
}

/// Helper function to convert DKGSessionMetadata
fn convert_dkg_session_metadata(
    metadata: &DKGSessionMetadata,
) -> gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
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
fn convert_validator_for_event(
    validator: &ValidatorConsensusInfo,
) -> gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
    // Convert address to 32-byte array (pad with zeros if needed)
    let mut addr_bytes = [0u8; 32];
    let validator_bytes = validator.validator.as_slice();
    addr_bytes[32 - validator_bytes.len()..].copy_from_slice(validator_bytes);

    gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfo {
        addr: gravity_api_types::account::ExternalAccountAddress::new(addr_bytes),
        pk_bytes: validator.consensusPubkey.to_vec(),
        // Convert wei to tokens by dividing by 10^18
        voting_power: (validator.votingPower /
            alloy_primitives::U256::from(10).pow(alloy_primitives::U256::from(18)))
        .try_into()
        .unwrap_or(u64::MAX),
    }
}

/// Helper function to convert DKGSessionMetadata for events
fn convert_dkg_session_metadata_for_event(
    metadata: &DKGSessionMetadata,
) -> gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
    gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
        dealer_epoch: metadata.dealerEpoch,
        randomness_config: convert_randomness_config(&metadata.randomnessConfig),
        dealer_validator_set: metadata
            .dealerValidatorSet
            .iter()
            .map(convert_validator_for_event)
            .collect(),
        target_validator_set: metadata
            .targetValidatorSet
            .iter()
            .map(convert_validator_for_event)
            .collect(),
    }
}

/// Convert DKGStartEvent to API type (public interface for external use)
pub fn convert_dkg_start_event_to_api(
    event: &DKGStartEvent,
) -> gravity_api_types::on_chain_config::dkg::DKGStartEvent {
    gravity_api_types::on_chain_config::dkg::DKGStartEvent {
        session_metadata: convert_dkg_session_metadata_for_event(&event.metadata),
        start_time_us: event.startTimeUs,
    }
}

// ============================================================================
// DKG State Conversion
// ============================================================================

/// Convert Solidity DKG state (multi-value return) to BCS-encoded bytes
fn convert_dkg_state_to_bcs(solidity_state: &getDKGStateReturn) -> Bytes {
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
    let bcs_bytes = bcs::to_bytes(&gravity_state).expect("Failed to serialize DKG state to BCS");

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

    let call = finishTransitionCall { dkgResult: dkg_transcript.transcript_bytes.into() };
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
