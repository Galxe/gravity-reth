//! Fetcher for DKG state information

use super::{
    base::{ConfigFetcher, OnchainConfigFetcher},
    RECONFIGURATION_WITH_DKG_ADDR, SYSTEM_CALLER, DKG_ADDR
};
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::dkg::DKGState as GravityDKGState;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::*;

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
}

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
        // info!("lightman1020: solidity_dkg_state={:?}", solidity_dkg_state);
        convert_dkg_state_to_bcs(&solidity_dkg_state)
    }

    fn contract_address() -> Address {
        DKG_ADDR
    }

    fn caller_address() -> Address {
        SYSTEM_CALLER
    }
}

/// Convert Solidity DKG state to BCS-encoded bytes
fn convert_dkg_state_to_bcs(solidity_state: &DKGState) -> Bytes {
    let gravity_state = GravityDKGState {
        last_completed: if !solidity_state.hasLastCompleted {
            None
        } else {
            Some(gravity_api_types::on_chain_config::dkg::DKGSessionState {
                metadata: gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
                    dealer_epoch: solidity_state.lastCompleted.metadata.dealerEpoch,
                    dealer_validator_set: solidity_state
                        .lastCompleted
                        .metadata
                        .dealerValidatorSet
                        .iter()
                        .map(|validator| {
                            gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfoMoveStruct {
                                addr: gravity_api_types::account::ExternalAccountAddress::new(
                                    validator.aptosAddress.to_vec().try_into().unwrap(),
                                ),
                                pk_bytes: validator.pkBytes.to_vec(),
                                voting_power: validator.votingPower,
                            }
                        })
                        .collect(),
                    target_validator_set: solidity_state
                        .lastCompleted
                        .metadata
                        .targetValidatorSet
                        .iter()
                        .map(|validator| {
                            gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfoMoveStruct {
                                addr: gravity_api_types::account::ExternalAccountAddress::new(
                                    validator.aptosAddress.to_vec().try_into().unwrap(),
                                ),
                                pk_bytes: validator.pkBytes.to_vec(),
                                voting_power: validator.votingPower,
                            }
                        })
                        .collect(),
                },
                start_time_us: solidity_state.lastCompleted.startTimeUs,
                transcript: solidity_state.lastCompleted.transcript.to_vec(),
            })
        },
        in_progress: if !solidity_state.hasInProgress {
            None
        } else {
            Some(gravity_api_types::on_chain_config::dkg::DKGSessionState {
                metadata: gravity_api_types::on_chain_config::dkg::DKGSessionMetadata {
                    dealer_epoch: solidity_state.inProgress.metadata.dealerEpoch,
                    dealer_validator_set: solidity_state
                        .inProgress
                        .metadata
                        .dealerValidatorSet
                        .iter()
                        .map(|validator| {
                            gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfoMoveStruct {
                                addr: gravity_api_types::account::ExternalAccountAddress::new(
                                    validator.aptosAddress.to_vec().try_into().unwrap(),
                                ),
                                pk_bytes: validator.pkBytes.to_vec(),
                                voting_power: validator.votingPower,
                            }
                        })
                        .collect(),
                    target_validator_set: solidity_state
                        .inProgress
                        .metadata
                        .targetValidatorSet
                        .iter()
                        .map(|validator| {
                            gravity_api_types::on_chain_config::dkg::ValidatorConsensusInfoMoveStruct {
                                addr: gravity_api_types::account::ExternalAccountAddress::new(
                                    validator.aptosAddress.to_vec().try_into().unwrap(),
                                ),
                                pk_bytes: validator.pkBytes.to_vec(),
                                voting_power: validator.votingPower,
                            }
                        })
                        .collect(),
                },
                start_time_us: solidity_state.inProgress.startTimeUs,
                transcript: solidity_state.inProgress.transcript.to_vec(),
            })
        },
    };
    info!("lightman1021: gravity_state={:?}", gravity_state);

    // Serialize to BCS
    let bcs_bytes = bcs::to_bytes(&gravity_state)
        .expect("Failed to serialize DKG state to BCS");

    Bytes::from(bcs_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onchain_config::base::tests::*;

    create_config_test!(
        test_dkg_state_fetcher,
        DKGStateFetcher,
        |framework: &ConfigFetcherTestFramework<DKGStateFetcher<'_, MockEthCall>>| {
            // Setup mock response
            let mock_dkg_state = DKGState {
                lastCompleted: DKGSessionState {
                    metadata: DKGSessionMetadata {
                        dealerEpoch: 1,
                        dealerValidatorSet: vec![],
                        targetValidatorSet: vec![],
                    },
                    startTimeUs: 1000,
                    transcript: Bytes::from(vec![1, 2, 3]),
                },
                inProgress: DKGSessionState {
                    metadata: DKGSessionMetadata {
                        dealerEpoch: 0,
                        dealerValidatorSet: vec![],
                        targetValidatorSet: vec![],
                    },
                    startTimeUs: 0,
                    transcript: Bytes::new(),
                },
            };

            framework.mock_eth_call.set_sol_response(
                DKGStateFetcher::caller_address(),
                DKGStateFetcher::contract_address(),
                getDKGStateCall {},
                framework.block_number,
                mock_dkg_state,
            );
        },
        |result: Bytes| {
            // Verify the result is BCS-encoded
            assert!(!result.is_empty());
            
            // Try to deserialize back to verify it's valid BCS
            let decoded: Result<GravityDKGState, _> = bcs::from_bytes(&result);
            assert!(decoded.is_ok());
        }
    );
}
