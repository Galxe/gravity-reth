//! Onchain config extension for Gravity

#![allow(missing_docs)]

pub mod base;
pub mod consensus_config;
pub mod dkg;
pub mod epoch;
pub mod jwk_consensus_config;
pub mod metadata_txn;
pub mod observed_jwk;
pub mod types;
pub mod validator_set;

// Re-export main types for convenience
pub use base::{ConfigFetcher, OnchainConfigFetcher};
pub use consensus_config::ConsensusConfigFetcher;
pub use dkg::DKGStateFetcher;
pub use epoch::EpochFetcher;
pub use metadata_txn::{transact_metadata_contract_call, MetadataTxnResult};
pub use types::{
    convert_account, convert_validator_info, Commission, ValidatorInfo, ValidatorSet,
    ValidatorStatus,
};
pub use validator_set::ValidatorSetFetcher;

// Common addresses and constants
use alloy_primitives::{address, Address};

pub const GRAVITY_FRAMEWORK_ADDRESS: Address = address!("00000000000000000000000000000000000000ff");
pub const RECONFIGURATION_ADDRESS: Address = address!("00000000000000000000000000000000000000f0");
pub const BLOCK_MODULE_ADDRESS: Address = address!("00000000000000000000000000000000000000f1");
pub const CONSENSUS_CONFIG_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f2");
pub const VALIDATOR_SET_CONTRACT_ADDRESS: Address =
    address!("00000000000000000000000000000000000000f3");

pub const DEAD_ADDRESS: Address = address!("000000000000000000000000000000000000dEaD");
pub const SYSTEM_CALLER: Address = address!("0000000000000000000000000000000000002000");
pub const PERFORMANCE_TRACKER_ADDR: Address = address!("000000000000000000000000000000000000200f");
pub const EPOCH_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002010");
pub const STAKE_CONFIG_ADDR: Address = address!("0000000000000000000000000000000000002011");
pub const DELEGATION_ADDR: Address = address!("0000000000000000000000000000000000002012");
pub const VALIDATOR_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002013");
pub const VALIDATOR_MANAGER_UTILS_ADDR: Address =
    address!("0000000000000000000000000000000000002014");
pub const VALIDATOR_PERFORMANCE_TRACKER_ADDR: Address =
    address!("0000000000000000000000000000000000002015");
pub const BLOCK_ADDR: Address = address!("0000000000000000000000000000000000002016");
pub const TIMESTAMP_ADDR: Address = address!("0000000000000000000000000000000000002017");
pub const JWK_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002018");
pub const KEYLESS_ACCOUNT_ADDR: Address = address!("0000000000000000000000000000000000002019");
pub const SYSTEM_REWARD_ADDR: Address = address!("000000000000000000000000000000000000201A");
pub const GOV_HUB_ADDR: Address = address!("000000000000000000000000000000000000201b");
pub const STAKE_CREDIT_ADDR: Address = address!("000000000000000000000000000000000000201c");
pub const GOV_TOKEN_ADDR: Address = address!("000000000000000000000000000000000000201D");
pub const GOVERNOR_ADDR: Address = address!("000000000000000000000000000000000000201E");
pub const TIMELOCK_ADDR: Address = address!("000000000000000000000000000000000000201F");
pub const DKG_ADDR: Address = address!("0000000000000000000000000000000000002021");
pub const RECONFIGURATION_WITH_DKG_ADDR: Address =
    address!("0000000000000000000000000000000000002022");
pub const SYSTEM_CONTRACT_ADDRESS: Address = address!("0000000000000000000000000000000000002000");

// ============================================================================
// Validator Transactions Construction
// ============================================================================

use alloy_consensus::{EthereumTxEnvelope, TxEip4844, TxLegacy};
use alloy_primitives::{Bytes, Signature, U256};
use reth_ethereum_primitives::{Transaction, TransactionSigned};
use revm_primitives::TxKind;
use tracing::info;

/// Construct validator transactions envelope (JWK updates and DKG transcripts)
///
/// Uses ExtraDataType to construct appropriate transactions:
/// - `ExtraDataType::JWK` → `upsertObservedJWKs` transaction to JWK_MANAGER_ADDR
/// - `ExtraDataType::DKG` → `finishWithDkgResult` transaction to RECONFIGURATION_WITH_DKG_ADDR
pub fn construct_validator_txns_envelope(
    extra_data: &Vec<gravity_api_types::ExtraDataType>,
    system_caller_nonce: u64,
    gas_price: u128,
) -> Result<Vec<EthereumTxEnvelope<TxEip4844>>, String> {
    let system_caller_nonce = system_caller_nonce + 1;
    let mut txns = Vec::new();

    for (index, data) in extra_data.iter().enumerate() {
        let current_nonce = system_caller_nonce + index as u64;

        // Process data based on ExtraDataType variant
        match process_extra_data(data, current_nonce, gas_price) {
            Ok(transaction) => txns.push(transaction),
            Err(e) => {
                return Err(format!("Failed to process extra data at index {}: {}", index, e));
            }
        }
    }

    Ok(txns)
}

/// Process extra data based on its ExtraDataType variant
///
/// Supports:
/// - JWK updates (ExtraDataType::JWK)
/// - DKG transcripts (ExtraDataType::DKG)
fn process_extra_data(
    data: &gravity_api_types::ExtraDataType,
    nonce: u64,
    gas_price: u128,
) -> Result<TransactionSigned, String> {
    match data {
        gravity_api_types::ExtraDataType::JWK(data_bytes) => {
            // Deserialize as ProviderJWKs
            let provider_jwks = bcs::from_bytes::<
                gravity_api_types::on_chain_config::jwks::ProviderJWKs,
            >(data_bytes)
            .map_err(|e| format!("Failed to deserialize JWK data: {}", e))?;
            info!(
                "Processing JWK update for issuer: {}",
                String::from_utf8_lossy(&provider_jwks.issuer)
            );
            observed_jwk::construct_jwk_transaction(provider_jwks, nonce, gas_price)
        }
        gravity_api_types::ExtraDataType::DKG(data_bytes) => {
            // Deserialize as DKGTranscript
            let dkg_transcript = bcs::from_bytes::<
                gravity_api_types::on_chain_config::dkg::DKGTranscript,
            >(data_bytes)
            .map_err(|e| format!("Failed to deserialize DKG data: {}", e))?;
            info!("Processing DKG transcript for epoch: {}", dkg_transcript.metadata.epoch);
            dkg::construct_dkg_transaction(dkg_transcript, nonce, gas_price)
        }
    }
}

/// Create a new system call transaction
///
/// Helper function used by both JWK and DKG transaction construction
pub(crate) fn new_system_call_txn(
    contract: Address,
    nonce: u64,
    gas_price: u128,
    input: Bytes,
) -> TransactionSigned {
    TransactionSigned::new_unhashed(
        Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce,
            gas_price,
            gas_limit: 30_000_000,
            to: TxKind::Call(contract),
            value: U256::ZERO,
            input,
        }),
        Signature::new(U256::ZERO, U256::ZERO, false),
    )
}
