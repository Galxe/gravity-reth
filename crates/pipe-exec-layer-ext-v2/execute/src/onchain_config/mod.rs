//! Onchain config extension for Gravity

#![allow(missing_docs)]

pub mod base;
pub mod consensus_config;
pub mod epoch;
pub mod jwk_consensus_config;
pub mod metadata_txn;
pub mod observed_jwk;
pub mod types;
pub mod validator_set;

// Re-export main types for convenience
pub use base::{ConfigFetcher, OnchainConfigFetcher};
pub use consensus_config::ConsensusConfigFetcher;
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
pub const VALIDATOR_MANAGER_UTILS_ADDR: Address = address!("0000000000000000000000000000000000002014");
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
pub const SYSTEM_CONTRACT_ADDRESS: Address = address!("0000000000000000000000000000000000002000");
