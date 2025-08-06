//! Onchain config extension for Gravity

#![allow(missing_docs)]

pub mod base;
pub mod consensus_config;
pub mod epoch;
pub mod metadata_txn;
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
pub const GENESIS_ADDR: Address = address!("0000000000000000000000000000000000001008");
pub const SYSTEM_CALLER: Address = address!("0000000000000000000000000000000000000000");
pub const PERFORMANCE_TRACKER_ADDR: Address = address!("00000000000000000000000000000000000000f1");
pub const EPOCH_MANAGER_ADDR: Address = address!("00000000000000000000000000000000000000f3");
pub const STAKE_CONFIG_ADDR: Address = address!("0000000000000000000000000000000000002008");
pub const DELEGATION_ADDR: Address = address!("0000000000000000000000000000000000002009");
pub const VALIDATOR_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002010");
pub const VALIDATOR_PERFORMANCE_TRACKER_ADDR: Address =
    address!("000000000000000000000000000000000000200b");
pub const BLOCK_ADDR: Address = address!("0000000000000000000000000000000000002001");
pub const TIMESTAMP_ADDR: Address = address!("0000000000000000000000000000000000002004");
pub const JWK_MANAGER_ADDR: Address = address!("0000000000000000000000000000000000002002");
pub const KEYLESS_ACCOUNT_ADDR: Address = address!("000000000000000000000000000000000000200A");
pub const SYSTEM_REWARD_ADDR: Address = address!("0000000000000000000000000000000000001002");
pub const GOV_HUB_ADDR: Address = address!("0000000000000000000000000000000000001007");
pub const STAKE_CREDIT_ADDR: Address = address!("0000000000000000000000000000000000002003");
pub const GOV_TOKEN_ADDR: Address = address!("0000000000000000000000000000000000002005");
pub const GOVERNOR_ADDR: Address = address!("0000000000000000000000000000000000002006");
pub const TIMELOCK_ADDR: Address = address!("0000000000000000000000000000000000002007");
