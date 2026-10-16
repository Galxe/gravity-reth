//! Gravity-specific hardfork state changes.
//!
//! Concrete hardfork modules should be added on the corresponding release
//! branch. The `common` module provides shared traits and types that hardfork
//! modules can implement.

pub mod alpha;
pub mod common;
pub mod eip_2935;
pub(crate) mod gamma;
pub mod oracle_task_install;
pub mod testnet_owner_fix;
