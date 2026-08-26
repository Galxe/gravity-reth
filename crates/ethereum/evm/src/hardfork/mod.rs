//! Gravity-specific hardfork state changes.
//!
//! Concrete hardfork modules should be added on the corresponding release
//! branch. The `common` module provides shared traits and types that hardfork
//! modules can implement.

pub mod common;
pub(crate) mod gamma;
pub mod testnet_owner_fix;
