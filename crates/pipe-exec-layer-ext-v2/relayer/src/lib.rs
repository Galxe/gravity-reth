//! Gravity Protocol Relayer
//!
//! This crate provides functionality for parsing Gravity protocol URIs and relaying blockchain
//! events.

/// Ethereum HTTP client functionality
pub mod eth_client;
/// Relayer manager for coordinating multiple relayers
pub mod manager;
/// Core relayer implementation for gravity protocol tasks
pub mod relayer;

/// URI parser for gravity protocol tasks
pub mod parser;

pub use eth_client::EthHttpCli;
pub use manager::{ManagerStats, RelayerManager};
pub use parser::{AccountActivityType, GravityTask, ParsedTask, UriParser};
pub use relayer::{GravityRelayer, ObserveState, ObservedValue, DEPOSIT_GRAVITY_EVENT_SIGNATURE};
