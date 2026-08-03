//! Gravity Protocol Relayer
//!
//! This crate provides functionality for monitoring external data sources and relaying
//! oracle data to the Gravity chain.
//!
//! ## Architecture
//!
//! The architecture uses trait + enum pattern for extensibility:
//! - `OracleDataSource` trait defines the interface
//! - `DataSourceKind` enum for runtime dispatch
//! - `OracleRelayerManager` for managing sources by URI

// ============================================================
// Modules
// ============================================================

/// Core data source abstraction
pub mod data_source;

/// Blockchain event source (GravityPortal.MessageSent)
pub mod blockchain_source;

/// Binance index-price feed source
pub mod price_feed_source;

/// Polygon Polymarket CTF settlement source
pub mod polymarket_settlement_source;

/// Factory for creating data sources
pub mod factory;

/// Oracle relayer manager (URI-keyed interface)
pub mod oracle_manager;

/// URI parser for extended gravity:// scheme
pub mod uri_parser;

/// Ethereum HTTP client functionality
pub mod eth_client;

/// State persistence for fast restart
pub mod persistence;

// ============================================================
// Re-exports
// ============================================================

pub use blockchain_source::BlockchainEventSource;
pub use data_source::{source_types, DataSourceKind, OracleData, OracleDataSource};
pub use eth_client::EthHttpCli;
pub use factory::DataSourceFactory;
pub use oracle_manager::{JWKStruct, OracleRelayerManager, PollResult};
pub use polymarket_settlement_source::PolymarketSettlementSource;
pub use price_feed_source::PriceFeedSource;
pub use uri_parser::{parse_oracle_uri, ParsedOracleTask};
