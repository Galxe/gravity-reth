//! Oracle Data Source Abstraction
//!
//! This module provides the core trait and enum for all oracle data sources.
//! The extensible design allows adding new source types without modifying existing code.

use alloy_primitives::{Bytes, U256};
use anyhow::Result;
use async_trait::async_trait;

use crate::{
    blockchain_source::BlockchainEventSource,
    polymarket_settlement_source::PolymarketSettlementSource, price_feed_source::PriceFeedSource,
};

/// Data returned by oracle data sources
///
/// This is the unified format for all data that flows into NativeOracle.
#[derive(Debug, Clone)]
pub struct OracleData {
    /// Strictly increasing nonce for this (sourceType, sourceId) pair
    /// - For Blockchain: MessageSent.nonce
    pub nonce: u128,

    /// ABI-encoded payload to be stored in NativeOracle
    pub payload: Bytes,
}

/// Trait for all oracle data sources
///
/// Each implementation represents a specific type of data source that validators
/// can poll for oracle data.
#[async_trait]
pub trait OracleDataSource: Send + Sync {
    /// Get the source type (corresponds to NativeOracle.sourceType)
    /// - 0: BLOCKCHAIN
    fn source_type(&self) -> u32;

    /// Get the source ID (corresponds to NativeOracle.sourceId)
    /// - For BLOCKCHAIN: chain ID
    fn source_id(&self) -> U256;

    /// Poll for new data
    ///
    /// Returns a list of (nonce, payload) pairs that should be recorded in NativeOracle.
    /// The implementation should track its own cursor to avoid returning duplicates.
    async fn poll(&self) -> Result<Vec<OracleData>>;
}

/// Source type constants (matching NativeOracle)
pub mod source_types {
    /// Blockchain cross-chain events (e.g., GravityPortal.MessageSent)
    pub const BLOCKCHAIN: u32 = 0;

    /// External price feed resolver payloads (sourceType=3)
    pub const PRICE_FEED: u32 = 3;

    /// Polygon Polymarket/CTF settlement mirror payloads
    pub const POLYMARKET_SETTLEMENT: u32 = 6;
}

/// Extensible enum for runtime dispatch of data sources
///
/// New source types can be added by:
/// 1. Adding a new variant here
/// 2. Implementing the source struct
/// 3. Adding a case in `OracleRelayerManager::create_source_from_task`
#[derive(Debug)]
pub enum DataSourceKind {
    /// Blockchain cross-chain events (sourceType=0)
    Blockchain(BlockchainEventSource),
    /// Binance index-price feed (sourceType=3)
    PriceFeed(PriceFeedSource),
    /// Polymarket CTF settlement observations (sourceType=6)
    PolymarketSettlement(PolymarketSettlementSource),
}

impl DataSourceKind {
    pub(crate) async fn last_nonce(&self) -> Option<u128> {
        match self {
            Self::Blockchain(source) => source.last_nonce().await,
            Self::PriceFeed(source) => source.last_nonce().await,
            Self::PolymarketSettlement(source) => source.last_nonce().await,
        }
    }

    pub(crate) async fn last_nonce_block(&self) -> Option<u64> {
        match self {
            Self::Blockchain(source) => source.last_nonce_block().await,
            Self::PriceFeed(source) => source.last_nonce_block().await,
            Self::PolymarketSettlement(source) => source.last_nonce_block().await,
        }
    }

    pub(crate) async fn fast_forward(&self, nonce: u128, block: u64) {
        match self {
            Self::Blockchain(source) => source.fast_forward(nonce, block).await,
            Self::PriceFeed(source) => source.fast_forward(nonce, block).await,
            Self::PolymarketSettlement(source) => source.fast_forward(nonce, block).await,
        }
    }

    pub(crate) fn cursor(&self) -> u64 {
        match self {
            Self::Blockchain(source) => source.cursor(),
            Self::PriceFeed(source) => source.cursor(),
            Self::PolymarketSettlement(source) => source.cursor(),
        }
    }

    pub(crate) fn source_id_u64(&self) -> u64 {
        match self {
            Self::Blockchain(source) => source.chain_id(),
            Self::PriceFeed(source) => source.feed_id(),
            Self::PolymarketSettlement(source) => source.mirror_id(),
        }
    }
}

#[async_trait]
impl OracleDataSource for DataSourceKind {
    fn source_type(&self) -> u32 {
        match self {
            DataSourceKind::Blockchain(_) => source_types::BLOCKCHAIN,
            DataSourceKind::PriceFeed(_) => source_types::PRICE_FEED,
            DataSourceKind::PolymarketSettlement(_) => source_types::POLYMARKET_SETTLEMENT,
        }
    }

    fn source_id(&self) -> U256 {
        match self {
            DataSourceKind::Blockchain(s) => s.source_id(),
            DataSourceKind::PriceFeed(s) => s.source_id(),
            DataSourceKind::PolymarketSettlement(s) => s.source_id(),
        }
    }

    async fn poll(&self) -> Result<Vec<OracleData>> {
        match self {
            DataSourceKind::Blockchain(s) => s.poll().await,
            DataSourceKind::PriceFeed(s) => s.poll().await,
            DataSourceKind::PolymarketSettlement(s) => s.poll().await,
        }
    }
}
