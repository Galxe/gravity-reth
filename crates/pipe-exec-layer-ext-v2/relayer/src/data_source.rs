//! Oracle Data Source Abstraction
//!
//! This module provides the core trait and enum for all oracle data sources.
//! The extensible design allows adding new source types without modifying existing code.

use alloy_primitives::{Bytes, U256};
use anyhow::Result;
use async_trait::async_trait;

use crate::blockchain_source::BlockchainEventSource;

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
}

/// Extensible enum for runtime dispatch of data sources
///
/// New source types can be added by:
/// 1. Adding a new variant here
/// 2. Implementing the source struct
/// 3. Adding a case in DataSourceFactory
#[derive(Debug)]
pub enum DataSourceKind {
    /// Blockchain cross-chain events (sourceType=0)
    Blockchain(BlockchainEventSource),
}

#[async_trait]
impl OracleDataSource for DataSourceKind {
    fn source_type(&self) -> u32 {
        match self {
            DataSourceKind::Blockchain(_) => source_types::BLOCKCHAIN,
        }
    }

    fn source_id(&self) -> U256 {
        match self {
            DataSourceKind::Blockchain(s) => s.source_id(),
        }
    }

    async fn poll(&self) -> Result<Vec<OracleData>> {
        match self {
            DataSourceKind::Blockchain(s) => s.poll().await,
        }
    }
}
