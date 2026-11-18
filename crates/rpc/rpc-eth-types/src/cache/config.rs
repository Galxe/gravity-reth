//! Configuration for RPC cache.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use reth_rpc_server_types::constants::cache::{
    DEFAULT_BAL_CACHE_MAX_LEN, DEFAULT_BLOCK_CACHE_MAX_BYTES, DEFAULT_BLOCK_CACHE_MAX_LEN,
    DEFAULT_CACHE_IDLE_TIMEOUT, DEFAULT_CONCURRENT_DB_REQUESTS, DEFAULT_MAX_CACHED_TX_HASHES,
    DEFAULT_RECEIPT_CACHE_MAX_BYTES, DEFAULT_RECEIPT_CACHE_MAX_LEN,
};

/// Settings for the [`EthStateCache`](super::EthStateCache).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EthStateCacheConfig {
    /// Maximum estimated block payload bytes; zero disables block caching.
    pub max_blocks_bytes: usize,
    /// Maximum estimated receipt payload bytes; zero disables receipt caching.
    pub max_receipts_bytes: usize,
    /// Idle duration before eviction; zero disables expiration. Hits refresh the timeout.
    pub idle_timeout: Duration,
    /// Max number of blocks in cache.
    ///
    /// Default is 1000.
    pub max_blocks: u32,
    /// Max number of blocks' receipts in cache.
    ///
    /// Default is 500.
    pub max_receipts: u32,
    /// Max number of revm BALs in cache.
    ///
    /// Default is 1000.
    pub max_bals: u32,
    /// Max number of concurrent database requests.
    ///
    /// Default is 512.
    pub max_concurrent_db_requests: usize,
    /// Maximum number of transaction hashes to cache for transaction lookups.
    pub max_cached_tx_hashes: u32,
}

impl Default for EthStateCacheConfig {
    fn default() -> Self {
        Self {
            max_blocks_bytes: DEFAULT_BLOCK_CACHE_MAX_BYTES,
            max_receipts_bytes: DEFAULT_RECEIPT_CACHE_MAX_BYTES,
            idle_timeout: DEFAULT_CACHE_IDLE_TIMEOUT,
            max_blocks: DEFAULT_BLOCK_CACHE_MAX_LEN,
            max_receipts: DEFAULT_RECEIPT_CACHE_MAX_LEN,
            max_bals: DEFAULT_BAL_CACHE_MAX_LEN,
            max_concurrent_db_requests: DEFAULT_CONCURRENT_DB_REQUESTS,
            max_cached_tx_hashes: DEFAULT_MAX_CACHED_TX_HASHES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_and_custom_limits_round_trip() {
        let config: EthStateCacheConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, EthStateCacheConfig::default());
        let config = EthStateCacheConfig {
            max_blocks_bytes: 0,
            max_receipts_bytes: 1024,
            idle_timeout: Duration::from_millis(1500),
            ..Default::default()
        };
        assert_eq!(
            serde_json::from_value::<EthStateCacheConfig>(serde_json::to_value(config).unwrap())
                .unwrap(),
            config
        );
    }
}
