//! Oracle Relayer Manager
//!
//! Manages oracle data sources keyed by URI, matching gaptos JWKObserver interface.

use crate::{
    blockchain_source::BlockchainEventSource,
    data_source::{source_types, DataSourceKind, OracleDataSource},
    uri_parser::{parse_oracle_uri, ParsedOracleTask},
};
use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, info};

// Re-export types from gravity-api-types for external use
pub use gravity_api_types::{on_chain_config::jwks::JWKStruct, relayer::PollResult};

/// Oracle Relayer Manager
///
/// Manages data sources keyed by URI for per-observer polling.
#[derive(Debug, Default)]
pub struct OracleRelayerManager {
    /// Data sources keyed by URI
    sources: RwLock<HashMap<String, Arc<DataSourceKind>>>,
}

impl OracleRelayerManager {
    /// Create a new OracleRelayerManager
    pub fn new() -> Self {
        Self { sources: RwLock::new(HashMap::new()) }
    }

    /// Add a source by URI with on-chain nonce for warm-start
    ///
    /// # Arguments
    /// * `uri` - The oracle task URI
    /// * `rpc_url` - RPC endpoint URL
    /// * `latest_onchain_nonce` - Latest nonce from NativeOracle (for warm-start)
    pub async fn add_uri(
        &self,
        uri: &str,
        rpc_url: &str,
        latest_onchain_nonce: u128,
    ) -> Result<()> {
        {
            let sources = self.sources.read().await;
            if sources.contains_key(uri) {
                info!(target: "oracle_manager", uri = uri, "Source already exists, skipping");
                return Ok(());
            }
        }

        let task = parse_oracle_uri(uri)?;

        // Create source with the provided nonce for warm-start
        let source = self.create_source_from_task(&task, rpc_url, latest_onchain_nonce).await?;

        info!(
            target: "oracle_manager",
            uri = uri,
            source_type = task.source_type,
            source_id = task.source_id,
            latest_onchain_nonce = latest_onchain_nonce,
            "Added data source"
        );

        let mut sources = self.sources.write().await;
        sources.insert(uri.to_string(), Arc::new(source));
        Ok(())
    }

    async fn create_source_from_task(
        &self,
        task: &ParsedOracleTask,
        rpc_url: &str,
        latest_onchain_nonce: u128,
    ) -> Result<DataSourceKind> {
        match task.source_type {
            source_types::BLOCKCHAIN => {
                let portal_address = task.portal_address()?;
                let config_start_block = task.from_block();

                // Use new_with_discovery for robust initialization
                let source = BlockchainEventSource::new_with_discovery(
                    task.source_id,
                    rpc_url,
                    portal_address,
                    config_start_block,
                    latest_onchain_nonce,
                )
                .await?;

                Ok(DataSourceKind::Blockchain(source))
            }
            _ => Err(anyhow!("Unknown source type: {}", task.source_type)),
        }
    }

    /// Poll a source by URI
    pub async fn poll_uri(&self, uri: &str) -> Result<PollResult> {
        let sources = self.sources.read().await;
        let source = sources.get(uri).ok_or_else(|| anyhow!("Source not found: {}", uri))?;

        let data = source.poll().await?;

        let jwk_structs: Vec<JWKStruct> = data
            .iter()
            .map(|d| JWKStruct {
                type_name: source.source_type().to_string(),
                data: d.payload.to_vec(),
            })
            .collect();

        // Get nonce and cursor - use last_nonce for exactly-once semantics
        let (nonce, max_block_number) = match source.as_ref() {
            DataSourceKind::Blockchain(s) => (s.last_nonce().await.map(|n| n as u64), s.cursor()),
        };

        let updated = !data.is_empty();

        debug!(
            target: "oracle_manager",
            uri = uri,
            num_items = data.len(),
            max_block = max_block_number,
            nonce = ?nonce,
            updated = updated,
            "Poll completed"
        );

        Ok(PollResult { jwk_structs, max_block_number, nonce, updated })
    }

    /// Remove a source by URI
    pub async fn remove_uri(&self, uri: &str) -> Option<Arc<DataSourceKind>> {
        self.sources.write().await.remove(uri)
    }

    /// Get the number of registered sources
    pub async fn source_count(&self) -> usize {
        self.sources.read().await.len()
    }

    /// Check if a source exists by URI
    pub async fn has_uri(&self, uri: &str) -> bool {
        self.sources.read().await.contains_key(uri)
    }

    /// List all registered URIs
    pub async fn list_uris(&self) -> Vec<String> {
        self.sources.read().await.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_manager_creation() {
        let manager = OracleRelayerManager::new();
        assert_eq!(manager.source_count().await, 0);
    }
}
