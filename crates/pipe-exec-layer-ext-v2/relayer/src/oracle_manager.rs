//! Oracle Relayer Manager
//!
//! Manages oracle data sources keyed by URI, matching gaptos JWKObserver interface.

use crate::{
    blockchain_source::BlockchainEventSource,
    data_source::{source_types, DataSourceKind, OracleDataSource},
    persistence::{load_state_if_exists, state_file_path, RelayerState},
    uri_parser::{parse_oracle_uri, ParsedOracleTask},
};
use anyhow::{anyhow, Result};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// Re-export types from gravity-api-types for external use
pub use gravity_api_types::{on_chain_config::jwks::JWKStruct, relayer::PollResult};

/// Oracle Relayer Manager
///
/// Manages data sources keyed by URI for per-observer polling.
/// Supports optional persistence for fast restart.
#[derive(Debug)]
pub struct OracleRelayerManager {
    /// Data sources keyed by URI
    sources: RwLock<HashMap<String, Arc<DataSourceKind>>>,
    datadir: PathBuf,
    /// In-memory state for persistence
    state: RwLock<RelayerState>,
}

impl Default for OracleRelayerManager {
    fn default() -> Self {
        Self::new(None)
    }
}

impl OracleRelayerManager {
    /// Create a new OracleRelayerManager with optional persistence
    ///
    /// # Arguments
    /// * `datadir` - Optional path to data directory for state persistence
    pub fn new(datadir: Option<PathBuf>) -> Self {
        let datadir = datadir.unwrap();
        let state = load_state_if_exists(&datadir).unwrap_or_else(RelayerState::new);

        Self { sources: RwLock::new(HashMap::new()), datadir, state: RwLock::new(state) }
    }

    /// Add a source by URI with on-chain state for warm-start
    ///
    /// If persistence is enabled and state exists for this URI, validates
    /// the persisted state against on-chain state. If on-chain is ahead,
    /// fast-forwards to on-chain state. Otherwise uses persisted state.
    ///
    /// # Arguments
    /// * `uri` - The oracle task URI
    /// * `rpc_url` - RPC endpoint URL
    /// * `onchain_nonce` - Latest nonce from NativeOracle
    /// * `onchain_block_number` - Block number where onchain_nonce was recorded
    pub async fn add_uri(
        &self,
        uri: &str,
        rpc_url: &str,
        onchain_nonce: u128,
        onchain_block_number: u64,
    ) -> Result<()> {
        {
            let sources = self.sources.read().await;
            if sources.contains_key(uri) {
                info!(target: "oracle_manager", uri = uri, "Source already exists, skipping");
                return Ok(());
            }
        }

        let task = parse_oracle_uri(uri)?;

        // Check persisted state and reconcile with on-chain state
        let (start_cursor, start_nonce) = {
            let state = self.state.read().await;
            if let Some(source_state) = state.get(uri) {
                // Compare persisted state with on-chain state
                if onchain_nonce > source_state.last_nonce as u128 {
                    // On-chain is ahead - fast-forward to on-chain state
                    warn!(
                        target: "oracle_manager",
                        uri = uri,
                        persisted_nonce = source_state.last_nonce,
                        onchain_nonce = onchain_nonce,
                        onchain_block = onchain_block_number,
                        "Persisted state is stale, fast-forwarding to on-chain state"
                    );
                    (onchain_block_number, onchain_nonce)
                } else {
                    // Persisted state is valid (same or ahead, though ahead shouldn't happen)
                    info!(
                        target: "oracle_manager",
                        uri = uri,
                        persisted_nonce = source_state.last_nonce,
                        cursor_block = source_state.cursor_block,
                        "Using persisted state for fast restart"
                    );
                    (source_state.cursor_block, source_state.last_nonce as u128)
                }
            } else {
                // No persisted state - use on-chain state or config default
                if onchain_nonce > 0 {
                    info!(
                        target: "oracle_manager",
                        uri = uri,
                        onchain_nonce = onchain_nonce,
                        onchain_block = onchain_block_number,
                        "Cold start with on-chain state"
                    );
                    (onchain_block_number, onchain_nonce)
                } else {
                    info!(
                        target: "oracle_manager",
                        uri = uri,
                        "Cold start from config (nonce=0)"
                    );
                    (task.from_block(), 0)
                }
            }
        };

        // Create source with reconciled state
        let source =
            self.create_source_from_task(&task, rpc_url, start_nonce, Some(start_cursor)).await?;

        info!(
            target: "oracle_manager",
            uri = uri,
            source_type = task.source_type,
            source_id = task.source_id,
            start_nonce = start_nonce,
            start_cursor = start_cursor,
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
        persisted_cursor: Option<u64>,
    ) -> Result<DataSourceKind> {
        match task.source_type {
            source_types::BLOCKCHAIN => {
                let portal_address = task.portal_address()?;
                let config_start_block = task.from_block();
                let source = BlockchainEventSource::new_with_cursor(
                    task.source_id,
                    rpc_url,
                    portal_address,
                    persisted_cursor.unwrap_or(config_start_block),
                    latest_onchain_nonce,
                )
                .await?;

                Ok(DataSourceKind::Blockchain(source))
            }
            _ => Err(anyhow!("Unknown source type: {}", task.source_type)),
        }
    }

    /// Poll a source by URI with optional on-chain state for reconciliation
    ///
    /// If on-chain state is provided and is ahead of local state, fast-forwards
    /// local state before polling. After polling, persists the updated state.
    ///
    /// # Arguments
    /// * `uri` - The oracle task URI to poll
    /// * `onchain_nonce` - Optional current on-chain nonce for reconciliation
    /// * `onchain_block_number` - Optional on-chain block for reconciliation
    pub async fn poll_uri(
        &self,
        uri: &str,
        onchain_nonce: Option<u128>,
        onchain_block_number: Option<u64>,
    ) -> Result<PollResult> {
        let sources = self.sources.read().await;
        let source = sources.get(uri).ok_or_else(|| anyhow!("Source not found: {}", uri))?;

        // Reconcile with on-chain state before polling
        if let (Some(onchain_nonce), Some(onchain_block)) = (onchain_nonce, onchain_block_number) {
            match source.as_ref() {
                DataSourceKind::Blockchain(s) => {
                    let current_nonce = s.last_nonce().await.unwrap_or(0);
                    if onchain_nonce > current_nonce {
                        info!(
                            target: "oracle_manager",
                            uri = uri,
                            local_nonce = current_nonce,
                            onchain_nonce = onchain_nonce,
                            onchain_block = onchain_block,
                            "On-chain ahead of local, fast-forwarding"
                        );
                        s.set_last_processed(onchain_nonce, onchain_block).await;
                        s.set_cursor(onchain_block);
                    }
                }
            }
        }

        let data = source.poll().await?;

        // Get nonce, cursor, and source info
        let (nonce, last_nonce_block, max_block_number, source_type, source_id) =
            match source.as_ref() {
                DataSourceKind::Blockchain(s) => (
                    s.last_nonce().await.map(|n| n as u64),
                    s.last_nonce_block().await,
                    s.cursor(),
                    source_types::BLOCKCHAIN,
                    s.chain_id(),
                ),
            };

        let jwk_structs: Vec<JWKStruct> = data
            .iter()
            .map(|d| JWKStruct {
                type_name: source.source_type().to_string(),
                data: d.payload.to_vec(),
            })
            .collect();

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

        if let Some(n) = nonce {
            self.update_and_save_state(
                uri,
                source_type,
                source_id,
                n as u128,
                last_nonce_block.unwrap_or(0),
                max_block_number,
            )
            .await;
        }

        Ok(PollResult { jwk_structs, max_block_number, nonce, updated })
    }

    /// Update in-memory state and persist to disk
    async fn update_and_save_state(
        &self,
        uri: &str,
        source_type: u32,
        source_id: u64,
        last_nonce: u128,
        last_nonce_block: u64,
        cursor_block: u64,
    ) {
        let mut state = self.state.write().await;
        state.update(uri, source_type, source_id, last_nonce, last_nonce_block, cursor_block);

        let path = state_file_path(&self.datadir);
        if let Err(e) = state.save(&path) {
            warn!(
                target: "oracle_manager",
                error = ?e,
                path = ?path,
                "Failed to persist relayer state"
            );
        }
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
        let manager = OracleRelayerManager::new(None);
        assert_eq!(manager.source_count().await, 0);
    }

    #[tokio::test]
    async fn test_manager_with_datadir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = OracleRelayerManager::new(Some(temp_dir.path().to_path_buf()));
        assert_eq!(manager.source_count().await, 0);
    }
}
