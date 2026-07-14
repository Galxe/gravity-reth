//! Oracle Relayer Manager
//!
//! Manages oracle data sources keyed by URI, matching gaptos JWKObserver interface.

use crate::{
    blockchain_source::BlockchainEventSource,
    data_source::{source_types, DataSourceKind, OracleDataSource},
    persistence::{load_state_if_exists, state_file_path, RelayerState, SourceState},
    polymarket_settlement_source::PolymarketSettlementSource,
    price_feed_source::PriceFeedSource,
    uri_parser::{parse_oracle_uri, ParsedOracleTask},
};
use anyhow::{anyhow, Result};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// Re-export types from gravity-api-types for external use
pub use gravity_api_types::{on_chain_config::jwks::JWKStruct, relayer::PollResult};

/// Startup scenario for determining initial source state
///
/// When a data source is added, we need to determine where to start scanning.
/// This enum captures the possible scenarios based on persisted and on-chain state.
#[derive(Debug)]
enum StartupScenario {
    /// Persisted state exists but is stale - fast-forward to on-chain state
    FastForward { onchain_nonce: u128, onchain_block: u64, persisted_nonce: u128 },
    /// Persisted state is ahead of NativeOracle, so it may only represent data
    /// fetched locally and not yet accepted on-chain. Rewind to the last
    /// confirmed on-chain record.
    RollbackToOnChain {
        onchain_nonce: u128,
        onchain_block: u64,
        persisted_nonce: u128,
        persisted_cursor: u64,
        restart_cursor: u64,
    },
    /// Persisted state is valid - use it for fast restart
    Restore { cursor: u64, nonce: u128 },
    /// No persisted state, but on-chain has data - sync from on-chain
    ColdStartWithSync { onchain_nonce: u128, onchain_block: u64 },
    /// No persisted state, no on-chain data - start from config default
    ColdStart { from_block: u64 },
}

impl StartupScenario {
    /// Determine which startup scenario applies based on persisted and on-chain state
    fn determine(
        persisted: Option<&SourceState>,
        onchain_nonce: u128,
        onchain_block: u64,
        default_from_block: u64,
    ) -> Self {
        match persisted {
            Some(state) if onchain_nonce > state.last_nonce as u128 => Self::FastForward {
                onchain_nonce,
                onchain_block,
                persisted_nonce: state.last_nonce as u128,
            },
            Some(state) if state.last_nonce as u128 > onchain_nonce => Self::RollbackToOnChain {
                onchain_nonce,
                onchain_block,
                persisted_nonce: state.last_nonce as u128,
                persisted_cursor: state.cursor_block,
                restart_cursor: if onchain_nonce > 0 { onchain_block } else { default_from_block },
            },
            Some(state) => {
                Self::Restore { cursor: state.cursor_block, nonce: state.last_nonce as u128 }
            }
            None if onchain_nonce > 0 => Self::ColdStartWithSync { onchain_nonce, onchain_block },
            None => Self::ColdStart { from_block: default_from_block },
        }
    }

    /// Get (cursor, nonce) for source initialization
    fn into_init_params(self) -> (u64, u128) {
        match self {
            Self::FastForward { onchain_nonce, onchain_block, .. } => {
                (onchain_block, onchain_nonce)
            }
            Self::RollbackToOnChain { onchain_nonce, restart_cursor, .. } => {
                (restart_cursor, onchain_nonce)
            }
            Self::Restore { cursor, nonce } => (cursor, nonce),
            Self::ColdStartWithSync { onchain_nonce, onchain_block } => {
                (onchain_block, onchain_nonce)
            }
            Self::ColdStart { from_block } => (from_block, 0),
        }
    }

    /// Log the startup scenario
    fn log(&self, uri: &str) {
        match self {
            Self::FastForward { onchain_nonce, onchain_block, persisted_nonce } => {
                warn!(
                    target: "oracle_manager",
                    uri,
                    persisted_nonce,
                    onchain_nonce,
                    onchain_block,
                    "Persisted state is stale, fast-forwarding to on-chain state"
                );
            }
            Self::RollbackToOnChain {
                onchain_nonce,
                onchain_block,
                persisted_nonce,
                persisted_cursor,
                restart_cursor,
            } => {
                warn!(
                    target: "oracle_manager",
                    uri,
                    persisted_nonce,
                    persisted_cursor,
                    onchain_nonce,
                    onchain_block,
                    restart_cursor,
                    "Persisted state is ahead of NativeOracle; rolling back to confirmed on-chain progress"
                );
            }
            Self::Restore { cursor, nonce } => {
                info!(
                    target: "oracle_manager",
                    uri,
                    persisted_nonce = nonce,
                    cursor_block = cursor,
                    "Using persisted state for fast restart"
                );
            }
            Self::ColdStartWithSync { onchain_nonce, onchain_block } => {
                info!(
                    target: "oracle_manager",
                    uri,
                    onchain_nonce,
                    onchain_block,
                    "Cold start with on-chain state"
                );
            }
            Self::ColdStart { .. } => {
                info!(target: "oracle_manager", uri, "Cold start from config (nonce=0)");
            }
        }
    }
}

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

impl OracleRelayerManager {
    /// Create a new OracleRelayerManager with optional persistence
    ///
    /// # Arguments
    /// * `datadir` - Optional path to data directory for state persistence
    pub fn new(datadir: PathBuf) -> Self {
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

        // Determine startup scenario based on persisted and on-chain state
        let scenario = {
            let state = self.state.read().await;
            StartupScenario::determine(
                state.get(uri),
                onchain_nonce,
                onchain_block_number,
                task.from_block(),
            )
        };
        scenario.log(uri);
        let (start_cursor, start_nonce) = scenario.into_init_params();

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
            source_types::PRICE_FEED => {
                let source = PriceFeedSource::from_task_with_reconciled_cursor(
                    task,
                    latest_onchain_nonce,
                    Some(rpc_url),
                    persisted_cursor,
                )?;
                Ok(DataSourceKind::PriceFeed(source))
            }
            source_types::POLYMARKET_SETTLEMENT => {
                let source = PolymarketSettlementSource::from_task(
                    task,
                    rpc_url,
                    latest_onchain_nonce,
                    persisted_cursor.unwrap_or(task.from_block()),
                )
                .await?;
                Ok(DataSourceKind::PolymarketSettlement(source))
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

        // Reconcile with on-chain state before polling.
        if let (Some(onchain_nonce), Some(onchain_block)) = (onchain_nonce, onchain_block_number) {
            let current_nonce = source.last_nonce().await.unwrap_or(0);
            if onchain_nonce > current_nonce {
                info!(
                    target: "oracle_manager",
                    uri,
                    local_nonce = current_nonce,
                    onchain_nonce,
                    onchain_block,
                    "On-chain state is ahead of local source; fast-forwarding"
                );
                source.fast_forward(onchain_nonce, onchain_block).await;
            }
        }

        let data = source.poll().await?;

        // Get nonce, cursor, and source info
        let nonce = source.last_nonce().await;
        let last_nonce_block = source.last_nonce_block().await;
        let max_block_number = source.cursor();
        let source_type = source.source_type();
        let source_id = source.source_id_u64();

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
                n,
                last_nonce_block.unwrap_or(0),
                max_block_number,
            )
            .await;
        }

        Ok(PollResult { jwk_structs, max_block_number, nonce, updated })
    }

    /// Update in-memory state and persist to disk.
    ///
    /// Writes to disk first (via a temporary clone) so that on the success
    /// path the on-disk state is never behind in-memory state. This state
    /// tracks data returned to consensus, not data accepted by `NativeOracle`;
    /// startup reconciliation rolls back any checkpoint that is ahead of the
    /// confirmed on-chain nonce. If the disk write fails, in-memory state is
    /// still advanced to avoid duplicate delivery in the running process.
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

        // Build a candidate state with the update applied, without mutating
        // the live state yet.
        let mut candidate = state.clone();
        candidate.update(uri, source_type, source_id, last_nonce, last_nonce_block, cursor_block);

        let path = state_file_path(&self.datadir);
        if let Err(e) = candidate.save(&path) {
            warn!(
                target: "oracle_manager",
                error = ?e,
                "Failed to persist relayer state; a crash may replay events from the last checkpoint"
            );
        }

        // Always commit to memory so the running process does not re-deliver.
        *state = candidate;
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

    fn price_uri() -> &'static str {
        "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:2000:10000000000:1,source-b:2000:10200000000:2,source-c:2000:9800000000:1"
    }

    fn polymarket_uri() -> &'static str {
        "gravity://6/42/polymarket_settlement?ctf=0x4D97DCd97eC945f40cF65F87097ACe5EA0476045&fromBlock=50000000&condition=0x1111111111111111111111111111111111111111111111111111111111111111"
    }

    #[tokio::test]
    async fn test_add_and_poll_price_feed_uri() {
        let datadir = tempfile::tempdir().unwrap();
        let manager = OracleRelayerManager::new(datadir.path().to_path_buf());

        manager.add_uri(price_uri(), "", 0, 0).await.unwrap();
        assert!(manager.has_uri(price_uri()).await);

        let first = manager.poll_uri(price_uri(), None, None).await.unwrap();
        assert!(first.updated);
        assert_eq!(first.nonce, Some(1));
        assert_eq!(first.max_block_number, 2010);
        assert_eq!(first.jwk_structs.len(), 1);
        assert_eq!(first.jwk_structs[0].type_name, source_types::PRICE_FEED.to_string());
        assert!(!first.jwk_structs[0].data.is_empty());

        let second = manager.poll_uri(price_uri(), None, None).await.unwrap();
        assert!(!second.updated);
        assert_eq!(second.jwk_structs.len(), 0);
    }

    #[tokio::test]
    async fn test_add_polymarket_settlement_uri() {
        let datadir = tempfile::tempdir().unwrap();
        let manager = OracleRelayerManager::new(datadir.path().to_path_buf());

        manager.add_uri(polymarket_uri(), "http://localhost:8545", 0, 0).await.unwrap();
        assert!(manager.has_uri(polymarket_uri()).await);
    }

    #[test]
    fn test_startup_rolls_back_persisted_state_ahead_of_onchain() {
        let mut state = RelayerState::new();
        state.update(
            polymarket_uri(),
            source_types::POLYMARKET_SETTLEMENT,
            42,
            5,
            50_000_010,
            50_000_020,
        );

        let scenario =
            StartupScenario::determine(state.get(polymarket_uri()), 3, 50_000_007, 50_000_000);
        let (cursor, nonce) = scenario.into_init_params();

        assert_eq!(cursor, 50_000_007);
        assert_eq!(nonce, 3);
    }

    #[test]
    fn test_startup_rolls_back_to_config_when_onchain_empty() {
        let mut state = RelayerState::new();
        state.update(
            polymarket_uri(),
            source_types::POLYMARKET_SETTLEMENT,
            42,
            2,
            50_000_010,
            50_000_020,
        );

        let scenario = StartupScenario::determine(state.get(polymarket_uri()), 0, 0, 50_000_000);
        let (cursor, nonce) = scenario.into_init_params();

        assert_eq!(cursor, 50_000_000);
        assert_eq!(nonce, 0);
    }
}
