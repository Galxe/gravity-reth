//! Oracle Relayer Manager
//!
//! Manages oracle data sources keyed by their full task URI.

use crate::{
    blockchain_source::BlockchainEventSource,
    data_source::{source_types, DataSourceKind, OracleDataSource},
    persistence::{load_state_if_exists, state_file_path, RelayerState, SourceState},
    price_feed_source::PriceFeedSource,
    uri_parser::{parse_oracle_uri, ParsedOracleTask},
};
use anyhow::{anyhow, Result};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

pub use gravity_api_types::{on_chain_config::jwks::JWKStruct, relayer::PollResult};

#[derive(Debug)]
enum StartupScenario {
    FastForward {
        onchain_nonce: u128,
        onchain_position: u64,
        persisted_nonce: u128,
    },
    RollbackToOnChain {
        onchain_nonce: u128,
        onchain_position: u64,
        persisted_nonce: u128,
        persisted_cursor: u64,
        restart_cursor: u64,
    },
    Restore {
        cursor: u64,
        nonce: u128,
        position: u64,
    },
    /// A legacy callback may have advanced `NativeOracle` nonce without storing
    /// the source position. Rescan from a safe watermark while filtering all
    /// observations at or below the authoritative on-chain nonce.
    RecoverUnknownPosition {
        cursor: u64,
        onchain_nonce: u128,
        persisted_nonce: Option<u128>,
    },
    ColdStartWithSync {
        onchain_nonce: u128,
        onchain_position: u64,
    },
    ColdStart {
        from_block: u64,
    },
}

impl StartupScenario {
    fn determine(
        persisted: Option<&SourceState>,
        onchain_nonce: u128,
        onchain_position: u64,
        default_from_block: u64,
    ) -> Result<Self> {
        if onchain_nonce == 0 && onchain_position != 0 {
            return Err(anyhow!(
                "Invalid NativeOracle progress: zero nonce has nonzero source position"
            ));
        }

        if onchain_nonce == 0 {
            return Ok(match persisted {
                Some(state) if state.last_nonce > 0 => Self::RollbackToOnChain {
                    onchain_nonce,
                    onchain_position,
                    persisted_nonce: state.last_nonce,
                    persisted_cursor: state.cursor_block,
                    restart_cursor: default_from_block,
                },
                Some(state) => Self::Restore { cursor: state.cursor_block, nonce: 0, position: 0 },
                None => Self::ColdStart { from_block: default_from_block },
            });
        }

        if onchain_position == 0 {
            return Ok(match persisted {
                Some(state) if state.last_nonce == onchain_nonce => Self::Restore {
                    cursor: state.cursor_block,
                    nonce: onchain_nonce,
                    position: state.last_nonce_block,
                },
                Some(state) if state.last_nonce < onchain_nonce => {
                    let cursor = if state.last_nonce > 0 && state.last_nonce_block > 0 {
                        state.last_nonce_block
                    } else {
                        default_from_block
                    };
                    Self::RecoverUnknownPosition {
                        cursor,
                        onchain_nonce,
                        persisted_nonce: Some(state.last_nonce),
                    }
                }
                Some(state) => Self::RecoverUnknownPosition {
                    cursor: default_from_block,
                    onchain_nonce,
                    persisted_nonce: Some(state.last_nonce),
                },
                None => Self::RecoverUnknownPosition {
                    cursor: default_from_block,
                    onchain_nonce,
                    persisted_nonce: None,
                },
            });
        }

        Ok(match persisted {
            Some(state) if onchain_nonce > state.last_nonce => Self::FastForward {
                onchain_nonce,
                onchain_position,
                persisted_nonce: state.last_nonce,
            },
            Some(state) if state.last_nonce > onchain_nonce => Self::RollbackToOnChain {
                onchain_nonce,
                onchain_position,
                persisted_nonce: state.last_nonce,
                persisted_cursor: state.cursor_block,
                restart_cursor: onchain_position,
            },
            Some(state) => Self::Restore {
                cursor: state.cursor_block,
                nonce: state.last_nonce,
                position: onchain_position,
            },
            None => Self::ColdStartWithSync { onchain_nonce, onchain_position },
        })
    }

    const fn into_init_params(self) -> (u64, u128, u64) {
        match self {
            Self::FastForward { onchain_nonce, onchain_position, .. } |
            Self::ColdStartWithSync { onchain_nonce, onchain_position } => {
                (onchain_position, onchain_nonce, onchain_position)
            }
            Self::RollbackToOnChain { onchain_nonce, onchain_position, restart_cursor, .. } => {
                (restart_cursor, onchain_nonce, onchain_position)
            }
            Self::Restore { cursor, nonce, position } => (cursor, nonce, position),
            Self::RecoverUnknownPosition { cursor, onchain_nonce, .. } => {
                (cursor, onchain_nonce, 0)
            }
            Self::ColdStart { from_block } => (from_block, 0, 0),
        }
    }

    fn log(&self, uri: &str) {
        match self {
            Self::FastForward { onchain_nonce, onchain_position, persisted_nonce } => warn!(
                target: "oracle_manager",
                uri,
                persisted_nonce,
                onchain_nonce,
                onchain_position,
                "Persisted state is stale; fast-forwarding to confirmed on-chain progress"
            ),
            Self::RollbackToOnChain {
                onchain_nonce,
                onchain_position,
                persisted_nonce,
                persisted_cursor,
                restart_cursor,
            } => warn!(
                target: "oracle_manager",
                uri,
                persisted_nonce,
                persisted_cursor,
                onchain_nonce,
                onchain_position,
                restart_cursor,
                "Persisted state is ahead of NativeOracle; rolling back to confirmed progress"
            ),
            Self::Restore { cursor, nonce, position } => info!(
                target: "oracle_manager",
                uri,
                persisted_nonce = nonce,
                cursor_block = cursor,
                source_position = position,
                "Using persisted state for restart"
            ),
            Self::RecoverUnknownPosition { cursor, onchain_nonce, persisted_nonce } => warn!(
                target: "oracle_manager",
                uri,
                cursor,
                onchain_nonce,
                ?persisted_nonce,
                "NativeOracle source position is unknown; replaying from a safe watermark"
            ),
            Self::ColdStartWithSync { onchain_nonce, onchain_position } => info!(
                target: "oracle_manager",
                uri,
                onchain_nonce,
                onchain_position,
                "Cold start from confirmed on-chain progress"
            ),
            Self::ColdStart { from_block } => info!(
                target: "oracle_manager",
                uri,
                from_block,
                "Cold start from task configuration"
            ),
        }
    }
}

#[derive(Debug)]
struct SourceEntry {
    source: Arc<DataSourceKind>,
    /// JWK observers may call one provider concurrently. Serialize a URI's
    /// cursor mutation so a scan range can only be emitted once locally.
    poll_lock: Mutex<()>,
}

#[derive(Debug)]
/// Owns configured relayer sources and their durable polling checkpoints.
pub struct OracleRelayerManager {
    sources: RwLock<HashMap<String, Arc<SourceEntry>>>,
    datadir: PathBuf,
    state: RwLock<RelayerState>,
}

impl OracleRelayerManager {
    /// Opens a manager rooted at `datadir`, restoring persisted source checkpoints when present.
    pub fn new(datadir: PathBuf) -> Self {
        let state = match load_state_if_exists(&datadir) {
            Some(state) => state,
            None => RelayerState::new(),
        };
        Self { sources: RwLock::new(HashMap::new()), datadir, state: RwLock::new(state) }
    }

    /// Registers a task URI and reconciles its local cursor with `NativeOracle` progress.
    pub async fn add_uri(
        &self,
        uri: &str,
        rpc_url: &str,
        onchain_nonce: u128,
        onchain_position: u128,
    ) -> Result<()> {
        if self.sources.read().await.contains_key(uri) {
            info!(target: "oracle_manager", uri, "Source already exists; skipping");
            return Ok(());
        }

        let task = parse_oracle_uri(uri)?;
        let onchain_position = u64::try_from(onchain_position)
            .map_err(|_| anyhow!("On-chain source position exceeds relayer u64 range"))?;

        let scenario = {
            let state = self.state.read().await;
            let persisted = matching_persisted_state(&state, uri, &task)?;
            StartupScenario::determine(
                persisted,
                onchain_nonce,
                onchain_position,
                task.from_block(),
            )?
        };
        scenario.log(uri);
        let (start_cursor, start_nonce, start_position) = scenario.into_init_params();

        let source = self
            .create_source_from_task(&task, rpc_url, start_nonce, start_position, start_cursor)
            .await?;
        let entry = Arc::new(SourceEntry { source: Arc::new(source), poll_lock: Mutex::new(()) });

        let mut sources = self.sources.write().await;
        if sources.contains_key(uri) {
            return Ok(());
        }
        sources.insert(uri.to_string(), entry);

        info!(
            target: "oracle_manager",
            uri,
            source_type = task.source_type,
            source_id = task.source_id,
            start_nonce,
            start_cursor,
            start_position,
            "Added data source"
        );
        Ok(())
    }

    async fn create_source_from_task(
        &self,
        task: &ParsedOracleTask,
        rpc_url: &str,
        latest_onchain_nonce: u128,
        latest_onchain_position: u64,
        cursor: u64,
    ) -> Result<DataSourceKind> {
        match task.source_type {
            source_types::BLOCKCHAIN => {
                let source = BlockchainEventSource::new_with_progress(
                    task.source_id,
                    rpc_url,
                    task.portal_address()?,
                    cursor,
                    latest_onchain_nonce,
                    latest_onchain_position,
                )
                .await?;
                Ok(DataSourceKind::Blockchain(source))
            }
            source_types::PRICE_FEED => {
                let source = PriceFeedSource::from_task_with_progress(
                    task,
                    latest_onchain_nonce,
                    latest_onchain_position,
                    Some(rpc_url),
                )?;
                Ok(DataSourceKind::PriceFeed(source))
            }
            _ => Err(anyhow!("Unknown source type: {}", task.source_type)),
        }
    }

    /// Polls one task URI and returns canonical payloads for JWK observation.
    pub async fn poll_uri(
        &self,
        uri: &str,
        onchain_nonce: Option<u128>,
        onchain_position: Option<u128>,
    ) -> Result<PollResult> {
        let entry = self
            .sources
            .read()
            .await
            .get(uri)
            .cloned()
            .ok_or_else(|| anyhow!("Source not found: {uri}"))?;
        let _poll_guard = entry.poll_lock.lock().await;
        let source = &entry.source;

        if let (Some(onchain_nonce), Some(onchain_position)) = (onchain_nonce, onchain_position) {
            let onchain_position = u64::try_from(onchain_position)
                .map_err(|_| anyhow!("On-chain source position exceeds relayer u64 range"))?;
            let current_nonce = source.last_nonce().await.unwrap_or(0);
            let current_position = source.last_nonce_position().await.unwrap_or(0);
            if onchain_nonce > current_nonce ||
                (onchain_nonce == current_nonce && onchain_position > 0 && current_position == 0)
            {
                info!(
                    target: "oracle_manager",
                    uri,
                    current_nonce,
                    onchain_nonce,
                    onchain_position,
                    "Reconciling local source with confirmed on-chain progress"
                );
                source.reconcile_progress(onchain_nonce, onchain_position).await?;
            }
        }

        let data = source.poll().await?;
        let nonce = source.last_nonce().await;
        let last_nonce_position = source.last_nonce_position().await;
        let cursor = source.cursor();
        let source_type = source.source_type();
        let source_id = source.source_id_u64();

        let jwk_structs = data
            .iter()
            .map(|data| JWKStruct {
                // This becomes UnsupportedJWK.id during observation. The SDK
                // execution adapter later restores the canonical Move type name.
                type_name: source.source_type().to_string(),
                data: data.payload.to_vec(),
            })
            .collect::<Vec<_>>();
        let updated = !data.is_empty();

        debug!(
            target: "oracle_manager",
            uri,
            num_items = data.len(),
            cursor,
            ?nonce,
            updated,
            "Poll completed"
        );

        // Persist empty scans too. Otherwise a source with no events repeats the
        // same finalized range after every restart.
        self.update_and_save_state(
            uri,
            source_type,
            source_id,
            nonce.unwrap_or(0),
            last_nonce_position.unwrap_or(0),
            cursor,
        )
        .await;

        Ok(PollResult { jwk_structs, max_block_number: cursor, nonce, updated })
    }

    async fn update_and_save_state(
        &self,
        uri: &str,
        source_type: u32,
        source_id: u64,
        last_nonce: u128,
        last_nonce_position: u64,
        cursor: u64,
    ) {
        let mut state = self.state.write().await;
        let mut candidate = state.clone();
        candidate.update(uri, source_type, source_id, last_nonce, last_nonce_position, cursor);

        if let Err(error) = candidate.save(&state_file_path(&self.datadir)) {
            warn!(
                target: "oracle_manager",
                ?error,
                "Failed to persist relayer state; a crash may replay the last checkpoint"
            );
        }

        // Keep the running process monotonic even if disk persistence failed.
        // Startup reconciliation treats this checkpoint as unconfirmed until
        // NativeOracle reports the same or a later nonce.
        *state = candidate;
    }

    /// Removes a configured task URI.
    pub async fn remove_uri(&self, uri: &str) -> Option<Arc<DataSourceKind>> {
        self.sources.write().await.remove(uri).map(|entry| entry.source.clone())
    }

    /// Returns the number of configured source URIs.
    pub async fn source_count(&self) -> usize {
        self.sources.read().await.len()
    }

    /// Returns whether a source URI is configured.
    pub async fn has_uri(&self, uri: &str) -> bool {
        self.sources.read().await.contains_key(uri)
    }

    /// Lists configured source URIs.
    pub async fn list_uris(&self) -> Vec<String> {
        self.sources.read().await.keys().cloned().collect()
    }
}

fn matching_persisted_state<'a>(
    state: &'a RelayerState,
    uri: &str,
    task: &ParsedOracleTask,
) -> Result<Option<&'a SourceState>> {
    let Some(persisted) = state.get(uri) else {
        return Ok(None);
    };
    if persisted.source_type != task.source_type || persisted.source_id != task.source_id {
        return Err(anyhow!("Persisted oracle source identity does not match configured URI"));
    }
    Ok(Some(persisted))
}

#[cfg(test)]
mod tests {
    use super::*;

    const URI: &str =
        "gravity://0/1/events?portal=0x0000000000000000000000000000000000000001&fromBlock=100";
    const PRICE_URI: &str =
        "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000000&decimals=8";

    fn state(last_nonce: u128, last_position: u64, cursor: u64) -> RelayerState {
        let mut state = RelayerState::new();
        state.update(URI, source_types::BLOCKCHAIN, 1, last_nonce, last_position, cursor);
        state
    }

    fn params(state: Option<&SourceState>, nonce: u128, position: u64) -> (u64, u128, u64) {
        StartupScenario::determine(state, nonce, position, 100).unwrap().into_init_params()
    }

    #[test]
    fn known_position_fast_forwards_stale_persistence() {
        let state = state(3, 130, 140);
        assert_eq!(params(state.get(URI), 5, 160), (160, 5, 160));
    }

    #[test]
    fn known_position_rolls_back_unconfirmed_local_data() {
        let state = state(7, 180, 200);
        assert_eq!(params(state.get(URI), 5, 160), (160, 5, 160));
    }

    #[test]
    fn empty_onchain_state_rolls_back_to_task_start() {
        let state = state(2, 120, 150);
        assert_eq!(params(state.get(URI), 0, 0), (100, 0, 0));
    }

    #[test]
    fn matching_checkpoint_preserves_scan_watermark() {
        let state = state(5, 160, 220);
        assert_eq!(params(state.get(URI), 5, 160), (220, 5, 160));
    }

    #[test]
    fn unknown_position_without_persistence_replays_from_task_start() {
        assert_eq!(params(None, 5, 0), (100, 5, 0));
    }

    #[test]
    fn unknown_position_uses_matching_local_checkpoint() {
        let state = state(5, 160, 220);
        assert_eq!(params(state.get(URI), 5, 0), (220, 5, 160));
    }

    #[test]
    fn unknown_position_rescans_from_last_locally_known_event_when_behind() {
        let state = state(3, 130, 220);
        assert_eq!(params(state.get(URI), 5, 0), (130, 5, 0));
    }

    #[test]
    fn unknown_position_with_empty_or_ahead_state_uses_task_start() {
        let empty = state(0, 0, 220);
        assert_eq!(params(empty.get(URI), 5, 0), (100, 5, 0));

        let ahead = state(7, 180, 220);
        assert_eq!(params(ahead.get(URI), 5, 0), (100, 5, 0));
    }

    #[test]
    fn rejects_inconsistent_zero_nonce_progress() {
        let error = StartupScenario::determine(None, 0, 1, 100).unwrap_err();
        assert!(error.to_string().contains("zero nonce"));
    }

    #[test]
    fn rejects_persisted_identity_mismatch() {
        let mut state = state(1, 110, 120);
        state.sources.get_mut(URI).unwrap().source_id = 2;
        let task = parse_oracle_uri(URI).unwrap();
        let error = matching_persisted_state(&state, URI, &task).unwrap_err();
        assert!(error.to_string().contains("identity"));
    }

    #[test]
    fn rejects_source_position_outside_runtime_range() {
        let position = u128::from(u64::MAX) + 1;
        assert!(u64::try_from(position).is_err());
    }

    #[tokio::test]
    async fn adds_binance_price_feed_without_network_access() {
        let datadir = tempfile::tempdir().unwrap();
        let manager = OracleRelayerManager::new(datadir.path().to_path_buf());

        manager.add_uri(PRICE_URI, "https://fapi.binance.com", 0, 0).await.unwrap();

        assert!(manager.has_uri(PRICE_URI).await);
    }

    #[tokio::test]
    async fn rejects_binance_history_mismatch_during_registration() {
        let datadir = tempfile::tempdir().unwrap();
        let manager = OracleRelayerManager::new(datadir.path().to_path_buf());

        let error = manager
            .add_uri(PRICE_URI, "https://fapi.binance.com", 2, 1_710_000_059_999)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("task history mismatch"));
    }

    #[tokio::test]
    async fn rejects_binance_history_mismatch_during_runtime_reconcile() {
        let datadir = tempfile::tempdir().unwrap();
        let manager = OracleRelayerManager::new(datadir.path().to_path_buf());
        manager.add_uri(PRICE_URI, "https://fapi.binance.com", 1, 1_710_000_059_999).await.unwrap();

        let error =
            manager.poll_uri(PRICE_URI, Some(2), Some(1_710_000_060_000)).await.unwrap_err();

        assert!(error.to_string().contains("task history mismatch"));
    }
}
