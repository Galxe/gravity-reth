//! Relayer Manager for lifecycle management

use crate::{parser::UriParser, relayer::GravityRelayer};
use anyhow::{anyhow, Result};
use gravity_api_types::relayer::PollResult;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Manages multiple Gravity relayers and their lifecycle
///
/// This struct provides centralized management for multiple relayers,
/// allowing addition, removal, and polling of URIs across different RPC endpoints.
#[derive(Debug)]
pub struct RelayerManager {
    uri_parser: UriParser,
    relayers: Arc<RwLock<HashMap<String, Arc<GravityRelayer>>>>,
}

impl RelayerManager {
    /// Creates a new RelayerManager instance
    ///
    /// Returns a new RelayerManager with an empty relayer map and a new URI parser.
    pub fn new() -> Self {
        Self { uri_parser: UriParser::new(), relayers: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Adds a new URI to be monitored by the relayer
    ///
    /// # Arguments
    /// * `uri` - The gravity protocol URI to monitor
    /// * `rpc_url` - The RPC endpoint URL for the blockchain
    /// * `last_state` - The last observed state to start monitoring from
    ///
    /// # Returns
    /// * `Result<()>` - Success or error if the URI cannot be added
    ///
    /// # Errors
    /// * Returns an error if the RPC URL is already being monitored
    /// * Returns an error if the URI cannot be parsed
    /// * Returns an error if the relayer cannot be created
    pub async fn add_uri(&self, uri: &str, rpc_url: &str, from_block: u64) -> Result<()> {
        {
            let relayers = self.relayers.read().await;
            if relayers.contains_key(rpc_url) {
                return Err(anyhow!("RPC URL {} is already being monitored", rpc_url));
            }
        }

        let task = self.uri_parser.parse(uri)?;
        info!("Adding URI: {} -> {:?}", uri, task);

        let relayer = GravityRelayer::new(rpc_url, task, from_block).await?;
        info!("Successfully added URI: {}, relayer: {:?}", uri, relayer);

        let mut relayers = self.relayers.write().await;
        relayers.insert(uri.to_string(), Arc::new(relayer));
        Ok(())
    }

    /// Polls a specific URI for updates
    ///
    /// # Arguments
    /// * `uri` - The URI to poll for updates
    ///
    /// # Returns
    /// * `Result<PollResult>` - The poll result containing JWK structures and max block number
    ///
    /// # Errors
    /// * Returns an error if the URI is not found in the managed relayers
    pub async fn poll_uri(&self, uri: &str) -> Result<PollResult> {
        let relayers = { self.relayers.read().await };
        let relayer =
            relayers.get(uri).ok_or(anyhow!("URI {} not found, relayers: {:?}", uri, relayers))?;
        let poll_result = relayer.poll_once().await?;
        let jwk_struct =
            GravityRelayer::convert_specific_observed_value(poll_result.observed_state.clone())
                .await?;
        Ok(PollResult {
            jwk_structs: jwk_struct,
            max_block_number: poll_result.max_queried_block,
            updated: poll_result.updated,
        })
    }
}

/// Statistics for the relayer manager
#[derive(Debug, Clone)]
pub struct ManagerStats {
    /// Total number of URIs being monitored
    pub total_uris: usize,
    /// Number of active URIs
    pub active_uris: usize,
}

/// Implements Drop trait for graceful shutdown
impl Drop for RelayerManager {
    fn drop(&mut self) {
        // Note: Cannot use async code in Drop trait
        // This is just for logging, actual cleanup should be done in graceful_shutdown
        debug!("RelayerManager is being dropped");
    }
}
