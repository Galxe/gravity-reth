//! Relayer Manager for lifecycle management

use crate::{
    parser::UriParser,
    relayer::{GravityRelayer, ObserveState},
    ObservedValue,
};
use anyhow::{anyhow, Result};
use gravity_api_types::on_chain_config::jwks::JWKStruct;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

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
    pub async fn add_uri(&self, uri: &str, rpc_url: &str) -> Result<()> {
        {
            let relayers = self.relayers.read().await;
            if relayers.contains_key(rpc_url) {
                return Err(anyhow!("RPC URL {} is already being monitored", rpc_url));
            }
        }

        let task = self.uri_parser.parse(uri)?;
        info!("Adding URI: {} -> {:?}", uri, task);

        let relayer = GravityRelayer::new(rpc_url, task).await?;
        info!("Successfully added URI: {}, relayer: {:?}", uri, relayer);

        let mut relayers = self.relayers.write().await;
        relayers.insert(uri.to_string(), Arc::new(relayer));
        Ok(())
    }

    async fn poll_once(&self, uri: &str) -> Result<ObserveState> {
        let relayers = { self.relayers.read().await };
        let relayer =
            relayers.get(uri).ok_or(anyhow!("URI {} not found, relayers: {:?}", uri, relayers))?;
        match relayer.poll_once().await {
            Ok(observed_state) => match observed_state.observed_value {
                ObservedValue::None => Err(anyhow!("Fetched none")),
                _ => Ok(observed_state),
            },
            Err(e) => {
                error!("Error polling URI {}: {}", uri, e);
                Err(e)
            }
        }
    }

    /// Polls a specific URI for updates
    ///
    /// # Arguments
    /// * `uri` - The URI to poll for updates
    ///
    /// # Returns
    /// * `Result<ObserveState>` - The current observed state or error
    ///
    /// # Errors
    /// * Returns an error if the URI is not found in the managed relayers
    pub async fn poll_uri(&self, uri: &str) -> Result<Vec<JWKStruct>> {
        let relayers = { self.relayers.read().await };
        let relayer =
            relayers.get(uri).ok_or(anyhow!("URI {} not found, relayers: {:?}", uri, relayers))?;
        let observed_state = relayer.poll_once().await?;
        let jwk_struct = GravityRelayer::convert_specific_observed_value(observed_state).await?;
        Ok(jwk_struct)
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
