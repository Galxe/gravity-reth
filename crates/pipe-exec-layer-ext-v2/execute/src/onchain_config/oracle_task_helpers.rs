//! Shared Oracle Task Configuration helpers
//!
//! This module provides common ABI definitions and helper functions for interacting
//! with OracleTaskConfig and NativeOracle contracts. Used by both:
//! - `jwk_consensus_config.rs` (for JWK consensus provider discovery)
//! - `observed_jwk.rs` (for observed JWK data fetching)

use super::{
    base::OnchainConfigFetcher, NATIVE_ORACLE_ADDR, ORACLE_TASK_CONFIG_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Bytes, B256, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::{info, warn};

// =============================================================================
// Constants
// =============================================================================

/// Source type for blockchain events in NativeOracle
pub const SOURCE_TYPE_BLOCKCHAIN: u32 = 0;

// Re-export SOURCE_TYPE_JWK from types for consistency
pub use super::types::SOURCE_TYPE_JWK;

// =============================================================================
// Shared ABI Definitions
// =============================================================================

sol! {
    // -------------------- OracleTaskConfig Types --------------------

    /// Configuration for a continuous oracle task
    struct OracleTask {
        bytes config;
        uint64 updatedAt;
    }

    /// Get all registered source IDs for a given source type
    function getSourceIds(
        uint32 sourceType
    ) external view returns (uint256[] memory sourceIds);

    /// Get all task names for a (sourceType, sourceId) pair
    function getTaskNames(
        uint32 sourceType,
        uint256 sourceId
    ) external view returns (bytes32[] memory taskNames);

    /// Get an oracle task by its key tuple
    function getTask(
        uint32 sourceType,
        uint256 sourceId,
        bytes32 taskName
    ) external view returns (OracleTask memory task);

    // -------------------- NativeOracle Types --------------------

    /// Get the latest nonce for a source (used to determine current progress)
    function getLatestNonce(
        uint32 sourceType,
        uint256 sourceId
    ) external view returns (uint128 nonce);
}

// =============================================================================
// OracleTaskClient - Shared Helper for Contract Calls
// =============================================================================

/// Client for interacting with OracleTaskConfig and NativeOracle contracts
#[derive(Debug)]
pub struct OracleTaskClient<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> OracleTaskClient<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }

    /// Call OracleTaskConfig.getTask()
    pub fn call_get_task(
        &self,
        source_type: u32,
        source_id: U256,
        task_name: B256,
        block_id: BlockId,
    ) -> Option<OracleTask> {
        let call =
            getTaskCall { sourceType: source_type, sourceId: source_id, taskName: task_name };
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(SYSTEM_CALLER, ORACLE_TASK_CONFIG_ADDR, input, block_id)
            .ok()?;

        getTaskCall::abi_decode_returns(&result).ok()
    }

    /// Call OracleTaskConfig.getTaskNames()
    pub fn call_get_task_names(
        &self,
        source_type: u32,
        source_id: U256,
        block_id: BlockId,
    ) -> Option<Vec<B256>> {
        let call = getTaskNamesCall { sourceType: source_type, sourceId: source_id };
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(SYSTEM_CALLER, ORACLE_TASK_CONFIG_ADDR, input, block_id)
            .ok()?;

        getTaskNamesCall::abi_decode_returns(&result).ok()
    }

    /// Fetch registered source IDs from OracleTaskConfig for a given source type
    pub fn fetch_registered_source_ids(
        &self,
        source_type: u32,
        block_id: BlockId,
    ) -> Option<Vec<U256>> {
        let call = getSourceIdsCall { sourceType: source_type };
        let input: Bytes = call.abi_encode().into();

        let result = self
            .base_fetcher
            .eth_call(SYSTEM_CALLER, ORACLE_TASK_CONFIG_ADDR, input, block_id)
            .ok()?;

        getSourceIdsCall::abi_decode_returns(&result).ok()
    }

    /// Call NativeOracle.getLatestNonce() to get current progress
    pub fn call_get_latest_nonce(
        &self,
        source_type: u32,
        source_id: U256,
        block_id: BlockId,
    ) -> Option<u128> {
        let call = getLatestNonceCall { sourceType: source_type, sourceId: source_id };
        let input: Bytes = call.abi_encode().into();

        let result =
            self.base_fetcher.eth_call(SYSTEM_CALLER, NATIVE_ORACLE_ADDR, input, block_id).ok()?;

        getLatestNonceCall::abi_decode_returns(&result).ok()
    }

    /// Fetch blockchain task URIs with their nonces
    ///
    /// Returns a vector of (URI, nonce) tuples for all configured blockchain tasks.
    /// Returns tasks even when nonce is 0 (no data recorded yet) to enable discovery.
    pub fn fetch_blockchain_task_uris(&self, block_id: BlockId) -> Vec<(String, u128)> {
        let mut results = Vec::new();

        // Get all registered blockchain source IDs
        let source_ids =
            self.fetch_registered_source_ids(SOURCE_TYPE_BLOCKCHAIN, block_id).unwrap_or_default();

        info!(
            target: "oracle_task_helper",
            length = source_ids.len(),
            "oracle task source ids length"
        );

        for source_id in source_ids {
            self.process_source_tasks(source_id, block_id, &mut results);
        }

        results
    }

    /// Process all tasks for a single source ID
    fn process_source_tasks(
        &self,
        source_id: U256,
        block_id: BlockId,
        results: &mut Vec<(String, u128)>,
    ) {
        // Fetch the latest nonce for this source (0 if no data recorded yet)
        let nonce =
            self.call_get_latest_nonce(SOURCE_TYPE_BLOCKCHAIN, source_id, block_id).unwrap_or(0);

        info!(
            target: "oracle_task_helper",
            source_id = source_id.to_string(),
            nonce,
            "oracle task source id and nonce"
        );

        let Some(task_names) =
            self.call_get_task_names(SOURCE_TYPE_BLOCKCHAIN, source_id, block_id)
        else {
            return;
        };

        info!(
            target: "oracle_task_helper",
            length = task_names.len(),
            "oracle task task names length"
        );

        for task_name in task_names {
            self.process_single_task(source_id, task_name, nonce, block_id, results);
        }
    }

    /// Process a single task and add valid URI to results
    fn process_single_task(
        &self,
        source_id: U256,
        task_name: B256,
        nonce: u128,
        block_id: BlockId,
        results: &mut Vec<(String, u128)>,
    ) {
        info!(
            target: "oracle_task_helper",
            task_name = task_name.to_string(),
            "oracle task task name"
        );

        let Some(task) = self.call_get_task(SOURCE_TYPE_BLOCKCHAIN, source_id, task_name, block_id)
        else {
            return;
        };

        info!(
            target: "oracle_task_helper",
            task = task.config.to_string(),
            "oracle task task"
        );

        if task.config.is_empty() {
            return;
        }

        let uri_string = String::from_utf8_lossy(&task.config).to_string();
        info!(
            target: "oracle_task_helper",
            uri_string = uri_string,
            "oracle task uri string"
        );

        // Validate URI
        match reth_pipe_exec_layer_relayer::uri_parser::parse_oracle_uri(&uri_string) {
            Ok(_) => {
                results.push((uri_string, nonce));
            }
            Err(e) => {
                warn!(
                    target: "oracle_task_helper",
                    uri_string = uri_string,
                    error = %e,
                    "Failed to parse oracle URI"
                );
            }
        }
    }
}
