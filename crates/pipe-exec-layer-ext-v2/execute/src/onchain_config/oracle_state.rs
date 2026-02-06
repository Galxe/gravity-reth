//! Oracle State Fetcher
//!
//! This module provides functionality to fetch the latest DataRecord from NativeOracle
//! for registered oracle tasks. It builds on top of oracle_task_helpers to get source
//! information and then fetches the corresponding data records.

use super::{
    base::OnchainConfigFetcher,
    oracle_task_helpers::{OracleTaskClient, SOURCE_TYPE_BLOCKCHAIN},
    NATIVE_ORACLE_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Bytes, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::oracle_state::{LatestDataRecord, OracleSourceState};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::info;

// =============================================================================
// ABI Definitions
// =============================================================================

sol! {
    /// DataRecord struct matching INativeOracle.DataRecord
    struct DataRecord {
        /// Timestamp when this was recorded (0 = not exists)
        uint64 recordedAt;
        /// Block number when this was created
        uint256 blockNumber;
        /// Stored payload data
        bytes data;
    }

    /// Get a record by its key tuple
    function getRecord(
        uint32 sourceType,
        uint256 sourceId,
        uint128 nonce
    ) external view returns (DataRecord memory record);
}

// =============================================================================
// Oracle State Client
// =============================================================================

/// Client for fetching oracle state (latest records) from NativeOracle
#[derive(Debug)]
pub struct OracleStateFetcher<'a, EthApi> {
    base_fetcher: &'a OnchainConfigFetcher<EthApi>,
}

impl<'a, EthApi> OracleStateFetcher<'a, EthApi>
where
    EthApi: EthCall,
    EthApi::NetworkTypes: RpcTypes<TransactionRequest = TransactionRequest>,
{
    pub const fn new(base_fetcher: &'a OnchainConfigFetcher<EthApi>) -> Self {
        Self { base_fetcher }
    }

    /// Call NativeOracle.getRecord() to fetch a specific record
    pub fn call_get_record(
        &self,
        source_type: u32,
        source_id: U256,
        nonce: u128,
        block_id: BlockId,
    ) -> Option<DataRecord> {
        let call = getRecordCall { sourceType: source_type, sourceId: source_id, nonce };
        let input: Bytes = call.abi_encode().into();

        let result =
            self.base_fetcher.eth_call(SYSTEM_CALLER, NATIVE_ORACLE_ADDR, input, block_id).ok()?;

        getRecordCall::abi_decode_returns(&result).ok()
    }

    /// Fetch the latest DataRecord for a source
    ///
    /// Uses the latest nonce to fetch the corresponding record.
    /// Returns None if nonce is 0 (no records exist).
    pub fn fetch_latest_record(
        &self,
        source_type: u32,
        source_id: U256,
        latest_nonce: u128,
        block_id: BlockId,
    ) -> Option<LatestDataRecord> {
        if latest_nonce == 0 {
            return None;
        }

        let record = self.call_get_record(source_type, source_id, latest_nonce, block_id)?;

        // Check if record exists (recordedAt > 0)
        if record.recordedAt == 0 {
            return None;
        }

        Some(LatestDataRecord {
            recorded_at: record.recordedAt,
            block_number: record.blockNumber.try_into().unwrap_or(0),
            data: record.data.to_vec(),
        })
    }

    /// Fetch all oracle source states for registered blockchain tasks
    ///
    /// Returns BCS-serialized OracleSourceStates for registered sources.
    pub fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let task_client = OracleTaskClient::new(self.base_fetcher);
        let mut results = Vec::new();

        // Get all registered blockchain source IDs
        let source_ids = task_client
            .fetch_registered_source_ids(SOURCE_TYPE_BLOCKCHAIN, block_id)
            .unwrap_or_default();

        info!(
            target: "oracle_state",
            source_count = source_ids.len(),
            "Fetching oracle source states"
        );

        for source_id in source_ids {
            // Get latest nonce for this source
            let latest_nonce = task_client
                .call_get_latest_nonce(SOURCE_TYPE_BLOCKCHAIN, source_id, block_id)
                .unwrap_or(0);

            // Fetch the latest record if nonce > 0
            let latest_record =
                self.fetch_latest_record(SOURCE_TYPE_BLOCKCHAIN, source_id, latest_nonce, block_id);

            info!(
                target: "oracle_state",
                source_id = source_id.to_string(),
                latest_nonce,
                has_record = latest_record.is_some(),
                "Fetched oracle source state"
            );

            results.push(OracleSourceState {
                source_type: SOURCE_TYPE_BLOCKCHAIN,
                source_id: source_id.try_into().unwrap_or(0),
                latest_nonce: latest_nonce as u64,
                latest_record,
            });
        }

        Some(bcs::to_bytes(&results).expect("Failed to BCS serialize OracleSourceStates").into())
    }

    /// Fetch a specific source's state by source ID
    pub fn fetch_source_state(
        &self,
        source_type: u32,
        source_id: U256,
        block_id: BlockId,
    ) -> OracleSourceState {
        let task_client = OracleTaskClient::new(self.base_fetcher);

        let latest_nonce =
            task_client.call_get_latest_nonce(source_type, source_id, block_id).unwrap_or(0);

        let latest_record =
            self.fetch_latest_record(source_type, source_id, latest_nonce, block_id);

        OracleSourceState {
            source_type,
            source_id: source_id.try_into().unwrap_or(0),
            latest_nonce: latest_nonce as u64,
            latest_record,
        }
    }
}
