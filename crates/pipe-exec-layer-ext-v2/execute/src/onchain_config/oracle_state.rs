//! Oracle State Fetcher
//!
//! This module provides functionality to fetch the latest DataRecord from NativeOracle
//! for registered oracle tasks. It builds on top of oracle_task_helpers to get source
//! information and then fetches the corresponding data records.

use super::{
    base::OnchainConfigFetcher,
    oracle_task_helpers::{OracleTaskClient, RELAYER_BACKED_SOURCE_TYPES},
    NATIVE_ORACLE_ADDR, SYSTEM_CALLER,
};
use alloy_eips::BlockId;
use alloy_primitives::{Bytes, U256};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_macro::sol;
use alloy_sol_types::SolCall;
use gravity_api_types::on_chain_config::oracle_state::{LatestDataRecord, OracleSourceState};
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::{info, warn};

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
        self.try_fetch_latest_record(source_type, source_id, latest_nonce, block_id).flatten()
    }

    /// Fetch the latest record while preserving the distinction between a failed read and a
    /// successful read of an intentionally unstored record (`StorageSkipped`).
    fn try_fetch_latest_record(
        &self,
        source_type: u32,
        source_id: U256,
        latest_nonce: u128,
        block_id: BlockId,
    ) -> Option<Option<LatestDataRecord>> {
        if latest_nonce == 0 {
            return Some(None);
        }

        let record = self.call_get_record(source_type, source_id, latest_nonce, block_id)?;

        // Check if record exists (recordedAt > 0)
        if record.recordedAt == 0 {
            return Some(None);
        }

        Some(Some(LatestDataRecord {
            recorded_at: record.recordedAt,
            block_number: record.blockNumber.try_into().ok()?,
            data: record.data.to_vec(),
        }))
    }

    /// Fetch all oracle source states for registered relayer-backed tasks.
    ///
    /// Returns BCS-serialized OracleSourceStates for registered sources.
    pub fn fetch(&self, block_id: BlockId) -> Option<Bytes> {
        let task_client = OracleTaskClient::new(self.base_fetcher);
        let results = collect_source_states(
            RELAYER_BACKED_SOURCE_TYPES,
            |source_type| {
                let source_ids = task_client.fetch_registered_source_ids(source_type, block_id);
                if source_ids.is_none() {
                    warn!(
                        target: "oracle_state",
                        source_type,
                        "Failed to fetch or decode registered oracle source ids"
                    );
                }
                source_ids
            },
            |source_type, source_id| {
                let latest_nonce =
                    task_client.call_get_latest_nonce(source_type, source_id, block_id);
                if latest_nonce.is_none() {
                    warn!(
                        target: "oracle_state",
                        source_type,
                        source_id = source_id.to_string(),
                        "Failed to fetch or decode latest oracle nonce"
                    );
                }
                latest_nonce
            },
            |source_type, source_id, latest_nonce| {
                let latest_record =
                    self.try_fetch_latest_record(source_type, source_id, latest_nonce, block_id);
                if latest_record.is_none() {
                    warn!(
                        target: "oracle_state",
                        source_type,
                        source_id = source_id.to_string(),
                        latest_nonce,
                        "Failed to fetch or decode latest oracle record"
                    );
                }
                latest_record
            },
        )?;

        bcs::to_bytes(&results)
            .map(Bytes::from)
            .map_err(|error| {
                warn!(
                    target: "oracle_state",
                    %error,
                    "Failed to BCS serialize OracleSourceStates"
                );
            })
            .ok()
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
            latest_nonce,
            latest_record,
        }
    }
}

fn collect_source_states<SourceIds, LatestNonce, LatestRecord>(
    source_types: &[u32],
    mut source_ids_for: SourceIds,
    mut latest_nonce_for: LatestNonce,
    mut latest_record_for: LatestRecord,
) -> Option<Vec<OracleSourceState>>
where
    SourceIds: FnMut(u32) -> Option<Vec<U256>>,
    LatestNonce: FnMut(u32, U256) -> Option<u128>,
    LatestRecord: FnMut(u32, U256, u128) -> Option<Option<LatestDataRecord>>,
{
    let mut results = Vec::new();

    for source_type in source_types {
        let source_ids = source_ids_for(*source_type)?;

        info!(
            target: "oracle_state",
            source_type,
            source_count = source_ids.len(),
            "Fetching oracle source states"
        );

        for source_id in source_ids {
            let latest_nonce = latest_nonce_for(*source_type, source_id)?;
            let latest_record = if latest_nonce == 0 {
                None
            } else {
                latest_record_for(*source_type, source_id, latest_nonce)?
            };

            info!(
                target: "oracle_state",
                source_type,
                source_id = source_id.to_string(),
                latest_nonce,
                has_record = latest_record.is_some(),
                "Fetched oracle source state"
            );

            results.push(OracleSourceState {
                source_type: *source_type,
                source_id: source_id.try_into().ok()?,
                latest_nonce,
                latest_record,
            });
        }
    }

    Some(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn authoritative_empty_source_lists_are_valid() {
        let states = collect_source_states(
            RELAYER_BACKED_SOURCE_TYPES,
            |_| Some(vec![]),
            |_, _| panic!("nonce fetch must not run without sources"),
            |_, _, _| panic!("record fetch must not run without sources"),
        )
        .expect("empty source lists are authoritative");

        assert!(states.is_empty());
    }

    #[test]
    fn source_id_read_failure_invalidates_whole_snapshot() {
        let calls = Cell::new(0);
        let states = collect_source_states(
            RELAYER_BACKED_SOURCE_TYPES,
            |_| {
                let call = calls.get();
                calls.set(call + 1);
                (call != 1).then(Vec::new)
            },
            |_, _| panic!("nonce fetch must not run without sources"),
            |_, _, _| panic!("record fetch must not run without sources"),
        );

        assert!(states.is_none());
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn nonce_read_failure_invalidates_whole_snapshot() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states = collect_source_states(
            &[source_type],
            |_| Some(vec![source_id]),
            |_, _| None,
            |_, _, _| panic!("record fetch must not run after nonce failure"),
        );

        assert!(states.is_none());
    }

    #[test]
    fn latest_record_read_failure_invalidates_nonzero_nonce_snapshot() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states = collect_source_states(
            &[source_type],
            |_| Some(vec![source_id]),
            |_, _| Some(7),
            |_, _, _| None,
        );

        assert!(states.is_none());
    }

    #[test]
    fn storage_skipped_is_valid_for_nonzero_nonce() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states = collect_source_states(
            &[source_type],
            |_| Some(vec![source_id]),
            |_, _| Some(7),
            |_, _, _| Some(None),
        )
        .expect("an intentionally unstored record is an authoritative state");

        assert_eq!(states.len(), 1);
        assert_eq!(states[0].latest_nonce, 7);
        assert!(states[0].latest_record.is_none());
    }

    #[test]
    fn zero_nonce_is_valid_without_a_record() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states = collect_source_states(
            &[source_type],
            |_| Some(vec![source_id]),
            |_, _| Some(0),
            |_, _, _| panic!("record fetch must not run for nonce zero"),
        )
        .expect("zero nonce is a valid empty source state");

        assert_eq!(states.len(), 1);
        assert_eq!(states[0].source_type, source_type);
        assert_eq!(states[0].source_id, 42);
        assert_eq!(states[0].latest_nonce, 0);
        assert!(states[0].latest_record.is_none());
    }
}
