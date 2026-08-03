//! Oracle source progress fetcher.
//!
//! New NativeOracle deployments expose one fixed-size progress checkpoint per
//! source. The legacy record fallback keeps pre-hardfork blocks readable.

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
use gravity_api_types::on_chain_config::oracle_state::OracleSourceState;
use reth_rpc_eth_api::{helpers::EthCall, RpcTypes};
use tracing::{info, warn};

sol! {
    struct SourceProgress {
        uint128 latestNonce;
        uint128 latestPosition;
    }

    function getSourceProgress(
        uint32 sourceType,
        uint256 sourceId
    ) external view returns (SourceProgress memory progress);

    /// Legacy record retained for pre-hardfork state fallback.
    struct DataRecord {
        uint64 recordedAt;
        uint256 blockNumber;
        bytes data;
    }

    function getRecord(
        uint32 sourceType,
        uint256 sourceId,
        uint128 nonce
    ) external view returns (DataRecord memory record);
}

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

    fn call_get_source_progress(
        &self,
        source_type: u32,
        source_id: U256,
        block_id: BlockId,
    ) -> Option<SourceProgress> {
        let call = getSourceProgressCall { sourceType: source_type, sourceId: source_id };
        let input: Bytes = call.abi_encode().into();
        let result =
            self.base_fetcher.eth_call(SYSTEM_CALLER, NATIVE_ORACLE_ADDR, input, block_id).ok()?;
        getSourceProgressCall::abi_decode_returns(&result).ok()
    }

    fn call_get_record(
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

    /// Read the fixed-size getter, falling back to legacy nonce and record state.
    fn try_fetch_source_progress(
        &self,
        task_client: &OracleTaskClient<'_, EthApi>,
        source_type: u32,
        source_id: U256,
        block_id: BlockId,
    ) -> Option<(u128, u128)> {
        if let Some(progress) = self.call_get_source_progress(source_type, source_id, block_id) {
            return Some((progress.latestNonce, progress.latestPosition));
        }

        let latest_nonce = task_client.call_get_latest_nonce(source_type, source_id, block_id)?;
        if latest_nonce == 0 {
            return Some((0, 0));
        }

        let record = self.call_get_record(source_type, source_id, latest_nonce, block_id)?;
        legacy_progress(latest_nonce, &record)
    }

    /// Fetch all currently supported relayer source states as a BCS snapshot.
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
                let progress =
                    self.try_fetch_source_progress(&task_client, source_type, source_id, block_id);
                if progress.is_none() {
                    warn!(
                        target: "oracle_state",
                        source_type,
                        source_id = source_id.to_string(),
                        "Failed to fetch or decode oracle source progress"
                    );
                }
                progress
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

    pub fn fetch_source_state(
        &self,
        source_type: u32,
        source_id: U256,
        block_id: BlockId,
    ) -> Option<OracleSourceState> {
        let task_client = OracleTaskClient::new(self.base_fetcher);
        let (latest_nonce, latest_position) =
            self.try_fetch_source_progress(&task_client, source_type, source_id, block_id)?;

        Some(OracleSourceState {
            source_type,
            source_id: source_id.try_into().ok()?,
            latest_nonce,
            latest_position,
        })
    }
}

fn legacy_progress(latest_nonce: u128, record: &DataRecord) -> Option<(u128, u128)> {
    if record.recordedAt == 0 {
        return Some((latest_nonce, 0));
    }

    Some((latest_nonce, record.blockNumber.try_into().ok()?))
}

fn collect_source_states<SourceIds, Progress>(
    source_types: &[u32],
    mut source_ids_for: SourceIds,
    mut progress_for: Progress,
) -> Option<Vec<OracleSourceState>>
where
    SourceIds: FnMut(u32) -> Option<Vec<U256>>,
    Progress: FnMut(u32, U256) -> Option<(u128, u128)>,
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
            let (latest_nonce, latest_position) = progress_for(*source_type, source_id)?;
            info!(
                target: "oracle_state",
                source_type,
                source_id = source_id.to_string(),
                latest_nonce,
                latest_position,
                "Fetched oracle source state"
            );

            results.push(OracleSourceState {
                source_type: *source_type,
                source_id: source_id.try_into().ok()?,
                latest_nonce,
                latest_position,
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
            |_, _| panic!("progress fetch must not run without sources"),
        )
        .expect("empty source lists are authoritative");
        assert!(states.is_empty());
    }

    #[test]
    fn source_id_read_failure_invalidates_whole_snapshot() {
        let calls = Cell::new(0);
        let states = collect_source_states(
            &[0, 3],
            |_| {
                let call = calls.get();
                calls.set(call + 1);
                (call != 1).then(Vec::new)
            },
            |_, _| panic!("progress fetch must not run without sources"),
        );
        assert!(states.is_none());
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn progress_read_failure_invalidates_whole_snapshot() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states = collect_source_states(&[source_type], |_| Some(vec![source_id]), |_, _| None);
        assert!(states.is_none());
    }

    #[test]
    fn progress_is_included_without_payload_history() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states = collect_source_states(
            &[source_type],
            |_| Some(vec![source_id]),
            |_, _| Some((7, 12_345)),
        )
        .expect("progress is authoritative");

        assert_eq!(states.len(), 1);
        assert_eq!(states[0].source_type, source_type);
        assert_eq!(states[0].source_id, 42);
        assert_eq!(states[0].latest_nonce, 7);
        assert_eq!(states[0].latest_position, 12_345);
    }

    #[test]
    fn legacy_record_without_payload_history_marks_position_unknown() {
        let record = DataRecord { recordedAt: 0, blockNumber: U256::ZERO, data: Bytes::new() };
        assert_eq!(legacy_progress(7, &record), Some((7, 0)));
    }

    #[test]
    fn zero_progress_is_valid() {
        let source_type = RELAYER_BACKED_SOURCE_TYPES[0];
        let source_id = U256::from(42);
        let states =
            collect_source_states(&[source_type], |_| Some(vec![source_id]), |_, _| Some((0, 0)))
                .expect("zero progress is a valid empty source state");

        assert_eq!(states[0].latest_nonce, 0);
        assert_eq!(states[0].latest_position, 0);
    }
}
