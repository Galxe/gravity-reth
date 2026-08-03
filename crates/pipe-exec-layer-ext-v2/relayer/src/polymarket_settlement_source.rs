//! Polymarket settlement mirror source.
//!
//! This source watches finalized Polygon logs from the Conditional Tokens
//! Framework (CTF) and mirrors `ConditionResolution` events into the same
//! UnsupportedJWK bytes path used by other Gravity oracle sources.

use crate::{
    data_source::{source_types, OracleData, OracleDataSource},
    eth_client::EthHttpCli,
    uri_parser::ParsedOracleTask,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rpc_types::{Filter, Log};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolEvent, SolValue};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, info};

sol! {
    /// CTF condition resolution emitted by Gnosis Conditional Tokens.
    event ConditionResolution(
        bytes32 indexed conditionId,
        address indexed oracle,
        bytes32 indexed questionId,
        uint256 outcomeSlotCount,
        uint256[] payoutNumerators
    );

    struct PolymarketSettlementPayloadSol {
        uint256 mirrorId;
        uint256 polygonChainId;
        address ctf;
        address oracle;
        bytes32 conditionId;
        bytes32 questionId;
        uint256 outcomeSlotCount;
        uint256[] payoutNumerators;
        bytes32 txHash;
        uint256 logIndex;
        uint8 settlementKind;
    }
}

const CTF_CONDITION_RESOLUTION: u8 = 1;
const DEFAULT_POLYGON_CHAIN_ID: u64 = 137;
const DEFAULT_MAX_BLOCKS_PER_POLL: u64 = 1_000;
const MAX_BLOCKS_PER_POLL: u64 = 10_000;
const MAX_OUTCOME_SLOT_COUNT: usize = 32;

/// Last CTF settlement returned to the caller.
#[derive(Debug, Clone, Copy, Default)]
struct LastSettlement {
    nonce: u128,
    block: u64,
}

impl LastSettlement {
    fn is_initialized(self) -> bool {
        self.nonce > 0
    }
}

/// A canonical CTF settlement observation decoded from Polygon logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolymarketSettlementObservation {
    /// NativeOracle sourceId for this mirror.
    pub mirror_id: u64,
    /// Source chain id, normally Polygon PoS mainnet `137`.
    pub polygon_chain_id: u64,
    /// CTF contract which emitted the settlement.
    pub ctf: Address,
    /// UMA adapter / oracle address recorded in CTF.
    pub oracle: Address,
    /// CTF condition id.
    pub condition_id: B256,
    /// UMA / CTF question id.
    pub question_id: B256,
    /// Number of outcome slots in the CTF condition.
    pub outcome_slot_count: U256,
    /// Final payout vector.
    pub payout_numerators: Vec<U256>,
    /// Transaction hash containing this settlement log.
    pub tx_hash: B256,
    /// Log index in the source block.
    pub log_index: u64,
    /// Source block number.
    pub block_number: u64,
}

impl PolymarketSettlementObservation {
    fn resolver_payload(&self) -> Vec<u8> {
        PolymarketSettlementPayloadSol {
            mirrorId: U256::from(self.mirror_id),
            polygonChainId: U256::from(self.polygon_chain_id),
            ctf: self.ctf,
            oracle: self.oracle,
            conditionId: self.condition_id,
            questionId: self.question_id,
            outcomeSlotCount: self.outcome_slot_count,
            payoutNumerators: self.payout_numerators.clone(),
            txHash: self.tx_hash,
            logIndex: U256::from(self.log_index),
            settlementKind: CTF_CONDITION_RESOLUTION,
        }
        .abi_encode()
    }

    fn wrapped_payload(&self, delivery_nonce: u128) -> Bytes {
        Bytes::from(SolValue::abi_encode(&(
            delivery_nonce,
            U256::from(self.block_number),
            self.resolver_payload().as_slice(),
        )))
    }
}

/// Polygon Polymarket settlement mirror for `sourceType=6`.
#[derive(Debug)]
pub struct PolymarketSettlementSource {
    mirror_id: u64,
    polygon_chain_id: u64,
    rpc_client: Arc<EthHttpCli>,
    ctf_address: Address,
    condition_id: B256,
    max_blocks_per_poll: u64,
    chain_verified: AtomicBool,
    cursor: AtomicU64,
    last_settlement: Mutex<LastSettlement>,
}

impl PolymarketSettlementSource {
    /// Create a source from a `gravity://6/<mirror_id>/polymarket_settlement`
    /// URI.
    pub async fn from_task(
        task: &ParsedOracleTask,
        rpc_url: &str,
        latest_onchain_nonce: u128,
        cursor: u64,
    ) -> Result<Self> {
        let latest_position = (latest_onchain_nonce > 0).then_some(cursor).unwrap_or(0);
        Self::from_task_with_progress(task, rpc_url, latest_onchain_nonce, cursor, latest_position)
            .await
    }

    pub(crate) async fn from_task_with_progress(
        task: &ParsedOracleTask,
        rpc_url: &str,
        latest_onchain_nonce: u128,
        cursor: u64,
        latest_position: u64,
    ) -> Result<Self> {
        if task.source_type != source_types::POLYMARKET_SETTLEMENT {
            return Err(anyhow!(
                "PolymarketSettlementSource requires sourceType={}",
                source_types::POLYMARKET_SETTLEMENT
            ));
        }

        let ctf_address = parse_address(task, "ctf")?;
        let polygon_chain_id = parse_optional(task, "chainId")?.unwrap_or(DEFAULT_POLYGON_CHAIN_ID);
        let condition_id = parse_required_b256(task, "condition")?;
        if polygon_chain_id != DEFAULT_POLYGON_CHAIN_ID {
            return Err(anyhow!(
                "Polymarket settlement chainId must be {}",
                DEFAULT_POLYGON_CHAIN_ID
            ));
        }
        if ctf_address == Address::ZERO {
            return Err(anyhow!("Polymarket settlement ctf cannot be zero"));
        }
        if condition_id == B256::ZERO {
            return Err(anyhow!("Polymarket settlement condition cannot be zero"));
        }
        let max_blocks_per_poll =
            parse_optional(task, "maxBlocksPerPoll")?.unwrap_or(DEFAULT_MAX_BLOCKS_PER_POLL);
        if max_blocks_per_poll == 0 || max_blocks_per_poll > MAX_BLOCKS_PER_POLL {
            return Err(anyhow!("maxBlocksPerPoll must be between 1 and {}", MAX_BLOCKS_PER_POLL));
        }

        info!(
            target: "polymarket_settlement_source",
            mirror_id = task.source_id,
            polygon_chain_id,
            ctf_address = ?ctf_address,
            condition_id = ?condition_id,
            max_blocks_per_poll,
            cursor,
            latest_onchain_nonce,
            latest_position,
            "Created PolymarketSettlementSource"
        );

        Ok(Self {
            mirror_id: task.source_id,
            polygon_chain_id,
            rpc_client: Arc::new(EthHttpCli::new(rpc_url)?),
            ctf_address,
            condition_id,
            max_blocks_per_poll,
            chain_verified: AtomicBool::new(false),
            cursor: AtomicU64::new(cursor),
            last_settlement: Mutex::new(LastSettlement {
                nonce: latest_onchain_nonce,
                block: latest_position,
            }),
        })
    }

    /// Current block cursor used for relayer persistence.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Mirror identifier used as NativeOracle sourceId.
    pub fn mirror_id(&self) -> u64 {
        self.mirror_id
    }

    /// Maximum finalized Polygon blocks scanned in one poll.
    pub fn max_blocks_per_poll(&self) -> u64 {
        self.max_blocks_per_poll
    }

    /// Last nonce returned or reconciled from NativeOracle.
    pub async fn last_nonce(&self) -> Option<u128> {
        let state = *self.last_settlement.lock().await;
        state.is_initialized().then_some(state.nonce)
    }

    /// Block number associated with the last returned or reconciled event.
    pub async fn last_nonce_block(&self) -> Option<u64> {
        let state = *self.last_settlement.lock().await;
        state.is_initialized().then_some(state.block)
    }

    /// Advance local state to match an already-recorded on-chain settlement.
    pub async fn fast_forward(&self, nonce: u128, block: u64) {
        *self.last_settlement.lock().await = LastSettlement { nonce, block };
        self.cursor.store(block, Ordering::Relaxed);
    }

    fn filter(&self, from_block: u64, to_block: u64) -> Filter {
        Filter::new()
            .address(self.ctf_address)
            .event_signature(ConditionResolution::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block)
            .topic1(self.condition_id)
    }

    fn decode_log(&self, log: &Log) -> Result<PolymarketSettlementObservation> {
        decode_condition_resolution_log(
            log,
            self.mirror_id,
            self.polygon_chain_id,
            self.ctf_address,
            self.condition_id,
        )
    }

    async fn ensure_polygon_chain(&self) -> Result<()> {
        if self.chain_verified.load(Ordering::Acquire) {
            return Ok(());
        }

        let actual_chain_id = self.rpc_client.get_chain_id().await?;
        if actual_chain_id != self.polygon_chain_id {
            return Err(anyhow!(
                "Polymarket RPC chain id mismatch: expected {}, got {}",
                self.polygon_chain_id,
                actual_chain_id
            ));
        }
        self.chain_verified.store(true, Ordering::Release);
        Ok(())
    }
}

#[async_trait]
impl OracleDataSource for PolymarketSettlementSource {
    fn source_type(&self) -> u32 {
        source_types::POLYMARKET_SETTLEMENT
    }

    fn source_id(&self) -> U256 {
        U256::from(self.mirror_id)
    }

    async fn poll(&self) -> Result<Vec<OracleData>> {
        if self.last_settlement.lock().await.is_initialized() {
            return Ok(vec![]);
        }
        self.ensure_polygon_chain().await?;
        let cursor = self.cursor.load(Ordering::Relaxed);
        let finalized_block = self.rpc_client.get_finalized_block_number().await?;
        let scan_limit = cursor
            .checked_add(self.max_blocks_per_poll)
            .ok_or_else(|| anyhow!("Polymarket block cursor overflow"))?;
        let to_block = std::cmp::min(scan_limit, finalized_block);

        if to_block <= cursor {
            return Ok(vec![]);
        }

        let from_block =
            cursor.checked_add(1).ok_or_else(|| anyhow!("Polymarket block cursor overflow"))?;
        let filter = self.filter(from_block, to_block);
        debug!(
            target: "polymarket_settlement_source",
            mirror_id = self.mirror_id,
            from_block,
            to_block,
            "Polling finalized Polygon CTF settlement logs"
        );

        let logs = self.rpc_client.get_logs(&filter).await?;
        let mut observations = Vec::with_capacity(logs.len());

        for log in logs {
            observations.push(self.decode_log(&log)?);
        }

        sort_and_dedup_observations(&mut observations);
        if observations.len() > 1 {
            return Err(anyhow!("multiple distinct settlements found for one Polymarket condition"));
        }

        let starting_nonce = self.last_settlement.lock().await.nonce;
        let results = observations_to_oracle_data(starting_nonce, &observations)?;

        self.cursor.store(to_block, Ordering::Relaxed);

        if let Some(last) = observations.last() {
            *self.last_settlement.lock().await = LastSettlement {
                nonce: results.last().map(|item| item.nonce).unwrap_or(starting_nonce),
                block: last.block_number,
            };
        }

        info!(
            target: "polymarket_settlement_source",
            mirror_id = self.mirror_id,
            events_found = results.len(),
            new_cursor = to_block,
            "Poll completed"
        );

        Ok(results)
    }
}

fn sort_and_dedup_observations(observations: &mut Vec<PolymarketSettlementObservation>) {
    observations.sort_by(|a, b| {
        a.block_number
            .cmp(&b.block_number)
            .then(a.log_index.cmp(&b.log_index))
            .then_with(|| a.tx_hash.as_slice().cmp(b.tx_hash.as_slice()))
    });
    observations.dedup_by(|a, b| {
        a.block_number == b.block_number && a.log_index == b.log_index && a.tx_hash == b.tx_hash
    });
}

fn observations_to_oracle_data(
    starting_nonce: u128,
    observations: &[PolymarketSettlementObservation],
) -> Result<Vec<OracleData>> {
    observations
        .iter()
        .enumerate()
        .map(|(idx, obs)| {
            let offset = u128::try_from(idx)
                .map_err(|_| anyhow!("Polymarket settlement batch exceeds u128"))?
                .checked_add(1)
                .ok_or_else(|| anyhow!("Polymarket settlement nonce overflow"))?;
            let nonce = starting_nonce
                .checked_add(offset)
                .ok_or_else(|| anyhow!("Polymarket settlement nonce overflow"))?;
            Ok(OracleData {
                nonce,
                source_position: obs.block_number,
                payload: obs.wrapped_payload(nonce),
            })
        })
        .collect()
}

fn decode_condition_resolution_log(
    log: &Log,
    mirror_id: u64,
    polygon_chain_id: u64,
    ctf_address: Address,
    condition_id: B256,
) -> Result<PolymarketSettlementObservation> {
    if log.removed {
        return Err(anyhow!("finalized Polymarket settlement log cannot be removed"));
    }
    if log.address() != ctf_address {
        return Err(anyhow!("Polymarket settlement log CTF address mismatch"));
    }

    let decoded = ConditionResolution::decode_log_validate(&log.inner)
        .map_err(|_| anyhow!("failed to decode filtered Polymarket settlement log"))?;

    let block_number = log
        .block_number
        .ok_or_else(|| anyhow!("Polymarket settlement log is missing block_number"))?;
    let log_index =
        log.log_index.ok_or_else(|| anyhow!("Polymarket settlement log is missing log_index"))?;
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow!("Polymarket settlement log is missing transaction_hash"))?;

    let event = decoded.data;
    if event.conditionId != condition_id {
        return Err(anyhow!("Polymarket settlement log condition mismatch"));
    }
    if event.oracle == Address::ZERO || event.questionId == B256::ZERO {
        return Err(anyhow!("Polymarket settlement log has zero oracle or questionId"));
    }

    validate_payouts(event.outcomeSlotCount, &event.payoutNumerators)?;

    Ok(PolymarketSettlementObservation {
        mirror_id,
        polygon_chain_id,
        ctf: ctf_address,
        oracle: event.oracle,
        condition_id: event.conditionId,
        question_id: event.questionId,
        outcome_slot_count: event.outcomeSlotCount,
        payout_numerators: event.payoutNumerators,
        tx_hash,
        log_index,
        block_number,
    })
}

fn validate_payouts(outcome_slot_count: U256, payout_numerators: &[U256]) -> Result<()> {
    let count: usize = outcome_slot_count
        .try_into()
        .map_err(|_| anyhow!("outcomeSlotCount too large for local validation"))?;
    if count == 0 {
        return Err(anyhow!("condition resolution outcomeSlotCount cannot be zero"));
    }
    if count > MAX_OUTCOME_SLOT_COUNT {
        return Err(anyhow!(
            "condition resolution outcomeSlotCount exceeds maximum {}",
            MAX_OUTCOME_SLOT_COUNT
        ));
    }
    if count != payout_numerators.len() {
        return Err(anyhow!(
            "condition resolution payout length mismatch: outcomeSlotCount={}, payoutNumerators={}",
            count,
            payout_numerators.len()
        ));
    }
    if payout_numerators.iter().all(|payout| *payout == U256::ZERO) {
        return Err(anyhow!("condition resolution payout vector cannot be all zero"));
    }

    Ok(())
}

fn parse_address(task: &ParsedOracleTask, key: &str) -> Result<Address> {
    task.params
        .get(key)
        .ok_or_else(|| anyhow!("Missing '{key}' parameter in Polymarket settlement URI"))?
        .parse()
        .map_err(|e| anyhow!("Invalid {key} address in Polymarket settlement URI: {e}"))
}

fn parse_required_b256(task: &ParsedOracleTask, key: &str) -> Result<B256> {
    task.params
        .get(key)
        .ok_or_else(|| anyhow!("Missing '{key}' parameter in Polymarket settlement URI"))?
        .parse()
        .map_err(|e| anyhow!("Invalid {key} bytes32 in Polymarket settlement URI: {e}"))
}

fn parse_optional<T>(task: &ParsedOracleTask, key: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    task.params
        .get(key)
        .map(|value| value.parse::<T>().map_err(|e| anyhow!("Invalid {key}: {e}")))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{data_source::OracleDataSource, uri_parser::parse_oracle_uri};
    use alloy_primitives::{address, b256, Log as PrimitiveLog};
    use alloy_rpc_types::Log as RpcLog;
    use std::env;

    const MIRROR_ID: u64 = 42;
    const BLOCK_NUMBER: u64 = 50_000_000;
    const LOG_INDEX: u64 = 17;

    fn ctf_address() -> Address {
        address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045")
    }

    fn tx_hash() -> B256 {
        b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    }

    fn condition_id() -> B256 {
        b256!("1111111111111111111111111111111111111111111111111111111111111111")
    }

    fn question_id() -> B256 {
        b256!("2222222222222222222222222222222222222222222222222222222222222222")
    }

    fn settlement_log() -> RpcLog {
        let event = ConditionResolution {
            conditionId: condition_id(),
            oracle: address!("d91E80cF2E7be2e162c6513ceD06f1dD0dA35296"),
            questionId: question_id(),
            outcomeSlotCount: U256::from(2),
            payoutNumerators: vec![U256::ZERO, U256::from(1)],
        };
        let inner = ConditionResolution::encode_log(&PrimitiveLog::new_from_event_unchecked(
            ctf_address(),
            event,
        ));

        RpcLog {
            inner,
            block_number: Some(BLOCK_NUMBER),
            transaction_hash: Some(tx_hash()),
            log_index: Some(LOG_INDEX),
            ..Default::default()
        }
    }

    #[test]
    fn test_decode_condition_resolution_log() {
        let log = settlement_log();
        let observation = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap();

        assert_eq!(observation.mirror_id, MIRROR_ID);
        assert_eq!(observation.polygon_chain_id, DEFAULT_POLYGON_CHAIN_ID);
        assert_eq!(observation.condition_id, condition_id());
        assert_eq!(observation.question_id, question_id());
        assert_eq!(observation.outcome_slot_count, U256::from(2));
        assert_eq!(observation.payout_numerators, vec![U256::ZERO, U256::from(1)]);
        assert_eq!(observation.tx_hash, tx_hash());
        assert_eq!(observation.log_index, LOG_INDEX);
        assert_eq!(observation.block_number, BLOCK_NUMBER);
    }

    #[test]
    fn test_condition_filter_fails_closed_on_other_condition() {
        let other_condition =
            b256!("3333333333333333333333333333333333333333333333333333333333333333");
        let log = settlement_log();
        let err = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            other_condition,
        )
        .unwrap_err();

        assert!(err.to_string().contains("condition mismatch"));
    }

    #[test]
    fn test_wrapped_payload_is_nonce_block_and_resolver_payload() {
        let log = settlement_log();
        let observation = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap();

        let delivery_nonce = 7;
        let wrapped = observation.wrapped_payload(delivery_nonce);
        assert_eq!(u128::from_be_bytes(wrapped[48..64].try_into().unwrap()), delivery_nonce);
        assert_eq!(U256::from_be_slice(&wrapped[64..96]), U256::from(observation.block_number));

        let resolver_offset = u64::from_be_bytes(wrapped[120..128].try_into().unwrap()) as usize;
        let resolver_len = u64::from_be_bytes(
            wrapped[32 + resolver_offset + 24..32 + resolver_offset + 32].try_into().unwrap(),
        ) as usize;
        assert!(resolver_len > 0);
    }

    #[test]
    fn test_observations_to_oracle_data_uses_sequential_delivery_nonce() {
        let mut obs_a = decode_condition_resolution_log(
            &settlement_log(),
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap();
        obs_a.log_index = 9;

        let mut obs_b = obs_a.clone();
        obs_b.log_index = 3;
        obs_b.tx_hash = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let mut observations = vec![obs_a, obs_b];
        sort_and_dedup_observations(&mut observations);
        let data = observations_to_oracle_data(7, &observations).unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data[0].nonce, 8);
        assert_eq!(u128::from_be_bytes(data[0].payload[48..64].try_into().unwrap()), 8);
        assert_eq!(data[1].nonce, 9);
        assert_eq!(u128::from_be_bytes(data[1].payload[48..64].try_into().unwrap()), 9);
        assert_eq!(observations[0].log_index, 3);
        assert_eq!(observations[1].log_index, 9);
    }

    #[test]
    fn test_sort_and_dedup_observations_removes_duplicate_source_event() {
        let obs = decode_condition_resolution_log(
            &settlement_log(),
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap();

        let mut observations = vec![obs.clone(), obs];
        sort_and_dedup_observations(&mut observations);

        assert_eq!(observations.len(), 1);
    }

    #[test]
    fn test_invalid_payout_vector_fails() {
        let err = validate_payouts(U256::from(2), &[U256::ZERO]).unwrap_err();
        assert!(err.to_string().contains("payout length mismatch"));

        let err = validate_payouts(U256::from(2), &[U256::ZERO, U256::ZERO]).unwrap_err();
        assert!(err.to_string().contains("all zero"));

        let payouts = vec![U256::from(1); MAX_OUTCOME_SLOT_COUNT + 1];
        let err = validate_payouts(U256::from(payouts.len()), &payouts).unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_filtered_log_missing_metadata_fails_closed() {
        let mut log = settlement_log();
        log.transaction_hash = None;
        let err = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("missing transaction_hash"));
    }

    #[tokio::test]
    async fn test_polymarket_source_accepts_max_blocks_per_poll() {
        let uri = format!(
            "gravity://6/42/polymarket_settlement?ctf={}&fromBlock=50000000&condition={}&maxBlocksPerPoll=10",
            ctf_address(),
            condition_id()
        );
        let task = parse_oracle_uri(&uri).expect("parse Polymarket URI");
        let source =
            PolymarketSettlementSource::from_task(&task, "http://localhost:8545", 0, 50_000_000)
                .await
                .expect("create source");

        assert_eq!(source.max_blocks_per_poll(), 10);
    }

    #[tokio::test]
    async fn test_polymarket_source_rejects_zero_max_blocks_per_poll() {
        let uri = format!(
            "gravity://6/42/polymarket_settlement?ctf={}&fromBlock=50000000&condition={}&maxBlocksPerPoll=0",
            ctf_address(),
            condition_id()
        );
        let task = parse_oracle_uri(&uri).expect("parse Polymarket URI");
        let err =
            PolymarketSettlementSource::from_task(&task, "http://localhost:8545", 0, 50_000_000)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("maxBlocksPerPoll"));
    }

    #[tokio::test]
    async fn test_polymarket_source_rejects_non_polygon_chain_id() {
        let uri = format!(
            "gravity://6/42/polymarket_settlement?ctf={}&fromBlock=50000000&condition={}&chainId=1",
            ctf_address(),
            condition_id()
        );
        let task = parse_oracle_uri(&uri).expect("parse Polymarket URI");
        let err =
            PolymarketSettlementSource::from_task(&task, "http://localhost:8545", 0, 50_000_000)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("chainId must be 137"));
    }

    #[tokio::test]
    async fn test_polymarket_source_rejects_excessive_scan_range() {
        let uri = format!(
            "gravity://6/42/polymarket_settlement?ctf={}&fromBlock=50000000&condition={}&maxBlocksPerPoll={}",
            ctf_address(),
            condition_id(),
            MAX_BLOCKS_PER_POLL + 1
        );
        let task = parse_oracle_uri(&uri).expect("parse Polymarket URI");
        let err =
            PolymarketSettlementSource::from_task(&task, "http://localhost:8545", 0, 50_000_000)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("maxBlocksPerPoll"));
    }

    #[tokio::test]
    async fn test_polymarket_source_requires_condition_filter() {
        let uri = format!(
            "gravity://6/42/polymarket_settlement?ctf={}&fromBlock=50000000",
            ctf_address()
        );
        let task = parse_oracle_uri(&uri).expect("parse Polymarket URI");
        let err =
            PolymarketSettlementSource::from_task(&task, "http://localhost:8545", 0, 50_000_000)
                .await
                .unwrap_err();

        assert!(err.to_string().contains("condition"));
    }

    #[tokio::test]
    #[ignore = "requires POLYGON_RPC_URL and a recent POLYMARKET_FROM_BLOCK"]
    async fn test_live_poll_polygon_polymarket_settlements() {
        let rpc_url = env::var("POLYGON_RPC_URL").expect("POLYGON_RPC_URL is required");
        let from_block: u64 = env::var("POLYMARKET_FROM_BLOCK")
            .expect("POLYMARKET_FROM_BLOCK is required")
            .parse()
            .expect("POLYMARKET_FROM_BLOCK must be a u64");
        let ctf = env::var("POLYMARKET_CTF_ADDRESS").unwrap_or_else(|_| ctf_address().to_string());

        let condition =
            env::var("POLYMARKET_CONDITION_ID").expect("POLYMARKET_CONDITION_ID is required");
        let mut uri = format!(
            "gravity://6/1/polymarket_settlement?ctf={ctf}&fromBlock={from_block}&condition={condition}"
        );
        if let Ok(max_blocks) = env::var("POLYMARKET_MAX_BLOCKS_PER_POLL") {
            uri.push_str("&maxBlocksPerPoll=");
            uri.push_str(&max_blocks);
        }
        let task = parse_oracle_uri(&uri).expect("parse live Polymarket URI");
        let source = PolymarketSettlementSource::from_task(&task, &rpc_url, 0, task.from_block())
            .await
            .expect("create live Polymarket source");

        let data = source.poll().await.expect("poll finalized Polygon settlement logs");
        println!(
            "live Polymarket settlement poll: {} item(s), cursor={}",
            data.len(),
            source.cursor()
        );
        for item in &data {
            println!("nonce={}, payload_len={}", item.nonce, item.payload.len());
        }
    }
}
