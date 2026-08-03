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
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
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
const MIN_OUTCOME_SLOT_COUNT: usize = 2;
const MAX_OUTCOME_SLOT_COUNT: usize = 32;
const POLYMARKET_TASK_TYPE: &str = "polymarket_settlement";
const POLYMARKET_TASK_PARAMETERS: &[&str] =
    &["ctf", "condition", "fromBlock", "chainId", "maxBlocksPerPoll"];

#[async_trait]
trait PolygonRpc: Send + Sync + std::fmt::Debug {
    async fn chain_id(&self) -> Result<u64>;
    async fn finalized_block_number(&self) -> Result<u64>;
    async fn logs(&self, filter: &Filter) -> Result<Vec<Log>>;
}

#[async_trait]
impl PolygonRpc for EthHttpCli {
    async fn chain_id(&self) -> Result<u64> {
        self.get_chain_id().await
    }

    async fn finalized_block_number(&self) -> Result<u64> {
        self.get_finalized_block_number().await
    }

    async fn logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        self.get_logs(filter).await
    }
}

/// Last CTF settlement returned to the caller.
#[derive(Debug, Clone, Copy, Default)]
struct LastSettlement {
    nonce: u128,
    source_position: u64,
}

impl LastSettlement {
    const fn is_initialized(self) -> bool {
        self.nonce > 0
    }
}

/// A canonical CTF settlement observation decoded from Polygon logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolymarketSettlementObservation {
    /// `NativeOracle` source ID for this mirror.
    pub mirror_id: u64,
    /// Source chain ID, normally Polygon `PoS` mainnet `137`.
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
    rpc_client: Arc<dyn PolygonRpc>,
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
        let latest_position = if latest_onchain_nonce > 0 { cursor } else { 0 };
        Self::from_task_with_progress(task, rpc_url, latest_onchain_nonce, latest_position, cursor)
            .await
    }

    pub(crate) async fn from_task_with_progress(
        task: &ParsedOracleTask,
        rpc_url: &str,
        latest_onchain_nonce: u128,
        latest_position: u64,
        cursor: u64,
    ) -> Result<Self> {
        let rpc_client = Arc::new(EthHttpCli::new(rpc_url)?);
        Self::from_task_with_rpc(task, rpc_client, latest_onchain_nonce, latest_position, cursor)
    }

    fn from_task_with_rpc(
        task: &ParsedOracleTask,
        rpc_client: Arc<dyn PolygonRpc>,
        latest_onchain_nonce: u128,
        latest_position: u64,
        cursor: u64,
    ) -> Result<Self> {
        if task.source_type != source_types::POLYMARKET_SETTLEMENT {
            return Err(anyhow!(
                "PolymarketSettlementSource requires sourceType={}",
                source_types::POLYMARKET_SETTLEMENT
            ));
        }
        if task.task_type != POLYMARKET_TASK_TYPE {
            return Err(anyhow!(
                "PolymarketSettlementSource requires task type '{POLYMARKET_TASK_TYPE}', got '{}'",
                task.task_type
            ));
        }
        for parameter in task.params.keys() {
            if !POLYMARKET_TASK_PARAMETERS.contains(&parameter.as_str()) {
                return Err(anyhow!("Unsupported Polymarket settlement parameter '{parameter}'"));
            }
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
        let from_block = parse_required::<u64>(task, "fromBlock")?;
        if cursor < from_block {
            return Err(anyhow!(
                "Polymarket settlement cursor {cursor} precedes configured fromBlock {from_block}"
            ));
        }
        let max_blocks_per_poll =
            parse_optional(task, "maxBlocksPerPoll")?.unwrap_or(DEFAULT_MAX_BLOCKS_PER_POLL);
        if max_blocks_per_poll == 0 || max_blocks_per_poll > MAX_BLOCKS_PER_POLL {
            return Err(anyhow!("maxBlocksPerPoll must be between 1 and {}", MAX_BLOCKS_PER_POLL));
        }
        if latest_onchain_nonce > 1 {
            return Err(anyhow!(
                "Polymarket settlement mirror is terminal and cannot have nonce {latest_onchain_nonce}"
            ));
        }
        if latest_onchain_nonce == 0 && latest_position != 0 {
            return Err(anyhow!(
                "Polymarket settlement task has zero nonce with nonzero source position"
            ));
        }
        if latest_position > cursor {
            return Err(anyhow!(
                "Polymarket settlement source position {latest_position} exceeds scan cursor {cursor}"
            ));
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
            rpc_client,
            ctf_address,
            condition_id,
            max_blocks_per_poll,
            chain_verified: AtomicBool::new(false),
            cursor: AtomicU64::new(cursor),
            last_settlement: Mutex::new(LastSettlement {
                nonce: latest_onchain_nonce,
                source_position: latest_position,
            }),
        })
    }

    /// Current block cursor used for relayer persistence.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Mirror identifier used as the `NativeOracle` source ID.
    pub const fn mirror_id(&self) -> u64 {
        self.mirror_id
    }

    /// Maximum finalized Polygon blocks scanned in one poll.
    pub const fn max_blocks_per_poll(&self) -> u64 {
        self.max_blocks_per_poll
    }

    /// Last nonce returned or reconciled from `NativeOracle`.
    pub async fn last_nonce(&self) -> Option<u128> {
        let state = *self.last_settlement.lock().await;
        state.is_initialized().then_some(state.nonce)
    }

    /// Block number associated with the last returned or reconciled event.
    pub async fn last_nonce_position(&self) -> Option<u64> {
        let state = *self.last_settlement.lock().await;
        state.is_initialized().then_some(state.source_position)
    }

    /// Reconcile local state with the terminal settlement recorded by `NativeOracle`.
    pub async fn reconcile_progress(&self, nonce: u128, source_position: u64) -> Result<()> {
        if nonce > 1 {
            return Err(anyhow!(
                "Polymarket settlement mirror is terminal and cannot reconcile nonce {nonce}"
            ));
        }
        if nonce == 0 {
            if source_position != 0 {
                return Err(anyhow!(
                    "Polymarket settlement task has zero nonce with nonzero source position"
                ));
            }
            return Ok(());
        }

        let mut state = self.last_settlement.lock().await;
        if nonce < state.nonce {
            return Err(anyhow!(
                "Polymarket settlement task cannot reconcile backwards from nonce {} to {nonce}",
                state.nonce
            ));
        }
        *state = LastSettlement { nonce, source_position };
        if source_position > 0 {
            self.cursor.fetch_max(source_position, Ordering::Relaxed);
        }
        Ok(())
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

        let actual_chain_id = self.rpc_client.chain_id().await?;
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
        let mut state = self.last_settlement.lock().await;
        if state.is_initialized() {
            return Ok(vec![]);
        }
        self.ensure_polygon_chain().await?;
        let cursor = self.cursor.load(Ordering::Relaxed);
        let finalized_block = self.rpc_client.finalized_block_number().await?;
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

        let logs = self.rpc_client.logs(&filter).await?;
        let mut observations = Vec::with_capacity(logs.len());

        for log in logs {
            let observation = self.decode_log(&log)?;
            if observation.block_number < from_block || observation.block_number > to_block {
                return Err(anyhow!(
                    "Polymarket RPC returned settlement block {} outside requested finalized range [{from_block}, {to_block}]",
                    observation.block_number
                ));
            }
            observations.push(observation);
        }

        sort_and_dedup_observations(&mut observations);
        if observations.len() > 1 {
            return Err(anyhow!("multiple distinct settlements found for one Polymarket condition"));
        }

        let results: Vec<OracleData> =
            observations.first().map(observation_to_oracle_data).into_iter().collect();

        self.cursor.store(to_block, Ordering::Relaxed);

        if let Some(last) = observations.last() {
            let delivered_nonce = results.last().map(|item| item.nonce).unwrap_or(state.nonce);
            *state = LastSettlement { nonce: delivered_nonce, source_position: last.block_number };
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

fn observation_to_oracle_data(observation: &PolymarketSettlementObservation) -> OracleData {
    const TERMINAL_SETTLEMENT_NONCE: u128 = 1;
    OracleData {
        nonce: TERMINAL_SETTLEMENT_NONCE,
        source_position: observation.block_number,
        payload: observation.wrapped_payload(TERMINAL_SETTLEMENT_NONCE),
    }
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
    if tx_hash == B256::ZERO {
        return Err(anyhow!("Polymarket settlement log has zero transaction_hash"));
    }

    let derived_condition_id =
        derive_condition_id(event.oracle, event.questionId, event.outcomeSlotCount);
    if derived_condition_id != event.conditionId {
        return Err(anyhow!(
            "Polymarket settlement condition does not match oracle, questionId, and outcomeSlotCount"
        ));
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

fn derive_condition_id(oracle: Address, question_id: B256, outcome_slot_count: U256) -> B256 {
    let mut preimage = Vec::with_capacity(84);
    preimage.extend_from_slice(oracle.as_slice());
    preimage.extend_from_slice(question_id.as_slice());
    preimage.extend_from_slice(&outcome_slot_count.to_be_bytes::<32>());
    keccak256(preimage)
}

fn validate_payouts(outcome_slot_count: U256, payout_numerators: &[U256]) -> Result<()> {
    let count: usize = outcome_slot_count
        .try_into()
        .map_err(|_| anyhow!("outcomeSlotCount too large for local validation"))?;
    if count < MIN_OUTCOME_SLOT_COUNT {
        return Err(anyhow!(
            "condition resolution outcomeSlotCount must be at least {MIN_OUTCOME_SLOT_COUNT}"
        ));
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

fn parse_required<T>(task: &ParsedOracleTask, key: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    task.params
        .get(key)
        .ok_or_else(|| anyhow!("Missing '{key}' parameter in Polymarket settlement URI"))?
        .parse()
        .map_err(|e| anyhow!("Invalid {key} in Polymarket settlement URI: {e}"))
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
    use alloy_sol_types::SolValue;
    use std::sync::atomic::AtomicU64;

    const MIRROR_ID: u64 = 42;
    const BLOCK_NUMBER: u64 = 50_000_000;
    const LOG_INDEX: u64 = 17;

    fn ctf_address() -> Address {
        address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045")
    }

    fn oracle_address() -> Address {
        address!("d91E80cF2E7be2e162c6513ceD06f1dD0dA35296")
    }

    fn tx_hash() -> B256 {
        b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    }

    fn second_tx_hash() -> B256 {
        b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    }

    fn question_id() -> B256 {
        b256!("2222222222222222222222222222222222222222222222222222222222222222")
    }

    fn condition_id() -> B256 {
        derive_condition_id(oracle_address(), question_id(), U256::from(2))
    }

    #[test]
    fn condition_derivation_matches_solidity_packed_encoding() {
        assert_eq!(
            condition_id(),
            b256!("d874d3b83fa09e192fdda031b6a3b3ec78be60cb82678aa67b23f8fd027c86ae")
        );
    }

    fn settlement_log() -> RpcLog {
        settlement_log_at(BLOCK_NUMBER, LOG_INDEX, tx_hash())
    }

    fn settlement_log_at(block_number: u64, log_index: u64, transaction_hash: B256) -> RpcLog {
        settlement_log_with_condition(condition_id(), block_number, log_index, transaction_hash)
    }

    fn settlement_log_with_condition(
        event_condition_id: B256,
        block_number: u64,
        log_index: u64,
        transaction_hash: B256,
    ) -> RpcLog {
        let event = ConditionResolution {
            conditionId: event_condition_id,
            oracle: oracle_address(),
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
            block_number: Some(block_number),
            transaction_hash: Some(transaction_hash),
            log_index: Some(log_index),
            ..Default::default()
        }
    }

    #[derive(Debug)]
    struct MockPolygonRpc {
        chain_id: u64,
        finalized_block: AtomicU64,
        logs: Mutex<Vec<Log>>,
        chain_id_calls: AtomicU64,
        finalized_calls: AtomicU64,
        log_calls: AtomicU64,
    }

    impl MockPolygonRpc {
        fn new(chain_id: u64, finalized_block: u64, logs: Vec<Log>) -> Arc<Self> {
            Arc::new(Self {
                chain_id,
                finalized_block: AtomicU64::new(finalized_block),
                logs: Mutex::new(logs),
                chain_id_calls: AtomicU64::new(0),
                finalized_calls: AtomicU64::new(0),
                log_calls: AtomicU64::new(0),
            })
        }
    }

    #[async_trait]
    impl PolygonRpc for MockPolygonRpc {
        async fn chain_id(&self) -> Result<u64> {
            self.chain_id_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.chain_id)
        }

        async fn finalized_block_number(&self) -> Result<u64> {
            self.finalized_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.finalized_block.load(Ordering::Relaxed))
        }

        async fn logs(&self, _filter: &Filter) -> Result<Vec<Log>> {
            self.log_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.logs.lock().await.clone())
        }
    }

    fn source_uri(from_block: u64, max_blocks_per_poll: u64) -> String {
        format!(
            "gravity://6/{MIRROR_ID}/polymarket_settlement?ctf={}&condition={}&fromBlock={from_block}&chainId=137&maxBlocksPerPoll={max_blocks_per_poll}",
            ctf_address(),
            condition_id()
        )
    }

    fn source_with_rpc(
        rpc: Arc<MockPolygonRpc>,
        latest_nonce: u128,
        latest_position: u64,
        cursor: u64,
        max_blocks_per_poll: u64,
    ) -> Result<PolymarketSettlementSource> {
        let task = parse_oracle_uri(&source_uri(cursor, max_blocks_per_poll))?;
        PolymarketSettlementSource::from_task_with_rpc(
            &task,
            rpc,
            latest_nonce,
            latest_position,
            cursor,
        )
    }

    #[test]
    fn decodes_condition_resolution_and_derives_identity() {
        let observation = decode_condition_resolution_log(
            &settlement_log(),
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
    fn resolver_payload_matches_final_contract_abi_field_for_field() {
        let observation = decode_condition_resolution_log(
            &settlement_log(),
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap();
        let data = observation_to_oracle_data(&observation);

        let (nonce, position, resolver_payload) =
            <(u128, U256, Bytes)>::abi_decode(&data.payload).unwrap();
        assert_eq!((nonce, position, resolver_payload.clone()).abi_encode(), data.payload);
        assert_eq!(nonce, 1);
        assert_eq!(position, U256::from(BLOCK_NUMBER));

        let payload = PolymarketSettlementPayloadSol::abi_decode(&resolver_payload).unwrap();
        assert_eq!(payload.abi_encode(), resolver_payload);
        assert_eq!(payload.mirrorId, U256::from(MIRROR_ID));
        assert_eq!(payload.polygonChainId, U256::from(DEFAULT_POLYGON_CHAIN_ID));
        assert_eq!(payload.ctf, ctf_address());
        assert_eq!(payload.oracle, oracle_address());
        assert_eq!(payload.conditionId, condition_id());
        assert_eq!(payload.questionId, question_id());
        assert_eq!(payload.outcomeSlotCount, U256::from(2));
        assert_eq!(payload.payoutNumerators, vec![U256::ZERO, U256::from(1)]);
        assert_eq!(payload.txHash, tx_hash());
        assert_eq!(payload.logIndex, U256::from(LOG_INDEX));
        assert_eq!(payload.settlementKind, CTF_CONDITION_RESOLUTION);
    }

    #[test]
    fn rejects_filter_condition_mismatch() {
        let other_condition =
            b256!("3333333333333333333333333333333333333333333333333333333333333333");
        let error = decode_condition_resolution_log(
            &settlement_log(),
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            other_condition,
        )
        .unwrap_err();

        assert!(error.to_string().contains("condition mismatch"));
    }

    #[test]
    fn rejects_condition_not_derived_from_event_identity() {
        let arbitrary_condition =
            b256!("3333333333333333333333333333333333333333333333333333333333333333");
        let log =
            settlement_log_with_condition(arbitrary_condition, BLOCK_NUMBER, LOG_INDEX, tx_hash());
        let error = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            arbitrary_condition,
        )
        .unwrap_err();

        assert!(error.to_string().contains("does not match oracle"));
    }

    #[test]
    fn rejects_invalid_payout_vectors() {
        let error = validate_payouts(U256::from(1), &[U256::from(1)]).unwrap_err();
        assert!(error.to_string().contains("at least 2"));

        let error = validate_payouts(U256::from(2), &[U256::ZERO]).unwrap_err();
        assert!(error.to_string().contains("payout length mismatch"));

        let error = validate_payouts(U256::from(2), &[U256::ZERO, U256::ZERO]).unwrap_err();
        assert!(error.to_string().contains("all zero"));

        let payouts = vec![U256::from(1); MAX_OUTCOME_SLOT_COUNT + 1];
        let error = validate_payouts(U256::from(payouts.len()), &payouts).unwrap_err();
        assert!(error.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn rejects_removed_or_incomplete_log_metadata() {
        let mut log = settlement_log();
        log.removed = true;
        let error = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cannot be removed"));

        let mut log = settlement_log();
        log.transaction_hash = None;
        let error = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("missing transaction_hash"));

        let mut log = settlement_log();
        log.transaction_hash = Some(B256::ZERO);
        let error = decode_condition_resolution_log(
            &log,
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("zero transaction_hash"));
    }

    #[test]
    fn deduplicates_only_the_same_polygon_log_identity() {
        let observation = decode_condition_resolution_log(
            &settlement_log(),
            MIRROR_ID,
            DEFAULT_POLYGON_CHAIN_ID,
            ctf_address(),
            condition_id(),
        )
        .unwrap();

        let mut observations = vec![observation.clone(), observation];
        sort_and_dedup_observations(&mut observations);
        assert_eq!(observations.len(), 1);

        let mut distinct = observations[0].clone();
        distinct.log_index += 1;
        distinct.tx_hash = second_tx_hash();
        observations.push(distinct);
        sort_and_dedup_observations(&mut observations);
        assert_eq!(observations.len(), 2);
    }

    #[tokio::test]
    async fn polls_only_finalized_range_and_emits_terminal_nonce_one() {
        let cursor = BLOCK_NUMBER - 2;
        let rpc = MockPolygonRpc::new(137, BLOCK_NUMBER + 5, vec![settlement_log()]);
        let source = source_with_rpc(rpc.clone(), 0, 0, cursor, 10).unwrap();

        let data = source.poll().await.unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].nonce, 1);
        assert_eq!(data[0].source_position, BLOCK_NUMBER);
        assert_eq!(source.cursor(), BLOCK_NUMBER + 5);
        assert_eq!(source.last_nonce().await, Some(1));
        assert_eq!(source.last_nonce_position().await, Some(BLOCK_NUMBER));
        assert_eq!(rpc.chain_id_calls.load(Ordering::Relaxed), 1);
        assert_eq!(rpc.finalized_calls.load(Ordering::Relaxed), 1);
        assert_eq!(rpc.log_calls.load(Ordering::Relaxed), 1);

        assert!(source.poll().await.unwrap().is_empty());
        assert_eq!(rpc.finalized_calls.load(Ordering::Relaxed), 1);
        assert_eq!(rpc.log_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn duplicate_rpc_logs_produce_one_terminal_observation() {
        let cursor = BLOCK_NUMBER - 2;
        let log = settlement_log();
        let rpc = MockPolygonRpc::new(137, BLOCK_NUMBER, vec![log.clone(), log]);
        let source = source_with_rpc(rpc, 0, 0, cursor, 10).unwrap();

        let data = source.poll().await.unwrap();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].nonce, 1);
    }

    #[tokio::test]
    async fn distinct_terminal_logs_fail_without_advancing_cursor() {
        let cursor = BLOCK_NUMBER - 2;
        let second = settlement_log_at(BLOCK_NUMBER, LOG_INDEX + 1, second_tx_hash());
        let rpc = MockPolygonRpc::new(137, BLOCK_NUMBER, vec![settlement_log(), second]);
        let source = source_with_rpc(rpc, 0, 0, cursor, 10).unwrap();

        let error = source.poll().await.unwrap_err();

        assert!(error.to_string().contains("multiple distinct settlements"));
        assert_eq!(source.cursor(), cursor);
        assert_eq!(source.last_nonce().await, None);
    }

    #[tokio::test]
    async fn empty_finalized_scan_advances_bounded_cursor_without_nonce() {
        let cursor = BLOCK_NUMBER;
        let rpc = MockPolygonRpc::new(137, BLOCK_NUMBER + 100, vec![]);
        let source = source_with_rpc(rpc, 0, 0, cursor, 10).unwrap();

        assert!(source.poll().await.unwrap().is_empty());
        assert_eq!(source.cursor(), cursor + 10);
        assert_eq!(source.last_nonce().await, None);
    }

    #[tokio::test]
    async fn rejects_rpc_log_outside_requested_finalized_range_without_progress() {
        let cursor = BLOCK_NUMBER;
        let finalized = BLOCK_NUMBER + 5;
        let out_of_range = settlement_log_at(finalized + 1, LOG_INDEX, tx_hash());
        let rpc = MockPolygonRpc::new(137, finalized, vec![out_of_range]);
        let source = source_with_rpc(rpc, 0, 0, cursor, 10).unwrap();

        let error = source.poll().await.unwrap_err();

        assert!(error.to_string().contains("outside requested finalized range"));
        assert_eq!(source.cursor(), cursor);
        assert_eq!(source.last_nonce().await, None);
    }

    #[tokio::test]
    async fn rejects_non_polygon_rpc_before_scanning() {
        let cursor = BLOCK_NUMBER;
        let rpc = MockPolygonRpc::new(1, BLOCK_NUMBER + 1, vec![]);
        let source = source_with_rpc(rpc.clone(), 0, 0, cursor, 10).unwrap();

        let error = source.poll().await.unwrap_err();

        assert!(error.to_string().contains("chain id mismatch"));
        assert_eq!(source.cursor(), cursor);
        assert_eq!(rpc.finalized_calls.load(Ordering::Relaxed), 0);
        assert_eq!(rpc.log_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn reconciles_terminal_progress_and_never_refetches() {
        let cursor = BLOCK_NUMBER - 10;
        let rpc = MockPolygonRpc::new(1, BLOCK_NUMBER + 100, vec![settlement_log()]);
        let source = source_with_rpc(rpc.clone(), 0, 0, cursor, 100).unwrap();

        source.reconcile_progress(1, BLOCK_NUMBER).await.unwrap();

        assert_eq!(source.cursor(), BLOCK_NUMBER);
        assert_eq!(source.last_nonce().await, Some(1));
        assert_eq!(source.last_nonce_position().await, Some(BLOCK_NUMBER));
        assert!(source.poll().await.unwrap().is_empty());
        assert_eq!(rpc.chain_id_calls.load(Ordering::Relaxed), 0);

        let error = source.reconcile_progress(2, BLOCK_NUMBER + 1).await.unwrap_err();
        assert!(error.to_string().contains("terminal"));
    }

    #[test]
    fn validates_uri_and_terminal_history() {
        let rpc = MockPolygonRpc::new(137, BLOCK_NUMBER, vec![]);

        let task = parse_oracle_uri(&source_uri(BLOCK_NUMBER, 10)).unwrap();
        let source = PolymarketSettlementSource::from_task_with_rpc(
            &task,
            rpc.clone(),
            1,
            BLOCK_NUMBER,
            BLOCK_NUMBER,
        )
        .unwrap();
        assert_eq!(source.max_blocks_per_poll(), 10);

        let error = PolymarketSettlementSource::from_task_with_rpc(
            &task,
            rpc.clone(),
            2,
            BLOCK_NUMBER,
            BLOCK_NUMBER,
        )
        .unwrap_err();
        assert!(error.to_string().contains("terminal"));

        let error =
            PolymarketSettlementSource::from_task_with_rpc(&task, rpc.clone(), 0, 1, BLOCK_NUMBER)
                .unwrap_err();
        assert!(error.to_string().contains("zero nonce"));

        let error = PolymarketSettlementSource::from_task_with_rpc(
            &task,
            rpc,
            1,
            BLOCK_NUMBER + 1,
            BLOCK_NUMBER,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exceeds scan cursor"));
    }

    #[test]
    fn rejects_unsupported_or_missing_uri_parameters() {
        let rpc = MockPolygonRpc::new(137, BLOCK_NUMBER, vec![]);

        for (uri, expected) in [
            (
                format!("{}&unknown=value", source_uri(BLOCK_NUMBER, 10)),
                "Unsupported Polymarket settlement parameter",
            ),
            (
                format!(
                    "gravity://6/{MIRROR_ID}/wrong_task?ctf={}&condition={}&fromBlock={BLOCK_NUMBER}",
                    ctf_address(),
                    condition_id()
                ),
                "requires task type",
            ),
            (
                format!(
                    "gravity://6/{MIRROR_ID}/polymarket_settlement?ctf={}&condition={}",
                    ctf_address(),
                    condition_id()
                ),
                "Missing 'fromBlock'",
            ),
            (
                format!(
                    "gravity://6/{MIRROR_ID}/polymarket_settlement?ctf={}&fromBlock={BLOCK_NUMBER}",
                    ctf_address()
                ),
                "Missing 'condition'",
            ),
            (
                format!(
                    "gravity://6/{MIRROR_ID}/polymarket_settlement?ctf={}&condition={}&fromBlock={BLOCK_NUMBER}&chainId=1",
                    ctf_address(),
                    condition_id()
                ),
                "chainId must be 137",
            ),
            (
                source_uri(BLOCK_NUMBER, 0),
                "maxBlocksPerPoll",
            ),
            (
                source_uri(BLOCK_NUMBER, MAX_BLOCKS_PER_POLL + 1),
                "maxBlocksPerPoll",
            ),
        ] {
            let task = parse_oracle_uri(&uri).unwrap();
            let error = PolymarketSettlementSource::from_task_with_rpc(
                &task,
                rpc.clone(),
                0,
                0,
                task.from_block(),
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{uri}: {error}");
        }
    }
}
