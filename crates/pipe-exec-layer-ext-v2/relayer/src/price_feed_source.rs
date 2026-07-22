//! Price feed oracle source.
//!
//! Data source for feeding deterministic price rounds through the
//! existing UnsupportedJWK consensus path.
//!
//! The explicit `provider=inline_fixture_v1` mode keeps all observations in the
//! `gravity://` URI for byte-identical tests. The production
//! `provider=binance_index_kline_v1` mode fetches a closed Binance USD-M
//! index-price candle and normalizes it into the same resolver payload shape.

use crate::{
    data_source::{source_types, OracleData, OracleDataSource},
    uri_parser::ParsedOracleTask,
};
use alloy_primitives::{keccak256, Bytes, B256, I256, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::SolValue;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tracing::info;
use url::Url;

const PRICE_AGG_WEIGHTED_MEAN: u8 = 1;
const PRICE_AGG_WEIGHTED_MEDIAN: u8 = 2;
const PROVIDER_INLINE_FIXTURE: &str = "inline_fixture_v1";
const PROVIDER_BINANCE_INDEX_KLINE: &str = "binance_index_kline_v1";
const DEFAULT_BINANCE_INDEX_FIELD: &str = "close";
const DEFAULT_BINANCE_GRACE_MS: u64 = 120_000;
const BINANCE_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BINANCE_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_PRICE_OBSERVATIONS: usize = 16;
const MAX_PRICE_DECIMALS: u8 = 18;

sol! {
    struct PriceObservationSol {
        bytes32 dataSourceId;
        uint64 observedAt;
        int256 price;
        uint256 weight;
    }

    struct PricePayloadSol {
        uint256 feedId;
        uint64 roundId;
        uint64 resolvedAt;
        uint8 decimals;
        uint8 aggregationMode;
        uint256 minSourceCount;
        uint256 minTotalWeight;
        uint64 maxStaleness;
        PriceObservationSol[] observations;
    }
}

#[derive(Debug, Clone)]
struct PriceObservation {
    data_source_id: B256,
    observed_at: u64,
    price: I256,
    weight: U256,
}

#[derive(Debug, Clone, Copy, Default)]
struct LastPriceRound {
    nonce: u128,
    block: u64,
}

impl LastPriceRound {
    fn is_initialized(self) -> bool {
        self.nonce > 0
    }
}

#[derive(Debug)]
enum PriceFeedMode {
    Static { round_id: u64, block_number: u64, payload: Bytes },
    BinanceIndexKline { config: BinanceIndexKlineConfig, client: Client },
}

#[derive(Debug, Clone)]
struct BinanceIndexKlineConfig {
    base_url: String,
    pair: String,
    interval: String,
    interval_ms: u64,
    bucket_start_ms: u64,
    grace_ms: u64,
    decimals: u8,
    aggregation_mode: u8,
    min_source_count: u64,
    min_total_weight: U256,
    max_staleness: u64,
    weight: U256,
    data_source_id: B256,
}

#[derive(Debug, Clone)]
struct BinanceIndexKlineRound {
    endpoint_url: String,
    bucket_start_ms: u64,
    bucket_end_ms: u64,
    round_id: u64,
    resolved_at: u64,
    delivery_nonce: u128,
    block_number: u64,
}

impl BinanceIndexKlineConfig {
    fn round_for_delivery_nonce(&self, delivery_nonce: u128) -> Result<BinanceIndexKlineRound> {
        let offset = delivery_nonce
            .checked_sub(1)
            .ok_or_else(|| anyhow!("Binance delivery nonce must start at 1"))?;
        let offset_ms = u128::from(self.interval_ms)
            .checked_mul(offset)
            .ok_or_else(|| anyhow!("Binance bucket offset overflow"))?;
        let bucket_start_ms = u128::from(self.bucket_start_ms)
            .checked_add(offset_ms)
            .ok_or_else(|| anyhow!("Binance bucket start overflow"))?;
        let bucket_start_ms = u64::try_from(bucket_start_ms)
            .map_err(|_| anyhow!("Binance bucket start exceeds u64"))?;
        let bucket_end_ms = bucket_start_ms
            .checked_add(self.interval_ms)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| anyhow!("Binance bucket end overflow"))?;
        let round_id = bucket_start_ms / self.interval_ms;
        let endpoint_url = build_binance_index_kline_url(
            &self.base_url,
            &self.pair,
            &self.interval,
            bucket_start_ms,
            bucket_end_ms,
        )?;

        Ok(BinanceIndexKlineRound {
            endpoint_url,
            bucket_start_ms,
            bucket_end_ms,
            round_id,
            resolved_at: bucket_end_ms,
            delivery_nonce,
            block_number: bucket_end_ms,
        })
    }
}

/// Price feed data source for `sourceType=3`.
#[derive(Debug)]
pub struct PriceFeedSource {
    feed_id: u64,
    mode: PriceFeedMode,
    cursor: AtomicU64,
    last_round: Mutex<LastPriceRound>,
}

impl PriceFeedSource {
    /// Create a price feed source from a `gravity://3/<feed_id>/price_feed` URI.
    pub fn from_task(task: &ParsedOracleTask, latest_onchain_nonce: u128) -> Result<Self> {
        Self::from_task_with_rpc(task, latest_onchain_nonce, None)
    }

    /// Create a price feed source and optionally supply the validator-local
    /// upstream URL from relayer config. The URL is intentionally not part of
    /// the on-chain URI for secret-bearing endpoints.
    pub fn from_task_with_rpc(
        task: &ParsedOracleTask,
        latest_onchain_nonce: u128,
        rpc_url: Option<&str>,
    ) -> Result<Self> {
        Self::from_task_with_reconciled_cursor(task, latest_onchain_nonce, rpc_url, None)
    }

    pub(crate) fn from_task_with_reconciled_cursor(
        task: &ParsedOracleTask,
        latest_onchain_nonce: u128,
        rpc_url: Option<&str>,
        confirmed_cursor: Option<u64>,
    ) -> Result<Self> {
        if task.source_type != source_types::PRICE_FEED {
            return Err(anyhow!("PriceFeedSource requires sourceType={}", source_types::PRICE_FEED));
        }

        match task.params.get("provider").map(|p| p.as_str()) {
            Some(PROVIDER_BINANCE_INDEX_KLINE) => {
                return Self::from_binance_index_kline_task(
                    task,
                    latest_onchain_nonce,
                    rpc_url,
                    confirmed_cursor,
                );
            }
            Some(PROVIDER_INLINE_FIXTURE) => {}
            Some(provider) => return Err(anyhow!("Unsupported price feed provider '{provider}'")),
            None => return Err(anyhow!("Missing 'provider' parameter for price feed")),
        }

        let round_id = parse_required::<u64>(task, "round")?;
        let resolved_at = parse_required::<u64>(task, "resolvedAt")?;
        let decimals = parse_required::<u8>(task, "decimals")?;
        if decimals > MAX_PRICE_DECIMALS {
            return Err(anyhow!(
                "price feed decimals {} exceeds maximum {}",
                decimals,
                MAX_PRICE_DECIMALS
            ));
        }
        let aggregation_mode = parse_required::<u8>(task, "aggregationMode")?;
        let max_staleness = parse_optional(task, "maxStaleness")?.unwrap_or(60u64);
        let block_number = parse_optional(task, "blockNumber")?.unwrap_or(resolved_at);
        let mut observations = parse_observations(task)?;

        observations.sort_by(|a, b| a.data_source_id.as_slice().cmp(b.data_source_id.as_slice()));

        let source_count = observations.len() as u64;
        let total_weight = observations.iter().try_fold(U256::ZERO, |acc, obs| {
            acc.checked_add(obs.weight).ok_or_else(|| anyhow!("price feed total weight overflow"))
        })?;
        let min_source_count = parse_optional(task, "minSourceCount")?.unwrap_or(source_count);
        let min_total_weight =
            parse_optional::<U256>(task, "minTotalWeight")?.unwrap_or(total_weight);
        validate_observations(
            &observations,
            resolved_at,
            aggregation_mode,
            min_source_count,
            min_total_weight,
            max_staleness,
            total_weight,
        )?;

        let resolver_payload = encode_price_payload(
            task.source_id,
            round_id,
            resolved_at,
            decimals,
            aggregation_mode,
            min_source_count,
            min_total_weight,
            max_staleness,
            &observations,
        );

        let wrapped_payload = SolValue::abi_encode(&(
            round_id as u128,
            U256::from(block_number),
            resolver_payload.as_slice(),
        ));

        let last_round = if latest_onchain_nonce > 0 {
            LastPriceRound { nonce: latest_onchain_nonce, block: block_number }
        } else {
            LastPriceRound::default()
        };

        info!(
            target: "price_feed_source",
            feed_id = task.source_id,
            round_id,
            source_count,
            total_weight = %total_weight,
            latest_onchain_nonce,
            "Created PriceFeedSource"
        );

        Ok(Self {
            feed_id: task.source_id,
            mode: PriceFeedMode::Static {
                round_id,
                block_number,
                payload: Bytes::from(wrapped_payload),
            },
            cursor: AtomicU64::new(block_number),
            last_round: Mutex::new(last_round),
        })
    }

    fn from_binance_index_kline_task(
        task: &ParsedOracleTask,
        latest_onchain_nonce: u128,
        rpc_url: Option<&str>,
        confirmed_cursor: Option<u64>,
    ) -> Result<Self> {
        let pair = task
            .params
            .get("pair")
            .ok_or_else(|| anyhow!("Missing 'pair' parameter for Binance index kline price feed"))?
            .to_string();
        validate_binance_pair(&pair)?;
        let interval = task.params.get("interval").cloned().unwrap_or_else(|| "1m".to_string());
        let interval_ms = binance_interval_ms(&interval)?;
        if task.params.contains_key("continuous") {
            return Err(anyhow!(
                "Binance index kline parameter 'continuous' is unsupported; price feeds are always continuous"
            ));
        }
        let bucket_start_ms = parse_required::<u64>(task, "bucketStartMs")?;
        if bucket_start_ms % interval_ms != 0 {
            return Err(anyhow!(
                "Binance index kline bucketStartMs {} is not aligned to interval {}",
                bucket_start_ms,
                interval
            ));
        }
        let bucket_end_ms = bucket_start_ms
            .checked_add(interval_ms)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| anyhow!("Binance index kline bucket end overflow"))?;
        for derived in ["round", "resolvedAt", "blockNumber"] {
            if task.params.contains_key(derived) {
                return Err(anyhow!(
                    "Binance index kline parameter '{derived}' is derived from the delivery bucket"
                ));
            }
        }
        let grace_ms = parse_optional(task, "graceMs")?.unwrap_or(DEFAULT_BINANCE_GRACE_MS);
        let round_id = bucket_start_ms / interval_ms;
        let resolved_at = bucket_end_ms;
        let decimals = parse_required::<u8>(task, "decimals")?;
        if decimals > MAX_PRICE_DECIMALS {
            return Err(anyhow!(
                "price feed decimals {} exceeds maximum {}",
                decimals,
                MAX_PRICE_DECIMALS
            ));
        }
        let aggregation_mode =
            parse_optional(task, "aggregationMode")?.unwrap_or(PRICE_AGG_WEIGHTED_MEDIAN);
        let field = task
            .params
            .get("field")
            .cloned()
            .unwrap_or_else(|| DEFAULT_BINANCE_INDEX_FIELD.to_string());
        if field != DEFAULT_BINANCE_INDEX_FIELD {
            return Err(anyhow!(
                "Binance index kline adapter only supports field={}",
                DEFAULT_BINANCE_INDEX_FIELD
            ));
        }
        let weight = parse_optional(task, "weight")?.unwrap_or(U256::from(1));
        let min_source_count = parse_optional(task, "minSourceCount")?.unwrap_or(1u64);
        let min_total_weight = parse_optional(task, "minTotalWeight")?.unwrap_or(weight);
        let max_staleness =
            parse_optional(task, "maxStaleness")?.unwrap_or(interval_ms.saturating_mul(3));
        let block_number = bucket_end_ms;
        let source_label =
            task.params.get("dataSourceLabel").cloned().unwrap_or_else(|| {
                format!("binance:usdm:indexPriceKlines:{pair}:{interval}:{field}")
            });
        let data_source_id = match task.params.get("dataSourceId") {
            Some(explicit) => source_id_from_label(explicit)?,
            None => source_id_from_label(&source_label)?,
        };
        if task.params.contains_key("baseUrl") {
            return Err(anyhow!(
                "Binance baseUrl must be validator-local relayer config, not an on-chain URI parameter"
            ));
        }
        let base_url = binance_base_url(rpc_url)?;
        validate_observations(
            &[PriceObservation {
                data_source_id,
                observed_at: bucket_end_ms,
                price: I256::ONE,
                weight,
            }],
            resolved_at,
            aggregation_mode,
            min_source_count,
            min_total_weight,
            max_staleness,
            weight,
        )?;

        let client = Client::builder()
            .no_proxy()
            .use_rustls_tls()
            .connect_timeout(Duration::from_secs(5))
            .timeout(BINANCE_HTTP_TIMEOUT)
            .build()
            .context("failed to build Binance index kline HTTP client")?;
        let config = BinanceIndexKlineConfig {
            base_url,
            pair,
            interval,
            interval_ms,
            bucket_start_ms,
            grace_ms,
            decimals,
            aggregation_mode,
            min_source_count,
            min_total_weight,
            max_staleness,
            weight,
            data_source_id,
        };

        let previous_block = if latest_onchain_nonce == 0 {
            None
        } else {
            let expected = config.round_for_delivery_nonce(latest_onchain_nonce)?.block_number;
            if let Some(confirmed) = confirmed_cursor {
                if confirmed != expected {
                    return Err(anyhow!(
                        "Binance task history mismatch: nonce {} implies block {}, confirmed cursor is {}; use a new feedId for a new bucket origin or interval",
                        latest_onchain_nonce,
                        expected,
                        confirmed
                    ));
                }
            }
            Some(expected)
        };
        let last_round = previous_block
            .map(|block| LastPriceRound { nonce: latest_onchain_nonce, block })
            .unwrap_or_default();
        let initial_cursor = previous_block.unwrap_or(block_number);

        info!(
            target: "price_feed_source",
            feed_id = task.source_id,
            provider = PROVIDER_BINANCE_INDEX_KLINE,
            pair = config.pair.as_str(),
            interval = config.interval.as_str(),
            round_id,
            bucket_start_ms,
            latest_onchain_nonce,
            "Created Binance index kline PriceFeedSource"
        );

        Ok(Self {
            feed_id: task.source_id,
            mode: PriceFeedMode::BinanceIndexKline { config, client },
            cursor: AtomicU64::new(initial_cursor),
            last_round: Mutex::new(last_round),
        })
    }

    /// Last round nonce returned or reconciled from NativeOracle.
    pub async fn last_nonce(&self) -> Option<u128> {
        let state = *self.last_round.lock().await;
        state.is_initialized().then_some(state.nonce)
    }

    /// Block number associated with the last returned or reconciled round.
    pub async fn last_nonce_block(&self) -> Option<u64> {
        let state = *self.last_round.lock().await;
        state.is_initialized().then_some(state.block)
    }

    /// Advance local state to match an already-recorded on-chain round.
    pub async fn fast_forward(&self, nonce: u128, block: u64) {
        *self.last_round.lock().await = LastPriceRound { nonce, block };
        self.cursor.store(block, Ordering::Relaxed);
    }

    /// Current block cursor used for relayer persistence.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Feed identifier used as NativeOracle sourceId.
    pub fn feed_id(&self) -> u64 {
        self.feed_id
    }
}

#[async_trait]
impl OracleDataSource for PriceFeedSource {
    fn source_type(&self) -> u32 {
        source_types::PRICE_FEED
    }

    fn source_id(&self) -> U256 {
        U256::from(self.feed_id)
    }

    async fn poll(&self) -> Result<Vec<OracleData>> {
        let mut state = self.last_round.lock().await;
        match &self.mode {
            PriceFeedMode::Static { round_id, block_number, payload } => {
                if state.nonce >= *round_id as u128 {
                    return Ok(vec![]);
                }

                state.nonce = *round_id as u128;
                state.block = *block_number;
                self.cursor.store(*block_number, Ordering::Relaxed);

                Ok(vec![OracleData { nonce: *round_id as u128, payload: payload.clone() }])
            }
            PriceFeedMode::BinanceIndexKline { config, client } => {
                let next_delivery_nonce = state
                    .nonce
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("Binance index kline delivery nonce overflow"))?;
                let round = config.round_for_delivery_nonce(next_delivery_nonce)?;
                if !is_binance_bucket_ready(config, &round)? {
                    return Ok(vec![]);
                }

                let observation =
                    fetch_binance_index_kline_observation(client, config, &round).await?;
                let total_weight = observation.weight;
                validate_observations(
                    std::slice::from_ref(&observation),
                    round.resolved_at,
                    config.aggregation_mode,
                    config.min_source_count,
                    config.min_total_weight,
                    config.max_staleness,
                    total_weight,
                )?;

                let resolver_payload = encode_price_payload(
                    self.feed_id,
                    round.round_id,
                    round.resolved_at,
                    config.decimals,
                    config.aggregation_mode,
                    config.min_source_count,
                    config.min_total_weight,
                    config.max_staleness,
                    &[observation],
                );
                let wrapped_payload = SolValue::abi_encode(&(
                    round.delivery_nonce,
                    U256::from(round.block_number),
                    resolver_payload.as_slice(),
                ));

                state.nonce = round.delivery_nonce;
                state.block = round.block_number;
                self.cursor.store(round.block_number, Ordering::Relaxed);

                Ok(vec![OracleData {
                    nonce: round.delivery_nonce,
                    payload: Bytes::from(wrapped_payload),
                }])
            }
        }
    }
}

async fn fetch_binance_index_kline_observation(
    client: &Client,
    config: &BinanceIndexKlineConfig,
    round: &BinanceIndexKlineRound,
) -> Result<PriceObservation> {
    ensure_binance_bucket_ready(config, round)?;
    let response = client
        .get(&round.endpoint_url)
        .send()
        .await
        .context("failed to fetch Binance index price kline")?
        .error_for_status()
        .context("Binance index price kline endpoint returned an error")?;
    let response = read_binance_response_limited(response).await?;
    let response: Value = serde_json::from_slice(&response)
        .context("failed to decode Binance index price kline response")?;

    binance_index_kline_observation_from_response(config, round, &response)
}

async fn read_binance_response_limited(mut response: reqwest::Response) -> Result<Vec<u8>> {
    if response.content_length().is_some_and(|len| len > MAX_BINANCE_RESPONSE_BYTES as u64) {
        return Err(binance_response_too_large());
    }

    let mut body = Vec::new();
    while let Some(chunk) =
        response.chunk().await.context("failed to read Binance index price kline response")?
    {
        append_binance_response_chunk(&mut body, &chunk)?;
    }
    Ok(body)
}

fn append_binance_response_chunk(body: &mut Vec<u8>, chunk: &[u8]) -> Result<()> {
    let new_len = body.len().checked_add(chunk.len()).ok_or_else(binance_response_too_large)?;
    if new_len > MAX_BINANCE_RESPONSE_BYTES {
        return Err(binance_response_too_large());
    }
    body.extend_from_slice(chunk);
    Ok(())
}

fn binance_response_too_large() -> anyhow::Error {
    anyhow!("Binance index price kline response exceeds {} bytes", MAX_BINANCE_RESPONSE_BYTES)
}

fn binance_index_kline_observation_from_response(
    config: &BinanceIndexKlineConfig,
    round: &BinanceIndexKlineRound,
    response: &Value,
) -> Result<PriceObservation> {
    let row = parse_binance_index_kline_row(round, response)?;
    let price = parse_fixed_decimal(row.close, config.decimals)?;
    Ok(PriceObservation {
        data_source_id: config.data_source_id,
        observed_at: row.close_time,
        price,
        weight: config.weight,
    })
}

#[derive(Debug, Clone, Copy)]
struct BinanceIndexKlineRow<'a> {
    close: &'a str,
    close_time: u64,
}

fn parse_binance_index_kline_row<'a>(
    round: &BinanceIndexKlineRound,
    response: &'a Value,
) -> Result<BinanceIndexKlineRow<'a>> {
    let rows = response
        .as_array()
        .ok_or_else(|| anyhow!("Binance index price kline response must be an array"))?;
    if rows.len() != 1 {
        return Err(anyhow!(
            "Binance index price kline response must contain exactly one row, got {}",
            rows.len()
        ));
    }
    let row = rows[0]
        .as_array()
        .ok_or_else(|| anyhow!("Binance index price kline row must be an array"))?;
    if row.len() < 7 {
        return Err(anyhow!("Binance index price kline row has fewer than 7 fields"));
    }
    let open_time = row[0]
        .as_u64()
        .ok_or_else(|| anyhow!("Binance index price kline openTime must be a u64"))?;
    let close = row[4]
        .as_str()
        .ok_or_else(|| anyhow!("Binance index price kline close must be a decimal string"))?;
    let close_time = row[6]
        .as_u64()
        .ok_or_else(|| anyhow!("Binance index price kline closeTime must be a u64"))?;
    if open_time != round.bucket_start_ms {
        return Err(anyhow!(
            "Binance index price kline openTime mismatch: expected {}, got {}",
            round.bucket_start_ms,
            open_time
        ));
    }
    if close_time != round.bucket_end_ms {
        return Err(anyhow!(
            "Binance index price kline closeTime mismatch: expected {}, got {}",
            round.bucket_end_ms,
            close_time
        ));
    }

    Ok(BinanceIndexKlineRow { close, close_time })
}

fn ensure_binance_bucket_ready(
    config: &BinanceIndexKlineConfig,
    round: &BinanceIndexKlineRound,
) -> Result<()> {
    if !is_binance_bucket_ready(config, round)? {
        let ready_at = round
            .bucket_end_ms
            .checked_add(config.grace_ms)
            .ok_or_else(|| anyhow!("Binance index kline ready time overflow"))?;
        let now_ms = current_unix_millis()?;
        return Err(anyhow!(
            "Binance index kline bucket is not ready: readyAtMs={}, nowMs={}",
            ready_at,
            now_ms
        ));
    }
    Ok(())
}

fn is_binance_bucket_ready(
    config: &BinanceIndexKlineConfig,
    round: &BinanceIndexKlineRound,
) -> Result<bool> {
    let ready_at = round
        .bucket_end_ms
        .checked_add(config.grace_ms)
        .ok_or_else(|| anyhow!("Binance index kline ready time overflow"))?;
    Ok(current_unix_millis()? >= u128::from(ready_at))
}

fn current_unix_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before UNIX_EPOCH")?
        .as_millis())
}

fn binance_base_url(rpc_url: Option<&str>) -> Result<String> {
    let base = rpc_url.filter(|value| !value.is_empty()).ok_or_else(|| {
        anyhow!("Binance price feed requires a validator-local relayer URL mapping")
    })?;
    validate_http_url(base, "Binance base URL")?;
    Ok(base.to_string())
}

fn validate_http_url(value: &str, label: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|e| anyhow!("invalid {label}: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(anyhow!("{label} must be an http(s) URL with a host"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(anyhow!("{label} must not contain URL userinfo"));
    }
    Ok(url)
}

fn validate_binance_pair(pair: &str) -> Result<()> {
    if pair.is_empty() ||
        pair.len() > 32 ||
        !pair.bytes().all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(anyhow!("Binance pair must contain 1-32 uppercase ASCII letters or digits"));
    }
    Ok(())
}

fn build_binance_index_kline_url(
    base_url: &str,
    pair: &str,
    interval: &str,
    bucket_start_ms: u64,
    bucket_end_ms: u64,
) -> Result<String> {
    let mut url = validate_http_url(base_url, "Binance base URL")?;
    url.set_path("/fapi/v1/indexPriceKlines");
    url.set_query(None);
    url.set_fragment(None);
    url.query_pairs_mut()
        .append_pair("pair", pair)
        .append_pair("interval", interval)
        .append_pair("startTime", &bucket_start_ms.to_string())
        .append_pair("endTime", &bucket_end_ms.to_string())
        .append_pair("limit", "1");
    Ok(url.to_string())
}

fn binance_interval_ms(interval: &str) -> Result<u64> {
    match interval {
        "1m" => Ok(60_000),
        "3m" => Ok(180_000),
        "5m" => Ok(300_000),
        "15m" => Ok(900_000),
        "30m" => Ok(1_800_000),
        "1h" => Ok(3_600_000),
        "2h" => Ok(7_200_000),
        "4h" => Ok(14_400_000),
        "6h" => Ok(21_600_000),
        "8h" => Ok(28_800_000),
        "12h" => Ok(43_200_000),
        "1d" => Ok(86_400_000),
        "3d" => Ok(259_200_000),
        "1w" => Ok(604_800_000),
        _ => Err(anyhow!("Unsupported Binance index kline interval '{interval}'")),
    }
}

fn encode_price_payload(
    feed_id: u64,
    round_id: u64,
    resolved_at: u64,
    decimals: u8,
    aggregation_mode: u8,
    min_source_count: u64,
    min_total_weight: U256,
    max_staleness: u64,
    observations: &[PriceObservation],
) -> Vec<u8> {
    let encoded_observations: Vec<PriceObservationSol> = observations
        .iter()
        .map(|obs| PriceObservationSol {
            dataSourceId: obs.data_source_id,
            observedAt: obs.observed_at,
            price: obs.price,
            weight: obs.weight,
        })
        .collect();

    PricePayloadSol {
        feedId: U256::from(feed_id),
        roundId: round_id,
        resolvedAt: resolved_at,
        decimals,
        aggregationMode: aggregation_mode,
        minSourceCount: U256::from(min_source_count),
        minTotalWeight: min_total_weight,
        maxStaleness: max_staleness,
        observations: encoded_observations,
    }
    .abi_encode()
}

fn parse_observations(task: &ParsedOracleTask) -> Result<Vec<PriceObservation>> {
    let raw = task
        .params
        .get("observations")
        .ok_or_else(|| anyhow!("Missing 'observations' parameter in price feed URI"))?;
    if raw.trim().is_empty() {
        return Err(anyhow!("Price feed observations cannot be empty"));
    }
    let observations: Vec<_> = raw
        .split(',')
        .enumerate()
        .map(|(idx, item)| parse_observation(idx, item))
        .collect::<Result<_>>()?;
    if observations.len() > MAX_PRICE_OBSERVATIONS {
        return Err(anyhow!(
            "price feed observation count {} exceeds maximum {}",
            observations.len(),
            MAX_PRICE_OBSERVATIONS
        ));
    }
    Ok(observations)
}

fn parse_observation(index: usize, raw: &str) -> Result<PriceObservation> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 4 {
        return Err(anyhow!(
            "Invalid price observation at index {}: expected source:observedAt:price:weight",
            index
        ));
    }

    Ok(PriceObservation {
        data_source_id: source_id_from_label(parts[0])?,
        observed_at: parse_str(parts[1], "observedAt")?,
        price: parse_str(parts[2], "price")?,
        weight: parse_str(parts[3], "weight")?,
    })
}

fn source_id_from_label(label: &str) -> Result<B256> {
    if label.is_empty() {
        return Err(anyhow!("price observation source label cannot be empty"));
    }

    if label.starts_with("0x") {
        return label
            .parse()
            .map_err(|e| anyhow!("invalid explicit price observation source id: {e}"));
    }

    Ok(keccak256(label.as_bytes()))
}

fn parse_fixed_decimal(value: &str, decimals: u8) -> Result<I256> {
    if value.starts_with('-') {
        return Err(anyhow!("price cannot be negative"));
    }
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err(anyhow!("empty decimal price"));
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) {
        return Err(anyhow!("invalid decimal price whole component"));
    }
    if !fraction.chars().all(|c| c.is_ascii_digit()) {
        return Err(anyhow!("invalid decimal price fractional component"));
    }

    let mut scaled = String::with_capacity(whole.len() + decimals as usize);
    scaled.push_str(if whole.is_empty() { "0" } else { whole });
    let decimals = decimals as usize;
    if fraction.len() >= decimals {
        scaled.push_str(&fraction[..decimals]);
    } else {
        scaled.push_str(fraction);
        scaled.extend(std::iter::repeat_n('0', decimals - fraction.len()));
    }

    let scaled = scaled.trim_start_matches('0');
    let scaled = if scaled.is_empty() { "0" } else { scaled };
    scaled.parse::<I256>().map_err(|e| anyhow!("invalid scaled decimal price: {e}"))
}

fn validate_observations(
    observations: &[PriceObservation],
    resolved_at: u64,
    aggregation_mode: u8,
    min_source_count: u64,
    min_total_weight: U256,
    max_staleness: u64,
    total_weight: U256,
) -> Result<()> {
    if observations.len() > MAX_PRICE_OBSERVATIONS {
        return Err(anyhow!(
            "price feed observation count {} exceeds maximum {}",
            observations.len(),
            MAX_PRICE_OBSERVATIONS
        ));
    }
    match aggregation_mode {
        PRICE_AGG_WEIGHTED_MEAN | PRICE_AGG_WEIGHTED_MEDIAN => {}
        _ => return Err(anyhow!("invalid price feed aggregationMode {aggregation_mode}")),
    }
    if min_source_count == 0 {
        return Err(anyhow!("price feed minSourceCount cannot be zero"));
    }
    if min_source_count > observations.len() as u64 {
        return Err(anyhow!(
            "price feed minSourceCount {} exceeds observation count {}",
            min_source_count,
            observations.len()
        ));
    }
    if min_total_weight > total_weight {
        return Err(anyhow!(
            "price feed minTotalWeight {} exceeds total weight {}",
            min_total_weight,
            total_weight
        ));
    }
    let max_int256 = U256::MAX >> 1;
    if total_weight > max_int256 {
        return Err(anyhow!("price feed total weight exceeds int256 max"));
    }

    for (index, observation) in observations.iter().enumerate() {
        if observation.data_source_id == B256::ZERO {
            return Err(anyhow!("price observation {index} has zero dataSourceId"));
        }
        if observation.price <= I256::ZERO {
            return Err(anyhow!("price observation {index} has non-positive price"));
        }
        if observation.weight.is_zero() {
            return Err(anyhow!("price observation {index} has zero weight"));
        }
        if observation.weight > max_int256 {
            return Err(anyhow!("price observation {index} weight exceeds int256 max"));
        }
        if observation.observed_at > resolved_at {
            return Err(anyhow!(
                "price observation {index} is from the future: observedAt={}, resolvedAt={}",
                observation.observed_at,
                resolved_at
            ));
        }
        let stale = max_staleness > 0 &&
            match observation.observed_at.checked_add(max_staleness) {
                Some(fresh_until) => fresh_until < resolved_at,
                None => true,
            };
        if stale {
            return Err(anyhow!(
                "price observation {index} is stale: observedAt={}, resolvedAt={}, maxStaleness={}",
                observation.observed_at,
                resolved_at,
                max_staleness
            ));
        }
        if index > 0 && observations[index - 1].data_source_id == observation.data_source_id {
            return Err(anyhow!(
                "duplicate price observation dataSourceId {:?}",
                observation.data_source_id
            ));
        }
    }

    Ok(())
}

fn parse_required<T>(task: &ParsedOracleTask, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = task.params.get(name).ok_or_else(|| anyhow!("Missing '{name}' parameter"))?;
    parse_str(value, name)
}

fn parse_optional<T>(task: &ParsedOracleTask, name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    task.params.get(name).map(|value| parse_str(value, name)).transpose()
}

fn parse_str<T>(value: &str, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e| anyhow!("Invalid {name}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uri_parser::parse_oracle_uri;
    use std::{env, fs, path::Path};

    fn price_uri() -> &'static str {
        "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:2000:10000000000:1,source-b:2000:10200000000:2,source-c:2000:9800000000:1"
    }

    fn binance_uri() -> &'static str {
        "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000000&decimals=8&aggregationMode=2"
    }

    fn env_or_dotenv(name: &str) -> Option<String> {
        env::var(name).ok().or_else(|| {
            let dotenv = Path::new(".env");
            let content = fs::read_to_string(dotenv).ok()?;
            content.lines().find_map(|line| {
                let (key, value) = line.split_once('=')?;
                if key.trim() != name {
                    return None;
                }
                Some(value.trim().trim_matches('"').to_string())
            })
        })
    }

    fn binance_testnet_base_url() -> String {
        env_or_dotenv("BINANCE_FUTURES_BASE_URL")
            .unwrap_or_else(|| "https://testnet.binancefuture.com".to_string())
    }

    #[tokio::test]
    async fn test_price_feed_source_polls_once() {
        let task = parse_oracle_uri(price_uri()).unwrap();
        let source = PriceFeedSource::from_task(&task, 0).unwrap();

        let first = source.poll().await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].nonce, 1);
        assert!(!first[0].payload.is_empty());
        assert_eq!(source.last_nonce().await, Some(1));

        let second = source.poll().await.unwrap();
        assert!(second.is_empty());
    }

    #[tokio::test]
    async fn test_price_feed_source_canonicalizes_observation_order() {
        let uri_a = "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:2000:10000000000:1,source-b:2000:10200000000:2,source-c:2000:9800000000:1";
        let uri_b = "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-c:2000:9800000000:1,source-a:2000:10000000000:1,source-b:2000:10200000000:2";

        let task_a = parse_oracle_uri(uri_a).unwrap();
        let task_b = parse_oracle_uri(uri_b).unwrap();
        let source_a = PriceFeedSource::from_task(&task_a, 0).unwrap();
        let source_b = PriceFeedSource::from_task(&task_b, 0).unwrap();

        let data_a = source_a.poll().await.unwrap();
        let data_b = source_b.poll().await.unwrap();

        assert_eq!(data_a.len(), 1);
        assert_eq!(data_b.len(), 1);
        assert_eq!(data_a[0].payload, data_b[0].payload);
    }

    #[test]
    fn test_price_feed_source_accepts_explicit_data_source_id() {
        let explicit = "0x00000000000000000000000000000000000000000000000000000000000000aa";
        assert_eq!(source_id_from_label(explicit).unwrap(), explicit.parse::<B256>().unwrap());
    }

    #[test]
    fn test_price_feed_source_rejects_duplicate_data_source_id() {
        let uri = "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:2000:10000000000:1,source-a:2000:10200000000:2";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task(&task, 0).unwrap_err();

        assert!(err.to_string().contains("duplicate price observation dataSourceId"));
    }

    #[test]
    fn test_price_feed_source_rejects_invalid_aggregation_mode() {
        let uri = "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=9&observations=source-a:2000:10000000000:1";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task(&task, 0).unwrap_err();

        assert!(err.to_string().contains("invalid price feed aggregationMode"));
    }

    #[test]
    fn test_price_feed_source_rejects_stale_observation() {
        let uri = "gravity://3/1/price_feed?provider=inline_fixture_v1&round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:1900:10000000000:1&maxStaleness=60";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task(&task, 0).unwrap_err();

        assert!(err.to_string().contains("price observation 0 is stale"));
    }

    #[test]
    fn test_price_feed_source_rejects_unknown_provider() {
        let uri = "gravity://3/1/price_feed?provider=hype";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task(&task, 0).unwrap_err();

        assert!(err.to_string().contains("Unsupported price feed provider"));
    }

    #[test]
    fn test_price_feed_source_requires_explicit_provider() {
        let uri = "gravity://3/1/price_feed?round=1&resolvedAt=2010&decimals=8&aggregationMode=1&observations=source-a:2000:10000000000:1";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task(&task, 0).unwrap_err();

        assert!(err.to_string().contains("Missing 'provider' parameter"));
    }

    #[test]
    fn test_binance_interval_ms() {
        assert_eq!(binance_interval_ms("1m").unwrap(), 60_000);
        assert_eq!(binance_interval_ms("4h").unwrap(), 14_400_000);
        assert_eq!(binance_interval_ms("1d").unwrap(), 86_400_000);
        assert!(binance_interval_ms("1x").unwrap_err().to_string().contains("Unsupported"));
    }

    #[test]
    fn test_binance_index_kline_source_config() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();
        let PriceFeedMode::BinanceIndexKline { config, .. } = &source.mode else {
            panic!("expected Binance index kline mode");
        };

        assert_eq!(config.pair, "TSLAUSDT");
        assert_eq!(config.interval, "1m");
        assert_eq!(config.interval_ms, 60_000);
        assert_eq!(config.bucket_start_ms, 1_710_000_000_000);
        let round = config.round_for_delivery_nonce(1).unwrap();
        assert_eq!(round.delivery_nonce, 1);
        assert_eq!(round.bucket_end_ms, 1_710_000_059_999);
        assert_eq!(round.round_id, 28_500_000);
        assert_eq!(
            round.endpoint_url,
            "https://fapi.binance.com/fapi/v1/indexPriceKlines?pair=TSLAUSDT&interval=1m&startTime=1710000000000&endTime=1710000059999&limit=1"
        );
    }

    #[test]
    fn test_binance_index_kline_round_mapping() {
        let uri = "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000000&decimals=8";
        let task = parse_oracle_uri(uri).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 2, Some("https://fapi.binance.com"))
                .unwrap();
        let PriceFeedMode::BinanceIndexKline { config, .. } = &source.mode else {
            panic!("expected Binance index kline mode");
        };

        assert_eq!(source.cursor(), 1_710_000_119_999);
        let round = config.round_for_delivery_nonce(3).unwrap();
        assert_eq!(round.delivery_nonce, 3);
        assert_eq!(round.bucket_start_ms, 1_710_000_120_000);
        assert_eq!(round.bucket_end_ms, 1_710_000_179_999);
        assert_eq!(round.round_id, 28_500_002);
        assert_eq!(round.resolved_at, 1_710_000_179_999);
        assert_eq!(
            round.endpoint_url,
            "https://fapi.binance.com/fapi/v1/indexPriceKlines?pair=TSLAUSDT&interval=1m&startTime=1710000120000&endTime=1710000179999&limit=1"
        );
    }

    #[test]
    fn test_binance_rejects_mismatched_history() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let err = PriceFeedSource::from_task_with_reconciled_cursor(
            &task,
            2,
            Some("https://fapi.binance.com"),
            Some(1_710_000_059_999),
        )
        .unwrap_err();

        assert!(err.to_string().contains("task history mismatch"));
    }

    #[test]
    fn test_binance_rejects_legacy_continuous_parameter() {
        for value in ["true", "false"] {
            let uri = format!("{}&continuous={value}", binance_uri());
            let task = parse_oracle_uri(&uri).unwrap();
            let err =
                PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                    .unwrap_err();

            assert!(err.to_string().contains("price feeds are always continuous"));
        }
    }

    #[test]
    fn test_binance_rejects_derived_time_overrides() {
        for parameter in ["round=1", "resolvedAt=1", "blockNumber=1"] {
            let uri = format!("{}&{}", binance_uri(), parameter);
            let task = parse_oracle_uri(&uri).unwrap();
            let err =
                PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                    .unwrap_err();
            assert!(err.to_string().contains("derived from the delivery bucket"));
        }
    }

    #[test]
    fn test_binance_index_kline_rejects_unaligned_bucket() {
        let uri = "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000001&decimals=8";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task_with_rpc(&task, 0, None).unwrap_err();

        assert!(err.to_string().contains("not aligned"));
    }

    #[test]
    fn test_binance_index_kline_observation_from_response() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();
        let PriceFeedMode::BinanceIndexKline { config, .. } = &source.mode else {
            panic!("expected Binance index kline mode");
        };
        let response = serde_json::json!([[
            1710000000000u64,
            "400.67293",
            "400.67546",
            "400.67293",
            "400.67545",
            "0",
            1710000059999u64,
            "0",
            0,
            "0",
            "0",
            "0"
        ]]);

        let round = config.round_for_delivery_nonce(1).unwrap();
        let observation =
            binance_index_kline_observation_from_response(config, &round, &response).unwrap();

        assert_eq!(observation.observed_at, 1_710_000_059_999);
        assert_eq!(observation.price, "40067545000".parse::<I256>().unwrap());
        assert_eq!(observation.weight, U256::from(1));
    }

    #[test]
    fn test_binance_response_chunk_limit_is_enforced_before_buffer_growth() {
        let mut body = vec![0; MAX_BINANCE_RESPONSE_BYTES - 1];
        append_binance_response_chunk(&mut body, &[1]).unwrap();
        assert_eq!(body.len(), MAX_BINANCE_RESPONSE_BYTES);

        let err = append_binance_response_chunk(&mut body, &[2]).unwrap_err();
        assert!(err.to_string().contains("response exceeds"));
        assert_eq!(body.len(), MAX_BINANCE_RESPONSE_BYTES);
    }

    #[test]
    fn test_binance_index_kline_rejects_wrong_open_time() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();
        let PriceFeedMode::BinanceIndexKline { config, .. } = &source.mode else {
            panic!("expected Binance index kline mode");
        };
        let response = serde_json::json!([[
            1710000060000u64,
            "400.67293",
            "400.67546",
            "400.67293",
            "400.67545",
            "0",
            1710000119999u64
        ]]);

        let round = config.round_for_delivery_nonce(1).unwrap();
        let err =
            binance_index_kline_observation_from_response(config, &round, &response).unwrap_err();

        assert!(err.to_string().contains("openTime mismatch"));
    }

    #[tokio::test]
    #[ignore = "requires outbound access to Binance Futures testnet"]
    async fn test_binance_index_kline_live_testnet_poll() {
        let base_url = binance_testnet_base_url();
        let pair = env_or_dotenv("BINANCE_INDEX_PAIR").unwrap_or_else(|| "TSLAUSDT".to_string());
        let client = Client::builder().no_proxy().use_rustls_tls().build().unwrap();
        let mut time_url = Url::parse(&base_url).unwrap();
        time_url.set_path("/fapi/v1/time");
        time_url.set_query(None);
        time_url.set_fragment(None);
        let server_time = client
            .get(time_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        let now_ms = server_time.get("serverTime").and_then(Value::as_u64).unwrap();
        let interval_ms = binance_interval_ms("1m").unwrap();
        let bucket_start_ms = now_ms.saturating_sub(2 * interval_ms) / interval_ms * interval_ms;
        let uri = format!(
            "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair={pair}&interval=1m&bucketStartMs={bucket_start_ms}&decimals=8&aggregationMode=2&graceMs=0"
        );
        let task = parse_oracle_uri(&uri).unwrap();
        let source = PriceFeedSource::from_task_with_rpc(&task, 0, Some(&base_url)).unwrap();

        let first = source.poll().await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].nonce, 1);
        assert!(!first[0].payload.is_empty());
        assert_eq!(source.last_nonce().await, Some(1));

        let second = source.poll().await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].nonce, 2);
        assert!(!second[0].payload.is_empty());
        assert_eq!(source.last_nonce().await, Some(2));

        let third = source.poll().await.unwrap();
        assert!(third.is_empty());
    }

    #[test]
    fn test_fixed_decimal_truncates_to_configured_decimals() {
        assert_eq!(parse_fixed_decimal("195.389", 2).unwrap(), "19538".parse::<I256>().unwrap());
        assert_eq!(parse_fixed_decimal("195", 8).unwrap(), "19500000000".parse::<I256>().unwrap());
    }
}
