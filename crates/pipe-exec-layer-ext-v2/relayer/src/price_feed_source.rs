//! Binance index-price feed source.
//!
//! Each delivery nonce maps to one immutable, closed Binance USD-M
//! `indexPriceKlines` bucket. The adapter verifies the exact bucket timestamps
//! and emits one close price through the existing UnsupportedJWK consensus path.

use crate::{
    data_source::{source_types, OracleData, OracleDataSource},
    uri_parser::ParsedOracleTask,
};
use alloy_primitives::{Bytes, I256, U256};
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

const PROVIDER_BINANCE_INDEX_KLINE: &str = "binance_index_kline_v1";
const DEFAULT_BINANCE_GRACE_MS: u64 = 120_000;
const BINANCE_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BINANCE_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_PRICE_DECIMALS: u8 = 18;
const BINANCE_TASK_PARAMETERS: &[&str] =
    &["provider", "pair", "interval", "bucketStartMs", "decimals", "graceMs"];

sol! {
    struct PricePayloadSol {
        uint256 feedId;
        uint64 roundId;
        uint64 resolvedAt;
        uint8 decimals;
        int256 price;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct LastPriceRound {
    nonce: u128,
    source_position: u64,
}

impl LastPriceRound {
    const fn is_initialized(self) -> bool {
        self.nonce > 0
    }
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
}

#[derive(Debug, Clone)]
struct BinanceIndexKlineRound {
    endpoint_url: String,
    bucket_start_ms: u64,
    bucket_end_ms: u64,
    round_id: u64,
    resolved_at: u64,
    delivery_nonce: u128,
    source_position: u64,
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
            source_position: bucket_end_ms,
        })
    }
}

/// Price feed data source for `sourceType=3`.
#[derive(Debug)]
pub struct PriceFeedSource {
    feed_id: u64,
    config: BinanceIndexKlineConfig,
    client: Client,
    cursor: AtomicU64,
    last_round: Mutex<LastPriceRound>,
}

impl PriceFeedSource {
    /// Create a source without a validator-local endpoint mapping.
    ///
    /// Binance sources require a URL mapping, so production callers use
    /// [`Self::from_task_with_rpc`] or [`Self::from_task_with_progress`].
    pub fn from_task(task: &ParsedOracleTask, latest_onchain_nonce: u128) -> Result<Self> {
        Self::from_task_with_rpc(task, latest_onchain_nonce, None)
    }

    /// Create a source with the validator-local Binance base URL.
    pub fn from_task_with_rpc(
        task: &ParsedOracleTask,
        latest_onchain_nonce: u128,
        rpc_url: Option<&str>,
    ) -> Result<Self> {
        Self::from_task_with_progress(task, latest_onchain_nonce, 0, rpc_url)
    }

    pub(crate) fn from_task_with_progress(
        task: &ParsedOracleTask,
        latest_onchain_nonce: u128,
        latest_onchain_position: u64,
        rpc_url: Option<&str>,
    ) -> Result<Self> {
        if task.source_type != source_types::PRICE_FEED {
            return Err(anyhow!("PriceFeedSource requires sourceType={}", source_types::PRICE_FEED));
        }
        if task.task_type != "price_feed" {
            return Err(anyhow!(
                "PriceFeedSource requires task type 'price_feed', got '{}'",
                task.task_type
            ));
        }

        match task.params.get("provider").map(String::as_str) {
            Some(PROVIDER_BINANCE_INDEX_KLINE) => {}
            Some(provider) => return Err(anyhow!("Unsupported price feed provider '{provider}'")),
            None => return Err(anyhow!("Missing 'provider' parameter for price feed")),
        }

        if task.params.contains_key("baseUrl") {
            return Err(anyhow!(
                "Binance baseUrl must be validator-local relayer config, not an on-chain URI parameter"
            ));
        }
        for parameter in task.params.keys() {
            if !BINANCE_TASK_PARAMETERS.contains(&parameter.as_str()) {
                return Err(anyhow!("Unsupported Binance index kline parameter '{parameter}'"));
            }
        }

        let pair = task
            .params
            .get("pair")
            .ok_or_else(|| anyhow!("Missing 'pair' parameter for Binance index kline price feed"))?
            .clone();
        validate_binance_pair(&pair)?;
        let interval = task.params.get("interval").cloned().unwrap_or_else(|| "1m".to_string());
        let interval_ms = binance_interval_ms(&interval)?;
        let bucket_start_ms = parse_required::<u64>(task, "bucketStartMs")?;
        if bucket_start_ms % interval_ms != 0 {
            return Err(anyhow!(
                "Binance index kline bucketStartMs {} is not aligned to interval {}",
                bucket_start_ms,
                interval
            ));
        }
        let decimals = parse_required::<u8>(task, "decimals")?;
        if decimals > MAX_PRICE_DECIMALS {
            return Err(anyhow!(
                "price feed decimals {} exceeds maximum {}",
                decimals,
                MAX_PRICE_DECIMALS
            ));
        }
        let grace_ms = parse_optional(task, "graceMs")?.unwrap_or(DEFAULT_BINANCE_GRACE_MS);
        let base_url = binance_base_url(rpc_url)?;
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
        };

        if latest_onchain_nonce == 0 && latest_onchain_position != 0 {
            return Err(anyhow!("Binance task has zero nonce with nonzero source position"));
        }
        let previous_position = if latest_onchain_nonce == 0 {
            None
        } else {
            let expected = config.round_for_delivery_nonce(latest_onchain_nonce)?.source_position;
            if latest_onchain_position != 0 && latest_onchain_position != expected {
                return Err(anyhow!(
                    "Binance task history mismatch: nonce {} implies source position {}, confirmed position is {}; use a new feedId for a new bucket origin or interval",
                    latest_onchain_nonce,
                    expected,
                    latest_onchain_position
                ));
            }
            Some(expected)
        };
        let last_round = previous_position
            .map(|source_position| LastPriceRound { nonce: latest_onchain_nonce, source_position })
            .unwrap_or_default();
        let initial_cursor = previous_position.unwrap_or(0);

        info!(
            target: "price_feed_source",
            feed_id = task.source_id,
            provider = PROVIDER_BINANCE_INDEX_KLINE,
            pair = config.pair.as_str(),
            interval = config.interval.as_str(),
            bucket_start_ms,
            latest_onchain_nonce,
            "Created Binance index kline PriceFeedSource"
        );

        Ok(Self {
            feed_id: task.source_id,
            config,
            client,
            cursor: AtomicU64::new(initial_cursor),
            last_round: Mutex::new(last_round),
        })
    }

    /// Last delivery nonce returned or reconciled from `NativeOracle`.
    pub async fn last_nonce(&self) -> Option<u128> {
        let state = *self.last_round.lock().await;
        state.is_initialized().then_some(state.nonce)
    }

    /// Bucket close timestamp associated with the last delivery nonce.
    pub async fn last_nonce_position(&self) -> Option<u64> {
        let state = *self.last_round.lock().await;
        state.is_initialized().then_some(state.source_position)
    }

    /// Reconcile local state with a round already recorded by `NativeOracle`.
    pub async fn reconcile_progress(&self, nonce: u128, source_position: u64) -> Result<()> {
        if nonce == 0 {
            if source_position != 0 {
                return Err(anyhow!("Binance task has zero nonce with nonzero source position"));
            }
            *self.last_round.lock().await = LastPriceRound::default();
            self.cursor.store(0, Ordering::Relaxed);
            return Ok(());
        }

        let expected = self.config.round_for_delivery_nonce(nonce)?.source_position;
        if source_position != 0 && source_position != expected {
            return Err(anyhow!(
                "Binance task history mismatch: nonce {} implies source position {}, confirmed position is {}; use a new feedId for a new bucket origin or interval",
                nonce,
                expected,
                source_position
            ));
        }

        let mut state = self.last_round.lock().await;
        if nonce < state.nonce {
            return Err(anyhow!(
                "Binance task cannot reconcile backwards from nonce {} to {}",
                state.nonce,
                nonce
            ));
        }
        *state = LastPriceRound { nonce, source_position: expected };
        self.cursor.store(expected, Ordering::Relaxed);
        Ok(())
    }

    /// Current bucket-close cursor used for relayer persistence.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Feed identifier used as `NativeOracle` sourceId.
    pub const fn feed_id(&self) -> u64 {
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
        let next_delivery_nonce = state
            .nonce
            .checked_add(1)
            .ok_or_else(|| anyhow!("Binance index kline delivery nonce overflow"))?;
        let round = self.config.round_for_delivery_nonce(next_delivery_nonce)?;
        if !is_binance_bucket_ready(&self.config, &round)? {
            return Ok(vec![]);
        }

        let price = fetch_binance_index_kline_price(&self.client, &self.config, &round).await?;
        let resolver_payload = encode_price_payload(
            self.feed_id,
            round.round_id,
            round.resolved_at,
            self.config.decimals,
            price,
        );
        let wrapped_payload = SolValue::abi_encode(&(
            round.delivery_nonce,
            U256::from(round.source_position),
            resolver_payload.as_slice(),
        ));

        state.nonce = round.delivery_nonce;
        state.source_position = round.source_position;
        self.cursor.store(round.source_position, Ordering::Relaxed);

        Ok(vec![OracleData {
            nonce: round.delivery_nonce,
            source_position: round.source_position,
            payload: Bytes::from(wrapped_payload),
        }])
    }
}

async fn fetch_binance_index_kline_price(
    client: &Client,
    config: &BinanceIndexKlineConfig,
    round: &BinanceIndexKlineRound,
) -> Result<I256> {
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

    binance_index_kline_price_from_response(config, round, &response)
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

fn binance_index_kline_price_from_response(
    config: &BinanceIndexKlineConfig,
    round: &BinanceIndexKlineRound,
    response: &Value,
) -> Result<I256> {
    let close = parse_binance_index_kline_close(round, response)?;
    let price = parse_fixed_decimal(close, config.decimals)?;
    if price <= I256::ZERO {
        return Err(anyhow!("Binance index price kline close must be positive"));
    }
    Ok(price)
}

fn parse_binance_index_kline_close<'a>(
    round: &BinanceIndexKlineRound,
    response: &'a Value,
) -> Result<&'a str> {
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

    Ok(close)
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
        _ => Err(anyhow!("Unsupported Binance index kline interval '{interval}'")),
    }
}

fn encode_price_payload(
    feed_id: u64,
    round_id: u64,
    resolved_at: u64,
    decimals: u8,
    price: I256,
) -> Vec<u8> {
    PricePayloadSol {
        feedId: U256::from(feed_id),
        roundId: round_id,
        resolvedAt: resolved_at,
        decimals,
        price,
    }
    .abi_encode()
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
    if fraction.len() > decimals && fraction[decimals..].bytes().any(|byte| byte != b'0') {
        return Err(anyhow!("decimal price precision exceeds configured decimals"));
    }
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
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    fn binance_uri() -> &'static str {
        "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000000&decimals=8"
    }

    fn serve_once(body: &'static str) -> (String, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn test_price_feed_requires_binance_provider() {
        for uri in [
            "gravity://3/1/price_feed?pair=TSLAUSDT",
            "gravity://3/1/price_feed?provider=hype",
            "gravity://3/1/price_feed?provider=inline_fixture_v1",
        ] {
            let task = parse_oracle_uri(uri).unwrap();
            let err = PriceFeedSource::from_task(&task, 0).unwrap_err();
            assert!(err.to_string().contains("provider"), "unexpected error: {err}");
        }
    }

    #[test]
    fn test_binance_index_kline_source_config() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();

        assert_eq!(source.config.pair, "TSLAUSDT");
        assert_eq!(source.config.interval, "1m");
        assert_eq!(source.config.interval_ms, 60_000);
        assert_eq!(source.config.bucket_start_ms, 1_710_000_000_000);
        let round = source.config.round_for_delivery_nonce(1).unwrap();
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
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 2, Some("https://fapi.binance.com"))
                .unwrap();

        assert_eq!(source.cursor(), 1_710_000_119_999);
        let round = source.config.round_for_delivery_nonce(3).unwrap();
        assert_eq!(round.delivery_nonce, 3);
        assert_eq!(round.bucket_start_ms, 1_710_000_120_000);
        assert_eq!(round.bucket_end_ms, 1_710_000_179_999);
        assert_eq!(round.round_id, 28_500_002);
        assert_eq!(round.resolved_at, 1_710_000_179_999);
    }

    #[test]
    fn test_binance_rejects_mismatched_history() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let err = PriceFeedSource::from_task_with_progress(
            &task,
            2,
            1_710_000_059_999,
            Some("https://fapi.binance.com"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("task history mismatch"));
    }

    #[tokio::test]
    async fn test_binance_reconcile_validates_history() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 1, Some("https://fapi.binance.com"))
                .unwrap();

        source.reconcile_progress(2, 1_710_000_119_999).await.unwrap();
        assert_eq!(source.last_nonce().await, Some(2));
        assert_eq!(source.last_nonce_position().await, Some(1_710_000_119_999));

        let error = source.reconcile_progress(3, 1_710_000_120_000).await.unwrap_err();
        assert!(error.to_string().contains("task history mismatch"));
    }

    #[test]
    fn test_binance_rejects_removed_or_derived_parameters() {
        for parameter in [
            "aggregationMode=2",
            "weight=1",
            "minSourceCount=1",
            "minTotalWeight=1",
            "maxStaleness=180000",
            "observations=source-a:1:1:1",
            "field=close",
            "dataSourceLabel=binance",
            "dataSourceId=0x01",
            "continuous=true",
            "round=1",
            "resolvedAt=1",
            "blockNumber=1",
        ] {
            let uri = format!("{}&{parameter}", binance_uri());
            let task = parse_oracle_uri(&uri).unwrap();
            let err =
                PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                    .unwrap_err();
            assert!(
                err.to_string().contains("Unsupported Binance index kline parameter"),
                "parameter {parameter}: {err}"
            );
        }
    }

    #[test]
    fn test_binance_rejects_onchain_base_url() {
        let uri = format!("{}&baseUrl=https://example.com", binance_uri());
        let task = parse_oracle_uri(&uri).unwrap();
        let err = PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
            .unwrap_err();
        assert!(err.to_string().contains("validator-local"));
    }

    #[test]
    fn test_binance_rejects_unaligned_bucket() {
        let uri = "gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000001&decimals=8";
        let task = parse_oracle_uri(uri).unwrap();
        let err = PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
            .unwrap_err();

        assert!(err.to_string().contains("not aligned"));
    }

    #[test]
    fn test_binance_index_kline_price_from_response() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();
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

        let round = source.config.round_for_delivery_nonce(1).unwrap();
        let price =
            binance_index_kline_price_from_response(&source.config, &round, &response).unwrap();
        assert_eq!(price, "40067545000".parse::<I256>().unwrap());
    }

    #[tokio::test]
    async fn test_binance_poll_fetches_and_wraps_exact_closed_bucket() {
        let (base_url, server) = serve_once(
            r#"[[1710000000000,"400.67293","400.67546","400.67293","400.67545","0",1710000059999]]"#,
        );
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source = PriceFeedSource::from_task_with_rpc(&task, 0, Some(&base_url)).unwrap();

        let data = source.poll().await.unwrap();

        assert_eq!(data.len(), 1);
        assert_eq!(data[0].nonce, 1);
        assert_eq!(data[0].source_position, 1_710_000_059_999);
        let (nonce, position, payload) =
            <(u128, U256, Bytes)>::abi_decode(&data[0].payload).unwrap();
        assert_eq!(nonce, 1);
        assert_eq!(position, U256::from(1_710_000_059_999u64));
        let payload = PricePayloadSol::abi_decode(&payload).unwrap();
        assert_eq!(payload.feedId, U256::from(2001));
        assert_eq!(payload.roundId, 28_500_000);
        assert_eq!(payload.resolvedAt, 1_710_000_059_999);
        assert_eq!(payload.decimals, 8);
        assert_eq!(payload.price, "40067545000".parse::<I256>().unwrap());
        assert_eq!(source.last_nonce().await, Some(1));
        assert_eq!(source.last_nonce_position().await, Some(1_710_000_059_999));

        let request = server.join().unwrap();
        assert!(request.starts_with(
            "GET /fapi/v1/indexPriceKlines?pair=TSLAUSDT&interval=1m&startTime=1710000000000&endTime=1710000059999&limit=1 HTTP/1.1\r\n"
        ));
    }

    #[test]
    fn test_binance_index_kline_rejects_zero_price() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();
        let response =
            serde_json::json!([[1710000000000u64, "0", "0", "0", "0", "0", 1710000059999u64]]);

        let round = source.config.round_for_delivery_nonce(1).unwrap();
        let err =
            binance_index_kline_price_from_response(&source.config, &round, &response).unwrap_err();
        assert!(err.to_string().contains("must be positive"));
    }

    #[test]
    fn test_binance_index_kline_rejects_wrong_open_time() {
        let task = parse_oracle_uri(binance_uri()).unwrap();
        let source =
            PriceFeedSource::from_task_with_rpc(&task, 0, Some("https://fapi.binance.com"))
                .unwrap();
        let response = serde_json::json!([[
            1710000060000u64,
            "400.67293",
            "400.67546",
            "400.67293",
            "400.67545",
            "0",
            1710000119999u64
        ]]);

        let round = source.config.round_for_delivery_nonce(1).unwrap();
        let err =
            binance_index_kline_price_from_response(&source.config, &round, &response).unwrap_err();
        assert!(err.to_string().contains("openTime mismatch"));
    }

    #[test]
    fn test_price_payload_has_single_price() {
        let encoded = encode_price_payload(
            2001,
            28_500_000,
            1_710_000_059_999,
            8,
            "40067545000".parse::<I256>().unwrap(),
        );
        let payload = PricePayloadSol::abi_decode(&encoded).unwrap();

        assert_eq!(payload.feedId, U256::from(2001));
        assert_eq!(payload.roundId, 28_500_000);
        assert_eq!(payload.resolvedAt, 1_710_000_059_999);
        assert_eq!(payload.decimals, 8);
        assert_eq!(payload.price, "40067545000".parse::<I256>().unwrap());
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
    fn test_fixed_decimal_requires_exact_configured_precision() {
        assert_eq!(parse_fixed_decimal("195.380", 2).unwrap(), "19538".parse::<I256>().unwrap());
        assert_eq!(parse_fixed_decimal("195", 8).unwrap(), "19500000000".parse::<I256>().unwrap());
        assert!(parse_fixed_decimal("195.389", 2).unwrap_err().to_string().contains("precision"));
    }
}
