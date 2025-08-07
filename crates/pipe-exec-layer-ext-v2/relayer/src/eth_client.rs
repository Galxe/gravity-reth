use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Log, Filter};
use anyhow::{Context as AnyhowContext, Result};
use serde::{Deserialize, Deserializer};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};
use url::Url;

/// Provider性能指标
#[derive(Debug, Default, Clone)]
pub struct ProviderMetrics {
    /// 发送的请求数
    pub requests_sent: u64,
    /// 成功的请求数
    pub requests_succeeded: u64,
    /// 失败的请求数
    pub requests_failed: u64,
    /// 总延迟时间（毫秒）
    pub total_latency_ms: u64,
}

/// 未使用的反序列化函数
#[allow(dead_code)]
fn deserialize_hex_to_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    if s.starts_with("0x") {
        usize::from_str_radix(&s[2..], 16).map_err(serde::de::Error::custom)
    } else {
        s.parse::<usize>().map_err(serde::de::Error::custom)
    }
}

/// Ethereum transaction sender, providing reliable communication with nodes
#[derive(Clone, Debug)]
pub struct EthHttpCli {
    inner: Vec<Arc<RootProvider<Ethereum>>>,
    chain_id: u64,
    metrics: Arc<tokio::sync::Mutex<ProviderMetrics>>,
    retry_config: RetryConfig,
    rpc: Arc<String>,
}

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 最大重试次数
    pub max_retries: usize,
    /// 基础延迟时间
    pub base_delay: Duration,
    /// 最大延迟时间
    pub max_delay: Duration,
    /// 退避倍数
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl EthHttpCli {
    /// 获取RPC URL
    pub fn rpc(&self) -> Arc<String> {
        self.rpc.clone()
    }

    /// Create new TxnSender instance
    pub fn new(rpc_url: &str, chain_id: u64) -> Result<Self> {
        debug!(
            "Creating TxnSender for URL: {}, Chain ID: {}",
            rpc_url, chain_id
        );
        // Parse URL

        let url =
            Url::parse(rpc_url).with_context(|| format!("Failed to parse RPC URL: {}", rpc_url))?;
        let mut inner = Vec::new();
        for _ in 0..1 {
            // let client = reqwest::Client::builder()
            //     // .pool_idle_timeout(Duration::from_secs(120))
            //     // .pool_max_idle_per_host(2000)
            //     // .connect_timeout(Duration::from_secs(10))
            //     // .timeout(Duration::from_secs(5))
            //     // .tcp_keepalive(Duration::from_secs(30))
            //     // .tcp_nodelay(true)
            //     // .http2_prior_knowledge()
            //     // .http2_adaptive_window(true)
            //     // .http2_keep_alive_timeout(Duration::from_secs(10))
            //     // .no_gzip()
            //     // .no_brotli()
            //     // .no_deflate()
            //     // .no_zstd()
            //     .build()
            //     .unwrap();

            let provider: RootProvider<Ethereum> =
                ProviderBuilder::default().connect_http(url.clone());

            inner.push(Arc::new(provider));
        }

        let txn_sender = Self {
            rpc: Arc::new(rpc_url.to_string()),
            inner,
            chain_id,
            metrics: Arc::new(tokio::sync::Mutex::new(ProviderMetrics::default())),
            retry_config: RetryConfig::default(),
        };

        // Verify connection

        debug!("TxnSender created successfully");
        Ok(txn_sender)
    }

    /// 获取链ID
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// 获取地址的nonce
    pub async fn get_nonce(&self, address: Address) -> Result<u64> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let nonce = self.inner[0].get_transaction_count(address).await?;
            Ok(nonce)
        })
        .await?
    }

    /// Verify network connection
    #[allow(unused)]
    async fn verify_connection(&self) -> Result<()> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async {
                let _block_number = self.inner[0].get_block_number().await?;
                Ok(())
            })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to verify connection to Ethereum node")
    }

    /// Get account transaction count (nonce)
    pub async fn get_transaction_count(&self, address: Address) -> Result<u64> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async { self.inner[0].get_transaction_count(address).await })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result
            .with_context(|| format!("Failed to get transaction count for address: {:?}", address))
    }

    /// Get account balance
    pub async fn get_balance(&self, address: &Address) -> Result<U256> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async { self.inner[0].get_balance(*address).await })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| format!("Failed to get balance for address: {:?}", address))
    }

    /// 获取合约事件日志
    pub async fn get_eth_logs(&self, address: Address, _topic: Vec<B256>) -> Result<Vec<Log>> {
        let _start = Instant::now();

        let filter = Filter::new().address(address);
        // TODO: 实现topic过滤
        // .topic(_topic)
        match self.inner[0].get_logs(&filter).await {
            Ok(logs) => Ok(logs),
            Err(e) => Err(anyhow::anyhow!("Failed to get logs: {:?}", e)),
        }
    }

    /// Get current gas price
    #[allow(unused)]
    pub async fn get_gas_price(&self) -> Result<u128> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async { self.inner[0].get_gas_price().await })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result
            .map_err(|e| anyhow::anyhow!("Failed to get gas price: {:?}", e))
            .with_context(|| "Failed to get gas price")
    }

    /// Get latest block number
    #[allow(unused)]
    pub async fn get_block_number(&self) -> Result<u64> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async { self.inner[0].get_block_number().await })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to get block number")
    }

    /// Execute operation with retry mechanism
    async fn retry_with_backoff<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, alloy_transport::TransportError>>,
    {
        let mut last_error = None;

        for attempt in 0..=self.retry_config.max_retries {
            match operation().await {
                Ok(result) => {
                    if attempt > 0 {
                        debug!("Operation succeeded on attempt {}", attempt + 1);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.retry_config.max_retries {
                        let delay = std::cmp::min(
                            Duration::from_millis(
                                (self.retry_config.base_delay.as_millis() as f64
                                    * self.retry_config.backoff_multiplier.powi(attempt as i32))
                                    as u64,
                            ),
                            self.retry_config.max_delay,
                        );
                        warn!(
                            "Operation failed on attempt {}, retrying in {:?}: {:?}",
                            attempt + 1,
                            delay,
                            last_error
                        );
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Operation failed after {} attempts. Last error: {:?}",
            self.retry_config.max_retries + 1,
            last_error
        ))
    }

    /// Update performance metrics
    async fn update_metrics(&self, success: bool, latency: Duration) {
        let mut metrics = self.metrics.lock().await;
        metrics.requests_sent += 1;

        if success {
            metrics.requests_succeeded += 1;
        } else {
            metrics.requests_failed += 1;
        }

        // Ensure at least 1ms latency is recorded to avoid 0 latency in very fast environments
        let latency_ms = std::cmp::max(1, latency.as_millis() as u64);
        metrics.total_latency_ms += latency_ms;
    }

    /// Get a copy of performance metrics
    #[allow(unused)]
    pub async fn get_metrics(&self) -> ProviderMetrics {
        self.metrics.lock().await.clone()
    }

    /// Get average latency (milliseconds)
    #[allow(unused)]
    pub async fn get_average_latency_ms(&self) -> f64 {
        let metrics = self.metrics.lock().await;
        if metrics.requests_sent > 0 {
            metrics.total_latency_ms as f64 / metrics.requests_sent as f64
        } else {
            0.0
        }
    }

    /// Get success rate
    #[allow(unused)]
    pub async fn get_success_rate(&self) -> f64 {
        let metrics = self.metrics.lock().await;
        if metrics.requests_sent > 0 {
            metrics.requests_succeeded as f64 / metrics.requests_sent as f64
        } else {
            0.0
        }
    }

    /// Reset metrics
    #[allow(unused)]
    pub async fn reset_metrics(&self) {
        let mut metrics = self.metrics.lock().await;
        *metrics = ProviderMetrics::default();
        debug!("TxnSender metrics reset");
    }
}
