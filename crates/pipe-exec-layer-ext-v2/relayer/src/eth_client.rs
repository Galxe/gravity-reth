use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use anyhow::{Context as AnyhowContext, Result};
use reqwest::ClientBuilder;
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
/// Ethereum transaction sender, providing reliable communication with nodes
#[derive(Clone, Debug)]
pub struct EthHttpCli {
    provider: RootProvider<Ethereum>,
    metrics: Arc<tokio::sync::Mutex<ProviderMetrics>>,
    retry_config: RetryConfig,
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
    /// Create new TxnSender instance
    pub fn new(rpc_url: &str) -> Self {
        debug!("Creating EthHttpCli for URL: {}", rpc_url);
        // Parse URL

        let url = Url::parse(rpc_url).unwrap();
        let client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();
        let client = client_builder.build().unwrap();
        let provider: RootProvider<Ethereum> =
            ProviderBuilder::default().connect_reqwest(client, url.clone());

        Self {
            provider,
            metrics: Arc::new(tokio::sync::Mutex::new(ProviderMetrics::default())),
            retry_config: RetryConfig::default(),
        }
    }

    pub async fn get_nonce(&self, address: Address) -> Result<u64> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let nonce = self.provider.get_transaction_count(address).await?;
            Ok(nonce)
        })
        .await?
    }

    /// Verify network connection
    #[allow(unused)]
    async fn verify_connection(&self) -> Result<()> {
        self.get_block_number().await.map(|_| ())
    }

    /// Get account transaction count (nonce)
    pub async fn get_transaction_count(&self, address: Address) -> Result<u64> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async { self.provider.get_transaction_count(address).await })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result
            .with_context(|| format!("Failed to get transaction count for address: {:?}", address))
    }

    /// Get account balance
    pub async fn get_balance(&self, address: &Address) -> Result<U256> {
        let start = Instant::now();

        let result =
            self.retry_with_backoff(|| async { self.provider.get_balance(*address).await }).await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| format!("Failed to get balance for address: {:?}", address))
    }

    /// 获取事件日志 - 支持完整的Filter对象
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let start = Instant::now();

        let result =
            self.retry_with_backoff(|| async { self.provider.get_logs(filter).await }).await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to get logs with filter")
    }

    pub async fn get_storage_at(&self, address: Address, slot: B256) -> Result<B256> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async {
                self.provider.get_storage_at(address, slot.into()).await
            })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;
        result.map(|v| v.into()).with_context(|| {
            format!("Failed to get storage at address: {:?}, slot: {:?}", address, slot)
        })
    }

    pub async fn get_block(&self, block_number: u64) -> Result<Option<alloy_rpc_types::Block>> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async {
                self.provider
                    .get_block_by_number(alloy_rpc_types::BlockNumberOrTag::Number(block_number))
                    .await
            })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| format!("Failed to get block: {}", block_number))
    }

    pub async fn get_finalized_block_number(&self) -> Result<u64> {
        let start = Instant::now();

        let result = self
            .retry_with_backoff(|| async {
                match self
                    .provider
                    .get_block_by_number(alloy_rpc_types::BlockNumberOrTag::Finalized)
                    .await?
                {
                    Some(block) => Ok(block.header.number),
                    None => Err(alloy_transport::TransportError::UnsupportedFeature(
                        "No finalized block found".into(),
                    )),
                }
            })
            .await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to get finalized block number")
    }

    #[allow(unused)]
    pub async fn get_gas_price(&self) -> Result<u128> {
        let start = Instant::now();

        let result =
            self.retry_with_backoff(|| async { self.provider.get_gas_price().await }).await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result
            .map_err(|e| anyhow::anyhow!("Failed to get gas price: {:?}", e))
            .with_context(|| "Failed to get gas price")
    }

    #[allow(unused)]
    pub async fn get_block_number(&self) -> Result<u64> {
        let start = Instant::now();

        let result =
            self.retry_with_backoff(|| async { self.provider.get_block_number().await }).await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to get block number")
    }

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

#[cfg(test)]
mod tests {
    use std::env;

    use reqwest::{Client, ClientBuilder, Proxy};

    use super::*;

    #[tokio::test]
    async fn test_get_logs() {
        let url = reqwest::Url::parse("https://ethereum-holesky-rpc.publicnode.com").unwrap();
        let client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();
        let client = client_builder.build().unwrap();
        let provider: RootProvider<Ethereum> =
            ProviderBuilder::default().connect_reqwest(client, url);

        let block_number = provider.get_block_number().await.unwrap();
        println!("block_number: {}", block_number);

        let finalized_block_number = provider
            .get_block_by_number(alloy_rpc_types::BlockNumberOrTag::Finalized)
            .await
            .unwrap();
        println!("finalized_block_number: {:?}", finalized_block_number);

        let block = provider
            .get_block_by_number(alloy_rpc_types::BlockNumberOrTag::Number(block_number))
            .await
            .unwrap();
        println!("block: {:?}", block);

        let filter = Filter::new().from_block(10000000).to_block(10000001);
        let logs = provider.get_logs(&filter).await.unwrap();
        println!("logs: {:?}", logs);
    }

    #[tokio::test]
    async fn test_get_finalized_block_number() {
        for var in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"] {
            std::env::remove_var(var);
        }
        println!("http proxy: {}", env::var("http_proxy").unwrap_or_default());
        println!("https proxy: {}", env::var("https_proxy").unwrap_or_default());
        // let proxy = Proxy::all("http://127.0.0.1:20172").unwrap();
        // let client = Client::builder().proxy(proxy).build().unwrap();

        let url = reqwest::Url::parse("https://ethereum-holesky-rpc.publicnode.com").unwrap();
        let client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();
        let client = client_builder.build().unwrap();
        let provider: RootProvider<Ethereum> =
            ProviderBuilder::default().connect_reqwest(client, url);
        let block_number = provider.get_block_number().await.unwrap();
        println!("block_number: {}", block_number);

        // let res = client
        //     .post("https://ethereum-holesky-rpc.publicnode.com")
        //     .header("Content-Type", "application/json")
        //     .body(r#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"#)
        //     .send()
        //     .await
        //     .unwrap();

        // println!("Response: {:?}", res);
    }
}
