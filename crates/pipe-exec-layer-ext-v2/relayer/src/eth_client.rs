use alloy_network::Ethereum;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use anyhow::{Context as AnyhowContext, Result};
use reqwest::ClientBuilder;
use std::{sync::Arc, time::Instant};
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};
use url::Url;

/// Provider performance metrics
#[derive(Debug, Default, Clone)]
pub struct ProviderMetrics {
    /// Number of requests sent
    pub requests_sent: u64,
    /// Number of successful requests
    pub requests_succeeded: u64,
    /// Number of failed requests
    pub requests_failed: u64,
    /// Total latency time (milliseconds)
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
    /// Maximum number of retries
    pub max_retries: usize,
    /// Base delay time
    pub base_delay: Duration,
    /// Maximum delay time
    pub max_delay: Duration,
    /// Backoff multiplier
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
    /// Creates a new EthHttpCli instance
    ///
    /// # Arguments
    /// * `rpc_url` - The RPC endpoint URL for blockchain communication
    ///
    /// # Returns
    /// * `Result<EthHttpCli>` - A new Ethereum HTTP client instance or error
    ///
    /// # Errors
    /// * Returns an error if the URL cannot be parsed or client cannot be built
    pub fn new(rpc_url: &str) -> Result<Self> {
        debug!("Creating EthHttpCli for URL: {}", rpc_url);

        // Parse URL with error handling
        let url =
            Url::parse(rpc_url).with_context(|| format!("Failed to parse RPC URL: {}", rpc_url))?;

        // Build HTTP client with error handling
        let client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();
        let client = client_builder.build().with_context(|| "Failed to build HTTP client")?;

        let provider: RootProvider<Ethereum> =
            ProviderBuilder::default().connect_reqwest(client, url.clone());

        Ok(Self {
            provider,
            metrics: Arc::new(tokio::sync::Mutex::new(ProviderMetrics::default())),
            retry_config: RetryConfig::default(),
        })
    }

    /// Gets the nonce (transaction count) for a given address
    ///
    /// # Arguments
    /// * `address` - The Ethereum address to get the nonce for
    ///
    /// # Returns
    /// * `Result<u64>` - The nonce value or error
    ///
    /// # Errors
    /// * Returns an error if the request times out or fails
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

    /// Get event logs - supports complete Filter object
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let start = Instant::now();

        let result =
            self.retry_with_backoff(|| async { self.provider.get_logs(filter).await }).await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to get logs with filter")
    }

    /// Gets the storage value at a specific slot for a given address
    ///
    /// # Arguments
    /// * `address` - The contract address to query
    /// * `slot` - The storage slot to read from
    ///
    /// # Returns
    /// * `Result<B256>` - The storage value or error
    ///
    /// # Errors
    /// * Returns an error if the storage query fails
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

    /// Gets a block by its number
    ///
    /// # Arguments
    /// * `block_number` - The block number to retrieve
    ///
    /// # Returns
    /// * `Result<Option<alloy_rpc_types::Block>>` - The block data or None if not found
    ///
    /// # Errors
    /// * Returns an error if the block query fails
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

    /// Gets the latest finalized block number
    ///
    /// # Returns
    /// * `Result<u64>` - The finalized block number or error
    ///
    /// # Errors
    /// * Returns an error if no finalized block is found or the query fails
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

    /// Gets the current gas price
    ///
    /// # Returns
    /// * `Result<u128>` - The gas price in wei or error
    ///
    /// # Errors
    /// * Returns an error if the gas price query fails
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

    /// Gets the latest block number
    ///
    /// # Returns
    /// * `Result<u64>` - The latest block number or error
    ///
    /// # Errors
    /// * Returns an error if the block number query fails
    #[allow(unused)]
    pub async fn get_block_number(&self) -> Result<u64> {
        let start = Instant::now();

        let result =
            self.retry_with_backoff(|| async { self.provider.get_block_number().await }).await;

        self.update_metrics(result.is_ok(), start.elapsed()).await;

        result.with_context(|| "Failed to get block number")
    }

    /// Retries an operation with exponential backoff
    ///
    /// # Arguments
    /// * `operation` - The async operation to retry
    ///
    /// # Returns
    /// * `Result<T>` - The result of the operation or error after all retries
    ///
    /// # Errors
    /// * Returns an error if all retry attempts fail
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
                                (self.retry_config.base_delay.as_millis() as f64 *
                                    self.retry_config.backoff_multiplier.powi(attempt as i32))
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

    /// Updates performance metrics with the result of an operation
    ///
    /// # Arguments
    /// * `success` - Whether the operation was successful
    /// * `latency` - The duration of the operation
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

    /// Gets a copy of the current performance metrics
    ///
    /// # Returns
    /// * `ProviderMetrics` - A copy of the current metrics
    #[allow(unused)]
    pub async fn get_metrics(&self) -> ProviderMetrics {
        self.metrics.lock().await.clone()
    }

    /// Gets the average latency in milliseconds
    ///
    /// # Returns
    /// * `f64` - The average latency in milliseconds, or 0.0 if no requests have been made
    #[allow(unused)]
    pub async fn get_average_latency_ms(&self) -> f64 {
        let metrics = self.metrics.lock().await;
        if metrics.requests_sent > 0 {
            metrics.total_latency_ms as f64 / metrics.requests_sent as f64
        } else {
            0.0
        }
    }

    /// Gets the success rate as a percentage
    ///
    /// # Returns
    /// * `f64` - The success rate as a decimal (0.0 to 1.0), or 0.0 if no requests have been made
    #[allow(unused)]
    pub async fn get_success_rate(&self) -> f64 {
        let metrics = self.metrics.lock().await;
        if metrics.requests_sent > 0 {
            metrics.requests_succeeded as f64 / metrics.requests_sent as f64
        } else {
            0.0
        }
    }

    /// Resets all performance metrics to their default values
    #[allow(unused)]
    pub async fn reset_metrics(&self) {
        let mut metrics = self.metrics.lock().await;
        *metrics = ProviderMetrics::default();
        debug!("TxnSender metrics reset");
    }
}
