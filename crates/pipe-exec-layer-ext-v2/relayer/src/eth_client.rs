use alloy_network::Ethereum;
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use anyhow::{Context as AnyhowContext, Result};
use reqwest::ClientBuilder;
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};
use url::Url;

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

/// Ethereum HTTP client for RPC communication
#[derive(Clone, Debug)]
pub struct EthHttpCli {
    provider: RootProvider<Ethereum>,
    retry_config: RetryConfig,
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

        let url =
            Url::parse(rpc_url).with_context(|| format!("Failed to parse RPC URL: {}", rpc_url))?;

        let client_builder = ClientBuilder::new().no_proxy().use_rustls_tls();
        let client = client_builder.build().with_context(|| "Failed to build HTTP client")?;

        let provider: RootProvider<Ethereum> =
            ProviderBuilder::default().connect_reqwest(client, url.clone());

        Ok(Self { provider, retry_config: RetryConfig::default() })
    }

    /// Get event logs with the specified filter
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        self.retry_with_backoff(|| async { self.provider.get_logs(filter).await })
            .await
            .with_context(|| "Failed to get logs with filter")
    }

    /// Gets the latest finalized block number
    pub async fn get_finalized_block_number(&self) -> Result<u64> {
        self.retry_with_backoff(|| async {
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
        .await
        .with_context(|| "Failed to get finalized block number")
    }

    /// Retries an operation with exponential backoff
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
}
