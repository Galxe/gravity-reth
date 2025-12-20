//! Configuration options for the Gravity Reth.

use grevm::PrewarmTask;
use std::sync::OnceLock;
use tokio::sync::mpsc;

/// Configuration options for the Gravity Reth.
#[derive(Debug, Clone)]
pub struct Config {
    /// Whether to disable pipe execution. default false.
    pub disable_pipe_execution: bool,
    /// Whether to disable the Grevm executor. default false.
    pub disable_grevm: bool,
    /// The gas limit for pipe block. default `1_000_000_000`.
    pub pipe_block_gas_limit: u64,
    /// The max block height between merged and pesist block height.
    pub cache_max_persist_gap: u64,
    /// The max size of cached items
    pub cache_capacity: u64,
    /// Report db metrics
    pub report_db_metrics: bool,
    /// Max parallel levels in nested hash
    pub trie_parallel_levels: u64,
    /// Whether MPT prewarming is enabled. default true.
    pub prewarm_enabled: bool,
}

/// MPT prewarm configuration.
#[derive(Debug, Clone, Copy)]
pub struct PrewarmConfig {
    /// Storage threshold for prewarming accounts with multiple storage slots.
    pub storage_threshold: usize,
    /// Whether to only prewarm contract accounts (skip EOAs).
    pub contracts_only: bool,
}

impl PrewarmConfig {
    /// Convert from environment variables.
    pub fn from_env() -> Self {
        Self {
            storage_threshold: std::env::var("RETH_PREWARM_STORAGE_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2),
            contracts_only: std::env::var("RETH_PREWARM_CONTRACTS_ONLY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
        }
    }
}

/// Global prewarm task sender.
static GLOBAL_PREWARM_SENDER: OnceLock<mpsc::UnboundedSender<PrewarmTask>> = OnceLock::new();

/// Sets the global prewarm sender.
///
/// Returns `Err(())` if the sender was already set.
pub fn set_global_prewarm_sender(sender: mpsc::UnboundedSender<PrewarmTask>) -> Result<(), ()> {
    GLOBAL_PREWARM_SENDER.set(sender).map_err(|_| ())
}

/// Gets a reference to the global prewarm sender.
///
/// Returns `None` if the sender has not been set yet.
pub fn get_global_prewarm_sender() -> Option<&'static mpsc::UnboundedSender<PrewarmTask>> {
    GLOBAL_PREWARM_SENDER.get()
}

/// Global configuration instance, initialized once.
static GLOBAL_CONFIG: OnceLock<Config> = OnceLock::new();

/// Initialize the global configuration
pub fn init_gravity_config(config: Config) {
    assert!(GLOBAL_CONFIG.set(config).is_ok(), "Global gravity config already initialized");
}

/// Get the global configuration
pub fn get_gravity_config() -> &'static Config {
    #[cfg(not(feature = "config-from-env"))]
    {
        GLOBAL_CONFIG.get().expect("Global gravity config not initialized")
    }
    #[cfg(feature = "config-from-env")]
    {
        GLOBAL_CONFIG.get_or_init(|| Config {
            disable_pipe_execution: std::env::var("GRETH_DISABLE_PIPE_EXECUTION").is_ok(),
            disable_grevm: std::env::var("GRETH_DISABLE_GREVM").is_ok(),
            pipe_block_gas_limit: 1_000_000_000,
            cache_max_persist_gap: 64,
            cache_capacity: 2_000_000,
            report_db_metrics: false,
            trie_parallel_levels: 1,
            prewarm: PrewarmConfig::default(),
        })
    }
}
