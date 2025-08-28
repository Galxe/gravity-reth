//! Configuration options for the Gravity Reth.

use std::sync::OnceLock;

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
        })
    }
}
