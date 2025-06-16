//! Configuration options for the Gravity Reth.

use std::sync::LazyLock;

/// Configuration options for the Gravity Reth.
#[derive(Debug)]
pub struct Config {
    /// Whether to disable the Grevm executor.
    pub disable_grevm: bool,
    /// Whether to use the storage cache.
    pub use_storage_cache: bool,
    /// Whether to use the parallel state root computation in pipe execution.
    pub use_parallel_state_root: bool,
}

/// Global configuration instance, initialized lazily.
pub static CONFIG: LazyLock<Config> = LazyLock::new(|| Config {
    disable_grevm: std::env::var("GRETH_DISABLE_GREVM").is_ok(),
    use_storage_cache: std::env::var("GRETH_USE_STORAGE_CACHE").is_ok(),
    use_parallel_state_root: std::env::var("GRETH_USE_PARALLEL_STATE_ROOT").is_ok(),
});
