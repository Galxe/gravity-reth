//! Configuration options for the Gravity Reth.

use std::sync::LazyLock;

/// Configuration options for the Gravity Reth.
#[derive(Debug)]
pub struct Config {
    /// Whether to use pipe execution. default true.
    pub disable_pipe_execution: bool,
    /// Whether to disable the Grevm executor. default true.
    pub disable_grevm: bool,
    /// The gas limit for pipe block. default `1_000_000_000`.
    pub pipe_block_gas_limit: u64,
}

/// Global configuration instance, initialized lazily.
pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let config = Config {
        disable_pipe_execution: std::env::var("GRETH_DISABLE_PIPE_EXECUTION").is_ok(),
        disable_grevm: std::env::var("GRETH_DISABLE_GREVM").is_ok(),
        pipe_block_gas_limit: std::env::var("GRETH_PIPE_BLOCK_GAS_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000_000),
    };
    println!("Gravity Reth config: {config:#?}");
    config
});
