//! clap [Args](clap::Args) for gravity purposes

use clap::Args;

/// Parameters for configuring the gravity driver.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[command(next_help_heading = "Gravity")]
pub struct GravityArgs {
    /// Disable pipe execution. default false.
    #[arg(long = "gravity.disable-pipe-execution", default_value = "false")]
    pub disable_pipe_execution: bool,

    /// Disable the Grevm executor. default false.
    #[arg(long = "gravity.disable-grevm", default_value = "false")]
    pub disable_grevm: bool,

    /// The gas limit for pipe block. default `1_000_000_000`.
    #[arg(long = "gravity.pipe-block-gas-limit", default_value_t = 1_000_000_000)]
    pub pipe_block_gas_limit: u64,

    /// The max block height between merged and pesist block height.
    #[arg(long = "gravity.cache.max-persist-gap", default_value_t = 64)]
    pub cache_max_persist_gap: u64,

    /// The max size of cached items
    #[arg(long = "gravity.cache.capacity", default_value_t = 2_000_000)]
    pub cache_capacity: u64,
}

impl Default for GravityArgs {
    fn default() -> Self {
        Self {
            disable_pipe_execution: false,
            disable_grevm: false,
            pipe_block_gas_limit: 1_000_000_000,
            cache_max_persist_gap: 64,
            cache_capacity: 2_000_000,
        }
    }
}

impl GravityArgs {
    /// Convert to gravity primitives config
    pub const fn to_config(&self) -> gravity_primitives::Config {
        gravity_primitives::Config {
            disable_pipe_execution: self.disable_pipe_execution,
            disable_grevm: self.disable_grevm,
            pipe_block_gas_limit: self.pipe_block_gas_limit,
            cache_max_persist_gap: self.cache_max_persist_gap,
            cache_capacity: self.cache_capacity,
        }
    }
}
