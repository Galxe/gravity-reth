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

    /// The max block height between merged and pesist block height.
    #[arg(long = "gravity.cache.max-persist-gap", default_value_t = 128)]
    pub cache_max_persist_gap: u64,

    /// Persist consecutive blocks as one merged commit per group to amortize the per-commit
    /// fsync (much faster from-genesis catch-up). default false.
    #[arg(long = "gravity.persist.merge-blocks", default_value = "false")]
    pub persist_merge_blocks: bool,

    /// The max size of cached items
    #[arg(long = "gravity.cache.capacity", default_value_t = 2_000_000, value_parser = clap::value_parser!(u64).range(1_000..=100_000_000))]
    pub cache_capacity: u64,

    /// Report db metrics. default false.
    #[arg(long = "gravity.report-db-metrics", default_value = "false")]
    pub report_db_metrics: bool,

    /// Max parallel level in nested hash
    #[arg(long = "gravity.trie.parallel-level", default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..=64))]
    pub trie_parallel_levels: u64,
}

impl Default for GravityArgs {
    fn default() -> Self {
        Self {
            disable_pipe_execution: false,
            disable_grevm: false,
            cache_max_persist_gap: 128,
            persist_merge_blocks: false,
            cache_capacity: 2_000_000,
            report_db_metrics: false,
            trie_parallel_levels: 1,
        }
    }
}

impl GravityArgs {
    /// Convert to gravity primitives config
    pub const fn to_config(&self) -> gravity_primitives::Config {
        gravity_primitives::Config {
            disable_pipe_execution: self.disable_pipe_execution,
            disable_grevm: self.disable_grevm,
            cache_max_persist_gap: self.cache_max_persist_gap,
            persist_merge_blocks: self.persist_merge_blocks,
            cache_capacity: self.cache_capacity,
            report_db_metrics: self.report_db_metrics,
            trie_parallel_levels: self.trie_parallel_levels,
        }
    }
}
