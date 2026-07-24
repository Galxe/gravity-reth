//! Configuration options for the Gravity Reth.

use std::sync::OnceLock;

/// Consensus-critical gas limit for every pipe-executed block.
///
/// Must be identical on every node — a per-node flag would let validators with different
/// values produce different block hashes from the same ordered block. Closes
/// gravity-audit#712.
pub const PIPE_BLOCK_GAS_LIMIT: u64 = 1_000_000_000;

/// Emergency EIP-7702 lockdown (gravity-audit#838). When `true`, `filter_invalid_txs`
/// additionally drops every type-4 (`SetCode`) tx (L1) and every tx from/to a currently-delegated
/// account (L2/L3), neutralising the 7702 nonce-bump executor-halt class until the durable
/// executor-skip fix ships.
///
/// This is a **compile-time constant on purpose**: like `PIPE_BLOCK_GAS_LIMIT` it is
/// consensus-critical (it changes which txs execute), so every node MUST agree on it. Baking it
/// into the binary makes that guarantee structural — a mixed value would fork the chain, and a
/// hardcoded const cannot be misconfigured per-node the way a CLI/env flag could.
///
/// Currently `true` — this build ships the lockdown ACTIVE. To REVERT, once the durable
/// executor-skip fix (gravity-reth #388 + grevm #110) is deployed and 7702 can be re-enabled,
/// set this back to `false` and rebuild (a coordinated upgrade, same as any binary version).
pub const EIP7702_LOCKDOWN: bool = true;

/// Configuration options for the Gravity Reth.
#[derive(Debug, Clone)]
pub struct Config {
    /// Whether to disable pipe execution. default false.
    pub disable_pipe_execution: bool,
    /// Whether to disable the Grevm executor. default false.
    pub disable_grevm: bool,
    /// The max block height between merged and pesist block height.
    pub cache_max_persist_gap: u64,
    /// Persist consecutive blocks as one merged commit per group (amortizes the per-commit
    /// fsync, much faster catch-up) instead of committing every block. default false.
    pub persist_merge_blocks: bool,
    /// The max size of cached items
    pub cache_capacity: u64,
    /// Report db metrics
    pub report_db_metrics: bool,
    /// Max parallel levels in nested hash
    pub trie_parallel_levels: u64,
}

/// Global configuration instance, initialized once.
static GLOBAL_CONFIG: OnceLock<Config> = OnceLock::new();

/// Initialize the global configuration
pub fn init_gravity_config(config: Config) {
    assert!(GLOBAL_CONFIG.set(config).is_ok(), "Global gravity config already initialized");
}

/// Get the global configuration
pub fn get_gravity_config() -> &'static Config {
    GLOBAL_CONFIG.get_or_init(|| Config {
        disable_pipe_execution: std::env::var("GRETH_DISABLE_PIPE_EXECUTION").is_ok(),
        disable_grevm: std::env::var("GRETH_DISABLE_GREVM").is_ok(),
        cache_max_persist_gap: 128,
        persist_merge_blocks: false,
        cache_capacity: 2_000_000,
        report_db_metrics: false,
        trie_parallel_levels: 1,
    })
}
