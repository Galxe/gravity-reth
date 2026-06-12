use metrics::Gauge;
use reth_metrics::{
    metrics::{Counter, Histogram},
    Metrics,
};

/// Metrics for the `PipeExecLayerMetrics`
#[derive(Metrics)]
#[metrics(scope = "pipe_exec_layer")]
pub(crate) struct PipeExecLayerMetrics {
    /// How long it took for blocks to be executed
    pub(crate) execute_duration: Histogram,
    /// How long it took for cache account state
    pub(crate) cache_account_state: Histogram,
    /// How long it took for blocks to be merklized
    pub(crate) merklize_duration: Histogram,
    /// How long it took for cache trie state
    pub(crate) cache_trie_state: Histogram,
    /// How long it took for blocks to be sealed
    pub(crate) seal_duration: Histogram,
    /// How long it took for block hash to be verified
    pub(crate) verify_duration: Histogram,
    /// How long it took for blocks to be made canonical
    pub(crate) make_canonical_duration: Histogram,
    /// How long it took for a block to be processed totally
    pub(crate) process_block_duration: Histogram,
    /// Total gas used
    pub(crate) total_gas_used: Counter,
    /// Time difference between two adjacent ordered blocks received
    pub(crate) recv_block_time_diff: Histogram,
    /// Time difference between two adjacent blocks starting execute
    pub(crate) start_execute_time_diff: Histogram,
    /// Time difference between two adjacent blocks completing commit
    pub(crate) finish_commit_time_diff: Histogram,
    /// How long it took for transactions to be filtered
    pub(crate) filter_transaction_duration: Histogram,
    /// block height of starting process
    pub(crate) start_process_block_number: Gauge,
    /// block height of end process
    pub(crate) end_process_block_number: Gauge,
}
