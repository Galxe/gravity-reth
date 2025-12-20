//! Metrics for MPT prewarming.

use metrics::{Counter, Gauge, Histogram};
use reth_metrics::Metrics;

/// Metrics for MPT prewarming
#[derive(Metrics)]
#[metrics(scope = "evm_prewarm")]
pub struct PrewarmMetrics {
    /// The total number of prewarmed transactions
    pub prewarm_txs_total: Counter,
    /// The total number of prewarmed accounts
    pub prewarm_accounts_total: Counter,
    /// The total number of prewarmed storage slots
    pub prewarm_slots_total: Counter,
    /// Duration of a single prewarm operation in seconds
    pub prewarm_duration: Histogram,
    /// The total number of skipped expired tasks
    pub prewarm_skipped_expired_total: Counter,
    /// The current block number being processed
    pub current_block_number: Gauge,
}
