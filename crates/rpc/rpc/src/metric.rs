use std::sync::OnceLock;

use reth_metrics::{metrics::{Counter, Histogram}, Metrics};
use reth_metrics::metrics;
#[derive(Metrics)]
#[metrics(scope = "rpc")]
pub(crate) struct RpcMetrics {
    /// txn recv counter
    pub(crate) txn_recv_counter: Counter,    
}

static RPC_METRICS: OnceLock<RpcMetrics> = OnceLock::new();

pub(crate) fn get_rpc_metrics() -> &'static RpcMetrics {
    RPC_METRICS.get_or_init(|| RpcMetrics::default())
}

