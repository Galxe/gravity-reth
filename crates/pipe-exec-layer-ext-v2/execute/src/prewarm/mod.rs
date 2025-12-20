//! MPT (Merkle Patricia Trie) prewarming module.
//!
//! This module provides functionality for prewarming MPT nodes by preloading
//! them into the database cache after transaction execution.

mod metrics;
mod service;

pub use metrics::PrewarmMetrics;
pub use service::PrewarmService;
