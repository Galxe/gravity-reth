//! One-shot activation logs for timestamp hardforks that have no irregular
//! state change (Gravity Beta, Ethereum Osaka).
//!
//! Prague and Alpha already log from their state-change hooks
//! (`eip_2935`, `system_caller_migration`). Beta and Osaka only switch
//! policy / SpecId, so this module is the matching observability hook:
//! `transitions_at_timestamp(current_ts, parent_ts)` fires on the unique
//! block that crosses the fork and is a no-op on every other block.

use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardfork, GravityHardfork, Hardforks};
use tracing::info;

/// Log Beta / Osaka the first time this block's timestamp crosses their
/// activation time. Safe to call on every ordered block.
pub(crate) fn log_activation_for_block(
    chain_spec: &ChainSpec,
    current_ts: u64,
    parent_ts: u64,
    block_number: u64,
) {
    let activations = timestamp_activations(chain_spec, current_ts, parent_ts);
    if activations.beta {
        info!(
            target: "execute_ordered_block",
            number = block_number,
            timestamp = current_ts,
            "Gravity Beta: activated"
        );
    }
    if activations.osaka {
        info!(
            target: "execute_ordered_block",
            number = block_number,
            timestamp = current_ts,
            "Osaka: activated"
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimestampActivations {
    beta: bool,
    osaka: bool,
}

fn timestamp_activations(
    chain_spec: &ChainSpec,
    current_ts: u64,
    parent_ts: u64,
) -> TimestampActivations {
    TimestampActivations {
        beta: chain_spec
            .gravity_hardforks()
            .fork(GravityHardfork::Beta)
            .transitions_at_timestamp(current_ts, parent_ts),
        osaka: chain_spec
            .fork(EthereumHardfork::Osaka)
            .transitions_at_timestamp(current_ts, parent_ts),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::{ChainHardforks, ChainSpecBuilder, ForkCondition, MAINNET};
    use std::sync::Arc;

    const T: u64 = 100;

    fn spec(beta: Option<u64>, osaka: Option<u64>) -> Arc<ChainSpec> {
        let mut builder = ChainSpecBuilder::from(&*MAINNET)
            .shanghai_activated()
            .cancun_activated()
            .prague_activated();
        if let Some(ts) = osaka {
            builder = builder.with_osaka_at(ts);
        }
        let mut spec = builder.build();
        if let Some(ts) = beta {
            spec.gravity_hardforks =
                ChainHardforks::from([(GravityHardfork::Beta, ForkCondition::Timestamp(ts))]);
        }
        Arc::new(spec)
    }

    #[test]
    fn both_fire_on_shared_activation_timestamp() {
        let cs = spec(Some(T), Some(T));
        assert_eq!(
            timestamp_activations(cs.as_ref(), T, T - 1),
            TimestampActivations { beta: true, osaka: true }
        );
    }

    #[test]
    fn neither_fires_before_or_after_the_boundary() {
        let cs = spec(Some(T), Some(T));
        assert_eq!(
            timestamp_activations(cs.as_ref(), T - 1, T - 2),
            TimestampActivations { beta: false, osaka: false }
        );
        assert_eq!(
            timestamp_activations(cs.as_ref(), T + 1, T),
            TimestampActivations { beta: false, osaka: false }
        );
    }

    #[test]
    fn staggered_times_fire_independently() {
        let cs = spec(Some(T), Some(T + 50));
        assert_eq!(
            timestamp_activations(cs.as_ref(), T, T - 1),
            TimestampActivations { beta: true, osaka: false }
        );
        assert_eq!(
            timestamp_activations(cs.as_ref(), T + 50, T + 49),
            TimestampActivations { beta: false, osaka: true }
        );
    }

    #[test]
    fn missing_beta_is_fail_closed() {
        let cs = spec(None, Some(T));
        assert_eq!(
            timestamp_activations(cs.as_ref(), T, T - 1),
            TimestampActivations { beta: false, osaka: true }
        );
    }
}
