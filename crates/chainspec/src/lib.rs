//! The spec of an Ethereum network

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

/// Chain specific constants
mod constants;
pub use constants::*;

mod api;
/// Gravity-specific hardforks module.
mod gravity;
/// The chain info module.
mod info;
/// The chain spec module.
mod spec;

pub use alloy_chains::{Chain, ChainKind, NamedChain};
/// Re-export for convenience
pub use reth_ethereum_forks::*;

pub use api::EthChainSpec;
pub use gravity::GravityHardfork;
pub use info::ChainInfo;
#[cfg(any(test, feature = "test-utils"))]
pub use spec::test_fork_ids;
pub use spec::{
    make_genesis_header, BaseFeeParams, BaseFeeParamsKind, ChainSpec, ChainSpecBuilder,
    ChainSpecProvider, DepositContract, ForkBaseFeeParams, DEV, HOLESKY, HOODI, MAINNET, SEPOLIA,
};

use reth_primitives_traits::sync::OnceLock;

/// Simple utility to create a thread-safe sync cell with a value set.
pub fn once_cell_set<T>(value: T) -> OnceLock<T> {
    let once = OnceLock::new();
    let _ = once.set(value);
    once
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use alloy_rlp::Encodable;
    use std::str::FromStr;

    #[test]
    fn test_id() {
        let chain = Chain::from(1234);
        assert_eq!(chain.id(), 1234);
    }

    #[test]
    fn test_named_id() {
        let chain = Chain::from_named(NamedChain::Holesky);
        assert_eq!(chain.id(), 17000);
    }

    #[test]
    fn test_display_named_chain() {
        let chain = Chain::from_named(NamedChain::Mainnet);
        assert_eq!(format!("{chain}"), "mainnet");
    }

    #[test]
    fn test_display_id_chain() {
        let chain = Chain::from(1234);
        assert_eq!(format!("{chain}"), "1234");
    }

    #[test]
    fn test_from_u256() {
        let n = U256::from(1234);
        let chain = Chain::from(n.to::<u64>());
        let expected = Chain::from(1234);

        assert_eq!(chain, expected);
    }

    #[test]
    fn test_into_u256() {
        let chain = Chain::from_named(NamedChain::Holesky);
        let n: U256 = U256::from(chain.id());
        let expected = U256::from(17000);

        assert_eq!(n, expected);
    }

    #[test]
    fn test_from_str_named_chain() {
        let result = Chain::from_str("mainnet");
        let expected = Chain::from_named(NamedChain::Mainnet);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_from_str_named_chain_error() {
        let result = Chain::from_str("chain");

        assert!(result.is_err());
    }

    #[test]
    fn test_from_str_id_chain() {
        let result = Chain::from_str("1234");
        let expected = Chain::from(1234);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_default() {
        let default = Chain::default();
        let expected = Chain::from_named(NamedChain::Mainnet);

        assert_eq!(default, expected);
    }

    #[test]
    fn test_id_chain_encodable_length() {
        let chain = Chain::from(1234);

        assert_eq!(chain.length(), 3);
    }

    #[test]
    fn test_dns_main_network() {
        let s = "enrtree://AKA3AM6LPBYEUDMVNU3BSVQJ5AD45Y7YPOHJLEF6W26QOE4VTUDPE@all.mainnet.ethdisco.net";
        let chain: Chain = NamedChain::Mainnet.into();
        assert_eq!(s, chain.public_dns_network_protocol().unwrap().as_str());
    }

    #[test]
    fn test_dns_holesky_network() {
        let s = "enrtree://AKA3AM6LPBYEUDMVNU3BSVQJ5AD45Y7YPOHJLEF6W26QOE4VTUDPE@all.holesky.ethdisco.net";
        let chain: Chain = NamedChain::Holesky.into();
        assert_eq!(s, chain.public_dns_network_protocol().unwrap().as_str());
    }

    #[test]
    fn test_centralized_base_fee_calculation() {
        use crate::{constants::GRAVITY_MIN_BASE_FEE, ChainSpec, EthChainSpec};
        use alloy_consensus::Header;
        use alloy_eips::eip1559::INITIAL_BASE_FEE;

        fn parent_header(number: u64, base_fee: u64) -> Header {
            Header {
                number,
                gas_used: 15_000_000,
                gas_limit: 30_000_000,
                base_fee_per_gas: Some(base_fee),
                timestamp: 1_000,
                ..Default::default()
            }
        }

        // Scenario 1: chainspec has no Gravity floor configured (Ethereum mainnet
        // history sync). Result follows upstream EIP-1559 with no clamp.
        let upstream_spec = ChainSpec::default();
        let parent = parent_header(0, INITIAL_BASE_FEE);
        let next_ts = parent.timestamp + 12;
        let expected = parent
            .next_block_base_fee(upstream_spec.base_fee_params_at_timestamp(next_ts))
            .unwrap_or_default();
        let got = upstream_spec.next_block_base_fee(&parent, next_ts).unwrap_or_default();
        assert_eq!(expected, got, "Upstream chainspec must follow vanilla EIP-1559 (no clamp)");
        assert!(got < GRAVITY_MIN_BASE_FEE, "sanity: upstream computed value is below floor");

        // Scenario 2: Gravity main — chainspec has gravityMinBaseFee = 50 Gwei,
        // schedule activates at block 0, so floor is enforced for every block.
        let main_spec =
            ChainSpec { gravity_min_base_fee: Some(GRAVITY_MIN_BASE_FEE), ..Default::default() };
        // floor query returns Some at any block
        assert_eq!(main_spec.gravity_min_base_fee_at_block(0), Some(GRAVITY_MIN_BASE_FEE));
        assert_eq!(main_spec.gravity_min_base_fee_at_block(123_456), Some(GRAVITY_MIN_BASE_FEE));
        // EIP-1559 result clamped at the floor when input is below
        let got_main = main_spec.next_block_base_fee(&parent, next_ts).unwrap_or_default();
        assert_eq!(got_main, GRAVITY_MIN_BASE_FEE, "main: clamp at floor");
        // when parent already at/above floor, recurrence runs above floor
        let parent_above = parent_header(0, GRAVITY_MIN_BASE_FEE);
        let got_above = main_spec.next_block_base_fee(&parent_above, next_ts).unwrap_or_default();
        assert!(got_above >= GRAVITY_MIN_BASE_FEE, "main: stays above floor");
    }
}
