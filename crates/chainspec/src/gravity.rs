//! Gravity-specific hardforks

use reth_ethereum_forks::hardfork;

hardfork!(
    /// Gravity hardforks.
    GravityHardfork {
        /// TestNet hardfork
        TestNetV1_1,
    }
);