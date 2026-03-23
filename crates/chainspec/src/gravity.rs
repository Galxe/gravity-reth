//! Gravity-specific hardforks

use reth_ethereum_forks::hardfork;

hardfork!(
    /// Gravity hardforks.
    GravityHardfork {
        /// Alpha hardfork: upgrade Staking/StakePool contracts and disable PoW rewards
        Alpha,
        /// Beta hardfork: upgrade StakePool contracts with correct FACTORY immutable
        Beta,
        /// Gamma hardfork: audit fixes, precompile changes, 12 contract bytecode upgrades
        Gamma,
        /// Delta hardfork: activate Governance contract by setting Ownable._owner
        Delta,
    }
);
