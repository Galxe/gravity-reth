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
        /// Epsilon hardfork: system transactions become gas-exempt (no base-fee charge and no
        /// balance requirement on `SYSTEM_CALLER`; gas is still metered) and the `SYSTEM_CALLER`
        /// sentinel genesis balance is zeroed. See gravity-reth#364 / gravity-audit#720.
        Epsilon,
    }
);
