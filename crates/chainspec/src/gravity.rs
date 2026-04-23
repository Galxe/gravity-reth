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
        /// Epsilon hardfork: D3-2 underbonded eviction, eviction call-site move,
        /// `autoEvictThresholdPct`, GBridgeReceiver `_processedNonces` removal
        Epsilon,
        /// Zeta hardfork: Governance initialize+owner, ValidatorManagement
        /// whitelist seed, StakePool 2-step role timelock, StakingConfig
        /// single-field setters, Reconfiguration DKG snapshot fix, JWKManager
        /// non-empty field validation
        Zeta,
    }
);
