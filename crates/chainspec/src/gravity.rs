//! Gravity-specific hardforks and per-chain hardcoded hardfork overrides.
//!
//! The [`GravityHardfork`] enum is the consensus-interface set of
//! Gravity-named hardforks.
//!
//! For each chain id that the binary owns, the dispatch maps
//! [`HARDCODED_ETH_FORK_OVERRIDES`] and [`HARDCODED_GRAVITY_FORK_OVERRIDES`]
//! are the authoritative source for the listed hardforks; values supplied
//! in `genesis.json` are ignored for forks in those tables. For any chain
//! id not in the dispatch map, the override is a no-op and genesis-driven
//! behaviour is unchanged.
//!
//! Today the only entry is gravity mainnet ([`GRAVITY_MAINNET_CHAIN_ID`]).
//! Adding another chain id is a one-tuple change inside each dispatch map.
//!
//! # Override semantics
//!
//! **`None` ⇒ remove, not [`ForkCondition::Never`]**. For `None` table
//! entries the matching entry is *removed* from the runtime hardforks vec
//! rather than upserted with `Never`. The two are observationally
//! equivalent for the rest of the codebase —
//! [`reth_ethereum_forks::ChainHardforks::fork`] returns `Never` as the
//! default for missing entries, and both [`crate::ChainSpec::fork_id`] and
//! [`crate::ChainSpec::fork_filter`] skip `Never` entries — but removal
//! avoids a latent panic in [`crate::ChainSpec::latest_fork_id`], which
//! unconditionally unwraps `hardforks.last()` and then unwraps the
//! `Option<ForkId>` returned by `hardfork_fork_id` (which is `None` for
//! `Never`). Removing the entry guarantees the tail is a real fork.
//!
//! # Bypass risk
//!
//! The override is wired only into the `From<Genesis> for ChainSpec`
//! funnel. Any path that constructs a `ChainSpec` with
//! `chain.id() == GRAVITY_MAINNET_CHAIN_ID` without going through that
//! funnel ([`crate::ChainSpecBuilder`], struct literal, a static
//! `LazyLock<Arc<ChainSpec>>`, etc.) silently skips the overrides. The
//! `chainspec_builder_mainnet_does_not_match_gravity_chain_id` test is a
//! tripwire for the most likely accidental case.
//!
//! # No runtime inputs
//!
//! The override functions read ONLY `chain_id` and the compile-time tables.
//! Do NOT introduce reads from environment variables, CLI flags, config
//! files, RPC, or any other runtime input — the whole point of this module
//! is that two nodes built from the same commit on the same chain id
//! compute identical fork conditions. To re-time a fork, ship a new binary;
//! there is no in-place override path by design.

use alloc::{boxed::Box, vec::Vec};
use reth_ethereum_forks::{hardfork, EthereumHardfork, ForkCondition, Hardfork};

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

/// Gravity mainnet chain id.
pub const GRAVITY_MAINNET_CHAIN_ID: u64 = 127_001;

/// Per-chain hardcoded override table for a single hardfork kind. Each outer
/// tuple is `(chain_id, &[entries])`; each inner entry is `(fork, Some(ts))`
/// to upsert with [`ForkCondition::Timestamp`] or `(fork, None)` to remove.
type HardcodedOverrideMap<H> = &'static [(u64, &'static [(H, Option<u64>)])];

/// Per-chain hardcoded overrides for standard Ethereum timestamp-based
/// hardforks. Entry semantics per inner-table tuple:
///
/// * `Some(ts)` → activate at `ts`; ignore any value in genesis.
/// * `None`     → never activate; ignore any value in genesis. The entry is *removed* from the
///   runtime hardforks vec (see the module-level doc for why removal rather than
///   [`ForkCondition::Never`]).
///
/// Forks **not** in the inner table fall through to genesis (current behaviour),
/// and chain ids **not** in the outer dispatch are entirely untouched.
///
/// Adding or modifying an entry on an already-shipped chain id is a
/// consensus change. The PR doing so must reference the onchain governance
/// or coordination record that authorised the timestamp. Existing entries
/// must never be removed — promoting `None` to `Some(ts)` is the only
/// allowed transition.
const HARDCODED_ETH_FORK_OVERRIDES: HardcodedOverrideMap<EthereumHardfork> = &[
    // Gravity mainnet.
    (
        GRAVITY_MAINNET_CHAIN_ID,
        &[
            // Prague activation timestamp on gravity mainnet, pinned to the value
            // shipped in the deployed mainnet genesis (`gravity-mainet-gitops /
            // genesis.json` → `config.pragueTime`). Changing this value requires
            // onchain governance / network-wide coordination.
            (EthereumHardfork::Prague, Some(1_782_709_200)),
        ],
    ),
];

/// Per-chain hardcoded overrides for Gravity-specific timestamp-based
/// hardforks. Same semantics as [`HARDCODED_ETH_FORK_OVERRIDES`].
///
/// Note: only **timestamp-based** Gravity hardforks belong here. Beta, Gamma
/// and Delta are *block-based* on the wire (read from `betaBlock` /
/// `gammaBlock` / `deltaBlock`); gating those would need a parallel
/// block-keyed dispatch and a different `ForkCondition` mapping. That's an
/// explicit non-goal for v1.
const HARDCODED_GRAVITY_FORK_OVERRIDES: HardcodedOverrideMap<GravityHardfork> = &[
    // Gravity mainnet.
    (
        GRAVITY_MAINNET_CHAIN_ID,
        &[
            // Alpha is forward-defended: no timestamp has been chosen onchain,
            // so an operator-supplied `alphaTime` in mainnet genesis must NOT
            // activate it. When governance picks a timestamp, change this
            // entry to Some(ts) in the release that ships the activation.
            (GravityHardfork::Alpha, None),
        ],
    ),
];

/// Apply the hardcoded **Ethereum-side** hardfork overrides for `chain_id`
/// to `hardforks`. No-op if `chain_id` is not present in
/// [`HARDCODED_ETH_FORK_OVERRIDES`].
///
/// **Caller invariant** (see `into_ethereum_chain_spec` in
/// `crates/chainspec/src/spec.rs`): pass the **fully-populated** Vec —
/// block forks, Paris, and time forks already appended — *before* the
/// canonical-ordering pass against [`EthereumHardfork::mainnet`].
///
/// See the module-level doc on the override invariants
/// (`None` ⇒ remove, bypass risk, no-runtime-input rule).
pub(crate) fn apply_hardcoded_eth_overrides(
    chain_id: u64,
    hardforks: &mut Vec<(Box<dyn Hardfork>, ForkCondition)>,
) {
    if let Some(table) = lookup_overrides(HARDCODED_ETH_FORK_OVERRIDES, chain_id) {
        apply_overrides_from_table(hardforks, table);
    }
}

/// Apply the hardcoded **Gravity-side** hardfork overrides for `chain_id`
/// to `gravity_hardforks`. No-op if `chain_id` is not present in
/// [`HARDCODED_GRAVITY_FORK_OVERRIDES`].
///
/// **Caller invariant** (see `into_ethereum_chain_spec` in
/// `crates/chainspec/src/spec.rs`): pass the **fully-populated** Vec —
/// `alphaTime` plus any `betaBlock`/`gammaBlock`/`deltaBlock` already
/// pushed from `extra_fields` — *before* wrapping into
/// [`reth_ethereum_forks::ChainHardforks`].
///
/// See the module-level doc on the override invariants.
pub(crate) fn apply_hardcoded_gravity_overrides(
    chain_id: u64,
    gravity_hardforks: &mut Vec<(Box<dyn Hardfork>, ForkCondition)>,
) {
    if let Some(table) = lookup_overrides(HARDCODED_GRAVITY_FORK_OVERRIDES, chain_id) {
        apply_overrides_from_table(gravity_hardforks, table);
    }
}

/// Look up the per-chain override table for `chain_id` in a dispatch map.
fn lookup_overrides<H>(map: &'static [(u64, &'static [H])], chain_id: u64) -> Option<&'static [H]> {
    map.iter().find_map(|(id, table)| (*id == chain_id).then_some(*table))
}

/// The override loop, generic over hardfork kind. Pure transform on the
/// supplied `table` — chain-id dispatch happens before this call.
fn apply_overrides_from_table<H: Hardfork + Copy>(
    hardforks: &mut Vec<(Box<dyn Hardfork>, ForkCondition)>,
    table: &[(H, Option<u64>)],
) {
    for &(fork, ts) in table {
        if let Some(t) = ts {
            upsert(hardforks, Box::new(fork), ForkCondition::Timestamp(t));
        } else {
            remove_by_name(hardforks, fork.name());
        }
    }
}

/// If `hardforks` already contains an entry whose hardfork name matches
/// `fork.name()`, replace its condition in place; otherwise push a new entry.
fn upsert(
    hardforks: &mut Vec<(Box<dyn Hardfork>, ForkCondition)>,
    fork: Box<dyn Hardfork>,
    cond: ForkCondition,
) {
    if let Some(slot) = hardforks.iter_mut().find(|(f, _)| f.name() == fork.name()) {
        slot.1 = cond;
    } else {
        hardforks.push((fork, cond));
    }
}

/// Drop every entry in `hardforks` whose hardfork name equals `name`.
fn remove_by_name(hardforks: &mut Vec<(Box<dyn Hardfork>, ForkCondition)>, name: &str) {
    hardforks.retain(|(f, _)| f.name() != name);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChainSpec;
    use alloy_genesis::Genesis;
    use reth_ethereum_forks::{EthereumHardforks, Head};

    /// Build a minimal genesis JSON suitable for `From<Genesis> for ChainSpec`,
    /// with all standard pre-merge forks at block 0, Shanghai+Cancun activated
    /// at genesis timestamp 0, and optional `pragueTime` and/or `alphaTime`.
    fn genesis_json(chain_id: u64, prague_time: Option<u64>, alpha_time: Option<u64>) -> String {
        let prague = match prague_time {
            Some(t) => format!(r#","pragueTime":{t}"#),
            None => String::new(),
        };
        let alpha = match alpha_time {
            Some(t) => format!(r#","alphaTime":{t}"#),
            None => String::new(),
        };
        format!(
            r#"{{"config":{{"chainId":{chain_id},"homesteadBlock":0,"eip150Block":0,"eip155Block":0,"eip158Block":0,"byzantiumBlock":0,"constantinopleBlock":0,"petersburgBlock":0,"istanbulBlock":0,"berlinBlock":0,"londonBlock":0,"terminalTotalDifficulty":0,"terminalTotalDifficultyPassed":true,"shanghaiTime":0,"cancunTime":0{prague}{alpha}}},"nonce":"0x0","timestamp":"0x0","extraData":"0x","gasLimit":"0x4c4b40","difficulty":"0x0","mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000","coinbase":"0x0000000000000000000000000000000000000000","alloc":{{}},"number":"0x0","gasUsed":"0x0","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000"}}"#
        )
    }

    fn build_chainspec(
        chain_id: u64,
        prague_time: Option<u64>,
        alpha_time: Option<u64>,
    ) -> ChainSpec {
        let s = genesis_json(chain_id, prague_time, alpha_time);
        let genesis: Genesis = serde_json::from_str(&s).expect("valid genesis json");
        genesis.into()
    }

    /// Look up Prague's table value for gravity mainnet without hard-coding
    /// it; the dedicated `gravity_mainnet_prague_timestamp_pinned_to_governance_value`
    /// test is the single tripwire for the specific value.
    fn table_prague_timestamp() -> u64 {
        lookup_overrides(HARDCODED_ETH_FORK_OVERRIDES, GRAVITY_MAINNET_CHAIN_ID)
            .expect("gravity mainnet must have an eth override table")
            .iter()
            .find(|(f, _)| *f == EthereumHardfork::Prague)
            .and_then(|(_, ts)| *ts)
            .expect("Prague must be a Some(_) entry in the gravity-mainnet table")
    }

    // ---------- Ethereum-side: Prague override ----------

    #[test]
    fn mainnet_overrides_prague_when_genesis_supplies_a_different_value() {
        let table_ts = table_prague_timestamp();
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, Some(100), None);
        assert_eq!(
            cs.hardforks.fork(EthereumHardfork::Prague),
            ForkCondition::Timestamp(table_ts),
            "binary value must win over genesis",
        );
        assert!(!cs.is_prague_active_at_timestamp(100), "genesis ts must not activate it");
        assert!(!cs.is_prague_active_at_timestamp(table_ts - 1));
        assert!(cs.is_prague_active_at_timestamp(table_ts));
    }

    #[test]
    fn mainnet_overrides_prague_when_genesis_omits_it() {
        let table_ts = table_prague_timestamp();
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, None, None);
        assert_eq!(cs.hardforks.fork(EthereumHardfork::Prague), ForkCondition::Timestamp(table_ts),);
        assert!(!cs.is_prague_active_at_timestamp(table_ts - 1));
        assert!(cs.is_prague_active_at_timestamp(table_ts));
    }

    #[test]
    fn non_mainnet_chain_id_passes_prague_through() {
        // Gravity testnet chain id — not in the table, so genesis wins.
        let cs = build_chainspec(7_771_625, Some(100), None);
        assert_eq!(cs.hardforks.fork(EthereumHardfork::Prague), ForkCondition::Timestamp(100));
        assert!(cs.is_prague_active_at_timestamp(100));
        assert!(!cs.is_prague_active_at_timestamp(99));
    }

    #[test]
    fn mainnet_does_not_touch_shanghai_or_cancun() {
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, None, None);
        assert_eq!(cs.hardforks.fork(EthereumHardfork::Shanghai), ForkCondition::Timestamp(0));
        assert_eq!(cs.hardforks.fork(EthereumHardfork::Cancun), ForkCondition::Timestamp(0));
        assert!(cs.is_shanghai_active_at_timestamp(0));
        assert!(cs.is_cancun_active_at_timestamp(0));
    }

    // ---------- Gravity-side: Alpha forward-defence ----------

    #[test]
    fn mainnet_forces_alpha_inactive_when_genesis_supplies_alpha_time() {
        // Operator submits a mainnet genesis with alphaTime set; the binary
        // must drop the entry from gravity_hardforks regardless.
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, None, Some(100));
        assert_eq!(
            cs.gravity_hardforks.fork(GravityHardfork::Alpha),
            ForkCondition::Never,
            "binary must veto operator-supplied alphaTime on mainnet",
        );
    }

    #[test]
    fn mainnet_forces_alpha_inactive_when_genesis_omits_alpha_time() {
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, None, None);
        assert_eq!(cs.gravity_hardforks.fork(GravityHardfork::Alpha), ForkCondition::Never);
    }

    #[test]
    fn non_mainnet_chain_id_passes_alpha_through() {
        // Testnet: no override, genesis-supplied alphaTime should be honoured.
        let cs = build_chainspec(7_771_625, None, Some(100));
        assert_eq!(
            cs.gravity_hardforks.fork(GravityHardfork::Alpha),
            ForkCondition::Timestamp(100),
        );
    }

    // ---------- Fork-id / fork-filter / genesis-header invariants ----------

    /// The override must yield identical `fork_id` and `fork_filter` for two
    /// mainnet chainspecs that differ only in whether genesis happened to
    /// supply a (different) `pragueTime`. Both end up with the binary's
    /// table value, so the overlay must collapse both inputs to the same
    /// active fork schedule for every head timestamp — including across the
    /// table-pinned Prague boundary.
    #[test]
    fn override_yields_consistent_fork_id_across_genesis_variants() {
        let with_prague = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, Some(100), None);
        let without_prague = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, None, None);
        let table_ts = table_prague_timestamp();
        let probes = [0u64, 99, 100, table_ts - 1, table_ts, table_ts + 1, u64::MAX];
        for &timestamp in &probes {
            let head = Head { number: 0, timestamp, ..Default::default() };
            assert_eq!(
                with_prague.fork_id(&head),
                without_prague.fork_id(&head),
                "fork_id diverges at head timestamp {timestamp}",
            );
            assert_eq!(
                with_prague.fork_filter(head),
                without_prague.fork_filter(head),
                "fork_filter diverges at head timestamp {timestamp}",
            );
        }
    }

    /// End-to-end coverage of the Prague override's effect on
    /// `make_genesis_header`. `pragueTime: 0` would normally cause
    /// `is_prague_active_at_timestamp(0)` to be true at the genesis block,
    /// setting `requests_root` on the genesis header. The binary's table
    /// pins Prague far in the future, so the override must replace the
    /// genesis-supplied `0` with the table value *and* that change must be
    /// visible to `genesis_header()` and `genesis_hash()`.
    #[test]
    fn mainnet_override_suppresses_prague_in_genesis_header_at_timestamp_zero() {
        let table_ts = table_prague_timestamp();
        let cs_with = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, Some(0), None);
        assert_eq!(
            cs_with.hardforks.fork(EthereumHardfork::Prague),
            ForkCondition::Timestamp(table_ts),
            "table value must replace genesis-supplied 0",
        );
        assert!(
            !cs_with.is_prague_active_at_timestamp(0),
            "Prague must not activate at genesis timestamp 0 after override",
        );
        assert!(
            cs_with.genesis_header().requests_hash.is_none(),
            "override must propagate through make_genesis_header",
        );
        let cs_without = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, None, None);
        assert_eq!(cs_with.genesis_hash(), cs_without.genesis_hash());
    }

    /// Tripwire for the `ChainSpecBuilder::mainnet()` bypass risk noted in
    /// the doc-comment on `apply_gravity_mainnet_overrides`: any `ChainSpec`
    /// constructed via the builder (or as a struct literal) skips the
    /// override. Today this is safe because the builder's default chain id
    /// is upstream Ethereum mainnet (1), not gravity mainnet (127001).
    /// If a future change ever makes this assertion false, the bypass
    /// becomes a real bug and the test fails loud.
    #[test]
    fn chainspec_builder_mainnet_does_not_match_gravity_chain_id() {
        use crate::ChainSpecBuilder;
        assert_ne!(ChainSpecBuilder::mainnet().build().chain.id(), GRAVITY_MAINNET_CHAIN_ID);
    }

    /// Regression for the latent panic in `ChainSpec::latest_fork_id` when a
    /// gated fork ends up at the tail of the canonical-ordered hardforks vec.
    /// If a future refactor goes back to inserting `ForkCondition::Never`
    /// for `None` table entries, this test will start panicking.
    #[test]
    fn latest_fork_id_does_not_panic_on_mainnet() {
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, Some(100), Some(50));
        let _ = cs.latest_fork_id();
    }

    /// EL-local invariant: the preserved `chain_spec.genesis` is not mutated
    /// by the override. Downstream consumers that re-read
    /// `chain_spec.genesis.config.*` or `extra_fields.*` (e.g. gravity-sdk's
    /// consensus side, or the `admin_nodeInfo` RPC) keep seeing whatever
    /// the operator supplied.
    #[test]
    fn mainnet_override_preserves_genesis_config() {
        let cs = build_chainspec(GRAVITY_MAINNET_CHAIN_ID, Some(100), Some(50));
        assert_eq!(
            cs.genesis().config.prague_time,
            Some(100),
            "override must not mutate the preserved Genesis struct (prague)",
        );
        assert_eq!(
            cs.genesis().config.extra_fields.get("alphaTime").and_then(|v| v.as_u64()),
            Some(50),
            "override must not mutate the preserved extra_fields (alpha)",
        );
        // And the overrides ARE in effect on the in-memory hardforks:
        assert_eq!(
            cs.hardforks.fork(EthereumHardfork::Prague),
            ForkCondition::Timestamp(table_prague_timestamp()),
        );
        assert_eq!(cs.gravity_hardforks.fork(GravityHardfork::Alpha), ForkCondition::Never);
    }

    /// `From<Genesis>` must be a pure function — calling it twice on the
    /// same parsed `Genesis` produces `ChainSpec`s with byte-equal
    /// `hardforks` and `gravity_hardforks` fields.
    #[test]
    fn from_genesis_is_idempotent_on_mainnet() {
        let s = genesis_json(GRAVITY_MAINNET_CHAIN_ID, Some(100), Some(50));
        let genesis: Genesis = serde_json::from_str(&s).expect("valid genesis json");
        let a: ChainSpec = genesis.clone().into();
        let b: ChainSpec = genesis.into();
        assert_eq!(a.hardforks, b.hardforks);
        assert_eq!(a.gravity_hardforks, b.gravity_hardforks);
    }

    // ---------- Governance pin ----------

    /// Consensus-affecting pin: this is the gravity-mainnet Prague activation
    /// timestamp, sourced from the deployed mainnet genesis
    /// (`gravity-mainet-gitops/genesis.json` → `config.pragueTime`).
    /// Changing this value requires onchain governance / network-wide
    /// coordination — the test exists specifically to catch a casual edit.
    /// To re-time the fork, update both the gitops genesis and this value,
    /// and ship a new binary release with a sufficient grace window for the
    /// network to upgrade before the new timestamp.
    #[test]
    fn gravity_mainnet_prague_timestamp_pinned_to_governance_value() {
        assert_eq!(table_prague_timestamp(), 1_782_709_200);
    }

    // ---------- Direct helper coverage (Some / None branches) ----------

    #[test]
    fn apply_overrides_writes_timestamp_when_absent() {
        let mut hardforks: Vec<(Box<dyn Hardfork>, ForkCondition)> = Vec::new();
        apply_overrides_from_table(&mut hardforks, &[(EthereumHardfork::Prague, Some(12_345))]);
        assert_eq!(hardforks.len(), 1);
        assert_eq!(hardforks[0].0.name(), EthereumHardfork::Prague.name());
        assert_eq!(hardforks[0].1, ForkCondition::Timestamp(12_345));
    }

    #[test]
    fn apply_overrides_replaces_existing_entry_with_some() {
        let mut hardforks: Vec<(Box<dyn Hardfork>, ForkCondition)> =
            vec![(EthereumHardfork::Prague.boxed(), ForkCondition::Timestamp(999))];
        apply_overrides_from_table(&mut hardforks, &[(EthereumHardfork::Prague, Some(42))]);
        assert_eq!(hardforks.len(), 1);
        assert_eq!(hardforks[0].1, ForkCondition::Timestamp(42));
    }

    #[test]
    fn apply_overrides_with_none_removes_existing_entry() {
        let mut hardforks: Vec<(Box<dyn Hardfork>, ForkCondition)> =
            vec![(EthereumHardfork::Prague.boxed(), ForkCondition::Timestamp(999))];
        apply_overrides_from_table(&mut hardforks, &[(EthereumHardfork::Prague, None)]);
        assert!(hardforks.is_empty(), "None entry must remove the matching fork");

        // No-op when the fork is not present.
        apply_overrides_from_table(&mut hardforks, &[(EthereumHardfork::Prague, None)]);
        assert!(hardforks.is_empty());
    }

    /// The generic helper also works for `GravityHardfork`.
    #[test]
    fn apply_overrides_works_for_gravity_hardforks() {
        let mut hardforks: Vec<(Box<dyn Hardfork>, ForkCondition)> = Vec::new();
        apply_overrides_from_table(&mut hardforks, &[(GravityHardfork::Alpha, Some(777))]);
        assert_eq!(hardforks.len(), 1);
        assert_eq!(hardforks[0].0.name(), GravityHardfork::Alpha.name());
        assert_eq!(hardforks[0].1, ForkCondition::Timestamp(777));
    }

    /// Chain-id dispatch: an unknown chain id makes both public entry points
    /// no-op without ever reaching `apply_overrides_from_table`.
    #[test]
    fn hardcoded_overrides_are_noop_for_unknown_chain_id() {
        let mut eth: Vec<(Box<dyn Hardfork>, ForkCondition)> = Vec::new();
        let mut gravity: Vec<(Box<dyn Hardfork>, ForkCondition)> = Vec::new();
        apply_hardcoded_eth_overrides(7_771_625 /* gravity testnet */, &mut eth);
        apply_hardcoded_gravity_overrides(7_771_625, &mut gravity);
        assert!(eth.is_empty());
        assert!(gravity.is_empty());
    }

    #[test]
    fn lookup_overrides_finds_gravity_mainnet_eth_table() {
        let table = lookup_overrides(HARDCODED_ETH_FORK_OVERRIDES, GRAVITY_MAINNET_CHAIN_ID);
        assert!(table.is_some(), "gravity mainnet must have an eth dispatch entry");
        assert!(!table.unwrap().is_empty());
    }

    #[test]
    fn lookup_overrides_returns_none_for_unknown_chain_id() {
        assert!(lookup_overrides(HARDCODED_ETH_FORK_OVERRIDES, 999_999).is_none());
        assert!(lookup_overrides(HARDCODED_GRAVITY_FORK_OVERRIDES, 999_999).is_none());
    }
}
