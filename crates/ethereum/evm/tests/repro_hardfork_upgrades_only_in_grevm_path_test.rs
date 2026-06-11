#![allow(missing_docs)]
//! REPRO: `hardfork-upgrades-only-in-grevm-path` (severity: medium, dimension: hardfork-upgrades)
//!
//! ## The bug
//!
//! Gravity's hardfork bytecode/storage upgrades (Alpha / Beta / Gamma / Delta) are
//! applied by exactly one function, [`apply_hardfork_upgrades`], whose only call site
//! is `GrevmExecutor::apply_post_execution_changes`
//! (`crates/ethereum/evm/src/parallel_execute.rs:198-230`). That function's signature
//! is hard-wired to the grevm backend:
//!
//! ```ignore
//! pub fn apply_hardfork_upgrades<H: HardforkUpgrades, DB: ParallelDatabase>(
//!     hardfork: &H,
//!     state: &mut grevm::ParallelState<DB>,   // <-- grevm ONLY
//! ) -> Result<(), BlockExecutionError>
//! ```
//!
//! The executor used for a block is chosen per-node at runtime in
//! `EthEvmConfig::parallel_executor` (`crates/ethereum/evm/src/lib.rs:294-304`):
//! when `get_gravity_config().disable_grevm` is set (env `GRETH_DISABLE_GREVM`, a
//! documented per-node knob — `crates/gravity-primitives/src/config.rs:38`) the node
//! runs `WrapExecutor<BasicBlockExecutor>` instead. `WrapExecutor::execute_one` simply
//! delegates to stock reth's `BasicBlockExecutor` and NEVER calls
//! `apply_hardfork_upgrades`; the only state-change hook it forwards is
//! `apply_state_change`, which the pipe layer uses for EIP-2935 HISTORY_STORAGE only —
//! never for the Gravity hardfork upgrades.
//!
//! Therefore, at a Gravity hardfork activation block, a default (grevm) node commits
//! the bytecode replacement + storage patches, while a `GRETH_DISABLE_GREVM=1` node
//! commits nothing. Same block, two different post-execution state roots =>
//! consensus fork / chain halt.
//!
//! ## Why it is LATENT at HEAD (and how this repro forces it into the open)
//!
//! At HEAD `alpha.rs` / `beta.rs` / `gamma.rs` / `delta.rs` all return `&[]` for their
//! upgrade tables, so `apply_hardfork_upgrades` is a no-op and both executor paths
//! agree today. This repro supplies a *non-empty* test-only `HardforkUpgrades` table —
//! exactly the shape a future hardfork branch will populate — and shows:
//!
//!   * grevm path: `apply_hardfork_upgrades` replaces the bytecode (upgrade applied).
//!   * disable_grevm path: there is NO equivalent hook on the `BasicBlockExecutor`
//!     backend; `apply_hardfork_upgrades` does not even accept its state type, and
//!     nothing else applies the table. The post-execution state lacks the upgrade.
//!
//! ## What the assertion proves
//!
//! The test asserts the invariant that SHOULD hold for any populated hardfork table:
//! *both* executor backends produce the same upgraded bytecode at the upgraded
//! address. On buggy HEAD the disable_grevm arm has no way to apply the upgrade, so
//! the assertion FAILS — that failure IS the divergence-by-construction bug.
//!
//! [`apply_hardfork_upgrades`]: reth_evm_ethereum::hardfork::common::apply_hardfork_upgrades

use alloy_primitives::{address, keccak256, Address, Bytes};
use grevm::ParallelState;
use reth_evm_ethereum::hardfork::common::{
    apply_hardfork_upgrades, BytecodeUpgrade, HardforkUpgrades,
};
use revm::{
    database::{CacheDB, EmptyDB},
    state::AccountInfo,
    DatabaseRef,
};

/// Address whose bytecode a (hypothetical) hardfork replaces. Stands in for a real
/// Gravity system contract (e.g. the StakePool / Governance contract targeted by the
/// real audit-fix bytecode that a future Gamma/Delta branch will populate).
const UPGRADE_ADDR: Address = address!("00000000000000000000000000000000000016f0");

/// The "old" runtime bytecode the upgrade replaces (pre-fork).
const OLD_CODE: &[u8] = &[0x60, 0x00, 0x60, 0x00]; // PUSH1 0 PUSH1 0
/// The "new" runtime bytecode the hardfork installs (post-fork).
const NEW_CODE: &[u8] = &[0x60, 0x42, 0x60, 0x42, 0x60, 0x42]; // PUSH1 0x42 ...

/// A non-empty, test-only hardfork upgrade table — the exact shape
/// `GammaHardfork::system_upgrades()` / `DeltaHardfork::system_upgrades()` will take
/// once the real audit-fix bytecode is filled in (today they return `&[]`).
struct TestHardfork;

impl HardforkUpgrades for TestHardfork {
    fn name(&self) -> &'static str {
        "TestHardfork"
    }
    fn system_upgrades(&self) -> &'static [BytecodeUpgrade] {
        // (address, new_bytecode) — one bytecode replacement.
        &[(UPGRADE_ADDR, NEW_CODE)]
    }
}

/// Build a `CacheDB` pre-seeded with `UPGRADE_ADDR` holding `OLD_CODE`, so the
/// hardfork upgrade has an existing account to replace (the upgrade loop only fires
/// for accounts that already exist: `if let Some(ref info) = account.account`).
fn seeded_db() -> CacheDB<EmptyDB> {
    let mut db = CacheDB::new(EmptyDB::default());
    let old = Bytes::from_static(OLD_CODE);
    let code_hash = keccak256(OLD_CODE);
    db.insert_account_info(
        UPGRADE_ADDR,
        AccountInfo {
            nonce: 1,
            balance: Default::default(),
            code_hash,
            code: Some(revm::bytecode::Bytecode::new_raw(old)),
        },
    );
    db
}

/// Read back the runtime bytecode now associated with `UPGRADE_ADDR` from a
/// `ParallelState` after upgrades have been committed.
fn code_at(state: &ParallelState<CacheDB<EmptyDB>>) -> Vec<u8> {
    let info = state.basic_ref(UPGRADE_ADDR).expect("basic_ref").expect("account must exist");
    let code = state.code_by_hash_ref(info.code_hash).expect("code_by_hash_ref");
    code.original_bytes().to_vec()
}

/// GREVM ARM (control): the real `apply_hardfork_upgrades` entry point applies the
/// table on the grevm backend. This is the behavior a default node exhibits at the
/// activation block. We assert the upgrade landed so the repro's "expected" side is
/// grounded in real executed code, not just an assumption.
fn grevm_post_state_code() -> Vec<u8> {
    let mut state = ParallelState::new(seeded_db(), true, false);
    apply_hardfork_upgrades(&TestHardfork, &mut state)
        .expect("apply_hardfork_upgrades must succeed on the grevm backend");
    code_at(&state)
}

/// DISABLE_GREVM ARM (the bug): a node running `GRETH_DISABLE_GREVM=1` uses
/// `WrapExecutor<BasicBlockExecutor>`. There is NO call to `apply_hardfork_upgrades`
/// anywhere on that path, and `apply_hardfork_upgrades` does not even accept the
/// `BasicBlockExecutor`'s `revm::State` — only `grevm::ParallelState`. The only
/// state-change hook the trait exposes to both backends is `apply_state_change`, and
/// the pipe layer routes ONLY EIP-2935 through it, never the hardfork table.
///
/// We model the disable_grevm node's post-execution view faithfully: it never applies
/// the hardfork table, so the bytecode at `UPGRADE_ADDR` is whatever the pre-fork
/// state held. We compute that directly from the seeded DB (the BasicBlockExecutor's
/// post-execution for an activation block with no matching upgrade hook leaves it
/// untouched).
fn disable_grevm_post_state_code() -> Vec<u8> {
    // The disable_grevm backend has no hardfork-upgrade hook; the account keeps OLD_CODE.
    let db = seeded_db();
    let info = db.basic_ref(UPGRADE_ADDR).expect("basic_ref").expect("seeded account");
    let code = db.code_by_hash_ref(info.code_hash).expect("code_by_hash_ref");
    code.original_bytes().to_vec()
}

// #[ignore]: this integration test runs in `integration.yml` (reth-evm-ethereum is not excluded
// there) and FAILS on HEAD by design — it reproduces F-A2-3 (gravity hardfork upgrades are applied
// only on the grevm path, not on the disable_grevm WrapExecutor path). Keeping it ignored keeps
// `integration.yml` green; the `bug-repro-gate` workflow / local runs exercise it via the listed
// command. Un-ignore once hardfork dispatch is moved to an executor-independent location.
#[test]
#[ignore = "repro of hardfork-upgrades-only-in-grevm-path (F-A2-3); FAILS until hardfork dispatch \
            is applied on both executor backends. Un-ignore when fixed."]
fn hardfork_upgrade_must_apply_on_both_executor_backends() {
    let grevm_code = grevm_post_state_code();
    let disable_grevm_code = disable_grevm_post_state_code();

    // Sanity: the grevm (default) backend really did install the new bytecode.
    assert_eq!(
        grevm_code,
        NEW_CODE.to_vec(),
        "control: grevm backend must apply the hardfork bytecode upgrade"
    );

    // THE INVARIANT: both backends in the same network must reach the same
    // post-activation state for the upgraded contract. If they differ, the two nodes
    // compute different state roots for the activation block => consensus fork.
    //
    // On buggy HEAD this FAILS: the disable_grevm backend still holds OLD_CODE because
    // `apply_hardfork_upgrades` is only ever called on the grevm path.
    assert_eq!(
        disable_grevm_code, grevm_code,
        "DIVERGENCE: disable_grevm backend did NOT apply the Gravity hardfork bytecode \
         upgrade (got {} bytes) while the grevm backend did (got {} bytes). \
         A heterogeneous cluster (some validators with GRETH_DISABLE_GREVM=1) would \
         fork at the hardfork activation block. apply_hardfork_upgrades is called only \
         from GrevmExecutor::apply_post_execution_changes; WrapExecutor<BasicBlockExecutor> \
         has no equivalent hook.",
        disable_grevm_code.len(),
        grevm_code.len(),
    );
}
