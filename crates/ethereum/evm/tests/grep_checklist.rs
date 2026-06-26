//! Long-term regression-prevention `rg` invariants for the Gravity Alpha
//! system-tx gas-exempt design.
//!
//! Each `#[test]` shells out to `rg` and asserts a load-bearing invariant
//! from `_local/drafts/system-tx-gas-exempt/acceptance-tests-2026-06-26.md`
//! §5 / design §6.1. These are deliberately implemented as `Command::new("rg")`
//! (not `regex::Regex` over a walk) so the assertion text doubles as a
//! self-documenting `rg` recipe a human can paste into the terminal.
//!
//! Each invariant is independently observable from one of:
//!   - the address-literal collapse done in commit `1c52fa8b13`
//!   - the L1+L2 gas-exempt wiring in `2a747fbdc0`
//!   - the RPC per-tx cfg toggles in `c2daad9232` / `d5831feab6`
//!
//! If any test fails, **read the failure message**: it tells you which
//! invariant got broken and where to look.
//!
//! Requirement: `rg` must be on `$PATH` (already required by the dev
//! workflow per top-level `CLAUDE.md`).

use std::{path::PathBuf, process::Command};

/// Return the workspace root, computed from this crate's `CARGO_MANIFEST_DIR`.
///
/// Walks up from `crates/ethereum/evm` to the workspace root.
fn workspace_root() -> PathBuf {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .ancestors()
        .nth(3)
        .expect("workspace root must be three levels above crates/ethereum/evm")
        .to_path_buf()
}

/// Run `rg --type rust` with the given arguments rooted at the workspace.
///
/// Returns the captured stdout as a `String`. Panics on rg failure with
/// stderr included — both for "rg not installed" (test env issue) and
/// "rg syntax error" (test bug). A clean "no matches" exits 1 and is
/// returned as empty stdout (we handle that in the callers).
fn rg_lines(args: &[&str]) -> Vec<String> {
    let root = workspace_root();
    let output = Command::new("rg")
        .args(args)
        .arg("--type")
        .arg("rust")
        .current_dir(&root)
        .output()
        .expect("rg binary must be installed and on PATH for grep checklist tests");

    // rg exits 1 on "no matches found"; only fail the test on exit codes 2+
    // (rg syntax error or IO error).
    let status = output.status.code().unwrap_or(-1);
    assert!(
        status == 0 || status == 1,
        "rg failed (exit {status}) — args={args:?}, stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8_lossy(&output.stdout).lines().map(|s| s.to_string()).collect::<Vec<_>>()
}

// --- §6.1 #1: transact_system_txn lives in exactly two source locations ---

/// `rg 'fn transact_system_txn' crates/ethereum/evm/src` must hit
/// exactly two source positions: the serial impl in `lib.rs` and the grevm
/// impl in `parallel_execute.rs`. A third hit means somebody added a third
/// system-tx execution path that bypasses the shared
/// `is_system_tx_gas_exempt` gate.
#[test]
fn grep_transact_system_txn_two_callsites_only() {
    let hits = rg_lines(&["fn transact_system_txn", "crates/ethereum/evm/src"]);
    let by_file: std::collections::BTreeSet<_> =
        hits.iter().filter_map(|line| line.split(':').next()).collect();
    assert_eq!(
        by_file.len(),
        2,
        "expected exactly two files defining `fn transact_system_txn` in `crates/ethereum/evm/src`, got {} files. Full hit list:\n{}",
        by_file.len(),
        hits.join("\n")
    );
    let has_lib = by_file.iter().any(|p| p.ends_with("lib.rs"));
    let has_parallel = by_file.iter().any(|p| p.ends_with("parallel_execute.rs"));
    assert!(
        has_lib && has_parallel,
        "expected serial impl in lib.rs and grevm impl in parallel_execute.rs, got files: {by_file:?}"
    );
}

// --- §6.1 #2: SYSTEM_CALLER literal lives in a single source of truth -----

/// `rg '625f0000' crates/ --type rust` should find a small set of approved
/// hits: the single `const` definition in `crates/chainspec/src/gravity.rs`,
/// its docstring, plus a small number of test fixtures that explicitly
/// reference the literal. Any non-test, non-chainspec-gravity hit is a
/// redeclaration of the SYSTEM_CALLER address and a regression for the
/// "single source of truth" invariant (design §2.5).
#[test]
fn grep_system_caller_address_literal_single_source() {
    let hits = rg_lines(&["625f0000", "crates/"]);

    // Bucket each hit into either:
    //   - the canonical definition site (chainspec/src/gravity.rs)
    //   - a test file (path contains `/tests/` or filename starts with `test` or the line is inside
    //     a `#[cfg(test)] mod tests`)
    //   - "other" — these are the regressions we want to catch
    let mut other: Vec<&str> = Vec::new();
    for h in &hits {
        let path = h.split(':').next().unwrap_or("");
        let is_canonical = path.ends_with("crates/chainspec/src/gravity.rs");
        let is_tests = path.contains("/tests/") ||
            path.contains("test.rs") ||
            // also accept assertion in system_caller_migration.rs#tests,
            // which checks the literal stays canonical
            path.ends_with("system_caller_migration.rs");
        if !is_canonical && !is_tests {
            other.push(h.as_str());
        }
    }
    assert!(
        other.is_empty(),
        "SYSTEM_CALLER address literal `625f0000` must only appear in chainspec/src/gravity.rs or test fixtures, but found extra hits:\n{}",
        other.join("\n")
    );
}

// --- §6.1 #5: RPC replay uses historical state, not node tip --------------

/// `rg 'state_at_block_id|parent_hash\(\)' crates/rpc/rpc-eth-api/src/helpers/trace.rs`
/// must produce at least one hit. The exemption gate's correctness relies
/// on replaying against the historical state at the parent of the block
/// being traced — if the implementation ever pivots to `latest` / node tip
/// state, the gate논증 (design §3.5.2) collapses and pre-Alpha blocks
/// could silently get gas-exempt treatment.
#[test]
fn grep_rpc_replay_uses_historical_state() {
    let hits = rg_lines(&[
        r"state_at_block_id|parent_hash\(\)",
        "crates/rpc/rpc-eth-api/src/helpers/trace.rs",
    ]);
    assert!(
        !hits.is_empty(),
        "trace.rs must call state_at_block_id or parent_hash() — historical state lookup is load-bearing for the gas-exempt gate논증 (design §3.5.2)"
    );
}

// --- §6.1 #6: execute_history_block has no non-test callers --------------

/// `rg 'push_history_block|execute_history_block' crates/ --type rust`
/// callers must be limited to the lib's own definition + dispatch and the
/// approved test files. If a non-test crate gains a caller, R6 is
/// upgraded to consensus-critical (see design §3.6) and the codepath must
/// be redesigned before merge.
#[test]
fn grep_execute_history_block_no_non_test_callers() {
    let hits = rg_lines(&["push_history_block|execute_history_block", "crates/"]);

    // Approved callers:
    //   - crates/pipe-exec-layer-ext-v2/execute/src/lib.rs   (def + internal dispatch)
    //   - crates/pipe-exec-layer-ext-v2/execute/tests/*      (any test file)
    //   - this very test file (we name the symbol in assertion strings)
    let mut other: Vec<&str> = Vec::new();
    for h in &hits {
        let path = h.split(':').next().unwrap_or("");
        let approved = path.ends_with("crates/pipe-exec-layer-ext-v2/execute/src/lib.rs") ||
            path.contains("crates/pipe-exec-layer-ext-v2/execute/tests/") ||
            path.ends_with("crates/ethereum/evm/tests/grep_checklist.rs");
        if !approved {
            other.push(h.as_str());
        }
    }
    assert!(
        other.is_empty(),
        "history_block dispatch must have only pipe-exec-layer src/lib.rs and tests as callers — non-test caller appeared:\n{}",
        other.join("\n")
    );
}

// --- §6.1 #7: pipe + RPC precompile registration paired ------------------

/// `rg 'custom_precompiles_for_ordered_block|register_custom_precompiles'
/// crates/ --type rust` must hit both the pipe-layer construction site
/// (lib.rs) and the RPC re-injection sites (eth/helpers/call.rs etc.). If
/// either disappears, the sketch A1 EVM-rebuild path will silently lose
/// custom precompiles (BLS, randomness) and break RPC replay parity.
#[test]
fn grep_pipe_rpc_precompile_registration_in_sync() {
    let hits =
        rg_lines(&["custom_precompiles_for_ordered_block|register_custom_precompiles", "crates/"]);

    let has_pipe =
        hits.iter().any(|h| h.contains("crates/pipe-exec-layer-ext-v2/execute/src/lib.rs"));
    assert!(
        has_pipe,
        "expected `custom_precompiles_for_ordered_block` in pipe-exec-layer-ext-v2/.../lib.rs, but no such hit found. Full list:\n{}",
        hits.join("\n")
    );

    let has_rpc = hits.iter().any(|h| h.contains("crates/rpc/"));
    assert!(
        has_rpc,
        "expected at least one `register_custom_precompiles` callsite in crates/rpc/, but none found — RPC trace replay would lose custom precompiles. Full list:\n{}",
        hits.join("\n")
    );
}

// --- bonus: is_system_tx_gas_exempt is referenced by every replay-y path -

/// The shared predicate `is_system_tx_gas_exempt` must be referenced from
/// at least three places: the execution layer (`crates/ethereum/evm/src`),
/// the pipe layer (`crates/pipe-exec-layer-ext-v2/execute/src`), AND the
/// RPC trace replay paths (`crates/rpc/`). Anyone adding a fourth replay
/// surface MUST route through the same predicate; this test guards the
/// "single source of truth" claim from the design doc §3.4.
#[test]
fn grep_is_system_tx_gas_exempt_referenced_by_all_three_layers() {
    let hits = rg_lines(&["is_system_tx_gas_exempt", "crates/"]);
    let mut has_evm = false;
    let mut has_pipe = false;
    let mut has_rpc = false;
    for h in &hits {
        if h.contains("crates/ethereum/evm/") {
            has_evm = true;
        }
        if h.contains("crates/pipe-exec-layer-ext-v2/") {
            has_pipe = true;
        }
        if h.contains("crates/rpc/") {
            has_rpc = true;
        }
    }
    assert!(
        has_evm,
        "is_system_tx_gas_exempt must be referenced from crates/ethereum/evm/ (the predicate's home). Full hits:\n{}",
        hits.join("\n")
    );
    assert!(
        has_pipe,
        "is_system_tx_gas_exempt must be referenced from crates/pipe-exec-layer-ext-v2/ (L2 construction-side). Full hits:\n{}",
        hits.join("\n")
    );
    assert!(
        has_rpc,
        "is_system_tx_gas_exempt must be referenced from crates/rpc/ (RPC replay gate). Full hits:\n{}",
        hits.join("\n")
    );
}
