# Timestamp Oracle Task installation

[Issue #131](https://github.com/Galxe/mono-grav/issues/131) installs the first
bridge task after Gamma. Reth owns the consensus transition; SDK owns startup
validation and the locked binary; Mono owns four-validator acceptance. The
[primary cross-repository record](https://github.com/Galxe/mono-grav/blob/codex/issue131-oracle-install-e2e/docs/changelogs/2026-10-08-oracle-task-install-e2e.md)
explains the complete flow. No production activation timestamp is selected.

## Configuration and execution

Only chain `7771625` participates. Both installation fields absent disables the
transition; malformed explicit configuration rejects startup/execution before
protocol or user transactions. Other chains ignore these fields.

The genesis `config` fields are:

| Field | Required value |
| --- | --- |
| `oracleTaskInstallTime` | Unix-seconds integer, greater than genesis timestamp and `gammaTime` |
| `oracleTaskInstall.sourceId` | Nonzero `0x` U256 |
| `oracleTaskInstall.taskName` | Nonzero `0x` bytes32 |
| `oracleTaskInstall.taskConfig` | Opaque `0x` bytes, length 1–4096 |
| `oracleTaskInstall.receiverCodeHash` | Nonzero `0x` bytes32 |
| `oracleTaskInstall.trustedBridge` | Nonzero address |

The object accepts exactly these five keys. Caller, source type, target contract
and Receiver address are fixed. `From<Genesis>` remains infallible; the shared
Result-returning validator handles execution and SDK startup.

H is the first block satisfying `parent.timestamp < T <= block.timestamp`.
Installation runs at the end of H after Gamma; block timestamps may jump over T.
Missing parent context at or after T rejects execution. H+1 and later do not
repeat installation or overwrite subsequent governance changes.

The installer reads the current executor cache and requires frozen Gamma
Oracle runtimes from contracts `3bbc0b7`, plus the mint-authorized Receiver's
configured code hash, trusted bridge and source ID. Source type 0 must be wholly
empty or fully matching: one source ID/name, exact task bytes, fixed default
callback, zero raw specialized callback, and `0 < updatedAt <= block.timestamp`.
Matching governance-installed state retains its timestamp. Partial/conflicting
state, including another bridge source, rejects installation. Other source types
and existing Oracle nonce/progress/history remain intact.

The existing Governance-only `setTask` and `setDefaultCallback` ABIs run with
bounded EVM system calls against one overlay. Getters discard their diffs;
setters accumulate touched accounts and changed storage, preserving the first
original value for repeated slots. Only both successful calls and complete
postconditions permit one base commit. A second-setter failure discards the first
setter. Installation creates no callback/mint, body transaction, receipt, caller
nonce increment, fee debit or header-gas charge.

Actual parent timestamps reach the same crossing rule through normal Grevm
execution, canonical epoch-change early return, staged history execution and
committed-block RPC replay. RPC simulation keeps its existing boundary. Forced
sequential Grevm uses the same transition. Existing Gamma epoch behavior remains.

## Dev verification

All builds, tests, formatters and scans run on Gravity Dev. **Verification at this
source checkpoint is pending fresh checks.** Historical outcomes below bind Reth
`0c0bfd0`; they do not establish results for this changed source. Subsequent Dev
checks, SDK pin/release and affected E2E outcomes are recorded in the primary
cross-repository record and linked PR bodies.

Representative owning commands, run with the recorded Dev toolchain/environment:

```bash
cargo test --locked -p reth-chainspec oracle_task_install
cargo test --locked -p reth-evm-ethereum
cargo test --locked -p reth-stages
cargo +nightly-2026-02-01 fmt --all --check
cargo +nightly-2026-02-01 clippy --workspace --lib --examples --tests --benches --all-features --locked
RUSTFLAGS='-D warnings' cargo +nightly-2026-02-01 clippy --lib --tests --benches --all-features --locked
cargo nextest run --workspace
cargo +nightly-2026-02-01 docs --locked --document-private-items
zepter
make lint-toml
```

| Historical owning scope | Recorded result |
| --- | --- |
| Parser / Ethereum EVM | 2 / 65 unit + 16 integration passed; 1 doctest ignored |
| Full stages | 91 passed, 5 independently reproduced baseline failures, 4 ignored |
| Actual canonical epoch installation | Passed |
| Separate CI unit / integration | 2487 / 359 passed; 22 / 39 skipped |
| Separate standard-engine E2E / default-enabled pipe | 21 / 2 passed; 3 explicit E2E exclusions |
| Formatting, Zepter, TOML lint, invariants, private-item docs | Passed; 150 documentation entry points |
| Active CI Clippy | Passed in its narrower scope |
| Broad workspace Clippy / raw workspace nextest | Failed / incomplete; limits below |
| Verification at this source checkpoint | Pending fresh Dev checks and downstream release/E2E; subsequent outcomes live in the primary record and linked PR bodies |

Broad Clippy's sparse and relayer diagnostics were independently reproduced on
pristine base. Pipe library/test diagnostics, existing parallel-executor test
lints and stages-bench compiler errors have static attribution only. Strict pipe
integration-test lint remained blocked by library errors. These are distinct
certainty levels, rather than a claim that every failure is proven baseline.

Raw nextest first stopped at the disk reserve before tests. A second mixed
engine/pipe run was stopped incomplete: 468 passed, 3 timed out, 1 FCU failure,
2 terminated, 2589 unrun and 63 skipped. CI separates ordinary-engine tests with
`GRETH_DISABLE_PIPE_EXECUTION=1` from real pipe tests with that variable absent.
Their independent passing groups do not replace the mixed run. CI groups used
`SEED=rustethereumethereumrust`, empty RUSTFLAGS and their selected features;
stages' five consistency tests passed there but failed in the separate matched
`SEED=issue131-consistency` baseline/candidate pair. Neither result erases the other.

Historical runtime checks used Rust 1.94.1 / Alloy 2.4.1; SDK's actual locked
graph used Rust 1.93.0 / Alloy 2.0.5. Standalone Reth evidence cannot substitute
for SDK compilation. Formatting/Clippy used nightly 2026-02-01. Exact commands,
feature selections, source snapshots, images, exclusions and failed attempts are
retained in the [immutable original Reth record](https://github.com/Galxe/gravity-reth/blob/0c0bfd023a5b4c4a52721e0a797ac89f52663c11/docs/changelogs/2026-10-08-oracle-task-install-hardfork.md)
and Dev `/home/ubuntu/issue131-build-20261008T1323Z/logs/`.

No database schema or public contract ABI changes. The configured transition
changes consensus state at H. Dev results and hosted required checks/approval
are separate gates; publication and deployment do not follow from this record.
