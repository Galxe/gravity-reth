# Testnet bridge installation hardfork

This is the Reth implementation record for [Issue #131](https://github.com/Galxe/mono-grav/issues/131).
[Mono's upgrade and reproduction guide](https://github.com/Galxe/mono-grav/blob/codex/issue131-oracle-install-e2e/docs/changelogs/2026-10-15-oracle-install-missing-receiver.md)
owns the cross-repository plan, complete E2E results, binary provenance and rerun procedure.

## 1. Background

The target testnet needs an Oracle Task and a Receiver that is not yet deployed.
It also needs the NativeOracle callback-continuation fix from Contracts #119.
Reth installs these together at one timestamp boundary.

## 2. Behavior

On chain `7771625`, H is the first block with `parent.timestamp < T <= block.timestamp`.
Transactions in H use the preceding runtimes. At H block end, one atomic transition:

1. Installs the fixed-address `GBridgeReceiver`, with the configured Sender/source ID.
2. Replaces NativeOracle with the Contracts #119 runtime.
3. Calls `OracleTaskConfig.setTask(0, sourceId, taskName, taskConfig)`.
4. Calls `NativeOracle.setDefaultCallback(0, receiver)`.

Receiver rejection or mint failure still reverts the callback. The new NativeOracle
reports that failure and advances delivery progress; malformed successful callback
returns and outer-transaction failures can still revert. Installation itself creates
no mint, body transaction, receipt, fee debit, caller nonce increment or header-gas charge.

## 3. Configuration and state policy

Genesis `config` contains `oracleTaskInstallTime` and an `oracleTaskInstall` object:

| Field | Value |
| --- | --- |
| `oracleTaskInstallTime` | Integer Unix seconds, after genesis and `gammaTime` |
| `sourceId` | Nonzero `0x` U256 |
| `taskName` | Nonzero `0x` bytes32 |
| `taskConfig` | `0x` bytes, length 1–4096 |
| `receiverCodeHash` | Exact nonzero runtime hash including Receiver immutables |
| `trustedBridge` | Nonzero Sender address |

The object accepts exactly those five fields. Both installation keys absent disables
installation; malformed explicit inputs reject execution. Other chains ignore them.
Freeze public-network values before release; test fixtures do not select them.

Source type 0 must be empty or fully matching, without a specialized callback override.
Conflicting/partial state fails before the overlay commits. Matching state retains its
configuration timestamp; H+1 never overwrites later governance edits. Existing account
fields, Oracle nonce/progress/history and unrelated source types are preserved.

## 4. Implementation

`chainspec/src/oracle_task_install.rs` owns parsing shared with SDK startup validation.
`ethereum/evm/src/hardfork/oracle_task_install.rs` owns the single migration and overlay;
its unit tests are in `oracle_task_install/tests.rs`. Canonical normal/epoch execution,
staged history and committed-block RPC replay supply real parent timestamps to it.
RPC simulation does not install. ExEx backfill is outside this rollout's scope.

The installer stages Gamma inside its overlay before checking the Oracle code, covering
an epoch block that crosses both timestamps. Runtime changes and both Solidity setters
commit together; a failure discards the staged changes. Gamma recognizes the exact
successor runtime only in the configured, activated scope. Historical Gamma bytes stay
frozen. The [runtime README](../../crates/ethereum/evm/src/hardfork/bytecodes/oracle_task_install/README.md)
records Contracts sources, compiler settings, hashes and Receiver immutable offsets.

## 5. Focused verification commands

Builds and runtime tests run on Gravity Dev. Owning checks:

```sh
cargo test --locked -p reth-chainspec oracle_task_install
cargo test --locked -p reth-evm-ethereum oracle_task_install
cargo test --locked -p reth-stages oracle_task_install_staged_history
export RUSTFLAGS="--cfg tokio_unstable -C link-arg=-fuse-ld=mold"
unset GRETH_DISABLE_PIPE_EXECUTION
cargo test --locked -p reth-pipe-exec-layer-ext-v2 --features tokio/taskdump --test gravity_pipe oracle_task_install::oracle_task_install_canonical_pipe_epoch_boundary -- --exact --nocapture --test-threads=1
cargo test --locked -p reth-pipe-exec-layer-ext-v2 --features tokio/taskdump --test gravity_pipe oracle_task_install::oracle_task_install_canonical_pipe_epoch_boundary_crosses_gamma_together -- --exact --nocapture --test-threads=1
```

The recorded Dev builder provides `mold`. Run canonical cases in separate processes
(or nextest): the node harness initializes process-global `GLOBAL_CONFIG` once.
Tests cover state preservation, rollback, H/H+1, epoch returns, replay and simulation
boundaries. The empty-block Grevm test covers installation hooks/accounting, not
parallel-versus-sequential transaction scheduling.

## 6. Review and integration

Merge order: [Contracts #119](https://github.com/Galxe/gravity_chain_core_contracts/pull/119)
→ [Reth #457](https://github.com/Galxe/gravity-reth/pull/457)
→ [SDK #844](https://github.com/Galxe/gravity-sdk/pull/844)
→ [Mono #132](https://github.com/Galxe/mono-grav/pull/132).
No database schema migration or public activation is performed by publishing these PRs.

## 7. Verification record and October 16 cleanup

Gravity Dev verified Reth `f028d188e207a5963af38120e76f7f406e563d6d`: 21 installer/RPC
tests, one staged-history test and two canonical epoch cases passed. SDK
`4412093c7cb48c39843c2b16ab19eb5926c06ba1` passed its locked release build. The focused
four-message callback flow and full twelve-message installation/recovery flow each
passed once, including independent archive verification. Mono owns the detailed evidence.

The October 16 cleanup consolidates the three Reth changelogs, extracts the unit-test
module, shares the RPC installation fixture, removes duplicate nonce/empty-block
samples and corrects the empty-block test's name and coverage description. Production
logic, configuration, bytecode artifacts and dependency pins are unchanged.

The user explicitly requested no rebuild or test rerun for this cleanup. The edited
tests have not been compiled or executed again; the prior results above describe the
previous test tree and unchanged production implementation, not a new test run.

## 8. Remaining work

Hosted checks, human approval, merge and release remain separate gates. The callback
batch-gas budget question remains tracked in Contracts #119 and Mono's guide.
SDK #843 and randomness recovery are outside this change.
