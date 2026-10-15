# fix: install the Receiver with the testnet bridge hardfork

## 1. Background and Current State

The target testnet lacks code at the native-mint precompile's authorized Receiver
address. This installation must provide the fixed Receiver, Contracts PR119
NativeOracle runtime, and bridge Task/default callback together. The cross-repository
source of truth is [Mono's integration plan and evidence](https://github.com/Galxe/mono-grav/blob/codex/issue131-oracle-install-e2e/docs/changelogs/2026-10-15-oracle-install-missing-receiver.md).

## 2. Problem Model and End-to-End Behavior

B1: At the first block crossing the configured installation timestamp H, install
all four components atomically. B2: The crossing must also work on an epoch block
whose parent precedes both Gamma and H, without requiring an intervening normal
Gamma block. B3: Preserve a matching Receiver and prefunded empty account fields
and storage. F1: Reject code/nonce conflicts, partial configuration, or mismatched
Receiver immutables/runtime hash. F2: A later installation failure commits none
of its staged Gamma prerequisite, Receiver, Oracle, or configuration changes.

## 3. Research, Findings, and Architecture Decision

Reuse the five existing configuration fields. Construct Receiver runtime from the
frozen Contracts `174123ad` template using compiler-provided immutable offsets;
verify its hash and real getters. After the authenticated timestamp-crossing gate,
stage Gamma through `DbHardforkState` inside the same installation overlay before
checking the Oracle runtime. The ordinary path's preceding Gamma migration is
idempotent; epoch and RPC replay paths use the same installer prerequisite.
Unconfigured and non-crossing historical epoch behavior is unchanged.

## 4. Implementation Design

The overlay preserves existing account fields/storage and marks only truly absent
accounts Created. It replaces Oracle code and calls the actual Task/default
callback setters, checks postconditions, then commits once. Already matching
successor state is accepted without downgrading code; partial state fails closed.
Historical Gamma bytes and mainnet activation remain outside this change.

- [x] Extract frozen Receiver artifact and document immutable provenance.
- [x] Stage Receiver, Gamma prerequisite, Oracle and configuration atomically.
- [x] Add same-block Gamma/installation epoch and RPC regressions plus rollback coverage.
- [x] Have the sole Gravity Dev operator verify formatting and installer tests.
- [x] Complete staged history, both canonical epoch cases and the locked SDK release.
- [x] Complete both final-binary acceptance archives and reconcile evidence.

## 5. Verification and E2E Design

All formatting, builds and tests run on Gravity Dev. Installer/RPC and staged
history checks are `cargo test -p reth-evm-ethereum oracle_task_install` and
`cargo test -p reth-stages oracle_task_install_staged_history`.

Run the two canonical cases as separate Cargo processes (or use nextest's process
isolation). The validated Dev builder had `mold` available, used the following
`RUSTFLAGS`, and required `GRETH_DISABLE_PIPE_EXECUTION` to be unset:

```sh
export RUSTFLAGS="--cfg tokio_unstable -C link-arg=-fuse-ld=mold"
unset GRETH_DISABLE_PIPE_EXECUTION
cargo test --locked -p reth-pipe-exec-layer-ext-v2 --features tokio/taskdump --test gravity_pipe oracle_task_install::oracle_task_install_canonical_pipe_epoch_boundary -- --exact --nocapture --test-threads=1
cargo test --locked -p reth-pipe-exec-layer-ext-v2 --features tokio/taskdump --test gravity_pipe oracle_task_install::oracle_task_install_canonical_pipe_epoch_boundary_crosses_gamma_together -- --exact --nocapture --test-threads=1
```

A combined Cargo invocation cannot run both node harnesses: the second hits the
process-global `GLOBAL_CONFIG` initialization guard before its case runs. Each
command above ran exactly one test. The cases retain Gamma-before-install and add
an actual epoch block crossing both forks; they verify the preceding runtime and
following normal block. RPC coverage calls real epoch `finish` from authenticated
pre-Gamma hashes, and rollback checks that a Receiver conflict commits neither
Gamma account change.

E2E Required: yes. The final SDK release must rerun Mono's focused four-message
callback flow and complete twelve-message installer/replay/recovery flow, beginning
without a Receiver. Old binary results do not verify the review fix.

## 6. Human Decisions and Interaction

The user authorized preparing and publishing the four existing PRs for review,
including the Gamma-crossing review fix, with one Gravity Dev operator. Local work
is static editing/review; formatting, builds and tests run only on Dev. Merge and
public activation remain separate work.

Review order: [Contracts #119](https://github.com/Galxe/gravity_chain_core_contracts/pull/119) owns callback semantics; [Reth #457](https://github.com/Galxe/gravity-reth/pull/457) owns atomic installation; [SDK #844](https://github.com/Galxe/gravity-sdk/pull/844) pins Reth; [Mono #132](https://github.com/Galxe/mono-grav/pull/132) owns acceptance flows and evidence.

## 7. Outcome and Evidence

Product Reth revision: `f028d188e207a5963af38120e76f7f406e563d6d`.
SDK product revision: `4412093c7cb48c39843c2b16ab19eb5926c06ba1`.
Gravity Dev formatting produced zero diff. All 21 installer/RPC tests, the staged
history test, and both canonical epoch cases passed. The canonical cases ran in
separate processes; an earlier combined invocation hit `GLOBAL_CONFIG` setup
initialization on its second harness and is not counted as that case passing.
Removing only the Gamma overlay prerequisite made the targeted RPC regression
run one test and fail for the expected runtime hash mismatch. The mutation
wrapper exited 127 after the test because its builder lacked Python; the durable
log proves the one-test failure, without an observed inner Cargo exit code.

SDK locked/offline metadata and its new release build passed. Independent
candidate verification bound 17,309 source files and both embedded Oracle runtimes
to ELF SHA256
`d6b6930e8a6f4301314de37e9e54d89387070ef16ab9ca1aba1076bae4d880ec`.
The focused runtime passed at H1220: four replicas ended at nonce 4, progress
`[4,13]`, balance 13 and exactly two mints. Its fresh four-receipt source reload, independent
verifier, archive reload and raw cleanup all passed; acceptance archive SHA256 is
`f1733b11197bf29cffe31a3268b2624ca8d5183d01d65ad77164917c62dcd194`.
The full flow passed at H457, including original-database replay and a second
recovery. Its four replicas ended at nonce 12, progress `[12,167]`, balance 138 and
12 mints. The final soak lasted 240.407 seconds and advanced common
blocks 2932 to 4013 (+1081), exceeding the 768-block minimum. Fresh source reload
verified 12 receipts; independent verification, archive reload and raw cleanup
passed. Full acceptance archive SHA256 is
`483dd88db3c57c0c593fb5d39aca66472bd8e1b84bd044a43cb81917f06c71f2`.
Both case archives remained unchanged after cleanup, and the original offline PFN
baseline remained intact. Local evidence is retained under
`/home/jingyue/.local/state/issue131/cross-fork-install-recovered-20261015/`.

The [initial verification record](https://github.com/Galxe/gravity-reth/blob/c6f37ce08e6d56fb1a1cbe72e16b917335dc767e/docs/changelogs/2026-10-15-install-bridge-receiver.md)
retains the earlier artifact, release and acceptance evidence. Those results bind
Reth `b84b33c` and SDK `ceae46b`; they are historical evidence, not verification of
`f028d188`. [Mono's primary report](https://github.com/Galxe/mono-grav/blob/codex/issue131-oracle-install-e2e/docs/changelogs/2026-10-15-oracle-install-missing-receiver.md) owns current cross-repository provenance
and acceptance archives.

## 8. Remaining Work

Implementation and Gravity Dev verification are complete. This documentation
closeout follows the tested product revisions. Hosted required checks, review,
merge, release publication and public activation remain separate steps. The shared
callback batch-gas budget boundary remains unresolved; see Mono's primary report.
