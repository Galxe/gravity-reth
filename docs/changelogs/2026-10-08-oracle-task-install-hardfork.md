# feat: install the first bridge task at a future timestamp hardfork

Status: Reth implementation complete for Issue #131; this document records its
design and repository verification. The user selected timestamp activation and
requested code-level review followed by implementation, with the orchestrator
owning technical design and delegated implementation/testing. Final SDK binary
integration and cross-repository E2E acceptance are maintained in mono's
corresponding acceptance record and linked PRs, without changing this pinned
Reth candidate. No production activation or deployment is authorized.

## 1. Background and Current State

[mono-grav Issue #131](https://github.com/Galxe/mono-grav/issues/131) requires
Gamma to have already activated with no bridge task/default callback, followed
by a separate future hardfork that installs configuration. Natural epoch
reconfiguration must then adopt the task and complete real bridge delivery.

Current refreshed baselines:

| Repository | Commit | Responsibility |
| --- | --- | --- |
| gravity-reth | 5235476a1ea1b498fca1b474d9e43bd7591512e8 | consensus state transition and replay |
| gravity-sdk | 3b982931f887f245bfbccf3329e0a025593427cb | dependency integration and actual node binary |
| mono-grav | f5a8b7577e413dfcc793c770e5e4c2b7706c4d18 | isolated four-validator acceptance |

The existing Gamma transition replaces Oracle runtimes while preserving
storage. It does not install task configuration. The frozen Gamma contracts
revision is `3bbc0b71bbccbeec89c706312fb2636723b594fa`; local contracts HEAD
is a different revision and must not define this migration's ABI or runtime.

`OracleTaskConfig.setTask` maintains task names, registered source types/source
IDs, config bytes, and updatedAt. `NativeOracle.setDefaultCallback` requires
the fixed Governance caller. SYSTEM_CALLER has no implicit setter authority.
The mint precompile authorizes a fixed Receiver address; merely pointing to a
contract that passes the Solidity callback code-length check is insufficient.

Normal Grevm execution, canonical epoch-change blocks, committed-block RPC
replay, and staged execution require attention. Epoch-change paths return
before normal post-execution hooks. Staged execution invokes Grevm directly
and does not pass through the pipe's outer execution method. Existing parent
header timestamps are available in the pipe and RPC reader; ordinary Grevm
execution does not currently receive a parent timestamp.

SDK main now enables forward epoch sync by default. Its recovery path still
executes authenticated historical blocks, rather than installing a state
snapshot. Runtime logs and common roots must prove the candidate's actual
replay; an environment default alone is insufficient evidence.

Gravity Dev read-only inspection found existing Foundry/fixture/baseline node
binaries, but no Rust build toolchain/cache. A task-specific Docker builder is
available as an isolated preparation path. Approximately 143 GiB disk space
and 49 GiB available memory were observed; swap was full. These are snapshots,
not guarantees for a later build. The old PR114 guard remains failed because
the protected PFN process identity changed. Preserve its historical failure;
a new run must explicitly record a separate current identity baseline.

## 2. Problem Model and End-to-End Behavior

- B1: A separately configured `oracleTaskInstallTime`, expressed in Unix
  seconds, activates on the first block with parent timestamp < T and current
  timestamp >= T. No block is required to land exactly on T. Installation is
  an end-of-block transition; all nodes use consensus block timestamps.
- B2: Only chain ID 7771625 with explicit installation configuration is in
  scope. The new disposable Dev fixture uses that destination ID; the existing
  regression fixtures and source chain ID 31337 retain their own contracts.
- B3: Install the sourceType 0 bridge task and default callback together,
  including every discovery index and updatedAt. Preserve unrelated source
  types, balances, Oracle nonce/progress/history, and existing runtime code.
  Installation itself must not deliver a callback or mint tokens.
- B4: At H+1 and later, the installer does not execute again or overwrite
  subsequent governance changes. Historical replay across H reproduces H's
  state, including when H coincides with an epoch transition.
- B5: The fixture demonstrates empty state after Gamma, stops core-4 before
  H while preserving its DB/Safety Rules, lets three peers install and adopt
  through natural epoch reconfiguration, and proves core-4 executes H during
  recovery. Two source messages then produce exactly one mint each, followed
  by at least 240 seconds and 768 new common blocks.
- F1: Missing activation configuration and non-target chains do not execute
  the migration. Invalid installation inputs must never silently produce a
  partial configuration or an alternative consensus state.
- F2: Unexpected Oracle code, Receiver code/trust, partial registry/callback
  state, or conflicting task configuration rejects installation. A complete
  matching installation can be recognized without re-writing it. Active
  bridge sources that would be affected by changing a default callback are
  not implicitly admitted.
- F3: Any call or postcondition failure discards the entire installation
  overlay. No first-setter state is committed independently.

The operational scope is implementation and isolated Dev acceptance. Actual
Testnet activation timestamp, source history, Receiver deployment/authorization,
and deployment itself require a separate deployment decision. No production
timestamp or source endpoint is selected by this proposal.

## 3. Research, Findings, and Architecture Decision

The user's explicit correction selects timestamp activation instead of block
height. This matches existing alphaTime/betaTime/gammaTime JSON conventions.
Use a new key; the already-activated Gamma point retains its existing meaning.

The proposed installation mechanism is bounded EVM system calls with fixed
Governance caller, fixed target contracts, and existing setter ABIs, executed
against an overlay of the current in-memory state. Commit one accumulated
state diff only after complete preconditions, both calls, and postconditions
succeed. This reuses Solidity's registry semantics without broadening normal
transaction permissions. The system-call execution path does not produce a
normal transaction, receipt, fee debit, or caller nonce increment; verification
must establish these absences independently.

Credible alternatives are exact native storage writes and synthetic block-body
transactions. Native writes require duplication and versioning of dynamic
bytes/EnumerableSet layouts. Synthetic Governance transactions introduce code
account sender validation, nonce, receipt, gas and replay contracts. Bounded
system calls avoid those additional responsibilities.

The crossing predicate, not an every-block active predicate, supplies the
one-shot boundary. A persistent installation marker is unnecessary. All real
execution paths must share both this predicate and the installation function.
Passing parent timestamp as execution context avoids consulting node wall
clocks, RPC responses, environment variables, or keys for consensus inputs.

The Receiver is restricted to the mint-authorized address and validated against
frozen runtime hash, trustedBridge, and trustedSourceId. Task inputs are frozen
in chain configuration. Their detailed schema belongs to the implementation
plan, rather than being inferred from mutable local deployment data.

Evidence sources include `crates/chainspec/src/spec.rs`,
`crates/ethereum/evm/src/hardfork/{gamma,common,eip_2935}.rs`,
`crates/ethereum/evm/src/{parallel_execute,gravity/executor}.rs`,
`crates/pipe-exec-layer-ext-v2/execute/src/lib.rs`, staged execution, and the
pinned Solidity interfaces. The dependency's `Evm::transact_system_call` and
revm `run_system_call` were inspected. External standards consulted during
research were [EIP-3607](https://eips.ethereum.org/EIPS/eip-3607) and
[EIP-4788](https://eips.ethereum.org/EIPS/eip-4788); neither grants the Gravity
SYSTEM_CALLER permission to configure Oracle callbacks.

Design review identified parent-context propagation, atomic overlay diff
composition, failure-before-commit, and epoch/replay root equivalence as risks.
Sections4-7 define and verify those boundaries. The known
PR114 guard failure and Issues #112/#124 remain separate evidence boundaries.


## 4. Reviewed Implementation Design

D1: Preserve separate `config.oracleTaskInstallTime` (Unix-seconds integer) and
`config.oracleTaskInstall` with exactly `sourceId` (0x U256), `taskName` (0x
bytes32), `taskConfig` (0x bytes, 1..4096 bytes), `receiverCodeHash` (0x bytes32),
and `trustedBridge` (address). Identifiers/hash/bridge must be nonzero. Task bytes
remain opaque; E2E independently binds their URI source/Portal. No caller,
sourceType, target-contract, or Receiver-address override is accepted.

D2: Both absent disables installation. Present malformed/missing counterpart
rejects target-chain execution before protocol/user transactions. Other chain
IDs are a no-op. Require T > genesis.timestamp and configured gammaTime < T;
actual exact Gamma Oracle runtimes remain a crossing-time precondition. Keep
infallible From<Genesis> and expose a Result-returning chainspec validation helper
for execution and SDK startup.

D3: The shared installer takes current `Database + DatabaseCommit` and EvmEnv.
Grevm passes its current ParallelState; RPC replay passes its current StateDB.
An ephemeral Database overlay executes fixed Governance system calls using the
upstream deterministic system-call gas bound (30M plus bounded state reservoir).
Getters discard their state results. Successful setters merge changed storage
slots into one cumulative EvmState, keeping the first original_value per slot.
Only complete postconditions permit a single base commit. No custom mint
precompile, block-body transaction, receipt, caller nonce increment, fees, or
header-gas accounting is introduced.

D4: Target sourceType0 registry must be entirely empty, including source type,
source IDs, target names/task bytes/updatedAt, default callback and raw target
specialized callback; or fully matching source IDs=[target], names=[name], exact
bytes, default=fixed Receiver, raw specialized=zero and 0<updatedAt<=block time.
The matching source type0 appears exactly once in the source type registry.
Matching governance-installed state is accepted without rewriting its earlier
valid updatedAt. All partial/conflicting states, including another source0 ID,
reject. Other sourceTypes and Oracle progress/history are preserved. Frozen
Gamma NativeOracle mapping slot3 supplies the specialized callback read because
getCallback resolves specialized/default and cannot distinguish them.

D5: ParallelExecutor adds validation, parent-aware execute_one/execute convenience,
and an installation-only epoch post hook. Normal Grevm post processing invokes
installation after existing Gamma. Pipe validates before system transactions and
calls the epoch hook before take_bundle. Stage gets the first actual parent
header then rolls timestamps; history helper fetches its actual parent header.
Committed RPC replay validates before mutations and runs installer in normal and
epoch finish paths. Simulation retains its existing execution boundary. Existing
Gamma epoch behavior is preserved. A configured active installation without
parent context rejects. Forced sequential execution remains Grevm with
force_sequential=true, so it shares the same hook.

## 5. Verification and E2E Plan

All builds, tests and formatters run on Gravity Dev; local work is source editing
and read-only inspection. Remote focused commands are `cargo test --locked -p
reth-chainspec oracle_task_install` and `cargo test --locked -p reth-evm-ethereum
oracle_task_install`, followed by existing Gamma and committed replay tests and
changed consumer compilation for reth-stages and pipe execute. Record exact
builder digest/toolchain/flags/source/tree and results after execution.

Meaningful low-level cases cover strict config parsing; timestamp jump/exact
bounds/missing context; actual Gamma setter discovery and updatedAt; fully
matching idempotence; partial/conflicting/default/specialized rejection; current
cache reads; second-setter failure after first succeeds with zero base commit;
merged dirty slots and original values; caller/coinbase/nonce/balance and gas/
receipt preservation; existing progress/history/unrelated sourceType retention;
forced sequential parity; epoch end hook and committed replay. The parent owns
cross-repository four-validator E2E, offline H replay, natural adoption, paired
bridge delivery and final candidate regressions on Gravity Dev.

The canonical pipe test starts real frozen protocol contracts, activates Gamma
in an actual normal block that opens DKG, then supplies the DKG transcript in
the installation block. It requires the actual EpochChanged result and checks
both discovery getters and persisted task/default slots. Normal Grevm tests
execute the same real block through parallel and forced-sequential constructors
and compare state, receipts and gas. Both additions passed on Gravity Dev.

## 6. Authorization and Implementation Checklist

The current user request authorizes Issue #131 code-level review followed by
implementation and explicitly permits delegation/network for this work. The
orchestrator reviewed D1-D5 as technical decisions within the existing Issue
model; this is not a claim of separate user approval for each schema field.
The prior user timestamp choice remains binding. Production deployment remains
outside scope.

- [x] Read current source, pinned contracts, Issue and owning instructions.
- [x] Complete orchestrator code-level design review.
- [x] Implement strict chainspec parser and execution-entry validation.
- [x] Implement shared atomic current-state installer.
- [x] Propagate parent context through canonical/epoch/stage/RPC paths.
- [x] Add meaningful low-level, canonical epoch, staged-history and replay regression tests.
- [x] Gravity Dev owning formatting and behavior tests; record final-tree evidence.
- [x] Complete orchestrator static code review with no product runtime blocker.
- [x] Run required workspace checks and record passes, failures and incomplete attempts accurately.

Final SDK ELF integration and cross-repository E2E acceptance are external to
this repository's checklist. Mono's acceptance record and the associated PRs
maintain those outcomes; they are not claimed as passed here.


## 7. Verification Evidence

All product commands ran only on Gravity Dev under
`/home/ubuntu/issue131-build-20261008T1323Z/`, with guarded task-owned containers
and caches, CPU=4/memory=16GiB/Cargo jobs=2. Protected existing node identities
and semantic state/progress remained unchanged or advanced as required; resource
guard failures are reported separately below. Each job's exact command,
immutable image, flags, source-file hashes and outcome are retained at
`logs/<job>.log`, `logs/<job>-source.json` and `logs/<job>-outcome.json`.

| Final check or scope | Result | Evidence job |
| --- | --- | --- |
| Whole-workspace nightly formatting | PASS | `reth-format-lintfix-01` |
| Strict chainspec parser | PASS, 2 tests | `reth-parser-final-1941-01` |
| Full Ethereum EVM suite, final std feature/test cleanup | PASS, 65 unit + 16 integration; 1 doctest ignored | `reth-evm-lintfix-final-01` |
| Full stages, including real staged installer | 91 passed / 5 verified baseline failures / 4 ignored | `reth-stages-final-1941-01` |
| Real canonical pipe Gamma normal block then installation epoch block | PASS, 1 test | `reth-pipe-final-1941-01` |
| Required workspace Clippy, all features/lib/examples/tests/benches | FAIL, verified sparse baseline diagnostics | `reth-clippy-workspace-01` |
| Active CI Clippy default-member scope | PASS | `reth-clippy-ci-01` |
| Supplemental owning five-package Clippy, no-deps/keep-going | FAIL, static unchanged-source attribution below; three owned test lints gone | `reth-clippy-owning-keepgoing-01` |
| Literal AGENTS Zepter and locked metadata/check | PASS, no source churn | `reth-zepter-literal-01`, `reth-zepter-check-02` |
| TOML lint | PASS with explicit Bash; no manifest churn | `reth-lint-toml-02` |
| Required raw workspace nextest | First attempt resource-stopped; second mixed-mode attempt controlled-stopped, incomplete | `reth-nextest-workspace-01`, `reth-nextest-workspace-02` |
| Correct CI unit environment group | PASS, 2487 tests; 22 skipped | `reth-nextest-ci-unit-01` |
| Correct CI integration environment group | PASS, 359 tests; 39 skipped | `reth-nextest-ci-integration-01` |
| Correct CI standard-engine E2E group | PASS, 21 tests; 3 CI exclusions | `reth-nextest-ci-engine-e2e-01` |
| Gravity invariant script | PASS, rules 1/2/4/7/9 | `reth-gravity-invariants-ci-02` |
| Default-enabled real pipe group | PASS, both timeline and canonical epoch tests | `reth-nextest-ci-gravity-pipe-01` |
| Required workspace private-item docs, including examples | PASS, 150 generated entry points | `reth-required-docs-01` |

Final EVM, CI Clippy, all four separated CI groups, docs and dependency evidence
bind source snapshot
`f63551283c52f1a510ff6dc57c57573c89ede3620a9d7728a9bda174a5b16b04`.
Parser/stages/pipe evidence binds preceding snapshot
`64958d525d3061e6452f9746ed81fc3ccddd238d4e55aa6a9cbf47e24e4e6adc`;
only EVM's `serde_json/std` manifest edge and three test-only initializer/iterator
cleanups changed afterward. Runtime and other parser/stage/pipe Rust bytes did
not change. Runtime tests use Rust1.94.1/cargo1.94.1 image
`sha256:32c3006127a0d5912e0b93258ad2ed8baed4669dfbcf6f0ac95e31bce65fa193`
and `RUSTFLAGS="--cfg tokio_unstable"`; pipe additionally enables
`tokio/taskdump`. Formatting/Clippy use pinned nightly2026-02-01 image
`sha256:ffcf2edf495da5bd14ae504727774d395a9d1f5ae6877ac99fb5ff1a6c8de73e`.
Required workspace and supplemental Clippy use empty RUSTFLAGS; the independently
passing active CI command uses `RUSTFLAGS="-D warnings"` and its narrower actual
scope. It does not replace the required broad failure.

The required docs command was `cargo +nightly-2026-02-01 docs --locked
--document-private-items`; the actual alias expands to workspace/all-features/
no-deps. It used the same pinned nightly image, empty RUSTFLAGS and
`CARGO_INCREMENTAL=0`, with no engine switch or SEED.

Failure attribution and retained evidence:

- Independently executed pristine-base reproductions: sparse's two unused
  serde/serde_json diagnostics (`reth-clippy-sparse-baseline-01`), relayer's five
  diagnostics (`reth-clippy-relayer-baseline-01`), and stages' five unchanged
  static-file consistency failures. The stages pair used fixed
  `SEED=issue131-consistency`, one test thread, identical resolved versions,
  toolchain/features/flags; the candidate adds only two dev-dependency lock
  edges. Pair evidence: `reth-stages-consistency-baseline-seeded-01` and
  `reth-stages-consistency-candidate-seeded-02`; initial baseline evidence:
  `reth-stages-consistency-baseline-01`. These tests do not execute the changed
  execution stage.
- Static attribution only: pipe lib/lib-test's 111/135 diagnostics, existing
  parallel-executor tests' 23 diagnostics, and 15 compiler errors in existing
  stages benches. Full spans, source bytes and originating APIs were inspected.
  Bench failures concern missing criterion, old Stage generic/method arguments,
  preexisting prune_mode and resulting inference errors; none arises from the
  added parent-context API. These are not independently reproduced baseline
  runs. Strict pipe integration-test lint remains blocked by old library errors;
  static inspection found no obvious new test lint issue.
- Task-owned lint corrections are included: std propagation for serde_json and
  three test initializer/iterator cleanups. Zepter, final formatting and full
  EVM were rerun. The five-package keep-going run confirms the three new lints
  disappeared without broad allow attributes or unrelated code edits.
- Required nextest's workspace build was stopped by the disk guard at 59.22GiB
  free after 27m50s, before any tests ran (`reth-nextest-workspace-01`, exit143,
  no OOM). This is a retained environment/resource failure; it is not evidence
  of workspace test execution or a pass. After reviewed absence of live refs
  and stopped task containers, only task-owned incremental cache was reclaimed;
  native/dependency/ELF/fingerprint artifacts and source bytes were retained.
  Fresh protection/resource guards passed before explicitly starting
  `reth-nextest-workspace-02` with the same full command, flags, profile and
  source. The first failure remains retained independently.
- The second nextest attempt entered actual tests and encountered ordinary
  Ethereum-node import/FCU timeouts. Entry tracing shows chain1 with no installer
  config and the regular Ethereum engine executor; staged import still exercises
  parent-context plumbing, and the second test progressed beyond successful
  import/canonical block10 inspection. CI already separates these self-driven
  engine tests with `GRETH_DISABLE_PIPE_EXECUTION=1` from real pipe tests. With
  that variable absent, the existing engine selects pipe mode for every chain
  and waits on an EventBus that these ordinary-node tests never drive, leaving
  standard FCU requests unanswered. This is source/CI-supported harness-mode
  attribution, not independent baseline execution. The first timeout's captured
  output has no stack, so its precise dynamic stop point remains unproven.
  The mixed run was then explicitly stopped: 468 passed / 3 timed out / 1 FCU
  failure / 2 terminated, 2589 unrun and 63 skipped. It is not a full workspace
  pass. The reviewed follow-up partitions ordinary-engine unit/integration/E2E
  with the existing CI switch from default-enabled real pipe tests, preserves
  CI's three inapplicable reorg/multinode exclusions. Integration/E2E already
  use `CARGO_INCREMENTAL=0` in CI; unit uses it here as an explicit resource
  setting rather than a claim of complete CI environment replication. Results
  and exclusions are recorded separately. These groups use empty RUSTFLAGS
  and `SEED=rustethereumethereumrust`; unit/integration enable `asm-keccak`,
  `ethereum` and `config-from-env`, while engine E2E enables `asm-keccak`.
  Those features/flags/seed differ from the earlier seeded baseline pair;
  its independent reproduction evidence is not represented as a matching CI
  feature-tree run. No runtime source change or global pipe disabling is
  introduced.
  The unit group passed all 2487 executed tests, with 22 skipped. Its five
  stages consistency tests passed in this feature/seed/flag combination; this
  does not erase the earlier reproducible failures under the paired settings.
  The integration group passed 359 executed tests, with 39 skipped, using the
  same feature tree and standard-engine environment.
  Standard-engine E2E passed all 21 selected tests; CI's three exclusions remain
  explicit. This validates that separated environment, without changing the
  incomplete mixed run's outcome.
  The default-enabled pipe group passed both tests with the engine switch
  absent: canonical epoch installation in 4.783s and the existing full hardfork
  timeline in 259.677s, within its existing 10-minute nextest ceiling.
- Intermediate failed attempts are retained, without counting as verification
  passes. Environment/toolchain IDs: `reth-parser-initial`,
  `reth-pipe-epoch-1941-01`, `reth-format-owned-01`,
  `reth-lint-toml-01`, `reth-gravity-invariants-ci-01` (missing rg; rules not
  executed). Repaired task-code/fixture IDs:
  `reth-installer-1941-01`, `reth-installer-1941-02`, `reth-staged-1941-01`,
  `reth-format-initial-01`, `reth-zepter-check-01`, `reth-clippy-core-nodeps-01`.
  Pre-final lint/stages
  IDs: `reth-clippy-owning-01`, `reth-clippy-owning-nodeps-01`,
  `reth-stages-full-1941-01`. Cargo mtime collision ID:
  `reth-stages-consistency-candidate-seeded-01`; repair evidence:
  `logs/source-mtime-cache-repair-01.json`. Baseline target cache was separated
  and source mtimes refreshed without byte changes before final four-path tests.
  Reviewed, finite cache reclamation before CI regrouping retained job evidence
  and preserved source/profile settings; the later removal of 34 passed-test
  executables is indexed at `logs/cache-test-elf-approved-removal.json`. The
  finite pre-pipe executable reclamation is indexed at
  `logs/cache-pipe-test-elf-approved-removal.json`, with corrected actual-path
  attribution of 89 passed and 9 compiled-but-unexecuted executables; the prior
  annotation error is retained and does not count as a test pass.

The orchestrator's full static review found no product runtime blocker after
reading schema, overlay/current-cache/slot composition, all four paths and RPC
simulation exclusion. This is Reth-side evidence; final SDK binary integration
and mono four-validator historical replay, natural adoption and bridge delivery
remain external acceptance gates. Their results belong in mono's changelog/PR
body after this Reth candidate is pinned, avoiding a documentation-only SHA
change afterward. No overall cross-repository verification pass is claimed.

Standalone Reth's lock resolves Alloy consensus/eips/genesis 2.4.1 and requires
Rust 1.94.1 for these dependencies, despite its manifest's 1.93 MSRV declaration.
The SDK lock pins the Reth-facing Alloy packages to 2.0.5; both locks resolve
alloy-evm 0.36.0, revm 40.0.3 and revm-handler 20.0.3. Static API compatibility is
plausible but does not verify SDK compilation or behavior. The final SDK build
and ELF/E2E evidence must use the actual pinned candidate and SDK feature tree;
standalone Reth passes do not satisfy that gate.

## 8. Remaining Work

The Reth implementation and repository verification record are complete. Final
SDK pin/build/ELF provenance and cross-repository E2E remain external acceptance
work. Required broad workspace Clippy and the earlier stages test configuration
retain the failures reported above; raw workspace nextest remains incomplete,
with the four correctly separated CI groups passing independently. Supplemental
pipe lint retains its stated static-attribution and test-module coverage
boundaries. These limitations are not an overall verification pass.
Current local static `git diff --check` is clean; it is not product-test evidence.
Production deployment is not authorized.
