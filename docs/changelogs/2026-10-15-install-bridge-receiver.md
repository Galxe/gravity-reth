# fix: install the Receiver with the testnet bridge hardfork

## 1. Background and Current State

The Issue 131 candidate already replaces NativeOracle with Contracts PR119 and
installs the Task/default callback atomically. Its Receiver check requires code
that the target testnet does not currently have. The primary record is
[Mono's integration plan](../../../mono-grav-bridge119/docs/changelogs/2026-10-15-oracle-install-missing-receiver.md).

## 2. Problem Model and End-to-End Behavior

B1: At the existing testnet crossing H, install the fixed Receiver when absent,
then the PR119 Oracle and Task/default callback in the same overlay commit.
B2: Preserve a matching deployed Receiver and any prefunded empty account balance
and account identity. F1: Reject conflicting code, nonce-bearing empty accounts,
or a derived runtime hash that differs from receiverCodeHash. F2: Any later
failure commits none of the Receiver, Oracle or configuration changes.

## 3. Research, Findings, and Architecture Decision

D1: Reuse the existing five configuration fields. Patch trustedBridge/sourceId
into the frozen Contracts 174123ad Receiver runtime at compiler-provided immutable
offsets; verify its hash and real getters. D2: A new Oracle/configuration paired
with a missing Receiver is a partial installation and fails closed. Historical
Gamma bytes and mainnet activation are outside this change.

## 4. Implementation Design

Change oracle_task_install.rs, its staged/canonical consumers, and a separately
embedded Receiver template. An absent account is created; an empty-code nonce-0
account receives code with its fields/storage preserved. Existing matching code
is unchanged. Only truly absent accounts receive Created status.

- [x] Extract frozen Receiver artifact and document immutable provenance.
- [x] Stage Receiver creation in the existing atomic overlay.
- [x] Cover missing/prefunded/conflicting/idempotent/rollback states.
- [x] Start staged and canonical tests without Receiver code.
- [x] Have the sole Gravity Dev operator format, build and test the final tree.

## 5. Verification and E2E Design

All builds, formatting and tests run on Gravity Dev. Required owning checks:
`cargo test -p reth-evm-ethereum oracle_task_install`,
`cargo test -p reth-stages oracle_task_install_staged_history`, and the canonical
pipe `oracle_task_install_canonical_pipe_epoch_boundary` test. E2E Required: yes;
Mono's focused bridge failure and installer flows must start without a Receiver
and verify deployment at H, failure consumption, subsequent minting and replay.

## 6. Human Decisions and Interaction

The user explicitly authorized testnet installation of the missing Receiver,
PR119 runtime and Task/callback together, with multiple agents and a sole Dev
operator. Local work is static editing/review; no local builds/tests/formatting,
mainnet activation, push or merge.

## 7. Outcome and Evidence

The tested Reth source is `b84b33c58e7db2566a8115e31efba5943d660183`.
The sole Gravity Dev operator ran the final installer group (19 passed), actual
staged-history test (1 passed), and canonical pipe epoch-boundary test (1 passed).
These checks begin without Receiver code and cover creation, prefunding,
preservation, code/nonce conflicts, partial installation and setter rollback.
The earlier chainspec checks are not relabeled as a new run on this final head.
Main-agent and independent static reviews found no remaining implementation issue.

The frozen 1212-byte Solidity template was also independently recompiled on Dev;
its bytes and named immutable offsets matched the retained compiler artifact.
SDK `ceae46b6a97bd74cc9fddce273a3123a6be6bfb8` locked this Reth revision and built
release ELF SHA256
`3fecc79aa22e2fad3ad1f55c67e6c84f896ce73c9c75c61c9532c7a64275e1bd`.

Both Mono flows ran once and passed runtime, fresh source receipt reload,
independent verification, archive reload and safe raw cleanup. The focused H=1235
case installed Receiver/PR119/task/callback together and consumed four nonces;
invalid Sender and real mint failure did not mint, and the following valid
message did. The full H=757 case retained all twelve deliveries, actual original-DB
Grevm replay, a second natural epoch recovery, and a 240.411-second/1081-block
final soak. Its four replicas ended at nonce 12, progress `[12,167]`, balance 138.
The [primary Mono report](../../../mono-grav-bridge119/docs/changelogs/2026-10-15-oracle-install-missing-receiver.md)
contains exact source/binary bindings, archive hashes and known setup boundaries.

Runtime/test results remain bound to the source head above. This closeout changes
only this record and the bytecode README; SDK's product pin stays at the tested
revision. No local build/test, push, merge or public activation occurred.

## 8. Remaining Work

None for the approved Receiver installation and Gravity Dev verification.
