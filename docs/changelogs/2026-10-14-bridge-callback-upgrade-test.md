# Bridge callback runtime upgrade test

Primary record: [Mono result and Dev evidence](../../../mono-grav-bridge119/docs/changelogs/2026-10-14-bridge-callback-upgrade-test.md).

## 1. Scope and current state

Temporary branch `codex/issue131-bridge119-reth-test` starts at `239bdaadf4`.
The original Issue 131 branch and PR #457 remain unchanged. Existing Issue 131
installs task/callback at the first crossing `parent.timestamp < T <= timestamp`;
Gamma still verifies its historical runtime hashes on every normal later block.

## 2. Approved behavior

At the same unreleased Issue 131 H, replace NativeOracle with the freshly built
PR #119 runtime from contracts `174123adf25395d0490210a3fad46e6018e10366`.
Transactions in H use the previous runtime; H post-state and H+1 use PR #119.
Code replacement and existing task/callback setters commit atomically. Account
fields, complete existing storage, progress and legacy records survive. Complete
matching old-runtime configuration is migrated without changing its timestamp;
only matching new-runtime configuration is an idempotent replay.

## 3. Boundaries and risks

Preserve frozen Gamma artifacts and reject unknown/partial system code states.
Gamma accepts the exact new NativeOracle hash only with valid activated existing
Issue 131 configuration. No new flag, SDK #845/randomness fix, production
activation, network call, push or PR modification belongs to this branch.

## 4. Implementation checklist

- [x] Embed freshly built PR #119 NativeOracle separately from Gamma artifacts.
- [x] Stage code in the installer overlay; resolve uncommitted code by hash.
- [x] Preserve atomic setters, matching-config migration and strict idempotence.
- [x] Scope Gamma's successor hash recognition across existing normal paths.
- [x] Extend focused runtime/account preservation, rollback and hash tests.
- [x] Review complete diff and commit locally with `[skip ci]`.

## 5. Verification

All compilation, formatters and runtime tests run on Gravity Dev through the
designated operator. Local work is source editing and static inspection only.
Dev owning checks passed: chainspec installer and the full Ethereum EVM suite
on `c7fcd31fbf47d5e6370b2c05cc5062733a2f9b36`; staged history and the actual
canonical epoch/H+1 test on `0a52b0c24c007fe1745c7f0f26ac7abf9012a0a6`.
The later head fixes only the staged test's address borrow; EVM product files
retain the previously checked content. Dev's formatter output was applied locally.

The locked node candidate uses SDK `2213742d5847a9834bcf19835e82f203673b52c1`
and Reth `0a52b0c24c007fe1745c7f0f26ac7abf9012a0a6`. The primary Mono record
owns the lock/ELF identities and cross-repository case results and evidence.

Fresh Dev artifact: `bytecodes/oracle_task_install/NativeOracle.bin`, 4010 bytes,
Keccak `13c995f14f5add575e656480be4c734a76bf1b4500097e8258f214c65e174a74`,
SHA256 `b4140d0eb3140d06dec304938594e8c5568b88412a82c51e132696760dce0444`.
Compiled exact contracts revision above with solc `0.8.30+commit.73712a01`,
optimizer 200, via IR, EVM Prague and metadata `bytecodeHash=none`. Dev operator
retains `contracts-provenance.json`, source archive and three passing Foundry
`OracleBridgeNonce1095` integration tests. These contract checks do not establish
Reth or downstream E2E results.

## 6. Human decision

The user approved the four-message Bridge plan and requested implementation and
testing on temporary branches with multiple 6.1 SOL agents. This authorization
overrides the skills' single-agent default. Keep PR #457 unchanged and perform
all builds/tests/formatting on Gravity Dev.

## 7. Outcome and evidence

The approved temporary runtime/configuration transition and focused regression
coverage are implemented, reviewed and committed locally. Dev checks above
validate the owning paths; cross-repository acceptance is documented in the
[primary Mono result](../../../mono-grav-bridge119/docs/changelogs/2026-10-14-bridge-callback-upgrade-test.md).
Original Issue 131/PR branches and historical Gamma artifacts remain unchanged.

## 8. Publication boundary

This final changelog edit is a documentation-only follow-up. The tested code
head remains Reth `0a52b0c24c007fe1745c7f0f26ac7abf9012a0a6`; its later docs
commit is not a new SDK dependency pin or candidate build. No push, PR change,
merge or deployment is authorized by this record.
