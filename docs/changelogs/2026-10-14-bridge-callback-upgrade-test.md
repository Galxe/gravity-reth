# Bridge callback runtime upgrade test

Primary record: [Mono cross-repository plan](../../../mono-grav/docs/changelogs/2026-10-14-bridge-callback-upgrade-test.md).

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
- [ ] Review complete diff and commit locally with `[skip ci]`.

## 5. Verification

All compilation, formatters and runtime tests run on Gravity Dev through the
designated operator. Local work is source editing and static inspection only.
Reuse existing chainspec, full Ethereum EVM and canonical epoch-path tests;
add H-1/H/H+1, second-setter code/config rollback, preserved account/storage,
new-code overlay lookup and scoped Gamma hash rejection coverage. Run the
historical Bridge suite once, not five rounds; Mono owns its final E2E evidence.
Exact commands and results will be appended after Dev verification.

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
