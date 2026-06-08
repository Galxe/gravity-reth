# Rebase 1.8.3 → 2.2.0: Real Conflict Map (trial-rebase findings)

**Author:** Richard, 2026-06-08
**Source:** trial `git rebase --onto v2.2.0 v1.8.3` of the re-anchored fork (tag `gravity-base/v1.8.3-clean-ancestry`). Aborted after surfacing the map — no code resolved yet.

This is **measured**, not estimated: the exact set of files where Gravity's customizations collide with upstream reth's 1.8.3 → 2.2.0 changes.

---

## 1. Headline

- **111 conflicting files.** (Gravity-owned crates — `pipe-exec-layer-ext-v2`, `gravity-storage`, `gravity-primitives` — do **not** conflict; they're pure additions. All 111 are upstream files Gravity edits inline.)
- By complexity (Gravity's own change size in the file):

  | Bucket | Count | Meaning |
  |---|---|---|
  | **Substantive** (>40 lines) | **28** | needs deep review + compile/test loop |
  | Medium (10–40 lines) | 43 | careful but tractable |
  | Mechanical (<10 lines) | 40 | mostly metric/flag/import tweaks |

- **Blocked on the revm fork:** none of this compiles until `Galxe/revm` is bumped 29 → 38 (see plan doc §3). Conflict resolution and compilation are interleaved, so the revm bump is the true first step.

## 2. Substantive conflicts (the 28 that need real engineering)

| File | Gravity Δ (lines) | Subsystem | Note |
|---|--:|---|---|
| `Cargo.lock` | 2748 | deps | regenerate, not hand-merge |
| `crates/storage/provider/src/providers/database/provider.rs` | 336 | storage | Gravity provider integration |
| `crates/node/core/src/args/database.rs` | 332 | node/cli | DB args |
| `crates/stages/stages/src/stages/merkle.rs` | 242 | stages | state-root stage — **consensus-critical** |
| `crates/engine/tree/src/persistence.rs` | 226 | engine | block persistence |
| `crates/engine/tree/src/tree/mod.rs` | 213 | engine | **block validation hook into Aptos ordering** |
| `crates/storage/provider/src/providers/static_file/manager.rs` | 135 | storage | static-file manager |
| `crates/transaction-pool/src/validate/eth.rs` | 123 | txpool | tx validation (EIP gating) |
| `crates/ethereum/evm/src/lib.rs` | 94 | evm | **system calls (4788/2935/7702), EVM cfg** |
| `crates/storage/db/src/lib.rs` | 85 | storage | db surface |
| `crates/evm/evm/src/execute.rs` | 85 | evm | **execution path (revm 38 lands here)** |
| `crates/chainspec/src/spec.rs` | 79 | chainspec | Gravity hardfork schedule + min-base-fee |
| `crates/stages/stages/benches/setup/mod.rs` | 72 | stages | bench setup |
| `crates/transaction-pool/src/pool/best.rs` | 61 | txpool | ordering |
| `crates/trie/common/src/nibbles.rs` | 49 | trie | **state-root — consensus-critical** |
| `crates/storage/provider/src/writer/mod.rs` | 46 | storage | writer |
| `crates/storage/db-common/src/init.rs` | 46 | storage | genesis init |
| `crates/evm/evm/src/lib.rs` | 45 | evm | EVM config trait |
| `crates/stages/stages/src/stages/sender_recovery.rs` | 44 | stages | sender recovery |
| `crates/storage/db/src/implementation/mdbx/mod.rs` | 43 | storage | mdbx |
| `crates/cli/commands/src/db/stats.rs` | 43 | cli | db stats |
| `crates/storage/db-api/src/models/mod.rs` | 42 | storage | db models |
| `crates/trie/common/src/updates.rs` | 41 | trie | **state-root — consensus-critical** |
| (+ `.github/workflows/*`, docs) | — | CI/docs | re-take upstream, re-apply Gravity CI bits |

The **consensus-critical** cluster (merkle stage, trie nibbles/updates, evm execute, ethereum/evm system calls) is where a careless merge silently breaks state-root parity → chain halt. These must be resolved with the Phase-6 block-replay verification (plan doc §5) before any rollout.

## 3. Subsystem heatmap (all 111)

```
storage   27    engine    8     ethereum  3     prune     1
trie      11    cli       6     optimism  3     net       1
stages    11    rpc       5     txpool    2     consensus 1
                node      4     evm       2     chainspec 1   …
```

Matches the plan doc's Layer D (storage/trie/stages = 49 of the 111) being the bulk. Storage is the single heaviest area — strong argument for the §4 extraction work (move Gravity storage into a trait-implementing crate so it stops conflicting).

## 4. Proposed PR breakdown (how this lands in the Gravity repo)

The rebase is too big for one PR. Suggested sequence, each independently reviewable + buildable:

| PR | Scope | Depends on |
|---|---|---|
| **PR 0** (this) | re-anchor foundation + workflow + script + this map. No reth code change. | — |
| **PR 1** | `Galxe/revm` v38 branch + `Galxe/grevm` revm-38 + alloy-evm/inspectors bumps (separate repos) | — |
| **PR 2** | reth scaffolding compiles: Cargo deps → 2.2.0 + dep forks; Layer A owned-crate API fixes | PR 1 |
| **PR 3** | Layer B execution/EVM conflicts (evm/execute, ethereum/evm) | PR 2 |
| **PR 4** | Layer C engine/tree + consensus validation | PR 2 |
| **PR 5** | Layer D storage/trie/stages (+ opportunistic extraction) | PR 2 |
| **PR 6** | Layer E periphery (cli/rpc/net/txpool) + tests green | PR 3–5 |
| **PR 7** | verification: mainnet block-range replay, state-root parity, shadow VFN | all |

PRs 3–6 can partly parallelize once PR 2 makes the workspace compile.

## 5. Reproduce

```bash
git fetch upstream --tags
git rebase --onto v2.2.0 v1.8.3 gravity-base/v1.8.3-clean-ancestry
git diff --name-only --diff-filter=U | wc -l           # → 111
git diff --name-only --diff-filter=U | sed 's|crates/||;s|/.*||' | sort | uniq -c | sort -rn
git rebase --abort
```

(`gravity-base/v1.8.3-clean-ancestry` is the re-anchored foundation tag created in PR 0.)
