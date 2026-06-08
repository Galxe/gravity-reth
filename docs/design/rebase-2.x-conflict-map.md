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
| **PR 1** ✅ **DONE** | the 4 dependency forks (separate repos) — all built + tested, see §6 | — |
| **PR 2** | reth scaffolding compiles: Cargo deps → 2.2.0 + dep forks (§7 patch); Layer A owned-crate API fixes | PR 1 ✅ |
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

---

## 6. Dependency forks — DONE ✅ (PR 1 complete, 2026-06-08)

All four Galxe forks the reth-2.2.0 transplant depends on are migrated to revm 38, built, and tested. The transplant (PR 2+) is now unblocked.

| Fork | Branch | Base / target | Tests |
|---|---|---|---|
| `Galxe/revm` | `v38.0.0-gravity` | bluealloy **v107** (= crates.io revm 38.0.0; NOT git tip) + lazy-reward | **420 pass** |
| `Galxe/alloy-evm` | `v0.34.0-gravity` | upstream v0.34.0 + lazy-reward + Galxe revm | **51 pass** |
| `Galxe/grevm` | `feat/revm-38` | parallel-state migrated to revm 38; MSRV 1.93 | **14 pass** (erc20/native/uniswap) |
| `Galxe/revm-inspectors` | `v0.39.0-gravity` | upstream v0.39.0 + Galxe revm | **42 pass** |

**Hard-won lesson (applied to all four):** bluealloy/paradigm **git tip runs ahead of crates.io**. Base each fork on the *exact commit/tag whose published version matches what reth 2.2.0 pins* (e.g. revm → tag `v107`, where `revm-handler` is `18.1.0` with `warm_addresses -> Box<impl Iterator>`, matching crates.io 38.0.0 — NOT git `afc22938`/"38.1" which had already changed it to `&AddressSet`). Verify by diffing an API surface against the crates.io source in `~/.cargo/registry/src/.../<crate>-<ver>/`. Getting this wrong surfaces as `warm_addresses`/`AccountInfo` mismatches when a downstream fork compiles against yours.

## 7. Cargo wiring for the transplant (ready to paste into reth's root Cargo.toml)

reth 2.2.0 pulls these revm-family crates from crates.io; redirect them all to the Galxe forks so the whole workspace uses one consistent (lazy-reward-bearing) revm. Put this in the **root** `Cargo.toml`:

```toml
[patch.crates-io]
# revm core (all sub-crates → one Galxe fork branch, based on bluealloy v107)
revm                    = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-bytecode           = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-context            = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-context-interface  = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-database           = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-database-interface = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-handler            = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-inspector          = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-interpreter        = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-precompile         = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-primitives         = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
revm-state              = { git = "https://github.com/Galxe/revm", branch = "v38.0.0-gravity" }
# higher-level
alloy-evm               = { git = "https://github.com/Galxe/alloy-evm", branch = "v0.34.0-gravity" }
revm-inspectors         = { git = "https://github.com/Galxe/revm-inspectors", branch = "v0.39.0-gravity" }
grevm                   = { git = "https://github.com/Galxe/grevm", branch = "feat/revm-38" }
```

Notes:
- **op-revm** is intentionally absent — revm 38 dropped it from the workspace and reth 2.2.0 no longer depends on it (confirmed in reth v2.2.0 `Cargo.lock`).
- Pin to commit SHAs instead of `branch` before any production rollout (branches move).
- MSRV is **1.93** for the whole tree (revm 38). Set `rust-toolchain.toml` accordingly.

## 8. Transplant execution note (PR 2+)

reth is **all-or-nothing to compile** — unlike the leaf forks (which compile-drive incrementally), the workspace won't build until *all* 111 conflicts + their API migrations are resolved. So the transplant is best done as a dedicated, uninterrupted effort that resolves Layer A→E in one branch and only then attempts `cargo check`, rather than expecting intermediate green builds. The consensus-critical cluster (§2) should pair with the grevm/revm authors and gate on the Phase-6 mainnet block-replay state-root parity check before any rollout.

## 9. Trial transplant onto v2.2.0 — measured resolution split (2026-06-08)

A full `git cherry-pick gravity-base/v1.8.3-clean-ancestry` onto upstream `v2.2.0` was run and the 111 conflicts triaged. Result — **81 of 111 resolve mechanically, 30 need a real merge:**

- **272 files apply clean** (the Gravity-owned crates `pipe-exec-layer-ext-v2`, `gravity-storage`, `gravity-primitives` + all non-overlapping edits).
- **81 conflicts → take upstream `v2.2.0` (`git checkout --ours`)**: these are Gravity's v1.8.3-era code that upstream simply evolved, or Gravity edits that v2.2.0 already adopted (e.g. `payload_validator.rs` `triev2: Default::default()` is now upstream; `engine.rs` `#[allow]`→`#[expect]` cosmetic). Identified automatically: a conflict file whose Gravity diff (`git diff v1.8.3 gravity-base -- <f>`) contains **no** Gravity-specific marker (`gravity|pipe_exec|min_base_fee|lazy_reward|gravity_storage|coinbase tip`) is take-HEAD. Plus 3 `DU` deletions (`codspeed-build.sh`, `windows.yml`, `zstd-compressors` — upstream removed them).
- **30 conflicts → real merge** (Gravity functionality must be re-applied onto v2.2.0). Ranked by Gravity-diff size:

  | Tier | Files | Notes |
  |---|---|---|
  | **Deep / consensus-critical** | `storage/provider/.../database/provider.rs` (322), `engine/tree/src/persistence.rs` (217), `engine/tree/src/tree/mod.rs` (193) + `tree/tests.rs` (175), `storage/provider/.../static_file/manager.rs` (126) | Gravity storage + pipe-exec engine integration vs v2.2.0's rewritten internals. **Pair with authors + Phase-6 state-root replay. Do NOT guess.** |
  | **Localized real** | `ethereum/evm/src/lib.rs` (89, lazy-reward/system-call EVM cfg), `chainspec/src/spec.rs` (72, Gravity hardfork schedule + 50-Gwei min base fee), `storage/.../blockchain_provider.rs` (32), `cli/commands/src/common.rs` (32) | Well-understood; localized; tractable with care. |
  | **Cargo wiring** | root `Cargo.toml` + 7 crate `Cargo.toml` | Take v2.2.0's version numbers, redirect revm-family to the §7 forks via `[patch.crates-io]`, add Gravity members + `gravity-api-types`. **op-revm omitted.** |
  | **Small** | `node/core/{args,node_config}`, `primitives-traits/{lib,storage}`, `optimism/chainspec`, `rpc-eth-api/helpers/transaction`, `ethereum/node`, `cli/commands/node`, stages benches | <16-line Gravity diffs; mostly mechanical. |

**Reproduce the split:**
```bash
git checkout -b transplant v2.2.0 && git cherry-pick -n gravity-base/v1.8.3-clean-ancestry
# classify: take-HEAD vs merge
for f in $(git diff --name-only --diff-filter=U); do
  git diff v1.8.3 gravity-base/v1.8.3-clean-ancestry -- "$f" \
    | grep -qiE 'gravity|pipe_exec|min_base_fee|lazy_reward|gravity_storage|coinbase.*tip' \
    && echo "MERGE  $f" || echo "HEAD   $f"
done
# auto-resolve the HEAD ones, then hand-merge the ~30 MERGE ones
```

**Status:** the 81-mechanical resolution is reproducible in seconds; the 30 real merges (esp. the 5 deep storage/engine ones) are the dedicated, author-paired, compile-fed effort gated on Phase-6 verification. This trial confirms the transplant is **bounded and tractable** — the bulk is mechanical, the irreducible hard core is ~5 files of storage/engine integration.

