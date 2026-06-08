# Gravity-reth Rebase Plan: reth 1.8.3 → 2.x

**Status:** Planning only — no code changes yet.
**Author:** Richard, 2026-06-08
**Decision inputs:** target = **latest stable on the main line, v2.2.0** (corrected from an earlier draft that wrongly picked v1.11.4 — see §1.3); approach = plan-doc first, then execute.

---

## 0. TL;DR

- Gravity-reth is currently based on **upstream reth v1.8.3** (Sept 2025). Upstream's main line is now **v2.x** — latest stable tag **v2.2.0** (Apr 2026), and `main` tip is v2.2.0-dev (June 2026). The 1.x line ended at v1.11; everything after is v2.x.
- **Target v2.2.0** (or the latest stable when execution starts). Forking to a 1.x tag would land us on an already-superseded line and force a second migration within months.
- **`git rebase` is NOT usable here.** The fork's git ancestry to upstream is broken (only shared merge-base is a Sept-2024 commit). The fork was built by importing the reth 1.8.3 tree, not by git rebase. So `git rebase` would replay from 2024 — meaningless. The real task is a **tree-based semantic migration** driven by `git diff v1.8.3 main`.
- The hardest part is **not reth itself** — it's the coordinated bump of four Galxe dependency forks, dominated by **revm v29 → v38 (9 major versions)**.
- **MSRV bumps 1.88 → 1.93** for v2.x — a Rust toolchain upgrade is now part of the work (this was *not* true for the 1.11 target, which is one reason the target choice matters).

---

## 1. Confirmed current state

### 1.1 Base version

| Fact | Value | How determined |
|---|---|---|
| Fork base | **reth v1.8.3** | `Cargo.toml version = "1.8.3"`; minimal `git diff main <tag>` is against `v1.8.3` |
| Git merge-base with upstream | `75b7172cf7` (2024-09-19) | `git merge-base main upstream/v1.8.0` — **proves ancestry is broken** |
| Fork customization surface | **277 files, +2330 / −32058** vs v1.8.3 (−32K = upstream files the fork stripped: book/docs/workflows) | `git diff --shortstat main v1.8.3` |

### 1.2 Upstream version line (so the target is unambiguous)

| Tag | Date | On main line? | rust | revm | alloy-evm | alloy-consensus | revm-inspectors |
|---|---|---|---|---|---|---|---|
| v1.11.0 | 2026-02-12 | ✅ | 1.88 | 34 | 0.27.2 | 1.6.3 | 0.34.2 |
| v1.11.4 | 2026-04-30 | ❌ **side branch** (1.11 backport) | — | — | — | — | — |
| v2.0.0 | 2026-04-07 | ✅ | **1.93** | 36 | 0.30 | 1.8.2 | — |
| v2.1.0 | 2026-04-20 | ✅ | 1.93 | 37 | — | — | — |
| **v2.2.0** ← target | 2026-04-29 | ✅ | **1.93** | **38** | **0.34.0** | **2.0.4** | **0.39.0** |
| main tip (2.2.0-dev) | 2026-06-07 | ✅ | 1.93 | 40.0.3 | 0.36.0 | — | 0.40.0 |

> v1.11 was the last 1.x minor; the main line then jumped to v2.0.0. **v1.11.4 is a maintenance backport off the 1.11 branch, not on the forward line — do not target it.**

### 1.3 Why v2.2.0, not v1.11

| Target | revm jump | MSRV | Trade-off |
|---|---|---|---|
| → v1.11.0 | 29 → 34 (5 major) | 1.88 → 1.88 ✅ | Smaller hop, but lands on a dead line → second migration needed within months |
| **→ v2.2.0** | 29 → 38 (**9 major**) | 1.88 → **1.93** | Bigger hop + toolchain bump, but on the forward line |

We pay the big revm-fork cost either way; the marginal cost of 34→38 on top of 29→34 is far less than running a whole second migration later. **Recommend the single jump to v2.2.0.** (Optional staged route in §6.)

### 1.4 Dependency forks Gravity maintains (all must move together)

| Dependency | Gravity pin (now) | reth v2.2.0 wants | Jump |
|---|---|---|---|
| **revm** | `Galxe/revm` `v29.0.1-gravity` | **revm 38.0.0** | **9 major** ⚠️ keystone |
| **grevm** (parallel EVM) | `Galxe/grevm` `v2.2.4` | must compile vs revm 38 | connected to revm |
| **alloy-evm** | `Galxe/alloy-evm` `v0.21.3-gravity` | 0.34.0 | ~13 minor |
| **revm-inspectors** | `Galxe/revm-inspectors` `v0.30.0-gravity` | 0.39.0 | 9 minor |
| alloy-consensus / -eips / etc. | 1.0.37 | 2.0.4 | major |
| **rust-version (MSRV)** | 1.88 | **1.93** | toolchain bump ⚠️ |

> "Rebase reth" = "**bump 5 interdependent crates in lockstep + upgrade the Rust toolchain**". revm is the keystone: grevm, alloy-evm, revm-inspectors, and reth's `evm/` all pin a specific revm version. You cannot touch reth until a `Galxe/revm` branch tracks revm 38.

---

## 2. Why `git rebase` is the wrong tool (and what to do instead)

A clean fork keeps git ancestry so `git rebase upstream/v2.2.0` replays only the fork's own commits. **Gravity-reth does not have this** — its history was built by importing the reth 1.8.3 tree wholesale:

- `git merge-base main upstream/v2.2.0` = a 2024-09 commit
- `git rebase upstream/v2.2.0` would attempt to replay the entire imported reth 1.8.3 tree → catastrophic, meaningless conflicts

**Correct approach — "transplant", not "rebase":**

1. Compute Gravity's customization as a reviewable diff: `git diff v1.8.3 main` (the *semantic* fork delta — 277 files).
2. Check out upstream `v2.2.0` as the new base on a fresh branch.
3. Re-apply the customization in **layered chunks** (§4), fixing API breaks per chunk.
4. Bump the four Galxe dependency forks to revm-38-compatible branches **first** (prerequisites — §3).

This is a porting exercise driven by the *diff*, not by git commit replay.

---

## 3. Dependency-fork prerequisites (do these FIRST, in their own repos)

reth v2.2.0 will not compile until these exist. Sequenced by dependency graph:

1. **`Galxe/revm`: create `v38.0.0-gravity` branch.** ⚠️ critical path
   - Rebase Gravity's revm delta (what `v29.0.1-gravity` adds over stock revm 29) onto stock revm 38.
   - **revm 29 → 38 is 9 major versions** — Context/Evm/Handler/Inspector traits changed repeatedly. This is the single largest sub-task of the whole project.
   - First identify the delta: `git diff v29.0.1 v29.0.1-gravity` in the revm repo.

2. **`Galxe/grevm`: bump to compile against revm 38.** grevm (parallel EVM) is tightly coupled to revm internals; a 9-major jump will break it substantially — treat as its own mini-migration.

3. **`Galxe/alloy-evm`: create `v0.34.0-gravity` branch** (rebase Gravity delta 0.21.3 → 0.34.0).

4. **`Galxe/revm-inspectors`: create `v0.39.0-gravity` branch** (0.30 → 0.39).

> **Sequencing:** revm → (grevm + alloy-evm, both depend on revm) → revm-inspectors → then reth.

---

## 4. reth customization re-apply — layered chunks

Gravity's 277-file delta vs v1.8.3, grouped by transplant difficulty. Easiest/safest first so we fail fast on hard parts only after the scaffolding compiles.

### Layer A — Gravity-owned crates (carry whole, fix only outbound API calls)

Don't exist upstream → zero merge conflicts; just update their *calls into reth* for v2.2.0 APIs.

| Crate | Files | Role |
|---|---|---|
| `crates/pipe-exec-layer-ext-v2` | 47 | **Core integration** — drives execution from Aptos consensus ordering; fills `parent_beacon_block_root`/blob fields; EIP-2935 (`eip_2935.rs`); tx filtering. |
| `crates/gravity-storage` | 3 | Gravity storage abstraction |
| `crates/gravity-primitives` | 3 | Gravity primitive types |

Risk: medium — they call deep reth APIs (execution/state/trie) that churned across 1.8→2.2, but conflicts surface as compile errors, not merge markers.

### Layer B — Execution / EVM (highest risk: revm 29→38 + MSRV land here)

| File(s) | Why Gravity touches it | Risk |
|---|---|---|
| `crates/evm/evm/src/{execute,lib,either,noop}.rs` | execution hooks + grevm wiring | **High** — revm 38 + alloy-evm 0.34 rewrote these traits |
| `crates/ethereum/evm/src/lib.rs` | EVM config, system calls (EIP-4788/2935/7702) | **High** |
| `crates/ethereum/node/src/node.rs` | node assembly | Medium |

### Layer C — Engine / block production

| File(s) | Why | Risk |
|---|---|---|
| `crates/engine/tree/src/tree/{mod,payload_validator}.rs` | hooks block validation into Aptos-ordered execution | **High** — engine tree changed a lot 1.8→2.2 |
| `crates/engine/tree/src/persistence.rs`, `block_buffer.rs` | persistence tweaks | Medium |
| `crates/consensus/common/src/validation.rs` | changed header validation for Gravity | Medium |

### Layer D — Storage / trie / stages

| Files | Why | Risk |
|---|---|---|
| `crates/storage/**` (46) | Gravity state storage integration | Medium-High |
| `crates/trie/**` (19) | state-root computation tied to pipe-exec | Medium-High |
| `crates/stages/**` (15) | custom/disabled stages | Medium |

### Layer E — Periphery (lowest risk)

chainspec (5), cli/commands (11), net (4), transaction-pool (6), rpc (12), prune (3), e2e-test-utils (5). Mostly mechanical API-rename fixes.

---

## 5. Suggested execution sequence

```
Phase 0  Prep
  - bump Rust toolchain to 1.93 (rust-toolchain.toml), confirm workspace builds pre-change
  - branch: rebase/reth-2.2.0 off a fresh checkout of upstream v2.2.0
  - keep backup tag backup/pre-rebase-galxe-main (already created)

Phase 1  Dependency forks (BLOCKING — in dep repos, not reth)
  - Galxe/revm            v29.0.1-gravity → v38.0.0-gravity   (long pole)
  - Galxe/grevm           v2.2.4 → revm-38-compatible
  - Galxe/alloy-evm       v0.21.3-gravity → v0.34.0-gravity
  - Galxe/revm-inspectors v0.30.0-gravity → v0.39.0-gravity
  - gate: each compiles standalone against its new upstream + passes its own tests

Phase 2  reth scaffolding compiles
  - point Cargo.toml at new dep-fork branches + reth 2.2.0 versions
  - transplant Layer A crates, get workspace to parse

Phase 3  Execution correctness (Layer B + C)
  - revm-38 execution path, engine tree, validation
  - gate: single-node devnet produces blocks; state root matches

Phase 4  Storage/trie/stages (Layer D)
  - gate: full sync from genesis on a staging chain; state-root parity

Phase 5  Periphery (Layer E) + tests
  - gate: gravity_eip2935_test, gravity_eip7702_test, e2e suite green

Phase 6  Verification before any mainnet exposure
  - replay a known mainnet block range, assert byte-identical state roots
  - shadow a VFN against live mainnet for N hours, diff block hashes
```

**Gate discipline:** never advance until the gate passes. State-root parity (Phase 3/4/6) is non-negotiable — one mismatch = consensus fork = chain halt.

---

## 6. Optional staged route (lower per-step risk, more total cycles)

If the team prefers smaller verifiable hops over one big jump:

```
1.8.3  →  1.11.0   (revm 29→34, MSRV stays 1.88, last 1.x on main)
       →  2.2.0    (revm 34→38, MSRV 1.88→1.93)
```

Pro: each step's state-root verification covers a smaller change set. Con: two full migration+verification cycles, and 1.11.0 is already EOL on the forward line. **Default recommendation remains the single jump to v2.2.0** unless verification risk is judged too high to do at once.

---

## 7. Risk register

| Risk | Severity | Mitigation |
|---|---|---|
| revm 29→38 (9 major) breaks grevm + Gravity revm fork | **Critical** | Do revm/grevm first as a standalone sub-project; don't start reth until they compile + pass revm's own tests |
| State-root divergence from a subtle execution change | **Critical** | Phase 6 mainnet block-range replay with byte-exact state-root assertion before any rollout |
| MSRV 1.88→1.93 surfaces new lints / edition changes | Medium | Bump toolchain in Phase 0, fix warnings before transplant |
| `pipe-exec-layer-ext-v2` calls removed/renamed reth APIs | High | Layer A first; treat compile errors as the API-break checklist |
| engine/tree payload-validation flow changed | High | Layer C dedicated review against upstream 1.8→2.2 engine changes |
| alloy-consensus 1.0→2.0 type changes ripple widely | Medium | Bump alloy early in Phase 2, fix type errors broadly |
| Hidden divergence because git ancestry is broken | Medium | Drive everything off `git diff v1.8.3 main`, not git replay |
| Re-introducing the EIP-4788 `parent_beacon_block_root` semantic issue | Low | Fold the fix into Layer A while in pipe-exec anyway |

---

## 8. Effort estimate (rough)

| Phase | Optimistic | Likely |
|---|---|---|
| 1 — dependency forks (revm 9-major = long pole) | 2 wk | 3–4 wk |
| 2 — scaffolding compiles | 3 d | 1 wk |
| 3 — execution correctness | 1 wk | 2 wk |
| 4 — storage/trie/stages | 1 wk | 2 wk |
| 5 — periphery + tests | 3 d | 1 wk |
| 6 — verification | 3 d | 1 wk |
| **Total** | **~5 wk** | **~9–11 wk** |

Multi-week, multi-repo project. The revm 9-major bump dominates the critical path (longer than the 5-major estimate for a 1.11 target).

---

## 9. Open questions for the team

1. **Target confirm:** v2.2.0 (recommended) vs main tip (revm 40, bleeding-edge) vs the staged 1.11→2.2 route (§6)?
2. **What's driving the rebase?** A specific upstream feature/fix, security patches, or general drift-reduction? If it's one specific fix, a smaller targeted backport may beat a full migration.
3. **Who owns the revm fork bump?** It's the long pole (9 major) and needs whoever knows the Gravity revm customizations best.
4. **Toolchain coordination:** MSRV 1.93 — confirm CI images, build infra, and other Galxe repos that share the toolchain are ready.

---

## Appendix — reproduce the analysis

```bash
cd gravity-reth
git fetch galxe && git fetch upstream
grep -m1 '^version = ' Cargo.toml                 # → 1.8.3 (fork base)
git diff --shortstat main v1.8.3                  # → ~276 files (fork delta)
git merge-base main upstream/v1.8.0               # → 2024-09 commit (broken ancestry)
# version line + deps
for t in v1.11.0 v2.0.0 v2.2.0; do echo "[$t]"; \
  git show $t:Cargo.toml | grep -E '^version|^rust-version|^revm = |alloy-evm = |alloy-consensus = '; done
# which tags are on the main line
git merge-base --is-ancestor v1.11.4 upstream/main && echo on-main || echo side-branch
# customization by subsystem
git diff --name-only main v1.8.3 -- 'crates/**' | sed 's|crates/||;s|/.*||' | sort | uniq -c | sort -rn
```
