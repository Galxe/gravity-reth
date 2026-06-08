# revm Fork Migration Eval: v29.0.1-gravity → revm 38 (the reth-2.x long pole)

**Author:** Richard, 2026-06-08
**Repo evaluated:** `Galxe/revm` (cloned, branch `v29.0.1-gravity`) against `bluealloy/revm`.
**Why:** revm is the critical-path dependency for the reth 1.8.3 → 2.2.0 rebase (reth 2.2.0 pins **revm 38.0.0**). This eval scopes that bump. No code changed.

---

## 0. TL;DR — much smaller than feared

- The Gravity revm fork is **2 semantic commits** on top of upstream revm 29 (the other 2 commits on the branch are upstream's own v87 release + changelog).
- **Commit 1 — "disable fee-charge" — should be DROPPED.** Upstream revm has since adopted the same concept natively as the **`optional_fee_charge` feature** (`is_fee_charge_disabled`, PRs #3005/#3007/#3020/#3401/#3559). Forward-port = delete Gravity's version, switch to the upstream feature flag.
- **Commit 2 — "lazy reward" — is the real work.** Not upstreamed. ~20 files threading a `ResultAndReward` return + `is_lazy_reward()` cfg through the handler/inspector pipeline. This is essentially the entire remaining Gravity revm delta.
- Net: the revm "9-major-version bump" collapses to **port one feature (lazy reward) into revm 38's reorganized handler/inspector traits + adopt one upstream feature**. Days, not weeks — though it is **consensus-critical** (it changes reward/balance accounting).

---

## 1. Facts

| Item | Value |
|---|---|
| Gravity fork branch | `Galxe/revm` `v29.0.1-gravity` (revm meta-crate 29.0.1) |
| Upstream fork point | bluealloy `v86` = revm **29.0.0** (2025-08-24) |
| Target | revm **38.0.0** = bluealloy `afc22938` ("chore: release #3679", 2026-05-19) — what reth v2.2.0 pins |
| Gravity delta vs fork point | **41 files, +311 / −105** (examples included) |
| Gravity-specific commits | **2** (`1310559a` fee-charge, `bfa048c6` lazy-reward); + 2 upstream maintenance commits on the branch |

### What Gravity customizes (nature)

Both changes are **fee/reward accounting** hooks — semantically load-bearing because they change balances → state root → consensus:

1. **`is_fee_charge_disabled()`** on the Cfg trait — skip fee charging (Gravity system txs pay no gas; ties to the `SYSTEM_CALLER` treasury accounting verified in `mainnet-funds-flow-verification.md`).
2. **`is_lazy_reward()` + `ResultAndReward`** — defer/ surface the block-reward/priority-fee amount out of execution instead of crediting the beneficiary inline, so Gravity's layer distributes it (ties to the coinbase-tip / epoch-reward accounting).

## 2. Trial rebase result (measured)

`git rebase --onto <revm38> <v86> v29.0.1-gravity` stops on the first commit with **4 conflicting files**:

```
context/Cargo.toml
context/interface/src/cfg.rs
context/src/cfg.rs
op-revm/src/handler.rs
```

Per-file upstream churn in the files Gravity touches (29→38):

| File | Gravity Δ | upstream 29→38 Δ | note |
|---|--:|--:|---|
| `op-revm/src/handler.rs` | 10 | **1006** | upstream heavily rewrote; re-place small hook |
| `context/src/cfg.rs` | 36 | 355 | cfg trait reorg |
| `handler/src/handler.rs` | 21 | 193 | handler trait reorg (lazy-reward lands here) |
| `op-revm/src/api/exec.rs` | 17 | 154 | |
| `handler/src/post_execution.rs` | 19 | 131 | reward path |
| `inspector/src/inspect.rs` | 28 | 0 | clean |

The pattern: **Gravity's edits are tiny but land in files upstream rewrote** — the difficulty is *understanding revm 38's new trait shapes to re-place the hooks correctly*, not volume.

## 3. Migration plan for `v38.0.0-gravity`

1. **Branch:** `git checkout -b v38.0.0-gravity <revm38 commit>` (off bluealloy revm 38, clean ancestry).
2. **Drop the fee-charge commit.** Adopt upstream `optional_fee_charge` instead:
   - enable the feature where Gravity builds revm
   - replace Gravity's `is_fee_charge_disabled` call sites with the upstream API (semantics match; verify behavior parity for system txs)
3. **Port the lazy-reward change** (`bfa048c6`) onto revm 38:
   - re-introduce `is_lazy_reward()` on the (reorganized) Cfg trait
   - re-thread `ResultAndReward` through revm 38's handler `execute`/`post_execution` and the inspector handler
   - the inspector files had ~0 upstream churn → those hunks apply cleanly; the handler files are the work
4. **Verify (consensus-critical):**
   - `cargo build -p revm -p op-revm` + `cargo nextest run` (revm has a thorough suite incl. statetests)
   - targeted: a tx that exercises lazy-reward must produce the **exact same beneficiary balance delta** as v29.0.1-gravity for the same input (reward amount parity)
5. **Publish** `v38.0.0-gravity`; reth's Cargo.toml then points at it.

> **Keep the fork minimal (goal-2 hygiene).** Every line Gravity carries in revm is a line that conflicts on the next revm bump. Since upstream absorbed fee-charge, the fork should shrink to ~the single lazy-reward feature. If lazy-reward can be expressed via an existing upstream extension point (custom handler) rather than editing upstream handler files inline, that's worth a hard look — it could make future revm bumps nearly conflict-free.

## 4. Impact on the reth rebase critical path

The reth plan (`rebase-to-reth-2.x-plan.md` §3) called revm "the long pole, 9 major versions". This eval revises that: the *upstream API churn* is real, but the *Gravity surface* to carry across it is **one feature**. The revm prerequisite is therefore smaller and more parallelizable than the headline "29→38" suggested — but it must be done first and verified for reward-accounting parity before the reth execution layer (Layer B) can compile or be trusted.

## 5. Open questions

1. **Can lazy-reward move to a custom handler** (upstream extension point) instead of inline edits to `handler/src/*.rs`? If yes, the revm fork could become ~zero-conflict on future bumps.
2. **op-revm dependence:** Gravity edits `op-revm` (the Optimism revm variant). Does Gravity-reth actually use op-revm, or is that delta vestigial? If unused, drop those hunks entirely.
3. **`#2980` provenance:** confirm Gravity's fee-charge commit is functionally identical to upstream `optional_fee_charge` before dropping it (behavior parity for system-tx fee skipping).

## Appendix — reproduce

```bash
git clone https://github.com/Galxe/revm && cd revm
git remote add bluealloy https://github.com/bluealloy/revm && git fetch bluealloy --tags
git fetch origin v29.0.1-gravity
MB=$(git merge-base origin/v29.0.1-gravity bluealloy/main)         # → v86 / revm 29.0.0
C38=$(git log bluealloy/main -S 'version = "38.0.0"' --oneline -- crates/revm/Cargo.toml | head -1 | awk '{print $1}')
git log --oneline $MB..origin/v29.0.1-gravity                       # → the 2 (+2) commits
git log bluealloy/main --oneline | grep -iE 'optional_fee_charge|is_fee_charge_disabled'   # upstream adopted it
git checkout -b _trial origin/v29.0.1-gravity && git rebase --onto $C38 $MB   # → real conflicts
git rebase --abort
```
