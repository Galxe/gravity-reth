# Upstream Rebase Workflow (goal: make every future reth rebase cheap)

**Author:** Richard, 2026-06-08
**Companion:** [`rebase-to-reth-2.x-plan.md`](./rebase-to-reth-2.x-plan.md) (the one-time big migration this workflow grew out of).

This document explains **why every past rebase was a huge conflict**, and the workflow that makes future rebases a routine `git rebase` instead of git archaeology.

---

## 1. Root cause: broken git ancestry

The fork's history was built by **importing the reth source tree as squashed commits** (e.g. `d620fd0eeb "feat: merge reth-v1.8.3 (#205)"` — a single-parent commit whose tree was overwritten with reth 1.8.3, *not* a git merge of the `v1.8.3` tag).

Consequence: the real upstream tag (`v1.8.3`) is **not an ancestor of `main`**. So:

```bash
git merge-base main upstream/main      # → a Sept-2024 commit (4600+ commits stale)
git rebase upstream/v2.2.0             # → tries to replay the entire imported tree → garbage
```

Every "rebase" therefore degenerated into a manual file-by-file tree diff with no 3-way merge help — hence the pain.

## 2. The fix: re-anchor on the real upstream tag (one-time)

Restore ancestry by creating a commit whose **parent is the real upstream tag** and whose **tree is the current fork**:

```bash
# foundation = real upstream v1.8.3  +  one commit carrying the fork's exact tree
git checkout -b _reanchor v1.8.3
git read-tree <fork-main> && git checkout-index -fa && git clean -fdq && git add -A
git commit -m "gravity: re-anchor customizations on upstream v1.8.3"
# verify: tree identical to fork-main, and v1.8.3 is now a real ancestor
git merge-base --is-ancestor v1.8.3 HEAD   # → success
```

This is preserved as tag **`gravity-base/v1.8.3-clean-ancestry`**. Once the rebase-to-2.x lands, the new `main` will have `v2.2.0` as a real ancestor and **this step never needs doing again** — that's the whole point.

## 3. The recurring workflow (after ancestry is clean)

From then on, every upstream bump is:

```bash
git fetch upstream --tags
# IMPORTANT: use --onto with the *base tag the fork currently sits on*, not plain `git rebase`.
git rebase --onto <new-tag> <current-base-tag> main
# resolve conflicts (now real 3-way merges, scoped to files we actually modified)
# build + test, then fast-forward main
```

### ⚠️ Gotcha discovered during the 1.8.3→2.2.0 work: reth release-branch topology

reth cuts maintenance releases (`v1.8.2`, `v1.8.3`, `v1.8.4`) on a **side branch off `v1.8.1`**, not on `main`. And the `1.x → 2.0` transition is itself a branch point: `merge-base(v1.8.3, v2.2.0) = v1.8.1`.

So plain `git rebase v2.2.0` picks `v1.8.1` as the base and tries to replay the 9 upstream `1.8.x` maintenance commits (e.g. the edition-2024 bump) as if they were ours. **Always pin the base explicitly:**

```bash
git rebase --onto v2.2.0 v1.8.3    # replay ONLY commits after v1.8.3 (= our customizations)
```

This is automated in [`scripts/rebase-upstream.sh`](../../scripts/rebase-upstream.sh).

## 4. The durable fix: shrink the conflict surface

Re-anchoring makes `git rebase` *work*; it doesn't make conflicts *zero*. The 1.8.3→2.2.0 trial produced **111 conflicting files** — every one is a place where Gravity edits an upstream file in-place that upstream also changed.

The number of recurring conflicts ≈ the number of upstream files we modify inline. To drive it down, move Gravity logic **out of upstream files** into extension points reth already provides:

| Conflict source today | Extraction target |
|---|---|
| inline edits in `crates/storage/**` (27 conflicts) | a Gravity storage crate implementing reth storage traits, wired via the provider factory |
| inline edits in `crates/engine/tree/**` (8) | custom `PayloadValidator` / engine component via `NodeBuilder` hooks |
| inline edits in `crates/ethereum/evm`, `crates/evm` (5) | already partly in `pipe-exec-layer-ext-v2`; push remaining hooks behind the `ConfigureEvm` trait |
| inline edits in `crates/stages/**` (11) | custom `StageSet` instead of editing upstream stages |
| inline edits in `crates/node/**`, `cli/**` | `NodeBuilder` add-ons / CLI extension, not forked commands |

Each upstream file we stop editing inline = one fewer conflict **forever**. This is tracked as a separate refactor track (see plan doc §4 / the "Layer" structure) and should be done opportunistically *while* resolving each subsystem's conflicts during the 2.x rebase — i.e. don't just re-apply the inline edit, ask "can this live in a Gravity-owned crate instead?"

## 5. Cheat sheet

```bash
# one-time (already done, tag: gravity-base/v1.8.3-clean-ancestry):
#   re-anchor fork tree onto real upstream tag

# every future rebase:
bash scripts/rebase-upstream.sh <new-upstream-tag> <current-base-tag>
#   e.g. bash scripts/rebase-upstream.sh v2.3.0 v2.2.0

# the rule that keeps it cheap:
#   when resolving a conflict in an UPSTREAM file, prefer moving the change
#   into a Gravity-owned crate / extension trait over re-applying it inline.
```
