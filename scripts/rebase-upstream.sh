#!/usr/bin/env bash
# rebase-upstream.sh — rebase Gravity-reth onto a newer upstream reth tag.
#
# Why this exists: see docs/design/upstream-rebase-workflow.md
#   - The fork's ancestry was re-anchored onto a real upstream tag, so
#     `git rebase` finally works.
#   - But reth cuts maintenance releases on side branches, so a plain
#     `git rebase <tag>` picks the wrong base. This script always uses the
#     explicit `--onto <new> <current-base>` form.
#
# Usage:
#   bash scripts/rebase-upstream.sh <new-upstream-tag> <current-base-tag> [branch]
#   e.g. bash scripts/rebase-upstream.sh v2.3.0 v2.2.0
#        bash scripts/rebase-upstream.sh v2.2.0 v1.8.3 feat/rebase-reth-2.x
#
# It does NOT auto-resolve conflicts or push — it stops at the first conflict
# and prints a categorized conflict map so you can resolve subsystem by subsystem.

set -euo pipefail

NEW_TAG="${1:?usage: rebase-upstream.sh <new-upstream-tag> <current-base-tag> [branch]}"
BASE_TAG="${2:?need current-base-tag (the upstream tag the fork currently sits on, e.g. v2.2.0)}"
BRANCH="${3:-$(git rev-parse --abbrev-ref HEAD)}"
UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"

echo "[rebase-upstream] fetching ${UPSTREAM_REMOTE} tags…"
git fetch "${UPSTREAM_REMOTE}" --tags --quiet

for t in "$NEW_TAG" "$BASE_TAG"; do
  git rev-parse -q --verify "refs/tags/${t}^{commit}" >/dev/null \
    || { echo "[rebase-upstream] ERROR: tag '${t}' not found (fetch ${UPSTREAM_REMOTE}?)"; exit 1; }
done

# Safety: confirm the base tag really is the fork's current ancestor.
if ! git merge-base --is-ancestor "$BASE_TAG" "$BRANCH"; then
  cat >&2 <<EOF
[rebase-upstream] ERROR: '${BASE_TAG}' is not an ancestor of '${BRANCH}'.
  The fork's ancestry is not anchored on ${BASE_TAG}. Either you passed the
  wrong base tag, or the one-time re-anchor (see upstream-rebase-workflow.md §2)
  hasn't been done for this base. Aborting before making a mess.
EOF
  exit 1
fi

# Safety backup tag.
STAMP_TAG="backup/pre-rebase-${BASE_TAG}-to-${NEW_TAG}"
git tag -f "$STAMP_TAG" "$BRANCH" >/dev/null
echo "[rebase-upstream] backup tag: ${STAMP_TAG} → $(git rev-parse --short "$BRANCH")"

echo "[rebase-upstream] git rebase --onto ${NEW_TAG} ${BASE_TAG} ${BRANCH}"
if git rebase --onto "$NEW_TAG" "$BASE_TAG" "$BRANCH"; then
  echo "[rebase-upstream] ✅ clean rebase — no conflicts. Build + test before pushing."
  exit 0
fi

echo
echo "[rebase-upstream] ⚠️ conflicts — categorized by subsystem (resolve one subsystem at a time):"
git diff --name-only --diff-filter=U | grep '^crates/' \
  | sed 's|crates/||; s|/.*||' | sort | uniq -c | sort -rn
echo
echo "[rebase-upstream] full list:"
git diff --name-only --diff-filter=U | sed 's/^/    /'
cat <<EOF

[rebase-upstream] Next:
  - Resolve conflicts. PREFER moving Gravity changes into Gravity-owned crates /
    extension traits over re-applying inline (keeps future rebases cheap —
    see upstream-rebase-workflow.md §4).
  - 'git add <file>' as you finish each, then 'git rebase --continue'.
  - Build + test (state-root parity is non-negotiable) before pushing.
  - Abort with 'git rebase --abort' (restores ${STAMP_TAG}).
EOF
exit 2
