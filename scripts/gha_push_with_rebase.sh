#!/usr/bin/env bash
set -euo pipefail

# Push the current committed HEAD to a GitHub Actions output branch, rebasing
# the commit on top of the latest remote tip if another scheduled job pushed
# first. Call this AFTER `git commit`; the artifact changes live in HEAD, not
# in the index. This avoids the bug where a failed push followed by checking
# `git diff --cached` concludes there is "nothing to push" and silently drops
# the output commit.
BRANCH="${1:-main}"
MAX_ATTEMPTS="${2:-3}"

for i in $(seq 1 "${MAX_ATTEMPTS}"); do
  if git push origin "HEAD:${BRANCH}"; then
    exit 0
  fi

  echo "Push failed (attempt ${i}/${MAX_ATTEMPTS}); rebasing committed outputs onto origin/${BRANCH}..."
  git fetch origin "${BRANCH}"
  # The replayed commit only contains this job's auto-generated artifacts, so on
  # any conflict (e.g. add/add on a per-run marker written by a concurrent job)
  # keep our freshly generated side rather than wedging the rebase. `theirs`
  # from rebase's perspective is the commit being replayed = our outputs.
  if ! git rebase -X theirs "origin/${BRANCH}"; then
    echo "Rebase could not auto-resolve; aborting and retrying."
    git rebase --abort || true
  fi
done

echo "Failed to push outputs after ${MAX_ATTEMPTS} attempts"
exit 1
