#!/usr/bin/env bash
set -euo pipefail

BRANCH="${1:-main}"
MAX_ATTEMPTS="${2:-3}"

pushed=false
for i in $(seq 1 "${MAX_ATTEMPTS}"); do
  if git push origin "HEAD:${BRANCH}"; then
    pushed=true
    break
  fi

  echo "Push failed (attempt ${i}/${MAX_ATTEMPTS}), rebasing while preserving staged outputs..."
  staged_list="$(mktemp)"
  git diff --cached --name-only > "${staged_list}"

  if [ ! -s "${staged_list}" ]; then
    echo "No staged files remain after failed push; nothing to push."
    pushed=true
    break
  fi

  git stash push --staged --message "gha-staged-output-rebase-${i}" || true
  git fetch origin "${BRANCH}"
  git rebase "origin/${BRANCH}"
  git stash pop || true

  # Stash pop restores files to the worktree, but not necessarily the index.
  # Restage exactly the files this job intended to publish.
  xargs -r git add < "${staged_list}"
  rm -f "${staged_list}"
done

if [ "${pushed}" != "true" ]; then
  echo "Failed to push outputs after ${MAX_ATTEMPTS} attempts"
  exit 1
fi
