#!/usr/bin/env bash
# Local fallback for the morning alert.
# Runs at 09:28 Shanghai via launchd, sends Telegram if GitHub Actions
# didn't already fire the morning check for today's watchlist.
#
# Uses a dedicated shallow clone so it never touches the interactive repo.

set -euo pipefail

ORIGIN_DIR="/Users/rayhuang/Documents/Python Project/dragon-pulse"
PYTHON="/opt/anaconda3/bin/python"
AUTO_DIR="${HOME}/.dragon-pulse-auto"
LOG_DIR="${ORIGIN_DIR}/outputs/local_logs"
LOG_FILE="${LOG_DIR}/morning_alert_$(date +%Y-%m-%d).log"

mkdir -p "${LOG_DIR}"
exec >> "${LOG_FILE}" 2>&1

echo "=== Local morning alert: $(date) ==="

# --- Maintain a dedicated automation clone ---
if [ ! -d "${AUTO_DIR}/.git" ]; then
    echo "Creating automation clone at ${AUTO_DIR}..."
    git clone --depth 1 --branch main "$(cd "${ORIGIN_DIR}" && git remote get-url origin)" "${AUTO_DIR}"
    # Copy .env from main repo (not in git)
    cp "${ORIGIN_DIR}/.env" "${AUTO_DIR}/.env"
fi

cd "${AUTO_DIR}"

# Always reset to latest remote state (safe — this clone has no user work)
git fetch origin main --depth 1 --quiet
git reset --hard origin/main --quiet

# Keep .env in sync
cp "${ORIGIN_DIR}/.env" "${AUTO_DIR}/.env" 2>/dev/null || true

# Load .env
set -a
source "${AUTO_DIR}/.env"
set +a

# Find the latest watchlist by filename date
LATEST_WL=$(ls outputs/*/execution_watchlist_*.json 2>/dev/null | sort -t_ -k3 -r | head -1)
if [ -z "${LATEST_WL}" ]; then
    echo "No watchlist found. Nothing to do."
    exit 0
fi

TRADE_DATE=$(basename "${LATEST_WL}" | sed 's/execution_watchlist_//;s/\.json//')
echo "Latest watchlist: ${LATEST_WL} (trade date: ${TRADE_DATE})"

# The shared dedup marker is now verdict-aware. morning_check.py decides
# whether to skip (same verdict) or re-send a corrected alert. Always run it
# and let the script handle dedup; the marker file is still pushed below.
MORNING_MARKER="outputs/${TRADE_DATE}/.morning_alert_sent"
echo "Running morning check locally (marker dedup handled inside script)."
${PYTHON} scripts/morning_check.py --date "${TRADE_DATE}"

# Push the marker so a late CI run sees it and skips
if [ -f "${MORNING_MARKER}" ]; then
    git add "${MORNING_MARKER}"
    git commit "${MORNING_MARKER}" -m "auto: local morning alert marker for ${TRADE_DATE}" --quiet

    pushed=false
    for i in 1 2 3; do
        if git push origin main --quiet 2>/dev/null; then
            echo "Marker pushed to remote."
            pushed=true
            break
        fi
        echo "Push attempt ${i}/3 failed, rebasing..."
        git fetch origin main --quiet
        git rebase origin/main --quiet
    done

    if [ "${pushed}" != "true" ]; then
        echo "ERROR: Failed to push marker after 3 attempts. Late CI may duplicate."
    fi
fi
echo "=== Done: $(date) ==="
