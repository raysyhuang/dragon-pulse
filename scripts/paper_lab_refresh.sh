#!/usr/bin/env bash
# Forward-refresh the paper lab so the surviving sleeves accumulate live evidence.
#
# LOCAL ONLY: Tushare rejects GitHub/CI runner IPs, so this can't run in Actions.
# Run it locally on a schedule (the index sleeves update every run; the quarterly
# factor sleeve auto-extends — it only pulls a new quarter-end when one has passed).
#
# Suggested cadence — weekly, via cron or launchd, e.g. crontab:
#   0 20 * * 0  cd /path/to/dragon-pulse && ./scripts/paper_lab_refresh.sh >> outputs/paper_lab/refresh.log 2>&1
#
# Then commit the refreshed outputs/paper_lab/*.csv to keep the live record in git.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== paper lab refresh $(date '+%Y-%m-%d %H:%M') ==="
# Focused watchlist = ChiNext-timing (robust survivor) + IVOL (stock-selection survivor) + benchmarks.
# 1) factor sleeve: extend IVOL/factors forward (resumes from per-date cache; adds new quarter-ends)
PYTHONWARNINGS=ignore python3 scripts/xsec_sleeves.py --months 96 2>/dev/null | tail -1
# 2) index sleeves: fetch new trading days + print the combined leaderboard (incl. xsec:ivol)
PYTHONWARNINGS=ignore python3 scripts/paper_lab.py --update 2>/dev/null
