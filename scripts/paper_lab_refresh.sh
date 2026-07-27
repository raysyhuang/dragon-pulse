#!/usr/bin/env bash
# Forward-refresh the paper lab so the surviving sleeves accumulate live evidence.
#
# Run locally: these jobs are long-running and the data cache lives here.
# (NOTE: Tushare DOES work from GitHub runners — the earlier "CI IPs are rejected"
# claim was a mis-diagnosis caused by an accidentally deleted TUSHARE_TOKEN secret.)
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
# 1) factor sleeve: extend IVOL/factors forward (resumes from per-date cache; adds new quarter-ends).
# --months 150 = FULL available history. Do not shorten: a shorter window silently reverts to the
# flattering sub-period (IVOL reads 0.31 on 8y vs its honest full-history 0.40 ~= CSI300 tie).
PYTHONWARNINGS=ignore python3 scripts/xsec_sleeves.py --months 150 2>/dev/null | tail -1
# 2) index sleeves: fetch new trading days + print the combined leaderboard (incl. xsec:ivol)
PYTHONWARNINGS=ignore python3 scripts/paper_lab.py --update 2>/dev/null
