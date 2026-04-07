#!/bin/bash
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Wait for the 1yr v2 tight backtest (PID 42654) to finish
echo "[$(date)] Waiting for 1yr v2 tight backtest (PID 42654) to finish..."
while kill -0 42654 2>/dev/null; do
    sleep 60
done
echo "[$(date)] 1yr v2 tight backtest completed."

# Log the 1yr result
if [ -f outputs/backtest/backtest_summary_1yr_v2_tight.json ]; then
    echo "[$(date)] 1yr v2 tight result:"
    cat outputs/backtest/backtest_summary_1yr_v2_tight.json
else
    echo "[$(date)] WARNING: 1yr v2 tight summary not found"
fi

# Run 3yr backtest
echo "[$(date)] Starting 3yr backtest..."
python scripts/backtest_1yr.py \
    --start 2023-03-14 \
    --end 2026-03-13 \
    --out-dir outputs/backtest \
    --label 3yr_v2_tight \
    2>&1 | tee outputs/backtest/backtest_3yr_v2_tight.log

echo "[$(date)] 3yr backtest completed."
