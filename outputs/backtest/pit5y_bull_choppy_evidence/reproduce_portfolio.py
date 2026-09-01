#!/usr/bin/env python3
"""Reproduce the frozen-snapshot portfolio ledger for this evidence bundle."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--detail", required=True)
    ap.add_argument("--snapshot-dir", required=True)
    ap.add_argument("--calendar", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--position-pct", type=float, default=20.0)
    ap.add_argument("--max-concurrent", type=int, default=5)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(root))
    from scripts.backtest_1yr import _read_price_snapshot
    from scripts.portfolio_sim_v2 import simulate

    detail = Path(args.detail).resolve()
    snapshots = Path(args.snapshot_dir).resolve()
    calendar_path = Path(args.calendar).resolve()
    out = Path(args.out_dir).resolve()

    trades = pd.read_csv(detail)
    tickers = sorted(trades.loc[trades["pnl_pct"].notna(), "ticker"].astype(str).unique())
    data_map = {ticker: _read_price_snapshot(snapshots, ticker) for ticker in tickers}
    missing = [ticker for ticker, frame in data_map.items() if frame.empty]
    if missing:
        raise SystemExit(f"missing snapshots: {missing[:10]} ({len(missing)} total)")

    receipt = json.loads(calendar_path.read_text(encoding="utf-8"))
    fields = receipt["data"]["fields"]
    cal_idx, open_idx = fields.index("cal_date"), fields.index("is_open")
    calendar = sorted(
        datetime.strptime(row[cal_idx], "%Y%m%d").date()
        for row in receipt["data"]["items"]
        if int(row[open_idx]) == 1
    )

    result = simulate(
        str(detail), data_map, calendar,
        position_pct=args.position_pct, max_concurrent=args.max_concurrent,
    )
    result["detail_csv"] = detail.name
    curve = result.pop("equity_curve")
    out.mkdir(parents=True, exist_ok=True)
    curve_path = out / "portfolio_equity_curve.csv"
    summary_path = out / "portfolio_sim_v2_summary.json"
    curve_path.write_text(pd.DataFrame(curve).to_csv(index=False), encoding="utf-8")
    payload = {
        "status": "RESEARCH_ONLY_NON_BINDING",
        "position_pct": args.position_pct,
        "max_concurrent": args.max_concurrent,
        "detail_file": detail.name,
        "detail_sha256": hashlib.sha256(detail.read_bytes()).hexdigest(),
        "calendar_file": calendar_path.name,
        "calendar_sha256": hashlib.sha256(calendar_path.read_bytes()).hexdigest(),
        "snapshot_count_loaded": len(data_map),
        "result": result,
        "equity_curve_file": curve_path.name,
        "equity_curve_sha256": hashlib.sha256(curve_path.read_bytes()).hexdigest(),
    }
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
