#!/usr/bin/env python3
"""Is the breadth filter better than trading less at random?

The sweep showed every breadth cutoff beating the baseline on Sharpe and
drawdown. That is not evidence on its own: the filter removes 60-90% of trades,
and cutting exposure mechanically cuts drawdown whatever the selection rule. A
random subsample of the same size gets the same benefit for free.

So the filter's claim is not "it beats the baseline" — it is "it beats a coin
flip of the same size". This draws that null distribution: for each cutoff, many
random subsamples of exactly the same trade count, each run through the same
compounded simulator, giving a percentile for where the breadth arm actually
lands. Anything inside the bulk of the null is trading less, not trading better.

Usage:
    python scripts/breadth_filter_null_test.py \\
        --detail outputs/backtest/breadth_filter/backtest_detail_pit5y_baseline.csv \\
        --daily  outputs/backtest/breadth_filter/backtest_daily_pit5y_breadthcal.csv \\
        --draws 300
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--detail", required=True)
    ap.add_argument("--daily", required=True)
    ap.add_argument("--quantiles", nargs="+", type=float,
                    default=[0.60, 0.70, 0.75, 0.78, 0.80, 0.85, 0.90])
    ap.add_argument("--draws", type=int, default=300)
    ap.add_argument("--position-pct", type=float, default=20.0)
    ap.add_argument("--max-concurrent", type=int, default=5)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--config", default="config/default.yaml")
    args = ap.parse_args()

    psim = _load("portfolio_sim_v2")
    sweep = _load("breadth_filter_pit_sweep")

    daily = pd.read_csv(args.daily)
    breadth = (
        daily.assign(date=lambda d: pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d"))
        .set_index("date")["breadth_above_sma20"]
    )
    detail = sweep.attach_breadth(pd.read_csv(args.detail), breadth)
    detail = detail[detail["pnl_pct"].notna()].copy()

    dates = pd.to_datetime(detail["date"]).dt.date
    data_map = psim._download_price_data(
        sorted(detail["ticker"].astype(str).unique()), dates.min(), dates.max(), args.config
    )
    calendar = psim._calendar_from_data(data_map)

    tmp = Path(tempfile.mkdtemp(prefix="breadth_null_"))

    def run(frame: pd.DataFrame) -> dict:
        path = tmp / "arm.csv"
        frame.drop(columns=["_breadth"], errors="ignore").to_csv(path, index=False)
        return psim.simulate(str(path), data_map, calendar,
                             position_pct=args.position_pct,
                             max_concurrent=args.max_concurrent)

    base = run(detail)
    print(f"baseline: {len(detail)} trades  Sharpe {base['sharpe_ratio']:+.2f}  "
          f"maxDD {base['max_drawdown_pct']:.1f}%  ret {base['total_return_pct']:+.1f}%\n")

    rng = np.random.default_rng(args.seed)
    out: dict = {"baseline": base, "cuts": {}}
    print(f"{'cut':<6}{'n':>5}{'Sharpe':>9}{'null p50':>10}{'pctile':>8}   "
          f"{'maxDD':>7}{'null p50':>10}{'pctile':>8}")
    for q in args.quantiles:
        thr = float(breadth.dropna().quantile(q))
        arm = detail[detail["_breadth"] >= thr]
        n = len(arm)
        if n < 5:
            continue
        real = run(arm)

        null_sharpe, null_dd = [], []
        for _ in range(args.draws):
            idx = rng.choice(len(detail), size=n, replace=False)
            r = run(detail.iloc[idx])
            null_sharpe.append(r["sharpe_ratio"])
            null_dd.append(r["max_drawdown_pct"])
        ns, nd = np.array(null_sharpe), np.array(null_dd)

        # Sharpe: higher is better. Drawdown: lower is better.
        p_sharpe = float((ns >= real["sharpe_ratio"]).mean())
        p_dd = float((nd <= real["max_drawdown_pct"]).mean())
        out["cuts"][f"q{int(q * 100)}"] = {
            "threshold": thr, "n": n, "real": real,
            "null_sharpe_median": float(np.median(ns)),
            "null_maxdd_median": float(np.median(nd)),
            "p_sharpe_beaten_by_random": p_sharpe,
            "p_maxdd_beaten_by_random": p_dd,
        }
        print(f"q{int(q * 100):<5}{n:>5}{real['sharpe_ratio']:>9.2f}{np.median(ns):>10.2f}"
              f"{(1 - p_sharpe) * 100:>7.0f}%   {real['max_drawdown_pct']:>7.1f}"
              f"{np.median(nd):>10.1f}{(1 - p_dd) * 100:>7.0f}%")

    print("\npctile = share of equal-sized random subsamples the breadth arm beat.")
    print("Near 50% means the cutoff is doing nothing a coin flip would not do.")

    dest = Path(args.detail).with_name("breadth_filter_null_test.json")
    dest.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwrote {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
