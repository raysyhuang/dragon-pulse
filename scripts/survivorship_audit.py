"""Survivorship-bias audit of a pinned pick set.

The backtest universe is the CURRENT top-1000 by market cap applied to all history.
A pick is "survivorship-contaminated" if its ticker was NOT in the top-1000 as of its
own pick date (it only became eligible because it grew into today's universe). A live
point-in-time system would never have scanned it.

This fetches the point-in-time top-N (Tushare daily_basic, monthly rebalance), tags each
pinned pick clean/contaminated, then re-runs the FIXED-exit portfolio sim on (a) all picks
and (b) clean-only picks — quantifying how much of the reported return is hindsight.

Note: this corrects one direction (picks that should not have been eligible). It does NOT
recover then-large/now-small names the scanner never saw — that needs a full point-in-time
re-scan. So clean-only is a conservative upper bound on the *real* edge.

Usage:
    python scripts/survivorship_audit.py --picks-csv outputs/backtest/.../pinned.csv \
        --windows 5y:2021-03-14:2026-03-13 3y:2023-03-14:2026-03-13 1y:2025-03-14:2026-03-13
"""

from __future__ import annotations

import argparse
import os
import time
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.features.performance.backtest import _to_dt
from src.core.cn_data import download_daily_range
from exit_logic_backtest import simulate_pick
from portfolio_sim_v2 import simulate, _calendar_from_data

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def _top_n_asof(pro, n: int, yyyymmdd: str, cache: dict) -> set[str]:
    """Top-n tickers by total market cap as of (or just before) yyyymmdd."""
    if yyyymmdd in cache:
        return cache[yyyymmdd]
    base = datetime.strptime(yyyymmdd, "%Y%m%d")
    for off in range(8):  # step back to the nearest trading day
        d = (base - timedelta(days=off)).strftime("%Y%m%d")
        df = pro.daily_basic(trade_date=d, fields="ts_code,total_mv")
        if df is not None and not df.empty:
            df = df.dropna(subset=["total_mv"]).sort_values("total_mv", ascending=False)
            top = set(df["ts_code"].astype(str).str.upper().head(n).tolist())
            cache[yyyymmdd] = top
            return top
    cache[yyyymmdd] = set()
    return set()


def _fixed_params() -> Namespace:
    return Namespace(mode="fixed", entry_mode="nochase", chase_band=0.0,
                     t1=3.0, t2=5.0, t1_frac=1 / 3, t2_frac=1 / 3,
                     breakeven_at=3.0, trail_atr=1.5, max_hold=10,
                     laggard_day=2, laggard_thresh=0.0)


def _perf(picks_df, data_map, calendar, args, run_dir: Path) -> dict:
    p = _fixed_params()
    detail = pd.DataFrame([
        simulate_pick(rec, data_map.get(rec["ticker"], pd.DataFrame()), p)
        for rec in picks_df.to_dict(orient="records")
    ])
    m = detail[detail["status"] == "matured"].copy()
    sim = {}
    sim_df = m.dropna(subset=["entry_price", "exit_price", "entry_date", "exit_date"])
    if len(sim_df):
        run_dir.mkdir(parents=True, exist_ok=True)
        sim_in = run_dir / "sim_input.csv"
        sim_df.to_csv(sim_in, index=False)
        sim = simulate(str(sim_in), data_map, calendar,
                       position_pct=args.position_pct, max_concurrent=args.max_concurrent,
                       initial_capital=args.capital)
    return {
        "matured": len(m),
        "avg_pnl": round(float(m["pnl_pct"].mean()), 2) if len(m) else None,
        "win_pct": round(float((m["pnl_pct"] > 0).mean()) * 100, 0) if len(m) else None,
        "ret_pct": sim.get("total_return_pct"),
        "ann_pct": sim.get("annualized_return_pct"),
        "sharpe": sim.get("sharpe_ratio"),
        "maxdd_pct": sim.get("max_drawdown_pct"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--picks-csv", required=True)
    ap.add_argument("--windows", nargs="+", required=True)
    ap.add_argument("--top-n", type=int, default=1000)
    ap.add_argument("--position-pct", type=float, default=20.0)
    ap.add_argument("--max-concurrent", type=int, default=5)
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--out-dir", default="outputs/backtest/survivorship_audit")
    args = ap.parse_args()

    out_root = Path(args.out_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    picks = pd.read_csv(args.picks_csv)
    picks["ticker"] = picks["ticker"].astype(str).str.upper()
    picks["baseline_date"] = picks["baseline_date"].astype(str)
    picks["ym"] = picks["baseline_date"].str.replace("-", "").str[:6]

    # ---- Point-in-time universe membership (monthly rebalance). ----
    import tushare as ts
    pro = ts.pro_api(token=os.environ.get("TUSHARE_TOKEN", ""))
    cache: dict[str, set[str]] = {}
    months = sorted(picks["ym"].unique())
    print(f"Fetching point-in-time top-{args.top_n} for {len(months)} months...", flush=True)
    pit: dict[str, set[str]] = {}
    for ym in months:
        pit[ym] = _top_n_asof(pro, args.top_n, ym + "15", cache)  # mid-month trade date
        time.sleep(0.25)

    picks["in_pit_universe"] = [
        row["ticker"] in pit.get(row["ym"], set()) for _, row in picks.iterrows()
    ]
    picks.to_csv(out_root / "picks_tagged.csv", index=False)

    # Contamination summary by year.
    picks["year"] = picks["baseline_date"].str[:4]
    print("\n=== SURVIVORSHIP CONTAMINATION (picks NOT in point-in-time top-N) ===")
    by_year = picks.groupby("year")["in_pit_universe"].agg(["size", "sum"])
    by_year["contaminated"] = by_year["size"] - by_year["sum"]
    by_year["contam_pct"] = (by_year["contaminated"] / by_year["size"] * 100).round(0)
    print(by_year.rename(columns={"size": "picks", "sum": "clean"})[
        ["picks", "clean", "contaminated", "contam_pct"]].to_string())
    overall = 100 * (1 - picks["in_pit_universe"].mean())
    print(f"\nOverall contamination: {overall:.0f}% of {len(picks)} picks")

    # ---- Performance: all picks vs clean-only, per window. ----
    tickers = sorted(picks["ticker"].dropna().unique().tolist())
    bdates = sorted(picks["baseline_date"].tolist())
    dl_start = (_to_dt(bdates[0]) - timedelta(days=45)).strftime("%Y-%m-%d")
    dl_end = (_to_dt(bdates[-1]) + timedelta(days=40)).strftime("%Y-%m-%d")
    print(f"\nDownloading {len(tickers)} tickers ONCE: {dl_start} -> {dl_end}", flush=True)
    data_map, _ = download_daily_range(tickers=tickers, start=dl_start, end=dl_end)
    calendar = _calendar_from_data(data_map)

    rows = []
    for win in args.windows:
        name, start, end = win.split(":")
        wp = picks[(picks["baseline_date"] >= start) & (picks["baseline_date"] <= end)]
        clean = wp[wp["in_pit_universe"]]
        rows.append({"window": name, "set": "all", "picks": len(wp),
                     **_perf(wp, data_map, calendar, args, out_root / f"{name}_all")})
        rows.append({"window": name, "set": "clean_only", "picks": len(clean),
                     **_perf(clean, data_map, calendar, args, out_root / f"{name}_clean")})

    table = pd.DataFrame(rows)
    table.to_csv(out_root / "survivorship_perf.csv", index=False)
    print("\n" + "=" * 100)
    print(f"PERFORMANCE: all vs survivorship-clean (FIXED exit, pos={args.position_pct}%)")
    print("=" * 100)
    print(table.to_string(index=False))
    print(f"\nWrote {out_root / 'survivorship_perf.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
