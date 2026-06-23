"""Entry-policy sweep on a PINNED pick set, holding the exit FIXED (validated).

The validated exit is the production fixed target/stop/hold. The open question is
the ENTRY: ~21% of picks gap above max_entry and are skipped (no-chase) — and those
tend to be the strongest momentum names. This sweeps entry policies against the same
pinned picks + same fixed exit, so differences are attributable to the entry rule.

Entry policies:
  nochase           fill at T+1 open iff open <= max_entry            (live behavior)
  chase1/2/3        fill at T+1 open iff open <= max_entry*(1+1/2/3%) (controlled chase)
  limit_touch       fill at open if <= max_entry, else at max_entry if T+1 low touches

Reports fill rate alongside P&L/Sharpe — recovering cancelled picks only helps if the
recovered trades are not net-losers.

Usage:
    python scripts/entry_sweep.py --picks-csv outputs/backtest/.../pinned.csv \
        --windows 5y:2021-03-14:2026-03-13 3y:2023-03-14:2026-03-13 1y:2025-03-14:2026-03-13
"""

from __future__ import annotations

import argparse
from argparse import Namespace
from datetime import timedelta
from pathlib import Path

import pandas as pd

from src.features.performance.backtest import _to_dt
from src.core.cn_data import download_daily_range
from exit_logic_backtest import simulate_pick
from portfolio_sim_v2 import simulate, _calendar_from_data

# (label, entry_mode, chase_band%) — all use the fixed exit.
ENTRY_CONFIGS = [
    ("nochase", "nochase", 0.0),
    ("chase1", "chase_band", 1.0),
    ("chase2", "chase_band", 2.0),
    ("chase3", "chase_band", 3.0),
    ("limit_touch", "limit_touch", 0.0),
]


def _params(entry_mode: str, band: float) -> Namespace:
    return Namespace(
        mode="fixed", entry_mode=entry_mode, chase_band=band,
        t1=3.0, t2=5.0, t1_frac=1 / 3, t2_frac=1 / 3,
        breakeven_at=3.0, trail_atr=1.5, max_hold=10, laggard_day=2, laggard_thresh=0.0,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--picks-csv", required=True)
    ap.add_argument("--windows", nargs="+", required=True)
    ap.add_argument("--position-pct", type=float, default=20.0)
    ap.add_argument("--max-concurrent", type=int, default=5)
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--out-dir", default="outputs/backtest/entry_sweep")
    args = ap.parse_args()

    out_root = Path(args.out_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    picks = pd.read_csv(args.picks_csv)
    picks["ticker"] = picks["ticker"].astype(str).str.upper()
    picks["baseline_date"] = picks["baseline_date"].astype(str)

    tickers = sorted(picks["ticker"].dropna().unique().tolist())
    bdates = sorted(picks["baseline_date"].tolist())
    dl_start = (_to_dt(bdates[0]) - timedelta(days=45)).strftime("%Y-%m-%d")
    dl_end = (_to_dt(bdates[-1]) + timedelta(days=40)).strftime("%Y-%m-%d")
    print(f"Downloading {len(tickers)} tickers ONCE: {dl_start} -> {dl_end}", flush=True)
    data_map, _ = download_daily_range(tickers=tickers, start=dl_start, end=dl_end)
    calendar = _calendar_from_data(data_map)
    print(f"Shared data_map: {len(data_map)} tickers, calendar {len(calendar)} days", flush=True)

    rows = []
    for win in args.windows:
        name, start, end = win.split(":")
        wp = picks[(picks["baseline_date"] >= start) & (picks["baseline_date"] <= end)]
        n_signals = len(wp)
        for label, em, band in ENTRY_CONFIGS:
            p = _params(em, band)
            detail = pd.DataFrame([
                simulate_pick(rec, data_map.get(rec["ticker"], pd.DataFrame()), p)
                for rec in wp.to_dict(orient="records")
            ])
            run_dir = out_root / f"{name}_{label}"
            run_dir.mkdir(parents=True, exist_ok=True)
            detail.to_csv(run_dir / "detail.csv", index=False)

            m = detail[detail["status"] == "matured"].copy()
            fill_pct = round(len(m) / max(n_signals, 1) * 100, 0)
            sim = {}
            sim_df = m.dropna(subset=["entry_price", "exit_price", "entry_date", "exit_date"])
            if len(sim_df):
                sim_in = run_dir / "sim_input.csv"
                sim_df.to_csv(sim_in, index=False)
                sim = simulate(str(sim_in), data_map, calendar,
                               position_pct=args.position_pct,
                               max_concurrent=args.max_concurrent,
                               initial_capital=args.capital)
            rows.append({
                "window": name, "entry": label,
                "signals": n_signals, "filled": len(m),
                "fill_pct": fill_pct,
                "avg_pnl": round(float(m["pnl_pct"].mean()), 2) if len(m) else None,
                "win_pct": round(float((m["pnl_pct"] > 0).mean()) * 100, 0) if len(m) else None,
                "ret_pct": sim.get("total_return_pct"),
                "ann_pct": sim.get("annualized_return_pct"),
                "sharpe": sim.get("sharpe_ratio"),
                "maxdd_pct": sim.get("max_drawdown_pct"),
            })

    table = pd.DataFrame(rows)
    table.to_csv(out_root / "entry_sweep_summary.csv", index=False)
    print("\n" + "=" * 100)
    print(f"ENTRY-POLICY SWEEP (pinned picks, FIXED exit, pos={args.position_pct}% concurrent={args.max_concurrent})")
    print("=" * 100)
    print(table.to_string(index=False))
    print(f"\nWrote {out_root / 'entry_sweep_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
