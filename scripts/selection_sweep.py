"""Selection sweep on a PINNED pick set — fixed exit + no-chase held constant.

MAS Sniper's most robust finding was that SELECTION beats signal: one-pick/day and
risk-aware ranking (avoid wide-stop names) consistently beat raw top-N. DP currently
takes <=2/day with a saturated score (66/68 >=90). This tests selection rules on the
same pinned picks + same validated exit/entry, so differences are pure selection.

Each rule, per signal day: optionally drop picks whose stop distance exceeds a risk
cap, then rank and keep the top `max_per_day`.

  stop_dist_pct = (planned_entry - stop_loss) / planned_entry * 100   (= ~1.1*ATR%)

Rank keys:
  score     raw score desc (current behaviour)
  lowrisk   smallest stop_dist_pct first (risk-aware)

Usage:
    python scripts/selection_sweep.py --picks-csv .../pinned.csv \
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

# (label, max_per_day, rank_key, risk_cap_pct or None)
SELECTION_CONFIGS = [
    ("baseline_all",      2, "score",   None),   # reproduces validated fixed numbers
    ("top1_score",        1, "score",   None),
    ("top1_lowrisk",      1, "lowrisk", None),
    ("riskcap10_all",     2, "score",   10.0),
    ("riskcap10_top1sc",  1, "score",   10.0),
    ("riskcap10_top1lr",  1, "lowrisk", 10.0),
]


def _fixed_params() -> Namespace:
    return Namespace(mode="fixed", entry_mode="nochase", chase_band=0.0,
                     t1=3.0, t2=5.0, t1_frac=1 / 3, t2_frac=1 / 3,
                     breakeven_at=3.0, trail_atr=1.5, max_hold=10,
                     laggard_day=2, laggard_thresh=0.0)


def select(picks: pd.DataFrame, max_per_day: int, rank_key: str, risk_cap) -> pd.DataFrame:
    df = picks.copy()
    df["stop_dist_pct"] = (df["planned_entry_price"] - df["stop_loss"]) / df["planned_entry_price"] * 100.0
    if risk_cap is not None:
        df = df[df["stop_dist_pct"] <= risk_cap]
    asc = rank_key == "lowrisk"
    sort_col = "stop_dist_pct" if rank_key == "lowrisk" else "score"
    df = df.sort_values(["baseline_date", sort_col], ascending=[True, asc])
    return df.groupby("baseline_date", group_keys=False).head(max_per_day)


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
        "selected": len(picks_df), "matured": len(m),
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
    ap.add_argument("--position-pct", type=float, default=20.0)
    ap.add_argument("--max-concurrent", type=int, default=5)
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--out-dir", default="outputs/backtest/selection_sweep")
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

    rows = []
    for win in args.windows:
        name, start, end = win.split(":")
        wp = picks[(picks["baseline_date"] >= start) & (picks["baseline_date"] <= end)]
        for label, mpd, rank, cap in SELECTION_CONFIGS:
            sel = select(wp, mpd, rank, cap)
            rows.append({"window": name, "rule": label, "signals": len(wp),
                         **_perf(sel, data_map, calendar, args, out_root / f"{name}_{label}")})

    table = pd.DataFrame(rows)
    table.to_csv(out_root / "selection_sweep_summary.csv", index=False)
    print("\n" + "=" * 104)
    print(f"SELECTION SWEEP (pinned picks, FIXED exit + no-chase, pos={args.position_pct}% concurrent={args.max_concurrent})")
    print("=" * 104)
    print(table.to_string(index=False))
    print(f"\nWrote {out_root / 'selection_sweep_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
