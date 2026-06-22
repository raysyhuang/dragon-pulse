"""Orchestrate the exit-logic validation against a PINNED pick set.

Reproducible by construction: every exit mode and window is evaluated against the
exact same picks (produced once by `backtest_1yr.py --dump-picks`), so differences
are attributable to the exit logic alone — not to scanner non-determinism.

Price data for the union of all pinned tickers is downloaded ONCE and shared across
every (window, mode) replay and portfolio sim — no redundant per-replay downloads.

For each (window, mode):
  1. replay the exit logic           -> exit_v2_detail.csv  (per-trade P&L)
  2. compounded portfolio sim        -> sim metrics (ret / sharpe / drawdown)
  3. collect per-trade + portfolio stats into one comparison table.

Usage:
    python scripts/exit_validation.py --picks-csv outputs/backtest/pinned_5y.csv \
        --windows 5y:2021-03-14:2026-03-13 3y:2023-03-14:2026-03-13 \
        --modes fixed runner runner_laggard scaleout \
        --position-pct 20 --max-concurrent 5 --out-dir outputs/backtest/exit_validation
"""

from __future__ import annotations

import argparse
from argparse import Namespace
from datetime import timedelta
from pathlib import Path

import pandas as pd

from src.features.performance.backtest import _to_dt
from src.core.cn_data import download_daily_range

# Same-directory imports (scripts/ is on sys.path when run as a script).
from exit_logic_backtest import simulate_pick
from portfolio_sim_v2 import simulate, _calendar_from_data

REPO = Path(__file__).resolve().parents[1]


def _replay_params(mode: str, args) -> Namespace:
    return Namespace(
        mode=mode, t1=3.0, t2=5.0, t1_frac=1 / 3, t2_frac=1 / 3,
        breakeven_at=args.breakeven_at, trail_atr=args.trail_atr,
        max_hold=args.runner_max_hold, laggard_day=2, laggard_thresh=0.0,
    )


def replay_metrics(detail: pd.DataFrame) -> dict:
    m = detail[detail["status"] == "matured"].copy()
    win = m[m["pnl_pct"] > 0]["pnl_pct"]
    loss = m[m["pnl_pct"] <= 0]["pnl_pct"]
    payoff = (win.mean() / abs(loss.mean())) if len(win) and len(loss) else float("nan")
    return {
        "picks": int(len(detail)),
        "matured": int(len(m)),
        "cancelled": int((detail["status"] == "cancelled").sum()),
        "avg_pnl": round(float(m["pnl_pct"].mean()), 2) if len(m) else None,
        "win_pct": round(float((m["pnl_pct"] > 0).mean()) * 100, 0) if len(m) else None,
        "payoff": round(payoff, 2) if payoff == payoff else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--picks-csv", required=True)
    ap.add_argument("--windows", nargs="+", required=True,
                    help="name:start:end tokens, e.g. 5y:2021-03-14:2026-03-13")
    ap.add_argument("--modes", nargs="+",
                    default=["fixed", "runner", "runner_laggard", "scaleout"])
    ap.add_argument("--position-pct", type=float, default=20.0)
    ap.add_argument("--max-concurrent", type=int, default=5)
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--trail-atr", type=float, default=1.5)
    ap.add_argument("--breakeven-at", type=float, default=3.0)
    ap.add_argument("--runner-max-hold", type=int, default=10)
    ap.add_argument("--out-dir", default="outputs/backtest/exit_validation")
    args = ap.parse_args()

    out_root = Path(args.out_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    picks = pd.read_csv(args.picks_csv)
    picks["ticker"] = picks["ticker"].astype(str).str.upper()
    picks["baseline_date"] = picks["baseline_date"].astype(str)

    # ---- Download price data ONCE for the union of tickers over the full span. ----
    tickers = sorted(picks["ticker"].dropna().unique().tolist())
    bdates = sorted(picks["baseline_date"].tolist())
    dl_start = (_to_dt(bdates[0]) - timedelta(days=45)).strftime("%Y-%m-%d")
    dl_end = (_to_dt(bdates[-1]) + timedelta(days=max(40, args.runner_max_hold * 3))).strftime("%Y-%m-%d")
    print(f"Downloading {len(tickers)} tickers ONCE: {dl_start} -> {dl_end}", flush=True)
    data_map, _ = download_daily_range(tickers=tickers, start=dl_start, end=dl_end)
    calendar = _calendar_from_data(data_map)
    print(f"Shared data_map: {len(data_map)} tickers, calendar {len(calendar)} days", flush=True)

    rows = []
    for win in args.windows:
        name, start, end = win.split(":")
        win_picks = picks[(picks["baseline_date"] >= start) & (picks["baseline_date"] <= end)]
        for mode in args.modes:
            tag = f"{name}_{mode}"
            print(f"\n=== {tag} ({start} -> {end}) {len(win_picks)} picks ===", flush=True)
            p = _replay_params(mode, args)
            detail = pd.DataFrame([
                simulate_pick(rec, data_map.get(rec["ticker"], pd.DataFrame()), p)
                for rec in win_picks.to_dict(orient="records")
            ])
            run_dir = out_root / tag
            run_dir.mkdir(parents=True, exist_ok=True)
            detail.to_csv(run_dir / "exit_v2_detail.csv", index=False)
            rep = replay_metrics(detail)

            sim = {}
            sim_df = detail[detail["status"] == "matured"].dropna(
                subset=["entry_price", "exit_price", "entry_date", "exit_date"])
            if len(sim_df):
                sim_in = run_dir / "sim_input.csv"
                sim_df.to_csv(sim_in, index=False)
                sim = simulate(str(sim_in), data_map, calendar,
                               position_pct=args.position_pct,
                               max_concurrent=args.max_concurrent,
                               initial_capital=args.capital)

            rows.append({
                "window": name, "mode": mode, **rep,
                "ret_pct": sim.get("total_return_pct"),
                "ann_pct": sim.get("annualized_return_pct"),
                "sharpe": sim.get("sharpe_ratio"),
                "maxdd_pct": sim.get("max_drawdown_pct"),
                "trades_exec": sim.get("trades_executed"),
            })

    table = pd.DataFrame(rows)
    table.to_csv(out_root / "exit_validation_summary.csv", index=False)

    print("\n" + "=" * 100)
    print(f"EXIT-LOGIC VALIDATION (pinned picks, pos={args.position_pct}% concurrent={args.max_concurrent})")
    print("=" * 100)
    cols = ["window", "mode", "matured", "avg_pnl", "win_pct", "payoff",
            "ret_pct", "ann_pct", "sharpe", "maxdd_pct"]
    print(table[cols].to_string(index=False))
    print(f"\nWrote {out_root / 'exit_validation_summary.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
