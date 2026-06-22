"""Orchestrate the exit-logic validation against a PINNED pick set.

Reproducible by construction: every exit mode and window is evaluated against the
exact same picks (produced once by `backtest_1yr.py --dump-picks`), so differences
are attributable to the exit logic alone — not to scanner non-determinism.

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
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> None:
    env_cmd = [sys.executable, *cmd]
    print(f"  $ {' '.join(env_cmd)}", flush=True)
    subprocess.run(env_cmd, cwd=REPO, check=True)


def replay_metrics(detail_csv: Path) -> dict:
    df = pd.read_csv(detail_csv)
    m = df[df["status"] == "matured"].copy()
    win = m[m["pnl_pct"] > 0]["pnl_pct"]
    loss = m[m["pnl_pct"] <= 0]["pnl_pct"]
    payoff = (win.mean() / abs(loss.mean())) if len(win) and len(loss) else float("nan")
    return {
        "picks": int(len(df)),
        "matured": int(len(m)),
        "cancelled": int((df["status"] == "cancelled").sum()),
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
    rows = []

    for win in args.windows:
        name, start, end = win.split(":")
        for mode in args.modes:
            tag = f"{name}_{mode}"
            print(f"\n=== {tag} ({start} -> {end}) ===", flush=True)
            run_dir = out_root / tag
            _run([
                "scripts/exit_logic_backtest.py", "--picks-csv", args.picks_csv,
                "--start", start, "--end", end, "--mode", mode,
                "--trail-atr", str(args.trail_atr), "--breakeven-at", str(args.breakeven_at),
                "--max-hold", str(args.runner_max_hold),
                "--out-dir", str(run_dir),
            ])
            detail = run_dir / "exit_v2_detail.csv"
            rep = replay_metrics(detail)

            # Build sim input (matured rows with valid trade fields) and run the sim.
            d = pd.read_csv(detail)
            d = d[d["status"] == "matured"].dropna(
                subset=["entry_price", "exit_price", "entry_date", "exit_date"])
            sim_in = run_dir / "sim_input.csv"
            d.to_csv(sim_in, index=False)
            sim = {}
            if len(d):
                _run([
                    "scripts/portfolio_sim_v2.py", str(sim_in),
                    "--position-pct", str(args.position_pct),
                    "--max-concurrent", str(args.max_concurrent),
                    "--capital", str(args.capital),
                ])
                summ = json.loads((run_dir / "portfolio_sim_v2_summary.json").read_text())
                sim = list(summ["files"][str(sim_in)].values())[0]

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
