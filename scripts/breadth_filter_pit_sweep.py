#!/usr/bin/env python3
"""Stock-level test of breadth as a gate FILTER, on point-in-time picks.

The index-overlay study (scripts/breadth_regime_study.py) rejected breadth as a
way to OPEN the gate early and found the opposite variant promising:

    bull = close>SMA_s and SMA_s>SMA_l and breadth >= B

Because that rule is a strict SUBSET of the production gate, it needs no second
backtest. Every trade it would take is a trade the baseline PIT run already
took; the filter only removes trades whose signal-day breadth fell short. So one
baseline run supports the whole threshold sweep, and the comparison stays inside
a single set of picks — no re-scan non-determinism (see project memory:
backtest_1yr picks vary run-to-run on threaded downloads).

Removing trades frees capital, which changes what the rest of the book can hold,
so each arm is re-simulated compounded through portfolio_sim_v2 rather than
compared as a bag of per-trade returns (project memory: post-hoc trade filtering
is diagnostic only; promotion decisions use the compounded sim).

No lookahead: breadth on signal day D is known at D's close and entry is D+1,
the same convention the overlay study uses.

Usage:
    python scripts/breadth_filter_pit_sweep.py \\
        --daily  outputs/backtest/breadth_filter/backtest_daily_pit5y_baseline.csv \\
        --detail outputs/backtest/breadth_filter/backtest_detail_pit5y_baseline.csv \\
        --thresholds 0.45 0.50 0.55 0.60 0.65 0.70
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def attach_breadth(detail: pd.DataFrame, breadth: pd.Series) -> pd.DataFrame:
    """Join each trade to the breadth measured on its signal day.

    A traded day always has measured breadth, so a gap means the join is wrong.
    Dropping those rows instead would silently flatter every filtered arm, which
    is exactly the error this study exists to avoid.
    """
    out = detail.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out["_breadth"] = out["date"].map(breadth)
    missing = int(out["_breadth"].isna().sum())
    if missing:
        raise ValueError(f"{missing} trades have no breadth for their signal day — join is broken")
    return out


def build_cuts(scanned: pd.Series, thresholds, quantiles) -> list[tuple[str, float]]:
    """Absolute cutoffs plus cutoffs read off this run's own breadth distribution.

    Percentiles are taken over every scanned session with measured breadth, not
    just traded ones — the gate is a claim about the market, not about the days
    the engine happened to fire.
    """
    cuts = [(f"abs{int(round(t * 100))}", float(t)) for t in thresholds]
    cuts += [(f"q{int(round(q * 100))}", float(scanned.quantile(q))) for q in quantiles]
    return cuts


def permutation_test(kept: pd.Series, dropped: pd.Series, iters: int = 20000, seed: int = 0) -> dict:
    """Would this split of per-trade returns arise by chance from a random split?

    The compounded sim reports whichever arm won; it cannot say whether the gap
    is signal. The filter's whole claim is that trades on high-breadth days are
    better trades, so test that claim directly on the trades themselves. With a
    few hundred trades and a near-zero baseline expectancy, the honest answer is
    often "cannot tell" — which is a result, not a failure.
    """
    import numpy as np

    k = kept.dropna().to_numpy(dtype=float)
    d = dropped.dropna().to_numpy(dtype=float)
    if len(k) < 2 or len(d) < 2:
        return {"n_kept": int(len(k)), "n_dropped": int(len(d)), "p_value": None}
    observed = k.mean() - d.mean()
    pool = np.concatenate([k, d])
    rng = np.random.default_rng(seed)
    hits = 0
    for _ in range(iters):
        rng.shuffle(pool)
        if abs(pool[: len(k)].mean() - pool[len(k) :].mean()) >= abs(observed):
            hits += 1
    return {
        "n_kept": int(len(k)),
        "n_dropped": int(len(d)),
        "mean_kept_pct": round(float(k.mean()), 3),
        "mean_dropped_pct": round(float(d.mean()), 3),
        "diff_pct": round(float(observed), 3),
        "p_value": round((hits + 1) / (iters + 1), 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", required=True)
    ap.add_argument("--detail", required=True)
    ap.add_argument("--thresholds", nargs="+", type=float, default=[],
                    help="Absolute breadth cutoffs. Prefer --quantiles: the overlay study "
                         "measured all-market breadth while the scanner measures the top 1000 "
                         "by market cap, and the two run at different levels (June 2026: 0.6-0.9 "
                         "vs 0.42-0.54), so an absolute cutoff does not carry across.")
    ap.add_argument("--quantiles", nargs="+", type=float,
                    default=[0.60, 0.70, 0.75, 0.78, 0.80, 0.85, 0.90],
                    help="Cutoffs taken from this run's own breadth distribution. The overlay "
                         "study's 0.70-0.80 plateau sits at the 78th-87th percentile of "
                         "all-market breadth, so percentile is the transferable quantity.")
    ap.add_argument("--position-pct", nargs="+", type=float, default=[20])
    ap.add_argument("--max-concurrent", type=int, default=5)
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    daily = pd.read_csv(args.daily)
    detail = pd.read_csv(args.detail)
    if "breadth_above_sma20" not in daily.columns:
        raise SystemExit(
            "daily CSV has no breadth_above_sma20 column — rerun the backtest with the "
            "patched scripts/backtest_1yr.py"
        )

    breadth = (
        daily.assign(date=lambda d: pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d"))
        .set_index("date")["breadth_above_sma20"]
    )
    try:
        detail = attach_breadth(detail, breadth)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    out_dir = Path(args.detail).parent
    base = Path(args.detail).stem
    files = [args.detail]
    counts = {"baseline": len(detail)}
    print(f"baseline trades: {len(detail)}  "
          f"breadth on signal days: min {detail['_breadth'].min():.3f} "
          f"median {detail['_breadth'].median():.3f} max {detail['_breadth'].max():.3f}")

    # Percentiles come from every scanned session with measured breadth, not just
    # traded ones — the gate is a statement about the market, not about the days
    # the engine happened to fire.
    cuts = build_cuts(breadth.dropna(), args.thresholds, args.quantiles)

    perms: dict = {}
    for label, thr in cuts:
        mask = detail["_breadth"] >= thr
        kept = detail[mask].drop(columns=["_breadth"])
        if kept.empty:
            print(f"  {label} (breadth>={thr:.3f}): 0 trades — skipped")
            continue
        path = out_dir / f"{base}__{label}.csv"
        kept.to_csv(path, index=False)
        files.append(str(path))
        counts[f"{label}:breadth>={thr:.3f}"] = len(kept)
        pt = permutation_test(detail.loc[mask, "pnl_pct"], detail.loc[~mask, "pnl_pct"])
        perms[label] = {"threshold": thr, **pt}
        pv = "n/a" if pt.get("p_value") is None else f"{pt['p_value']:.3f}"
        print(f"  {label} (breadth>={thr:.3f}): {len(kept):4d} trades kept "
              f"({len(kept) / max(len(detail), 1) * 100:.0f}% of baseline)  "
              f"kept {pt.get('mean_kept_pct', float('nan')):+.2f}% vs dropped "
              f"{pt.get('mean_dropped_pct', float('nan')):+.2f}% per trade, p={pv}")

    cmd = [args.python, str(PROJECT_ROOT / "scripts" / "portfolio_sim_v2.py"), *files,
           "--position-pct", *[str(p) for p in args.position_pct],
           "--max-concurrent", str(args.max_concurrent)]
    print("\nrunning compounded portfolio sim on all arms...\n", flush=True)
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if proc.returncode != 0:
        return proc.returncode

    summary_path = Path(files[0]).with_name("portfolio_sim_v2_summary.json")
    if not summary_path.exists():
        return 0
    summary = json.loads(summary_path.read_text())

    print("\n" + "=" * 84)
    print("BREADTH FILTER SWEEP — compounded, point-in-time picks")
    print("=" * 84)
    print(f"{'arm':<22}{'trades':>8}{'ret%':>9}{'ann%':>8}{'maxDD%':>9}{'Sharpe':>9}{'util%':>8}")
    for fpath, rows in summary["files"].items():
        label = "baseline" if Path(fpath).stem == base else Path(fpath).stem.split("__")[-1]
        for alloc, r in rows.items():
            if "error" in r:
                print(f"{label:<22}  ERROR {r['error']}")
                continue
            print(f"{label + ' @' + alloc:<22}{r['trades_executed']:>8}"
                  f"{r['total_return_pct']:>9.1f}{r['annualized_return_pct']:>8.1f}"
                  f"{r['max_drawdown_pct']:>9.1f}{r['sharpe_ratio']:>9.2f}"
                  f"{r['utilization_pct']:>8.1f}")

    verdict = out_dir / "breadth_filter_sweep.json"
    verdict.write_text(json.dumps(
        {"trade_counts": counts, "permutation_tests": perms, "sim": summary},
        indent=2, default=str))
    print(f"\nwrote {verdict}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
