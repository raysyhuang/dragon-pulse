#!/usr/bin/env python3
"""Screen CYQ chip-distribution features: predictive power x orthogonality + OOS sign stability.

A feature is worth pursuing only if it (a) predicts forward pnl, (b) is orthogonal
to the existing score, and (c) keeps its sign OUT-OF-SAMPLE. On alpha_rs_pullback
picks (2026-07-26) cyq_profit_ratio was the only one whose sign held OOS, but too
weak/period-inconsistent to build a filter — see memory project_cyq_and_myhhub_stock.

Usage:
    python scripts/cyq_screen.py <train_features.csv> [oos1.csv oos2.csv ...]

Build the feature CSVs first with scripts/cyq_build.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

CYQ = ["cyq_profit_ratio", "cyq_price_to_avgcost", "cyq_concentration", "cyq_price_position"]


def _predictive_orthogonality(path: str) -> None:
    d = pd.read_csv(path).dropna(subset=["pnl_pct"])
    d["win"] = (d["pnl_pct"] > 0).astype(float)
    print(f"\n=== predictive x orthogonality: {Path(path).name} "
          f"(N={len(d)} base_win={d['win'].mean():.3f} base_avg={d['pnl_pct'].mean():+.3f}) ===")
    print(f"{'feature':22s}{'sp_pnl':>9s}{'sp_score':>10s}{'Q1_win':>9s}{'Q4_win':>9s}{'Q1_avg':>9s}{'Q4_avg':>9s}")
    for c in CYQ:
        s = d[[c, "pnl_pct", "score", "win"]].dropna()
        if len(s) < 40:
            print(f"{c:22s}  N={len(s)} skip"); continue
        sp = s[c].corr(s["pnl_pct"], method="spearman")
        so = s[c].corr(s["score"], method="spearman")
        q = s[c].quantile([.25, .75])
        top, bot = s[s[c] >= q[.75]], s[s[c] <= q[.25]]
        print(f"{c:22s}{sp:>+9.3f}{so:>+10.3f}{bot['win'].mean():>9.3f}{top['win'].mean():>9.3f}"
              f"{bot['pnl_pct'].mean():>+9.2f}{top['pnl_pct'].mean():>+9.2f}")


def _oos_sign(paths: list[str]) -> None:
    print("\n=== OOS sign stability: corr(feature, pnl) by period [neg=holds; flip=dead] ===")
    print(f"{'feature':22s}" + "".join(f"{Path(p).stem:>20s}" for p in paths))
    for c in CYQ:
        line = f"{c:22s}"
        for p in paths:
            d = pd.read_csv(p).dropna(subset=["pnl_pct", c])
            line += f"{d[c].corr(d['pnl_pct'], method='spearman'):>+13.3f}(N{len(d)})".rjust(20)
        print(line)


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: cyq_screen.py <train.csv> [oos1.csv ...]")
    paths = sys.argv[1:]
    for p in paths:
        _predictive_orthogonality(p)
    if len(paths) > 1:
        _oos_sign(paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
