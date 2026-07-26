#!/usr/bin/env python3
"""Robustness sweep for the cross-sectional factor sleeves (harden low-vol before trusting).

A real edge must survive parameter variation. Sweeps basket size, base-universe size,
rebalance frequency, cost, and vol-lookback — reading the cached per-date cross-sections
(outputs/paper_lab/xsec_cache), so NO new downloads. Verdict = does low-vol's Sharpe stay
above the CSI300 baseline (~0.21) across the grid, or is the edge param-specific?

Usage: python scripts/xsec_robustness.py
"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE = PROJECT_ROOT / "outputs" / "paper_lab" / "xsec_cache"


def load():
    files = sorted(glob.glob(str(CACHE / "*.csv")))
    closes, basics = {}, {}
    for f in files:
        d = Path(f).stem
        m = pd.read_csv(f, dtype={"ts_code": str}).set_index("ts_code")
        for c in m.columns:
            m[c] = pd.to_numeric(m[c], errors="coerce")
        closes[d] = m["close"]
        basics[d] = m
    px = pd.DataFrame(closes).sort_index(axis=1)
    csi = pd.read_csv(PROJECT_ROOT / "outputs" / "paper_lab" / "index_CSI300.csv",
                      parse_dates=["trade_date"]).set_index("trade_date")["close"]
    return px, basics, csi


PX, BASICS, CSI = load()
DATES = list(PX.columns)


def _z(s):
    s = pd.to_numeric(s, errors="coerce")
    return (s - s.mean()) / s.std()


def factor_series(i, base_n, vol_m, mom_m, base_lo=0):
    d = DATES[i]
    b = BASICS[d].dropna(subset=["circ_mv"])
    base = b.sort_values("circ_mv", ascending=False).iloc[base_lo:base_n].index  # base_lo>0 = mid-cap
    p_now = PX[d].reindex(base)
    mom = p_now / PX[DATES[max(0, i - mom_m)]].reindex(base) - 1
    rev = p_now / PX[DATES[max(0, i - 1)]].reindex(base) - 1   # 1-step return (short-term reversal)
    wdates = [DATES[j] for j in range(max(0, i - vol_m), i + 1)]
    window = PX[wdates].reindex(base)
    sret = window.pct_change(axis=1).iloc[:, 1:]              # base x (window-1) stock returns
    vol = sret.std(axis=1)
    # IVOL: residual vol vs CSI300 = total vol * sqrt(1 - corr(stock,market)^2)
    mkt = pd.Series([CSI[CSI.index <= pd.Timestamp(x)].iloc[-1] for x in wdates]).pct_change().values[1:]
    mkt = pd.Series(mkt, index=sret.columns)
    corr = sret.apply(lambda r: r.corr(mkt), axis=1)
    ivol = vol * np.sqrt((1 - corr ** 2).clip(lower=0))
    bb = b.reindex(base)
    value = _z(-bb["pb"]) + _z(-bb["pe_ttm"]) + _z(bb["dv_ttm"])
    f = pd.DataFrame({"vol": vol, "ivol": ivol, "mom": mom, "rev": rev, "value": value})
    f["multifactor"] = _z(f["mom"]) + _z(-f["vol"]) + _z(f["value"])
    return f.dropna(subset=["vol"])


def run(factor, asc, base_n=300, hold_frac=0.10, rebal=1, cost_bps=30, vol_m=6, mom_m=6, base_lo=0):
    start = max(vol_m, mom_m)
    idx = list(range(start, len(DATES) - 1, rebal))
    eq, prior, curve = 1.0, set(), []
    for k in range(len(idx) - 1):
        i, j = idx[k], idx[k + 1]
        f = factor_series(i, base_n, vol_m, mom_m, base_lo)
        if f.empty:
            continue
        n = max(5, int(len(f) * hold_frac))
        held = set(f[factor].sort_values(ascending=asc).head(n).index)
        turn = 1.0 - len(held & prior) / len(held) if held else 0.0
        fwd = (PX[DATES[j]].reindex(list(held)) / PX[DATES[i]].reindex(list(held)) - 1)
        r = fwd.dropna().mean()
        eq *= (1 + (0.0 if pd.isna(r) else r) - turn * cost_bps / 10000.0)
        curve.append(eq)
        prior = held
    c = np.array(curve)
    rr = np.diff(c) / c[:-1]
    ppy = 12 / rebal
    yrs = len(rr) / ppy
    dd = (1 - c / np.maximum.accumulate(c)).max()
    return {"CAGR%": round((c[-1] ** (1 / yrs) - 1) * 100, 1) if yrs > 0 and c[-1] > 0 else -100,
            "Sharpe": round(rr.mean() / rr.std() * np.sqrt(ppy), 2) if rr.std() > 0 else 0,
            "maxDD%": round(dd * 100, 1)}


def csi_baseline():
    # sample CSI300 at the same monthly rebalance dates as the sleeves (apples-to-apples)
    lvl = [CSI[CSI.index <= pd.Timestamp(d)].iloc[-1] for d in DATES]
    r = pd.Series(lvl).pct_change().dropna()
    return round(r.mean() / r.std() * np.sqrt(12), 2)


def main():
    print(f"cache: {len(DATES)} monthly cross-sections {DATES[0]}..{DATES[-1]}")
    print(f"CSI300 daily Sharpe baseline = {csi_baseline()}  (low-vol must beat this to be real)\n")

    print("== low_vol: basket size (rows) x base universe (cols) — Sharpe [monthly, 30bps, vol6m] ==")
    bases = [200, 300, 500, 800]
    print("hold_frac \\ base_n   " + "".join(f"{b:>8d}" for b in bases))
    for hf in (0.05, 0.10, 0.20, 0.30):
        print(f"  top {int(hf*100):>2d}%           " +
              "".join(f"{run('vol', True, base_n=b, hold_frac=hf)['Sharpe']:>8.2f}" for b in bases))

    print("\n== low_vol: rebalance (rows) x cost (cols) — Sharpe [base300, decile, vol6m] ==")
    costs = [30, 50, 80]
    print("rebal \\ cost_bps     " + "".join(f"{c:>8d}" for c in costs))
    for rb, lbl in ((1, "monthly"), (2, "2-month"), (3, "quarterly")):
        print(f"  {lbl:<12s}      " + "".join(f"{run('vol', True, rebal=rb, cost_bps=c)['Sharpe']:>8.2f}" for c in costs))

    print("\n== vol lookback sensitivity — Sharpe [base300, decile, monthly, 30bps] ==")
    print("  " + "  ".join(f"vol{m}m={run('vol', True, vol_m=m)['Sharpe']:.2f}" for m in (3, 6, 12)))

    print("\n== full detail at default (base300, decile, monthly, 30bps, vol6m) ==")
    for name, (col, asc) in {"low_vol": ("vol", True), "value": ("value", False),
                             "multifactor": ("multifactor", False), "momentum": ("mom", False)}.items():
        print(f"  {name:12s} {run(col, asc)}")


if __name__ == "__main__":
    main()
