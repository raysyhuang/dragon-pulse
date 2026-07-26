#!/usr/bin/env python3
"""Package the one robust edge: ChiNext 50/200 trend-timing, drawdown-tamed via vol-targeting.

Signal: hold ChiNext when close>SMA50>SMA200 (bull), else cash. No lookahead — signal
from close[t] sets the position HELD on t+1. Raw timing beats buy&hold but still carries
a ~42% max drawdown; vol-targeting scales exposure by target_vol/realized_vol (capped at
1x, no leverage) to cut that. Tradable via a ChiNext ETF (e.g. 159915), costs modeled.

Usage: TUSHARE_TOKEN=... python scripts/chinext_timing_strategy.py   (reads cached index)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE = PROJECT_ROOT / "outputs" / "paper_lab" / "index_ChiNext.csv"
START = "2015-01-01"
SIDE_BPS = 5.0   # ETF one-way cost (commission + spread; no stamp duty on ETF)


def _metrics(ret, pos=None):
    ret = np.nan_to_num(ret.values if hasattr(ret, "values") else ret)
    eq = np.cumprod(1 + ret)
    yrs = len(ret) / 252.0
    dd = (1 - eq / np.maximum.accumulate(eq)).max()
    out = {"CAGR%": round((eq[-1] ** (1 / yrs) - 1) * 100, 1) if yrs > 0 and eq[-1] > 0 else -100,
           "Sharpe": round(ret.mean() / ret.std() * np.sqrt(252), 2) if ret.std() > 0 else 0,
           "maxDD%": round(dd * 100, 1), "total%": round((eq[-1] - 1) * 100, 0)}
    if pos is not None:
        out["avg_expo%"] = round(np.nanmean(pos) * 100, 0)
    return out


def main():
    d = pd.read_csv(CACHE, parse_dates=["trade_date"]).sort_values("trade_date").reset_index(drop=True)
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    # compute signals on the FULL history (2014+ warmup) BEFORE slicing to START —
    # otherwise the 200-day SMA is invalid through 2015 and the strategy wrongly sits out.
    d["ret"] = d["close"].pct_change()
    d["sma50"] = d["close"].rolling(50).mean()
    d["sma200"] = d["close"].rolling(200).mean()
    bull = ((d["close"] > d["sma50"]) & (d["sma50"] > d["sma200"])).shift(1).fillna(False).astype(float)
    rvol = (d["ret"].rolling(20).std() * np.sqrt(252)).shift(1)   # realized ann. vol, lagged
    mask = d["trade_date"] >= pd.Timestamp(START)
    d, bull, rvol = d[mask].reset_index(drop=True), bull[mask].reset_index(drop=True), rvol[mask].reset_index(drop=True)

    def run(pos):
        cost = pos.diff().abs().fillna(0) * (SIDE_BPS / 10000.0)
        return (pos * d["ret"] - cost).fillna(0.0), pos

    print(f"ChiNext timing package — {d['trade_date'].min().date()}..{d['trade_date'].max().date()}\n")
    rows = {}
    r, p = _metrics(d["ret"]), None
    rows["buy & hold"] = _metrics(d["ret"], pd.Series(1.0, index=d.index))
    rows["timed (raw)"] = _metrics(*run(bull))
    for tv in (0.15, 0.20, 0.25):
        pos = (bull * (tv / rvol).clip(upper=1.0)).fillna(0.0)
        rows[f"timed vol-target {int(tv*100)}%"] = _metrics(*run(pos))

    board = pd.DataFrame(rows).T
    print(board.to_string())
    print("\nExecution: signal on close, trade next open; ETF 159915 (or any ChiNext ETF);")
    print("vol-target = min(1x, target/realized-20d-vol) exposure; ~5bps/side cost modeled.")

    # per-year consistency of the chosen package (vol-target 20%)
    pos20 = (bull * (0.20 / rvol).clip(upper=1.0)).fillna(0.0)
    sr, _ = run(pos20)
    dd = d.assign(sr=sr, yr=d["trade_date"].dt.year)
    print("\nvol-target 20% — per-year return %:")
    yearly = dd.groupby("yr")["sr"].apply(lambda s: round(((1 + s).prod() - 1) * 100, 1))
    print(yearly.to_string())


if __name__ == "__main__":
    main()
