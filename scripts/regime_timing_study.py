#!/usr/bin/env python3
"""Robustness study: is index regime-timing a real, tradeable edge? (companion to paper_lab)

Rule: hold the index when in a bull trend (close>SMA_s and SMA_s>SMA_l), else cash.
Guards against self-deception:
  - NO LOOKAHEAD: signal from close[t] decides the position HELD on t+1 (shift).
  - PARAM ROBUSTNESS: a grid of (short,long); an edge needing one magic pair is overfit.
  - MULTI-INDEX: CSI300 / CSI500 / ChiNext must generalize.
  - LONG HISTORY: 2015-2026 (2015 crash, 2018 bear, 2021-22 bear, 2024 bottom).
  - COSTS: round-trip cost on every switch (base 5bps/side + 15bps stress).

Finding (2026-07-26, memory project_benchmark_vs_etf): timing is mainly a DRAWDOWN
overlay (cuts maxDD ~1/3-1/2 on every index); return+Sharpe improvement only on
trendier indices with SLOW MAs. Best = ChiNext 50/200 (Sharpe 0.40->0.71, survives
costs). Fast MAs whipsaw and die on costs.

Usage: TUSHARE_TOKEN=... python scripts/regime_timing_study.py   (LOCAL only)
"""
from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INDICES = [("CSI300", "000300.SH"), ("CSI500", "000905.SH"), ("ChiNext", "399006.SZ")]
GRID = [(10, 30), (20, 50), (20, 60), (50, 120), (50, 200)]
START = "2015-01-01"


def _token() -> str:
    tok = os.environ.get("TUSHARE_TOKEN")
    if tok:
        return tok.strip()
    for line in (PROJECT_ROOT / ".env").read_text().splitlines():
        if line.strip().startswith("TUSHARE_TOKEN"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("TUSHARE_TOKEN not set")


TOKEN = _token()


def load_index(code):
    frames = []
    for y in range(2014, 2027):
        body = json.dumps({"api_name": "index_daily", "token": TOKEN,
                           "params": {"ts_code": code, "start_date": f"{y}0101", "end_date": f"{y}1231"},
                           "fields": ""}).encode()
        req = urllib.request.Request("https://api.tushare.pro", data=body, headers={"Content-Type": "application/json"})
        r = json.loads(urllib.request.urlopen(req, timeout=40).read())
        if r.get("code") == 0 and r["data"]["items"]:
            frames.append(pd.DataFrame(r["data"]["items"], columns=r["data"]["fields"]))
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.drop_duplicates("trade_date").sort_values("trade_date").reset_index(drop=True)


def metrics(ret, pos=None):
    ret = np.nan_to_num(ret)
    eq = np.cumprod(1 + ret)
    yrs = len(ret) / 252.0
    dd = (1 - eq / np.maximum.accumulate(eq)).max()
    out = dict(cagr=(eq[-1] ** (1 / yrs) - 1) * 100 if yrs > 0 and eq[-1] > 0 else -100,
               sharpe=ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else 0,
               maxdd=dd * 100, total=(eq[-1] - 1) * 100)
    if pos is not None:
        out["expo"] = pos.mean() * 100
        out["switch"] = int((pos.diff().abs() > 0).sum())
    return out


def timed(df, s, l, cost_bps_side):
    d = df.copy()
    d["ret"] = d["close"].pct_change()
    d["sma_s"] = d["close"].rolling(s).mean()
    d["sma_l"] = d["close"].rolling(l).mean()
    bull = (d["close"] > d["sma_s"]) & (d["sma_s"] > d["sma_l"])
    d["pos"] = bull.shift(1).fillna(False).astype(float)
    d["stratret"] = d["pos"] * d["ret"] - d["pos"].diff().abs().fillna(0) * (cost_bps_side / 10000.0)
    return d.dropna(subset=["sma_l"]).reset_index(drop=True)


def main() -> int:
    for name, code in INDICES:
        raw = load_index(code)
        sub0 = raw[raw["trade_date"] >= pd.Timestamp(START)]
        bh = metrics(sub0["close"].pct_change().values)
        print(f"\n===== {name} ({code})  {sub0['trade_date'].min().date()}..{sub0['trade_date'].max().date()} =====")
        print(f"  BUY&HOLD           CAGR {bh['cagr']:+6.1f}%  Sharpe {bh['sharpe']:+.2f}  maxDD {bh['maxdd']:4.1f}%")
        for s, l in GRID:
            d = timed(raw, s, l, 5)
            d = d[d["trade_date"] >= pd.Timestamp(START)]
            m = metrics(d["stratret"].values, d["pos"])
            ds = timed(raw, s, l, 15)
            ds = ds[ds["trade_date"] >= pd.Timestamp(START)]
            ms = metrics(ds["stratret"].values)
            print(f"  timed {s:>2}/{l:<3} CAGR {m['cagr']:+6.1f}%  Sharpe {m['sharpe']:+.2f}  maxDD {m['maxdd']:4.1f}%  "
                  f"expo {m['expo']:2.0f}%  switch {m['switch']:3d}  | 15bps CAGR {ms['cagr']:+5.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
