#!/usr/bin/env python3
"""Point-in-time chip-distribution (CYQ / 筹码分布) features for backtest picks.

Triangular chip model (after github.com/myhhub/stock instock/core/kline/cyq.py,
Apache-2.0): each day decay existing chips by the day's turnover, then deposit
today's volume as a triangle peaked at the day's average price. As-of only: uses
raw daily OHLC + turnover_rate with trade_date <= scan date T; current price P =
raw close at T (same scale as the chips, so features are internally consistent
regardless of the detail CSV's price adjustment).

Screened 2026-07-26 on alpha_rs_pullback PIT picks (see memory
project_cyq_and_myhhub_stock): cyq_profit_ratio is the only orthogonal signal
whose corr sign held out-of-sample, but too weak/period-inconsistent to build a
filter around. Kept as research tooling + a candidate meta-labeling input.

Features per pick:
  cyq_profit_ratio     fraction of chips with cost <= P (获利比例)
  cyq_price_to_avgcost P/avg_cost - 1
  cyq_concentration    (p95-p5)/(p95+p5)  (smaller = more concentrated)
  cyq_price_position   (P-p5)/(p95-p5)    (where P sits in the 90% cost band)

Usage:
    TUSHARE_TOKEN=... python scripts/cyq_build.py <detail.csv> <out_features.csv>

Run locally (long-running). NOTE: Tushare also works from CI — the earlier
"CI IPs are rejected" claim was a mis-diagnosis (deleted TUSHARE_TOKEN secret).
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOOKBACK = 150
FACTOR = 150


def _load_token() -> str:
    tok = os.environ.get("TUSHARE_TOKEN")
    if tok:
        return tok.strip()
    env = PROJECT_ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.strip().startswith("TUSHARE_TOKEN"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("TUSHARE_TOKEN not set (env or .env)")


TOKEN = _load_token()


def _call(api: str, params: dict) -> pd.DataFrame:
    body = json.dumps({"api_name": api, "token": TOKEN, "params": params, "fields": ""}).encode()
    req = urllib.request.Request("https://api.tushare.pro", data=body,
                                 headers={"Content-Type": "application/json"})
    for _ in range(3):
        try:
            r = json.loads(urllib.request.urlopen(req, timeout=40).read())
            if r.get("code") == 0:
                d = r["data"]
                return pd.DataFrame(d["items"], columns=d["fields"])
            time.sleep(1.0)
        except Exception:
            time.sleep(1.2)
    return pd.DataFrame()


def cyq_features(win: pd.DataFrame, price: float) -> dict | None:
    """Triangular chip distribution over `win` (needs open/close/high/low/turnover_rate)."""
    o, c, h, l, tv = (win[k].values for k in ("open", "close", "high", "low", "turnover_rate"))
    minp, maxp = float(np.nanmin(l)), float(np.nanmax(h))
    if not (maxp > minp):
        return None
    acc = max(0.01, (maxp - minp) / (FACTOR - 1))
    prices = minp + acc * np.arange(FACTOR)
    chips = np.zeros(FACTOR)
    for oo, cc, hh, ll, tt in zip(o, c, h, l, tv):
        if np.isnan(tt):
            continue
        tr = min(1.0, max(0.0, float(tt) / 100.0))
        if tr <= 0:
            continue
        avg = (oo + cc + hh + ll) / 4.0
        chips *= (1 - tr)
        if hh <= ll:
            idx = min(max(int(round((avg - minp) / acc)), 0), FACTOR - 1)
            chips[idx] += tr
            continue
        gh = 2.0 / (hh - ll)
        lo = max(int(np.ceil((ll - minp) / acc)), 0)
        hi = min(int((hh - minp) / acc), FACTOR - 1)
        for j in range(lo, hi + 1):
            cur = minp + acc * j
            w = (cur - ll) / (avg - ll) if (cur <= avg and avg > ll) else \
                ((hh - cur) / (hh - avg) if (cur > avg and hh > avg) else 1.0)
            chips[j] += w * gh * acc * tr
    s = chips.sum()
    if s <= 0:
        return None
    chips /= s
    avg_cost = float((prices * chips).sum())
    cum = np.cumsum(chips)
    p5 = float(prices[np.searchsorted(cum, 0.05)])
    p95 = float(prices[min(np.searchsorted(cum, 0.95), FACTOR - 1)])
    return {
        "cyq_profit_ratio": float(chips[prices <= price].sum()),
        "cyq_price_to_avgcost": price / avg_cost - 1 if avg_cost else None,
        "cyq_concentration": (p95 - p5) / (p95 + p5) if (p95 + p5) else None,
        "cyq_price_position": (price - p5) / (p95 - p5) if (p95 > p5) else None,
    }


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: cyq_build.py <detail.csv> <out_features.csv>")
    detail, out = Path(sys.argv[1]), Path(sys.argv[2])
    d = pd.read_csv(detail)
    d["ticker"] = d["ticker"].astype(str).str.upper()
    d["Tdate"] = pd.to_datetime(d["date"])
    tickers = sorted(d["ticker"].unique())
    start = (d["Tdate"].min() - pd.Timedelta(days=330)).strftime("%Y%m%d")
    end = d["Tdate"].max().strftime("%Y%m%d")
    print(f"{len(d)} picks, {len(tickers)} tickers, {start}..{end}", flush=True)

    hist: dict[str, pd.DataFrame] = {}
    for i, tc in enumerate(tickers):
        dd = _call("daily", {"ts_code": tc, "start_date": start, "end_date": end})
        db = _call("daily_basic", {"ts_code": tc, "start_date": start, "end_date": end})
        if dd.empty or db.empty:
            continue
        for col in ("open", "high", "low", "close"):
            dd[col] = pd.to_numeric(dd[col], errors="coerce")
        db["turnover_rate"] = pd.to_numeric(db["turnover_rate"], errors="coerce")
        hist[tc] = dd.merge(db[["trade_date", "turnover_rate"]], on="trade_date", how="left").sort_values("trade_date")
        if i % 25 == 0:
            print(f"  {i}/{len(tickers)}", flush=True)
        time.sleep(0.12)
    print("pulled", flush=True)

    rows = []
    for _, p in d.iterrows():
        r = {"ticker": p["ticker"], "date": p["date"], "pnl_pct": p.get("pnl_pct"),
             "hit_target": p.get("hit_target"), "score": p.get("score")}
        h = hist.get(p["ticker"])
        if h is not None:
            win = h[h["trade_date"] <= p["Tdate"].strftime("%Y%m%d")].tail(LOOKBACK)
            if len(win) >= 30:
                feat = cyq_features(win, float(win.iloc[-1]["close"]))
                if feat:
                    r.update(feat)
        rows.append(r)

    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"saved {out} rows={len(rows)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
