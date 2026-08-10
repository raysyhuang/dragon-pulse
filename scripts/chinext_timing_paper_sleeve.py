#!/usr/bin/env python3
"""Paper sleeve for the one confirmed edge: ChiNext 50/200 trend timing.

Records, daily and append-only, exactly the rule validated in
docs/research/2026-08-10-edge-test-trend-timing.md. It places no orders, sends no
alerts, and touches no selector, cron, or production artifact.

THE RULE (identical to the backtest, deliberately):
    signal[t] = close[t] > SMA50[t] > SMA200[t]
    position held over session t+1 = signal[t]
The signal is computed from a completed session's close and governs the NEXT session,
so the live sleeve and the backtest cannot diverge in their timing convention.

Ledger discipline: append-only, one row per session, idempotent by trade_date. A rerun
never rewrites history; it only adds sessions that are not already recorded. Every row
carries its evidence labels so a row read in isolation cannot be mistaken for execution.

Usage:
    python scripts/chinext_timing_paper_sleeve.py            # update ledger to latest session
    python scripts/chinext_timing_paper_sleeve.py --status   # print current state, no writes
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
LEDGER = ROOT / "outputs" / "paper_lab" / "chinext_timing_paper_ledger.jsonl"
INCEPTION_FILE = ROOT / "outputs" / "paper_lab" / "chinext_timing_paper_inception.txt"
TS_CODE = "399006.SZ"
FAST, SLOW = 50, 200
SIDE_BPS = 5.0
CASH_ANNUAL = 0.018
TRADING_DAYS = 243.0
EVIDENCE_LABEL = "RESEARCH_ONLY_NON_BINDING"
EXECUTION_STATUS = "PAPER_ONLY_NO_ORDERS"
SLEEVE = "chinext_50_200_trend_timing"


def _load_env() -> None:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            m = re.match(r"^\s*([A-Z_]+)\s*=\s*(.*?)\s*$", line)
            if m:
                os.environ.setdefault(m.group(1), m.group(2).strip("\"'"))


def fetch_index() -> pd.DataFrame:
    """Full ChiNext history. Uses the REST endpoint directly; the tushare package is optional."""
    _load_env()
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set")
    frames = []
    for start, end in (("20100101", "20180101"), ("20180101", "20260101"), ("20260101", "20991231")):
        resp = requests.post("http://api.tushare.pro", timeout=60, json={
            "api_name": "index_daily", "token": token,
            "params": {"ts_code": TS_CODE, "start_date": start, "end_date": end},
            "fields": "trade_date,close"}).json()
        if resp.get("code") != 0:
            raise SystemExit(f"tushare error: {resp.get('msg')}")
        data = resp["data"]
        frames.append(pd.DataFrame(data["items"], columns=data["fields"]))
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df["close"] = df["close"].astype(float)
    return df.sort_values("trade_date").drop_duplicates("trade_date").reset_index(drop=True)


def build_rows(df: pd.DataFrame, inception: str | None = None) -> list[dict]:
    """One row per session, with the position that the PRIOR session's signal dictates.

    Rows dated before `inception` are BACKFILLED_FROM_HISTORY — a reconstruction, not a
    forward paper record. Rows from inception onward are FORWARD_PAPER. This repository's
    evidence hierarchy ranks live above replay above reconstruction, so the two must not
    be readable as the same thing.
    """
    c = df["close"]
    sma_f, sma_s = c.rolling(FAST).mean(), c.rolling(SLOW).mean()
    signal = ((c > sma_f) & (sma_f > sma_s))
    position = signal.shift(1)                      # held over the NEXT session
    ret = c.pct_change()
    turn = position.astype(float).diff().abs()
    cash_daily = CASH_ANNUAL / TRADING_DAYS
    net = (position.astype(float) * ret
           + (1 - position.astype(float)) * cash_daily
           - turn * (SIDE_BPS / 10_000.0))

    rows, equity = [], 1.0
    for i in range(len(df)):
        if pd.isna(sma_s.iloc[i]) or pd.isna(position.iloc[i]) or pd.isna(net.iloc[i]):
            continue
        equity *= (1 + float(net.iloc[i]))
        date_str = df["trade_date"].iloc[i].strftime("%Y-%m-%d")
        rows.append({
            "sleeve": SLEEVE,
            "record_origin": ("FORWARD_PAPER" if inception and date_str >= inception
                              else "BACKFILLED_FROM_HISTORY"),
            "trade_date": date_str,
            "close": round(float(c.iloc[i]), 4),
            "sma_fast": round(float(sma_f.iloc[i]), 4),
            "sma_slow": round(float(sma_s.iloc[i]), 4),
            "signal_today": bool(signal.iloc[i]),
            "position_held_today": bool(position.iloc[i]),
            "position_next_session": bool(signal.iloc[i]),
            "traded_today": bool(turn.iloc[i] and turn.iloc[i] > 0),
            "net_return": round(float(net.iloc[i]), 8),
            "paper_equity": round(equity, 8),
            "rule": f"close > SMA{FAST} > SMA{SLOW}; signal[t] governs session t+1",
            "cost_bps_per_side": SIDE_BPS,
            "cash_annual_rate": CASH_ANNUAL,
            "evidence_label": EVIDENCE_LABEL,
            "execution_status": EXECUTION_STATUS,
        })
    return rows


def existing_dates() -> set[str]:
    if not LEDGER.exists():
        return set()
    return {json.loads(l)["trade_date"] for l in LEDGER.read_text().splitlines() if l.strip()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--status", action="store_true", help="print state without writing")
    args = ap.parse_args()

    have_before = existing_dates()
    inception = INCEPTION_FILE.read_text().strip() if INCEPTION_FILE.exists() else None
    rows = build_rows(fetch_index(), inception=inception)
    if not rows:
        raise SystemExit("no rows produced")
    latest = rows[-1]

    print(f"ChiNext 50/200 trend-timing paper sleeve — {EVIDENCE_LABEL} / {EXECUTION_STATUS}")
    print(f"  latest session      {latest['trade_date']}   close {latest['close']:.2f}")
    print(f"  SMA{FAST} {latest['sma_fast']:.2f}   SMA{SLOW} {latest['sma_slow']:.2f}")
    print(f"  signal today        {'BULL (in)' if latest['signal_today'] else 'no signal (cash)'}")
    print(f"  position NEXT sess. {'HOLD ChiNext ETF' if latest['position_next_session'] else 'CASH'}")
    trailing = rows[-243:] if len(rows) >= 243 else rows
    expo = sum(r["position_held_today"] for r in trailing) / len(trailing)
    print(f"  trailing 1y exposure {expo:.0%}   paper equity since {rows[0]['trade_date']}: {latest['paper_equity']:.3f}x")

    if args.status:
        return 0

    have = have_before
    new = [r for r in rows if r["trade_date"] not in have]
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    if inception is None:
        INCEPTION_FILE.parent.mkdir(parents=True, exist_ok=True)
        INCEPTION_FILE.write_text(latest["trade_date"] + "\n")
        print(f"  inception set to {latest['trade_date']} — later rows count as FORWARD_PAPER")
    with LEDGER.open("a", encoding="utf-8") as fh:
        for r in new:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
    print(f"  ledger {LEDGER.relative_to(ROOT)}: +{len(new)} rows (total {len(have) + len(new)})")
    print(f"  generated {datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
