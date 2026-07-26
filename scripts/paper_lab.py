#!/usr/bin/env python3
"""Paper lab — run several candidate strategy sleeves in parallel and score them on
one leaderboard (MAS-style horse race). Decide which pipeline(s) to adopt from
forward + full-cycle evidence, not a single backtest.

Each sleeve is a function(panel, calendar) -> daily strategy return Series
(cost-adjusted). Index/ETF-level sleeves are deterministic and backfilled from
index history; run daily to extend forward. Stock-level sleeves (cross-sectional
selection) plug into the same registry next.

No lookahead: trend/momentum signals from close[t] decide the position HELD on t+1.
Costs charged on every switch. Leaderboard reports full-cycle + trailing-1Y.

Usage:
    TUSHARE_TOKEN=... python scripts/paper_lab.py            # backfill + score
    TUSHARE_TOKEN=... python scripts/paper_lab.py --alert    # + telegram leaderboard
Runs LOCAL (Tushare rejects CI runner IPs) unless index data is cached.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE = PROJECT_ROOT / "outputs" / "paper_lab"
INDICES = {"CSI300": "000300.SH", "CSI500": "000905.SH", "ChiNext": "399006.SZ"}
START = "2015-01-01"
COST_BPS_SIDE = 5.0  # per switch, one side


def _token() -> str:
    tok = os.environ.get("TUSHARE_TOKEN")
    if tok:
        return tok.strip()
    env = PROJECT_ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.strip().startswith("TUSHARE_TOKEN"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("TUSHARE_TOKEN not set")


def _index_daily(code: str, tok: str) -> pd.DataFrame:
    frames = []
    for y in range(2014, 2027):
        body = json.dumps({"api_name": "index_daily", "token": tok,
                           "params": {"ts_code": code, "start_date": f"{y}0101", "end_date": f"{y}1231"},
                           "fields": ""}).encode()
        req = urllib.request.Request("http://api.tushare.pro", data=body, headers={"Content-Type": "application/json"})
        r = json.loads(urllib.request.urlopen(req, timeout=40).read())
        if r.get("code") == 0 and r["data"]["items"]:
            frames.append(pd.DataFrame(r["data"]["items"], columns=r["data"]["fields"]))
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.drop_duplicates("trade_date").sort_values("trade_date").reset_index(drop=True)


def load_panel() -> dict[str, pd.DataFrame]:
    CACHE.mkdir(parents=True, exist_ok=True)
    tok = None
    panel = {}
    for name, code in INDICES.items():
        f = CACHE / f"index_{name}.csv"
        if f.exists():
            df = pd.read_csv(f, parse_dates=["trade_date"])
        else:
            tok = tok or _token()
            df = _index_daily(code, tok)
            df.to_csv(f, index=False)
        df["ret"] = df["close"].pct_change()
        df["sma50"] = df["close"].rolling(50).mean()
        df["sma200"] = df["close"].rolling(200).mean()
        df["mom126"] = df["close"] / df["close"].shift(126) - 1
        panel[name] = df.set_index("trade_date")
    return panel


# ---------------- sleeves: each returns a daily cost-adjusted return Series ----------------

def _switch_cost(pos: pd.Series) -> pd.Series:
    return pos.diff().abs().fillna(0) * (COST_BPS_SIDE / 10000.0)


def sleeve_buyhold(panel, cal, index):
    return panel[index].reindex(cal)["ret"].fillna(0.0)


def sleeve_timed(panel, cal, index, s=50, l=200):
    d = panel[index].reindex(cal)
    bull = (d["close"] > d[f"sma{s}"]) & (d[f"sma{s}"] > d[f"sma{l}"])
    pos = bull.shift(1).fillna(False).astype(float)
    return (pos * d["ret"].fillna(0.0) - _switch_cost(pos)).fillna(0.0)


def sleeve_rotation(panel, cal):
    """Dual momentum: hold the index with highest 126d momentum among those in a
    50>200 uptrend; else cash. Signal from close[t], held t+1."""
    names = list(INDICES)
    aligned = {n: panel[n].reindex(cal) for n in names}
    pick = pd.Series(index=cal, dtype=object)
    for t in cal:
        cands = [(n, aligned[n].loc[t, "mom126"]) for n in names
                 if aligned[n].loc[t, "close"] > aligned[n].loc[t, "sma200"]
                 and not pd.isna(aligned[n].loc[t, "mom126"])]
        pick.loc[t] = max(cands, key=lambda x: x[1])[0] if cands else "CASH"
    held = pick.shift(1).fillna("CASH")
    ret = pd.Series(0.0, index=cal)
    for t in cal:
        h = held.loc[t]
        if h != "CASH":
            r = aligned[h].loc[t, "ret"]
            ret.loc[t] = 0.0 if pd.isna(r) else r
    switch = (held != held.shift(1)).astype(float).fillna(0) * (COST_BPS_SIDE / 10000.0)
    return (ret - switch).fillna(0.0)


SLEEVES = {
    "CSI300 buy&hold": lambda p, c: sleeve_buyhold(p, c, "CSI300"),
    "ChiNext buy&hold": lambda p, c: sleeve_buyhold(p, c, "ChiNext"),
    "CSI300 timed 50/200": lambda p, c: sleeve_timed(p, c, "CSI300"),
    "ChiNext timed 50/200": lambda p, c: sleeve_timed(p, c, "ChiNext"),
    "Index rotation (dual-mom)": sleeve_rotation,
}


def _metrics(ret: pd.Series) -> dict:
    r = ret.values
    eq = np.cumprod(1 + r)
    yrs = len(r) / 252.0
    dd = (1 - eq / np.maximum.accumulate(eq)).max()
    return {
        "CAGR%": round((eq[-1] ** (1 / yrs) - 1) * 100, 1) if yrs > 0 and eq[-1] > 0 else -100,
        "Sharpe": round(r.mean() / r.std() * np.sqrt(252), 2) if r.std() > 0 else 0,
        "maxDD%": round(dd * 100, 1),
        "total%": round((eq[-1] - 1) * 100, 1),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--alert", action="store_true")
    args = ap.parse_args()

    panel = load_panel()
    cal = panel["CSI300"].loc[START:].index
    rows = []
    for name, fn in SLEEVES.items():
        ret = fn(panel, cal).reindex(cal).fillna(0.0)
        full = _metrics(ret)
        last1y = _metrics(ret.iloc[-252:])
        rows.append({"sleeve": name, **{f"{k}": v for k, v in full.items()},
                     "1Y_CAGR%": last1y["CAGR%"], "1Y_Sharpe": last1y["Sharpe"], "1Y_maxDD%": last1y["maxDD%"]})
    board = pd.DataFrame(rows).sort_values("Sharpe", ascending=False)
    CACHE.mkdir(parents=True, exist_ok=True)
    board.to_csv(CACHE / "leaderboard.csv", index=False)
    (CACHE / "leaderboard.json").write_text(board.to_json(orient="records", indent=2))
    print(f"PAPER LAB leaderboard — {cal.min().date()}..{cal.max().date()} (cost {COST_BPS_SIDE:.0f}bps/switch)\n")
    pd.set_option("display.width", 200)
    print(board.to_string(index=False))
    print("\nStock-picking sleeves (top-1/top-2) tracked separately in outputs/top1_paper/summary.json.")

    if args.alert:
        try:
            sys.path.insert(0, str(PROJECT_ROOT))
            from src.core.alerts import AlertConfig, AlertManager
            cfg = AlertConfig(enabled=True, channels=["telegram"])
            if cfg.telegram_bot_token and cfg.telegram_chat_id:
                lines = ["<b>🧪 Paper Lab 排行榜 (full-cycle)</b>", "PAPER — 全周期风险调整对比", ""]
                for _, r in board.iterrows():
                    lines.append(f"<b>{r['sleeve']}</b>: Sharpe {r['Sharpe']} | CAGR {r['CAGR%']}% | 回撤 {r['maxDD%']}%")
                AlertManager(cfg).send_alert(title="Paper Lab leaderboard", message="\n".join(lines),
                                             data={}, priority="low")
        except Exception as e:
            print("alert skipped:", e)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
