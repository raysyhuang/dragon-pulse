#!/usr/bin/env python3
"""Cross-sectional stock-alpha sleeves for the paper lab (MAS-style horse race).

Long top-decile of a tradable base universe (top-300 by circ mkt cap), equal-weight,
monthly rebalance, benchmarked vs CSI300. Four sleeves:
  momentum    top decile by 120-day return
  low_vol     bottom decile by return volatility (low-vol anomaly)
  value       top decile by z(-PB)+z(-PE)+z(dividend yield)
  multifactor top decile by z(momentum)+z(-vol)+z(value)

Point-in-time & no survivorship: uses per-date cross-sections (one `daily` /
`daily_basic` call returns all stocks trading that day, as-of). Weekly-sampled
price matrix keeps the pull light. Writes per-sleeve equity to outputs/paper_lab/
so paper_lab.py includes them on the leaderboard.

Usage:
    TUSHARE_TOKEN=... python scripts/xsec_sleeves.py --months 12   # seed PIT history + forward
Run locally (long-running; the per-date cache lives here). NOTE: Tushare also works
from CI — the earlier "CI IPs are rejected" claim was a mis-diagnosis.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE = PROJECT_ROOT / "outputs" / "paper_lab"
BASE_N = 300          # tradable base universe by circ mkt cap
HOLD_FRAC = 0.10      # top decile
MOM_WEEKS = 6         # months of momentum lookback (monthly grid)
VOL_WEEKS = 6         # months for volatility (monthly grid)
IVOL_WEEKS = 12       # months for idiosyncratic-vol / market-corr (needs a longer, stabler window)
REBAL_MONTHS = 3      # quarterly rebalance — robustness sweep: quarterly >> monthly, lower cost


def _token() -> str:
    tok = os.environ.get("TUSHARE_TOKEN")
    if tok:
        return tok.strip()
    for line in (PROJECT_ROOT / ".env").read_text().splitlines():
        if line.strip().startswith("TUSHARE_TOKEN"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("TUSHARE_TOKEN not set")


TOKEN = _token()


def _call(api, params):
    body = json.dumps({"api_name": api, "token": TOKEN, "params": params, "fields": ""}).encode()
    req = urllib.request.Request("https://api.tushare.pro", data=body, headers={"Content-Type": "application/json"})
    for _ in range(3):
        try:
            r = json.loads(urllib.request.urlopen(req, timeout=45).read())
            if r.get("code") == 0:
                return pd.DataFrame(r["data"]["items"], columns=r["data"]["fields"])
            time.sleep(1.0)
        except Exception:
            time.sleep(1.2)
    return pd.DataFrame()


def _trade_cal(start, end):
    df = _call("trade_cal", {"exchange": "SSE", "start_date": start, "end_date": end, "is_open": "1"})
    return sorted(df["cal_date"].tolist())


def _z(s):
    s = pd.to_numeric(s, errors="coerce")
    return (s - s.mean()) / s.std()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=12)
    args = ap.parse_args()

    end = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y%m%d")
    start = (pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(days=30 * args.months + 220)).strftime("%Y%m%d")
    cal = _trade_cal(start, end)
    cal_ts = pd.to_datetime(cal)
    weekly = [cal[j] for j in range(len(cal))          # month-end trading dates
              if j + 1 == len(cal) or cal_ts[j].month != cal_ts[j + 1].month]
    print(f"span {cal[0]}..{cal[-1]}  monthly points={len(weekly)}", flush=True)

    # price matrix (month-end close) + circ_mv/pb/pe/dv at each date.
    # Per-date cache (small, reduced columns) makes the pull resumable after a kill.
    cachedir = CACHE / "xsec_cache"
    cachedir.mkdir(parents=True, exist_ok=True)
    closes, basics = {}, {}
    for i, d in enumerate(weekly):
        cf = cachedir / f"{d}.csv"
        if cf.exists():
            m = pd.read_csv(cf, dtype={"ts_code": str}).set_index("ts_code")
        else:
            dd = _call("daily", {"trade_date": d})
            db = _call("daily_basic", {"trade_date": d})
            if dd.empty or db.empty:
                continue
            m = dd.set_index("ts_code")[["close"]].join(
                db.set_index("ts_code")[[c for c in ("circ_mv", "pb", "pe_ttm", "dv_ttm") if c in db.columns]],
                how="outer")
            for c in m.columns:
                m[c] = pd.to_numeric(m[c], errors="coerce")
            m.to_csv(cf)
            time.sleep(0.1)
        closes[d] = m["close"]
        basics[d] = m[[c for c in ("circ_mv", "pb", "pe_ttm", "dv_ttm") if c in m.columns]]
        if i % 15 == 0:
            print(f"  {i}/{len(weekly)}", flush=True)
    px = pd.DataFrame(closes).sort_index(axis=1)          # tickers x weekly_dates
    print(f"price matrix {px.shape}", flush=True)

    # rebalance = last weekly date of each month
    wk = pd.to_datetime(weekly)
    reb = [weekly[j] for j in range(len(weekly))
           if j + 1 == len(weekly) or wk[j].month != wk[j + 1].month]
    # require full warmup so every factor (incl. 12mo IVOL) uses a complete window — no
    # partial-window early rebalances contaminating the estimates.
    reb = [d for d in reb if d in basics and weekly.index(d) >= IVOL_WEEKS][::REBAL_MONTHS]

    csi = pd.read_csv(CACHE / "index_CSI300.csv", parse_dates=["trade_date"]).set_index("trade_date")["close"]

    def factors(d):
        cols = list(px.columns)
        j = cols.index(d)
        base_b = basics[d].dropna(subset=["circ_mv"])
        base = base_b.sort_values("circ_mv", ascending=False).head(BASE_N).index
        p_now = px[d].reindex(base)
        mom = p_now / px[cols[max(0, j - MOM_WEEKS)]].reindex(base) - 1
        rev = p_now / px[cols[max(0, j - 1)]].reindex(base) - 1   # 1-month return (short-term reversal)
        wret = px[cols[max(0, j - VOL_WEEKS):j + 1]].reindex(base).pct_change(axis=1)
        vol = wret.std(axis=1)
        # IVOL: residual vol vs CSI300 = total_vol * sqrt(1 - corr(stock,market)^2), 12mo window
        iw = cols[max(0, j - IVOL_WEEKS):j + 1]
        iret = px[iw].reindex(base).pct_change(axis=1).iloc[:, 1:]
        mkt = pd.Series([csi[csi.index <= pd.Timestamp(x)].iloc[-1] for x in iw]).pct_change().values[1:]
        mkt = pd.Series(mkt, index=iret.columns)
        icorr = iret.apply(lambda r: r.corr(mkt), axis=1)
        ivol = iret.std(axis=1) * np.sqrt((1 - icorr ** 2).clip(lower=0))
        b = base_b.reindex(base)
        value = _z(-b["pb"]) + _z(-b["pe_ttm"]) + _z(b["dv_ttm"])
        f = pd.DataFrame({"mom": mom, "rev": rev, "vol": vol, "ivol": ivol, "value": value})
        f["multifactor"] = _z(f["mom"]) + _z(-f["vol"]) + _z(f["value"])
        return f.dropna(subset=["mom", "vol"])

    SLEEVES = {"ivol": ("ivol", True), "momentum": ("mom", False), "reversal": ("rev", True),
               "low_vol": ("vol", True), "value": ("value", False), "multifactor": ("multifactor", False)}
    TIMED = ["low_vol", "multifactor", "momentum"]  # factor basket + CSI300 bull-regime overlay
    ROUNDTRIP_BPS = 30.0                      # commission+stamp+slippage on turned-over fraction

    # CSI300 50/200 bull-regime gate for the timed overlay (no lookahead: decided at d)
    csi_s50, csi_s200 = csi.rolling(50).mean(), csi.rolling(200).mean()

    def is_bull(dstr):
        t = pd.Timestamp(dstr)
        sub = csi[csi.index <= t]
        if len(sub) < 200:
            return True
        return sub.iloc[-1] > csi_s50[csi_s50.index <= t].iloc[-1] > csi_s200[csi_s200.index <= t].iloc[-1]

    equity = {s: [1.0] for s in SLEEVES}
    equity["CSI300"] = [1.0]
    for b in TIMED:
        equity[f"{b}_timed"] = [1.0]
    dates_out = [reb[0]]
    prior = {s: set() for s in SLEEVES}
    prior_t = {f"{b}_timed": set() for b in TIMED}
    turnover = {s: [] for s in SLEEVES}
    side = ROUNDTRIP_BPS / 2 / 10000.0

    for k in range(len(reb) - 1):
        d, dn = reb[k], reb[k + 1]
        f = factors(d)
        n = max(5, int(len(f) * HOLD_FRAC))
        fwd = (px[dn].reindex(f.index) / px[d].reindex(f.index) - 1)
        bull = is_bull(d)
        held_sets = {}
        for s, (col, asc) in SLEEVES.items():
            held = set(f[col].sort_values(ascending=asc).head(n).index)
            held_sets[s] = held
            turn = 1.0 - (len(held & prior[s]) / len(held)) if held else 0.0
            turnover[s].append(turn)
            r = fwd.reindex(list(held)).dropna().mean()
            equity[s].append(equity[s][-1] * (1 + (0.0 if pd.isna(r) else r) - turn * ROUNDTRIP_BPS / 10000.0))
            prior[s] = held
        for b in TIMED:                        # timed overlay: basket if bull else cash
            ts = f"{b}_timed"
            target = held_sets[b] if bull else set()
            cost = (len(prior_t[ts] - target) + len(target - prior_t[ts])) / n * side
            r = fwd.reindex(list(target)).dropna().mean() if target else 0.0
            equity[ts].append(equity[ts][-1] * (1 + (0.0 if pd.isna(r) else r) - cost))
            prior_t[ts] = target
        c0 = csi[csi.index <= pd.Timestamp(d)].iloc[-1]
        c1 = csi[csi.index <= pd.Timestamp(dn)].iloc[-1]
        equity["CSI300"].append(equity["CSI300"][-1] * (c1 / c0))
        dates_out.append(dn)

    out = pd.DataFrame(equity, index=pd.to_datetime(dates_out))
    out.to_csv(CACHE / "xsec_equity.csv")
    print(f"\nCross-sectional sleeves — {dates_out[0]}..{dates_out[-1]} ({len(reb)} rebalances)\n")
    ppy = 12 / REBAL_MONTHS
    rows = []
    for s in equity:
        e = np.array(equity[s]); rr = np.diff(e) / e[:-1]
        dd = (1 - e / np.maximum.accumulate(e)).max()
        yrs = len(rr) / ppy
        rows.append({"sleeve": s, "total%": round((e[-1] - 1) * 100, 1),
                     "CAGR%": round((e[-1] ** (1 / yrs) - 1) * 100, 1) if yrs > 0 and e[-1] > 0 else -100,
                     "Sharpe": round(rr.mean() / rr.std() * np.sqrt(ppy), 2) if rr.std() > 0 else 0,
                     "maxDD%": round(dd * 100, 1),
                     "avg_turnover%": round(np.mean(turnover[s]) * 100, 0) if s in turnover else 0})
    board = pd.DataFrame(rows).sort_values("total%", ascending=False)
    print(board.to_string(index=False))
    board.to_csv(CACHE / "xsec_leaderboard.csv", index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
