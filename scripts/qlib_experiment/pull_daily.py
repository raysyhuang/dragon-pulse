#!/usr/bin/env python3
"""Pull daily OHLCV for a universe of A-shares — data for the qlib-methodology TS experiment.

Per-ticker daily bars (tushare `daily`), resumable per-ticker cache. Universe = current
top-N by circ mkt cap (fixed universe = KNOWN survivorship bias, which FLATTERS the model;
if ML fails walk-forward even with that tailwind, the negative is strong. A PIT universe is
the follow-up if the biased version shows promise).

Usage: TUSHARE_TOKEN=... python scripts/qlib_experiment/pull_daily.py [N] [start]
Run locally (long-running). NOTE: Tushare also works from CI — the earlier
"CI IPs are rejected" claim was a mis-diagnosis.
"""
from __future__ import annotations
import json, os, sys, time, urllib.request
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
CACHE = ROOT / "outputs" / "qlib_experiment" / "daily_cache"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 400
START = sys.argv[2] if len(sys.argv) > 2 else "20160101"


def _token():
    t = os.environ.get("TUSHARE_TOKEN")
    if t:
        return t.strip()
    for line in (ROOT / ".env").read_text().splitlines():
        if line.strip().startswith("TUSHARE_TOKEN"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("TUSHARE_TOKEN not set")


TOK = _token()


def call(api, params):
    body = json.dumps({"api_name": api, "token": TOK, "params": params, "fields": ""}).encode()
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


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    end = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y%m%d")
    # universe: latest daily_basic, top-N by circ_mv (exclude ST via name later if needed)
    latest = None
    for off in range(0, 8):
        d = (pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(days=off)).strftime("%Y%m%d")
        db = call("daily_basic", {"trade_date": d})
        if not db.empty:
            latest = db
            break
    latest["circ_mv"] = pd.to_numeric(latest["circ_mv"], errors="coerce")
    uni = latest.dropna(subset=["circ_mv"]).sort_values("circ_mv", ascending=False).head(N)["ts_code"].tolist()
    (CACHE.parent / "universe.json").write_text(json.dumps({"n": N, "asof": end, "tickers": uni}))
    print(f"universe: top-{N} by circ_mv, pulling daily {START}..{end}", flush=True)

    done = 0
    for i, tc in enumerate(uni):
        cf = CACHE / f"{tc}.csv"
        if cf.exists():
            done += 1
            continue
        df = call("daily", {"ts_code": tc, "start_date": START, "end_date": end})
        if not df.empty:
            df.to_csv(cf, index=False)
            done += 1
        if i % 25 == 0:
            print(f"  {i}/{len(uni)} (cached {done})", flush=True)
        time.sleep(0.1)
    print(f"done: {done}/{len(uni)} tickers cached in {CACHE}", flush=True)


if __name__ == "__main__":
    main()
