#!/usr/bin/env python3
"""Does adding market breadth to the regime gate re-enter earlier — and pay?

Production gate (``src/pipelines/funnel.py:classify_regime``) is price-only:

    bull   iff close > SMA_s and SMA_s > SMA_l
    bear   iff close < SMA_l
    choppy otherwise

Only ``bull`` produces picks (rs_pullback ``regimes: ["bull"]``, acceptance
``excluded_regimes: [bear, choppy]``, ``book_size.{bear,choppy}.max_picks: 0``),
so the gate alone decides whether the scanner is in the market at all. Breadth is
computed and written into every artifact but never consulted. As of 2026-08-12
the two disagree loudly: breadth 72%, CSI300 above its SMA20, yet SMA20 still
under SMA50 — 29 sessions with no picks.

This asks whether a breadth term would have opened the gate sooner, and whether
the days it opens are days worth owning.

Guards against self-deception, matching scripts/regime_timing_study.py:
  - NO LOOKAHEAD: close[t] and breadth[t] set the position HELD on t+1.
  - SURVIVORSHIP-FREE BREADTH: the panel includes delisted tickers, so breadth in
    2015-2022 counts companies that actually traded then.
  - PARAM GRID: a term that needs one magic threshold is overfit.
  - MULTI-INDEX: CSI300 (the gate's own index) plus CSI500 / ChiNext.
  - SPLIT-SAMPLE: first half vs second half must agree in sign.
  - COSTS: 5bps/side base, 15bps stress, on every switch.
  - MARGINAL DAYS: the days a variant adds over baseline are scored on their own.
    This is the crux — extra exposure is only worth it if those specific days pay.

Usage (LOCAL — needs TUSHARE_TOKEN and ~20min cold, then cached):
    python scripts/breadth_regime_study.py
    python scripts/breadth_regime_study.py --start 2015-01-01 --json out.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE = PROJECT_ROOT / "outputs" / "research" / "breadth_study"
PANEL = CACHE / "panel"

INDICES = [("CSI300", "000300.SH"), ("CSI500", "000905.SH"), ("ChiNext", "399006.SZ")]
# Production pair first; the rest test whether any result needs a magic pair.
GRID = [(20, 50), (10, 30), (20, 60), (50, 120), (50, 200)]
BREADTH_GRID = [0.50, 0.55, 0.60, 0.65, 0.70]
PANEL_START, PANEL_END = "20141001", "20260812"
CHUNK = 250
BREADTH_SMA = 20  # matches market_breadth_pct_above_sma20 in the artifacts


# --------------------------------------------------------------------------
# Tushare
# --------------------------------------------------------------------------

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


TOKEN = ""


def call(api: str, params: dict, fields: str = "", tries: int = 6) -> dict:
    body = json.dumps({"api_name": api, "token": TOKEN, "params": params, "fields": fields}).encode()
    for attempt in range(tries):
        try:
            req = urllib.request.Request(
                "https://api.tushare.pro", data=body, headers={"Content-Type": "application/json"}
            )
            r = json.loads(urllib.request.urlopen(req, timeout=90).read())
            if r.get("code") == 0:
                return r
            time.sleep(2 + attempt * 3)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            time.sleep(2 + attempt * 3)
    return {"code": -1, "data": None}


def _all_tickers() -> list[str]:
    out: list[str] = []
    for status in ("L", "D", "P"):
        r = call("stock_basic", {"list_status": status}, "ts_code,list_date,delist_date")
        if r.get("code") == 0:
            out += [row[0] for row in r["data"]["items"]]
    return sorted(set(out))


def _fetch_one(code: str) -> pd.DataFrame:
    r = call("daily", {"ts_code": code, "start_date": PANEL_START, "end_date": PANEL_END},
             "ts_code,trade_date,close")
    if r.get("code") != 0 or not r["data"]["items"]:
        return pd.DataFrame(columns=["ts_code", "trade_date", "close"])
    return pd.DataFrame(r["data"]["items"], columns=r["data"]["fields"])


def ensure_panel() -> None:
    """Download the survivorship-free close panel, one resumable chunk at a time."""
    PANEL.mkdir(parents=True, exist_ok=True)
    codes = _all_tickers()
    chunks = [codes[i : i + CHUNK] for i in range(0, len(codes), CHUNK)]
    missing = [i for i in range(len(chunks)) if not (PANEL / f"chunk_{i:03d}.parquet").exists()]
    if not missing:
        return
    print(f"fetching {len(missing)}/{len(chunks)} panel chunks ({len(codes)} tickers)...", flush=True)
    for idx in missing:
        with ThreadPoolExecutor(max_workers=4) as pool:
            frames = list(pool.map(_fetch_one, chunks[idx]))
        df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
        df.to_parquet(PANEL / f"chunk_{idx:03d}.parquet", index=False)
        print(f"  chunk {idx + 1}/{len(chunks)}  {len(df)} rows", flush=True)


def load_index(name: str, code: str) -> pd.DataFrame:
    path = CACHE / f"index_{name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    frames = []
    for year in range(2014, 2027):
        r = call("index_daily", {"ts_code": code, "start_date": f"{year}0101", "end_date": f"{year}1231"})
        if r.get("code") == 0 and r["data"]["items"]:
            frames.append(pd.DataFrame(r["data"]["items"], columns=r["data"]["fields"]))
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[["trade_date", "close"]].drop_duplicates("trade_date").sort_values("trade_date")
    df = df.reset_index(drop=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return df


# --------------------------------------------------------------------------
# Breadth
# --------------------------------------------------------------------------

def build_breadth() -> pd.Series:
    """Daily fraction of trading A-shares above their own SMA20.

    A stock counts on a day only if it has a full SMA20 window and printed a
    close that day, so newly listed and delisted names enter and leave the
    denominator on their real dates instead of biasing the level.
    """
    path = CACHE / "breadth.parquet"
    if path.exists():
        s = pd.read_parquet(path).set_index("trade_date")["breadth"]
        s.index = pd.to_datetime(s.index)
        return s

    frames = [pd.read_parquet(p) for p in sorted(PANEL.glob("chunk_*.parquet"))]
    if not frames:
        raise SystemExit("panel is empty — run ensure_panel() first")
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    wide = df.pivot_table(index="trade_date", columns="ts_code", values="close", aggfunc="last")
    wide = wide.sort_index()

    sma = wide.rolling(BREADTH_SMA, min_periods=BREADTH_SMA).mean()
    live = wide.notna() & sma.notna()
    above = (wide > sma) & live
    breadth = above.sum(axis=1) / live.sum(axis=1).replace(0, np.nan)
    breadth = breadth.dropna()
    breadth.name = "breadth"

    CACHE.mkdir(parents=True, exist_ok=True)
    breadth.rename_axis("trade_date").reset_index().to_parquet(path, index=False)
    return breadth


def build_eqw_return() -> pd.Series:
    """Equal-weight daily return across all trading A-shares.

    The scanner holds a handful of names out of the top 1000 by market cap, not
    the CSI300 basket, so an equal-weight market series is a closer proxy for
    what the gate actually switches on and off than the index it reads.
    """
    path = CACHE / "eqw_return.parquet"
    if path.exists():
        s = pd.read_parquet(path).set_index("trade_date")["eqw"]
        s.index = pd.to_datetime(s.index)
        return s

    frames = [pd.read_parquet(p) for p in sorted(PANEL.glob("chunk_*.parquet"))]
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    wide = df.pivot_table(index="trade_date", columns="ts_code", values="close", aggfunc="last")
    wide = wide.sort_index()
    # limit=1 so a halted name carries one day, not a fabricated flat history
    rets = wide.ffill(limit=1).pct_change()
    rets = rets.where(wide.notna())  # never earn a return on a day with no print
    eqw = rets.mean(axis=1, skipna=True).dropna()
    eqw.name = "eqw"
    eqw.rename_axis("trade_date").reset_index().to_parquet(path, index=False)
    return eqw


def validate_breadth(breadth: pd.Series) -> dict:
    """Check the reconstruction against breadth the live scanner already wrote.

    The scan runs pre-open, so an artifact stamped D is built from the close of
    D-1 (verified: ``scan_results_2026-08-12`` carries csi300_last 4663.79, the
    2026-08-11 close). Each live value is therefore compared against the
    reconstruction on the previous trading day, not on its own stamp.
    """
    live = {}
    out_dir = PROJECT_ROOT / "outputs"
    for child in sorted(out_dir.glob("20*-*-*")):
        f = child / f"scan_results_{child.name}.json"
        if not f.is_file():
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        val = (data.get("regime_detail") or {}).get("market_breadth_pct_above_sma20")
        if isinstance(val, (int, float)):
            live[pd.Timestamp(child.name)] = float(val)
    if not live:
        return {"n": 0}
    ls = pd.Series(live).sort_index()
    idx = breadth.index
    rows = {}
    for stamp, val in ls.items():
        prior = idx[idx < stamp]
        if len(prior):
            rows[prior[-1]] = (val, float(breadth.loc[prior[-1]]))
    if not rows:
        return {"n": 0}
    joined = pd.DataFrame(rows, index=["live", "recon"]).T.sort_index()
    diff = joined["recon"] - joined["live"]
    return {
        "n": int(len(joined)),
        "corr": float(joined["recon"].corr(joined["live"])),
        "mean_abs_diff": float(diff.abs().mean()),
        "mean_diff": float(diff.mean()),
        "first": str(joined.index.min().date()),
        "last": str(joined.index.max().date()),
    }


# --------------------------------------------------------------------------
# Gate variants
# --------------------------------------------------------------------------

def gate_signals(px: pd.DataFrame, s: int, l: int, breadth: pd.Series, thr: float) -> dict:
    """Bull masks for each variant, aligned on the index's trading calendar."""
    close = px["close"]
    sma_s = close.rolling(s).mean()
    sma_l = close.rolling(l).mean()
    base = (close > sma_s) & (sma_s > sma_l)
    b = breadth.reindex(px.index).ffill(limit=3)
    wide = (b >= thr).fillna(False).astype(bool)
    return {
        "baseline": base,
        # the literal proposal: keep the gate, let strong breadth open it early
        "breadth_or": base | ((close > sma_s) & wide),
        # breadth as an extra filter rather than an extra door
        "breadth_and": base & wide,
        # is breadth simply the whole signal?
        "breadth_only": wide,
    }


def metrics(ret: np.ndarray, pos: pd.Series | None = None) -> dict:
    ret = np.nan_to_num(ret)
    if len(ret) == 0:
        return {"cagr": 0.0, "sharpe": 0.0, "maxdd": 0.0, "total": 0.0}
    eq = np.cumprod(1 + ret)
    yrs = len(ret) / 252.0
    dd = (1 - eq / np.maximum.accumulate(eq)).max()
    out = {
        "cagr": (eq[-1] ** (1 / yrs) - 1) * 100 if yrs > 0 and eq[-1] > 0 else -100.0,
        "sharpe": ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else 0.0,
        "maxdd": dd * 100,
        "total": (eq[-1] - 1) * 100,
    }
    if pos is not None:
        out["expo"] = float(pos.mean() * 100)
        out["switch"] = int((pos.diff().abs() > 0).sum())
    return out


def run_overlay(px: pd.DataFrame, bull: pd.Series, cost_bps_side: float) -> pd.DataFrame:
    d = pd.DataFrame(index=px.index)
    d["ret"] = px["close"].pct_change()
    d["pos"] = bull.astype(bool).shift(1, fill_value=False).astype(float)  # no lookahead
    d["stratret"] = d["pos"] * d["ret"] - d["pos"].diff().abs().fillna(0) * (cost_bps_side / 10000.0)
    return d


def marginal_days(px: pd.DataFrame, base: pd.Series, variant: pd.Series) -> dict:
    """Score exactly the days a variant is long and baseline is not.

    Earlier re-entry is only an improvement if these specific days pay; if they
    are where the falling knives live, the variant is buying drawdown.
    """
    ret = px["close"].pct_change()
    extra = (variant & ~base).astype(bool).shift(1, fill_value=False)
    held = base.astype(bool).shift(1, fill_value=False)
    r_extra = ret[extra].dropna()
    r_held = ret[held].dropna()
    return {
        "n_extra_days": int(len(r_extra)),
        "extra_mean_bps": float(r_extra.mean() * 10000) if len(r_extra) else 0.0,
        "extra_sharpe": float(r_extra.mean() / r_extra.std() * np.sqrt(252))
        if len(r_extra) > 2 and r_extra.std() > 0
        else 0.0,
        "extra_hit": float((r_extra > 0).mean() * 100) if len(r_extra) else 0.0,
        "baseline_mean_bps": float(r_held.mean() * 10000) if len(r_held) else 0.0,
    }


def reentry_lead(dates: pd.DatetimeIndex, base: pd.Series, variant: pd.Series) -> list[dict]:
    """For each baseline bull episode, how many sessions earlier did the variant open?"""
    b = base.astype(bool).values
    v = variant.astype(bool).values
    episodes = []
    for i in range(1, len(b)):
        if b[i] and not b[i - 1]:
            j = i
            while j > 0 and v[j - 1]:
                j -= 1
            episodes.append(
                {
                    "baseline_entry": str(dates[i].date()),
                    "variant_entry": str(dates[j].date()),
                    "lead_sessions": int(i - j),
                }
            )
    return episodes


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def main() -> int:
    global TOKEN
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--json", default=str(CACHE / "breadth_regime_study.json"))
    args = ap.parse_args()

    TOKEN = _token()
    CACHE.mkdir(parents=True, exist_ok=True)
    ensure_panel()
    breadth = build_breadth()

    report: dict = {"start": args.start, "breadth_validation": validate_breadth(breadth)}
    v = report["breadth_validation"]
    print("\n===== BREADTH RECONSTRUCTION vs LIVE ARTIFACTS =====")
    if v.get("n"):
        print(f"  {v['n']} overlapping sessions {v['first']}..{v['last']}  "
              f"corr {v['corr']:+.3f}  mean|diff| {v['mean_abs_diff']:.3f}  bias {v['mean_diff']:+.3f}")
    else:
        print("  no overlap with live artifacts")
    print(f"  history {breadth.index.min().date()}..{breadth.index.max().date()}  "
          f"n={len(breadth)}  mean {breadth.mean():.2f}")

    start = pd.Timestamp(args.start)
    report["indices"] = {}

    for name, code in INDICES:
        px = load_index(name, code).set_index("trade_date")
        bh_full = px["close"].pct_change()
        bh = metrics(bh_full[px.index >= start].values)
        print(f"\n===== {name} ({code})  {max(px.index.min(), start).date()}..{px.index.max().date()} =====")
        print(f"  BUY&HOLD            CAGR {bh['cagr']:+6.1f}%  Sharpe {bh['sharpe']:+.2f}  maxDD {bh['maxdd']:4.1f}%")

        idx_out: dict = {"buy_hold": bh, "grid": {}}
        for s, l in GRID:
            sig0 = gate_signals(px, s, l, breadth, BREADTH_GRID[0])
            d = run_overlay(px, sig0["baseline"], 5)
            d = d[d.index >= start]
            mb = metrics(d["stratret"].values, d["pos"])
            tag = "  <-- production" if (s, l) == (20, 50) else ""
            print(f"\n  --- MA {s}/{l}{tag}")
            print(f"  baseline            CAGR {mb['cagr']:+6.1f}%  Sharpe {mb['sharpe']:+.2f}  "
                  f"maxDD {mb['maxdd']:4.1f}%  expo {mb['expo']:2.0f}%  sw {mb['switch']:3d}")
            idx_out["grid"][f"{s}/{l}"] = {"baseline": mb, "variants": {}}

            for thr in BREADTH_GRID:
                sig = gate_signals(px, s, l, breadth, thr)
                for vname in ("breadth_or", "breadth_and", "breadth_only"):
                    dv = run_overlay(px, sig[vname], 5)
                    dv = dv[dv.index >= start]
                    m = metrics(dv["stratret"].values, dv["pos"])
                    dstress = run_overlay(px, sig[vname], 15)
                    dstress = dstress[dstress.index >= start]
                    ms = metrics(dstress["stratret"].values)
                    win = px.index >= start
                    mg = marginal_days(px[win], sig["baseline"][win], sig[vname][win])
                    row = {**m, "cagr_15bps": ms["cagr"], "sharpe_15bps": ms["sharpe"], **mg}
                    idx_out["grid"][f"{s}/{l}"]["variants"][f"{vname}@{thr:.2f}"] = row
                    if vname == "breadth_or":
                        print(f"  or  b>={thr:.2f}        CAGR {m['cagr']:+6.1f}%  Sharpe {m['sharpe']:+.2f}  "
                              f"maxDD {m['maxdd']:4.1f}%  expo {m['expo']:2.0f}%  sw {m['switch']:3d}  "
                              f"| +{mg['n_extra_days']:4d}d @ {mg['extra_mean_bps']:+5.1f}bps "
                              f"(base {mg['baseline_mean_bps']:+5.1f})  | 15bps CAGR {ms['cagr']:+5.1f}%")
            for thr in BREADTH_GRID:
                sig = gate_signals(px, s, l, breadth, thr)
                for vname in ("breadth_and", "breadth_only"):
                    r = idx_out["grid"][f"{s}/{l}"]["variants"][f"{vname}@{thr:.2f}"]
                    print(f"  {vname:<12} b>={thr:.2f} CAGR {r['cagr']:+6.1f}%  Sharpe {r['sharpe']:+.2f}  "
                          f"maxDD {r['maxdd']:4.1f}%  expo {r['expo']:2.0f}%  sw {r['switch']:3d}")

        report["indices"][name] = idx_out

    # --- split sample on the production pair, CSI300 ---
    px = load_index("CSI300", "000300.SH").set_index("trade_date")
    win = px.index >= start
    mid = px.index[win][len(px.index[win]) // 2]
    print(f"\n===== SPLIT SAMPLE — CSI300, MA 20/50 (split {mid.date()}) =====")
    split_out: dict = {"split_date": str(mid.date()), "halves": {}}
    for half, mask in (("H1", (px.index >= start) & (px.index < mid)), ("H2", px.index >= mid)):
        line = {}
        base_sig = gate_signals(px, 20, 50, breadth, 0.60)["baseline"]
        db = run_overlay(px, base_sig, 5)[mask]
        line["baseline"] = metrics(db["stratret"].values, db["pos"])
        for thr in BREADTH_GRID:
            sig = gate_signals(px, 20, 50, breadth, thr)
            dv = run_overlay(px, sig["breadth_or"], 5)[mask]
            line[f"breadth_or@{thr:.2f}"] = metrics(dv["stratret"].values, dv["pos"])
        split_out["halves"][half] = line
        print(f"  {half}  baseline        Sharpe {line['baseline']['sharpe']:+.2f}  "
              f"CAGR {line['baseline']['cagr']:+6.1f}%  maxDD {line['baseline']['maxdd']:4.1f}%")
        for thr in BREADTH_GRID:
            r = line[f"breadth_or@{thr:.2f}"]
            print(f"  {half}  or b>={thr:.2f}      Sharpe {r['sharpe']:+.2f}  "
                  f"CAGR {r['cagr']:+6.1f}%  maxDD {r['maxdd']:4.1f}%  expo {r['expo']:2.0f}%")
    report["split_sample_csi300_20_50"] = split_out

    # --- per calendar year: is any effect period-specific? ---
    print("\n===== BY YEAR — CSI300, MA 20/50, breadth_or@0.60 (5bps) =====")
    print("  year   baseline CAGR  Sharpe |  breadth_or CAGR  Sharpe |  expo  extra-days")
    sig = gate_signals(px, 20, 50, breadth, 0.60)
    db = run_overlay(px, sig["baseline"], 5)
    dv = run_overlay(px, sig["breadth_or"], 5)
    years = {}
    for year in sorted({d.year for d in px.index if d >= start}):
        mask = px.index.year == year
        mb_y = metrics(db["stratret"][mask].values, db["pos"][mask])
        mv_y = metrics(dv["stratret"][mask].values, dv["pos"][mask])
        mg_y = marginal_days(px[mask], sig["baseline"][mask], sig["breadth_or"][mask])
        years[year] = {"baseline": mb_y, "breadth_or": mv_y, **mg_y}
        print(f"  {year}   {mb_y['cagr']:+8.1f}%  {mb_y['sharpe']:+.2f} | "
              f"{mv_y['cagr']:+11.1f}%  {mv_y['sharpe']:+.2f} | {mv_y['expo']:3.0f}%  "
              f"{mg_y['n_extra_days']:4d}d @ {mg_y['extra_mean_bps']:+6.1f}bps")
    report["by_year_csi300_20_50_thr060"] = years

    # --- the current episode ---
    print("\n===== CURRENT EPISODE — CSI300, MA 20/50 =====")
    sig60 = gate_signals(px, 20, 50, breadth, 0.60)
    recent = px.index[px.index >= pd.Timestamp("2026-06-25")]
    b = breadth.reindex(px.index).ffill(limit=3)
    cur = []
    for d in recent:
        cur.append({
            "date": str(d.date()),
            "breadth": None if pd.isna(b.loc[d]) else round(float(b.loc[d]), 3),
            "baseline": bool(sig60["baseline"].loc[d]),
            "breadth_or": bool(sig60["breadth_or"].loc[d]),
        })
    # Most recent transition into bull — not merely "true on the first row",
    # which would misreport a window that opens mid-episode.
    opened = None
    for i in range(1, len(cur)):
        if cur[i]["breadth_or"] and not cur[i - 1]["breadth_or"]:
            opened = cur[i]["date"]
    last_baseline = max((c["date"] for c in cur if c["baseline"]), default=None)
    print(f"  baseline last bull: {last_baseline or 'not once in window'}")
    print(f"  breadth_or@0.60 currently: {cur[-1]['breadth_or']}  "
          f"(latest open: {opened or 'no transition in window'})")
    for c in cur[-8:]:
        print(f"    {c['date']}  breadth {c['breadth']}  baseline {str(c['baseline']):5}  "
              f"breadth_or {c['breadth_or']}")
    report["current_episode"] = cur

    # --- re-entry leads, production pair ---
    leads = reentry_lead(px.index[win], sig60["baseline"][win], sig60["breadth_or"][win])
    report["reentry_leads_csi300_20_50_thr060"] = leads
    if leads:
        arr = np.array([e["lead_sessions"] for e in leads])
        print(f"\n===== RE-ENTRY LEAD (CSI300 20/50, breadth_or@0.60) =====")
        print(f"  {len(leads)} baseline entries  median lead {np.median(arr):.0f} sessions  "
              f"mean {arr.mean():.1f}  max {arr.max()}")
        for e in leads[-6:]:
            print(f"    baseline {e['baseline_entry']}  variant {e['variant_entry']}  "
                  f"lead {e['lead_sessions']:3d} sessions")

    # --- leave-one-year-out: does any single year carry the verdict? ---
    print("\n===== LEAVE-ONE-YEAR-OUT — CSI300 20/50, breadth_or@0.60 vs baseline =====")
    print("  dropped   baseline Sharpe   breadth_or Sharpe   delta")
    loo = {}
    for drop in [None] + sorted({d.year for d in px.index if d >= start}):
        keep = (px.index >= start) if drop is None else ((px.index >= start) & (px.index.year != drop))
        sb = metrics(db["stratret"][keep].values)["sharpe"]
        sv = metrics(dv["stratret"][keep].values)["sharpe"]
        loo["none" if drop is None else str(drop)] = {"baseline": sb, "breadth_or": sv, "delta": sv - sb}
        label = "(none)" if drop is None else str(drop)
        print(f"  {label:<9} {sb:+13.2f}   {sv:+15.2f}   {sv - sb:+6.2f}")
    report["leave_one_year_out_csi300_20_50_thr060"] = loo

    # --- production-faithful: CSI300 gate, equal-weight A-share book ---
    print("\n===== CSI300 GATE APPLIED TO AN EQUAL-WEIGHT A-SHARE BOOK =====")
    eqw = build_eqw_return()
    eqw_px = pd.DataFrame({"close": (1 + eqw).cumprod()})
    eqw_out = {}
    common = px.index.intersection(eqw_px.index)
    bh_eqw = metrics(eqw.reindex(common)[common >= start].values)
    print(f"  BUY&HOLD (eqw)      CAGR {bh_eqw['cagr']:+6.1f}%  Sharpe {bh_eqw['sharpe']:+.2f}  "
          f"maxDD {bh_eqw['maxdd']:4.1f}%")
    eqw_out["buy_hold"] = bh_eqw
    for s, l in GRID:
        sig_g = gate_signals(px, s, l, breadth, 0.60)  # gate read off CSI300, as production does
        for vname in ("baseline", "breadth_or"):
            pos = sig_g[vname].reindex(common).astype(bool).shift(1, fill_value=False).astype(float)
            r = eqw.reindex(common)
            sr = pos * r - pos.diff().abs().fillna(0) * (5 / 10000.0)
            m = metrics(sr[common >= start].values, pos[common >= start])
            eqw_out[f"{s}/{l}:{vname}"] = m
            tag = "  <-- production" if (s, l) == (20, 50) and vname == "baseline" else ""
            print(f"  {s:>2}/{l:<3} {vname:<11} CAGR {m['cagr']:+6.1f}%  Sharpe {m['sharpe']:+.2f}  "
                  f"maxDD {m['maxdd']:4.1f}%  expo {m['expo']:2.0f}%{tag}")
    report["csi300_gate_on_eqw_book"] = eqw_out

    # --- kill tests on any book where breadth_or looked good ---
    # The equal-weight all-share book is NOT tradeable: daily rebalancing across
    # ~5900 names, many of them tiny, earns bid-ask bounce that no real book gets.
    # CSI500 and ChiNext are tradeable and carry no bounce, so a finding that
    # survives on them is real and one that only lives on eqw is an artifact.
    print("\n===== KILL TESTS — CSI300 gate, breadth_or@0.60, per book =====")
    books = {
        "eqw_allshare(NOT tradeable)": eqw,
        "CSI500": load_index("CSI500", "000905.SH").set_index("trade_date")["close"].pct_change(),
        "ChiNext": load_index("ChiNext", "399006.SZ").set_index("trade_date")["close"].pct_change(),
        "CSI300": px["close"].pct_change(),
    }
    sig_k = gate_signals(px, 20, 50, breadth, 0.60)
    kill: dict = {}
    for bname, r in books.items():
        idx = px.index.intersection(r.dropna().index)
        r = r.reindex(idx)
        row: dict = {}
        for vname in ("baseline", "breadth_or"):
            pos = sig_k[vname].reindex(idx).astype(bool).shift(1, fill_value=False).astype(float)
            for cost, ctag in ((5, ""), (15, "_15bps")):
                sr = pos * r - pos.diff().abs().fillna(0) * (cost / 10000.0)
                for half, mask in (("full", idx >= start),
                                   ("H1", (idx >= start) & (idx < mid)),
                                   ("H2", idx >= mid),
                                   ("ex2024", (idx >= start) & (idx.year != 2024))):
                    row[f"{vname}{ctag}:{half}"] = metrics(sr[mask].values, pos[mask])
        row["buy_hold:full"] = metrics(r[idx >= start].values)
        kill[bname] = row
        bh_s = row["buy_hold:full"]["sharpe"]
        print(f"\n  --- {bname}   (buy&hold Sharpe {bh_s:+.2f})")
        for half in ("full", "H1", "H2", "ex2024"):
            b5 = row[f"baseline:{half}"]
            v5 = row[f"breadth_or:{half}"]
            v15 = row[f"breadth_or_15bps:{half}"]
            print(f"    {half:<7} baseline {b5['sharpe']:+.2f} / breadth_or {v5['sharpe']:+.2f} "
                  f"(15bps {v15['sharpe']:+.2f})   delta {v5['sharpe'] - b5['sharpe']:+.2f}   "
                  f"CAGR {b5['cagr']:+5.1f}% -> {v5['cagr']:+5.1f}%   "
                  f"maxDD {b5['maxdd']:4.1f}% -> {v5['maxdd']:4.1f}%")
    report["kill_tests"] = kill

    # --- the one variant that looked good: breadth as a filter, split-sampled ---
    print("\n===== breadth_and@0.70 (breadth as a FILTER) — split sample, CSI300 20/50 =====")
    sig_and = gate_signals(px, 20, 50, breadth, 0.70)
    da = run_overlay(px, sig_and["breadth_and"], 5)
    filt = {}
    for half, mask in (("full", px.index >= start),
                       ("H1", (px.index >= start) & (px.index < mid)),
                       ("H2", px.index >= mid)):
        mb_h = metrics(db["stratret"][mask].values, db["pos"][mask])
        ma_h = metrics(da["stratret"][mask].values, da["pos"][mask])
        filt[half] = {"baseline": mb_h, "breadth_and_070": ma_h}
        print(f"  {half:<4} baseline    Sharpe {mb_h['sharpe']:+.2f}  CAGR {mb_h['cagr']:+6.1f}%  "
              f"maxDD {mb_h['maxdd']:4.1f}%  expo {mb_h['expo']:2.0f}%")
        print(f"  {half:<4} and b>=0.70 Sharpe {ma_h['sharpe']:+.2f}  CAGR {ma_h['cagr']:+6.1f}%  "
              f"maxDD {ma_h['maxdd']:4.1f}%  expo {ma_h['expo']:2.0f}%")
    report["breadth_and_070_split"] = filt

    Path(args.json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
