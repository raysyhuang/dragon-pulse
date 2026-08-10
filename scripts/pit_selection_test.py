#!/usr/bin/env python3
"""Monthly PIT cross-sectional selection test, 2021-2026.

Preregistered before any output was inspected.

QUESTION: on a survivorship-free point-in-time universe, with executable next-session-open
fills and censoring rather than dropping, does ANY standard cross-sectional factor beat an
equal-weight control on the same universe?

DESIGN (fixed in advance):
  universe   top 1000 by circ_mv at each signal date, filtered by real list/delist dates
             (list_status L+D+P), so delisted names are present while they existed
  calendar   last trading day of each month, 2021-01 .. 2026-06
  entry      NEXT session open after the signal date          (via verified replay core)
  exit       last trading day of the following month, close
  selection  top 50 by factor rank
  costs      30 bps round trip, research simplification
  censoring  a name without an entry or exit bar is CENSORED, never dropped
  controls   equal-weight universe (same selection machinery, random-free), and CSI300

FACTORS (all computable from daily_basic + monthly closes; each already failed on this
repo's non-PIT harness, so the question is whether PIT + honest accounting changes it):
  value_pe, value_pb, dividend, size_small, momentum_12_1, reversal_1m, turnover_low

The accounting is done by src/core/xsec_runner.run_xsec_replay, so fills, no-fills,
censoring and denominators come from the externally verified implementation rather than
from anything written here.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import sys
import time
from datetime import date

import pandas as pd
import requests

ROOT = pathlib.Path(__file__).resolve().parents[1]
CACHE = ROOT / "outputs" / "pit_selection_cache"
API = "http://api.tushare.pro"
UNIVERSE_N = 1000
TOP_K = 50
COST_BPS = 30.0
START_Y, END_Y = 2021, 2026


def _token() -> str:
    env = pathlib.Path(__file__).resolve().parents[1] / ".env"
    for line in env.read_text().splitlines():
        m = re.match(r"^\s*([A-Z_]+)\s*=\s*(.*?)\s*$", line)
        if m:
            os.environ.setdefault(m.group(1), m.group(2).strip("\"'"))
    return os.environ["TUSHARE_TOKEN"]


TOKEN = _token()
_last = [0.0]


def api(name: str, fields: str = "", **params) -> pd.DataFrame:
    for attempt in range(6):
        gap = time.time() - _last[0]
        if gap < 0.32:
            time.sleep(0.32 - gap)
        _last[0] = time.time()
        r = requests.post(API, timeout=120, json={"api_name": name, "token": TOKEN,
                                                  "params": params, "fields": fields}).json()
        if r.get("code") == 0:
            d = r["data"]
            return pd.DataFrame(d["items"], columns=d["fields"])
        if "每分钟" in str(r.get("msg", "")) or "rate" in str(r.get("msg", "")).lower():
            time.sleep(20)
            continue
        raise SystemExit(f"{name} failed: {r.get('msg')}")
    raise SystemExit(f"{name} exhausted retries")


def cached(key: str, fn):
    CACHE.mkdir(parents=True, exist_ok=True)
    p = CACHE / f"{key}.parquet"
    if p.exists():
        return pd.read_parquet(p)
    df = fn()
    df.to_parquet(p)
    return df


def main() -> int:
    print("PIT MONTHLY CROSS-SECTIONAL SELECTION TEST 2021-2026", flush=True)

    # ---- calendar -------------------------------------------------------------
    cal = cached("trade_cal", lambda: api("trade_cal", exchange="SSE",
                                          start_date="20200101", end_date="20261231",
                                          fields="cal_date,is_open"))
    open_days = sorted(cal[cal["is_open"].astype(int) == 1]["cal_date"].tolist())
    by_month: dict[str, list[str]] = {}
    for d in open_days:
        by_month.setdefault(d[:6], []).append(d)
    months = sorted(m for m in by_month if START_Y <= int(m[:4]) <= END_Y)
    # signal = last session of month M; entry = first session of M+1; exit = last of M+1
    plan = []
    for i in range(len(months) - 1):
        m, nxt = months[i], months[i + 1]
        plan.append({"signal": by_month[m][-1], "entry": by_month[nxt][0], "exit": by_month[nxt][-1]})
    today = date.today().strftime("%Y%m%d")
    plan = [p for p in plan if p["signal"] >= f"{START_Y}0101" and p["exit"] < today]
    print(f"  {len(plan)} monthly rebalances: {plan[0]['signal']} .. {plan[-1]['signal']}", flush=True)

    # ---- listing table (survivorship-free) ------------------------------------
    def _listings():
        frames = [api("stock_basic", fields="ts_code,list_date,delist_date", list_status=s)
                  for s in ("L", "D", "P")]
        return pd.concat([f for f in frames if len(f)], ignore_index=True)
    lst = cached("listings", _listings)
    lst["list_date"] = lst["list_date"].fillna("")
    lst["delist_date"] = lst["delist_date"].fillna("")
    print(f"  listing table: {len(lst)} issuers ({(lst['delist_date'] != '').sum()} delisted)", flush=True)

    # ---- fetch per-date panels -------------------------------------------------
    need = sorted({p["signal"] for p in plan} | {p["entry"] for p in plan} | {p["exit"] for p in plan})
    print(f"  fetching {len(need)} daily panels + {len(plan)+1} daily_basic panels ...", flush=True)
    daily, basics = {}, {}
    for i, d in enumerate(need):
        daily[d] = cached(f"daily_{d}", lambda d=d: api(
            "daily", trade_date=d, fields="ts_code,open,high,low,close,vol"))
        if i % 20 == 0:
            print(f"    daily {i}/{len(need)}", flush=True)
    sig_dates = sorted({p["signal"] for p in plan})
    for i, d in enumerate(sig_dates):
        basics[d] = cached(f"basic_{d}", lambda d=d: api(
            "daily_basic", trade_date=d,
            fields="ts_code,circ_mv,pe_ttm,pb,ps_ttm,dv_ratio,turnover_rate,volume_ratio"))
        if i % 20 == 0:
            print(f"    basic {i}/{len(sig_dates)}", flush=True)
    print("  fetch complete", flush=True)

    # ---- monthly close panel for momentum / reversal ---------------------------
    closes = {}
    for d in sig_dates:
        s = daily[d].set_index("ts_code")["close"].astype(float)
        closes[d] = s
    close_panel = pd.DataFrame(closes)          # rows = ts_code, cols = signal dates

    ld = dict(zip(lst["ts_code"], lst["list_date"]))
    dd = dict(zip(lst["ts_code"], lst["delist_date"]))

    FACTORS = {
        "value_pe":      ("pe_ttm", True,  "cheapest positive P/E"),
        "value_pb":      ("pb", True,      "lowest P/B"),
        "dividend":      ("dv_ratio", False, "highest dividend yield"),
        "size_small":    ("circ_mv", True, "smallest float cap in the top-1000"),
        "turnover_low":  ("turnover_rate", True, "lowest turnover"),
        "momentum_12_1": (None, False,     "12-1 month momentum"),
        "reversal_1m":   (None, True,      "worst prior month"),
        "control_spread": (None, None,     "50 names evenly spaced across the cap-rank distribution"),
    }

    rebalances = {k: [] for k in FACTORS}
    stats_rows = []
    for pi, p in enumerate(plan):
        sd, ed, xd = p["signal"], p["entry"], p["exit"]
        b = basics[sd].copy()
        for c in ("circ_mv", "pe_ttm", "pb", "ps_ttm", "dv_ratio", "turnover_rate"):
            b[c] = pd.to_numeric(b[c], errors="coerce")
        sd_iso = f"{sd[:4]}-{sd[4:6]}-{sd[6:]}"
        b["list_date"] = b["ts_code"].map(ld).fillna("")
        b["delist_date"] = b["ts_code"].map(dd).fillna("")
        elig = b[(b["list_date"] != "") & (b["list_date"] <= sd)
                 & ((b["delist_date"] == "") | (b["delist_date"] > sd))
                 & (b["circ_mv"] > 0)].copy()
        uni = elig.nlargest(UNIVERSE_N, "circ_mv").set_index("ts_code")
        stats_rows.append({"signal": sd, "eligible": len(elig), "universe": len(uni)})

        entry_bars = daily[ed].set_index("ts_code")
        exit_bars = daily[xd].set_index("ts_code")
        j = sig_dates.index(sd)

        for fname, (col, ascending, _desc) in FACTORS.items():
            if fname == "control_spread":
                # every (N/K)-th name by cap rank: a deterministic, universe-representative
                # sample. uni is cap-DESCENDING, so uni.index[:TOP_K] would be the 50
                # largest caps - a mega-cap bet, not a control.
                step = max(1, len(uni) // TOP_K)
                pick = uni.index[::step][:TOP_K]
            elif fname == "momentum_12_1":
                if j < 12:
                    continue
                mom = (close_panel[sig_dates[j - 1]] / close_panel[sig_dates[j - 12]] - 1).dropna()
                mom = mom[mom.index.isin(uni.index)]
                pick = mom.nlargest(TOP_K).index
            elif fname == "reversal_1m":
                if j < 1:
                    continue
                rev = (close_panel[sd] / close_panel[sig_dates[j - 1]] - 1).dropna()
                rev = rev[rev.index.isin(uni.index)]
                pick = rev.nsmallest(TOP_K).index
            else:
                s = uni[col].dropna()
                if col in ("pe_ttm", "pb"):
                    s = s[s > 0]
                pick = (s.nsmallest(TOP_K) if ascending else s.nlargest(TOP_K)).index
            if len(pick) == 0:
                continue

            sel = []
            for t in pick:
                def bar(frame, day):
                    if t not in frame.index:
                        return None
                    r = frame.loc[t]
                    try:
                        o, h, l, c, v = (float(r["open"]), float(r["high"]),
                                         float(r["low"]), float(r["close"]), float(r["vol"]))
                    except (TypeError, ValueError):
                        return None
                    if not all(x > 0 for x in (o, h, l, c)) or not (l <= o <= h and l <= c <= h):
                        return None
                    return {"day": f"{day[:4]}-{day[4:6]}-{day[6:]}", "open": o, "high": h,
                            "low": l, "close": c, "volume": max(v, 0.0)}
                sel.append({"ticker": t, "factor_score": float(len(sel)),
                            "next_session": bar(entry_bars, ed),
                            "exit_session": bar(exit_bars, xd)})
            rebalances[fname].append({"rebalance_date": sd_iso, "sleeve": fname,
                                      "factor_order": "DESC", "max_entry_cap": 1e12,
                                      "selected": sel})
        if pi % 12 == 0:
            print(f"    built {pi}/{len(plan)} rebalances", flush=True)

    us = pd.DataFrame(stats_rows)
    print(f"\n  PIT universe: eligible {us['eligible'].min()}-{us['eligible'].max()}, "
          f"selected top {UNIVERSE_N} each month", flush=True)

    # ---- replay through the externally verified core ---------------------------
    from src.core.xsec_runner import run_xsec_replay
    out_root = ROOT / "outputs" / "pit_selection_out"
    results = {}
    for fname, rebs in rebalances.items():
        if not rebs:
            continue
        d = out_root / fname
        if d.exists():
            import shutil
            shutil.rmtree(d)
        art = run_xsec_replay(rebs, output_dir=d, max_concurrent_slots=TOP_K,
                              total_cost_bps=COST_BPS)
        recs = [json.loads(l) for l in art.read_text().splitlines() if l.strip()]
        tot_sel = sum(r["summary"]["selected"] for r in recs)
        tot_fill = sum(r["summary"]["filled"] for r in recs)
        tot_cens = sum(r["summary"]["censored"] for r in recs)
        means = [r["summary"]["filled_mean_net_return"] for r in recs
                 if r["summary"]["filled_mean_net_return"] is not None]
        eq = 1.0
        for m in means:
            eq *= (1 + m)
        yrs = len(means) / 12.0
        results[fname] = {"rebalances": len(recs), "selected": tot_sel, "filled": tot_fill,
                          "censored": tot_cens, "months": len(means),
                          "total": eq - 1, "cagr": eq ** (1 / yrs) - 1 if yrs > 0 else float("nan"),
                          "mean_m": sum(means) / len(means), "series": means}
    # CSI300 benchmark on the same calendar
    csi = cached("csi300", lambda: api("index_daily", ts_code="000300.SH",
                                       start_date="20201201", end_date="20261231",
                                       fields="trade_date,close"))
    csi = csi.set_index("trade_date")["close"].astype(float)
    bench = []
    for p in plan:
        if p["entry"] in csi.index and p["exit"] in csi.index:
            bench.append(csi[p["exit"]] / csi[p["entry"]] - 1)
    beq = 1.0
    for m in bench:
        beq *= (1 + m)

    print("\n" + "=" * 104)
    print(f"RESULTS — top {TOP_K} of a PIT top-{UNIVERSE_N} universe, monthly, {COST_BPS:.0f}bps round trip")
    print("=" * 104)
    print(f"  {'sleeve':16}{'months':>7}{'selected':>9}{'filled':>7}{'cens':>6}"
          f"{'total':>9}{'CAGR':>8}{'mean/mo':>9}{'vs ctrl':>9}")
    ctrl = results.get("control_spread", {}).get("cagr", float("nan"))
    order = sorted(results, key=lambda k: -results[k]["cagr"])
    for k in order:
        r = results[k]
        print(f"  {k:16}{r['months']:>7}{r['selected']:>9}{r['filled']:>7}{r['censored']:>6}"
              f"{r['total']:>+9.1%}{r['cagr']:>+8.2%}{r['mean_m']:>+9.3%}"
              f"{r['cagr']-ctrl:>+9.2%}")
    byrs = len(bench) / 12.0
    print(f"  {'CSI300 (bench)':16}{len(bench):>7}{'-':>9}{'-':>7}{'-':>6}"
          f"{beq-1:>+9.1%}{beq**(1/byrs)-1:>+8.2%}{'-':>9}{'-':>9}")
    json.dump({k: {kk: vv for kk, vv in v.items() if kk != 'series'} for k, v in results.items()},
              (out_root / "summary.json").open("w"), indent=2)
    print(f"\n  artifacts: {out_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
