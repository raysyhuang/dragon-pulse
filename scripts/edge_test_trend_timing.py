#!/usr/bin/env python3
"""Independent re-test of the one surviving lead: index trend-timing.

Run: python scripts/edge_test_trend_timing.py   (reads cached index CSVs, no provider calls)

Preregistered before looking at any output. The prior claim is that ChiNext 50/200
trend timing survives (Sharpe 0.40 -> 0.71, maxDD 70% -> 42%, costs included). Everything
else in this repo's alpha hunt died. So the question is not "does 50/200 look good" but
"does it survive the specific failure modes that killed the others".

Three kills to attempt, in order of how much damage they have historically done here:

  K1 WINDOW SELECTION. low_vol showed Sharpe 0.48, then 0.42 at 6y, then 0.11 at 7y once
     2019 entered the sample. Any result that depends on where the window starts is a
     window artifact. Test: sweep the start date across every available quarter.

  K2 PARAMETER SELECTION. 50/200 is one cell of a grid. If it is an isolated peak the
     result is a lucky cell; if the surrounding grid also works it is a real effect.
     Test: full (fast, slow) grid, report the distribution, not the best cell.

  K3 REGIME/INDEX SPECIFICITY. Test the identical rule on CSI300 and CSI500.

Plus mechanical checks: explicit no-lookahead, costs on every position change, and a
split-sample comparison.

No provider calls. Reads only the cached index CSVs.
"""
from __future__ import annotations

import itertools
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "outputs" / "paper_lab" / "index_inputs"
SIDE_BPS = 5.0          # ETF one-way cost: commission + spread, no stamp duty
TRADING_DAYS = 243.0    # A-share year


def load(name: str) -> pd.DataFrame:
    df = pd.read_csv(CACHE / f"{name}.csv")
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.sort_values("trade_date").drop_duplicates("trade_date").reset_index(drop=True)
    return df[["trade_date", "open", "close"]]


def equity_curve(df: pd.DataFrame, fast: int, slow: int, side_bps: float = SIDE_BPS,
                 fill: str = "next_open", dividend_yield: float = 0.0,
                 cash_annual: float = 0.0):
    """Long-or-cash trend timing, no leverage, no shorting.

    NO LOOKAHEAD: the signal from close[t] sets the position held over t+1.

    FILL CONVENTION. `same_close` credits the full close-to-close move on the day a
    position changes, which silently assumes you traded at the close you were still
    observing. That is the same-close fill this repository's replay work exists to
    eliminate. `next_open` (the default) credits only open-to-close on an entry day,
    which is what an ETF order placed after the signal can actually achieve.
    """
    if fill not in ("next_open", "same_close"):
        raise ValueError("fill must be next_open or same_close")
    c, o = df["close"].astype(float), df["open"].astype(float)
    sma_f, sma_s = c.rolling(fast).mean(), c.rolling(slow).mean()
    signal = ((c > sma_f) & (sma_f > sma_s)).astype(float)
    position = signal.shift(1).fillna(0.0)
    ret = c.pct_change().fillna(0.0)
    turnover = position.diff().abs().fillna(0.0)
    entry_adj = pd.Series(0.0, index=c.index)
    if fill == "next_open":
        entering = ((position == 1) & (position.shift(1) == 0)).astype(float)
        entry_adj = entering * ((c / o - 1) - ret)
    d_daily, cash_daily = dividend_yield / TRADING_DAYS, cash_annual / TRADING_DAYS
    net = (position * (ret + d_daily) + entry_adj + (1 - position) * cash_daily
           - turnover * (side_bps / 10_000.0))
    valid = slow + 1
    return (pd.DataFrame({"date": df["trade_date"], "net": net, "bh": ret + d_daily,
                          "pos": position, "turn": turnover})
            .iloc[valid:].reset_index(drop=True))


def stats(series: pd.Series) -> dict:
    if len(series) < 60:
        return {"ret": np.nan, "cagr": np.nan, "sharpe": np.nan, "maxdd": np.nan}
    eq = (1 + series).cumprod()
    years = len(series) / TRADING_DAYS
    total = eq.iloc[-1] - 1
    cagr = eq.iloc[-1] ** (1 / years) - 1 if eq.iloc[-1] > 0 else -1.0
    sd = series.std()
    sharpe = (series.mean() / sd * np.sqrt(TRADING_DAYS)) if sd > 0 else np.nan
    maxdd = (eq / eq.cummax() - 1).min()
    return {"ret": total, "cagr": cagr, "sharpe": sharpe, "maxdd": maxdd}


def evaluate(df, fast, slow, start=None, end=None, side_bps=SIDE_BPS,
             fill="next_open", dividend_yield=0.0, cash_annual=0.0):
    cur = equity_curve(df, fast, slow, side_bps, fill, dividend_yield, cash_annual)
    if start is not None:
        cur = cur[cur["date"] >= pd.Timestamp(start)]
    if end is not None:
        cur = cur[cur["date"] <= pd.Timestamp(end)]
    if len(cur) < 60:
        return None
    timed, bh = stats(cur["net"]), stats(cur["bh"])
    return {"n": len(cur), "timed": timed, "bh": bh,
            "d_sharpe": timed["sharpe"] - bh["sharpe"],
            "d_ret": timed["ret"] - bh["ret"],
            "d_maxdd": timed["maxdd"] - bh["maxdd"],
            "exposure": cur["pos"].mean(),
            "trades_per_yr": cur["turn"].sum() / (len(cur) / TRADING_DAYS)}


def fmt(r) -> str:
    if r is None:
        return "  (insufficient data)"
    t, b = r["timed"], r["bh"]
    return (f"timed: ret {t['ret']:+7.1%}  Sharpe {t['sharpe']:5.2f}  maxDD {t['maxdd']:6.1%}   |   "
            f"B&H: ret {b['ret']:+7.1%}  Sharpe {b['sharpe']:5.2f}  maxDD {b['maxdd']:6.1%}   |   "
            f"dSharpe {r['d_sharpe']:+5.2f}  dDD {r['d_maxdd']:+6.1%}  expo {r['exposure']:4.0%}")


DIVIDEND_YIELD = {"ChiNext": 0.005, "CSI300": 0.025, "CSI500": 0.015}
CASH_ANNUAL = 0.018


def main():
    import hashlib
    import json
    manifest = json.loads((CACHE / "manifest.json").read_text())
    idx = {n: load(n) for n in ("ChiNext", "CSI300", "CSI500")}
    ch = idx["ChiNext"]
    out = {"inputs": manifest, "fill": "next_open",
           "cash_annual": CASH_ANNUAL, "dividend_yield": DIVIDEND_YIELD,
           "side_bps": SIDE_BPS,
           "convention": ("signal from close[t] governs t+1; an entry day earns "
                          "open->close, not the full close-to-close move")}

    def ev(df, name, **kw):
        return evaluate(df, kw.pop("fast", 50), kw.pop("slow", 200),
                        dividend_yield=DIVIDEND_YIELD[name], cash_annual=CASH_ANNUAL, **kw)

    r = ev(ch, "ChiNext")
    out["headline"] = {"timed": r["timed"], "buy_hold": r["bh"], "exposure": r["exposure"],
                       "trades_per_year": r["trades_per_yr"]}

    rows = []
    for start in pd.date_range("2012-03-31", "2022-12-31", freq="QE"):
        q = ev(ch, "ChiNext", start=start)
        if q:
            rows.append({"start": start.strftime("%Y-%m-%d"),
                         "d_sharpe": q["d_sharpe"], "d_maxdd": q["d_maxdd"],
                         "d_ret": q["d_ret"]})
    out["k1_window"] = {"starts": len(rows),
                        "cuts_drawdown": sum(1 for x in rows if x["d_maxdd"] > 0),
                        "beats_sharpe": sum(1 for x in rows if x["d_sharpe"] > 0),
                        "beats_return": sum(1 for x in rows if x["d_ret"] > 0),
                        "worst_d_sharpe": min(x["d_sharpe"] for x in rows),
                        "median_d_sharpe": float(pd.Series([x["d_sharpe"] for x in rows]).median())}

    grid = []
    for f, s_ in itertools.product([10, 20, 30, 50, 75, 100], [100, 150, 200, 250, 300]):
        if f >= s_:
            continue
        g = ev(ch, "ChiNext", fast=f, slow=s_)
        if g:
            grid.append({"fast": f, "slow": s_, "sharpe": g["timed"]["sharpe"],
                         "d_sharpe": g["d_sharpe"], "d_maxdd": g["d_maxdd"]})
    cell = next(x for x in grid if x["fast"] == 50 and x["slow"] == 200)
    out["k2_grid"] = {"cells": len(grid),
                      "cuts_drawdown": sum(1 for x in grid if x["d_maxdd"] > 0),
                      "beats_sharpe": sum(1 for x in grid if x["d_sharpe"] > 0),
                      "percentile_50_200": float(
                          sum(1 for x in grid if x["sharpe"] < cell["sharpe"]) / len(grid)),
                      "best": max(grid, key=lambda x: x["sharpe"])}

    out["k3_indices"] = {n: {"timed": ev(d, n)["timed"], "buy_hold": ev(d, n)["bh"]}
                         for n, d in idx.items()}

    mid = ch["trade_date"].iloc[len(ch) // 2].strftime("%Y-%m-%d")
    out["split_post_hoc"] = {"split_at": mid,
                             "first": ev(ch, "ChiNext", end=mid)["timed"],
                             "second": ev(ch, "ChiNext", start=mid)["timed"],
                             "note": "POST-HOC split, not a sealed holdout"}
    h = ev(ch, "ChiNext", end="2014-01-02")
    out["pre_cache_window"] = {"window": "2010-05..2014-01", "timed": h["timed"],
                               "buy_hold": h["bh"],
                               "note": ("a window the original cached study could not see; "
                                        "it is out-of-cache, NOT a preregistered holdout")}

    out["cost_sensitivity"] = [
        {"side_bps": b, **{k: v for k, v in ev(ch, "ChiNext", side_bps=b)["timed"].items()}}
        for b in (0, 5, 10, 20, 50)]

    be = {}
    for n, d in idx.items():
        r_ = None
        for dy in [x / 2000 for x in range(0, 241)]:
            t = evaluate(d, 50, 200, dividend_yield=dy, cash_annual=CASH_ANNUAL)
            if t["timed"]["cagr"] <= t["bh"]["cagr"]:
                r_ = dy
                break
        be[n] = {"cagr_break_even_yield": r_, "assumed_yield": DIVIDEND_YIELD[n]}
    out["dividend_break_even"] = be

    payload = json.dumps(out, sort_keys=True, separators=(",", ":"), allow_nan=False,
                         default=float).encode()
    out["analysis_sha256"] = hashlib.sha256(payload).hexdigest()
    dest = ROOT / "outputs" / "paper_lab" / "timing_study_analysis.json"
    dest.write_text(json.dumps(out, indent=2, sort_keys=True, default=float) + "\n")

    hl = out["headline"]
    print("ChiNext 50/200, executable next-open fill, dividends and cash included")
    print(f"  timed  CAGR {hl['timed']['cagr']:+.2%}  Sharpe {hl['timed']['sharpe']:.2f}  "
          f"maxDD {hl['timed']['maxdd']:.1%}  exposure {hl['exposure']:.0%}")
    print(f"  B&H    CAGR {hl['buy_hold']['cagr']:+.2%}  Sharpe {hl['buy_hold']['sharpe']:.2f}  "
          f"maxDD {hl['buy_hold']['maxdd']:.1%}")
    k1, k2 = out["k1_window"], out["k2_grid"]
    print(f"  K1 {k1['cuts_drawdown']}/{k1['starts']} cut DD, {k1['beats_sharpe']}/{k1['starts']} "
          f"beat Sharpe (worst {k1['worst_d_sharpe']:+.2f})")
    print(f"  K2 {k2['cuts_drawdown']}/{k2['cells']} cut DD, 50/200 at "
          f"{k2['percentile_50_200']:.0%} percentile")
    print(f"  break-even dividend yield: " +
          ", ".join(f"{n} {v['cagr_break_even_yield']:.2%}" if v['cagr_break_even_yield']
                    else f"{n} none" for n, v in be.items()))
    print(f"  analysis_sha256 {out['analysis_sha256']}")
    print(f"  written {dest.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
