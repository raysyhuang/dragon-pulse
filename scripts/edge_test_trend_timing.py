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
CACHE = ROOT / "outputs" / "paper_lab"
SIDE_BPS = 5.0          # ETF one-way cost: commission + spread, no stamp duty
TRADING_DAYS = 243.0    # A-share year


def load(name: str) -> pd.DataFrame:
    df = pd.read_csv(CACHE / f"index_{name}.csv")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").drop_duplicates("trade_date").reset_index(drop=True)
    return df[["trade_date", "close"]]


def equity_curve(df: pd.DataFrame, fast: int, slow: int, side_bps: float = SIDE_BPS):
    """Long-or-cash trend timing, no leverage, no shorting.

    NO LOOKAHEAD: the signal computed from close[t] sets the position HELD over t+1,
    i.e. position is shifted one bar before being multiplied by the forward return.
    """
    c = df["close"].astype(float)
    sma_f, sma_s = c.rolling(fast).mean(), c.rolling(slow).mean()
    signal = ((c > sma_f) & (sma_f > sma_s)).astype(float)
    position = signal.shift(1).fillna(0.0)            # <-- the only lookahead guard needed
    ret = c.pct_change().fillna(0.0)
    turnover = position.diff().abs().fillna(0.0)
    net = position * ret - turnover * (side_bps / 10_000.0)
    valid = slow + 1
    return (pd.DataFrame({"date": df["trade_date"], "net": net, "bh": ret,
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


def evaluate(df, fast, slow, start=None, end=None, side_bps=SIDE_BPS):
    cur = equity_curve(df, fast, slow, side_bps)
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


def main():
    print("=" * 118)
    print("INDEPENDENT RE-TEST OF THE SURVIVING LEAD — index trend timing (long or cash)")
    print(f"costs {SIDE_BPS:.0f} bps/side · signal from close[t] held over t+1 · no leverage, no shorting")
    print("=" * 118)

    idx = {n: load(n) for n in ("ChiNext", "CSI300", "CSI500")}
    for n, d in idx.items():
        print(f"  {n:8} {d['trade_date'].min():%Y-%m-%d} -> {d['trade_date'].max():%Y-%m-%d}  ({len(d)} bars)")

    ch = idx["ChiNext"]

    # ---------------------------------------------------------------- headline reproduction
    print("\n" + "-" * 118)
    print("0. REPRODUCE THE CLAIM — ChiNext 50/200, the script's hard-coded 2015-01-01 start")
    print("-" * 118)
    print(fmt(evaluate(ch, 50, 200, start="2015-01-01")))

    # ---------------------------------------------------------------- K1 window selection
    print("\n" + "-" * 118)
    print("K1. WINDOW SELECTION — same rule, every possible start quarter (this killed low_vol)")
    print("-" * 118)
    rows = []
    for start in pd.date_range("2014-03-31", "2022-12-31", freq="QE"):
        r = evaluate(ch, 50, 200, start=start)
        if r:
            rows.append({"start": start.date(), "sharpe": r["timed"]["sharpe"],
                         "bh_sharpe": r["bh"]["sharpe"], "d_sharpe": r["d_sharpe"],
                         "ret": r["timed"]["ret"], "d_ret": r["d_ret"],
                         "maxdd": r["timed"]["maxdd"], "d_maxdd": r["d_maxdd"]})
    w = pd.DataFrame(rows)
    print(f"  {len(w)} start dates tested (2014Q1 -> 2022Q4), all ending 2026-07-24\n")
    print(f"  timed Sharpe   : min {w['sharpe'].min():5.2f}  median {w['sharpe'].median():5.2f}  max {w['sharpe'].max():5.2f}")
    print(f"  delta Sharpe   : min {w['d_sharpe'].min():+5.2f}  median {w['d_sharpe'].median():+5.2f}  max {w['d_sharpe'].max():+5.2f}")
    print(f"  delta maxDD    : min {w['d_maxdd'].min():+6.1%}  median {w['d_maxdd'].median():+6.1%}  max {w['d_maxdd'].max():+6.1%}")
    print(f"  beats B&H on Sharpe : {(w['d_sharpe'] > 0).sum()}/{len(w)} starts")
    print(f"  beats B&H on return : {(w['d_ret'] > 0).sum()}/{len(w)} starts")
    print(f"  cuts drawdown       : {(w['d_maxdd'] > 0).sum()}/{len(w)} starts")
    print("\n  worst 3 starts by delta-Sharpe:")
    for _, r in w.nsmallest(3, "d_sharpe").iterrows():
        print(f"    {r['start']}  timed Sharpe {r['sharpe']:5.2f} vs B&H {r['bh_sharpe']:5.2f}  (delta {r['d_sharpe']:+5.2f})")
    print("  best 3 starts by delta-Sharpe:")
    for _, r in w.nlargest(3, "d_sharpe").iterrows():
        print(f"    {r['start']}  timed Sharpe {r['sharpe']:5.2f} vs B&H {r['bh_sharpe']:5.2f}  (delta {r['d_sharpe']:+5.2f})")

    # ---------------------------------------------------------------- K2 parameter selection
    print("\n" + "-" * 118)
    print("K2. PARAMETER SELECTION — full MA grid on the FULL sample (is 50/200 a peak or a plateau?)")
    print("-" * 118)
    fasts = [10, 20, 30, 50, 75, 100]
    slows = [100, 150, 200, 250, 300]
    grid = []
    for f, s in itertools.product(fasts, slows):
        if f >= s:
            continue
        r = evaluate(ch, f, s)
        if r:
            grid.append({"fast": f, "slow": s, "sharpe": r["timed"]["sharpe"],
                         "d_sharpe": r["d_sharpe"], "d_maxdd": r["d_maxdd"], "d_ret": r["d_ret"]})
    g = pd.DataFrame(grid)
    bh_full = evaluate(ch, 50, 200)["bh"]
    print(f"  full sample B&H: Sharpe {bh_full['sharpe']:.2f}, maxDD {bh_full['maxdd']:.1%}\n")
    print("  timed Sharpe by (fast, slow):")
    print(g.pivot(index="fast", columns="slow", values="sharpe").round(2).to_string())
    print(f"\n  cells tested {len(g)} · beats B&H Sharpe in {(g['d_sharpe'] > 0).sum()}/{len(g)}"
          f" · beats B&H return in {(g['d_ret'] > 0).sum()}/{len(g)}"
          f" · cuts drawdown in {(g['d_maxdd'] > 0).sum()}/{len(g)}")
    print(f"  Sharpe across grid: min {g['sharpe'].min():.2f}  median {g['sharpe'].median():.2f}  max {g['sharpe'].max():.2f}")
    cell = g[(g["fast"] == 50) & (g["slow"] == 200)].iloc[0]
    pct = (g["sharpe"] < cell["sharpe"]).mean()
    print(f"  50/200 sits at the {pct:.0%} percentile of the grid  (peak => lucky cell; mid => plateau)")

    # ---------------------------------------------------------------- K3 index specificity
    print("\n" + "-" * 118)
    print("K3. INDEX SPECIFICITY — identical 50/200 rule, full available sample")
    print("-" * 118)
    for n, d in idx.items():
        print(f"  {n:8} {fmt(evaluate(d, 50, 200))}")

    # ---------------------------------------------------------------- split sample
    print("\n" + "-" * 118)
    print("SPLIT SAMPLE — ChiNext 50/200, first half vs second half of available data")
    print("-" * 118)
    mid = ch["trade_date"].iloc[len(ch) // 2]
    print(f"  split at {mid:%Y-%m-%d}")
    print(f"  first  half  {fmt(evaluate(ch, 50, 200, end=mid))}")
    print(f"  second half  {fmt(evaluate(ch, 50, 200, start=mid))}")

    # ---------------------------------------------------------------- cost sensitivity
    print("\n" + "-" * 118)
    print("COST SENSITIVITY — ChiNext 50/200, full sample")
    print("-" * 118)
    for bps in (0, 5, 10, 20, 50):
        r = evaluate(ch, 50, 200, side_bps=bps)
        print(f"  {bps:3.0f} bps/side: Sharpe {r['timed']['sharpe']:5.2f}  ret {r['timed']['ret']:+7.1%}  "
              f"turnover {r['trades_per_yr']:.1f} side-trades/yr")


if __name__ == "__main__":
    main()
