#!/usr/bin/env python3
"""Answer the actual question: how often does this pipeline hand you a ticker?

Reads a backtest daily CSV and reports pick cadence, the distribution of dry
spells, and where the live 30-session drought sits against them. Deliberately
separate from performance: "is it too conservative" is a frequency question,
and conflating it with returns is how a thin-but-real edge gets argued away.
"""
import sys
import numpy as np
import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "outputs/backtest/pit5y_clean/backtest_daily_pit5y_clean.csv"
LIVE_DROUGHT = 30

d = pd.read_csv(path)
d["date"] = pd.to_datetime(d["date"])
d = d.sort_values("date").reset_index(drop=True)
d["yr"] = d["date"].dt.year

print(f"window {d.date.min().date()} .. {d.date.max().date()}   {len(d)} sessions\n")

# --- regime sanity: a run starved of index history shows as all-choppy ---
reg = d.groupby("yr").regime.value_counts().unstack(fill_value=0)
for c in ("bull", "choppy", "bear"):
    if c not in reg.columns:
        reg[c] = 0
print("regime by year (all-choppy years = starved index history, not a calm market)")
print(reg[["bull", "choppy", "bear"]].to_string(), "\n")
if (reg["bull"] == 0).any():
    print(f"  ⚠ years with zero bull days: {list(reg.index[reg['bull'] == 0])} — verify CSI300 coverage\n")

print("  year   sessions  active  active%   picks  picks/active")
for y, g in d.groupby("yr"):
    a = int((g.picks > 0).sum())
    pk = int(g.picks.sum())
    print(f"  {y}     {len(g):5d}   {a:5d}   {a/len(g)*100:5.1f}%   {pk:5d}   "
          f"{pk/a if a else 0:5.2f}")
a = int((d.picks > 0).sum())
print(f"  TOTAL  {len(d):5d}   {a:5d}   {a/len(d)*100:5.1f}%   {int(d.picks.sum()):5d}\n")

# --- dry spells ---
runs, cur = [], 0
for p in d.picks:
    if p == 0:
        cur += 1
    else:
        if cur:
            runs.append(cur)
        cur = 0
trailing = cur          # an unfinished spell at the end of the sample
if cur:
    runs.append(cur)
r = np.array(runs)

print(f"dry spells: {len(r)}   median {int(np.median(r))}   75th {int(np.percentile(r,75))}   "
      f"90th {int(np.percentile(r,90))}   max {r.max()}")
print(f"  >=10 sessions: {(r>=10).sum()}   >=20: {(r>=20).sum()}   >=30: {(r>=30).sum()}   >=60: {(r>=60).sum()}")
if trailing:
    print(f"  note: sample ends mid-spell ({trailing} sessions) — counted, but right-censored")
pct = (r < LIVE_DROUGHT).mean() * 100
print(f"\n  live drought of {LIVE_DROUGHT} sessions sits at the {pct:.0f}th percentile "
      f"({(r>=LIVE_DROUGHT).sum()} of {len(r)} historical spells were at least this long)")
verdict = ("ordinary" if pct < 75 else "long but precedented" if pct < 95 else "unprecedented in this sample")
print(f"  -> {verdict}")
