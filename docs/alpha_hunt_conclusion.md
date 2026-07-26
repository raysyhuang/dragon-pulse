# A-Share Alpha Hunt — Conclusion (2026-07-26)

Systematic search for a tradable alpha in liquid A-shares, run as a MAS-style
multi-pipeline "horse race" (`scripts/paper_lab.py`) with an autonomous research
loop. Every candidate tested point-in-time, full-cycle, cost-aware, out-of-sample,
and across the longest available history.

## Verdict

**No stock-selection alpha survives rigorous, full-history, cost-aware testing in
liquid A-shares.** The only robust edge found is **trend-timing an index** —
market timing, not stock-picking, and modest.

## What was tested and killed

| Candidate | Result |
|---|---|
| RS-pullback engine (original live pipeline) | Loses to the index full-cycle |
| Northbound flow (沪股通/深股通) | Collinear with price; net-flow data discontinued 2024 |
| Crowdedness (volume/order-flow/margin) | Overfit; flipped sign out-of-sample |
| CYQ chip distribution (from myhhub/stock) | Too weak; sign-stable but not tradable |
| Momentum (6-month) | Crashes over the cycle (retail reverses, not trends) |
| Short-term reversal | Long-only large-cap version ties the index (the ~22% is long-short + small-cap) |
| Low-volatility (total vol) | Window-fitted: 0.48 → 0.11 on full sample |
| Value / multifactor | At or below the index |
| **IVOL (idiosyncratic vol)** | Closest call — beat the index 2020–2026 (0.47), but **ties over full 2015–2026 (0.40 vs 0.39)**. Defensive tilt, regime-dependent, not alpha |
| Mid-cap (CSI500) factor selection | Mid-cap *beta* beats large-cap; selection *within* adds nothing |
| Timing + factor combinations | Worse than the parts alone |

## The one survivor

**ChiNext 50/200 trend-timing** — hold the index in a bull trend, cash otherwise.
Sharpe **0.71** over 2015–2026, robust across parameters, indices, and costs.
Packaged with volatility-targeting as **VT20** (`scripts/chinext_timing_strategy.py`):
Sharpe 0.67, drawdown tamed 42% → 32%. Tradable via a ChiNext ETF (e.g. 159915).
Caveat: it's market timing, modest, and lumpy (≈3 payday years in 11).

## Why US engines (MAS Sniper/MR) don't transfer

Structural, not a data or pipeline failure:
1. **Retail-driven** (~80% retail) → momentum *reverses* instead of persisting; US-calibrated trend/breakout logic trades the wrong sign.
2. **Constraints** (T+1, 10% price limits, short-sale restrictions) block the tradable version of the real anomalies (the reversal alpha lives in the short leg).
3. **Domestic quant competition** has arbitraged the liquid-name factor premia.
4. **Policy non-stationarity** shreds backtested edges.

The MAS *framework* transferred fine — the paper lab *is* MAS applied here, and it
worked as a method. The US-tuned *signals* did not; re-deriving them for A-shares
mostly returned empty.

## Methodological lesson

This data manufactures window-specific "edges" trivially. Low-vol and IVOL both
looked like winners in a flattering window and reverted to ≤ index over full
history. The single discipline that caught every false positive: **test the
longest available history and refuse to stop at the flattering window.** ~10
plausible "alphas" died to it before any reached capital — the real win.

## What is tracked forward

`scripts/paper_lab.py` (focused watchlist) tracks ChiNext-timing (survivor) + IVOL
(defensive-tie tracker) vs CSI300/ChiNext buy-and-hold. Forward-refresh via
`scripts/paper_lab_refresh.sh` (local; Tushare rejects CI IPs). Stock-picking
top-1/day paper track runs separately in `outputs/top1_paper/`.

## If reopening

The lever is **not another factor** — it's a different data regime: intraday/tick,
alternative data, or true ML factor mining (qlib Alpha158/360, WorldQuant Alpha101).
That's an infrastructure decision, and the only thing with a real chance of
changing this answer.

## Reproduction

- `scripts/paper_lab.py` — multi-sleeve leaderboard (`--all` shows archived/failed sleeves)
- `scripts/xsec_sleeves.py` — cross-sectional factor sleeves (PIT, cost-aware)
- `scripts/xsec_robustness.py` — factor robustness sweeps (basket/universe/cost/window)
- `scripts/regime_timing_study.py` — index trend-timing robustness
- `scripts/chinext_timing_strategy.py` — the packaged survivor (vol-targeted)
