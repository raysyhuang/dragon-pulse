> **SUPERSEDED (2026-08-10).** An external audit rejected this note's evidentiary
> standing and its closure claim. The direction survived re-testing; the standing did
> not. See `2026-08-10-pit-selection-test-v2-corrected.md`. Retained unaltered as the
> record of what was claimed and on what basis.

# PIT Cross-Sectional Selection Test, 2021–2026 — the Null, Confirmed

**Status:** research / paper-only. Changes no selector, cron, execution rule, or order authority.
**Verdict:** **No stock-selection alpha.** Every factor that appears to work is a beta hedge.

## Design (preregistered before any output was inspected)

| | |
|---|---|
| Universe | top 1000 by `circ_mv` at each signal date, filtered by real `list_date`/`delist_date` from `list_status` L+D+P — **delisted names present while they existed** |
| Calendar | last trading day of each month, 66 rebalances 2021-01 → 2026-06 |
| Entry | **next session open** after the signal date |
| Exit | last trading day of the following month, close |
| Selection | top 50 by factor rank |
| Costs | 30 bps round trip |
| Censoring | a name without an entry or exit bar is **CENSORED, never dropped** |
| Control | 50 names evenly spaced across the cap-rank distribution, plus CSI300 |

Fills, no-fills, censoring and denominators are computed by `src/core/xsec_runner.run_xsec_replay` — the externally verified Task 3/4 implementation — not by anything written for this test. Censoring ran at 1–6 rows per sleeve out of 3,300.

## Results

| Sleeve | CAGR | vs control | beta | annual alpha | t(alpha) |
|---|---|---|---|---|---|
| turnover_low | **+2.17%** | +8.13% | **0.44** | +5.25% | **0.90** |
| dividend | −3.23% | +2.73% | 0.48 | +0.54% | 0.07 |
| value_pb | −3.38% | +2.58% | 0.46 | +0.22% | 0.03 |
| value_pe | −4.46% | +1.50% | 0.29 | −1.95% | −0.29 |
| **control (spread)** | **−5.96%** | — | 1.00 | — | — |
| size_small | −6.80% | −0.84% | 1.00 | −0.42% | −0.11 |
| reversal_1m | −12.65% | −6.69% | 1.18 | −4.61% | −0.85 |
| momentum_12_1 | −16.96% | −11.00% | — | — | — |
| CSI300 | −7.49% | — | — | — | — |

**No factor produces significant alpha.** The best t-statistic across the entire test is 0.90.

## Why the apparent winner is not an edge

`turnover_low` returns +2.17% CAGR against a control of −5.96%, which looks like a result until it is decomposed.

**It is a half-beta portfolio.** Beta 0.44. Every sleeve that beat the control has beta between 0.29 and 0.48; every sleeve with beta near 1.0 lost. The ranking is a beta ranking.

**Its excess is a direct function of market direction:**

- months the market **rose** (29): mean excess **−2.75%**
- months the market **fell** (37): mean excess **+3.28%**

**It is concentrated in one year and reverses in another.** Excess over control by year: +6.0%, +12.4%, **+30.4%** (2023), +8.3%, **−24.4%** (2025), +2.7%. The control returned −32.9% in 2023 and +26.2% in 2025. The factor wins exactly when the market crashes and loses exactly when it rallies.

**It collapses out of sample.** First half +14.97% over control; second half **+1.15%**.

**It is a coin flip monthly.** Beats the control in 34/66 months — 52%.

2021–2026 was predominantly a bear period, so *any* defensive tilt flatters over the full sample. This is the same diagnosis this repository already reached for low-vol: a defensive tilt, not an alpha. `dividend`, `value_pb` and `value_pe` show the identical signature — large positive excess in 2023, large negative in 2025 — because they are all proxies for the same low-beta exposure.

## What this closes, and what it does not

**Closed:** standard cross-sectional factor selection on liquid A-shares, tested on survivorship-free point-in-time data with executable fills and honest censoring. The null holds, and it now holds under a method whose every component was independently verified rather than trusted.

**Not closed:** this does not test `alpha_rs_pullback`, the live engine, which is a short-horizon technical rule rather than a monthly factor sort. That remains a separate question, though nothing here encourages optimism about it.

**Limits:** one country, one 5.5-year window, monthly rebalance, top-50, no sector neutrality, no ST/liquidity screen beyond the top-1000 cap filter. Low-turnover names are also the hardest to trade, so the `turnover_low` line is if anything optimistic — implementation cost would reduce it further.

## The unifying observation

The only property that reliably produced excess return in this test — reduced exposure when the market falls — is precisely what the ChiNext trend-timing rule does, and the timing rule does it far better:

| | Selection tilt (`turnover_low`) | Trend timing (ChiNext 50/200) |
|---|---|---|
| Mechanism | implicit, via a beta proxy | explicit, via exposure |
| Evidence | 5.5 years, t = 0.90 | 16 years, Sharpe better in 44/44 start quarters |
| Out of sample | collapses (+14.97% → +1.15%) | confirms (2011–2014 holdout positive on all three measures) |
| Control | none — you get whatever beta the factor happens to carry | direct — in or out |

Both are the same bet. One is measured, controllable and robust; the other is incidental, unstable and unmeasured. There is no reason to take the defensive exposure through a stock-selection rule when it can be taken directly.

## Recommendation

Stop looking for cross-sectional selection alpha in this universe. The question has now been asked properly and the answer is no.

Direct the paper-track effort at the timing sleeve, which is already running, and treat any future selection candidate as requiring a beta decomposition and an out-of-sample split **before** its headline number is discussed.
