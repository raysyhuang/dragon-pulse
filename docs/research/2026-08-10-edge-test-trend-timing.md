# Is There an Edge? — Independent Re-Test of the Surviving Lead

**Status:** research / paper-only. Changes no selector, cron, execution rule, or order authority.
**Reproduce:** `python scripts/edge_test_trend_timing.py` — reads only committed, hashed inputs under `outputs/paper_lab/index_inputs/`. No provider call.
**Every figure below is generated from `outputs/paper_lab/timing_study_analysis.json`.** Verify with `python scripts/render_timing_doc.py --check`.

## Question

The repository's alpha hunt killed every stock-selection candidate. One lead survived — ChiNext 50/200 trend timing — measured with the harness the infrastructure track was built to replace. The question is whether it survives the failure modes that killed the others.

## Headline

<!-- BEGIN GENERATED: headline -->
| Arm | CAGR | Sharpe | maxDD |
|---|---|---|---|
| ChiNext 50/200 timed | +11.55% | 0.72 | -37.86% |
| Buy & hold | +9.07% | 0.44 | -69.22% |

Exposure 32%, 7.8 side-trades/year, 5 bps/side, cash 1.8%, ChiNext dividend yield 0.5%.
Fill: signal from close[t] governs t+1; an entry day earns open->close, not the full close-to-close move.
<!-- END GENERATED: headline -->

## Kill tests

<!-- BEGIN GENERATED: kills -->
| Kill test | Result |
|---|---|
| K1 window selection | cuts drawdown **44/44** start quarters; beats B&H Sharpe **44/44**, worst +0.04, median +0.21 |
| K2 parameter grid | cuts drawdown **29/29** cells; 50/200 at the **79%** percentile (best cell 30/300 at 0.92 — a plateau, not a peak; do not switch to the maximum) |
| K3 CSI300 | timed +3.75% / 0.39 vs B&H +4.55% / 0.32 |
| K3 CSI500 | timed +4.69% / 0.41 vs B&H +5.89% / 0.36 |
| K3 ChiNext | timed +11.55% / 0.72 vs B&H +9.07% / 0.44 |
| Post-hoc split (2018-07-03) | first +12.82%, second +10.41% — POST-HOC split, not a sealed holdout |
| 2010-05..2014-01 | timed +13.25% / 0.82 vs B&H +11.30% / 0.51 — a window the original cached study could not see; it is out-of-cache, NOT a preregistered holdout |
<!-- END GENERATED: kills -->

## Dividend sensitivity

Index series are price-return. Dividends accrue to the holder, so buy & hold earns the full yield while the timed arm earns it only while invested.

<!-- BEGIN GENERATED: breakeven -->
| Index | CAGR break-even yield | Assumed actual yield |
|---|---|---|
| CSI300 | 1.40% | 2.5% |
| CSI500 | 0.00% | 1.5% |
| ChiNext | 3.85% | 0.5% |

Assumed yields are stated inputs, not measured from the data; the break-even column is what the result is sensitive to.
<!-- END GENERATED: breakeven -->

## Cost sensitivity

<!-- BEGIN GENERATED: costs -->
| bps/side | CAGR | Sharpe |
|---|---|---|
| 0 | +11.98% | 0.75 |
| 5 | +11.55% | 0.72 |
| 10 | +11.11% | 0.70 |
| 20 | +10.25% | 0.66 |
| 50 | +7.68% | 0.52 |
<!-- END GENERATED: costs -->

## What the edge is

A **drawdown and Sharpe overlay on one index** — not stock selection, and not a broad return engine. It re-enters late, so it gives up a substantial share of sharp recoveries in exchange for avoiding a substantial share of crashes. On CSI300 and CSI500 it costs return and buys only drawdown reduction.

## Limits

1. Not a test of stock selection.
2. Index-level, so it needs none of the Tasks 1-5 PIT machinery; that machinery remains required for any selection claim.
3. ETF implementation is unmodelled: tracking error, premium/discount and real spreads on 159915 are not in these numbers.
4. One index, one country, one history. The post-hoc split is not a sealed holdout, and the 2010-2014 window is out-of-cache rather than preregistered.
5. Dividend and cash yields are stated assumptions, not measurements.

## Provenance

<!-- BEGIN GENERATED: provenance -->
- analysis hash `38d831a6d7e871893f203471f87d389b5f586cc8d8f2bf1c53e89533b8107d47`
- capture grade `TRUSTED_HISTORICAL_ASSUMPTION` (historical_tushare_trusted_assumption)
- CSI300 `000300.SH` 4275 rows 20090105..20260810 sha256 `436d70ea656612c1…`
- CSI500 `000905.SH` 4275 rows 20090105..20260810 sha256 `faf2b3dd39b16d3e…`
- ChiNext `399006.SZ` 3933 rows 20100531..20260810 sha256 `c2d6333a33cbbbf3…`
<!-- END GENERATED: provenance -->

## Paper sleeve

`scripts/chinext_timing_paper_sleeve.py` records the rule daily, append-only and idempotent, using the same executable fill as this study — an equivalence enforced by `tests/test_chinext_timing_sleeve.py`. Rows up to and including the inception session are `BACKFILLED_FROM_HISTORY`; only sessions appended after it are `FORWARD_PAPER`. It places no orders and sends no alerts.
