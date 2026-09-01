# Opening the choppy regime to rs_pullback — 2026-08-20

## Question

Since 2026-07-03 the scanner emitted no picks for 35 consecutive sessions. Is the
regime gate correctly abstaining, or is it structurally too narrow?

## Method

Offline replay of `outputs/backtest/pit5y_final` inputs over 2021-08-12..2026-08-11
(1210 sessions): frozen PIT membership schedule, frozen basic_info, cached price
snapshots. No provider calls, so runs are deterministic. The frozen schedule avoids
current-universe survivorship drift but remains a trusted historical membership
assumption rather than provider-attested point-in-time truth.

Two layers bind under the production `acceptance.enabled: true` path; the book layer is
kept aligned as defense-in-depth for acceptance-off tooling:

1. `alpha_candidates.rs_pullback.regimes`   — engine
2. `acceptance.excluded_regimes`            — funnel
3. `book_size.{regime}.max_picks/min_score` — acceptance-off defense only

Note `scripts/backtest_1yr.py:823` unions CLI `--excluded-regimes` with the config
value, so the CLI flag alone cannot widen the gate.

## Result

| gate | active days | filled | win% | exp% | portfolio equity | portfolio maxDD | Sharpe | capacity skipped |
|---|---|---|---|---|---|---|---|---|
| bull only (frozen baseline) | 322 | 616 | 41.6 | +0.39 | 1.604x | 33.35% | 0.684 | 70 |
| **bull + choppy (paper candidate)** | **559** | **1071** | **42.6** | **+0.49** | **2.259x** | **22.90%** | **0.895** | **148** |

Portfolio figures use 20% of equity per position and max 5 concurrent positions. The
bull+choppy ledger executes 923/1,071 filled replay trades; the remaining 148 are
mechanically skipped at the concurrency cap. Unlike per-trade compounding, these are
daily marked-to-market portfolio figures.

Per-regime expectancy, measured separately:

| regime | n | win% | gross | net @0.15% | net @0.25% | net @0.40% |
|---|---|---|---|---|---|---|
| choppy | 455 | 44.0 | +0.62 | +0.47 | +0.37 | +0.22 |
| bull | 616 | 41.6 | +0.39 | +0.24 | +0.14 | -0.01 |

Choppy improves the frozen bull-only baseline. Bear is not widened by this change and
stays excluded; the committed adoption replay intentionally tests only the proposed
bull+choppy policy rather than manufacturing an all-regime comparator.

## Robustness

Stability across years and split-sample — choppy is the *more* robust of the two:

| | years positive | split H1 | split H2 |
|---|---|---|---|
| choppy | 5/6 | +0.627% | +0.616% |
| bull | 3/6 | -0.551% | +1.328% |

Bull's entire edge sits in the recent half. Choppy's halves are near-identical.

Score-floor sensitivity on the binding parameter (`rs_pullback.score_floor`):

| floor | filled | gross | net @0.15% | equity | choppy-only |
|---|---|---|---|---|---|
| 85 | 1074 | +0.47 | +0.32 | 6.62x | +0.62% |
| 90 (adopted) | 1071 | +0.49 | +0.34 | 6.87x | +0.62% |
| 95 | 1043 | +0.48 | +0.33 | 6.14x | +0.55% |

Flat across +/-5 points; every variant beats bull-only. `book_size.min_score` below
90 is inert because the engine floor clamps first. With acceptance enabled, the binding
participation caps are `acceptance.max_full/max_selective: 2`, not the regime book entry.

## What this does NOT fix

The operational drought observation through 2026-08-20 extends beyond the frozen replay
window ending 2026-08-11 and is context only, not part of the replay evidence. Opening
choppy does not solve prolonged bear abstention; bear remains deliberately blocked.

## Rejected: breadth as an OR term

`scripts/breadth_regime_study.py` tested adding breadth to widen the gate. Its own
crux test kills it: the 288 extra days average +1.1bps, entirely from 9 days in
2024 (+259.7bps). Ex-2024 the extra days average **-7.3bps** and 8 of 12 years are
negative. Not adopted.

## Caveats

- **Costs are not modelled** in `scripts/backtest_1yr.py` — no stamp duty, no
  commission, no slippage. All gross figures above carry net columns at an assumed
  0.15% A-share round trip (0.10% stamp sell-side + ~0.025%/side commission).
- `per_trade_compounded_dd_pct` compounds every trade at full size; it is a relative
  comparison between variants, not a portfolio drawdown.
- Choppy inherited bull's `min_score` and `max_picks` rather than being tuned
  separately. Deliberate (avoids fitting) but means +0.62% is unoptimised.
- Frozen membership is reproducible but remains a `TRUSTED_HISTORICAL_ASSUMPTION`, not
  provider-attested point-in-time truth.

## Machine-readable evidence

The committed `outputs/backtest/pit5y_bull_choppy_evidence/` directory contains the
fresh offline replay summary/detail/daily files, pinned picks, portfolio summary and
daily equity curve. `evidence_manifest.json` binds the exact code commit, config, four
frozen input classes, output hashes and commands. The snapshot archive is published at
the URL in the manifest rather than duplicated in git.

## Follow-up: participation dials are already optimal (2026-08-22)

`book_size.{regime}.max_picks` is **dead config** while `acceptance.enabled: true` —
`src/pipelines/funnel.py:474-477` only consults it in the `else` branch when
acceptance is off. The dials that actually bind are `acceptance.max_full` /
`max_selective` (2) and `book_size.max_per_sector` (1).

Tested on top of the adopted bull+choppy gate, same 5y PIT replay:

| variant | active | filled | pk/day | gross | net @0.15% | equity | maxDD% |
|---|---|---|---|---|---|---|---|
| 2 picks / sector 1 (adopted) | 559 | 1071 | 1.92 | +0.49 | +0.34 | 6.87x | 57.7 |
| 3 picks / sector 1 | 559 | 1612 | 2.88 | +0.33 | +0.18 | 3.34x | 61.0 |
| 4 picks / sector 1 | 559 | 2140 | 3.83 | +0.28 | +0.13 | 2.83x | 72.3 |
| 2 picks / sector 2 | 559 | 1073 | 1.92 | +0.46 | +0.31 | 6.67x | 58.6 |

Marginal-trade test — do the ADDED trades pay for themselves?

| change | n extra | gross | net @0.15% | win% |
|---|---|---|---|---|
| 2 -> 3 picks | 541 | +0.00% | -0.15% | 40.9 |
| 2 -> 4 picks | 1069 | +0.08% | -0.07% | 41.1 |
| sector 1 -> 2 | 27 | -0.49% | -0.64% | 33.3 |

Every widening is flat-to-negative gross and negative net. Keep `max_full: 2`,
`max_selective: 2`, `max_per_sector: 1`.

Read positively: picks #3 and #4 are materially worse than #1 and #2, which is
evidence the composite score genuinely ranks rather than shuffles. The only
participation gain available was the choppy regime itself.
