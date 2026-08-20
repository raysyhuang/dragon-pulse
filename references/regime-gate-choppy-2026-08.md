# Opening the choppy regime to rs_pullback — 2026-08-20

## Question

Since 2026-07-03 the scanner emitted no picks for 35 consecutive sessions. Is the
regime gate correctly abstaining, or is it structurally too narrow?

## Method

Offline replay of `outputs/backtest/pit5y_final` inputs over 2021-08-12..2026-08-11
(1210 sessions): frozen PIT membership schedule, frozen basic_info, cached price
snapshots. No provider calls, so runs are deterministic and survivorship-clean.

Three gate layers independently zero out a regime and all three must open together:

1. `alpha_candidates.rs_pullback.regimes`   — engine
2. `acceptance.excluded_regimes`            — funnel
3. `book_size.{regime}.max_picks/min_score` — book

Note `scripts/backtest_1yr.py:823` unions CLI `--excluded-regimes` with the config
value, so the CLI flag alone cannot widen the gate.

## Result

| gate | active days | filled | win% | exp% | cum PnL% | equity | maxDD% |
|---|---|---|---|---|---|---|---|
| bull only (previous production) | 322 | 616 | 41.6 | +0.39 | 138.6 | 2.39x | 78.0 |
| **bull + choppy (adopted)** | **559** | **1071** | **42.6** | **+0.49** | **586.6** | **6.87x** | **57.7** |
| all regimes | 1144 | 2197 | 40.0 | +0.15 | 63.7 | 1.64x | 88.2 |

Per-regime expectancy, measured separately:

| regime | n | win% | gross | net @0.15% | net @0.25% | net @0.40% |
|---|---|---|---|---|---|---|
| choppy | 455 | 44.0 | +0.62 | +0.47 | +0.37 | +0.22 |
| bull | 616 | 41.6 | +0.39 | +0.24 | +0.14 | -0.01 |
| bear | 1118 | 37.5 | -0.17 | -0.32 | -0.42 | -0.57 |

Choppy is the engine's best regime and was the one excluded. Bear is negative
before costs and stays excluded.

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
90 is inert because the engine floor clamps first, and fill count barely moves with
the floor — `max_picks: 2` is the binding constraint.

## What this does NOT fix

Of the 35 drought sessions (2026-07-03..2026-08-20) **34 were bear and one was
choppy** (2026-07-10). Opening choppy adds a single day. The drought is a genuine
bear market; CSI300 fell 4.56% across it and bear expectancy is negative, so
abstaining was correct. The 5y study already put this drought at the 79th
percentile of 47 historical dry spells (median 4 sessions, max 129).

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
- Evidence was produced with the PIT replay harness on
  `fix/backtest-real-trading-calendar` (`--pit-schedule-in`, `--basic-info-in`),
  which is not yet on main. `config/default.yaml` is identical across both branches.
