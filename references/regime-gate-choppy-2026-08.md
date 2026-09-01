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
| **bull + choppy (paper candidate)** | **560** | **1080** | **43.8** | **+0.58** | **2.482x** | **21.01%** | **0.991** | **145** |

Portfolio figures use 20% of equity per position and max 5 concurrent positions. The
bull+choppy ledger executes 935/1,080 filled replay trades; the remaining 145 are
mechanically skipped at the concurrency cap. Unlike per-trade compounding, these are
daily marked-to-market portfolio figures.

Six filled entries opened below their planned stop. The replay enforces A-share T+1 by
holding through the entry session and exiting at the next-session open; it does not book
those cases as same-bar 0.00% exits. Three occurred in BULL and three in CHOPPY.

The prior frozen bull-only portfolio was not regenerated under the same STAR-board
exclusion and is therefore **not a valid like-for-like comparator**. This report makes
no portfolio delta claim versus that baseline; the adoption evidence is the absolute
paper result and the separately positive choppy cohort.

Per-regime expectancy, measured separately:

| regime | n | win% | gross | net @0.15% | net @0.25% | net @0.40% |
|---|---|---|---|---|---|---|
| choppy | 462 | 44.8 | +0.72 | +0.57 | +0.47 | +0.32 |
| bull | 618 | 43.0 | +0.48 | +0.33 | +0.23 | +0.08 |

Choppy is positive within the committed replay. Bear is not widened by this change and
stays excluded; the committed adoption replay intentionally tests only the proposed
bull+choppy policy rather than manufacturing an all-regime comparator.

## Historical exploratory sweeps

Earlier split-sample, score-floor and participation sweeps predated the complete
688xxx/689xxx STAR-board exclusion. They are quarantined as exploratory context and are
not merge evidence or a valid comparator for the regenerated adoption replay. New
sensitivity claims require regeneration under the exact current board filter.

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
  separately. Deliberate (avoids fitting) but means +0.72% is unoptimised.
- Frozen membership is reproducible but remains a `TRUSTED_HISTORICAL_ASSUMPTION`, not
  provider-attested point-in-time truth.

## Machine-readable evidence

The committed `outputs/backtest/pit5y_bull_choppy_evidence/` directory contains the
fresh offline replay summary/detail/daily files, pinned picks, portfolio summary and
daily equity curve. `evidence_manifest.json` binds the exact code commit, config, four
frozen input classes, output hashes and commands. The snapshot archive is published at
the URL in the manifest rather than duplicated in git.
