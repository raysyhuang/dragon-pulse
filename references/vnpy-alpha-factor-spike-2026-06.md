# vn.py Alpha Factor Spike — A-share 3–7D Candidate Shortlist

Status: **research note only**. Do **not** import `vnpy` or route any factor here into live Dragon Pulse ranking/alerts without PIT-clean bracket replay.

Source inspected: `vnpy/vnpy` 4.4.0, especially:

- `vnpy/alpha/dataset/datasets/alpha_158.py`
- `vnpy/alpha/dataset/datasets/alpha_101.py`

Decision: **spike the formulas, not the package**. `vnpy.alpha` is useful as a Qlib/WorldQuant-style factor idea catalog, but its package/backtester/dependency tree is not a fit for Dragon Pulse's A-share single-name, T+1 no-chase, bracket-replay workflow.

## Guardrails

1. No `vnpy` dependency in Dragon Pulse for this spike.
2. Hand-port selected formulas into our own feature layer if/when tested.
3. All tests must use Dragon Pulse replay discipline:
   - PIT-ish universe / no survivorship shortcuts where possible.
   - Signal only from data known at signal close.
   - Conservative T+1 no-chase fill.
   - Bracket stop/target replay, not only close-exit returns.
   - Walk-forward / holdout by date boundary.
4. Use these first as **filters/rankers** on existing candidates or paper sleeves, not standalone live engines.
5. Any positive result remains research/paper-only until it beats RS Pullback and survives Hawk review.

## Candidate factors to port first

### A. Pullback / reversal quality

These are most aligned with Dragon Pulse's current RS Pullback and A-share 3–7D mean-reversion/continuation horizon.

| ID | Source | Working name | Formula sketch | Rationale | First test |
|---|---|---|---|---|---|
| F01 | Alpha158 `rsv_w` | `rsv_20` / `rsv_60` | `(close - rolling_min(low,w)) / (rolling_max(high,w)-rolling_min(low,w))` | Measures location inside recent range. Useful to distinguish controlled pullback vs breakdown. | Filter RS Pullback: require 20D RSV not too high and 60D RSV still constructive. |
| F02 | Alpha158 `rank_w` | `ts_rank_close_20` | rolling percentile rank of close over 20D | Simpler range-location factor; low-mid rank in strong stock may be better than fresh high chase. | Rank RS Pullback candidates by lower 20D rank but positive 60D RS. |
| F03 | Alpha101 `alpha9/10` | `short_reversal_delta` | conditional sign flip based on 1D close delta extremes over 4–5D | Captures short-horizon exhaustion/reversal. | Test as bearish-chase veto after large 1–5D runups. |
| F04 | Alpha101 `alpha46/49/51` | `slope_deceleration_20_10` | compare 20D→10D slope vs 10D→0D slope | Detects trend deceleration / pullback phase. | Use as RS Pullback quality score; avoid accelerating downside. |

### B. Volume-price confirmation

These are likely useful as **filters**, because many Dragon Pulse false positives come from weak volume acceptance or crowded/high-turnover failure.

| ID | Source | Working name | Formula sketch | Rationale | First test |
|---|---|---|---|---|---|
| F05 | Alpha158 `corr_w` | `price_volume_corr_10/20` | rolling corr(`close`, log(`volume`+1), w) | Positive price-volume confirmation may help trend continuation; negative/unstable corr may mark distribution. | Add to RS Pullback and northbound paper candidate ranking. |
| F06 | Alpha158 `cord_w` | `return_volume_corr_10/20` | rolling corr(close return, log(volume ratio), w) | Better than raw price-volume corr for short horizon acceptance. | Filter breakouts: require non-negative return/volume corr. |
| F07 | Alpha101 `alpha2` | `volume_delta_candle_corr_6` | `-corr(rank(delta(log(volume),2)), rank((close-open)/open), 6)` | Detects whether volume expansion is aligned with candle body direction. | Test as failed-breakout / exhaustion veto. |
| F08 | Alpha101 `alpha12` | `volume_price_divergence_1` | `sign(delta(volume,1)) * -delta(close,1)` | Simple divergence: volume up while price down is bad; volume up while price up is supportive depending sign convention. | Use as one-day penalty/bonus in paper ranking. |
| F09 | Alpha101 `alpha43` | `rvol_rank_x_reversal_rank` | rank(volume/vol20,20) × rank(-delta(close,7),8) | Combines volume abnormality with 7D pullback. | Test as RS Pullback boost: high rvol + controlled pullback. |

### C. Range / candle structure

These are cheap to compute and match the A-share problem of avoiding gap/limit-up/chase artifacts.

| ID | Source | Working name | Formula sketch | Rationale | First test |
|---|---|---|---|---|---|
| F10 | Alpha158 `kmid/klen/ksft` | `candle_body_range_pack` | body `(close-open)/open`, range `(high-low)/open`, shift `(2*close-high-low)/open` | Candle quality: close location and range shape. | Veto wide red candle / weak close on candidate day. |
| F11 | Alpha158 `kup/klow` | `upper_lower_shadow_pack` | upper/lower shadow ratios | Long upper shadow after rank signal often means rejection. | Add upper-shadow penalty to breakout/northbound paper. |
| F12 | Alpha101 `alpha101` | `body_to_range` | `(close-open)/(high-low+eps)` | One-line close location factor, robust and interpretable. | Require positive or non-terrible close location for continuation candidates. |
| F13 | Alpha101 `alpha53/60` | `intraday_pressure_delta` | delta/rank of close-within-range pressure × volume | Accumulation/distribution pressure. | Test as smart-money-like factor without relying on LHB/netflow. |

### D. Trend residual / smoothness

These may help distinguish clean RS leaders from noisy crowded names.

| ID | Source | Working name | Formula sketch | Rationale | First test |
|---|---|---|---|---|---|
| F14 | Alpha158 `beta_w/rsqr_w/resi_w` | `trend_slope_rsquare_residual_20/60` | rolling linear slope, R², residual / close | Strong but smooth trend should have positive slope, decent R², controlled residual. | Improve RS Pullback trend quality and avoid parabolic/choppy names. |
| F15 | Alpha158 `wvma_w` | `volume_weighted_volatility_20` | std(abs(return)*volume) / mean(abs(return)*volume) | Measures noisy volume-weighted volatility. | Penalize unstable crowded moves in northbound paper and breakout candidates. |

## First implementation batch

Keep the first code spike small. Implement only these 8 columns first because they are interpretable and cheap:

1. `rsv_20`
2. `ts_rank_close_20`
3. `return_volume_corr_10`
4. `body_to_range`
5. `upper_shadow_pct`
6. `lower_shadow_pct`
7. `trend_rsq_20`
8. `trend_resid_pct_20`

Avoid the full Alpha101 set initially. Many formulas are complex, cross-sectional, or easy to overfit.

## Validation plan

### Phase 1 — feature-only smoke

- Add features to `src/features/technical.py` behind normal technical feature computation.
- Unit test on synthetic OHLCV to prove no future shifts.
- Ensure features are finite/NaN-safe and do not mutate existing columns unexpectedly.

### Phase 2 — filter/ranker replay

Use pinned input sets; do not let candidate selection drift between variants.

Candidate replay surfaces:

1. RS Pullback candidates:
   - baseline RS Pullback
   - RS Pullback + candle close-location veto
   - RS Pullback + return-volume corr filter
   - RS Pullback + trend smoothness filter
2. Northbound paper sleeve:
   - current active-rank riskband
   - + candle close-location ranker
   - + return-volume corr ranker
   - + rsv/ts-rank pullback filter
3. Disabled breakout research candidates:
   - only as paper/research variants; do not enable live.

### Phase 3 — gates

Minimum gate before any broader use:

- Same or better event count quality; no tiny-sample artifact.
- Better bracket PF and average than baseline on holdout.
- No worse p10 / tail loss.
- Better one-pick-per-day capacity-realistic result, not only all-events result.
- Hawk/Opus read-only review before config exposure.

## Expected base rate

Most factors should fail. Success means finding one or two robust **filters** that reduce bad fills/tail losses, not discovering a standalone Sniper replacement.

## Do-not-do list

- Do not import `vnpy` into Dragon Pulse.
- Do not add PySide6/ta-lib/torch/lightgbm just for this spike.
- Do not run LightGBM on Alpha158 and promote high in-sample results.
- Do not replace Dragon Pulse's bracket replay with vn.py's backtester.
- Do not route these features into live ranking until replay gates pass.
