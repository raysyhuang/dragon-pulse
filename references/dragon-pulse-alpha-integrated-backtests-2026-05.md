# Dragon Pulse alpha integrated backtests — 2026-05

## 2026-05-26 — full 1Y top1000 alpha-only integrated limit-touch

**Label:** `full1y_top1000_alpha_integrated_limit_touch_20260526`  
**Run completed:** 2026-05-26T09:12:24Z  
**Primary config:** `outputs/backtest_alpha_research/alpha_enabled_research.yaml`  
**Command observed in log:**

```bash
python scripts/backtest_1yr.py \
  --start 2025-05-26 --end 2026-05-25 \
  --config outputs/backtest_alpha_research/alpha_enabled_research.yaml \
  --label full1y_top1000_alpha_integrated_limit_touch_20260526 \
  --top-n 2 \
  --acceptance-mode off \
  --universe-source market_cap \
  --out-dir outputs/backtest_alpha_integrated_full \
  --engines alpha_only \
  --entry-mode limit_touch
```

### Parameters / scope

- Period: 2025-05-26 to 2026-05-25, 261 trading days scanned.
- Universe: top 1000 A-shares by market cap.
- Engines: `alpha_only` integrated candidates:
  - `alpha_rs_pullback`
  - `alpha_sniper_breakout`
- Exit mode: `target_stop`.
- Entry mode: `limit_touch`.
- MR target mode: `sma5`.
- Gap filter: ON.
- Sniper trailing: ON.
- Acceptance mode: off.
- Regimes: all allowed; none excluded.
- Score floor: disabled.

### Event-level backtest summary

| Metric | Value |
|---|---:|
| Total picks evaluated | 499 |
| No-chase skipped | 10 |
| PnL win rate | 48.5% |
| Target hit rate | 25.7% |
| Avg win / avg loss | +7.46% / -4.07% |
| True expectancy | +1.52% |
| Day-1 stops | 49 (9.8%) |
| Hold-expired positive | 62.2% |
| Profitable days | 62.3% |
| Zero-pick days | 4 (1.5%) |
| Avg picks / active day | 1.9 |
| Event max drawdown | 40.46% |
| Event final equity multiple | 42.08x |

### Engine breakdown

| Engine | n | WR | Target HR | Expectancy | Day-1 stops | Hold-expired positive |
|---|---:|---:|---:|---:|---:|---:|
| alpha_rs_pullback | 404 | 48.5% | 29.7% | +1.73% | 44 | 60% |
| alpha_sniper_breakout | 95 | 48.4% | 8.4% | +0.61% | 5 | 67% |

### Regime breakdown

| Regime | n | WR | Expectancy | Day-1 stops |
|---|---:|---:|---:|---:|
| bull | 283 | 53.7% | +2.44% | 28 |
| bear | 121 | 36.4% | +0.08% | 16 |
| choppy | 95 | 48.4% | +0.61% | 5 |

### Capital-constrained portfolio analyses

1. **Decision-grade MTM replay** (`scripts/portfolio_sim.py`, max 5 positions, highest-score first, equal 20% slot sizing, daily close MTM):
   - Trades simulated: 389 of 499; skipped for capacity: 77.
   - Final equity: 382,489.62 from 100,000 initial.
   - Return: +282.49%; equity multiple: 3.82x.
   - Annualized return: +304.29%.
   - Max DD: 11.45% on 2025-09-04.
   - Sharpe: 3.80.
   - Avg active positions: 4.2; max positions: 5; utilization: 100%.

2. **Spike-compatible modeled-PnL replay** (`spikes/001-alpha-tournament-reset/analyze_tournament.py` logic, max 5 positions, 35% daily budget, 17.5% per-trade cap, synthetic variant labels):
   - Integrated alpha-only: 377 trades / 499 available, +156.54%, max DD -13.90%, Sharpe 3.66, final equity 256,542.48, expectancy +1.519%.
   - Integrated RS pullback only: 325 / 404, +160.38%, max DD -12.11%, Sharpe 4.19, final equity 260,383.96, expectancy +1.732%.
   - Integrated sniper breakout only: 81 / 95, +8.97%, max DD -4.40%, Sharpe 1.58, final equity 108,965.82, expectancy +0.614%.

### Comparison to spike/tournament baselines

Relevant tournament references:

- `rs_pullback_nonchoppy_top2_hi`: event expectancy +1.822%, WR 50.26%, event trades 386; spike-compatible portfolio +166.33%, max DD -6.84%, Sharpe 4.49.
- `rs_pullback_all_top2`: event expectancy +1.523%, WR 47.54%, event trades 509; spike-compatible portfolio +139.51%, max DD -9.67%, Sharpe 3.30.
- `sniper_breakout_all_top2`: event expectancy +1.697%, WR 50.25%, event trades 408; spike-compatible portfolio +137.45%, max DD -7.12%, Sharpe 4.14.

Integrated result vs tournament:

- Integrated event expectancy (+1.52%) matches `rs_pullback_all_top2` (+1.523%) but is below `rs_pullback_nonchoppy_top2_hi` (+1.822%) and `sniper_breakout_all_top2` (+1.697%).
- Integrated modeled-PnL portfolio (+156.54%, Sharpe 3.66) beats `rs_pullback_all_top2` (+139.51%, Sharpe 3.30) but trails `rs_pullback_nonchoppy_top2_hi` on return/drawdown/Sharpe (+166.33%, -6.84%, 4.49).
- Integrated RS-only is very close to tournament best return (+160.38% vs +166.33%) but with worse drawdown (-12.11% vs -6.84%).
- Integrated sniper leg is much weaker than tournament sniper (+8.97%, Sharpe 1.58 vs +137.45%, Sharpe 4.14), likely due implementation/config gating differences or candidate dilution.
- Regime evidence says bear adds almost no alpha in integrated run (+0.08% expectancy, WR 36.4%), while bull is strong (+2.44%).

### Artifacts

- Log: `outputs/backtest_alpha_integrated_full/logs/full1y_top1000_alpha_integrated_limit_touch_20260526.log`
- Summary: `outputs/backtest_alpha_integrated_full/backtest_summary_full1y_top1000_alpha_integrated_limit_touch_20260526.json`
- Detail: `outputs/backtest_alpha_integrated_full/backtest_detail_full1y_top1000_alpha_integrated_limit_touch_20260526.csv`
- Daily: `outputs/backtest_alpha_integrated_full/backtest_daily_full1y_top1000_alpha_integrated_limit_touch_20260526.csv`
- MTM portfolio: `outputs/backtest_alpha_integrated_full/backtest_portfolio_full1y_top1000_alpha_integrated_limit_touch_20260526.json`
- MTM equity curve: `outputs/backtest_alpha_integrated_full/backtest_equity_full1y_top1000_alpha_integrated_limit_touch_20260526.csv`
- Modeled-PnL portfolio: `outputs/backtest_alpha_integrated_full/backtest_portfolio_modeled_full1y_top1000_alpha_integrated_limit_touch_20260526.json`
- Compact summary: `outputs/backtest_alpha_integrated_full/compact_summary_full1y_top1000_alpha_integrated_limit_touch_20260526.json`

### Verdict / caveats

Verdict: **good enough as an integration proof, but not a clean promotion of the combined alpha stack as-is.** RS pullback integration is the real contributor and is near tournament-best; sniper integration is weak and should be gated/retuned before relying on it. A conservative next config should prefer RS pullback, suppress bear exposure, and either disable integrated sniper or require the original tournament-like breakout gates until parity is recovered.

Caveats:

- Backtest is alpha-only with acceptance mode off; live acceptance/risk overlays may change selection and realized capacity.
- Portfolio analyses differ: MTM replay marks open positions with downloaded closes; spike-compatible replay uses modeled trade PnL and calendar-day exit approximation to match tournament analysis style.
- No transaction costs/slippage beyond modeled limit-touch/no-chase behavior are reflected unless already in the harness.
- Top1000 market-cap universe and downloaded data were complete at run time (1000/1000 OHLCV in primary run; 285/285 for portfolio MTM replay).
- MAS is read-only and was not touched.


## 2026-05-26 — full 3Y top1000 RS-only integrated limit-touch

**Label:** `full3y_top1000_alpha_rs_only_limit_touch_20260526`  
**Run completed:** 2026-05-26T13:02:30Z  
**Config:** `outputs/backtest_alpha_research/alpha_rs_only_research.yaml`

### Scope

- Period: 2023-05-26 to 2026-05-25, 782 trading days scanned.
- Universe: top 1000 A-shares by market cap; 1000/1000 OHLCV downloaded.
- Engine: `alpha_rs_pullback` only.
- Entry: `limit_touch` DAY-limit style replay.
- Acceptance: off; regimes all allowed; score floor disabled.
- Top-N: 2/day.

### Event-level result

| Metric | Value |
|---|---:|
| Total evaluated picks | 1,209 |
| No-chase skipped | 22 |
| PnL win rate | 42.1% |
| Target hit rate | 20.2% |
| Avg win / avg loss | +6.18% / -3.67% |
| True expectancy | +0.48% / trade |
| Day-1 stops | 112 |
| Zero-pick days | 166 / 782 = 21.2% |
| Event max DD | 68.72% |
| Event final equity multiple | 11.69x |

### Capital-constrained replay

Spike-compatible modeled-PnL replay:

- Max positions: 5
- Daily budget: 35% NAV
- Per-trade cap: 17.5% NAV
- Trades taken: 920 / 1,209 available
- Return: +144.15%
- Final equity: 244,152.37 from 100,000
- Max DD: -18.13%
- Sharpe: 1.63
- Avg active positions: 4.09

### Robustness notes

By year:

| Year | n | WR | Expectancy |
|---|---:|---:|---:|
| 2023 | 301 | 35.2% | -0.338% |
| 2024 | 375 | 42.7% | +0.253% |
| 2025 | 385 | 44.4% | +0.825% |
| 2026 YTD | 148 | 48.6% | +1.785% |

By regime:

| Regime | n | WR | Expectancy |
|---|---:|---:|---:|
| bear | 734 | 39.0% | +0.015% |
| bull | 475 | 46.9% | +1.187% |

Worst months by expectancy:

- 2025-03: n=20, WR 20.0%, exp -3.831%
- 2023-12: n=42, WR 31.0%, exp -1.526%
- 2025-11: n=30, WR 33.3%, exp -1.476%
- 2024-11: n=18, WR 33.3%, exp -1.386%
- 2024-10: n=31, WR 22.6%, exp -1.136%

### Artifacts

- Log: `outputs/backtest_alpha_integrated_full_3y/logs/full3y_top1000_alpha_rs_only_limit_touch_20260526.log`
- Summary: `outputs/backtest_alpha_integrated_full_3y/backtest_summary_full3y_top1000_alpha_rs_only_limit_touch_20260526.json`
- Detail: `outputs/backtest_alpha_integrated_full_3y/backtest_detail_full3y_top1000_alpha_rs_only_limit_touch_20260526.csv`
- Daily: `outputs/backtest_alpha_integrated_full_3y/backtest_daily_full3y_top1000_alpha_rs_only_limit_touch_20260526.csv`
- Modeled-PnL portfolio: `outputs/backtest_alpha_integrated_full_3y/portfolio_full3y_top1000_alpha_rs_only_limit_touch_20260526.json`

### Verdict

3Y confirms that RS Pullback has real edge versus old DP MR/Sniper, but it is **less robust than the 1Y headline**. The 3Y result is positive and tradeable in research terms (+144% modeled portfolio), but drawdown is higher (-18.13%) and alpha is concentrated in bull/improving periods. Bear regime is roughly flat, not strong.

Recommended production posture:

- Replace old DP primary alpha research core with RS Pullback.
- Keep default live disabled until friction/stress tests are added.
- Prefer bull/improving-regime gating or lower exposure in bear.
- Do not enable integrated Sniper as-is.
- Next required tests: fees/slippage, open-only fill stress, liquidity/capacity filters, max 2–3 positions, and bad-month diagnostics.


## 2026-05-26 — full 3Y top1000 RS Pullback bull-gated integrated limit-touch

**Label:** `full3y_top1000_alpha_rs_bull_gated_limit_touch_20260526`  
**Run completed:** 2026-05-26T15:01:19Z  
**Config:** `outputs/backtest_alpha_research/alpha_rs_bull_gated_research.yaml`

### Scope

- Period: 2023-05-26 to 2026-05-25, 782 trading days scanned.
- Universe: top 1000 A-shares by market cap; 1000/1000 OHLCV downloaded.
- Engine: `alpha_rs_pullback` only.
- Entry: `limit_touch` DAY-limit style replay.
- Acceptance: off; config excluded regimes `bear,choppy` (bull-only candidates); score floor disabled globally.
- Top-N: 2/day.

### Event-level result

| Metric | Bull-gated 3Y | Baseline RS-only 3Y |
|---|---:|---:|
| Total evaluated picks | 471 | 1,209 |
| No-chase skipped | 8 | 22 |
| PnL win rate | 47.4% | 42.1% |
| Target hit rate | 25.5% | 20.2% |
| Avg win / avg loss | +7.25% / -4.20% | +6.18% / -3.67% |
| True expectancy | +1.22% / trade | +0.48% / trade |
| Day-1 stops | 43 | 112 |
| Day-1 stops / evaluated | 9.1% | 9.3% |
| Zero-pick days | 541 / 782 = 69.2% | 166 / 782 = 21.2% |
| Event max DD | 60.47% | 68.72% |
| Event final equity multiple | 17.77x | 11.69x |

### Capital-constrained replay

Spike-compatible modeled-PnL replay convention: initial capital 100,000; daily budget 35% NAV; per-trade cap 17.5% NAV; highest-score first; modeled `exit_day`; no MTM between modeled exits.

| Max positions | Cost | Trades taken / available | Return | Final equity | Max DD | Sharpe | Avg active positions |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 0 bps | 177 / 471 | +39.56% | 139,563.20 | -14.37% | 2.11 | 1.99 |
| 3 | 0 bps | 252 / 471 | +66.76% | 166,755.98 | -17.43% | 2.46 | 2.75 |
| 5 | 0 bps | 380 / 471 | +128.84% | 228,844.76 | -20.78% | 3.15 | 3.96 |
| 2 | 30 bps RT | 177 / 471 | +27.22% | 127,221.85 | -17.43% | 1.57 | 1.99 |
| 3 | 30 bps RT | 252 / 471 | +46.21% | 146,208.50 | -21.82% | 1.87 | 2.75 |
| 5 | 30 bps RT | 380 / 471 | +87.93% | 187,926.62 | -24.13% | 2.45 | 3.96 |

For same replay implementation on baseline 3Y RS-only:

| Max positions | Cost | Trades taken / available | Return | Final equity | Max DD | Sharpe |
|---:|---:|---:|---:|---:|---:|---:|
| 2 | 0 bps | 422 / 1,209 | +34.59% | 134,594.56 | -12.57% | 0.91 |
| 3 | 0 bps | 607 / 1,209 | +66.23% | 166,225.70 | -16.48% | 1.21 |
| 5 | 0 bps | 920 / 1,209 | +144.15% | 244,152.37 | -18.13% | 1.63 |
| 2 | 30 bps RT | 422 / 1,209 | +7.87% | 107,868.38 | -21.20% | 0.30 |
| 3 | 30 bps RT | 607 / 1,209 | +20.95% | 120,948.59 | -25.95% | 0.52 |
| 5 | 30 bps RT | 920 / 1,209 | +51.01% | 151,006.85 | -33.62% | 0.83 |

### Robustness notes

By year for bull-gated picks:

| Year | n | WR | Expectancy |
|---|---:|---:|---:|
| 2023 | 20 | 25.0% | -0.822% |
| 2024 | 144 | 42.4% | -0.131% |
| 2025 | 229 | 47.6% | +1.425% |
| 2026 YTD | 78 | 61.5% | +3.649% |

Worst bull-gated months by expectancy:

- 2026-03: n=2, WR 0.0%, exp -6.380%
- 2025-03: n=18, WR 22.2%, exp -3.572%
- 2025-11: n=14, WR 21.4%, exp -3.314%
- 2025-02: n=6, WR 16.7%, exp -2.500%
- 2023-08: n=16, WR 18.8%, exp -1.800%

### Comparison to baseline 3Y RS-only

- Bull-gating sharply improves event quality: expectancy rises from +0.48% to +1.22%, win rate from 42.1% to 47.4%, and target hit rate from 20.2% to 25.5%.
- It cuts candidate count by 61% (1,209 to 471) and creates many more no-trade days (69.2% zero-pick days), so capital deployment is lower.
- In capital replay, bull-gating improves risk-adjusted quality materially: max-5 Sharpe 3.15 vs 1.63 uncosted, and 2.45 vs 0.83 under 30 bps stress.
- Absolute max-5 uncosted return is slightly lower than baseline (+128.84% vs +144.15%) because fewer trades are available, but stressed return is much stronger (+87.93% vs +51.01%) and stressed drawdown is smaller (-24.13% vs -33.62%).
- Max 2/3 position variants are viable: max-2 already beats baseline max-2 return after costs (+27.22% vs +7.87%) with far higher Sharpe (1.57 vs 0.30); max-3 gives +46.21% under 30 bps with Sharpe 1.87.
- Residual weakness remains: 2023 and 2024 bull-labeled samples are flat/negative, and bad months still cluster. The gate improves robustness but does not eliminate regime-label or timing risk.

### Artifacts

- Log: `outputs/backtest_alpha_integrated_bull_gated_3y/logs/full3y_top1000_alpha_rs_bull_gated_limit_touch_20260526.log`
- Summary: `outputs/backtest_alpha_integrated_bull_gated_3y/backtest_summary_full3y_top1000_alpha_rs_bull_gated_limit_touch_20260526.json`
- Detail: `outputs/backtest_alpha_integrated_bull_gated_3y/backtest_detail_full3y_top1000_alpha_rs_bull_gated_limit_touch_20260526.csv`
- Daily: `outputs/backtest_alpha_integrated_bull_gated_3y/backtest_daily_full3y_top1000_alpha_rs_bull_gated_limit_touch_20260526.csv`
- Modeled-PnL portfolio: `outputs/backtest_alpha_integrated_bull_gated_3y/portfolio_full3y_top1000_alpha_rs_bull_gated_limit_touch_20260526.json`

### Verdict

Bull-gated RS Pullback is a clear robustness improvement over 3Y RS-only, especially once 30 bps round-trip friction is applied. It is strong enough to start **daily paper trading**, but not yet strong enough for automatic capital deployment: use bull-only, RS Pullback only, no integrated Sniper, max 2–3 positions, and record fills/slippage before considering live enablement. MAS was not touched.
