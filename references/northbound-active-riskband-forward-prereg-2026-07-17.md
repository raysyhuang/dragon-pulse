# Northbound Active Risk-Band Paper Sleeve — Forward Pre-registration

**Registered:** 2026-07-17 UTC  
**Status:** paper-track only. This document cannot promote the sleeve to a live signal, alert, ranking input, or trade plan.

## Scope and observation window

- **Sleeve:** `northbound_active_riskband_paper` — northbound active-list rank ≤5, 4–7% stop-risk bracket, T+1 no-chase cap, 5-trading-day bracket observation.
- **Start:** the first paper watchlist generated after `c661d4d` (the native regime-stamping release). Earlier artifacts, including 2026-07-16, are retained only as explicitly marked `backfilled` history and are excluded from the native-stamp integrity denominator.
- **Readout:** at the later of (a) 20 completed A-share trading sessions from the start or (b) 8 eligible picks whose 5-session observation window has matured. Open positions are reported but do not count as mature outcomes.
- **No parameter changes:** rank rule, 4–7% risk band, no-chase cap, bracket levels, source-date rule, and holding period are frozen for this window. Any operational repair must be separately labelled and must not be used to reinterpret prior outcomes.

## Data and attribution gates

1. **Native stamp integrity.** Report the count and share of new watchlists with `regime_stamp_status == "ok"`, plus missing/unavailable counts. Target: at least 95% native usable stamps. A backfill is never counted as native.
2. **Publication lag.** For every pick, record `source_trade_date`, `signal_date`, and completed A-share-session lag. One completed-session lag is expected for a pre-open T+1 test; report all larger/unknown lags separately.
3. **Lag cost.** For every open-check result with a reported open, compute
   `open_drift_pct = (open_price / entry_price - 1) × 100` and
   `open_drift_R = (open_price - entry_price) / (entry_price - stop_loss)`.
   Report median/mean drift, and counts/means split into gap-up (`>0`), flat (`=0`), and gap-down (`<0`). This is descriptive and never alters a historical fill decision.

## Execution accounting

- **Eligible pick:** a watchlist pick with an open-check result and usable source/entry/bracket fields.
- **Fill:** `GO` or `WARN` only when the actual next-open price is at or below the recorded no-chase cap. A `CANCEL` caused by opening above the cap is a **skip**, not a loss and not part of realised bracket statistics.
- **Fill rate:** filled eligible picks / eligible picks, reported with the numerator and denominator.
- **Skipped-entry counterfactual:** every no-chase `CANCEL` receives a separate, explicitly non-realised 5-session replay using the actual next open as a forced hypothetical fill and the originally recorded absolute stop/target levels. It is tagged `counterfactual_only` and is excluded from the realised fill, hit-rate, R, and promotion statistics. Report its bracket outcomes alongside filled outcomes to detect adverse selection by the cap.

## Amendment 1 — 2026-07-17 UTC, before the first native-stamped pick matures

The original same-bar convention was target-first because it mirrored the then-current `evaluate_pick` branch. That engine ordering is inconsistent with its runner branch's conservative intrabar policy and is result-flattering for a kill gate. This amendment supersedes that convention **before the forward readout has mature native observations**.

- **Same-bar convention:** **stop-first**. When a daily bar has `high >= target_1` and `low <= stop_loss`, record `same_bar_stop_first`, use the stop as the terminal exit, and include its realised negative R in the kill-gate median. Report the same-bar count separately in every readout.
- **Backtest gate:** the current `scripts.backtest_1yr.evaluate_pick(..., exit_mode="target_stop")` target-first ordering is not promotion-valid. It must be repaired to this stop-first rule, with regression coverage for T+1 timing and gap-through-stop behavior, before any backtest result can support promotion. Until then it is research context only.

## Bracket convention and outcomes

- **Forward ledger convention:** stop-first as fixed by Amendment 1 above; it does not inherit the current target-first `evaluate_pick` defect.
- **Canonical evaluator for promotion:** only a repaired `scripts.backtest_1yr.evaluate_pick` whose tested semantics match this document may be used for PIT/backtest promotion evidence.
- **Terminal outcome:** `target_hit`, `stop_hit`, `hold_expired`, or `same_bar_stop_first`; unresolved/no-data cases remain separate. `target-1-before-stop` and `stop-before-target-1` rates exclude no-data outcomes and state their denominator.
- **R multiple:** for a filled, terminal observation, `(exit_price - actual_entry_price) / (actual_entry_price - stop_loss)`. Time exits use their 5-session close. Counterfactual outcomes use the same formula but remain outside realised statistics.

## Pre-committed decisions

The 2–4 week readout is **not promotion evidence**. Promotion still requires the existing PIT-universe, bias-controlled backtest/walk-forward, and regime-sliced forward evidence.

### Kill

Kill the sleeve (stop new paper entries; preserve artifacts) if, after at least 8 mature, non-counterfactual filled observations:

- **6 or more of the first 8** terminal filled observations are `stop_hit` or `same_bar_stop_first` before `target_1`; **or**
- median realised terminal R is **≤ -0.50R**.

Same-bar events use the stop-first convention in Amendment 1 above. No-chase skips and no-data observations cannot be counted as stop losses.

### Extend

Extend the paper window, without promotion, if data integrity is adequate and the kill criteria are not met but the sample remains too small or outcome confidence is mixed. The readout must still publish all gates, denominators, regime splits, skipped-entry counterfactuals, and lag-cost metrics.

### Integrity hold

Pause new interpretation and diagnose the pipeline if native usable regime stamps are below 95%, if an artifact is unlabelled/reconstructed, or if source lag is unknown for any purportedly filled observation. Existing artifacts remain immutable evidence.

### Promotion

Not available from this forward window. Do not convert a positive 2–4 week result into a live or alerting rule.

## Required readout table

For the full sample and each available `regime_at_signal` group, publish:

1. watchlists, eligible picks, fills, skips, no-data, and all denominators;
2. stamp status / source-date lag counts;
3. open drift percentage and R, including gap-up/flat/gap-down splits;
4. target, stop, time, same-bar-stop-first, and unresolved counts;
5. realised R distribution (median, mean, p10, p90) and the pre-registered decision;
6. skipped-entry counterfactual outcomes, clearly separated from realised outcomes.
