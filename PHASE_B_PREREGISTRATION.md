# Mean-Reversion Phase B: Preregistration and Preflight

**Status:** frozen planning document; **execution is blocked** until every preflight gate below passes and its evidence is archived. This document does not authorize a Phase B run or a strategy/configuration change.

## Scope and freeze

Phase B is a mean-reversion (MR)-only exit/payoff attribution experiment. The invocation, if unblocked, is the existing `scripts/run_mr_phase_b_ablation.sh` with `--acceptance-mode live_equivalent` and `--engines mr_only`. No candidate, date range, metric, tie-breaker, or promotion rule may be changed after any Phase B outcome is inspected. A change requires a new dated preregistration and a new experiment identifier.

### Frozen candidate set

The only eligible labels/configurations are the baseline resolved from `config/default.yaml` and these partial overlays:

| Label | Overlay | Frozen intended overrides |
| --- | --- | --- |
| `mr_b1_wider_payoff` | `config/experiments/mr_b1_wider_payoff.yaml` | `stop_atr_mult=1.0`, `target_1_atr_mult=2.0`, `target_2_atr_mult=3.0`, `holding_period=4` |
| `mr_b2_asymmetry_max` | `config/experiments/mr_b2_asymmetry_max.yaml` | `stop_atr_mult=0.8`, `target_1_atr_mult=2.0`, `target_2_atr_mult=3.5`, `holding_period=4` |
| `mr_b3_hold_longer` | `config/experiments/mr_b3_hold_longer.yaml` | `holding_period=5` |
| `mr_b4_entry_chase_down` | `config/experiments/mr_b4_entry_chase_down.yaml` | `max_entry_atr_mult=0.1` |
| `mr_b5_best_combo` | `config/experiments/mr_b5_best_combo.yaml` | B1 exit set plus `rsi2_max=4`, `score_floor=70` |

The Phase B exit-key universe is exactly `mean_reversion.stop_atr_mult`, `target_1_atr_mult`, `target_2_atr_mult`, `max_entry_atr_mult`, and `holding_period`. `mr_b5_best_combo` is confirmatory only: it may be considered only after the full-sample binding decision for B1–B4 is recorded, and it cannot replace a failed B1–B4 binding test. The runner currently enumerates B1–B4; adding B5 to a future execution plan requires a separately archived runner-manifest hash, not an unrecorded ad hoc command.

## Blocking preflight: configuration, activation, and wiring

Run this preflight before any Phase B backtest. Each assertion is a hard gate, not a warning.

1. **Explicit MR activation.** Every candidate's *resolved* configuration must contain `mean_reversion.enabled: true`; the baseline must be resolved under the same explicit activation. `--engines mr_only` is necessary but is not evidence that the MR engine was enabled. The current default has `mean_reversion.enabled: false`, so the current candidate set is not executable under this preregistration without a separately reviewed configuration update.
2. **Explicit exit input.** Before execution, each candidate manifest must record an explicit resolved value for all five exit keys above, including values inherited from the baseline. Missing, null, unparsable, or default-only/unproven values fail preflight. The candidate overlay and the resolved configuration must both be retained.
3. **Engine wiring assertion.** A deterministic fixture must call the actual MR scoring path used by the backtest and prove, one key at a time, that changing each exit key changes the corresponding emitted signal field: `stop_atr_mult → stop_loss`; `target_1_atr_mult → target_1`; `target_2_atr_mult → target_2`; `max_entry_atr_mult → max_entry_price`; `holding_period → holding_period`. The fixture must use conditions where target fallback is active, so SMA-derived targets cannot mask target-key changes. Record expected and observed values, source revision, and fixture hash.
4. **Backtest wiring assertion.** On the same fixture, prove the runner evaluates the emitted `stop_loss`, `target_1`, `target_2`, `max_entry_price`, and `holding_period`, rather than re-reading a hardcoded/default exit. Store the trace or test output. The selected `exit_mode`, runner overrides, fees, slippage, and sizing must be written into the manifest; any override that masks an MR signal exit fails preflight.
5. **Isolation assertion.** Record the exact command template, `--engines mr_only`, `--acceptance-mode live_equivalent`, resolved config path, and source commit. Any enabled non-MR engine, untracked source/config change, unavailable data source, or command deviation aborts the run.

No code or configuration change is made by this document. The separate change needed to satisfy a failed activation or wiring gate must be reviewed, committed, and its resulting commit/hash inserted into the preflight record before Phase B can start.

## Data windows, labels, and point-in-time status

All dates are Shanghai trading-calendar dates (`Asia/Shanghai`) and are inclusive unless the runner documents otherwise:

| Slice label | Start | End | Role |
| --- | --- | --- | --- |
| `1Y` | 2025-03-14 | 2026-03-13 | descriptive only |
| `3Y` | 2023-03-14 | 2026-03-13 | descriptive only |
| `5Y` / `FULL_SAMPLE` | 2021-03-14 | 2026-03-13 | sole binding selection sample |

Every result and artifact must carry: `experiment_id`, candidate label, source commit, execution timestamp with timezone, `start_trade_date`, `end_trade_date`, market-data retrieval timestamp, data-provider/version identifier, and one of these exact provenance labels:

- `PIT`: every input (universe membership, price/volume, corporate-action adjustment, benchmark/regime data, and configuration) is demonstrably available as of each decision date; retain the evidence/location for each input.
- `NON_PIT`: any input lacks that demonstration, is a later revised/restated dataset, or the evidence is incomplete.

Until all PIT evidence is present, label every Phase B result `NON_PIT`. `NON_PIT` results may be used only as research diagnostics and cannot support a production or live-policy change.

## Binding and duplicate rules

### Knob binding

A candidate is **configuration-bound** only when both conditions hold on `FULL_SAMPLE`:

1. the preflight fixture proves the intended key-to-signal-field wiring; and
2. versus the explicitly activated baseline, at least one eligible `(trade_date, ticker)` emits a different canonical exit tuple `(entry_price, max_entry_price, stop_loss, target_1, target_2, holding_period)` attributable to that candidate's declared key(s).

For `mr_b5_best_combo`, selection/gate changes are assessed separately from exit binding; it is not an exit-binding substitute. A key whose fixture passes but whose full-sample emitted values never differ is **non-binding in this sample**. A candidate that does not satisfy both conditions is not efficacy-tested, is reported as non-binding, and is ineligible for selection.

### Duplicate detection

For each candidate and window, construct a canonical ordered record of every selected trade: `(trade_date, ticker, direction, entry_price, max_entry_price, stop_loss, target_1, target_2, holding_period, exit_mode)`. Normalize dates to ISO-8601, numeric values to the recorded output precision, sort lexicographically, serialize as UTF-8 newline-delimited JSON, and calculate SHA-256.

Two labels are **duplicate selection sets** when their `FULL_SAMPLE` canonical-record SHA-256 values match exactly. This is a complete duplicate, not a similarity judgment; equal aggregate metrics alone do not make a duplicate. Compare all candidate pairs and publish the comparison table. A duplicate of the baseline or another eligible label is ineligible for selection; retain it as a labeled duplicate rather than silently dropping it. This formalizes the Phase A observation that 10 labels produced 6 observed selection sets without using that observation as a Phase B outcome.

## Frozen decision and reporting rules

The `FULL_SAMPLE` (5Y) is the only binding sample for selecting an eligible candidate. `1Y`, `3Y`, regime, calendar-year, and any other slices are **descriptive only**: publish them without using them to select, reject, retune, or break a tie.

Against the same explicitly activated baseline and execution manifest, an eligible candidate passes the binding performance gate only if its `FULL_SAMPLE` values are strictly higher for all three primary metrics: average return per trade, profit factor, and `true_expectancy_pct`. Publish trade count, win rate, drawdown/portfolio metrics, and all descriptive slices, but none alters that gate. If no candidate passes, the decision is **no change**.

If more than one candidate passes, choose the one with the largest `FULL_SAMPLE` `true_expectancy_pct` improvement; then the largest profit-factor improvement; then the largest average-return-per-trade improvement; then lexicographically smallest label. This deterministic rule applies before inspecting results.

## Promotion policy

There is no automatic production, live, default-config, strategy-code, or execution-policy promotion from Phase B. A passing, `PIT`-labeled candidate may be designated **paper-only** after independent review of the archived artifacts. It must remain MR-only, retain the frozen parameters, and use a separately documented paper period. A `NON_PIT`, non-binding, duplicate, failed, incomplete, or aborted candidate remains research-only. Any later live/config/code promotion requires a new preregistration and review; Phase B outcomes alone are insufficient.

## Required artifact bundle and abort criteria

Before results are read, create an immutable per-run artifact directory and a top-level index containing SHA-256 for every file. Required files are:

- this preregistration's SHA-256, source commit, clean-tree status, and `git diff` (empty is required);
- original overlays, fully resolved configs, and canonical SHA-256 for each;
- command manifest, environment/dependency versions, runner/script SHA-256, data provenance/PIT evidence, and execution log;
- activation, engine-wiring, and backtest-wiring fixture inputs, outputs, assertions, and hashes;
- raw per-trade output, canonical selection records, each selection-set SHA-256, duplicate-comparison table, aggregate metrics, and descriptive-slice table;
- an artifact index with relative paths, byte sizes, SHA-256 values, and creation timestamps.

Abort immediately—do not inspect, compare, or promote partial outcomes—if an activation/wiring assertion fails; any candidate is missing an explicit key; the tree/commit/config/script/data provenance changes; a command differs from the manifest; a required artifact/hash is absent or mismatched; a date/window or `PIT` label cannot be established; data retrieval is incomplete; or the run cannot produce canonical per-trade records. Mark the attempt `ABORTED`, preserve logs/artifacts, and require a new preregistration for any rerun after remediation.
