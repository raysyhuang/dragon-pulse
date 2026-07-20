# MR Ablation Matrix

Baseline for all runs:
- `--acceptance-mode live_equivalent`
- `--engines mr_only`

## Phase A: runnable now

These variants use partial YAML configs under [`config/experiments`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments).
They merge on top of [`config/default.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/default.yaml) via [`load_config`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/src/core/config.py#L25).

| Label | Config | YAML diff | Intent |
| --- | --- | --- | --- |
| `mr_a0_baseline` | [`mr_a0_baseline.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a0_baseline.yaml) | no overrides | anchor |
| `mr_a1_bull_tight_1` | [`mr_a1_bull_tight_1.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a1_bull_tight_1.yaml) | `book_size.bull.min_score=70`, `max_picks=4` | cut weaker bull names |
| `mr_a2_bull_tight_2` | [`mr_a2_bull_tight_2.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a2_bull_tight_2.yaml) | `book_size.bull.min_score=75`, `max_picks=3` | stronger bull suppression |
| `mr_a3_mr_score_floor_up` | [`mr_a3_mr_score_floor_up.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a3_mr_score_floor_up.yaml) | `mean_reversion.score_floor=70` | higher engine quality |
| `mr_a4_mr_score_floor_up_plus_bull` | [`mr_a4_mr_score_floor_up_plus_bull.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a4_mr_score_floor_up_plus_bull.yaml) | `score_floor=70`, bull `min_score=75`, bull `max_picks=3` | combined quality + bull tightening |
| `mr_a5_rsi_deeper` | [`mr_a5_rsi_deeper.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a5_rsi_deeper.yaml) | `mean_reversion.rsi2_max=4` | deeper oversold only |
| `mr_a6_rsi_deeper_plus_score` | [`mr_a6_rsi_deeper_plus_score.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a6_rsi_deeper_plus_score.yaml) | `rsi2_max=4`, `score_floor=70` | deeper pullback + stronger composite |
| `mr_a7_liquidity_up` | [`mr_a7_liquidity_up.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a7_liquidity_up.yaml) | `mean_reversion.adv_min_cny=150000000` | remove lower-quality tape |
| `mr_a8_damage_filter` | [`mr_a8_damage_filter.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a8_damage_filter.yaml) | `mean_reversion.max_single_day_move=0.08` | reject more violent damage bars |
| `mr_a9_acceptance_tighter` | [`mr_a9_acceptance_tighter.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/experiments/mr_a9_acceptance_tighter.yaml) | `acceptance.dq_full_threshold=60`, `dq_selective_threshold=40`, `max_full=4` | verify allocator still has room |

Runner:
- [`scripts/run_mr_phase_a_ablation.sh`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/scripts/run_mr_phase_a_ablation.sh)

Examples:
```bash
scripts/run_mr_phase_a_ablation.sh 1y
scripts/run_mr_phase_a_ablation.sh 3y
scripts/run_mr_phase_a_ablation.sh 5y
scripts/run_mr_phase_a_ablation.sh 1y mr_a4_mr_score_floor_up_plus_bull
```

## Phase B: requires one code change first

Execution is blocked pending the frozen [Phase B preregistration and preflight checklist](PHASE_B_PREREGISTRATION.md). It defines the candidate set, mandatory explicit activation and exit-wiring proof, binding/duplicate rules, artifact hashes, and abort policy; it is the controlling document for any future Phase B run.

Current issue:
- MR exit config exists in [`config/default.yaml`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/config/default.yaml#L58)
- but the scorer still hardcodes exits in [`mean_reversion.py`](/Users/rayhuang/Documents/Python%20Project/dragon-pulse/src/signals/mean_reversion.py#L183)

Wire these config keys into the engine before running Phase B:
- `mean_reversion.stop_atr_mult`
- `mean_reversion.target_1_atr_mult`
- `mean_reversion.target_2_atr_mult`
- `mean_reversion.max_entry_atr_mult`
- `mean_reversion.holding_period`

Planned labels once wired:
- `mr_b1_wider_payoff`
- `mr_b2_asymmetry_max`
- `mr_b3_hold_longer`
- `mr_b4_entry_chase_down`
- `mr_b5_full_best_combo`

Suggested parameter sets:

`mr_b1_wider_payoff`
```yaml
mean_reversion:
  stop_atr_mult: 1.0
  target_1_atr_mult: 2.0
  target_2_atr_mult: 3.0
  holding_period: 4
```

`mr_b2_asymmetry_max`
```yaml
mean_reversion:
  stop_atr_mult: 0.8
  target_1_atr_mult: 2.0
  target_2_atr_mult: 3.5
  holding_period: 4
```

`mr_b3_hold_longer`
```yaml
mean_reversion:
  holding_period: 5
```

`mr_b4_entry_chase_down`
```yaml
mean_reversion:
  max_entry_atr_mult: 0.1
```

`mr_b5_full_best_combo`
- combine the best Phase A gate set with the best Phase B exit set

## Promotion rule

Promote only if `3Y` and `5Y` both improve on:
- `avg return / trade`
- `profit factor`
- `true_expectancy_pct`

Win rate can soften a bit, but only if payoff asymmetry improves enough to make portfolio metrics better.
