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

## Phase B: blocked by the controlling preregistration

**Do not execute Phase B from this matrix.** The frozen [Phase B preregistration and preflight checklist](PHASE_B_PREREGISTRATION.md) is the sole controlling document for candidate labels, exact keys, baseline requirement, execution manifest, binding/duplicate rules, selection rule, promotion boundary, and abort criteria.

Phase B remains blocked until its explicit activation and exit-wiring assertions pass. In particular, the current default has MR disabled and the scorer must prove every configured exit key reaches the emitted signal and the evaluator. No outcome-driven `best combo`, Phase-A gate substitution, 3Y decision rule, qualitative exception, or ad hoc runner argument is authorized by this historical planning matrix.

The existing runner is not itself an authorization to execute: before any run it must be separately reviewed so the approved manifest contains the explicitly activated baseline and an allowlisted candidate set.
