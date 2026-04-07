#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'EOF'
Run MR-only Phase B exit/payoff ablations.

Usage:
  scripts/run_mr_phase_b_ablation.sh 1y
  scripts/run_mr_phase_b_ablation.sh 3y
  scripts/run_mr_phase_b_ablation.sh 5y
  scripts/run_mr_phase_b_ablation.sh 1y mr_b2_asymmetry_max

Periods:
  1y  -> 2025-03-14 to 2026-03-13
  3y  -> 2023-03-14 to 2026-03-13
  5y  -> 2021-03-14 to 2026-03-13
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

period="$1"
single_variant="${2:-}"

case "$period" in
  1y)
    start="2025-03-14"
    end="2026-03-13"
    ;;
  3y)
    start="2023-03-14"
    end="2026-03-13"
    ;;
  5y)
    start="2021-03-14"
    end="2026-03-13"
    ;;
  *)
    echo "Unknown period: $period" >&2
    usage
    exit 1
    ;;
esac

variants=(
  mr_b1_wider_payoff
  mr_b2_asymmetry_max
  mr_b3_hold_longer
  mr_b4_entry_chase_down
)

if [[ -n "$single_variant" ]]; then
  variants=("$single_variant")
fi

for variant in "${variants[@]}"; do
  config_path="config/experiments/${variant}.yaml"
  if [[ ! -f "$config_path" ]]; then
    echo "Missing config: $config_path" >&2
    exit 1
  fi

  label="${variant}_${period}"
  echo
  echo "=== ${label} ($(date '+%Y-%m-%d %H:%M:%S')) ==="
  python scripts/backtest_1yr.py \
    --start "$start" \
    --end "$end" \
    --config "$config_path" \
    --acceptance-mode live_equivalent \
    --engines mr_only \
    --label "$label"
done
