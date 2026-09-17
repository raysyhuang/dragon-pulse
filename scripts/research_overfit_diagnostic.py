#!/usr/bin/env python3
"""Offline CLI for the Dragon Pulse research-only overfit diagnostic."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.research.overfit_diagnostic import (  # noqa: E402
    DEFAULT_THRESHOLDS,
    DiagnosticError,
    run_diagnostic,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a non-binding, research-only overfit evidence directory"
    )
    parser.add_argument("--matrix", required=True, help="Immutable aligned daily net-return CSV")
    parser.add_argument("--manifest", required=True, help="Immutable experiment manifest JSON")
    parser.add_argument("--output", required=True, help="New evidence directory; must not exist")
    parser.add_argument("--blocks", type=int, default=8, help="Even number of contiguous CSCV blocks")
    parser.add_argument("--min-block-observations", type=int, default=20)
    parser.add_argument(
        "--min-selected-sharpe", type=float,
        default=DEFAULT_THRESHOLDS["min_selected_annualized_sharpe"],
    )
    parser.add_argument(
        "--min-deflated-sharpe-probability", type=float,
        default=DEFAULT_THRESHOLDS["min_deflated_sharpe_probability"],
    )
    parser.add_argument("--max-pbo", type=float, default=DEFAULT_THRESHOLDS["max_pbo"])
    parser.add_argument(
        "--max-bonferroni-p-value", type=float,
        default=DEFAULT_THRESHOLDS["max_bonferroni_p_value"],
    )
    args = parser.parse_args()
    thresholds = {
        "min_selected_annualized_sharpe": args.min_selected_sharpe,
        "min_deflated_sharpe_probability": args.min_deflated_sharpe_probability,
        "max_pbo": args.max_pbo,
        "max_bonferroni_p_value": args.max_bonferroni_p_value,
    }
    try:
        summary = run_diagnostic(
            args.matrix,
            args.manifest,
            args.output,
            n_blocks=args.blocks,
            min_block_observations=args.min_block_observations,
            thresholds=thresholds,
        )
    except (DiagnosticError, OSError, UnicodeError, ValueError) as exc:
        print(f"research overfit diagnostic refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
