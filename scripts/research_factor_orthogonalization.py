#!/usr/bin/env python3
"""Offline CLI for Dragon Pulse's research-only factor orthogonalizer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.research.factor_orthogonalization import DiagnosticError, run_diagnostic  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a non-binding offline factor-orthogonalization diagnostic"
    )
    parser.add_argument("--matrix", required=True, help="Strict sorted cross-sectional CSV")
    parser.add_argument("--manifest", required=True, help="Schema 1 research manifest JSON")
    parser.add_argument("--output", required=True, help="New output directory; must not exist")
    args = parser.parse_args()
    try:
        summary = run_diagnostic(args.matrix, args.manifest, args.output)
    except (DiagnosticError, OSError, UnicodeError, ValueError) as exc:
        print(f"research factor orthogonalization refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
