#!/usr/bin/env python3
"""
Build strict-no-chase filtered detail CSVs from a baseline detail CSV.

Filter rule: keep a trade only if T+1 open <= entry_price (the original
"strict no-chase" rule). The output CSV keeps all baseline columns intact
so portfolio_sim_v2.py can derive entry_date/exit_date from date+exit_day.

Usage:
    python scripts/build_strict_no_chase_csv.py \
        outputs/backtest/backtest_detail_1yr_live_equiv.csv \
        outputs/backtest/backtest_detail_3yr_live_equiv.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from scripts.mas_style_analysis import load_t1_opens, apply_no_chase

logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")

    for f in args.files:
        path = Path(f)
        logger.info("Processing %s", path.name)
        df = pd.read_csv(f)
        df = df[df["pnl_pct"].notna()].copy()
        open_map, _, _ = load_t1_opens(df)
        filtered = apply_no_chase(df, open_map)
        out = path.with_name(path.stem + "__strict_no_chase.csv")
        filtered.to_csv(out, index=False)
        logger.info("Wrote %s (%d / %d rows)", out, len(filtered), len(df))


if __name__ == "__main__":
    sys.exit(main())
