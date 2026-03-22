#!/usr/bin/env python3
"""
CN Provider Smoke Test
======================

Quick sanity check that the CN data stack (AkShare primary, Tushare backup)
can actually fetch OHLCV data in the current environment. Run before the
full 996-ticker scan to fail fast on dependency or provider issues.

Usage:
    python scripts/smoke_cn_provider.py
    python scripts/smoke_cn_provider.py --config config/default.yaml

Exit codes:
    0 — all probe tickers fetched successfully
    1 — provider or dependency failure
"""

from __future__ import annotations

import argparse
import importlib.metadata
import logging
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from src.core.config import load_config
from src.core.data import get_data_functions

logger = logging.getLogger(__name__)

# Representative tickers: large-cap, different exchanges
PROBE_TICKERS = ["600519.SH", "000858.SZ", "000300.SH"]


def log_versions() -> None:
    """Log installed versions of provider-sensitive packages."""
    for pkg in ("pandas", "akshare", "tushare", "numpy"):
        try:
            ver = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            ver = "NOT INSTALLED"
        logger.info("  %s==%s", pkg, ver)


def main() -> int:
    parser = argparse.ArgumentParser(description="CN provider smoke test")
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    logger.info("=== CN Provider Smoke Test ===")
    logger.info("Installed versions:")
    log_versions()

    config = load_config(args.config)
    _, download_range_fn, provider_config, _ = get_data_functions(config)

    logger.info("Probing %d tickers: %s", len(PROBE_TICKERS), PROBE_TICKERS)

    data_map, report = download_range_fn(
        tickers=PROBE_TICKERS,
        start="2026-01-01",
        end="2026-03-20",
        provider_config=provider_config,
    )

    ok_count = len(data_map)
    fail_count = len(report.get("bad_tickers", []))
    reasons = report.get("reasons", {})

    logger.info("Results: %d OK, %d failed", ok_count, fail_count)
    for ticker in report.get("bad_tickers", []):
        logger.error("  FAIL %s: %s", ticker, reasons.get(ticker, "unknown"))

    if ok_count == 0:
        logger.error("SMOKE TEST FAILED — no tickers fetched. Check provider/dependency compatibility.")
        return 1

    # Validate DataFrame shape on successful fetches
    for ticker, df in data_map.items():
        if len(df) < 10:
            logger.warning("  %s: only %d rows (expected 50+)", ticker, len(df))
        else:
            logger.info("  %s: %d rows, columns=%s", ticker, len(df), list(df.columns))

    if fail_count > 0:
        logger.warning("SMOKE TEST PARTIAL — %d/%d failed. Scan may be degraded.",
                        fail_count, len(PROBE_TICKERS))
        return 1

    logger.info("SMOKE TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
