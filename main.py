#!/usr/bin/env python3
"""
Dragon Pulse — Deterministic A-Share Scanner

Usage:
    python main.py scan             # Run deterministic scan (primary)
    python main.py performance      # Backtest picks from outputs/
    python main.py all              # Alias → scan
"""

from __future__ import annotations
import sys
import argparse
import logging
import warnings
from pathlib import Path

# Suppress FutureWarning from tushare's internal use of deprecated pandas methods
warnings.filterwarnings("ignore", category=FutureWarning, module="tushare")

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from src.commands import cmd_scan, cmd_performance
    from src.core.logging_utils import setup_logging
except ImportError as e:
    print(f"Error: {e}")
    print("Make sure you're running from the project root directory.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Dragon Pulse — Deterministic A-Share Scanner")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Global flags
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", help="Write logs to file")

    # Scan (primary command)
    p_scan = subparsers.add_parser("scan", help="Run deterministic scan (mean reversion + sniper)")
    p_scan.add_argument("--config", default="config/default.yaml")
    p_scan.add_argument("--date", help="As-of date (YYYY-MM-DD)")

    # All → alias for scan
    p_all = subparsers.add_parser("all", help="Alias for scan")
    p_all.add_argument("--config", default="config/default.yaml")
    p_all.add_argument("--date", help="As-of date (YYYY-MM-DD)")

    # Performance backtest
    p_perf = subparsers.add_parser("performance", help="Backtest picks from outputs/")
    p_perf.add_argument("--outputs-root", default="outputs", help="Root outputs directory")
    p_perf.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    p_perf.add_argument("--end", help="End date YYYY-MM-DD (inclusive)")
    p_perf.add_argument(
        "--source",
        choices=["auto", "legacy", "watchlist"],
        default="auto",
        help="Evaluation source. 'auto' prefers execution_watchlist artifacts when present.",
    )
    p_perf.add_argument("--out-dir", default="outputs/performance", help="Where to write performance artifacts")
    p_perf.add_argument("--forward-days", type=int, default=7, help="Forward trading days window")
    p_perf.add_argument("--threshold", type=float, default=10.0, help="Hit threshold percent")
    p_perf.add_argument("--use-close-only", action="store_true")
    p_perf.add_argument("--include-entry-day", action="store_true")
    p_perf.add_argument("--auto-adjust", action="store_true")
    p_perf.add_argument("--no-threads", action="store_true")

    args = parser.parse_args()

    # Initialize logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=log_level, log_file=log_file)

    if not args.command:
        parser.print_help()
        return 1

    commands = {
        "scan": cmd_scan,
        "all": cmd_scan,  # alias
        "performance": cmd_performance,
    }

    handler = commands.get(args.command)
    if handler:
        try:
            return handler(args)
        except KeyboardInterrupt:
            logging.getLogger(__name__).info("\nInterrupted by user")
            return 130
        except Exception as e:
            logging.getLogger(__name__).error(f"Unexpected error: {e}", exc_info=True)
            return 1
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
