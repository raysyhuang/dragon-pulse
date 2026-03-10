#!/usr/bin/env python3
"""
Momentum Trading System - Unified Entry Point

Simple, consolidated system with progress indicators.
All functionality in one place for easy copying to AI chatbots.

Usage:
    python main.py weekly      # Weekly scanner
    python main.py pro30       # 30-day screener
    python main.py movers      # Daily movers only
    python main.py all         # Run everything + hybrid analysis
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
    # python-dotenv not installed, skip .env loading
    pass

# Add src to path (project root for `src.X` imports, src/ for bare imports in Dragon Pulse modules)
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from src.commands import (
        cmd_weekly, cmd_pro30, cmd_llm, cmd_movers, cmd_all,
        cmd_performance, cmd_replay, cmd_scan, cmd_backtest, cmd_evolve,
        cmd_calibrate,
    )
    from src.core.logging_utils import setup_logging
except ImportError as e:
    print(f"Error: {e}")
    print("Make sure you're running from the project root directory.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Momentum Trading System - Unified CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Global flags
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", help="Write logs to file")
    
    # ALL - Run everything
    p_all = subparsers.add_parser("all", help="Run all screeners + LLM + hybrid analysis (RECOMMENDED)")
    p_all.add_argument("--date", help="Date (YYYY-MM-DD), defaults to today")
    p_all.add_argument("--config", default="config/default.yaml")
    p_all.add_argument("--no-movers", action="store_true")
    p_all.add_argument("--provider", default="openai", choices=["openai", "anthropic"])
    p_all.add_argument("--model", default="gpt-5.2", help="Model name (default: gpt-5.2, falls back to gpt-4o if not available)")
    p_all.add_argument("--api-key")
    p_all.add_argument("--open", action="store_true", help="Open HTML report in browser after completion")
    
    # Weekly
    p_weekly = subparsers.add_parser("weekly", help="Run Weekly Scanner only")
    p_weekly.add_argument("--config", default="config/default.yaml")
    p_weekly.add_argument("--no-movers", action="store_true")
    p_weekly.add_argument("--date", help="As-of date (YYYY-MM-DD) for historical replay")
    
    # Pro30
    p_pro30 = subparsers.add_parser("pro30", help="Run 30-Day Screener only")
    p_pro30.add_argument("--config", default="config/default.yaml")
    p_pro30.add_argument("--no-movers", action="store_true")
    p_pro30.add_argument("--date", help="As-of date (YYYY-MM-DD) for historical replay")
    
    # LLM
    p_llm = subparsers.add_parser("llm", help="Run LLM ranking on weekly packets only")
    p_llm.add_argument("--date", help="Date (YYYY-MM-DD)")
    p_llm.add_argument("--provider", default="openai", choices=["openai", "anthropic"])
    p_llm.add_argument("--model", default="gpt-5.2", help="Model name (default: gpt-5.2)")
    p_llm.add_argument("--api-key")
    
    # Movers
    p_movers = subparsers.add_parser("movers", help="Daily movers discovery only")
    p_movers.add_argument("--config", default="config/default.yaml")

    # Performance backtest
    p_perf = subparsers.add_parser("performance", help="Backtest picks from outputs/ (Hit +10%% within 7 trading days)")
    p_perf.add_argument("--outputs-root", default="outputs", help="Root outputs directory (default: outputs)")
    p_perf.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    p_perf.add_argument("--end", help="End date YYYY-MM-DD (inclusive)")
    p_perf.add_argument("--out-dir", default="outputs/performance", help="Where to write performance artifacts")
    p_perf.add_argument("--forward-days", type=int, default=7, help="Forward trading days window (default: 7)")
    p_perf.add_argument("--threshold", type=float, default=10.0, help="Hit threshold percent (default: 10.0)")
    p_perf.add_argument("--use-close-only", action="store_true", help="Use Close instead of High for max-forward-price")
    p_perf.add_argument("--include-entry-day", action="store_true", help="Include entry day in the forward window (default excludes)")
    p_perf.add_argument("--auto-adjust", action="store_true", help="yfinance auto_adjust prices")
    p_perf.add_argument("--no-threads", action="store_true", help="Disable threaded yfinance download")

    # Historical replay
    p_replay = subparsers.add_parser("replay", help="Replay past dates to regenerate outputs/YYYY-MM-DD/ (optionally with LLM)")
    p_replay.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    p_replay.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    p_replay.add_argument("--config", default="config/default.yaml")
    p_replay.add_argument("--no-movers", action="store_true")
    p_replay.add_argument("--llm", action="store_true", help="Also generate weekly LLM Top5 for each day (requires API key)")
    p_replay.add_argument("--provider", default="openai", choices=["openai", "anthropic"])
    p_replay.add_argument("--model", default="gpt-5.2")
    p_replay.add_argument("--api-key")
    p_replay.add_argument("--max-days", type=int, default=0, help="Limit number of replayed days (0 = no limit)")
    
    # Dragon Pulse - Daily Scan
    p_scan = subparsers.add_parser("scan", help="Run Dragon Pulse (龙脉) high win-rate scan")
    p_scan.add_argument("--config", default="config/default.yaml")
    p_scan.add_argument("--date", help="As-of date (YYYY-MM-DD)")
    
    # Dragon Pulse - Backtest
    p_bt = subparsers.add_parser("backtest", help="Run Dragon Pulse full backtest")
    p_bt.add_argument("--config", default="config/default.yaml")
    p_bt.add_argument("--start", help="Start date YYYY-MM-DD")
    p_bt.add_argument("--end", help="End date YYYY-MM-DD")
    p_bt.add_argument("--max-universe", type=int, default=500, help="Limit tickers for speed")
    
    # Dragon Pulse - Evolve
    p_evolve = subparsers.add_parser("evolve", help="Run Evolutionary Parameter Optimization")
    p_evolve.add_argument("--config", default="config/default.yaml")
    p_evolve.add_argument("--population", type=int, default=10)
    p_evolve.add_argument("--generations", type=int, default=5)

    # Calibrate - Parameter sweep
    p_cal = subparsers.add_parser("calibrate", help="Sweep composite/technical thresholds against historical outcomes")
    p_cal.add_argument("--outputs-root", default="outputs", help="Root outputs directory")
    p_cal.add_argument("--start", help="Start date YYYY-MM-DD")
    p_cal.add_argument("--end", help="End date YYYY-MM-DD")
    p_cal.add_argument("--out-dir", default="outputs/calibration", help="Where to write calibration artifacts")
    
    args = parser.parse_args()
    
    # Initialize logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = Path(args.log_file) if args.log_file else None
    setup_logging(level=log_level, log_file=log_file)
    
    if not args.command:
        parser.print_help()
        return 1
    
    commands = {
        "all": cmd_all,
        "weekly": cmd_weekly,
        "pro30": cmd_pro30,
        "llm": cmd_llm,
        "movers": cmd_movers,
        "performance": cmd_performance,
        "replay": cmd_replay,
        "scan": cmd_scan,
        "backtest": cmd_backtest,
        "evolve": cmd_evolve,
        "calibrate": cmd_calibrate,
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

