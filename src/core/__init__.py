# Core modules
"""
Core Module Exports

This module provides the main functionality for the trading system.
"""

from .config import load_config, get_config_value
from .filters import apply_hard_filters
from .scoring import (
    compute_technical_score_weekly,
    compute_score_30d_breakout,
    compute_score_30d_reversal,
)
from .technicals import atr, rsi, sma, compute_technicals
from .universe import build_universe, get_sp500_universe, get_nasdaq100_universe
from .helpers import get_ny_date, get_trading_date, fetch_news_for_tickers
from .io import get_run_dir, save_csv, save_json, save_run_metadata
from .llm import rank_weekly_candidates, rank_with_debate
from .logging_utils import setup_logging

# New modules (v3.1+)
from .options import compute_options_score, OptionsScore
from .sentiment import compute_sentiment_score, SentimentScore, analyze_news_tone
from .cache import PriceCache, get_cache
from .alerts import AlertManager, AlertConfig, send_overlap_alert, send_run_summary_alert
from .parallel import parallel_map, parallel_batch_map, ParallelScreener, ParallelResult
from .typed_config import AppConfig, load_typed_config

__all__ = [
    # Config
    "load_config",
    "get_config_value",
    "AppConfig",
    "load_typed_config",
    
    # Filters
    "apply_hard_filters",
    
    # Scoring
    "compute_technical_score_weekly",
    "compute_score_30d_breakout",
    "compute_score_30d_reversal",
    
    # Technicals
    "atr",
    "rsi",
    "sma",
    "compute_technicals",
    
    # Universe
    "build_universe",
    "get_sp500_universe",
    "get_nasdaq100_universe",
    
    # Helpers
    "get_ny_date",
    "get_trading_date",
    "fetch_news_for_tickers",
    
    # IO
    "get_run_dir",
    "save_csv",
    "save_json",
    "save_run_metadata",
    
    # LLM
    "rank_weekly_candidates",
    "rank_with_debate",
    
    # Logging
    "setup_logging",
    
    # Options (NEW)
    "compute_options_score",
    "OptionsScore",
    
    # Sentiment (NEW)
    "compute_sentiment_score",
    "SentimentScore",
    "analyze_news_tone",
    
    # Cache (NEW)
    "PriceCache",
    "get_cache",
    
    # Alerts (NEW)
    "AlertManager",
    "AlertConfig",
    "send_overlap_alert",
    "send_run_summary_alert",
    
    # Parallel (NEW)
    "parallel_map",
    "parallel_batch_map",
    "ParallelScreener",
    "ParallelResult",
]
