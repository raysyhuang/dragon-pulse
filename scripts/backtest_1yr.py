#!/usr/bin/env python3
"""
Bulk-download backtest — downloads all data once, then iterates in memory.

Usage:
    python scripts/backtest_1yr.py --start 2025-03-14 --end 2026-03-13
    python scripts/backtest_1yr.py --start 2023-03-14 --end 2026-03-13 --label 3yr
    python scripts/backtest_1yr.py --max-days 50
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Load .env if present so Tushare-backed universe ranking works in local runs.
load_dotenv(project_root / ".env")

from src.core.config import load_config, get_config_value
from src.core.input_bundle import set_input_mode, validate_input_bundle
from src.core.data import get_data_functions
from src.core.universe import (
    get_top_n_cn_by_market_cap,
    build_pit_universe_schedule,
    active_pit_universe,
)
from src.features.technical import (
    compute_all_technical_features,
    compute_rsi2_features,
    latest_features,
)
from src.core.acceptance import run_acceptance, score_day_quality
from src.core.cn_limits import get_daily_limit
from src.features.performance.backtest import load_execution_watchlist_tickers_in_range
from src.pipelines.scanner import _classify_regime
from src.signals.mean_reversion import (
    resolve_mr_subtype_and_exit_params,
    score_mean_reversion,
)
from src.signals.sniper import score_sniper
from src.signals.alpha_candidates import (
    score_accumulation_breakout_alpha,
    score_limitup_continuation_alpha,
    score_rs_pullback_alpha,
    score_sniper_breakout_alpha,
)

_TARGET_EXIT_REASONS = {"target_hit", "target_hit_gap"}
_STOP_EXIT_REASONS = {
    "gap_stop_open", "same_bar_stop_first", "stop_hit", "stop_hit_gap",
    "trail_stop", "trail_stop_gap",
}
_HOLD_EXIT_REASONS = {"hold_expired", "hold_expired_runner"}


def _is_stop_exit(exit_reason: str | None) -> bool:
    return exit_reason in _STOP_EXIT_REASONS


def _daily_outcome_bucket(exit_reason: str | None) -> str:
    if exit_reason in _TARGET_EXIT_REASONS:
        return "win"
    if exit_reason in _STOP_EXIT_REASONS:
        return "loss"
    if exit_reason in _HOLD_EXIT_REASONS:
        return "hold"
    return "no_data"


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _stable_price_fingerprint(data_map: dict[str, pd.DataFrame]) -> str:
    """Hash compact OHLCV facts so reruns can detect provider/cache drift."""
    digest = hashlib.sha256()
    for ticker in sorted(data_map):
        df = data_map.get(ticker, pd.DataFrame())
        digest.update(ticker.encode("utf-8"))
        if df.empty:
            digest.update(b"|empty\n")
            continue
        cols = [c for c in ("Open", "High", "Low", "Close", "Volume", "open", "high", "low", "close", "volume") if c in df.columns]
        compact = df[cols].copy() if cols else df.copy()
        compact.index = pd.to_datetime(compact.index).strftime("%Y-%m-%d")
        digest.update(compact.to_csv(float_format="%.6f").encode("utf-8"))
    return digest.hexdigest()


def _snapshot_path(snapshot_dir: Path, ticker: str) -> Path:
    safe = ticker.replace("/", "_").replace("\\", "_").replace(":", "_")
    return snapshot_dir / f"{safe}.csv"


def _read_price_snapshot(snapshot_dir: Path, ticker: str) -> pd.DataFrame:
    path = _snapshot_path(snapshot_dir, ticker)
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index.name = "Date"
        return df
    except Exception:
        return pd.DataFrame()


def _write_price_snapshot(snapshot_dir: Path, ticker: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out.index = pd.to_datetime(out.index).strftime("%Y-%m-%d")
    out.to_csv(_snapshot_path(snapshot_dir, ticker))


def _load_price_snapshot(snapshot_dir: Path, tickers: list[str]) -> tuple[dict[str, pd.DataFrame], list[str]]:
    data: dict[str, pd.DataFrame] = {}
    missing: list[str] = []
    for ticker in tickers:
        df = _read_price_snapshot(snapshot_dir, ticker)
        if df.empty:
            missing.append(ticker)
        else:
            data[ticker] = df
    return data, missing


def _risk_metrics(sig) -> tuple[float, float, float]:
    """Return stop distance %, R:R to target_1, and entry extension %."""
    entry = _safe_float(getattr(sig, "entry_price", None))
    stop = _safe_float(getattr(sig, "stop_loss", None))
    target = _safe_float(getattr(sig, "target_1", None))
    max_entry = _safe_float(getattr(sig, "max_entry_price", None), entry)
    stop_dist_pct = ((entry - stop) / entry * 100.0) if entry > 0 and stop > 0 else 999.0
    reward_risk = ((target - entry) / (entry - stop)) if target > entry and entry > stop else 0.0
    entry_ext_pct = ((max_entry - entry) / entry * 100.0) if entry > 0 and max_entry > 0 else 999.0
    return stop_dist_pct, reward_risk, entry_ext_pct


def rank_picks(picks: list[tuple[str, object]], rank_mode: str = "score") -> list[tuple[str, object]]:
    """Rank candidate picks for allocator ordering.

    score: current behavior, highest signal score first.
    lowrisk: prefer the tightest stop distance, then higher score.
    rr: prefer highest target_1 reward/risk, then higher score.
    lowrisk_rr: prefer low stop distance, then high reward/risk, then score.
    """
    if rank_mode == "score":
        return sorted(picks, key=lambda x: (-getattr(x[1], "score", 0.0),))
    if rank_mode == "lowrisk":
        return sorted(picks, key=lambda x: (_risk_metrics(x[1])[0], -getattr(x[1], "score", 0.0)))
    if rank_mode == "rr":
        return sorted(picks, key=lambda x: (-_risk_metrics(x[1])[1], -getattr(x[1], "score", 0.0)))
    if rank_mode == "lowrisk_rr":
        return sorted(picks, key=lambda x: (_risk_metrics(x[1])[0], -_risk_metrics(x[1])[1], -getattr(x[1], "score", 0.0)))
    raise ValueError(f"Unknown pick rank mode: {rank_mode}")


def apply_ticker_cooldowns(
    picks: list[tuple[str, object]],
    day_index: int,
    cooldown_until: dict[str, int],
) -> list[tuple[str, object]]:
    """Remove candidates whose ticker is still in a research cooldown window."""
    return [
        (engine, sig)
        for engine, sig in picks
        if int(cooldown_until.get(getattr(sig, "ticker", ""), -1)) < day_index
    ]

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 400  # technical indicators need history


def resolve_backtest_engines(engines_arg: str, config: dict) -> tuple[bool, bool, bool, bool, bool]:
    """Return (run_mean_reversion, run_sniper, sniper_requested, run_alpha, alpha_requested).

    Backtests must mirror live scanner behavior: quarantined/research engines can
    be requested by CLI, but they only run when their config block is enabled.
    """
    requested = {
        part.strip()
        for part in str(engines_arg or "all").split(",")
        if part.strip()
    }
    if not requested:
        requested = {"all"}

    run_mr = bool(
        "all" in requested
        or "mr_only" in requested
        or "mr" in requested
        or "mean_reversion" in requested
    )
    sniper_requested = bool(
        "all" in requested
        or "sniper_only" in requested
        or "sniper" in requested
    )
    alpha_requested = bool(
        "alpha_only" in requested
        or "alpha" in requested
        or "alpha_candidates" in requested
    )
    mr_enabled = bool((config.get("mean_reversion") or {}).get("enabled", True))
    sniper_enabled = bool((config.get("sniper") or {}).get("enabled", False))
    alpha_enabled = bool((config.get("alpha_candidates") or {}).get("enabled", False))
    run_mr = run_mr and mr_enabled
    run_sniper = sniper_requested and sniper_enabled
    run_alpha = alpha_requested and alpha_enabled
    return run_mr, run_sniper, sniper_requested, run_alpha, alpha_requested


def parse_regime_set(regimes_arg: str | list[str] | tuple[str, ...] | set[str]) -> set[str]:
    """Parse a comma-separated or config-list regime filter into normalized names."""
    if isinstance(regimes_arg, (list, tuple, set)):
        parts = regimes_arg
    else:
        parts = str(regimes_arg or "").split(",")
    return {str(part).strip().lower() for part in parts if str(part).strip()}


def apply_score_floor(
    candidates: list[tuple[str, object]],
    score_floor: float | None,
) -> list[tuple[str, object]]:
    """Filter scored signal candidates by a global score floor."""
    if score_floor is None:
        return candidates
    return [candidate for candidate in candidates if candidate[1].score >= score_floor]


def get_cn_trading_days(start: date, end: date) -> list[date]:
    """Generate weekdays between start and end (approximate CN trading calendar)."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def evaluate_pick(
    ticker: str,
    entry_price: float,
    stop_loss: float,
    target_1: float,
    holding_period: int,
    forward_df: pd.DataFrame,
    exit_mode: str = "target_stop",
    engine: str = "",
    atr: float = 0.0,
    max_entry_price: float | None = None,
    entry_mode: str = "signal",
    runner_max_hold: int = 10,
    trail_atr: float = 1.5,
    breakeven_at: float = 3.0,
    laggard_day: int = 2,
    laggard_thresh: float = 0.0,
) -> dict:
    """Evaluate a single pick against forward price data.

    T+1: entry fills at the first forward bar; exits begin on the following
    bar. The sole entry-bar exception is an open at/below stop, recorded as
    ``gap_stop_open`` at the open.

    Entry modes:
        signal              — legacy: assume fill at signal entry_price
        no_chase_next_open  — use next open as fill if it is <= max_entry_price;
                              otherwise skip/cancel trade
        limit_touch         — MAS-like DAY limit replay: fill at next open if it
                              is <= max_entry_price; otherwise fill at max_entry
                              if T+1 intraday low touches it; otherwise skip

    Exit modes:
        target_stop     — original: exit on target or stop hit, hold_expired at end
        profitable_close — exit on close > entry after day 2+ (captures MR drift)
        trailing         — once P&L > 1×ATR intraday, move stop to breakeven
        runner          — full position; ignore fixed target; once close >= entry*(1+breakeven_at%)
                          engage breakeven + trail at trail_atr×ATR; time-exit at runner_max_hold.
                          Captures trend the fixed 3-day/target exit leaves on the table.
        runner_laggard  — runner + cut early if flat/red at the laggard_day close.
    """
    runner_modes = {"runner", "runner_laggard"}
    if holding_period < 2:
        raise ValueError("holding_period must be >= 2 under CN T+1")
    if exit_mode in runner_modes and runner_max_hold < 2:
        raise ValueError("runner_max_hold must be >= 2 under CN T+1")
    requested_entry_price = float(entry_price)
    result = {
        "ticker": ticker,
        "entry_price": requested_entry_price,
        "requested_entry_price": requested_entry_price,
        "actual_entry_price": None,
        "max_entry_price": max_entry_price,
        "entry_status": "pending",
        "stop_loss": stop_loss,
        "target_1": target_1,
        "holding_period": holding_period,
        "exit_price": None,
        "exit_day": None,
        "exit_reason": None,
        "pnl_pct": None,
        "unrealized_pnl_pct": None,
        "hit_target": False,
        "hit_stop": False,
        "same_bar_both_touched": False,
    }

    if forward_df.empty:
        result["entry_status"] = "no_data"
        result["exit_reason"] = "no_data"
        return result

    close_col = "Close" if "Close" in forward_df.columns else "close"
    high_col = "High" if "High" in forward_df.columns else "high"
    low_col = "Low" if "Low" in forward_df.columns else "low"
    open_col = "Open" if "Open" in forward_df.columns else "open"

    # T+1: skip day 0 (signal day), enter/evaluate from day 1.
    eff_hold = runner_max_hold if exit_mode in runner_modes else holding_period
    bars = forward_df.iloc[1:eff_hold + 1] if len(forward_df) > 1 else pd.DataFrame()

    if bars.empty:
        result["entry_status"] = "no_data"
        result["exit_reason"] = "insufficient_forward_data"
        return result

    if entry_mode == "no_chase_next_open":
        first_bar = bars.iloc[0]
        next_open = float(first_bar[open_col]) if open_col in bars.columns else float(first_bar[close_col])
        if max_entry_price is not None and next_open > float(max_entry_price):
            result["entry_status"] = "skipped"
            result["exit_reason"] = "entry_skipped_no_chase"
            return result
        entry_price = next_open
    elif entry_mode == "limit_touch":
        first_bar = bars.iloc[0]
        next_open = float(first_bar[open_col]) if open_col in bars.columns else float(first_bar[close_col])
        next_low = float(first_bar[low_col]) if low_col in bars.columns else next_open
        if max_entry_price is None or next_open <= float(max_entry_price):
            entry_price = next_open
        elif next_low <= float(max_entry_price):
            entry_price = float(max_entry_price)
        else:
            result["entry_status"] = "skipped"
            result["exit_reason"] = "entry_skipped_no_chase"
            return result
    elif entry_mode != "signal":
        raise ValueError(f"Unknown entry_mode: {entry_mode}")

    entry_price = float(entry_price)
    result["entry_price"] = entry_price
    result["actual_entry_price"] = entry_price
    result["entry_status"] = "filled"

    first_open = float(bars.iloc[0][open_col])
    if first_open <= stop_loss:
        result.update(
            exit_price=min(stop_loss, first_open),
            exit_day=1,
            exit_reason="gap_stop_open",
            hit_stop=True,
        )
        result["pnl_pct"] = round((result["exit_price"] / entry_price - 1) * 100, 2)
        return result

    # ---- Runner / runner_laggard: let winners run on an ATR trailing stop. ----
    if exit_mode in runner_modes:
        eff_atr = atr if atr and atr > 0 else abs(entry_price - stop_loss)
        effective_stop = stop_loss
        engaged = False
        highest_close = entry_price
        for day_idx, (dt, row) in enumerate(bars.iterrows(), start=1):
            high = float(row[high_col])
            low = float(row[low_col])
            close = float(row[close_col])

            if day_idx == 1:
                # CN T+1 prevents entry-bar exits, not next-session trail arming.
                highest_close = max(highest_close, close)
                if close >= entry_price * (1 + breakeven_at / 100.0):
                    engaged = True
                if engaged and eff_atr > 0:
                    effective_stop = max(effective_stop, entry_price,
                                         highest_close - trail_atr * eff_atr)
                continue

            # Conservative intrabar order: stop (Low) before any upside.
            if low <= effective_stop:
                open_px = float(row[open_col])
                gapped = open_px < effective_stop
                result["exit_price"] = min(effective_stop, open_px)
                result["exit_day"] = day_idx
                result["exit_reason"] = (
                    ("trail_stop_gap" if gapped else "trail_stop") if engaged
                    else ("stop_hit_gap" if gapped else "stop_hit")
                )
                result["hit_stop"] = True
                break

            # Laggard cut: flat/red at the cut bar's close -> abandon early.
            if (exit_mode == "runner_laggard" and day_idx == laggard_day
                    and close <= entry_price * (1 + laggard_thresh / 100.0)):
                result["exit_price"] = close
                result["exit_day"] = day_idx
                result["exit_reason"] = "laggard_cut"
                break

            highest_close = max(highest_close, close)
            if not engaged and close >= entry_price * (1 + breakeven_at / 100.0):
                engaged = True
            if engaged and eff_atr > 0:
                effective_stop = max(effective_stop, entry_price,
                                     highest_close - trail_atr * eff_atr)
        else:
            last_close = float(bars.iloc[-1][close_col])
            if len(bars) >= eff_hold:
                result["exit_price"] = last_close
                result["exit_day"] = len(bars)
                result["exit_reason"] = "hold_expired_runner" if engaged else "hold_expired"
            else:
                result["unrealized_pnl_pct"] = round((last_close / entry_price - 1) * 100, 2)
                result["exit_reason"] = "holding_window_incomplete"

        if result["exit_price"] is not None and entry_price > 0:
            result["pnl_pct"] = round((result["exit_price"] / entry_price - 1) * 100, 2)
        return result

    # Trailing stop state: once high exceeds entry + 1×ATR, stop moves to breakeven
    effective_stop = stop_loss
    trailing_activated = False

    for day_idx, (dt, row) in enumerate(bars.iterrows(), start=1):
        high = float(row[high_col])
        low = float(row[low_col])
        close = float(row[close_col])

        if day_idx == 1:
            # CN T+1 prevents entry-bar exits, not next-session stop arming.
            if exit_mode == "trailing" and atr > 0 and high >= entry_price + atr:
                effective_stop = max(effective_stop, entry_price)
                trailing_activated = True
            continue

        touched_stop = low <= effective_stop
        touched_target = high >= target_1
        if touched_stop:
            result["same_bar_both_touched"] = bool(touched_target)
            open_px = float(row[open_col])
            result["exit_price"] = min(effective_stop, open_px)
            result["exit_day"] = day_idx
            if touched_target:
                result["exit_reason"] = "same_bar_stop_first"
            else:
                result["exit_reason"] = "stop_hit_gap" if open_px < effective_stop else "stop_hit"
            result["hit_stop"] = True
            break
        if touched_target:
            open_px = float(row[open_col])
            result["exit_price"] = max(target_1, open_px)
            result["exit_day"] = day_idx
            result["exit_reason"] = "target_hit_gap" if open_px > target_1 else "target_hit"
            result["hit_target"] = True
            break

        # A high observed this bar can only ratchet the stop for the next bar.
        if exit_mode == "trailing" and atr > 0 and not trailing_activated:
            if high >= entry_price + atr:
                effective_stop = max(effective_stop, entry_price)
                trailing_activated = True

        # Profitable-close exit: after day 2, exit if close > entry
        if exit_mode == "profitable_close" and day_idx >= 2 and close > entry_price:
            result["exit_price"] = close
            result["exit_day"] = day_idx
            result["exit_reason"] = "profitable_close"
            break
    else:
        last_close = float(bars.iloc[-1][close_col])
        if len(bars) >= eff_hold:
            result["exit_price"] = last_close
            result["exit_day"] = len(bars)
            result["exit_reason"] = "hold_expired"
        else:
            result["unrealized_pnl_pct"] = round((last_close / entry_price - 1) * 100, 2)
            result["exit_reason"] = "holding_window_incomplete"

    if result["exit_price"] is not None and entry_price > 0:
        result["pnl_pct"] = round((result["exit_price"] / entry_price - 1) * 100, 2)

    return result


def _slice_up_to(df: pd.DataFrame, scan_date: date) -> pd.DataFrame:
    """Slice a DataFrame to include only rows up to and including scan_date."""
    if df.empty:
        return df
    idx = df.index
    if hasattr(idx, 'date'):
        mask = idx.date <= scan_date
    else:
        mask = pd.to_datetime(idx).date <= scan_date
    return df.loc[mask]


def _slice_from(df: pd.DataFrame, scan_date: date) -> pd.DataFrame:
    """Slice a DataFrame to include rows from scan_date onward."""
    if df.empty:
        return df
    idx = df.index
    if hasattr(idx, 'date'):
        mask = idx.date >= scan_date
    else:
        mask = pd.to_datetime(idx).date >= scan_date
    return df.loc[mask]

def _atr14_up_to(df: pd.DataFrame, scan_date: date, period: int = 14) -> float:
    """ATR(period) using bars up to and including scan_date. 0.0 if insufficient."""
    if df is None or df.empty:
        return 0.0
    hist = _slice_up_to(df, scan_date)
    if len(hist) < period + 1:
        return 0.0
    h = "High" if "High" in hist.columns else "high"
    l = "Low" if "Low" in hist.columns else "low"
    c = "Close" if "Close" in hist.columns else "close"
    high = pd.to_numeric(hist[h], errors="coerce")
    low = pd.to_numeric(hist[l], errors="coerce")
    close = pd.to_numeric(hist[c], errors="coerce")
    prev = close.shift(1)
    tr = pd.concat([(high - low), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return float(atr) if pd.notna(atr) else 0.0


def main():
    parser = argparse.ArgumentParser(description="Dragon Pulse Backtest (bulk download)")
    parser.add_argument("--start", default="2025-03-14", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2026-03-13", help="End date YYYY-MM-DD")
    parser.add_argument("--config", default="config/default.yaml")
    parser.add_argument("--max-days", type=int, default=0, help="Limit number of scan days (0=all)")
    parser.add_argument("--out-dir", default="outputs/backtest", help="Output directory")
    parser.add_argument("--label", default="", help="Label suffix for output files")
    parser.add_argument("--top-n", type=int, default=5, help="Top N picks per day")
    parser.add_argument("--exit-mode", default="target_stop",
                        choices=["target_stop", "profitable_close", "trailing",
                                 "runner", "runner_laggard"],
                        help="Exit strategy: target_stop (default), profitable_close, "
                             "trailing, runner, runner_laggard")
    parser.add_argument("--runner-max-hold", type=int, default=10,
                        help="Max hold (trading days) for runner exit modes")
    parser.add_argument("--trail-atr", type=float, default=1.5,
                        help="Trailing-stop distance in ATR for runner modes")
    parser.add_argument("--breakeven-at", type=float, default=3.0,
                        help="Close pct gain that engages breakeven+trail (runner modes)")
    parser.add_argument("--laggard-day", type=int, default=2,
                        help="Bar index whose close decides the laggard cut (runner_laggard)")
    parser.add_argument("--laggard-thresh", type=float, default=0.0,
                        help="Cut if return-to-close <= this pct at laggard-day")
    parser.add_argument("--dump-picks", default="",
                        help="If set, write the pinned pick set (pre-exit) to this CSV "
                             "for reproducible exit-logic replay.")
    parser.add_argument("--entry-mode", default="no_chase_next_open",
                        choices=["signal", "no_chase_next_open", "limit_touch"],
                        help="Entry replay: signal=legacy signal-price fill; no_chase_next_open=next-open no-chase; limit_touch=MAS-like DAY limit touch")
    parser.add_argument("--disable-gap-filter", action="store_true",
                        help="Disable MR gap-risk rejection (for baseline comparison)")
    parser.add_argument("--mr-target", default="sma5", choices=["sma5", "atr"],
                        help="MR target mode: sma5 (v4.1 default) or atr (legacy)")
    parser.add_argument("--no-sniper-trailing", action="store_true",
                        help="Disable auto-trailing for sniper picks")
    parser.add_argument("--engines", default="all",
                        help="Engines to run: all, mr_only, sniper_only (comma-sep also works)")
    parser.add_argument("--acceptance-mode", default="off",
                        choices=["off", "engine_only", "live_equivalent"],
                        help="Acceptance: off, engine_only (raw deduped), live_equivalent (full funnel)")
    parser.add_argument("--universe-source", default="market_cap",
                        choices=["market_cap", "watchlist"],
                        help="Universe source: market_cap (default) or tickers seen in execution watchlists")
    parser.add_argument("--universe-mode", default="static",
                        choices=["static", "point_in_time"],
                        help="static = current top-N over all history (survivorship-biased); "
                             "point_in_time = top-N by market cap as of each rebalance (unbiased)")
    parser.add_argument("--universe-n", type=int, default=1000,
                        help="Universe size (top-N by market cap)")
    parser.add_argument("--universe-rebalance-months", type=int, default=1,
                        help="Point-in-time universe rebalance cadence in months")
    parser.add_argument("--outputs-root", default="outputs",
                        help="Outputs root used when universe-source=watchlist")
    input_source_group = parser.add_mutually_exclusive_group()
    input_source_group.add_argument(
        "--price-snapshot-dir", default="",
        help="Optional CSV price snapshot directory. Existing ticker CSVs are reused; downloaded data is written back for deterministic reruns.",
    )
    input_source_group.add_argument(
        "--input-bundle", default="",
        help="Immutable offline bundle directory; refuses all provider/cache acquisition paths.",
    )
    parser.add_argument("--allowed-regimes", default="",
                        help="Comma-separated regimes to scan: bull,choppy,bear. Empty means all.")
    parser.add_argument("--excluded-regimes", default="",
                        help="Comma-separated regimes to skip: bull,choppy,bear. Empty means none.")
    parser.add_argument("--min-score-floor", type=float, default=None,
                        help="Optional global score floor applied before pick allocation.")
    parser.add_argument("--pick-rank-mode", default="score",
                        choices=["score", "lowrisk", "rr", "lowrisk_rr"],
                        help="Candidate ordering before top-N/acceptance allocation.")
    parser.add_argument("--ticker-cooldown-days", type=int, default=0,
                        help="Research-only: skip tickers picked in the prior N trading days.")
    parser.add_argument("--post-stop-cooldown-days", type=int, default=0,
                        help="Research-only: if a pick stops out, skip that ticker for N trading days.")
    args = parser.parse_args()
    bundle_incompatible_flags = (
        "--universe-source", "--universe-mode", "--universe-n",
        "--universe-rebalance-months", "--outputs-root",
    )
    if args.input_bundle and any(
        token == flag or token.startswith(f"{flag}=")
        for token in sys.argv[1:]
        for flag in bundle_incompatible_flags
    ):
        parser.error("--input-bundle cannot be combined with universe acquisition flags")
    if args.input_bundle:
        set_input_mode("bundle")
    else:
        set_input_mode("live")
    allowed_regimes = parse_regime_set(args.allowed_regimes)
    excluded_regimes = parse_regime_set(args.excluded_regimes)
    valid_regimes = {"bull", "choppy", "bear"}
    invalid_regimes = (allowed_regimes | excluded_regimes) - valid_regimes
    if invalid_regimes:
        parser.error(f"Unknown regime(s): {', '.join(sorted(invalid_regimes))}")
    if args.min_score_floor is not None and not 0 <= args.min_score_floor <= 100:
        parser.error("--min-score-floor must be between 0 and 100")
    if args.ticker_cooldown_days < 0 or args.post_stop_cooldown_days < 0:
        parser.error("cooldown days must be non-negative")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    config = load_config(args.config)
    config_excluded_regimes = parse_regime_set((config.get("acceptance") or {}).get("excluded_regimes", []))
    excluded_regimes |= config_excluded_regimes

    trading_days = get_cn_trading_days(start_date, end_date)
    if args.max_days > 0:
        trading_days = trading_days[:args.max_days]

    logger.info("=" * 60)
    logger.info("BACKTEST: %s to %s (%d trading days)", args.start, args.end, len(trading_days))
    logger.info("  exit_mode=%s  entry_mode=%s  mr_target=%s  gap_filter=%s  sniper_trailing=%s  acceptance=%s",
                args.exit_mode, args.entry_mode, args.mr_target,
                "OFF" if args.disable_gap_filter else "ON",
                "OFF" if args.no_sniper_trailing else "ON",
                args.acceptance_mode)
    logger.info("  allowed_regimes=%s  excluded_regimes=%s",
                ",".join(sorted(allowed_regimes)) if allowed_regimes else "all",
                ",".join(sorted(excluded_regimes)) if excluded_regimes else "none")
    logger.info("  min_score_floor=%s",
                args.min_score_floor if args.min_score_floor is not None else "disabled")
    logger.info("=" * 60)

    # --- One-time setup ---
    csi_cfg = config.get("mean_reversion", {}).get("regime", {}) or config.get("sniper", {}).get("regime", {}) or {}
    csi_symbol = csi_cfg.get("csi300_symbol", "000300.SH")
    bundle = None
    pit_schedule = None

    if args.input_bundle:
        bundle = validate_input_bundle(
            args.input_bundle,
            acceptance_mode=args.acceptance_mode,
            support_tickers=(csi_symbol,),
        )
        universe = bundle.tickers
        data_map = {ticker: bundle.data_map[ticker] for ticker in universe}
        csi300_full = bundle.data_map[csi_symbol]
        info_map = bundle.basic_info
        report = {"bad_tickers": [], "reasons": {}}
        snapshot_dir = None
        price_snapshot_mode = "bundle"
        logger.info("Input bundle: id=%s hash=%s pit_grade=false (%d tickers)", bundle.bundle_id, bundle.composite_sha256, len(universe))
    else:
        _, download_daily_range_fn, provider_config, _ = get_data_functions(config)
        if args.universe_source == "watchlist":
            universe = load_execution_watchlist_tickers_in_range(args.outputs_root, start_date=args.start, end_date=args.end)
            if not universe:
                logger.error("No execution watchlist tickers found under %s for %s to %s", args.outputs_root, args.start, args.end)
                return 1
        elif args.universe_mode == "point_in_time":
            pit_schedule = build_pit_universe_schedule(start_date, end_date, n=args.universe_n, rebalance_months=args.universe_rebalance_months, provider_config=provider_config)
            if not pit_schedule:
                logger.error("Point-in-time universe build returned no members.")
                return 1
            universe = sorted({ticker for _, members in pit_schedule for ticker in members})
        else:
            universe = get_top_n_cn_by_market_cap(n=args.universe_n, provider_config=provider_config)

        max_hold = max(int(config.get("mean_reversion", {}).get("holding_period", 3)), int(config.get("sniper", {}).get("holding_period", 7)))
        dl_start_str = (start_date - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        dl_end_str = (end_date + timedelta(days=max_hold + 15)).strftime("%Y-%m-%d")
        snapshot_dir = Path(args.price_snapshot_dir) if args.price_snapshot_dir else None
        price_snapshot_mode = "read_write" if snapshot_dir else "off"
        csi300_full = _read_price_snapshot(snapshot_dir, csi_symbol) if snapshot_dir else pd.DataFrame()
        if csi300_full.empty:
            csi300_data, _ = download_daily_range_fn(tickers=[csi_symbol], start=dl_start_str, end=dl_end_str, provider_config=provider_config)
            csi300_full = csi300_data.get(csi_symbol, pd.DataFrame())
            if snapshot_dir and not csi300_full.empty:
                _write_price_snapshot(snapshot_dir, csi_symbol, csi300_full)

        snapshot_data, snapshot_missing = ({}, list(universe))
        if snapshot_dir:
            snapshot_data, snapshot_missing = _load_price_snapshot(snapshot_dir, universe)
        if snapshot_missing:
            downloaded_map, report = download_daily_range_fn(tickers=snapshot_missing, start=dl_start_str, end=dl_end_str, provider_config=provider_config)
            if snapshot_dir:
                for ticker, frame in downloaded_map.items():
                    _write_price_snapshot(snapshot_dir, ticker, frame)
            data_map = {**snapshot_data, **downloaded_map}
        else:
            report = {"bad_tickers": [], "reasons": {}}
            data_map = snapshot_data
        if not data_map:
            logger.error("Aborting backtest: no OHLCV data downloaded for %d universe tickers.", len(universe))
            return 1

        info_map: dict[str, dict] = {}
        if args.acceptance_mode == "live_equivalent":
            from src.core.cn_data import get_cn_basic_info
            info_map = get_cn_basic_info(universe, provider_config=provider_config)

    csi300_full = csi300_full.rename(columns={column: column.lower() for column in csi300_full.columns if column in ("Open", "High", "Low", "Close", "Volume")})

    # --- Config ---
    mr_config = config.get("mean_reversion", {})
    sniper_config = config.get("sniper", {})
    alpha_config = config.get("alpha_candidates", {}) or {}
    alpha_engines = set(alpha_config.get("engines", []))

    def alpha_engine_enabled(name: str) -> bool:
        """Require both engines-list membership and per-engine opt-in for research alphas."""
        return name in alpha_engines and bool((alpha_config.get(name) or {}).get("enabled", False))
    sma_short = int(get_config_value(config, "mean_reversion", "regime", "sma_short", default=20))
    sma_long = int(get_config_value(config, "mean_reversion", "regime", "sma_long", default=50))

    # --- Precompute features and date indices for all tickers (avoids repeated slicing) ---
    logger.info("Precomputing technical features for %d tickers...", len(data_map))
    t_pre = time.time()
    feat_map: dict[str, pd.DataFrame] = {}  # ticker -> full feature DataFrame
    date_pos_map: dict[str, dict[date, int]] = {}  # ticker -> {date: iloc position}
    breadth_above_sma20_map: dict[str, dict[date, bool]] = {}  # ticker -> {date: above_sma20}

    for ticker, full_df in data_map.items():
        if full_df.empty:
            continue
        try:
            feat_df = compute_all_technical_features(full_df)
            feat_df = compute_rsi2_features(feat_df)
            feat_map[ticker] = feat_df

            # Build date -> iloc position map
            idx = feat_df.index
            dates = idx.date if hasattr(idx, 'date') else pd.to_datetime(idx).date
            date_pos_map[ticker] = {d: i for i, d in enumerate(dates)}

            # Precompute breadth: close > SMA20 for each date
            close_col = "close" if "close" in feat_df.columns else "Close"
            close_series = feat_df[close_col].astype(float)
            sma20 = close_series.rolling(20, min_periods=20).mean()
            above = (close_series > sma20)
            breadth_above_sma20_map[ticker] = {
                d: bool(above.iloc[i]) if pd.notna(above.iloc[i]) else False
                for i, d in enumerate(dates)
            }
        except Exception:
            continue

    # Precompute CSI 300 date positions
    csi_dates = csi300_full.index.date if hasattr(csi300_full.index, 'date') else pd.to_datetime(csi300_full.index).date
    csi_date_pos = {d: i for i, d in enumerate(csi_dates)}

    logger.info("Precompute done: %d tickers (%.1f min)", len(feat_map), (time.time() - t_pre) / 60)

    # --- Iterate trading days ---
    all_results = []
    day_summaries = []
    dumped_picks = []  # pinned pick set for reproducible exit-logic replay
    cooldown_until: dict[str, int] = {}
    t_start = time.time()

    # Engine filter (computed once). Mirrors live scanner config gates.
    run_mr, run_sniper, sniper_requested, run_alpha, alpha_requested = resolve_backtest_engines(args.engines, config)
    if sniper_requested and not run_sniper:
        logger.warning("Sniper requested by --engines=%s but disabled by config; skipping sniper.", args.engines)
    if alpha_requested and not run_alpha:
        logger.warning("Alpha requested by --engines=%s but disabled by config; skipping alpha.", args.engines)
    logger.info("Engines: mr=%s sniper=%s alpha=%s", run_mr, run_sniper, run_alpha)

    min_bars = int(mr_config.get("min_bars", 60))
    regime_filtered_days = 0

    for i, scan_date in enumerate(trading_days):
        date_str = scan_date.strftime("%Y-%m-%d")

        # Classify regime from CSI 300 up to this date (fast iloc slice)
        csi_pos = csi_date_pos.get(scan_date)
        if csi_pos is not None:
            csi_slice = csi300_full.iloc[:csi_pos + 1]
        else:
            # Fallback: find nearest date <= scan_date
            valid_dates = [d for d in csi_date_pos if d <= scan_date]
            if valid_dates:
                nearest = max(valid_dates)
                csi_slice = csi300_full.iloc[:csi_date_pos[nearest] + 1]
            else:
                csi_slice = csi300_full.iloc[:0]
        regime = _classify_regime(csi_slice, sma_short, sma_long)

        if (allowed_regimes and regime not in allowed_regimes) or regime in excluded_regimes:
            regime_filtered_days += 1
            day_summaries.append({
                "date": date_str, "regime": regime, "picks": 0,
                "wins": 0, "losses": 0, "holds": 0, "no_data": 0,
                "acceptance_mode": "regime_filtered",
                "eligible_count": 0,
            })
            if (i + 1) % 20 == 0:
                elapsed = time.time() - t_start
                rate = elapsed / (i + 1)
                eta = rate * (len(trading_days) - i - 1)
                logger.info("[%d/%d] %s: %s skipped by regime filter (%.1fs/day, ETA %.0fm)",
                            i + 1, len(trading_days), date_str, regime, rate, eta / 60)
            continue

        # Point-in-time universe gate: restrict to members as of this scan day.
        pit_members = active_pit_universe(pit_schedule, scan_date) if pit_schedule else None

        # Score all tickers using precomputed features up to scan_date
        all_signals = []
        for ticker, feat_df in feat_map.items():
            try:
                if pit_members is not None and ticker not in pit_members:
                    continue
                pos = date_pos_map[ticker].get(scan_date)
                if pos is None:
                    # Find nearest date <= scan_date
                    valid = [d for d in date_pos_map[ticker] if d <= scan_date]
                    if not valid:
                        continue
                    pos = date_pos_map[ticker][max(valid)]
                if pos + 1 < min_bars:
                    continue

                hist_df = feat_df.iloc[:pos + 1]
                feats = {k: (None if pd.isna(v) else float(v) if isinstance(v, (float, np.floating)) else v)
                         for k, v in feat_df.iloc[pos].items()}
                if not feats:
                    continue

                mr_signal = None
                if run_mr:
                    mr_subtype, mr_exit_params = resolve_mr_subtype_and_exit_params(mr_config, feats)
                    mr_signal = score_mean_reversion(
                        ticker=ticker, df=feat_df, features=feats, regime=regime,
                        disable_gap_filter=args.disable_gap_filter,
                        target_mode=args.mr_target,
                        rsi2_max=float(mr_config.get("rsi2_max", 5)),
                        adv_min_cny=float(mr_config.get("adv_min_cny", 100_000_000)),
                        score_floor=float(mr_config.get("score_floor", 65)),
                        min_bars=int(mr_config.get("min_bars", 60)),
                        max_single_day_move=float(mr_config.get("max_single_day_move", 0.11)),
                        stop_atr_mult=mr_exit_params["stop_atr_mult"],
                        target_1_atr_mult=mr_exit_params["target_1_atr_mult"],
                        target_2_atr_mult=mr_exit_params["target_2_atr_mult"],
                        max_entry_atr_mult=mr_exit_params["max_entry_atr_mult"],
                        holding_period=mr_exit_params["holding_period"],
                        subtype=mr_subtype,
                    )
                if mr_signal:
                    all_signals.append(("mean_reversion", mr_signal))

                # Sniper
                sniper_signal = None
                if run_sniper:
                    sniper_signal = score_sniper(
                        ticker=ticker, df=feat_df, features=feats, regime=regime,
                        csi300_df=csi_slice,
                        atr_pct_floor=float(sniper_config.get("atr_pct_floor", 3.5)),
                        min_avg_volume=int(sniper_config.get("min_avg_volume", 500_000)),
                        stop_atr_mult=float(sniper_config.get("stop_atr_mult", 2.0)),
                        target_atr_mult=float(sniper_config.get("target_atr_mult", 3.0)),
                        target_2_atr_mult=float(sniper_config.get("target_2_atr_mult", 5.0)),
                        holding_period=int(sniper_config.get("holding_period", 7)),
                        max_entry_pct=float(sniper_config.get("max_entry_pct", 0.02)),
                    )
                if sniper_signal:
                    all_signals.append(("sniper", sniper_signal))

                # Research alpha candidates (disabled by default; must be explicitly enabled and requested).
                if run_alpha:
                    is_st = info_map.get(ticker, {}).get("is_st", False)
                    if alpha_engine_enabled("rs_pullback"):
                        rs_cfg = alpha_config.get("rs_pullback", {}) or {}
                        rs_signal = score_rs_pullback_alpha(
                            ticker=ticker,
                            df=hist_df,
                            regime=regime,
                            csi300_df=csi_slice,
                            is_st=is_st,
                            regimes=tuple(rs_cfg.get("regimes", ["bull", "bear"])),
                            score_floor=float(rs_cfg.get("score_floor", 80.0)),
                            max_entry_pct=float(rs_cfg.get("max_entry_pct", 0.02)),
                            min_adv_cny=float(rs_cfg.get("min_adv_cny", 80_000_000)),
                            stop_atr_mult=float(rs_cfg.get("stop_atr_mult", 1.1)),
                            target_atr_mult=float(rs_cfg.get("target_atr_mult", 2.1)),
                            target_2_atr_mult=float(rs_cfg.get("target_2_atr_mult", 3.6)),
                            holding_period=int(rs_cfg.get("holding_period", 5)),
                        )
                        if rs_signal:
                            all_signals.append(("alpha_rs_pullback", rs_signal))

                    if alpha_engine_enabled("sniper_breakout"):
                        bo_cfg = alpha_config.get("sniper_breakout", {}) or {}
                        bo_signal = score_sniper_breakout_alpha(
                            ticker=ticker,
                            df=hist_df,
                            regime=regime,
                            csi300_df=csi_slice,
                            is_st=is_st,
                            regimes=tuple(bo_cfg.get("regimes", ["bull", "bear", "choppy"])),
                            score_floor=float(bo_cfg.get("score_floor", 74.0)),
                            max_entry_pct=float(bo_cfg.get("max_entry_pct", 0.03)),
                            min_adv_cny=float(bo_cfg.get("min_adv_cny", 80_000_000)),
                            stop_atr_mult=float(bo_cfg.get("stop_atr_mult", 1.6)),
                            target_atr_mult=float(bo_cfg.get("target_atr_mult", 3.2)),
                            target_2_atr_mult=float(bo_cfg.get("target_2_atr_mult", 4.7)),
                            holding_period=int(bo_cfg.get("holding_period", 5)),
                            min_stop_risk_pct=float(bo_cfg.get("min_stop_risk_pct", 0.0)),
                            max_stop_risk_pct=float(bo_cfg.get("max_stop_risk_pct", 99.0)),
                        )
                        if bo_signal:
                            all_signals.append(("alpha_sniper_breakout", bo_signal))

                    if alpha_engine_enabled("limitup_continuation"):
                        lu_cfg = alpha_config.get("limitup_continuation", {}) or {}
                        lu_signal = score_limitup_continuation_alpha(
                            ticker=ticker,
                            df=hist_df,
                            regime=regime,
                            csi300_df=csi_slice,
                            is_st=is_st,
                            regimes=tuple(lu_cfg.get("regimes", ["bull"])),
                            score_floor=float(lu_cfg.get("score_floor", 78.0)),
                            max_entry_pct=float(lu_cfg.get("max_entry_pct", 0.02)),
                            min_adv_cny=float(lu_cfg.get("min_adv_cny", 80_000_000)),
                            stop_atr_mult=float(lu_cfg.get("stop_atr_mult", 1.25)),
                            target_atr_mult=float(lu_cfg.get("target_atr_mult", 2.4)),
                            target_2_atr_mult=float(lu_cfg.get("target_2_atr_mult", 3.8)),
                            holding_period=int(lu_cfg.get("holding_period", 4)),
                            max_atr_pct=float(lu_cfg.get("max_atr_pct", 7.5)),
                            min_rs20=float(lu_cfg.get("min_rs20", 3.0)),
                            min_post_impulse_hold=float(lu_cfg.get("min_post_impulse_hold", -0.075)),
                            max_post_impulse_extension=float(lu_cfg.get("max_post_impulse_extension", 0.055)),
                        )
                        if lu_signal:
                            all_signals.append(("alpha_limitup_continuation", lu_signal))

                    if alpha_engine_enabled("accumulation_breakout"):
                        acc_cfg = alpha_config.get("accumulation_breakout", {}) or {}
                        acc_signal = score_accumulation_breakout_alpha(
                            ticker=ticker,
                            df=hist_df,
                            regime=regime,
                            csi300_df=csi_slice,
                            is_st=is_st,
                            regimes=tuple(acc_cfg.get("regimes", ["bull"])),
                            score_floor=float(acc_cfg.get("score_floor", 76.0)),
                            max_entry_pct=float(acc_cfg.get("max_entry_pct", 0.025)),
                            min_adv_cny=float(acc_cfg.get("min_adv_cny", 80_000_000)),
                            stop_atr_mult=float(acc_cfg.get("stop_atr_mult", 1.2)),
                            target_atr_mult=float(acc_cfg.get("target_atr_mult", 2.3)),
                            target_2_atr_mult=float(acc_cfg.get("target_2_atr_mult", 3.6)),
                            holding_period=int(acc_cfg.get("holding_period", 5)),
                            min_breakout_proximity=float(acc_cfg.get("min_breakout_proximity", 0.985)),
                            max_sma20_extension=float(acc_cfg.get("max_sma20_extension", 1.075)),
                            min_rvol=float(acc_cfg.get("min_rvol", 1.15)),
                            max_dryup=float(acc_cfg.get("max_dryup", 1.15)),
                        )
                        if acc_signal:
                            all_signals.append(("alpha_accumulation_breakout", acc_signal))

            except Exception:
                continue

        # Dedupe: keep higher score per ticker
        best: dict[str, tuple[str, object]] = {}
        for engine, sig in all_signals:
            existing = best.get(sig.ticker)
            if existing is None or sig.score > existing[1].score:
                best[sig.ticker] = (engine, sig)

        # Sort by configured selection priority before top-N / acceptance allocation.
        sorted_picks = rank_picks(list(best.values()), args.pick_rank_mode)
        sorted_picks = apply_score_floor(sorted_picks, args.min_score_floor)
        sorted_picks = apply_ticker_cooldowns(sorted_picks, i, cooldown_until)

        # --- Pick selection: three modes ---
        # Defaults; overridden by acceptance paths below
        day_acceptance_mode = "off"
        day_eligible_count = len(sorted_picks)

        if args.acceptance_mode == "off":
            # Legacy: simple top-N
            final_picks = sorted_picks[:args.top_n]

        else:
            # Both acceptance modes need breadth (from precomputed map — no slicing)
            above_sma20_count = 0
            breadth_denom = 0
            for ticker in feat_map:
                ticker_breadth = breadth_above_sma20_map.get(ticker, {})
                # Find this date or nearest prior
                val = ticker_breadth.get(scan_date)
                if val is None:
                    # Ticker doesn't have data for this date — check if it has enough history
                    pos = date_pos_map.get(ticker, {}).get(scan_date)
                    if pos is None:
                        valid = [d for d in date_pos_map.get(ticker, {}) if d <= scan_date]
                        pos = date_pos_map[ticker][max(valid)] if valid else None
                    if pos is None or pos < 19:
                        continue
                    # Use nearest prior date's breadth
                    valid_dates = [d for d in ticker_breadth if d <= scan_date]
                    if not valid_dates:
                        continue
                    val = ticker_breadth[max(valid_dates)]
                breadth_denom += 1
                if val:
                    above_sma20_count += 1
            above_sma20 = above_sma20_count / max(breadth_denom, 1)

            acceptance_cfg = config.get("acceptance", {})

            if args.acceptance_mode == "engine_only":
                # Acceptance on raw deduped set — no scanner policy filters
                acceptance_result = run_acceptance(
                    candidates=sorted_picks,
                    breadth_pct=above_sma20,
                    regime=regime,
                    universe_size=len(data_map),
                    config=acceptance_cfg,
                )
                final_picks = [(e, s) for e, s, _tier in acceptance_result.accepted]
                day_acceptance_mode = acceptance_result.mode
                day_eligible_count = acceptance_result.eligible_count

            else:
                # live_equivalent: replicate scanner funnel then acceptance
                # Stage 1: Breadth suppression
                breadth_floor = float(config.get("book_size", {}).get("breadth_floor", 0.30))
                if above_sma20 < breadth_floor:
                    final_picks = []
                    day_acceptance_mode = "breadth_suppressed"
                    day_eligible_count = len(sorted_picks)  # preserve pre-breadth opportunity set
                else:
                    # Stage 2: Regime-specific score floor
                    book_cfg = config.get("book_size", {}).get(regime, {})
                    min_score = float(book_cfg.get("min_score", 0)) if isinstance(book_cfg, dict) else 0
                    quality_picks = [p for p in sorted_picks if p[1].score >= min_score]

                    # Stage 3: Limit-down veto (using precomputed features)
                    non_limit_picks = []
                    for engine, sig in quality_picks:
                        t_feat = feat_map.get(sig.ticker)
                        t_pos = date_pos_map.get(sig.ticker, {}).get(scan_date)
                        if t_feat is not None and t_pos is not None and t_pos >= 1:
                            close_col = "close" if "close" in t_feat.columns else "Close"
                            c = float(t_feat[close_col].iloc[t_pos])
                            pc = float(t_feat[close_col].iloc[t_pos - 1])
                            limit_pct = get_daily_limit(sig.ticker,
                                                        is_st=info_map.get(sig.ticker, {}).get("is_st", False))
                            if (c / pc - 1) <= (-limit_pct + 0.001):
                                continue
                        non_limit_picks.append((engine, sig))

                    # Stage 4: Sector cap
                    max_per_sector = int(config.get("book_size", {}).get("max_per_sector", 1))
                    sector_counts: dict[str, int] = {}
                    sector_filtered: list[tuple[str, object]] = []
                    for engine, sig in non_limit_picks:
                        industry = (info_map.get(sig.ticker, {}) or {}).get("industry", "unknown") or "unknown"
                        if sector_counts.get(industry, 0) < max_per_sector:
                            sector_filtered.append((engine, sig))
                            sector_counts[industry] = sector_counts.get(industry, 0) + 1

                    # Stage 5: Acceptance allocator on full post-veto set
                    acceptance_result = run_acceptance(
                        candidates=sector_filtered,
                        breadth_pct=above_sma20,
                        regime=regime,
                        universe_size=len(data_map),
                        config=acceptance_cfg,
                        info_map=info_map,
                    )
                    final_picks = [(e, s) for e, s, _tier in acceptance_result.accepted]
                    day_acceptance_mode = acceptance_result.mode
                    day_eligible_count = acceptance_result.eligible_count

        if not final_picks:
            day_summaries.append({
                "date": date_str, "regime": regime, "picks": 0,
                "wins": 0, "losses": 0, "holds": 0, "no_data": 0,
                "acceptance_mode": day_acceptance_mode,
                "eligible_count": day_eligible_count,
            })
            if (i + 1) % 20 == 0:
                elapsed = time.time() - t_start
                rate = elapsed / (i + 1)
                eta = rate * (len(trading_days) - i - 1)
                logger.info("[%d/%d] %s — no picks (%.1fs/day, ETA %.0fm)",
                            i + 1, len(trading_days), date_str, rate, eta / 60)
            continue

        # Evaluate picks using forward data from the cached bulk download
        wins = losses = holds = no_data = 0
        for engine, sig in final_picks:
            if args.dump_picks:
                dumped_picks.append({
                    "baseline_date": date_str,
                    "regime": regime,
                    "engine": engine,
                    "ticker": sig.ticker,
                    "name_cn": getattr(sig, "name_cn", None) or getattr(sig, "name", None),
                    "score": getattr(sig, "score", None),
                    "planned_entry_price": getattr(sig, "entry_price", None),
                    "max_entry_price": getattr(sig, "max_entry_price", None),
                    "stop_loss": getattr(sig, "stop_loss", None),
                    "target_1": getattr(sig, "target_1", None),
                    "holding_period": getattr(sig, "holding_period", None),
                })
            fwd_df = _slice_from(data_map.get(sig.ticker, pd.DataFrame()), scan_date)

            # Determine exit mode: trailing for sniper unless disabled
            pick_exit_mode = args.exit_mode
            pick_atr = 0.0
            if (engine == "sniper" and args.exit_mode == "target_stop"
                    and not args.no_sniper_trailing):
                pick_exit_mode = "trailing"
                # Estimate ATR from stop distance / multiplier
                pick_atr = abs(sig.entry_price - sig.stop_loss) / float(
                    sniper_config.get("stop_atr_mult", 2.0))
            elif args.exit_mode in ("runner", "runner_laggard"):
                # True ATR(14) from history up to scan_date; fall back to stop distance.
                pick_atr = _atr14_up_to(data_map.get(sig.ticker, pd.DataFrame()), scan_date)
                if pick_atr <= 0:
                    pick_atr = abs(sig.entry_price - sig.stop_loss)

            eval_result = evaluate_pick(
                ticker=sig.ticker,
                entry_price=sig.entry_price,
                stop_loss=sig.stop_loss,
                target_1=sig.target_1,
                holding_period=sig.holding_period,
                forward_df=fwd_df,
                exit_mode=pick_exit_mode,
                engine=engine,
                atr=pick_atr,
                max_entry_price=getattr(sig, "max_entry_price", None),
                entry_mode=args.entry_mode,
                runner_max_hold=args.runner_max_hold,
                trail_atr=args.trail_atr,
                breakeven_at=args.breakeven_at,
                laggard_day=args.laggard_day,
                laggard_thresh=args.laggard_thresh,
            )
            eval_result["date"] = date_str
            eval_result["engine"] = engine
            eval_result["score"] = sig.score
            eval_result["regime"] = regime
            eval_result["subtype"] = getattr(sig, "subtype", None)
            all_results.append(eval_result)

            cooldown_ticker = getattr(sig, "ticker", "")
            if args.ticker_cooldown_days > 0 and cooldown_ticker:
                cooldown_until[cooldown_ticker] = max(cooldown_until.get(cooldown_ticker, -1), i + args.ticker_cooldown_days)
            exit_reason = eval_result.get("exit_reason")
            if _is_stop_exit(exit_reason) and args.post_stop_cooldown_days > 0 and cooldown_ticker:
                cooldown_until[cooldown_ticker] = max(cooldown_until.get(cooldown_ticker, -1), i + args.post_stop_cooldown_days)

            outcome_bucket = _daily_outcome_bucket(exit_reason)
            if outcome_bucket == "win":
                wins += 1
            elif outcome_bucket == "loss":
                losses += 1
            elif outcome_bucket == "hold":
                holds += 1
            else:
                no_data += 1

        day_summaries.append({
            "date": date_str, "regime": regime,
            "picks": len(final_picks),
            "wins": wins, "losses": losses, "holds": holds, "no_data": no_data,
            "acceptance_mode": day_acceptance_mode,
            "eligible_count": day_eligible_count,
        })

        elapsed = time.time() - t_start
        rate = elapsed / (i + 1)
        eta = rate * (len(trading_days) - i - 1)
        logger.info("[%d/%d] %s: %s %dp %dw %dl %dh (%.1fs/day, ETA %.0fm)",
                    i + 1, len(trading_days), date_str, regime,
                    len(final_picks), wins, losses, holds, rate, eta / 60)

    # --- Aggregate stats ---
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_time = time.time() - t_start
    logger.info("Scan loop complete: %.1f min (%.2fs/day avg)",
                total_time / 60, total_time / max(len(trading_days), 1))

    suffix = f"_{args.label}" if args.label else ""
    acceptance_mode_counts: dict[str, int] = {}
    breadth_suppressed_days = 0
    for d in day_summaries:
        m = d.get("acceptance_mode", "off")
        if m == "breadth_suppressed":
            breadth_suppressed_days += 1
        else:
            acceptance_mode_counts[m] = acceptance_mode_counts.get(m, 0) + 1

    artifact_input_stamp = {
        "input_mode": "bundle" if bundle else "live",
        "bundle_id": bundle.bundle_id if bundle else None,
        "bundle_composite_sha256": bundle.composite_sha256 if bundle else None,
        "pit_grade": False if bundle else None,
    }
    for result in all_results:
        result.update(artifact_input_stamp)
    for day_summary in day_summaries:
        day_summary.update(artifact_input_stamp)
    for dumped_pick in dumped_picks:
        dumped_pick.update(artifact_input_stamp)

    if not all_results:
        logger.warning("No results to summarize.")
        summary = {
            "created_at": datetime.utcnow().isoformat() + "Z",
            **artifact_input_stamp,
            "config_path": args.config,
            "label": args.label,
            "start": args.start,
            "end": args.end,
            "period": f"{args.start} to {args.end}",
            "exit_mode": args.exit_mode,
            "entry_mode": args.entry_mode,
            "mr_target_mode": args.mr_target,
            "engines": args.engines,
            "acceptance_mode": args.acceptance_mode,
            "universe_source": args.universe_source,
            "outputs_root": args.outputs_root if args.universe_source == "watchlist" else None,
            "allowed_regimes": sorted(allowed_regimes),
            "excluded_regimes": sorted(excluded_regimes),
            "min_score_floor": args.min_score_floor,
            "pick_rank_mode": args.pick_rank_mode,
            "ticker_cooldown_days": args.ticker_cooldown_days,
            "post_stop_cooldown_days": args.post_stop_cooldown_days,
            "trading_days_scanned": len(trading_days),
            "regime_filtered_days": regime_filtered_days,
            "days_with_picks": 0,
            "zero_pick_days": len(trading_days),
            "zero_pick_days_pct": 1.0 if trading_days else 0.0,
            "avg_picks_per_active_day": 0.0,
            "total_picks": 0,
            "entry_skipped_no_chase": 0,
            "target_hits": 0,
            "stop_hits": 0,
            "hold_expired": 0,
            "pnl_win_rate": 0.0,
            "target_hit_rate": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "true_expectancy_pct": 0.0,
            "weighted_expectancy_pct": 0.0,
            "hold_expired_positive_pct": 0.0,
            "day1_stop_count": 0,
            "profitable_days_pct": 0.0,
            "exit_day_distribution": {},
            "max_drawdown_pct": 0.0,
            "cumulative_pnl_pct": 0.0,
            "final_equity_multiple": 1.0,
            "total_time_min": round(total_time / 60, 1),
            "breadth_suppressed_days": breadth_suppressed_days,
            "acceptance_mode_counts": acceptance_mode_counts,
            "price_data_fingerprint": _stable_price_fingerprint(data_map),
            "price_snapshot_mode": price_snapshot_mode,
            "price_snapshot_dir": str(snapshot_dir) if snapshot_dir else None,
            "download_bad_tickers": sorted(report.get("bad_tickers", [])),
            "download_bad_ticker_count": len(report.get("bad_tickers", [])),
            "per_engine": {},
            "per_regime": {},
            "per_subtype": {},
        }
        summary_path = out_dir / f"backtest_summary{suffix}.json"
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        detail_path = out_dir / f"backtest_detail{suffix}.csv"
        pd.DataFrame(all_results).to_csv(detail_path, index=False)
        daily_path = out_dir / f"backtest_daily{suffix}.csv"
        pd.DataFrame(day_summaries).to_csv(daily_path, index=False)
        if args.dump_picks:
            # Preserve the promised provenance artifact even when no day produced picks.
            pd.DataFrame(dumped_picks, columns=list(artifact_input_stamp)).to_csv(args.dump_picks, index=False)
            logger.info("Pinned picks (%d): %s", len(dumped_picks), args.dump_picks)
        logger.info("Summary: %s", summary_path)
        logger.info("Detail: %s", detail_path)
        logger.info("Daily: %s", daily_path)
        return 0

    df = pd.DataFrame(all_results)
    df_valid = df[df["pnl_pct"].notna()].copy()

    total = len(df_valid)
    entry_skipped_no_chase = int((df["exit_reason"] == "entry_skipped_no_chase").sum())
    target_hits = int(df_valid["hit_target"].sum())
    stop_hits = int(df_valid["hit_stop"].sum())
    hold_expired = int((df_valid["exit_reason"] == "hold_expired").sum())

    # PnL-based win rate (the real measure of profitability)
    pnl_win_rate = (df_valid["pnl_pct"] > 0).sum() / total if total > 0 else 0
    # Target hit rate (how often we reach the ambitious target)
    target_hit_rate = target_hits / total if total > 0 else 0
    avg_win = float(df_valid[df_valid["pnl_pct"] > 0]["pnl_pct"].mean()) if (df_valid["pnl_pct"] > 0).any() else 0
    avg_loss = float(df_valid[df_valid["pnl_pct"] <= 0]["pnl_pct"].mean()) if (df_valid["pnl_pct"] <= 0).any() else 0
    # True expectancy: simple average of all trade PnL (no population mismatch)
    true_expectancy = float(df_valid["pnl_pct"].mean()) if total > 0 else 0
    # Weighted expectancy for comparison
    weighted_expectancy = (pnl_win_rate * avg_win) + ((1 - pnl_win_rate) * avg_loss) if total > 0 else 0

    # Hold-expired trades that were actually profitable
    hold_expired_df = df_valid[df_valid["exit_reason"] == "hold_expired"]
    hold_expired_positive_pct = (
        (hold_expired_df["pnl_pct"] > 0).sum() / len(hold_expired_df)
        if len(hold_expired_df) > 0 else 0
    )

    # Day-1 stop count (gap-through losses)
    day1_stops = int(((df_valid["exit_reason"] == "stop_hit") & (df_valid["exit_day"] == 1)).sum())

    # Exit-day distribution for stop hits
    stop_df = df_valid[df_valid["exit_reason"] == "stop_hit"]
    exit_day_dist = {}
    for d in range(1, max(int(df_valid["exit_day"].max()) + 1 if total > 0 else 1, 8)):
        count = int((stop_df["exit_day"] == d).sum())
        if count > 0:
            exit_day_dist[f"day_{d}_stops"] = count

    # Profitable days percentage
    daily_pnl_by_date = df_valid.groupby("date")["pnl_pct"].mean()
    profitable_days_pct = (daily_pnl_by_date > 0).sum() / len(daily_pnl_by_date) if len(daily_pnl_by_date) > 0 else 0

    # App-level metrics
    days_with_picks = sum(1 for d in day_summaries if d["picks"] > 0)
    zero_pick_days = len(trading_days) - days_with_picks
    zero_pick_days_pct = zero_pick_days / len(trading_days) if len(trading_days) > 0 else 0
    avg_picks_per_active_day = total / days_with_picks if days_with_picks > 0 else 0

    # Per-engine breakdown
    engine_stats = {}
    for engine in df_valid["engine"].unique():
        edf = df_valid[df_valid["engine"] == engine]
        e_total = len(edf)
        e_pnl_wins = int((edf["pnl_pct"] > 0).sum())
        e_pnl_wr = e_pnl_wins / e_total if e_total > 0 else 0
        e_hr = int(edf["hit_target"].sum()) / e_total if e_total > 0 else 0
        e_avg_win = float(edf[edf["pnl_pct"] > 0]["pnl_pct"].mean()) if (edf["pnl_pct"] > 0).any() else 0
        e_avg_loss = float(edf[edf["pnl_pct"] <= 0]["pnl_pct"].mean()) if (edf["pnl_pct"] <= 0).any() else 0
        e_true_exp = float(edf["pnl_pct"].mean())
        e_day1_stops = int(((edf["exit_reason"] == "stop_hit") & (edf["exit_day"] == 1)).sum())
        e_hold_exp = edf[edf["exit_reason"] == "hold_expired"]
        e_hold_pos = (e_hold_exp["pnl_pct"] > 0).sum() / len(e_hold_exp) if len(e_hold_exp) > 0 else 0
        engine_stats[engine] = {
            "total": e_total, "pnl_wins": e_pnl_wins,
            "pnl_win_rate": round(e_pnl_wr, 4), "target_hit_rate": round(e_hr, 4),
            "avg_win_pct": round(e_avg_win, 2), "avg_loss_pct": round(e_avg_loss, 2),
            "true_expectancy_pct": round(e_true_exp, 2),
            "day1_stop_count": e_day1_stops,
            "hold_expired_positive_pct": round(e_hold_pos, 4),
        }

    # Per-regime breakdown
    regime_stats = {}
    for regime in df_valid["regime"].unique():
        rdf = df_valid[df_valid["regime"] == regime]
        r_total = len(rdf)
        r_pnl_wins = int((rdf["pnl_pct"] > 0).sum())
        r_pnl_wr = r_pnl_wins / r_total if r_total > 0 else 0
        r_true_exp = float(rdf["pnl_pct"].mean())
        r_day1_stops = int(((rdf["exit_reason"] == "stop_hit") & (rdf["exit_day"] == 1)).sum())
        regime_stats[regime] = {
            "total": r_total, "pnl_wins": r_pnl_wins,
            "pnl_win_rate": round(r_pnl_wr, 4),
            "true_expectancy_pct": round(r_true_exp, 2),
            "day1_stop_count": r_day1_stops,
        }

    # Per-subtype breakdown (MR only)
    subtype_stats = {}
    subtype_df = df_valid[df_valid["subtype"].notna()].copy()
    for subtype in subtype_df["subtype"].unique():
        sdf = subtype_df[subtype_df["subtype"] == subtype]
        s_total = len(sdf)
        s_pnl_wins = int((sdf["pnl_pct"] > 0).sum())
        s_pnl_wr = s_pnl_wins / s_total if s_total > 0 else 0
        s_true_exp = float(sdf["pnl_pct"].mean()) if s_total > 0 else 0
        s_day1_stops = int(((sdf["exit_reason"] == "stop_hit") & (sdf["exit_day"] == 1)).sum())
        subtype_stats[subtype] = {
            "total": s_total,
            "pnl_wins": s_pnl_wins,
            "pnl_win_rate": round(s_pnl_wr, 4),
            "true_expectancy_pct": round(s_true_exp, 2),
            "day1_stop_count": s_day1_stops,
        }

    # Equity curve (compounded daily equal-weighted returns)
    # Group trades by date and compute daily mean PnL (as decimal)
    daily_returns = df_valid.groupby("date")["pnl_pct"].mean() / 100.0
    # Fill in zeros for days with no picks among the trading days
    full_daily_series = pd.Series(0.0, index=[d.strftime("%Y-%m-%d") for d in trading_days])
    full_daily_series.update(daily_returns)

    # Compound to equity curve (start at 1.0)
    equity = (1 + full_daily_series).cumprod()
    equity_peak = equity.cummax()
    drawdown_series = (equity - equity_peak) / equity_peak
    max_dd = float(-drawdown_series.min()) * 100  # positive percentage
    cumulative_pnl_pct = float(equity.iloc[-1] - 1) * 100

    summary = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        **artifact_input_stamp,
        "config_path": args.config,
        "label": args.label if hasattr(args, "label") else None,
        "start": args.start,
        "end": args.end,
        "period": f"{args.start} to {args.end}",
        "exit_mode": args.exit_mode,
        "entry_mode": args.entry_mode,
        "mr_target_mode": args.mr_target,
        "engines": args.engines,
        "gap_filter_disabled": args.disable_gap_filter,
        "sniper_trailing_disabled": args.no_sniper_trailing,
        "acceptance_mode": args.acceptance_mode,
        "universe_source": args.universe_source,
        "outputs_root": args.outputs_root if args.universe_source == "watchlist" else None,
        "allowed_regimes": sorted(allowed_regimes),
        "excluded_regimes": sorted(excluded_regimes),
        "min_score_floor": args.min_score_floor,
        "pick_rank_mode": args.pick_rank_mode,
        "ticker_cooldown_days": args.ticker_cooldown_days,
        "post_stop_cooldown_days": args.post_stop_cooldown_days,
        "trading_days_scanned": len(trading_days),
        "regime_filtered_days": regime_filtered_days,
        "days_with_picks": days_with_picks,
        "zero_pick_days": zero_pick_days,
        "zero_pick_days_pct": round(zero_pick_days_pct, 4),
        "avg_picks_per_active_day": round(avg_picks_per_active_day, 2),
        "total_picks": total,
        "entry_skipped_no_chase": entry_skipped_no_chase,
        "target_hits": target_hits,
        "stop_hits": stop_hits,
        "hold_expired": hold_expired,
        "pnl_win_rate": round(pnl_win_rate, 4),
        "target_hit_rate": round(target_hit_rate, 4),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "true_expectancy_pct": round(true_expectancy, 2),
        "weighted_expectancy_pct": round(weighted_expectancy, 2),
        "hold_expired_positive_pct": round(hold_expired_positive_pct, 4),
        "day1_stop_count": day1_stops,
        "profitable_days_pct": round(profitable_days_pct, 4),
        "exit_day_distribution": exit_day_dist,
        "max_drawdown_pct": round(max_dd, 2),
        "cumulative_pnl_pct": round(cumulative_pnl_pct, 2),
        "final_equity_multiple": round(float(equity.iloc[-1]), 2),
        "total_time_min": round(total_time / 60, 1),
        "breadth_suppressed_days": breadth_suppressed_days,
        "acceptance_mode_counts": acceptance_mode_counts,
        "price_data_fingerprint": _stable_price_fingerprint(data_map),
        "price_snapshot_mode": price_snapshot_mode,
        "price_snapshot_dir": str(snapshot_dir) if snapshot_dir else None,
        "download_bad_tickers": sorted(report.get("bad_tickers", [])),
        "download_bad_ticker_count": len(report.get("bad_tickers", [])),
        "per_engine": engine_stats,
        "per_regime": regime_stats,
        "per_subtype": subtype_stats,
    }

    # Print summary
    logger.info("=" * 60)
    logger.info("BACKTEST SUMMARY: %s to %s (exit_mode=%s, entry_mode=%s)",
                args.start, args.end, args.exit_mode, args.entry_mode)
    logger.info("=" * 60)
    logger.info("Total picks evaluated: %d | No-chase skipped: %d", total, entry_skipped_no_chase)
    logger.info("PnL win rate: %.1f%% | Target hit rate: %.1f%%", pnl_win_rate * 100, target_hit_rate * 100)
    logger.info("Avg win: +%.2f%% | Avg loss: %.2f%%", avg_win, avg_loss)
    logger.info("True expectancy: %.2f%% | Weighted: %.2f%%", true_expectancy, weighted_expectancy)
    logger.info("Day-1 stops: %d (%.1f%%) | Hold-expired positive: %.1f%%",
                day1_stops, day1_stops / total * 100 if total > 0 else 0,
                hold_expired_positive_pct * 100)
    logger.info("Profitable days: %.1f%% | Zero-pick days: %d (%.1f%%) | Avg picks/active day: %.1f",
                profitable_days_pct * 100, zero_pick_days, zero_pick_days_pct * 100, avg_picks_per_active_day)
    logger.info("Max drawdown: %.2f%% | Cumulative PnL: %.2f%% | Equity: %.2fx", max_dd, cumulative_pnl_pct, float(equity.iloc[-1]))
    if exit_day_dist:
        logger.info("Stop exit-day dist: %s", exit_day_dist)
    logger.info("")
    for engine, stats in engine_stats.items():
        logger.info("  %s: n=%d WR=%.1f%% HR=%.1f%% trueExp=%.2f%% d1stops=%d holdPos=%.0f%%",
                     engine, stats["total"], stats["pnl_win_rate"] * 100, stats["target_hit_rate"] * 100,
                     stats["true_expectancy_pct"], stats["day1_stop_count"],
                     stats["hold_expired_positive_pct"] * 100)
    logger.info("")
    for regime_name, stats in regime_stats.items():
        logger.info("  %s: n=%d WR=%.1f%% trueExp=%.2f%% d1stops=%d",
                     regime_name, stats["total"], stats["pnl_win_rate"] * 100,
                     stats["true_expectancy_pct"], stats["day1_stop_count"])
    if subtype_stats:
        logger.info("")
        for subtype, stats in subtype_stats.items():
            logger.info("  subtype=%s: n=%d WR=%.1f%% trueExp=%.2f%% d1stops=%d",
                        subtype, stats["total"], stats["pnl_win_rate"] * 100,
                        stats["true_expectancy_pct"], stats["day1_stop_count"])

    # Save artifacts
    summary_path = out_dir / f"backtest_summary{suffix}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info("Summary: %s", summary_path)

    detail_path = out_dir / f"backtest_detail{suffix}.csv"
    df.to_csv(detail_path, index=False)
    logger.info("Detail: %s", detail_path)

    daily_path = out_dir / f"backtest_daily{suffix}.csv"
    pd.DataFrame(day_summaries).to_csv(daily_path, index=False)
    logger.info("Daily: %s", daily_path)

    if args.dump_picks:
        pd.DataFrame(dumped_picks).to_csv(args.dump_picks, index=False)
        logger.info("Pinned picks (%d): %s", len(dumped_picks), args.dump_picks)

    return 0


if __name__ == "__main__":
    sys.exit(main())
