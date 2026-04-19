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
from src.core.data import get_data_functions
from src.core.universe import get_top_n_cn_by_market_cap
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

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 400  # technical indicators need history


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
) -> dict:
    """Evaluate a single pick against forward price data.

    T+1 rule: no same-day exits. Evaluation starts from day 1.

    Exit modes:
        target_stop     — original: exit on target or stop hit, hold_expired at end
        profitable_close — exit on close > entry after day 2+ (captures MR drift)
        trailing         — once P&L > 1×ATR intraday, move stop to breakeven
    """
    result = {
        "ticker": ticker,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target_1": target_1,
        "holding_period": holding_period,
        "exit_price": None,
        "exit_day": None,
        "exit_reason": None,
        "pnl_pct": None,
        "hit_target": False,
        "hit_stop": False,
    }

    if forward_df.empty:
        result["exit_reason"] = "no_data"
        return result

    close_col = "Close" if "Close" in forward_df.columns else "close"
    high_col = "High" if "High" in forward_df.columns else "high"
    low_col = "Low" if "Low" in forward_df.columns else "low"

    # T+1: skip day 0 (entry day), evaluate from day 1
    bars = forward_df.iloc[1:holding_period + 1] if len(forward_df) > 1 else pd.DataFrame()

    if bars.empty:
        result["exit_reason"] = "insufficient_forward_data"
        return result

    # Trailing stop state: once high exceeds entry + 1×ATR, stop moves to breakeven
    effective_stop = stop_loss
    trailing_activated = False

    for day_idx, (dt, row) in enumerate(bars.iterrows(), start=1):
        high = float(row[high_col])
        low = float(row[low_col])
        close = float(row[close_col])

        # Trailing stop logic (for sniper or when exit_mode=trailing)
        if exit_mode == "trailing" and atr > 0:
            if high >= entry_price + atr and not trailing_activated:
                effective_stop = entry_price  # move to breakeven
                trailing_activated = True

        if high >= target_1:
            result["exit_price"] = target_1
            result["exit_day"] = day_idx
            result["exit_reason"] = "target_hit"
            result["hit_target"] = True
            break
        elif low <= effective_stop:
            result["exit_price"] = effective_stop
            result["exit_day"] = day_idx
            result["exit_reason"] = "stop_hit"
            result["hit_stop"] = True
            break

        # Profitable-close exit: after day 2, exit if close > entry
        if exit_mode == "profitable_close" and day_idx >= 2 and close > entry_price:
            result["exit_price"] = close
            result["exit_day"] = day_idx
            result["exit_reason"] = "profitable_close"
            break
    else:
        last_close = float(bars.iloc[-1][close_col])
        result["exit_price"] = last_close
        result["exit_day"] = len(bars)
        result["exit_reason"] = "hold_expired"

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
                        choices=["target_stop", "profitable_close", "trailing"],
                        help="Exit strategy: target_stop (default), profitable_close, trailing")
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
    parser.add_argument("--outputs-root", default="outputs",
                        help="Outputs root used when universe-source=watchlist")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()
    config = load_config(args.config)

    trading_days = get_cn_trading_days(start_date, end_date)
    if args.max_days > 0:
        trading_days = trading_days[:args.max_days]

    logger.info("=" * 60)
    logger.info("BACKTEST: %s to %s (%d trading days)", args.start, args.end, len(trading_days))
    logger.info("  exit_mode=%s  mr_target=%s  gap_filter=%s  sniper_trailing=%s  acceptance=%s",
                args.exit_mode, args.mr_target,
                "OFF" if args.disable_gap_filter else "ON",
                "OFF" if args.no_sniper_trailing else "ON",
                args.acceptance_mode)
    logger.info("=" * 60)

    # --- One-time setup ---
    _, download_daily_range_fn, provider_config, _ = get_data_functions(config)

    # Build universe once
    if args.universe_source == "watchlist":
        logger.info(
            "Building universe from execution watchlists in %s for %s to %s...",
            args.outputs_root,
            args.start,
            args.end,
        )
        universe = load_execution_watchlist_tickers_in_range(
            args.outputs_root,
            start_date=args.start,
            end_date=args.end,
        )
        if not universe:
            logger.error(
                "No execution watchlist tickers found under %s for %s to %s",
                args.outputs_root,
                args.start,
                args.end,
            )
            return 1
        logger.info("Universe: %d tickers from execution watchlists", len(universe))
    else:
        logger.info("Building universe: top 1000 A-shares by market cap...")
        universe = get_top_n_cn_by_market_cap(n=1000, provider_config=provider_config)
        logger.info("Universe: %d tickers", len(universe))

    # Download date range: lookback before start through end + max holding buffer
    max_hold = max(
        int(config.get("mean_reversion", {}).get("holding_period", 3)),
        int(config.get("sniper", {}).get("holding_period", 7)),
    )
    dl_start = start_date - timedelta(days=LOOKBACK_DAYS)
    dl_end = end_date + timedelta(days=max_hold + 15)  # buffer for weekends/holidays
    dl_start_str = dl_start.strftime("%Y-%m-%d")
    dl_end_str = dl_end.strftime("%Y-%m-%d")

    # Download CSI 300 once
    csi_cfg = config.get("mean_reversion", {}).get("regime", {}) or config.get("sniper", {}).get("regime", {}) or {}
    csi_symbol = csi_cfg.get("csi300_symbol", "000300.SH")
    logger.info("Downloading CSI 300 (%s) for %s to %s...", csi_symbol, dl_start_str, dl_end_str)
    csi300_data, _ = download_daily_range_fn(
        tickers=[csi_symbol], start=dl_start_str, end=dl_end_str,
        provider_config=provider_config,
    )
    csi300_full = csi300_data.get(csi_symbol, pd.DataFrame())
    if not csi300_full.empty:
        csi300_full = csi300_full.rename(
            columns={c: c.lower() for c in csi300_full.columns if c in ("Open", "High", "Low", "Close", "Volume")}
        )
    logger.info("CSI 300: %d bars", len(csi300_full))

    # Bulk download all universe tickers
    logger.info("Bulk downloading OHLCV for %d tickers (%s to %s)...", len(universe), dl_start_str, dl_end_str)
    t0 = time.time()
    data_map, report = download_daily_range_fn(
        tickers=universe, start=dl_start_str, end=dl_end_str,
        provider_config=provider_config,
    )
    dl_time = time.time() - t0
    logger.info("Download complete: %d OK, %d failed (%.1f min)",
                len(data_map), len(report.get("bad_tickers", [])), dl_time / 60)
    if not data_map:
        logger.error(
            "Aborting backtest: no OHLCV data downloaded for %d universe tickers. "
            "This usually indicates a market-data provider or network outage.",
            len(universe),
        )
        return 1

    # Fetch basic info for live_equivalent mode (sector cap, limit-down ST detection)
    info_map: dict[str, dict] = {}
    if args.acceptance_mode == "live_equivalent":
        from src.core.cn_data import get_cn_basic_info
        logger.info("Fetching basic info for %d tickers (live_equivalent mode)...", len(universe))
        info_map = get_cn_basic_info(universe, provider_config=provider_config)
        logger.info("Basic info: %d tickers", len(info_map))

    # --- Config ---
    mr_config = config.get("mean_reversion", {})
    sniper_config = config.get("sniper", {})
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
    t_start = time.time()

    # Engine filter (computed once)
    run_mr = args.engines in ("all", "mr_only") or "mean_reversion" in args.engines
    run_sniper = args.engines in ("all", "sniper_only") or "sniper" in args.engines
    logger.info("Engines: mr=%s sniper=%s", run_mr, run_sniper)

    min_bars = int(mr_config.get("min_bars", 60))

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

        # Score all tickers using precomputed features up to scan_date
        all_signals = []
        for ticker, feat_df in feat_map.items():
            try:
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
                    )
                if sniper_signal:
                    all_signals.append(("sniper", sniper_signal))

            except Exception:
                continue

        # Dedupe: keep higher score per ticker
        best: dict[str, tuple[str, object]] = {}
        for engine, sig in all_signals:
            existing = best.get(sig.ticker)
            if existing is None or sig.score > existing[1].score:
                best[sig.ticker] = (engine, sig)

        # Sort by score desc
        sorted_picks = sorted(best.values(), key=lambda x: (-x[1].score,))

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
            )
            eval_result["date"] = date_str
            eval_result["engine"] = engine
            eval_result["score"] = sig.score
            eval_result["regime"] = regime
            eval_result["subtype"] = getattr(sig, "subtype", None) if engine == "mean_reversion" else None
            all_results.append(eval_result)

            if eval_result["exit_reason"] == "target_hit":
                wins += 1
            elif eval_result["exit_reason"] == "stop_hit":
                losses += 1
            elif eval_result["exit_reason"] == "hold_expired":
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

    if not all_results:
        logger.warning("No results to summarize.")
        return 0

    df = pd.DataFrame(all_results)
    df_valid = df[df["pnl_pct"].notna()].copy()

    total = len(df_valid)
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

    # Acceptance mode distribution (how many days in each mode)
    # breadth_suppressed is tracked separately — market-quality gate, not allocator decision
    acceptance_mode_counts: dict[str, int] = {}
    breadth_suppressed_days = 0
    for d in day_summaries:
        m = d.get("acceptance_mode", "off")
        if m == "breadth_suppressed":
            breadth_suppressed_days += 1
        else:
            acceptance_mode_counts[m] = acceptance_mode_counts.get(m, 0) + 1

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

    suffix = f"_{args.label}" if args.label else ""
    summary = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "config_path": args.config,
        "label": args.label if hasattr(args, "label") else None,
        "start": args.start,
        "end": args.end,
        "period": f"{args.start} to {args.end}",
        "exit_mode": args.exit_mode,
        "mr_target_mode": args.mr_target,
        "engines": args.engines,
        "gap_filter_disabled": args.disable_gap_filter,
        "sniper_trailing_disabled": args.no_sniper_trailing,
        "acceptance_mode": args.acceptance_mode,
        "universe_source": args.universe_source,
        "outputs_root": args.outputs_root if args.universe_source == "watchlist" else None,
        "trading_days_scanned": len(trading_days),
        "days_with_picks": days_with_picks,
        "zero_pick_days": zero_pick_days,
        "zero_pick_days_pct": round(zero_pick_days_pct, 4),
        "avg_picks_per_active_day": round(avg_picks_per_active_day, 2),
        "total_picks": total,
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
        "per_engine": engine_stats,
        "per_regime": regime_stats,
        "per_subtype": subtype_stats,
    }

    # Print summary
    logger.info("=" * 60)
    logger.info("BACKTEST SUMMARY: %s to %s (exit_mode=%s)", args.start, args.end, args.exit_mode)
    logger.info("=" * 60)
    logger.info("Total picks evaluated: %d", total)
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
