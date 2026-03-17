"""Unified deterministic scanner pipeline.

Flow: build top-1000-by-cap universe → fetch CSI 300 + classify regime →
download OHLCV → quality/liquidity gates → compute features → score both
engines per ticker → dedupe (keep higher score) → sort → top 5 picks.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from src.core.acceptance import run_acceptance
from src.core.config import load_config, get_config_value
from src.core.data import get_data_functions
from src.core.regime import check_regime
from src.core.universe import get_top_n_cn_by_market_cap
from src.features.technical import (
    compute_all_technical_features,
    compute_rsi2_features,
    latest_features,
)
from src.signals.mean_reversion import score_mean_reversion
from src.signals.sniper import score_sniper

logger = logging.getLogger(__name__)


def _classify_regime(csi300_df: pd.DataFrame, sma_short: int = 20, sma_long: int = 50) -> str:
    """Classify market regime from CSI 300 data.

    bull:   close > SMA(short) and SMA(short) > SMA(long)
    bear:   close < SMA(long)
    choppy: everything else
    """
    if csi300_df.empty or len(csi300_df) < sma_long + 1:
        return "choppy"

    close_col = "close" if "close" in csi300_df.columns else "Close"
    close = csi300_df[close_col].astype(float)
    last = float(close.iloc[-1])
    sma_s = float(close.tail(sma_short).mean())
    sma_l = float(close.tail(sma_long).mean())

    if last > sma_s and sma_s > sma_l:
        return "bull"
    elif last < sma_l:
        return "bear"
    return "choppy"


def _compute_adv_cny(raw_df: pd.DataFrame) -> float:
    """Compute 20-day average daily turnover in CNY for ranking/output."""
    if raw_df.empty or len(raw_df) < 20:
        return 0.0

    close_col = "Close" if "Close" in raw_df.columns else "close"
    volume_col = "Volume" if "Volume" in raw_df.columns else "volume"
    if close_col not in raw_df.columns or volume_col not in raw_df.columns:
        return 0.0

    close = raw_df[close_col].astype(float).tail(20)
    volume = raw_df[volume_col].astype(float).tail(20)
    return float((close * volume).mean())


def _sort_signal_candidates(
    candidates: list[tuple[str, object]],
    data_map: dict[str, pd.DataFrame],
    info_map: dict[str, dict],
) -> list[tuple[str, object]]:
    """Sort by score desc, then ADV desc, then market cap desc, then ticker."""

    def sort_key(item: tuple[str, object]) -> tuple[float, float, float, str]:
        _, sig = item
        adv_cny = _compute_adv_cny(data_map.get(sig.ticker, pd.DataFrame()))
        market_cap = float((info_map.get(sig.ticker, {}) or {}).get("market_cap") or 0.0)
        return (-float(sig.score), -adv_cny, -market_cap, str(sig.ticker))

    return sorted(candidates, key=sort_key)


def run_scan(config: dict, asof_date: Optional[str] = None) -> dict:
    """Run the full scan pipeline.

    Returns dict with keys: date, regime, regime_detail, universe_size, picks, errors.
    """
    # --- Resolve date ---
    if asof_date:
        scan_date = asof_date
    else:
        scan_date = pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("DRAGON PULSE SCAN — %s", scan_date)
    logger.info("=" * 60)

    # --- Data functions ---
    download_daily_fn, download_daily_range_fn, provider_config, market = get_data_functions(config)

    # --- Build universe: top 1000 by market cap ---
    logger.info("Building universe: top 1000 A-shares by market cap...")
    universe = get_top_n_cn_by_market_cap(n=1000, provider_config=provider_config)
    logger.info("Universe: %d tickers", len(universe))

    if not universe:
        logger.error("Empty universe — cannot proceed")
        return {"date": scan_date, "regime": "unknown", "universe_size": 0, "picks": [], "errors": ["Empty universe"]}

    # --- Fetch CSI 300 for regime + sniper relative strength ---
    logger.info("Fetching CSI 300 index data...")
    csi_cfg = config.get("mean_reversion", {}).get("regime", {}) or config.get("sniper", {}).get("regime", {}) or {}
    csi_symbol = csi_cfg.get("csi300_symbol", "000300.SH")
    end_dt = pd.to_datetime(scan_date)
    start_dt = end_dt - timedelta(days=400)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    csi300_data, _ = download_daily_range_fn(
        tickers=[csi_symbol],
        start=start_str,
        end=end_str,
        provider_config=provider_config,
    )
    csi300_df = csi300_data.get(csi_symbol, pd.DataFrame())

    # Normalize CSI 300 columns to lowercase
    if not csi300_df.empty:
        csi300_df = csi300_df.rename(columns={c: c.lower() for c in csi300_df.columns if c in ("Open", "High", "Low", "Close", "Volume")})

    # --- Regime ---
    sma_short = int(get_config_value(config, "mean_reversion", "regime", "sma_short", default=20))
    sma_long = int(get_config_value(config, "mean_reversion", "regime", "sma_long", default=50))
    regime = _classify_regime(csi300_df, sma_short, sma_long)
    regime_emoji = {"bull": "🟢", "bear": "🔴", "choppy": "🟡"}.get(regime, "⚪")
    logger.info("Regime: %s %s", regime.upper(), regime_emoji)

    regime_detail = {}
    if not csi300_df.empty:
        close_col = "close" if "close" in csi300_df.columns else "Close"
        regime_detail = {
            "csi300_last": round(float(csi300_df[close_col].iloc[-1]), 2),
            "csi300_sma_short": round(float(csi300_df[close_col].tail(sma_short).mean()), 2),
            "csi300_sma_long": round(float(csi300_df[close_col].tail(sma_long).mean()), 2),
        }

    # --- Fetch basic info for ST detection + name enrichment ---
    from src.core.cn_data import get_cn_basic_info
    info_map = get_cn_basic_info(universe, provider_config=provider_config)

    # --- Filter out ST / *ST stocks if configured ---
    exclude_st = config.get("universe", {}).get("exclude_st", True)
    if exclude_st:
        st_tickers = {t for t, info in info_map.items() if info.get("is_st")}
        if st_tickers:
            universe = [t for t in universe if t not in st_tickers]
            logger.info("Excluded %d ST stocks, universe now %d", len(st_tickers), len(universe))

    # --- Download OHLCV for universe ---
    logger.info("Downloading OHLCV for %d tickers...", len(universe))
    data_map, report = download_daily_range_fn(
        tickers=universe,
        start=start_str,
        end=end_str,
        provider_config=provider_config,
    )
    logger.info("Downloaded: %d OK, %d failed", len(data_map), len(report.get("bad_tickers", [])))

    # --- Score each ticker with both engines ---
    all_signals = []
    errors = []

    mr_config = config.get("mean_reversion", {})
    sniper_config = config.get("sniper", {})

    for ticker, raw_df in data_map.items():
        try:
            # Compute features
            feat_df = compute_all_technical_features(raw_df)
            feat_df = compute_rsi2_features(feat_df)
            feats = latest_features(feat_df)

            if not feats:
                continue

            ticker_is_st = info_map.get(ticker, {}).get("is_st", False)

            # Mean reversion
            mr_signal = score_mean_reversion(
                ticker=ticker,
                df=feat_df,
                features=feats,
                regime=regime,
                is_st=ticker_is_st,
                rsi2_max=float(mr_config.get("rsi2_max", 5)),
                adv_min_cny=float(mr_config.get("adv_min_cny", 100_000_000)),
                score_floor=float(mr_config.get("score_floor", 65)),
                min_bars=int(mr_config.get("min_bars", 60)),
                max_single_day_move=float(mr_config.get("max_single_day_move", 0.11)),
                stop_atr_mult=float(mr_config.get("stop_atr_mult", 0.75)),
                target_1_atr_mult=float(mr_config.get("target_1_atr_mult", 1.5)),
                target_2_atr_mult=float(mr_config.get("target_2_atr_mult", 2.0)),
                max_entry_atr_mult=float(mr_config.get("max_entry_atr_mult", 0.2)),
                holding_period=int(mr_config.get("holding_period", 3)),
            )
            if mr_signal:
                all_signals.append(("mean_reversion", mr_signal))

            # Sniper (quarantined by default — set sniper.enabled: true to re-enable)
            if not sniper_config.get("enabled", False):
                sniper_signal = None
            else:
                sniper_signal = score_sniper(
                    ticker=ticker,
                    df=feat_df,
                    features=feats,
                    regime=regime,
                    csi300_df=csi300_df,
                    atr_pct_floor=float(sniper_config.get("atr_pct_floor", 3.5)),
                    min_avg_volume=int(sniper_config.get("min_avg_volume", 500_000)),
                    stop_atr_mult=float(sniper_config.get("stop_atr_mult", 2.0)),
                    target_atr_mult=float(sniper_config.get("target_atr_mult", 3.0)),
                    target_2_atr_mult=float(sniper_config.get("target_2_atr_mult", 5.0)),
                    holding_period=int(sniper_config.get("holding_period", 7)),
                    is_st=ticker_is_st,
                )
            if sniper_signal:
                all_signals.append(("sniper", sniper_signal))

        except Exception as e:
            errors.append(f"{ticker}: {e}")
            continue

    logger.info("Signals: %d total (%d MR, %d Sniper)",
                len(all_signals),
                sum(1 for e, _ in all_signals if e == "mean_reversion"),
                sum(1 for e, _ in all_signals if e == "sniper"))

    # --- Dedupe: if a ticker appears in both engines, keep higher score ---
    best: dict[str, tuple[str, object]] = {}
    for engine, sig in all_signals:
        existing = best.get(sig.ticker)
        if existing is None or sig.score > existing[1].score:
            best[sig.ticker] = (engine, sig)

    # --- Sort by score desc, then ADV desc, then market cap desc ---
    sorted_picks = _sort_signal_candidates(list(best.values()), data_map, info_map)

    # --- Breadth-based abstention: suppress all picks if market breadth too weak ---
    above_sma20 = sum(
        1 for df in data_map.values()
        if len(df) >= 20 and float(df["close"].iloc[-1] if "close" in df.columns else df["Close"].iloc[-1])
        > float((df["close"] if "close" in df.columns else df["Close"]).tail(20).mean())
    ) / max(len(data_map), 1)
    breadth_floor = float(config.get("book_size", {}).get("breadth_floor", 0.30))
    regime_detail["market_breadth_pct_above_sma20"] = round(above_sma20, 4)

    if above_sma20 < breadth_floor:
        logger.info("Market breadth %.1f%% < %.0f%% floor — suppressing all picks",
                     above_sma20 * 100, breadth_floor * 100)
        sorted_picks = []

    # --- Quality-gated book size ---
    book_cfg = config.get("book_size", {}).get(regime, {})
    max_picks = int(book_cfg.get("max_picks", 5)) if isinstance(book_cfg, dict) else 5
    min_score = float(book_cfg.get("min_score", 0)) if isinstance(book_cfg, dict) else 0
    
    # Filter by minimum score
    quality_picks = [p for p in sorted_picks if p[1].score >= min_score]
    
    # --- Limit Down Veto: reject if yesterday closed at limit-down ---
    from src.core.cn_limits import get_daily_limit
    non_limit_picks = []
    for engine, sig in quality_picks:
        raw_df = data_map.get(sig.ticker, pd.DataFrame())
        if not raw_df.empty:
            close = float(raw_df["close" if "close" in raw_df.columns else "Close"].iloc[-1])
            prev_close = float(raw_df["close" if "close" in raw_df.columns else "Close"].iloc[-2]) if len(raw_df) > 1 else close
            limit_pct = get_daily_limit(sig.ticker, is_st=info_map.get(sig.ticker, {}).get("is_st", False))
            
            # If closed within 0.1% of limit-down, reject
            if (close / prev_close - 1) <= (-limit_pct + 0.001):
                logger.info("Rejecting %s: Closed at limit-down (%.2f%%)", sig.ticker, (close/prev_close-1)*100)
                continue
        non_limit_picks.append((engine, sig))
    
    # --- Sector concentration limit: max 1 per industry ---
    max_per_sector = int(config.get("book_size", {}).get("max_per_sector", 1))
    sector_counts: dict[str, int] = {}
    sector_filtered: list[tuple[str, object]] = []
    for engine, sig in non_limit_picks:
        industry = (info_map.get(sig.ticker, {}) or {}).get("industry", "unknown") or "unknown"
        if sector_counts.get(industry, 0) < max_per_sector:
            sector_filtered.append((engine, sig))
            sector_counts[industry] = sector_counts.get(industry, 0) + 1
            
    # --- Acceptance layer: app-level allocator (decides book size) ---
    # IMPORTANT: acceptance sees the FULL post-veto candidate set.
    # It is the allocator — do NOT truncate to max_picks before this point.
    acceptance_cfg = config.get("acceptance", {})
    acceptance_enabled = acceptance_cfg.get("enabled", True)

    if acceptance_enabled:
        acceptance_result = run_acceptance(
            candidates=sector_filtered,
            breadth_pct=above_sma20,
            regime=regime,
            universe_size=len(universe),
            config=acceptance_cfg,
            info_map=info_map,
        )
        final_picks = [(e, s) for e, s, _tier in acceptance_result.accepted]
        regime_detail["day_quality_score"] = acceptance_result.day_quality.score
        regime_detail["day_quality_components"] = acceptance_result.day_quality.components
        regime_detail["acceptance_mode"] = acceptance_result.mode
        regime_detail["acceptance_abstained"] = acceptance_result.abstained
        regime_detail["acceptance_eligible_count"] = acceptance_result.eligible_count
        regime_detail["acceptance_rejected_count"] = len(acceptance_result.rejected)
    else:
        # Legacy path: simple truncation
        final_picks = sector_filtered[:max_picks]

    # --- Build output ---
    picks_out = []
    for engine, sig in final_picks:
        info = info_map.get(sig.ticker, {})
        name_cn = info.get("name_cn", "")
        market_cap = info.get("market_cap")

        # Compute ADV for output
        raw_df = data_map.get(sig.ticker, pd.DataFrame())
        adv_cny = _compute_adv_cny(raw_df)

        # Build reason summary
        top_components = sorted(sig.components.items(), key=lambda x: -x[1])[:3]
        reason = ", ".join(f"{k}={v:.0f}" for k, v in top_components)

        picks_out.append({
            "ticker": sig.ticker,
            "name_cn": name_cn,
            "engine": engine,
            "score": sig.score,
            "entry_price": sig.entry_price,
            "max_entry_price": getattr(sig, "max_entry_price", None),
            "stop_loss": sig.stop_loss,
            "target_1": sig.target_1,
            "target_2": sig.target_2,
            "holding_period": sig.holding_period,
            "adv_cny": round(adv_cny, 0),
            "market_cap_cny": market_cap,
            "reason_summary": reason,
            "components": sig.components,
        })

    logger.info("Final picks: %d", len(picks_out))
    for p in picks_out:
        logger.info("  %s %s [%s] score=%.1f entry=%.2f stop=%.2f t1=%.2f",
                     p["name_cn"], p["ticker"], p["engine"],
                     p["score"], p["entry_price"], p["stop_loss"], p["target_1"])

    return {
        "date": scan_date,
        "regime": regime,
        "regime_detail": regime_detail,
        "universe_size": len(universe),
        "downloaded": len(data_map),
        "signals_total": len(all_signals),
        "picks": picks_out,
        "errors": errors[:20],
    }
