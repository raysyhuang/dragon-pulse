"""Unified deterministic scanner pipeline.

Flow: build top-1000-by-cap universe → fetch CSI 300 + classify regime →
download OHLCV → quality/liquidity gates → compute features → score both
engines per ticker → dedupe (keep higher score) → selection funnel → picks.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import pandas as pd

from src.core.config import load_config, get_config_value
from src.core.data import get_data_functions
from src.core.universe import get_top_n_cn_by_market_cap
from src.features.technical import (
    compute_all_technical_features,
    compute_rsi2_features,
    latest_features,
)
from src.pipelines.funnel import (
    build_engine_candidates,
    build_regime_detail,
    compute_breadth,
    run_selection_funnel,
)

logger = logging.getLogger(__name__)


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


from src.pipelines.funnel import classify_regime as _classify_regime  # noqa: backtest_1yr compat


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

    # --- Fetch CSI 300 for regime ---
    logger.info("Fetching CSI 300 index data...")
    csi_cfg = config.get("mean_reversion", {}).get("regime", {}) or {}
    csi_symbol = csi_cfg.get("csi300_symbol", "000300.SH")
    end_dt = pd.to_datetime(scan_date)
    start_dt = end_dt - timedelta(days=400)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    csi300_data, _ = download_daily_range_fn(
        tickers=[csi_symbol], start=start_str, end=end_str,
        provider_config=provider_config,
    )
    csi300_df = csi300_data.get(csi_symbol, pd.DataFrame())
    if not csi300_df.empty:
        csi300_df = csi300_df.rename(columns={c: c.lower() for c in csi300_df.columns if c in ("Open", "High", "Low", "Close", "Volume")})

    # --- Regime ---
    sma_short = int(get_config_value(config, "mean_reversion", "regime", "sma_short", default=20))
    sma_long = int(get_config_value(config, "mean_reversion", "regime", "sma_long", default=50))
    regime, regime_detail = build_regime_detail(csi300_df, sma_short, sma_long)
    regime_emoji = {"bull": "🟢", "bear": "🔴", "choppy": "🟡"}.get(regime, "⚪")
    logger.info("Regime: %s %s", regime.upper(), regime_emoji)

    # --- Fetch basic info for ST detection + name enrichment ---
    from src.core.cn_data import get_cn_basic_info
    info_map = get_cn_basic_info(universe, provider_config=provider_config)

    # --- Filter out ST stocks ---
    exclude_st = config.get("universe", {}).get("exclude_st", True)
    if exclude_st:
        st_tickers = {t for t, info in info_map.items() if info.get("is_st")}
        if st_tickers:
            universe = [t for t in universe if t not in st_tickers]
            logger.info("Excluded %d ST stocks, universe now %d", len(st_tickers), len(universe))

    # --- Download OHLCV ---
    logger.info("Downloading OHLCV for %d tickers...", len(universe))
    data_map, report = download_daily_range_fn(
        tickers=universe, start=start_str, end=end_str,
        provider_config=provider_config,
    )
    n_ok = len(data_map)
    n_fail = len(report.get("bad_tickers", []))
    logger.info("Downloaded: %d OK, %d failed", n_ok, n_fail)

    # Warn loudly if download success rate is critically low
    if n_ok + n_fail > 0:
        success_rate = n_ok / (n_ok + n_fail)
        if success_rate < 0.50:
            logger.error(
                "DATA QUALITY ALERT: Only %.0f%% of tickers downloaded "
                "(%d/%d). Results are unreliable.",
                success_rate * 100, n_ok, n_ok + n_fail,
            )

    # --- Compute features + score candidates ---
    feat_items = []
    errors = []
    for ticker, raw_df in data_map.items():
        try:
            feat_df = compute_all_technical_features(raw_df)
            feat_df = compute_rsi2_features(feat_df)
            feats = latest_features(feat_df)
            if feats:
                feat_items.append((ticker, feat_df, feats))
        except Exception as e:
            errors.append(f"{ticker}: {e}")

    candidates = build_engine_candidates(
        feat_items, regime, config, csi300_df=csi300_df, info_map=info_map,
    )
    sorted_picks = _sort_signal_candidates(candidates, data_map, info_map)

    logger.info("Signals: %d total (%d MR, %d Sniper)",
                len(candidates),
                sum(1 for e, _ in candidates if e == "mean_reversion"),
                sum(1 for e, _ in candidates if e == "sniper"))

    # --- Breadth ---
    breadth_pct = compute_breadth(data_map)
    regime_detail["market_breadth_pct_above_sma20"] = round(breadth_pct, 4)

    # --- Selection funnel ---
    stage = run_selection_funnel(
        sorted_picks, regime, breadth_pct, config,
        universe_size=len(universe),
        data_map=data_map,
        info_map=info_map,
        acceptance_mode="live_equivalent",
    )

    # --- Populate regime_detail from stage result ---
    if stage.acceptance_result:
        regime_detail["day_quality_score"] = stage.day_quality_score
        regime_detail["day_quality_components"] = stage.day_quality_components
        regime_detail["acceptance_mode"] = stage.acceptance_mode
        regime_detail["acceptance_abstained"] = stage.acceptance_result.abstained
        regime_detail["acceptance_eligible_count"] = stage.acceptance_eligible_count
        regime_detail["acceptance_rejected_count"] = len(stage.acceptance_result.rejected)
    elif stage.breadth_suppressed:
        regime_detail["acceptance_mode"] = "breadth_suppressed"
        regime_detail["acceptance_eligible_count"] = stage.acceptance_eligible_count
        regime_detail["day_quality_score"] = 0
        regime_detail["day_quality_components"] = {}

    # --- Build output ---
    picks_out = []
    for engine, sig in stage.final_picks:
        info = info_map.get(sig.ticker, {})
        name_cn = info.get("name_cn", "")
        market_cap = info.get("market_cap")
        raw_df = data_map.get(sig.ticker, pd.DataFrame())
        adv_cny = _compute_adv_cny(raw_df)

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

    # Surface download health in results
    bad_tickers_list = report.get("bad_tickers", [])
    download_reasons = report.get("reasons", {})
    circuit_breaker_msg = download_reasons.get("__circuit_breaker__")

    return {
        "date": scan_date,
        "regime": regime,
        "regime_detail": regime_detail,
        "universe_size": len(universe),
        "downloaded": len(data_map),
        "download_failed": len(bad_tickers_list),
        "download_health": "critical" if len(data_map) < len(universe) * 0.5 else "ok",
        "circuit_breaker": circuit_breaker_msg,
        "signals_total": len(candidates),
        "picks": picks_out,
        "errors": errors[:20],
    }
