"""Shared selection funnel — canonical candidate generation and filtering logic.

Used by scanner.py (live), backtest_1yr.py, and validate_scanner_policy.py.
Extracted from scanner.py to eliminate duplication while preserving exact behavior.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from src.core.acceptance import run_acceptance, AcceptanceResult
from src.core.cn_limits import get_daily_limit
from src.signals.mean_reversion import score_mean_reversion
from src.signals.sniper import score_sniper

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@dataclass
class StageResult:
    """Output of the selection funnel — carries per-stage metadata."""
    final_picks: list[tuple[str, object]]  # [(engine, signal), ...]
    breadth_pct: float = 0.0
    breadth_suppressed: bool = False
    regime: str = "unknown"
    # Counts at each stage
    raw_signal_count: int = 0
    deduped_count: int = 0
    score_floor_count: int = 0
    limit_veto_count: int = 0
    sector_cap_count: int = 0
    acceptance_eligible_count: int = 0
    # Acceptance detail (None if acceptance disabled)
    acceptance_result: Optional[AcceptanceResult] = None
    acceptance_mode: str = ""
    day_quality_score: float = 0.0
    day_quality_components: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Regime
# ---------------------------------------------------------------------------

def classify_regime(
    csi300_df: pd.DataFrame,
    sma_short: int = 20,
    sma_long: int = 50,
) -> str:
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


def build_regime_detail(
    csi300_df: pd.DataFrame,
    sma_short: int = 20,
    sma_long: int = 50,
) -> tuple[str, dict]:
    """Classify regime and return (regime, detail dict)."""
    regime = classify_regime(csi300_df, sma_short, sma_long)
    detail: dict = {}
    if not csi300_df.empty:
        close_col = "close" if "close" in csi300_df.columns else "Close"
        detail = {
            "csi300_last": round(float(csi300_df[close_col].iloc[-1]), 2),
            "csi300_sma_short": round(float(csi300_df[close_col].tail(sma_short).mean()), 2),
            "csi300_sma_long": round(float(csi300_df[close_col].tail(sma_long).mean()), 2),
        }
    return regime, detail


# ---------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------

def build_engine_candidates(
    feat_items: list[tuple[str, pd.DataFrame, dict]],
    regime: str,
    config: dict,
    csi300_df: pd.DataFrame | None = None,
    info_map: dict[str, dict] | None = None,
) -> list[tuple[str, object]]:
    """Score tickers with MR (and optionally Sniper) engines, dedupe, return sorted candidates.

    Args:
        feat_items: list of (ticker, feat_df, features_dict) tuples — precomputed.
        regime: current regime string.
        config: full config dict.
        csi300_df: CSI 300 DataFrame for Sniper relative strength.
        info_map: ticker -> basic info dict (for ST detection).

    Returns:
        Deduped and sorted list of (engine, signal) tuples.
    """
    info_map = info_map or {}
    mr_config = config.get("mean_reversion", {})
    sniper_config = config.get("sniper", {})
    run_sniper = sniper_config.get("enabled", False)

    all_signals: list[tuple[str, object]] = []

    for ticker, feat_df, feats in feat_items:
        if not feats:
            continue
        try:
            is_st = info_map.get(ticker, {}).get("is_st", False)

            # Mean reversion
            mr_signal = score_mean_reversion(
                ticker=ticker,
                df=feat_df,
                features=feats,
                regime=regime,
                is_st=is_st,
                rsi2_max=float(mr_config.get("rsi2_max", 5)),
                adv_min_cny=float(mr_config.get("adv_min_cny", 100_000_000)),
                score_floor=float(mr_config.get("score_floor", 65)),
                min_bars=int(mr_config.get("min_bars", 60)),
                max_single_day_move=float(mr_config.get("max_single_day_move", 0.11)),
                stop_atr_mult=float(mr_config.get("stop_atr_mult", 0.95)),
                target_1_atr_mult=float(mr_config.get("target_1_atr_mult", 1.5)),
                target_2_atr_mult=float(mr_config.get("target_2_atr_mult", 2.0)),
                max_entry_atr_mult=float(mr_config.get("max_entry_atr_mult", 0.2)),
                holding_period=int(mr_config.get("holding_period", 3)),
            )
            if mr_signal:
                all_signals.append(("mean_reversion", mr_signal))

            # Sniper (quarantined by default)
            if run_sniper and csi300_df is not None:
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
                    is_st=is_st,
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

    # Sort by score desc (caller can apply _sort_signal_candidates for ADV/mcap tiebreak)
    sorted_candidates = sorted(best.values(), key=lambda x: -x[1].score)

    return sorted_candidates


# ---------------------------------------------------------------------------
# Breadth
# ---------------------------------------------------------------------------

def compute_breadth(
    data_map: dict[str, pd.DataFrame],
    scan_date: object = None,
    precomputed_breadth: dict[str, dict] | None = None,
) -> float:
    """Compute market breadth: fraction of tickers with close > SMA20.

    If precomputed_breadth map is provided (ticker -> {date: bool}), use it.
    Otherwise compute from raw data_map.
    """
    if precomputed_breadth is not None and scan_date is not None:
        above = 0
        denom = 0
        for ticker, date_map in precomputed_breadth.items():
            val = date_map.get(scan_date)
            if val is None:
                valid = [d for d in date_map if d <= scan_date]
                val = date_map[max(valid)] if valid else None
            if val is None:
                continue
            denom += 1
            if val:
                above += 1
        return above / max(denom, 1)

    above = 0
    denom = 0
    for df in data_map.values():
        if len(df) < 20:
            continue
        denom += 1
        close_col = "close" if "close" in df.columns else "Close"
        if float(df[close_col].iloc[-1]) > float(df[close_col].tail(20).mean()):
            above += 1
    return above / max(denom, 1)


# ---------------------------------------------------------------------------
# Selection funnel
# ---------------------------------------------------------------------------

def run_selection_funnel(
    sorted_candidates: list[tuple[str, object]],
    regime: str,
    breadth_pct: float,
    config: dict,
    universe_size: int,
    *,
    data_map: dict[str, pd.DataFrame] | None = None,
    info_map: dict[str, dict] | None = None,
    feat_map: dict[str, pd.DataFrame] | None = None,
    date_pos_map: dict[str, dict] | None = None,
    scan_date: object = None,
    acceptance_mode: str = "live_equivalent",
) -> StageResult:
    """Run the full post-scoring selection funnel.

    Stages (live_equivalent):
        1. Breadth suppression
        2. Regime-specific score floor
        3. Limit-down veto
        4. Sector cap
        5. Acceptance allocator

    Args:
        sorted_candidates: deduped, score-sorted (engine, signal) tuples.
        regime: current market regime.
        breadth_pct: market breadth fraction (close > SMA20).
        config: full config dict.
        universe_size: number of tickers in universe.
        data_map: ticker -> raw DataFrame (for limit-down check with raw data).
        info_map: ticker -> basic info (for ST, industry).
        feat_map: ticker -> precomputed feature DataFrame (backtest mode).
        date_pos_map: ticker -> {date: iloc position} (backtest mode).
        scan_date: current date (backtest mode).
        acceptance_mode: "off", "engine_only", or "live_equivalent".

    Returns:
        StageResult with final picks and per-stage metadata.
    """
    info_map = info_map or {}
    result = StageResult(
        final_picks=[],
        breadth_pct=breadth_pct,
        regime=regime,
        raw_signal_count=len(sorted_candidates),
        deduped_count=len(sorted_candidates),
    )

    if acceptance_mode == "off":
        max_picks = _get_max_picks(config, regime)
        result.final_picks = sorted_candidates[:max_picks]
        result.acceptance_mode = "off"
        return result

    # --- Stage 1: Breadth suppression ---
    breadth_floor = float(config.get("book_size", {}).get("breadth_floor", 0.30))
    if breadth_pct < breadth_floor:
        result.breadth_suppressed = True
        result.acceptance_mode = "breadth_suppressed"
        result.acceptance_eligible_count = len(sorted_candidates)
        return result

    if acceptance_mode == "engine_only":
        # Acceptance on raw deduped set — no scanner policy filters
        acceptance_cfg = config.get("acceptance", {})
        acc_result = run_acceptance(
            candidates=sorted_candidates,
            breadth_pct=breadth_pct,
            regime=regime,
            universe_size=universe_size,
            config=acceptance_cfg,
        )
        result.final_picks = [(e, s) for e, s, _tier in acc_result.accepted]
        result.acceptance_result = acc_result
        result.acceptance_mode = acc_result.mode
        result.acceptance_eligible_count = acc_result.eligible_count
        result.day_quality_score = acc_result.day_quality.score
        result.day_quality_components = acc_result.day_quality.components
        return result

    # --- live_equivalent: full scanner funnel ---

    # Stage 2: Regime-specific score floor
    book_cfg = config.get("book_size", {}).get(regime, {})
    min_score = float(book_cfg.get("min_score", 0)) if isinstance(book_cfg, dict) else 0
    quality_picks = [p for p in sorted_candidates if p[1].score >= min_score]
    result.score_floor_count = len(quality_picks)

    # Stage 3: Limit-down veto
    non_limit_picks = _limit_down_veto(
        quality_picks, data_map=data_map, info_map=info_map,
        feat_map=feat_map, date_pos_map=date_pos_map, scan_date=scan_date,
    )
    result.limit_veto_count = len(non_limit_picks)

    # Stage 4: Sector cap
    max_per_sector = int(config.get("book_size", {}).get("max_per_sector", 1))
    sector_filtered = _sector_cap(non_limit_picks, info_map, max_per_sector)
    result.sector_cap_count = len(sector_filtered)

    # Stage 5: Acceptance allocator
    acceptance_cfg = config.get("acceptance", {})
    acceptance_enabled = acceptance_cfg.get("enabled", True)

    if acceptance_enabled:
        acc_result = run_acceptance(
            candidates=sector_filtered,
            breadth_pct=breadth_pct,
            regime=regime,
            universe_size=universe_size,
            config=acceptance_cfg,
            info_map=info_map,
        )
        result.final_picks = [(e, s) for e, s, _tier in acc_result.accepted]
        result.acceptance_result = acc_result
        result.acceptance_mode = acc_result.mode
        result.acceptance_eligible_count = acc_result.eligible_count
        result.day_quality_score = acc_result.day_quality.score
        result.day_quality_components = acc_result.day_quality.components
    else:
        max_picks = _get_max_picks(config, regime)
        result.final_picks = sector_filtered[:max_picks]
        result.acceptance_mode = "off"

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_max_picks(config: dict, regime: str) -> int:
    book_cfg = config.get("book_size", {}).get(regime, {})
    return int(book_cfg.get("max_picks", 5)) if isinstance(book_cfg, dict) else 5


def _limit_down_veto(
    candidates: list[tuple[str, object]],
    *,
    data_map: dict[str, pd.DataFrame] | None = None,
    info_map: dict[str, dict] | None = None,
    feat_map: dict[str, pd.DataFrame] | None = None,
    date_pos_map: dict[str, dict] | None = None,
    scan_date: object = None,
) -> list[tuple[str, object]]:
    """Reject tickers that closed at limit-down."""
    info_map = info_map or {}
    result = []

    for engine, sig in candidates:
        close_val = None
        prev_close = None

        # Try precomputed feat_map first (backtest), fall back to data_map (live)
        if feat_map is not None and date_pos_map is not None and scan_date is not None:
            t_feat = feat_map.get(sig.ticker)
            t_pos = (date_pos_map.get(sig.ticker) or {}).get(scan_date)
            if t_feat is not None and t_pos is not None and t_pos >= 1:
                close_col = "close" if "close" in t_feat.columns else "Close"
                close_val = float(t_feat[close_col].iloc[t_pos])
                prev_close = float(t_feat[close_col].iloc[t_pos - 1])
        elif data_map is not None:
            raw_df = data_map.get(sig.ticker, pd.DataFrame())
            if not raw_df.empty and len(raw_df) > 1:
                close_col = "close" if "close" in raw_df.columns else "Close"
                close_val = float(raw_df[close_col].iloc[-1])
                prev_close = float(raw_df[close_col].iloc[-2])

        if close_val is not None and prev_close is not None and prev_close > 0:
            limit_pct = get_daily_limit(
                sig.ticker,
                is_st=info_map.get(sig.ticker, {}).get("is_st", False),
            )
            if (close_val / prev_close - 1) <= (-limit_pct + 0.001):
                continue

        result.append((engine, sig))
    return result


def _sector_cap(
    candidates: list[tuple[str, object]],
    info_map: dict[str, dict],
    max_per_sector: int = 1,
) -> list[tuple[str, object]]:
    """Enforce max picks per industry sector."""
    sector_counts: dict[str, int] = {}
    result: list[tuple[str, object]] = []
    for engine, sig in candidates:
        industry = (info_map.get(sig.ticker, {}) or {}).get("industry", "unknown") or "unknown"
        if sector_counts.get(industry, 0) < max_per_sector:
            result.append((engine, sig))
            sector_counts[industry] = sector_counts.get(industry, 0) + 1
    return result
