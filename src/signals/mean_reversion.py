"""RSI(2) oversold mean-reversion model — CN A-share adaptation.

Fires on day T close, execution at T+1 open (CN T+1 rule).
No fundamental factor. Active in ALL regimes (bull, choppy, bear).

Weights: RSI(2) 40%, trend 25%, streak 15%, 5d-low 10%, volume 10%.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.core.cn_limits import get_daily_limit


def _valid(x) -> bool:
    if x is None:
        return False
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


@dataclass
class MeanReversionSignal:
    ticker: str
    score: float
    direction: str
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    holding_period: int
    components: dict
    max_entry_price: float | None = None
    subtype: str = "default"


def classify_mean_reversion_subtype(
    features: dict,
    *,
    rsi2_bounce_max: float = 3.0,
    streak_bounce_max: int = -3,
    dist_from_5d_low_bounce_max: float = 0.75,
) -> str:
    """Classify MR setups into fast bounce vs slower drift profiles.

    v1 heuristic intentionally stays simple and point-in-time safe:
    extreme dislocations are treated as fast snapback candidates; the rest
    of the valid MR universe is treated as slower drift.
    """
    rsi_2 = features.get("rsi_2")
    streak = features.get("streak")
    dist = features.get("dist_from_5d_low")

    if _valid(rsi_2) and float(rsi_2) <= float(rsi2_bounce_max):
        return "bounce"
    if _valid(streak) and int(float(streak)) <= int(streak_bounce_max):
        return "bounce"
    if _valid(dist) and float(dist) <= float(dist_from_5d_low_bounce_max):
        return "bounce"
    return "drift"


def resolve_mr_subtype_and_exit_params(mr_config: dict, features: dict) -> tuple[str, dict]:
    """Resolve MR subtype and any subtype-specific exit overrides."""
    params = {
        "stop_atr_mult": float(mr_config.get("stop_atr_mult", 0.95)),
        "target_1_atr_mult": float(mr_config.get("target_1_atr_mult", 1.5)),
        "target_2_atr_mult": float(mr_config.get("target_2_atr_mult", 2.0)),
        "max_entry_atr_mult": float(mr_config.get("max_entry_atr_mult", 0.2)),
        "holding_period": int(mr_config.get("holding_period", 3)),
    }

    subtype_cfg = mr_config.get("subtype_split", {}) or {}
    if not subtype_cfg.get("enabled", False):
        return "default", params

    subtype = classify_mean_reversion_subtype(
        features,
        rsi2_bounce_max=float(subtype_cfg.get("rsi2_bounce_max", 3.0)),
        streak_bounce_max=int(subtype_cfg.get("streak_bounce_max", -3)),
        dist_from_5d_low_bounce_max=float(subtype_cfg.get("dist_from_5d_low_bounce_max", 0.75)),
    )
    if subtype != "drift":
        return subtype, params

    drift_cfg = subtype_cfg.get("drift", {}) or {}
    drift_params = {
        "stop_atr_mult": float(drift_cfg.get("stop_atr_mult", params["stop_atr_mult"])),
        "target_1_atr_mult": float(drift_cfg.get("target_1_atr_mult", params["target_1_atr_mult"])),
        "target_2_atr_mult": float(drift_cfg.get("target_2_atr_mult", params["target_2_atr_mult"])),
        "max_entry_atr_mult": float(drift_cfg.get("max_entry_atr_mult", params["max_entry_atr_mult"])),
        "holding_period": int(drift_cfg.get("holding_period", params["holding_period"])),
    }
    return subtype, drift_params


def score_mean_reversion(
    ticker: str,
    df: pd.DataFrame,
    features: dict,
    regime: str = "unknown",
    is_st: bool = False,
    *,
    disable_gap_filter: bool = False,
    target_mode: str = "sma5",
    rsi2_max: float = 5,
    adv_min_cny: float = 100_000_000,
    score_floor: float = 65,
    min_bars: int = 60,
    max_single_day_move: float = 0.11,
    stop_atr_mult: float = 0.75,
    target_1_atr_mult: float = 1.5,
    target_2_atr_mult: float = 2.0,
    max_entry_atr_mult: float = 0.2,
    holding_period: int = 3,
    subtype: str = "default",
) -> MeanReversionSignal | None:
    """Score a ticker for RSI(2) mean-reversion potential.

    All gate thresholds are configurable for backtest attribution.
    Live-scan defaults match current tightened values; pass config
    values explicitly to reproduce v4.0 baseline behavior.

    Returns None if gates fail or score too low.
    """
    if df.empty or len(df) < min_bars:
        return None

    # --- Data quality gate: reject split/bad-data artifacts ---
    close = df["close"].astype(float)
    daily_returns = close.pct_change().abs()
    if (daily_returns > max_single_day_move).any():
        return None

    # --- Liquidity gate ---
    if len(df) >= 20 and "volume" in df.columns:
        adv_20 = float((close.tail(20) * df["volume"].astype(float).tail(20)).mean())
        if adv_20 < adv_min_cny:
            return None
    else:
        return None

    # --- Hard gate: must be above SMA200 (reject downtrends) ---
    pct_above_sma200 = features.get("pct_above_sma200")
    if _valid(pct_above_sma200) and float(pct_above_sma200) <= 0:
        return None
    # If SMA200 not available, fall back to SMA50
    if not _valid(pct_above_sma200):
        pct_above_sma50 = features.get("pct_above_sma50")
        if _valid(pct_above_sma50) and float(pct_above_sma50) <= 0:
            return None

    scores = {}

    # --- 1. RSI(2) Oversold (40%) ---
    rsi_2 = features.get("rsi_2")
    if not _valid(rsi_2):
        return None

    if rsi_2 <= rsi2_max:
        # Scale: ≤5 gets full score, 5-10 gets partial (if rsi2_max > 5)
        rsi_score = 100.0 if rsi_2 <= 5 else max(60.0, 100.0 - (rsi_2 - 5) * 8)
    else:
        return None
    scores["rsi2_oversold"] = rsi_score

    # --- 2. Trend Intact (25%) — already gated above, score reflects quality ---
    pct_above_sma200 = features.get("pct_above_sma200")
    pct_above_sma50 = features.get("pct_above_sma50")
    sma50 = features.get("sma_50")
    sma200 = features.get("sma_200")

    # Above SMA200 is guaranteed by hard gate; score by how strong the trend is
    sma50_above_200 = (_valid(sma50) and _valid(sma200) and float(sma50) > float(sma200))
    if sma50_above_200:
        trend_score = 100.0  # strong uptrend: SMA50 > SMA200
    else:
        trend_score = 60.0   # above SMA200 but SMA50 hasn't crossed yet
    scores["trend_intact"] = trend_score

    # --- 3. Down Streak (15%) ---
    streak = features.get("streak", 0)
    if _valid(streak) and streak <= -3:
        streak_score = 100.0
    elif _valid(streak) and streak <= -2:
        streak_score = 60.0
    elif _valid(streak) and streak <= -1:
        streak_score = 30.0
    else:
        streak_score = 0.0
    scores["down_streak"] = streak_score

    # --- 4. Distance from 5-day Low (10%) ---
    dist = features.get("dist_from_5d_low", 0)
    if _valid(dist) and dist < 1.0:
        prox_score = 80.0
    elif _valid(dist) and dist < 2.0:
        prox_score = 50.0
    else:
        prox_score = 0.0
    scores["proximity_to_low"] = prox_score

    # --- 5. Volume Signature (10%) ---
    rvol = features.get("rvol")
    vol_score = 50.0
    if len(df) >= 4 and "volume" in df.columns:
        recent_vol = df["volume"].astype(float).iloc[-3:].values
        if len(recent_vol) == 3 and all(v > 0 for v in recent_vol):
            x = np.arange(3, dtype=float)
            slope = float(np.polyfit(x, recent_vol, 1)[0])
            if slope > 0 and _valid(rvol) and rvol > 1.5:
                vol_score = 10.0  # distribution — penalize
            elif slope < 0:
                vol_score = 80.0  # selling exhaustion
            elif _valid(rvol) and rvol >= 0.5:
                vol_score = 70.0
            elif _valid(rvol) and rvol < 0.3:
                vol_score = 20.0
    scores["volume"] = vol_score

    # --- Composite (no fundamental factor) ---
    weights = {
        "rsi2_oversold": 0.40,
        "trend_intact": 0.25,
        "down_streak": 0.15,
        "proximity_to_low": 0.10,
        "volume": 0.10,
    }
    composite = sum(scores[k] * weights[k] for k in weights)

    if composite < score_floor:
        return None

    # --- Price targets ---
    close_price = features.get("close", 0)
    atr = features.get("atr_14")
    if not _valid(close_price) or close_price <= 0:
        return None
    if not _valid(atr) or atr <= 0:
        atr = close_price * 0.02
    atr = max(atr, close_price * 0.005)

    # Stop distance is configurable for exit/payoff ablations.
    stop_loss = close_price - stop_atr_mult * atr

    # Gap risk filter: reject if recent gap volatility exceeds stop cushion
    if not disable_gap_filter and "gap_pct" in df.columns and len(df) >= 20:
        gap_std = float(df["gap_pct"].tail(20).std())
        stop_dist_pct = (close_price - stop_loss) / close_price * 100
        if _valid(gap_std) and gap_std > stop_dist_pct * 0.8:
            return None  # gap risk exceeds stop cushion

    # Targets
    sma_5 = close.rolling(5).mean().iloc[-1]
    sma_5_val = float(sma_5) if pd.notna(sma_5) else close_price * 1.03
    sma_10 = close.rolling(10).mean().iloc[-1]
    sma_10_val = float(sma_10) if pd.notna(sma_10) else close_price * 1.05

    if target_mode == "atr":
        # Legacy: ATR-floor targets (pre-v4.1)
        target_1 = max(sma_5_val, close_price + target_1_atr_mult * atr)
        target_2 = max(sma_10_val, close_price + target_2_atr_mult * atr)
    else:
        # v4.1: SMA5 reversion. If SMA5 is too close to matter, use a configurable
        # ATR fallback so payoff experiments can alter the near target.
        target_1 = (
            sma_5_val
            if sma_5_val > close_price * 1.005
            else close_price + target_1_atr_mult * atr
        )
        target_2 = max(sma_10_val, close_price + target_2_atr_mult * atr)

    # Cap targets at board-aware daily limit
    daily_limit = get_daily_limit(ticker, is_st=is_st)
    cap = close_price * (1 + daily_limit)
    target_1 = min(target_1, cap)
    target_2 = min(target_2, cap)

    max_entry = round(close_price + max_entry_atr_mult * atr, 2)
    max_entry = min(max_entry, cap)

    return MeanReversionSignal(
        ticker=ticker,
        score=round(composite, 2),
        direction="LONG",
        entry_price=round(close_price, 2),
        stop_loss=round(stop_loss, 2),
        target_1=round(target_1, 2),
        target_2=round(target_2, 2),
        holding_period=holding_period,
        components=scores,
        max_entry_price=max_entry,
        subtype=subtype,
    )
