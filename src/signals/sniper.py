"""Sniper track — concentrated high-velocity signal model (CN A-share adaptation).

Targets stocks that CAN move 7-10% in setups with BB squeeze + volume compression.
Uses CSI 300 relative strength instead of SPY.

Bear regime → hard block. Score floors: 60 bull, 65 choppy.
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
class SniperSignal:
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


def score_sniper(
    ticker: str,
    df: pd.DataFrame,
    features: dict,
    regime: str = "unknown",
    csi300_df: pd.DataFrame | None = None,
    atr_pct_floor: float = 3.5,
    min_avg_volume: int = 500_000,
    stop_atr_mult: float = 2.0,
    target_atr_mult: float = 3.0,
    target_2_atr_mult: float = 5.0,
    holding_period: int = 7,
    max_entry_pct: float = 0.02,
    is_st: bool = False,
) -> SniperSignal | None:
    """Score a ticker for sniper (BB squeeze + vol compression) potential.

    Hard gates:
      - Bear regime → block
      - ATR% ≥ 3.5
      - 20-day avg volume ≥ 500K shares
    """
    if df.empty or len(df) < 60:
        return None

    # --- Hard gate: bear regime block ---
    if regime == "bear":
        return None

    scores: dict[str, float] = {}
    close = df["close"].astype(float)

    # --- Hard gate: ATR% floor ---
    atr_pct = features.get("atr_pct")
    if not _valid(atr_pct) or float(atr_pct) < atr_pct_floor:
        return None

    # --- Hard gate: average volume (shares) ---
    vol_sma_20 = features.get("vol_sma_20")
    if _valid(vol_sma_20) and float(vol_sma_20) < min_avg_volume:
        return None
    if not _valid(vol_sma_20) and len(df) >= 20:
        avg_vol = float(df["volume"].astype(float).tail(20).mean())
        if avg_vol < min_avg_volume:
            return None

    # --- 1. BB Squeeze (30%) ---
    bb_width_col = "BBB_20_2.0"
    bb_score = 50.0
    if bb_width_col in df.columns and len(df) >= 60:
        bb_width = df[bb_width_col].astype(float)
        current_bb = bb_width.iloc[-1]
        if _valid(current_bb):
            recent_60 = bb_width.tail(60).dropna()
            if len(recent_60) >= 20:
                pctile = float((recent_60 <= current_bb).sum()) / len(recent_60)
                if pctile <= 0.20:
                    bb_score = 100.0
                elif pctile <= 0.40:
                    bb_score = 70.0
                else:
                    bb_score = max(0, 50 - (pctile - 0.40) * 100)
    scores["bb_squeeze"] = bb_score

    # --- 2. Volume Compression → Expansion (25%) ---
    vol_score = 50.0
    if "volume" in df.columns and len(df) >= 6:
        vol = df["volume"].astype(float)
        recent_5 = vol.tail(6).values
        today_vol = float(vol.iloc[-1])

        if len(recent_5) >= 5:
            x = np.arange(5, dtype=float)
            slope = float(np.polyfit(x, recent_5[:5], 1)[0])
            vol_declining = slope < 0
        else:
            vol_declining = False

        vol_sma = features.get("vol_sma_20")
        avg_vol_20 = float(vol_sma) if _valid(vol_sma) else float(vol.tail(6).iloc[:-1].mean())

        if vol_declining and avg_vol_20 > 0 and today_vol > 1.5 * avg_vol_20:
            vol_score = 100.0
        elif vol_declining:
            vol_score = 60.0
        elif avg_vol_20 > 0 and today_vol > 1.5 * avg_vol_20:
            vol_score = 70.0
    scores["vol_compression"] = vol_score

    # --- 3. Relative Strength vs CSI 300 (20%) ---
    rs_score = 50.0
    roc_10 = features.get("roc_10")
    if _valid(roc_10) and csi300_df is not None and len(csi300_df) >= 11:
        csi_close = csi300_df["close"].astype(float) if "close" in csi300_df.columns else csi300_df["Close"].astype(float)
        csi_roc_10 = float((csi_close.iloc[-1] - csi_close.iloc[-11]) / csi_close.iloc[-11] * 100)
        rs_diff = float(roc_10) - csi_roc_10
        if rs_diff > 5:
            rs_score = 100.0
        elif rs_diff > 2:
            rs_score = 80.0
        elif rs_diff > 0:
            rs_score = 60.0
        else:
            rs_score = max(0, 40 + rs_diff * 5)
    scores["relative_strength"] = rs_score

    # --- 4. Trend Alignment (15%) ---
    pct_above_sma50 = features.get("pct_above_sma50")
    pct_above_sma200 = features.get("pct_above_sma200")
    above_50 = _valid(pct_above_sma50) and float(pct_above_sma50) > 0
    above_200 = _valid(pct_above_sma200) and float(pct_above_sma200) > 0

    sma50 = features.get("sma_50")
    sma200 = features.get("sma_200")
    sma50_above_200 = (_valid(sma50) and _valid(sma200) and float(sma50) > float(sma200))

    if above_50 and sma50_above_200:
        trend_score = 100.0
    elif above_50:
        trend_score = 60.0
    elif above_200:
        trend_score = 40.0
    else:
        trend_score = 10.0
    scores["trend_alignment"] = trend_score

    # --- 5. Momentum Base (10%) ---
    rsi_14 = features.get("rsi_14")
    mom_score = 50.0
    if _valid(rsi_14):
        rsi_val = float(rsi_14)
        if 40 <= rsi_val <= 65:
            mom_score = 100.0
        elif 30 <= rsi_val < 40:
            mom_score = 60.0
        elif 65 < rsi_val <= 75:
            mom_score = 50.0
        else:
            mom_score = 20.0
    scores["momentum_base"] = mom_score

    # --- Composite ---
    weights = {
        "bb_squeeze": 0.30,
        "vol_compression": 0.25,
        "relative_strength": 0.20,
        "trend_alignment": 0.15,
        "momentum_base": 0.10,
    }
    composite = sum(scores[k] * weights[k] for k in weights)

    # Regime-dependent score floors
    regime_floors = {"bull": 60, "choppy": 65}
    floor = regime_floors.get(regime, 60)
    if composite < floor:
        return None

    # --- Price targets ---
    close_price = features.get("close", 0)
    atr = features.get("atr_14")
    if not _valid(close_price) or close_price <= 0:
        return None
    if not _valid(atr) or atr <= 0:
        atr = close_price * 0.03
    atr = max(atr, close_price * 0.005)

    stop_loss = close_price - stop_atr_mult * atr
    target_1 = close_price + target_atr_mult * atr
    target_2 = close_price + target_2_atr_mult * atr

    # Cap targets at board-aware daily limit
    daily_limit = get_daily_limit(ticker, is_st=is_st)
    cap = close_price * (1 + daily_limit)
    target_1 = min(target_1, cap)
    target_2 = min(target_2, cap)

    # MAS-style no-chase ceiling for T+1 / 09:35 execution checks.
    # Keep this explicit on the signal so morning_check can CANCEL rather
    # than falling back to a broad global gap threshold.
    max_entry = min(close_price * (1 + max_entry_pct), cap)

    return SniperSignal(
        ticker=ticker,
        score=round(composite, 2),
        direction="LONG",
        entry_price=round(close_price, 2),
        stop_loss=round(stop_loss, 2),
        target_1=round(target_1, 2),
        target_2=round(target_2, 2),
        holding_period=holding_period,
        components=scores,
        max_entry_price=round(max_entry, 2),
    )
