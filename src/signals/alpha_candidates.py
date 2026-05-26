"""Research alpha candidates promoted from the Dragon Pulse alpha tournament.

These engines are disabled by default and must remain research/dry-run until
full integrated backtests validate them in the production pipeline. They are
separate from the legacy Sniper/MR engines.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


def _valid(x) -> bool:
    if x is None:
        return False
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


@dataclass
class AlphaCandidateSignal:
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
    subtype: str = "alpha"


def _col(df: pd.DataFrame, name: str) -> str:
    return name if name in df.columns else name.capitalize()


def _pct(a: float, b: float) -> float:
    return (a / b - 1.0) * 100.0 if b else np.nan


def _rolling_rank_percentile(series: pd.Series, value: float) -> float:
    s = series.dropna()
    if len(s) == 0 or not np.isfinite(value):
        return np.nan
    return float((s <= value).sum()) / float(len(s))


def _base_liquidity(df: pd.DataFrame, min_adv_cny: float) -> bool:
    if len(df) < 80:
        return False
    close = df[_col(df, "close")].astype(float)
    volume = df[_col(df, "volume")].astype(float)
    return float((close.tail(20) * volume.tail(20)).mean()) >= min_adv_cny


def _make_signal(
    ticker: str,
    score: float,
    close: float,
    atr: float,
    *,
    stop_atr_mult: float,
    target_atr_mult: float,
    target_2_atr_mult: float,
    holding_period: int,
    max_entry_pct: float,
    components: dict,
    subtype: str,
) -> AlphaCandidateSignal:
    atr = max(float(atr), close * 0.01)
    return AlphaCandidateSignal(
        ticker=ticker,
        score=round(float(score), 2),
        direction="LONG",
        entry_price=round(close, 2),
        stop_loss=round(close - stop_atr_mult * atr, 2),
        target_1=round(close + target_atr_mult * atr, 2),
        target_2=round(close + target_2_atr_mult * atr, 2),
        holding_period=int(holding_period),
        components=components,
        max_entry_price=round(close * (1 + max_entry_pct), 2),
        subtype=subtype,
    )


def score_rs_pullback_alpha(
    ticker: str,
    df: pd.DataFrame,
    regime: str,
    csi300_df: pd.DataFrame | None = None,
    *,
    is_st: bool = False,
    regimes: tuple[str, ...] = ("bull", "bear"),
    score_floor: float = 80.0,
    max_entry_pct: float = 0.02,
    min_adv_cny: float = 80_000_000,
    stop_atr_mult: float = 1.1,
    target_atr_mult: float = 2.1,
    target_2_atr_mult: float = 3.6,
    holding_period: int = 5,
) -> AlphaCandidateSignal | None:
    """Strong-stock pullback: controlled first dip in a relative-strength leader."""
    if is_st or regime not in regimes or df.empty or len(df) < 140:
        return None
    if not _base_liquidity(df, min_adv_cny=min_adv_cny):
        return None

    close_s = df[_col(df, "close")].astype(float)
    high_s = df[_col(df, "high")].astype(float)
    low_s = df[_col(df, "low")].astype(float)
    vol_s = df[_col(df, "volume")].astype(float)
    close = float(close_s.iloc[-1])
    prev = float(close_s.iloc[-2])
    if close <= 0 or prev <= 0:
        return None
    day_ret = close / prev - 1
    if day_ret < -0.055 or day_ret > 0.025:
        return None

    sma20 = close_s.rolling(20).mean().iloc[-1]
    sma50 = close_s.rolling(50).mean().iloc[-1]
    sma120 = close_s.rolling(120).mean().iloc[-1]
    atr = (high_s - low_s).rolling(14).mean().iloc[-1]
    if not all(_valid(x) for x in [sma20, sma50, sma120, atr]):
        return None
    if not (sma20 > sma50 > sma120 * 0.96):
        return None

    hi60 = float(high_s.tail(60).max())
    drawdown = close / hi60 - 1 if hi60 else -1
    if drawdown > -0.015 or drawdown < -0.10:
        return None
    if close < sma50 * 0.98:
        return None

    vol20 = float(vol_s.tail(20).mean())
    rvol = float(vol_s.iloc[-1] / vol20) if vol20 > 0 else 1.0
    if rvol > 1.8 and day_ret < -0.025:
        return None

    rs_score = 50.0
    if csi300_df is not None and len(csi300_df) >= 61:
        csi_close = csi300_df[_col(csi300_df, "close")].astype(float)
        rs20 = _pct(close_s.iloc[-1], close_s.iloc[-21]) - _pct(csi_close.iloc[-1], csi_close.iloc[-21])
        rs60 = _pct(close_s.iloc[-1], close_s.iloc[-61]) - _pct(csi_close.iloc[-1], csi_close.iloc[-61])
        if rs20 < -3 or rs60 < 0:
            return None
        rs_score = min(100.0, max(0.0, 50 + rs20 * 2.5 + rs60 * 1.3))

    pull_quality = min(100.0, max(0.0, 100 - abs(drawdown + 0.045) * 900))
    trend = min(100.0, max(0.0, 55 + _pct(sma20, sma50) * 7 + _pct(close, sma120) * 1.5))
    volume = 90.0 if rvol < 0.9 else 75.0 if rvol < 1.3 else 55.0
    components = {"rs": rs_score, "pull_quality": pull_quality, "trend": trend, "volume": volume}
    score = sum(components[k] * {"rs": .35, "pull_quality": .30, "trend": .25, "volume": .10}[k] for k in components)
    if score < score_floor:
        return None
    return _make_signal(
        ticker, score, close, atr,
        stop_atr_mult=stop_atr_mult,
        target_atr_mult=target_atr_mult,
        target_2_atr_mult=target_2_atr_mult,
        holding_period=holding_period,
        max_entry_pct=max_entry_pct,
        components=components,
        subtype="rs_pullback_alpha",
    )


def score_sniper_breakout_alpha(
    ticker: str,
    df: pd.DataFrame,
    regime: str,
    csi300_df: pd.DataFrame | None = None,
    *,
    is_st: bool = False,
    regimes: tuple[str, ...] = ("bull", "bear", "choppy"),
    score_floor: float = 74.0,
    max_entry_pct: float = 0.03,
    min_adv_cny: float = 80_000_000,
    stop_atr_mult: float = 1.6,
    target_atr_mult: float = 3.2,
    target_2_atr_mult: float = 4.7,
    holding_period: int = 5,
) -> AlphaCandidateSignal | None:
    """A-share Sniper-like breakout/continuation candidate from the reset tournament."""
    if is_st or regime not in regimes or df.empty or len(df) < 120:
        return None
    if not _base_liquidity(df, min_adv_cny=min_adv_cny):
        return None

    close_s = df[_col(df, "close")].astype(float)
    high_s = df[_col(df, "high")].astype(float)
    low_s = df[_col(df, "low")].astype(float)
    vol_s = df[_col(df, "volume")].astype(float)
    close = float(close_s.iloc[-1])
    prev = float(close_s.iloc[-2])
    if close <= 0 or prev <= 0:
        return None
    day_ret = close / prev - 1
    if day_ret > 0.095 or day_ret < -0.02:
        return None

    sma20 = close_s.rolling(20).mean().iloc[-1]
    sma50 = close_s.rolling(50).mean().iloc[-1]
    sma120 = close_s.rolling(120).mean().iloc[-1]
    atr = (high_s - low_s).rolling(14).mean().iloc[-1]
    atr_pct = atr / close * 100 if close else np.nan
    if not all(_valid(x) for x in [sma20, sma50, atr_pct]):
        return None
    if close < sma20 or sma20 < sma50:
        return None
    if _valid(sma120) and close < sma120:
        return None
    if atr_pct < 2.0 or atr_pct > 8.0:
        return None

    prior20_high = float(high_s.iloc[-21:-1].max())
    high_60 = float(high_s.tail(60).max())
    breakout_strength = close / prior20_high - 1 if prior20_high else -1
    near_high = close / high_60 if high_60 else 0
    if breakout_strength < -0.01 or near_high < 0.92:
        return None

    vol20 = float(vol_s.tail(20).mean())
    vol5_prev = float(vol_s.iloc[-6:-1].mean())
    rvol = float(vol_s.iloc[-1] / vol20) if vol20 > 0 else np.nan
    dryup = vol5_prev / vol20 if vol20 > 0 else np.nan
    if not _valid(rvol):
        return None

    tr = (high_s - low_s) / close_s
    tr20 = tr.rolling(20).mean()
    comp_pct = _rolling_rank_percentile(tr20.tail(80), float(tr20.iloc[-1]))

    rs_score = 50.0
    if csi300_df is not None and len(csi300_df) >= 61:
        csi_close = csi300_df[_col(csi300_df, "close")].astype(float)
        rs20 = _pct(close_s.iloc[-1], close_s.iloc[-21]) - _pct(csi_close.iloc[-1], csi_close.iloc[-21])
        rs60 = _pct(close_s.iloc[-1], close_s.iloc[-61]) - _pct(csi_close.iloc[-1], csi_close.iloc[-61])
        rs_score = min(100.0, max(0.0, 50 + rs20 * 3 + rs60 * 1.2))

    components = {
        "breakout": min(100.0, max(0.0, 50 + breakout_strength * 2000)),
        "near_high": min(100.0, max(0.0, (near_high - 0.90) * 500)),
        "volume": min(100.0, max(0.0, 40 + (rvol - 1.0) * 35 + max(0.0, 1.0 - dryup) * 25)),
        "compression": 100.0 if _valid(comp_pct) and comp_pct <= 0.35 else 60.0 if _valid(comp_pct) and comp_pct <= 0.55 else 30.0,
        "rs": rs_score,
        "not_extended": max(0.0, 100 - max(0.0, close / sma20 - 1.08) * 800),
    }
    weights = {"breakout": .25, "near_high": .15, "volume": .20, "compression": .15, "rs": .20, "not_extended": .05}
    score = sum(components[k] * weights[k] for k in components)
    if score < score_floor:
        return None
    return _make_signal(
        ticker, score, close, atr,
        stop_atr_mult=stop_atr_mult,
        target_atr_mult=target_atr_mult,
        target_2_atr_mult=target_2_atr_mult,
        holding_period=holding_period,
        max_entry_pct=max_entry_pct,
        components=components,
        subtype="sniper_breakout_alpha",
    )
