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

from src.core.cn_limits import get_daily_limit
from src.core.universe import is_star_market_ticker

DEFAULT_RS_PULLBACK_REGIMES = ("bull",)


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
    is_st: bool = False,
) -> AlphaCandidateSignal:
    atr = max(float(atr), close * 0.01)
    daily_limit = get_daily_limit(ticker, is_st=is_st)
    limit_cap = close * (1 + daily_limit)
    target_1 = min(close + target_atr_mult * atr, limit_cap)
    target_2 = min(close + target_2_atr_mult * atr, limit_cap)
    max_entry = min(close * (1 + max_entry_pct), limit_cap)
    return AlphaCandidateSignal(
        ticker=ticker,
        score=round(float(score), 2),
        direction="LONG",
        entry_price=round(close, 2),
        stop_loss=round(close - stop_atr_mult * atr, 2),
        target_1=round(target_1, 2),
        target_2=round(target_2, 2),
        holding_period=int(holding_period),
        components=components,
        max_entry_price=round(max_entry, 2),
        subtype=subtype,
    )


def score_rs_pullback_alpha(
    ticker: str,
    df: pd.DataFrame,
    regime: str,
    csi300_df: pd.DataFrame | None = None,
    *,
    is_st: bool = False,
    regimes: tuple[str, ...] = DEFAULT_RS_PULLBACK_REGIMES,
    score_floor: float = 80.0,
    max_entry_pct: float = 0.02,
    min_adv_cny: float = 80_000_000,
    stop_atr_mult: float = 1.1,
    target_atr_mult: float = 2.1,
    target_2_atr_mult: float = 3.6,
    holding_period: int = 5,
) -> AlphaCandidateSignal | None:
    """Strong-stock pullback: controlled first dip in a relative-strength leader."""
    if (
        is_st
        or is_star_market_ticker(ticker)
        or regime not in regimes
        or df.empty
        or len(df) < 140
        or csi300_df is None
        or len(csi300_df) < 61
    ):
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
        is_st=is_st,
    )


def score_sniper_breakout_alpha(
    ticker: str,
    df: pd.DataFrame,
    regime: str,
    csi300_df: pd.DataFrame | None = None,
    *,
    is_st: bool = False,
    regimes: tuple[str, ...] = ("bull",),
    score_floor: float = 82.0,
    max_entry_pct: float = 0.03,
    min_adv_cny: float = 120_000_000,
    stop_atr_mult: float = 1.35,
    target_atr_mult: float = 2.8,
    target_2_atr_mult: float = 4.2,
    holding_period: int = 5,
    min_stop_risk_pct: float = 0.0,
    max_stop_risk_pct: float = 99.0,
) -> AlphaCandidateSignal | None:
    """A-share Sniper v2: high-conviction breakout/continuation leader.

    This is intentionally stricter than the quarantined legacy Sniper. It seeks
    MAS-like expansion setups: strong relative strength, clean uptrend, fresh
    breakout/near-breakout, volume expansion after a base, and no exhausted
    one-day limit-up chase. It remains a research alpha until PIT/no-chase
    replay proves it beats the RS Pullback baseline.
    """
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
    limit_pct = get_daily_limit(ticker, is_st=is_st)
    # Avoid limit-up/near-limit exhaustion and red failed breakouts.
    if day_ret >= limit_pct - 0.008 or day_ret < -0.012:
        return None

    sma20 = close_s.rolling(20).mean().iloc[-1]
    sma50 = close_s.rolling(50).mean().iloc[-1]
    sma120 = close_s.rolling(120).mean().iloc[-1]
    atr = (high_s - low_s).rolling(14).mean().iloc[-1]
    atr_pct = atr / close * 100 if close else np.nan
    stop_risk_pct = stop_atr_mult * atr / close * 100 if close else np.nan
    if not all(_valid(x) for x in [sma20, sma50, sma120, atr_pct]):
        return None
    if not (close > sma20 > sma50 > sma120 * 0.98):
        return None
    if atr_pct < 2.0 or atr_pct > 7.5:
        return None
    if not _valid(stop_risk_pct) or stop_risk_pct < min_stop_risk_pct or stop_risk_pct > max_stop_risk_pct:
        return None
    if close / sma20 > 1.09:
        return None

    prior20_high = float(high_s.iloc[-21:-1].max())
    high_60 = float(high_s.tail(60).max())
    high_120 = float(high_s.tail(120).max())
    breakout_strength = close / prior20_high - 1 if prior20_high else -1
    near_60_high = close / high_60 if high_60 else 0
    near_120_high = close / high_120 if high_120 else 0
    # Require either a fresh 20D breakout or a very tight consolidation just under highs.
    if breakout_strength < -0.006 and near_60_high < 0.975:
        return None
    if near_120_high < 0.90:
        return None

    vol20 = float(vol_s.tail(20).mean())
    vol5_prev = float(vol_s.iloc[-6:-1].mean())
    rvol = float(vol_s.iloc[-1] / vol20) if vol20 > 0 else np.nan
    dryup = vol5_prev / vol20 if vol20 > 0 else np.nan
    if not _valid(rvol) or rvol < 1.10:
        return None
    # Prefer expansion, but reject obvious blow-off exhaustion.
    if rvol > 4.5 and day_ret > 0.06:
        return None

    tr = (high_s - low_s) / close_s
    range10_pct = (float(high_s.tail(10).max()) / float(low_s.tail(10).min()) - 1) * 100
    tr20 = tr.rolling(20).mean()
    comp_pct = _rolling_rank_percentile(tr20.tail(80), float(tr20.iloc[-1]))

    rs20 = rs60 = np.nan
    rs_score = 50.0
    if csi300_df is not None and len(csi300_df) >= 61:
        csi_close = csi300_df[_col(csi300_df, "close")].astype(float)
        rs20 = _pct(close_s.iloc[-1], close_s.iloc[-21]) - _pct(csi_close.iloc[-1], csi_close.iloc[-21])
        rs60 = _pct(close_s.iloc[-1], close_s.iloc[-61]) - _pct(csi_close.iloc[-1], csi_close.iloc[-61])
        # MAS-like Sniper must be a real leader; weak relative strength is not a Sniper.
        if rs20 < 2.0 or rs60 < 5.0:
            return None
        rs_score = min(100.0, max(0.0, 45 + rs20 * 3.2 + rs60 * 1.5))

    breakout_score = min(100.0, max(0.0, 55 + breakout_strength * 1800 + max(0.0, near_60_high - 0.97) * 650))
    trend_score = min(100.0, max(0.0, 45 + _pct(sma20, sma50) * 7 + _pct(close, sma120) * 1.2))
    volume_score = min(100.0, max(0.0, 35 + (rvol - 1.0) * 32 + max(0.0, 1.0 - dryup) * 22))
    compression_score = 100.0 if _valid(comp_pct) and comp_pct <= 0.35 else 75.0 if _valid(comp_pct) and comp_pct <= 0.55 else 45.0
    base_quality = min(100.0, max(0.0, 95 - max(0.0, range10_pct - 13.0) * 3.0))
    not_exhausted = min(100.0, max(0.0, 100 - max(0.0, day_ret - 0.055) * 1200 - max(0.0, close / sma20 - 1.06) * 700))

    components = {
        "rs": rs_score,
        "breakout": breakout_score,
        "volume": volume_score,
        "trend": trend_score,
        "compression": compression_score,
        "base_quality": base_quality,
        "not_exhausted": not_exhausted,
    }
    weights = {
        "rs": .28,
        "breakout": .22,
        "volume": .16,
        "trend": .14,
        "compression": .08,
        "base_quality": .07,
        "not_exhausted": .05,
    }
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
        is_st=is_st,
    )


def score_limitup_continuation_alpha(
    ticker: str,
    df: pd.DataFrame,
    regime: str,
    csi300_df: pd.DataFrame | None = None,
    *,
    is_st: bool = False,
    regimes: tuple[str, ...] = ("bull",),
    score_floor: float = 78.0,
    max_entry_pct: float = 0.02,
    min_adv_cny: float = 80_000_000,
    stop_atr_mult: float = 1.25,
    target_atr_mult: float = 2.4,
    target_2_atr_mult: float = 3.8,
    holding_period: int = 4,
    max_atr_pct: float = 7.5,
    min_rs20: float = 3.0,
    min_post_impulse_hold: float = -0.075,
    max_post_impulse_extension: float = 0.055,
    min_stop_risk_pct: float = 0.0,
    max_stop_risk_pct: float = 99.0,
) -> AlphaCandidateSignal | None:
    """A-share limit-up continuation research sleeve.

    Looks for a recent first-board/near-board impulse that has paused without
    collapsing, then resumes on controlled volume. This targets A-share theme
    momentum while rejecting same-day limit-up chase.
    """
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

    limit_pct = get_daily_limit(ticker, is_st=is_st)
    day_ret = close / prev - 1
    if day_ret >= limit_pct - 0.008 or day_ret < -0.018:
        return None

    ret = close_s.pct_change()
    recent_impulse = ret.iloc[-7:-1]
    impulse_mask = recent_impulse >= limit_pct - 0.012
    if not bool(impulse_mask.any()):
        return None
    impulse_offset = int(np.where(impulse_mask.to_numpy())[0][-1])
    days_since_impulse = len(recent_impulse) - impulse_offset
    if days_since_impulse < 2 or days_since_impulse > 6:
        return None

    sma20 = close_s.rolling(20).mean().iloc[-1]
    sma50 = close_s.rolling(50).mean().iloc[-1]
    atr = (high_s - low_s).rolling(14).mean().iloc[-1]
    if not all(_valid(x) for x in [sma20, sma50, atr]):
        return None
    atr_pct = atr / close * 100 if close else np.nan
    stop_risk_pct = stop_atr_mult * atr / close * 100 if close else np.nan
    if not _valid(atr_pct) or atr_pct > max_atr_pct:
        return None
    if not _valid(stop_risk_pct) or stop_risk_pct < min_stop_risk_pct or stop_risk_pct > max_stop_risk_pct:
        return None
    if not (close > sma20 > sma50 * 0.98):
        return None

    impulse_close = float(close_s.iloc[-1 - days_since_impulse])
    pullback_from_impulse = close / impulse_close - 1
    if pullback_from_impulse < min_post_impulse_hold or pullback_from_impulse > max_post_impulse_extension:
        return None
    high_20 = float(high_s.tail(20).max())
    if close / high_20 < 0.90:
        return None

    vol20 = float(vol_s.tail(20).mean())
    rvol = float(vol_s.iloc[-1] / vol20) if vol20 > 0 else np.nan
    if not _valid(rvol) or rvol < 0.75 or rvol > 3.8:
        return None

    rs_score = 55.0
    if csi300_df is not None and len(csi300_df) >= 21:
        csi_close = csi300_df[_col(csi300_df, "close")].astype(float)
        rs20 = _pct(close_s.iloc[-1], close_s.iloc[-21]) - _pct(csi_close.iloc[-1], csi_close.iloc[-21])
        if rs20 < min_rs20:
            return None
        rs_score = min(100.0, max(0.0, 50 + rs20 * 3.0))

    continuation = min(100.0, max(0.0, 70 - abs(pullback_from_impulse - 0.005) * 650))
    impulse_freshness = {2: 100.0, 3: 92.0, 4: 82.0, 5: 70.0, 6: 60.0}.get(days_since_impulse, 50.0)
    volume = min(100.0, max(0.0, 50 + (rvol - 0.8) * 30))
    trend = min(100.0, max(0.0, 55 + _pct(sma20, sma50) * 8 + _pct(close, sma20) * 2))
    not_chasing = min(100.0, max(0.0, 100 - max(0.0, day_ret - 0.045) * 1300))
    components = {
        "rs": rs_score,
        "continuation": continuation,
        "impulse_freshness": impulse_freshness,
        "volume": volume,
        "trend": trend,
        "not_chasing": not_chasing,
    }
    weights = {"rs": .24, "continuation": .24, "impulse_freshness": .18, "volume": .12, "trend": .14, "not_chasing": .08}
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
        subtype="limitup_continuation_alpha",
        is_st=is_st,
    )


def score_accumulation_breakout_alpha(
    ticker: str,
    df: pd.DataFrame,
    regime: str,
    csi300_df: pd.DataFrame | None = None,
    *,
    is_st: bool = False,
    regimes: tuple[str, ...] = ("bull",),
    score_floor: float = 76.0,
    max_entry_pct: float = 0.025,
    min_adv_cny: float = 80_000_000,
    stop_atr_mult: float = 1.2,
    target_atr_mult: float = 2.3,
    target_2_atr_mult: float = 3.6,
    holding_period: int = 5,
    min_breakout_proximity: float = 0.985,
    max_sma20_extension: float = 1.075,
    min_rvol: float = 1.15,
    max_dryup: float = 1.15,
) -> AlphaCandidateSignal | None:
    """Accumulation breakout research sleeve: dry-up base + fresh expansion."""
    if is_st or regime not in regimes or df.empty or len(df) < 150:
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
    limit_pct = get_daily_limit(ticker, is_st=is_st)
    if day_ret >= limit_pct - 0.01 or day_ret < -0.012:
        return None

    sma20 = close_s.rolling(20).mean().iloc[-1]
    sma50 = close_s.rolling(50).mean().iloc[-1]
    sma120 = close_s.rolling(120).mean().iloc[-1]
    atr = (high_s - low_s).rolling(14).mean().iloc[-1]
    if not all(_valid(x) for x in [sma20, sma50, sma120, atr]):
        return None
    if not (close > sma20 > sma50 > sma120 * 0.97):
        return None

    high_60_prior = float(high_s.iloc[-61:-1].max())
    if close < high_60_prior * min_breakout_proximity:
        return None
    if close / sma20 > max_sma20_extension:
        return None

    vol20 = float(vol_s.tail(20).mean())
    vol10_prev = float(vol_s.iloc[-11:-1].mean())
    vol50 = float(vol_s.tail(50).mean())
    rvol = float(vol_s.iloc[-1] / vol20) if vol20 > 0 else np.nan
    dryup = vol10_prev / vol50 if vol50 > 0 else np.nan
    if not _valid(rvol) or not _valid(dryup):
        return None
    if rvol < min_rvol or dryup > max_dryup:
        return None

    obv_score = 50.0
    if "obv" in df.columns:
        obv = df["obv"].astype(float)
        if len(obv.dropna()) >= 60:
            obv_score = min(100.0, max(0.0, 45 + _pct(obv.iloc[-1], obv.iloc[-21]) * 1.8))

    rs_score = 55.0
    if csi300_df is not None and len(csi300_df) >= 61:
        csi_close = csi300_df[_col(csi300_df, "close")].astype(float)
        rs20 = _pct(close_s.iloc[-1], close_s.iloc[-21]) - _pct(csi_close.iloc[-1], csi_close.iloc[-21])
        rs60 = _pct(close_s.iloc[-1], close_s.iloc[-61]) - _pct(csi_close.iloc[-1], csi_close.iloc[-61])
        if rs20 < 1.0 or rs60 < 2.0:
            return None
        rs_score = min(100.0, max(0.0, 45 + rs20 * 2.7 + rs60 * 1.1))

    breakout = min(100.0, max(0.0, 55 + (close / high_60_prior - 1) * 1600))
    accumulation = min(100.0, max(0.0, 70 + max(0.0, 1.0 - dryup) * 55 + (rvol - 1.0) * 20))
    trend = min(100.0, max(0.0, 55 + _pct(sma20, sma50) * 7 + _pct(close, sma120) * 1.1))
    not_extended = min(100.0, max(0.0, 100 - max(0.0, close / sma20 - 1.045) * 900))
    components = {
        "rs": rs_score,
        "breakout": breakout,
        "accumulation": accumulation,
        "trend": trend,
        "obv": obv_score,
        "not_extended": not_extended,
    }
    weights = {"rs": .24, "breakout": .22, "accumulation": .22, "trend": .14, "obv": .10, "not_extended": .08}
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
        subtype="accumulation_breakout_alpha",
        is_st=is_st,
    )
