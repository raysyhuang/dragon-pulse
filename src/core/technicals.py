"""
Technical Indicator Calculations

Pure functions that compute technical metrics from OHLCV data.
"""

from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Optional


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR).
    
    Args:
        df: DataFrame with High, Low, Close columns
        period: ATR period (default: 14)
    
    Returns:
        Series with ATR values
    """
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1
    ).max(axis=1)
    return tr.rolling(period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        series: Price series (typically Close)
        period: RSI period (default: 14)
    
    Returns:
        Series with RSI values (0-100)
    """
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def sma(series: pd.Series, n: int) -> pd.Series:
    """
    Calculate Simple Moving Average (SMA).
    
    Args:
        series: Price series
        n: Period
    
    Returns:
        Series with SMA values
    """
    return series.rolling(n).mean()


def compute_technicals(
    df: pd.DataFrame,
    ma_windows: list[int] = [10, 20, 50],
    rsi_period: int = 14,
    atr_period: int = 14,
    realized_vol_window: int = 5,
) -> pd.DataFrame:
    """
    Compute all technical indicators for a ticker.
    
    Args:
        df: DataFrame with OHLCV columns
        ma_windows: List of MA periods (default: [10, 20, 50])
        rsi_period: RSI period (default: 14)
        atr_period: ATR period (default: 14)
        realized_vol_window: Window for realized volatility (default: 5)
    
    Returns:
        DataFrame with computed technical metrics (one row per date)
    """
    if df.empty:
        return pd.DataFrame()
    
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]
    
    results = pd.DataFrame(index=df.index)
    
    # RSI
    results["rsi"] = rsi(close, rsi_period)
    
    # Moving Averages
    for window in ma_windows:
        if len(close) >= window:
            results[f"ma{window}"] = sma(close, window)
            results[f"above_ma{window}"] = (close >= results[f"ma{window}"]).astype(int)
    
    # 52W High (use High, not Close)
    if len(high) >= 252:
        high_52w = high.tail(252).max()
    else:
        high_52w = high.max()
    results["high_52w"] = high_52w
    results["dist_to_52w_high_pct"] = ((high_52w - close) / high_52w * 100)
    
    # ATR
    results["atr"] = atr(df, atr_period)
    results["atr_pct"] = (results["atr"] / close * 100)
    
    # Volume metrics
    if len(volume) >= 20:
        results["avg_volume_20d"] = volume.tail(20).mean()
        results["volume_ratio_3d_to_20d"] = volume.tail(3).mean() / results["avg_volume_20d"]
    else:
        results["avg_volume_20d"] = np.nan
        results["volume_ratio_3d_to_20d"] = np.nan
    
    # Average Dollar Volume (20d)
    if len(close) >= 20 and len(volume) >= 20:
        results["avg_dollar_volume_20d"] = (close.tail(20) * volume.tail(20)).mean()
    else:
        results["avg_dollar_volume_20d"] = np.nan
    
    # Realized Volatility (5-day, annualized)
    if len(close) >= realized_vol_window + 1:
        returns = close.pct_change().tail(realized_vol_window)
        std_5d = returns.std()
        results["realized_vol_5d_ann_pct"] = std_5d * np.sqrt(252) * 100
    else:
        results["realized_vol_5d_ann_pct"] = np.nan
    
    # Returns
    if len(close) >= 6:
        results["ret_5d_pct"] = ((close.iloc[-1] / close.iloc[-6]) - 1) * 100
    else:
        results["ret_5d_pct"] = np.nan
    
    if len(close) >= 21:
        results["ret_20d_pct"] = ((close.iloc[-1] / close.iloc[-21]) - 1) * 100
    else:
        results["ret_20d_pct"] = np.nan
    
    # Current price
    results["last_price"] = close.iloc[-1]
    
    return results


def compute_extended_technicals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Backward-compatible alias for older Dragon Pulse modules.

    The current codebase only maintains one technical feature builder, so the
    "extended" API delegates to ``compute_technicals``.
    """
    return compute_technicals(df)
