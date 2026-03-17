"""Technical feature engineering — ATR, RSI, VWAP deviation, RVOL, MAs, volume stats, streaks."""

from __future__ import annotations

import numpy as np
import pandas as pd

try:
    import pandas_ta as ta  # original pandas-ta (requires Python 3.12+)
except (ImportError, ModuleNotFoundError, AttributeError):
    import pandas_ta_classic as ta  # Python 3.11 compatible fork


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize OHLCV columns to lowercase for pandas_ta compatibility."""
    col_map = {c: c.lower() for c in df.columns if c in ("Open", "High", "Low", "Close", "Volume")}
    return df.rename(columns=col_map)


def compute_all_technical_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all technical indicators on an OHLCV DataFrame.

    Accepts columns in any case (Open/open, High/high, etc.).
    Returns the same DataFrame with new indicator columns appended, all lowercase.
    """
    df = _normalize_columns(df.copy())

    if df.empty or len(df) < 20:
        return df

    # --- Trend / Moving Averages ---
    df["sma_10"] = ta.sma(df["close"], length=10)
    df["sma_20"] = ta.sma(df["close"], length=20)
    df["sma_50"] = ta.sma(df["close"], length=50)
    df["sma_200"] = ta.sma(df["close"], length=200)
    df["ema_9"] = ta.ema(df["close"], length=9)
    df["ema_21"] = ta.ema(df["close"], length=21)

    # Price vs MAs
    df["pct_above_sma20"] = (df["close"] - df["sma_20"]) / df["sma_20"].replace(0, pd.NA) * 100
    df["pct_above_sma50"] = (df["close"] - df["sma_50"]) / df["sma_50"].replace(0, pd.NA) * 100
    df["pct_above_sma200"] = (df["close"] - df["sma_200"]) / df["sma_200"].replace(0, pd.NA) * 100

    # MA slope: normalized 10-bar change
    df["sma_20_slope"] = (df["sma_20"] - df["sma_20"].shift(10)) / df["sma_20"].shift(10).replace(0, pd.NA) * 100

    # --- Momentum ---
    df["rsi_14"] = ta.rsi(df["close"], length=14)
    df["rsi_2"] = ta.rsi(df["close"], length=2)

    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        df = pd.concat([df, macd], axis=1)

    df["roc_5"] = ta.roc(df["close"], length=5)
    df["roc_10"] = ta.roc(df["close"], length=10)

    # --- Volatility ---
    atr = ta.atr(df["high"], df["low"], df["close"], length=14)
    if atr is not None:
        df["atr_14"] = atr
        df["atr_pct"] = df["atr_14"] / df["close"].replace(0, pd.NA) * 100

    bb = ta.bbands(df["close"], length=20, std=2)
    if bb is not None and not bb.empty:
        df = pd.concat([df, bb], axis=1)

    # BB width percentile (60-bar) for squeeze detection
    if "BBB_20_2.0" in df.columns:
        df["bb_width_pctile_60"] = df["BBB_20_2.0"].rolling(60).rank(pct=True)

    # Volume slope (5-bar regression) for compression detection
    if len(df) >= 5:
        df["vol_slope_5"] = df["volume"].rolling(5).apply(
            lambda x: np.polyfit(np.arange(len(x)), x, 1)[0], raw=False
        )

    # --- Volume ---
    df["vol_sma_20"] = ta.sma(df["volume"], length=20)
    df["rvol"] = df["volume"] / df["vol_sma_20"].replace(0, pd.NA)

    df["volume_surge"] = (df["volume"] > df["vol_sma_20"] * 2.0).astype(int)

    obv = ta.obv(df["close"], df["volume"])
    if obv is not None:
        df["obv"] = obv

    # --- VWAP deviation (approximate) ---
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cum_tp_vol = (typical_price * df["volume"]).cumsum()
    cum_vol = df["volume"].cumsum()
    df["vwap_approx"] = cum_tp_vol / cum_vol.replace(0, pd.NA)
    df["vwap_dev"] = (df["close"] - df["vwap_approx"]) / df["vwap_approx"].replace(0, pd.NA) * 100

    # --- Breakout signals ---
    df["high_20d"] = df["high"].rolling(20).max()
    df["low_20d"] = df["low"].rolling(20).min()
    df["near_20d_high"] = (df["close"] >= df["high_20d"] * 0.98).astype(int)
    df["near_20d_low"] = (df["close"] <= df["low_20d"] * 1.02).astype(int)

    # Consolidation detection
    df["range_10d"] = df["high"].rolling(10).max() - df["low"].rolling(10).min()
    if "atr_14" in df.columns:
        df["is_consolidating"] = (df["range_10d"] < df["atr_14"] * 1.5).astype(int)

    # Gap detection
    df["gap_pct"] = (df["open"] - df["close"].shift(1)) / df["close"].shift(1).replace(0, pd.NA) * 100
    df["is_gap_up"] = (df["gap_pct"] >= 3.0).astype(int)
    df["is_gap_down"] = (df["gap_pct"] <= -3.0).astype(int)

    return df


def compute_rsi2_features(df: pd.DataFrame) -> pd.DataFrame:
    """Specific features for the RSI(2) mean-reversion model."""
    df = _normalize_columns(df.copy())

    if df.empty or len(df) < 10:
        return df

    df["rsi_2"] = ta.rsi(df["close"], length=2)

    # Streak counting: consecutive down/up days
    df["daily_return"] = df["close"].pct_change()
    streak = []
    current = 0
    for ret in df["daily_return"]:
        if pd.isna(ret):
            streak.append(0)
            continue
        if ret < 0:
            current = current - 1 if current < 0 else -1
        elif ret > 0:
            current = current + 1 if current > 0 else 1
        else:
            current = 0
        streak.append(current)
    df["streak"] = streak

    # Distance from 5-day low
    df["low_5d"] = df["low"].rolling(5).min()
    df["dist_from_5d_low"] = (df["close"] - df["low_5d"]) / df["low_5d"].replace(0, pd.NA) * 100

    return df


def latest_features(df: pd.DataFrame) -> dict:
    """Extract the most recent row's features as a flat dictionary."""
    if df.empty:
        return {}
    row = df.iloc[-1]
    return {k: (None if pd.isna(v) else float(v) if isinstance(v, (np.floating, float)) else v)
            for k, v in row.items()}
