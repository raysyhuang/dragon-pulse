"""Tests for regime classification and market breadth."""

import sys
from pathlib import Path

# strategy modules use bare imports (from core.xxx), so add src/ to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest
import pandas as pd
import numpy as np
from strategy.regime import compute_breadth, classify_regime, RegimeAssessment


def _make_price_df(prices: list[float]) -> pd.DataFrame:
    """Helper to create a minimal OHLCV DataFrame from close prices."""
    dates = pd.date_range("2025-01-01", periods=len(prices), freq="B")
    return pd.DataFrame({
        "Open": prices,
        "High": [p * 1.01 for p in prices],
        "Low": [p * 0.99 for p in prices],
        "Close": prices,
        "Volume": [1_000_000] * len(prices),
    }, index=dates)


class TestComputeBreadth:
    """Tests for compute_breadth()."""

    def test_all_above_sma(self):
        # Steadily rising prices → all above SMA20
        rising = list(range(100, 130))
        universe = {f"T{i}": _make_price_df(rising) for i in range(10)}
        breadth = compute_breadth(universe, sma_period=20)
        assert breadth == 1.0

    def test_all_below_sma(self):
        # Steadily falling prices → all below SMA20
        falling = list(range(130, 100, -1))
        universe = {f"T{i}": _make_price_df(falling) for i in range(10)}
        breadth = compute_breadth(universe, sma_period=20)
        assert breadth == 0.0

    def test_mixed_breadth(self):
        rising = list(range(100, 130))
        falling = list(range(130, 100, -1))
        universe = {
            "BULL1": _make_price_df(rising),
            "BULL2": _make_price_df(rising),
            "BEAR1": _make_price_df(falling),
            "BEAR2": _make_price_df(falling),
        }
        breadth = compute_breadth(universe, sma_period=20)
        assert breadth == 0.5

    def test_empty_universe_returns_neutral(self):
        assert compute_breadth({}, sma_period=20) == 0.5

    def test_insufficient_data_skipped(self):
        # Short data (< sma_period) should be skipped, not crash
        short = _make_price_df([100, 101, 102])
        rising = list(range(100, 130))
        universe = {
            "SHORT": short,
            "BULL": _make_price_df(rising),
        }
        breadth = compute_breadth(universe, sma_period=20)
        # Only BULL counted (1/1 = 1.0), SHORT skipped
        assert breadth == 1.0


class TestClassifyRegime:
    """Tests for classify_regime()."""

    def _make_csi300(self, trend: str = "up") -> pd.DataFrame:
        """Create CSI300 DataFrame with 60 days of data."""
        if trend == "up":
            prices = list(np.linspace(3800, 4200, 60))
        elif trend == "down":
            prices = list(np.linspace(4200, 3800, 60))
        else:  # sideways
            prices = [4000 + np.sin(i / 5) * 50 for i in range(60)]
        return _make_price_df(prices)

    def test_bull_regime(self):
        csi = self._make_csi300("up")
        rising = list(range(100, 130))
        universe = {f"T{i}": _make_price_df(rising) for i in range(20)}
        regime = classify_regime(csi, universe, {"breadth_bullish": 0.50, "breadth_bearish": 0.30})
        assert regime.label == "bull"
        assert regime.sizing_mult == 1.0
        assert regime.breadth_score == 1.0

    def test_bear_regime(self):
        csi = self._make_csi300("down")
        regime = classify_regime(csi, None, {})
        assert regime.label == "bear"
        assert regime.sizing_mult == 0.3

    def test_breadth_below_bearish_forces_bear(self):
        """Bull CSI300 but breadth < bearish threshold → forced to bear."""
        csi = self._make_csi300("up")
        falling = list(range(130, 100, -1))
        # All stocks falling → breadth = 0.0 < 0.30
        universe = {f"T{i}": _make_price_df(falling) for i in range(20)}
        regime = classify_regime(csi, universe, {"breadth_bullish": 0.50, "breadth_bearish": 0.30})
        assert regime.label == "bear"
        assert regime.sizing_mult == 0.3

    def test_breadth_between_bearish_and_bullish_downgrades_bull_to_caution(self):
        """Bull CSI300 but breadth between bearish and bullish → caution."""
        csi = self._make_csi300("up")
        rising = list(range(100, 130))
        falling = list(range(130, 100, -1))
        # 40% above SMA → between 0.30 and 0.50
        universe = {}
        for i in range(4):
            universe[f"BULL{i}"] = _make_price_df(rising)
        for i in range(6):
            universe[f"BEAR{i}"] = _make_price_df(falling)
        regime = classify_regime(csi, universe, {"breadth_bullish": 0.50, "breadth_bearish": 0.30})
        assert regime.label == "caution"
        assert regime.sizing_mult == 0.6

    def test_no_index_data_returns_caution(self):
        regime = classify_regime(pd.DataFrame(), None, {})
        assert regime.label == "caution"

    def test_breadth_score_stored(self):
        csi = self._make_csi300("up")
        rising = list(range(100, 130))
        universe = {f"T{i}": _make_price_df(rising) for i in range(5)}
        regime = classify_regime(csi, universe, {})
        assert regime.breadth_score == 1.0
        assert "breadth_pct" in regime.details
