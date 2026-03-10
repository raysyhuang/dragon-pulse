"""
Smoke tests for core functionality.

These tests verify that critical functions work without crashing.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest
from src.core.technicals import compute_technicals, atr, rsi, sma
from src.core.universe import build_universe
from src.core.config import load_config
from src.core.logging_utils import setup_logging, get_logger


def test_compute_technicals_shape():
    """Test that compute_technicals returns expected columns."""
    # Create sample data
    dates = pd.date_range("2024-01-01", periods=100, freq="D")
    df = pd.DataFrame({
        "Date": dates,
        "Open": [100 + i * 0.1 for i in range(100)],
        "High": [101 + i * 0.1 for i in range(100)],
        "Low": [99 + i * 0.1 for i in range(100)],
        "Close": [100.5 + i * 0.1 for i in range(100)],
        "Volume": [1000000] * 100,
    })
    
    result = compute_technicals(df)
    
    # Check that result has expected columns
    assert "atr" in result.columns
    assert "rsi" in result.columns
    assert "ma20" in result.columns
    assert "ma50" in result.columns
    assert len(result) == len(df)


def test_technical_indicators():
    """Test that technical indicators compute without errors."""
    dates = pd.date_range("2024-01-01", periods=50, freq="D")
    df = pd.DataFrame({
        "Date": dates,
        "Open": [100] * 50,
        "High": [101] * 50,
        "Low": [99] * 50,
        "Close": [100] * 50,
        "Volume": [1000000] * 50,
    })
    
    # Test ATR
    atr_series = atr(df, period=14)
    assert len(atr_series) == len(df)
    assert not atr_series.isna().all()
    
    # Test RSI
    rsi_series = rsi(df["Close"], period=14)
    assert len(rsi_series) == len(df)
    
    # Test SMA
    sma_series = sma(df["Close"], n=20)
    assert len(sma_series) == len(df)


def test_build_universe():
    """Test that universe building works."""
    # Test with minimal mode
    tickers = build_universe(mode="SP500", cache_file=None)
    assert isinstance(tickers, list)
    assert len(tickers) > 0
    assert all(isinstance(t, str) for t in tickers)


def test_build_universe_china_a_uses_cn_source(monkeypatch):
    """CHINA_A mode should not silently fall back to US universes."""
    monkeypatch.setattr("src.core.universe.load_universe_from_cache", lambda *args, **kwargs: [])
    monkeypatch.setattr("src.core.universe.save_universe_to_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr("src.core.universe.get_china_a_universe", lambda **kwargs: ["600519.SH", "000001.SZ"], raising=False)
    monkeypatch.setattr("src.core.universe.get_sp500_universe", lambda: pytest.fail("US universe should not be used for CHINA_A"))
    monkeypatch.setattr("src.core.universe.get_nasdaq100_universe", lambda: pytest.fail("US universe should not be used for CHINA_A"))
    monkeypatch.setattr("src.core.universe.get_russell2000_universe", lambda: pytest.fail("US universe should not be used for CHINA_A"))

    tickers = build_universe(mode="CHINA_A", cache_file=None, quarantine_enabled=False)

    assert sorted(tickers) == ["000001.SZ", "600519.SH"]


def test_load_config():
    """Test that config loading works."""
    config = load_config("config/default.yaml")
    assert isinstance(config, dict)
    assert "universe" in config
    assert "movers" in config


def test_logging_setup():
    """Test that logging setup works."""
    logger = setup_logging()
    assert logger is not None
    
    test_logger = get_logger("test")
    assert test_logger is not None
    test_logger.info("Test log message")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
