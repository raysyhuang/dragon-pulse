"""Tests for the shared selection funnel (src/pipelines/funnel.py)."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import patch

import pandas as pd
import pytest

from src.pipelines.funnel import (
    StageResult,
    classify_regime,
    build_regime_detail,
    compute_breadth,
    run_selection_funnel,
    _limit_down_veto,
    _sector_cap,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@dataclass
class FakeSignal:
    ticker: str
    score: float
    entry_price: float = 100.0
    stop_loss: float = 95.0
    target_1: float = 110.0
    target_2: float = 120.0
    holding_period: int = 3
    components: dict = None
    max_entry_price: float | None = None

    def __post_init__(self):
        if self.components is None:
            self.components = {"rsi2_oversold": 80, "trend_intact": 60}


def _make_csi(prices: list[float]) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=len(prices), freq="B")
    return pd.DataFrame({"close": prices}, index=idx)


def _make_candidates(*ticker_scores) -> list[tuple[str, FakeSignal]]:
    return [("mean_reversion", FakeSignal(ticker=t, score=s)) for t, s in ticker_scores]


# ---------------------------------------------------------------------------
# Regime tests
# ---------------------------------------------------------------------------

class TestClassifyRegime:
    def test_bull(self):
        # Rising prices — close > sma_short > sma_long
        prices = list(range(100, 160))
        assert classify_regime(_make_csi(prices), 5, 10) == "bull"

    def test_bear(self):
        # Falling prices — close < sma_long
        prices = list(range(160, 100, -1))
        assert classify_regime(_make_csi(prices), 5, 10) == "bear"

    def test_choppy_empty(self):
        assert classify_regime(pd.DataFrame(), 20, 50) == "choppy"

    def test_build_regime_detail_returns_tuple(self):
        prices = list(range(100, 160))
        regime, detail = build_regime_detail(_make_csi(prices), 5, 10)
        assert regime == "bull"
        assert "csi300_last" in detail
        assert detail["csi300_last"] == 159


# ---------------------------------------------------------------------------
# Breadth tests
# ---------------------------------------------------------------------------

class TestComputeBreadth:
    def test_all_above(self):
        # 30 bars all going up — close > sma20
        idx = pd.date_range("2025-01-01", periods=30, freq="B")
        df = pd.DataFrame({"close": list(range(100, 130))}, index=idx)
        assert compute_breadth({"A": df}) == 1.0

    def test_empty_data(self):
        assert compute_breadth({}) == 0.0

    def test_precomputed(self):
        from datetime import date
        precomputed = {
            "A": {date(2025, 1, 1): True},
            "B": {date(2025, 1, 1): False},
            "C": {date(2025, 1, 1): True},
        }
        assert compute_breadth({}, scan_date=date(2025, 1, 1), precomputed_breadth=precomputed) == pytest.approx(2/3)


# ---------------------------------------------------------------------------
# Limit-down veto tests
# ---------------------------------------------------------------------------

class TestLimitDownVeto:
    def test_veto_limit_down(self):
        """Ticker that closed at exactly limit-down should be vetoed."""
        candidates = _make_candidates(("600001.SH", 90))
        # Simulate close at -10% from prev
        idx = pd.date_range("2025-01-01", periods=5, freq="B")
        df = pd.DataFrame({"close": [100, 100, 100, 100, 90]}, index=idx)
        result = _limit_down_veto(candidates, data_map={"600001.SH": df}, info_map={})
        assert len(result) == 0

    def test_keep_normal(self):
        """Normal close should pass through."""
        candidates = _make_candidates(("600001.SH", 90))
        idx = pd.date_range("2025-01-01", periods=5, freq="B")
        df = pd.DataFrame({"close": [100, 100, 100, 100, 98]}, index=idx)
        result = _limit_down_veto(candidates, data_map={"600001.SH": df}, info_map={})
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Sector cap tests
# ---------------------------------------------------------------------------

class TestSectorCap:
    def test_caps_at_one(self):
        candidates = _make_candidates(("A", 90), ("B", 85), ("C", 80))
        info = {
            "A": {"industry": "tech"},
            "B": {"industry": "tech"},
            "C": {"industry": "finance"},
        }
        result = _sector_cap(candidates, info, max_per_sector=1)
        tickers = [sig.ticker for _, sig in result]
        assert tickers == ["A", "C"]  # B dropped (second tech)

    def test_caps_at_two(self):
        candidates = _make_candidates(("A", 90), ("B", 85), ("C", 80))
        info = {
            "A": {"industry": "tech"},
            "B": {"industry": "tech"},
            "C": {"industry": "tech"},
        }
        result = _sector_cap(candidates, info, max_per_sector=2)
        assert len(result) == 2  # A, B kept; C dropped


# ---------------------------------------------------------------------------
# Full funnel tests
# ---------------------------------------------------------------------------

class TestRunSelectionFunnel:
    def _default_config(self):
        return {
            "book_size": {
                "breadth_floor": 0.30,
                "max_per_sector": 1,
                "bull": {"max_picks": 5, "min_score": 65},
                "choppy": {"max_picks": 3, "min_score": 75},
                "bear": {"max_picks": 1, "min_score": 85},
            },
            "acceptance": {
                "enabled": True,
                "dq_full_threshold": 55,
                "dq_selective_threshold": 35,
                "max_full": 5,
                "max_selective": 2,
            },
        }

    def test_off_mode_truncates(self):
        candidates = _make_candidates(("A", 90), ("B", 85), ("C", 80), ("D", 75), ("E", 70), ("F", 65))
        result = run_selection_funnel(
            candidates, "bull", 0.5, self._default_config(),
            universe_size=100, acceptance_mode="off",
        )
        assert len(result.final_picks) == 5  # max_picks for bull
        assert result.acceptance_mode == "off"

    def test_breadth_suppression(self):
        candidates = _make_candidates(("A", 90))
        result = run_selection_funnel(
            candidates, "bull", 0.20, self._default_config(),
            universe_size=100, acceptance_mode="live_equivalent",
        )
        assert len(result.final_picks) == 0
        assert result.breadth_suppressed is True
        assert result.acceptance_mode == "breadth_suppressed"

    def test_score_floor_filters(self):
        # Bear min_score=85, only A qualifies
        candidates = _make_candidates(("A", 90), ("B", 80))
        result = run_selection_funnel(
            candidates, "bear", 0.5, self._default_config(),
            universe_size=100, acceptance_mode="live_equivalent",
        )
        assert result.score_floor_count == 1  # only A passed score floor

    def test_live_equivalent_returns_stage_result(self):
        candidates = _make_candidates(("A", 90), ("B", 85))
        info = {"A": {"industry": "tech"}, "B": {"industry": "finance"}}
        result = run_selection_funnel(
            candidates, "bull", 0.6, self._default_config(),
            universe_size=100, info_map=info,
            acceptance_mode="live_equivalent",
        )
        assert isinstance(result, StageResult)
        assert result.acceptance_mode in ("full", "selective", "abstain")
        assert result.day_quality_score >= 0
