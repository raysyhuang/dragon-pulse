"""Tests for Phase 3 components: time stop, sector boost, LLM accuracy."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest
import pandas as pd
from risk.position_manager import Position, check_exits
from strategy.confluence import (
    run_confluence, ConfluenceConfig, SECTOR_BOOST, SECTOR_PENALTY,
)
from strategy.base import StrategySignal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_position(**overrides) -> Position:
    defaults = dict(
        id=1, ticker="600000.SH", name_cn="浦发银行",
        entry_date="2025-01-02", entry_price=10.0,
        target_price=11.0, stop_price=9.0, current_stop=9.0,
        max_hold_days=7, position_size_mult=1.0,
        lens="lens_a", confluence_type="double", status="open",
        days_held=0,
    )
    defaults.update(overrides)
    return Position(**defaults)


def _make_ohlcv(open_: float, high: float, low: float, close: float) -> pd.Series:
    return pd.Series({"Open": open_, "High": high, "Low": low, "Close": close})


def _signal(ticker: str, lens: str, score: float, triggered: bool = True, **evidence) -> StrategySignal:
    return StrategySignal(
        ticker=ticker, name_cn="测试", lens=lens,
        score=score, triggered=triggered,
        entry_price=10.0, target_price=11.0, stop_price=9.0,
        evidence=evidence,
    )


# ---------------------------------------------------------------------------
# Dead Money Time Stop
# ---------------------------------------------------------------------------

class TestDeadMoney:
    """Tests for the 3-day dead money exit rule."""

    def test_exit_after_3_days_no_move(self):
        """Position with <2% gain after 3 days → dead_money exit."""
        pos = _make_position(days_held=2)  # will become 3 after increment
        ohlcv = _make_ohlcv(10.0, 10.10, 9.90, 10.05)  # +1% high, < 2%
        result = check_exits(pos, ohlcv, "2025-01-07", {"dead_money_days": 3})
        assert result.status == "closed"
        assert result.exit_reason == "dead_money"

    def test_no_exit_if_moved_enough(self):
        """Position with >=2% gain after 3 days → stays open."""
        pos = _make_position(days_held=2)
        ohlcv = _make_ohlcv(10.0, 10.25, 9.90, 10.20)  # +2.5% high
        result = check_exits(pos, ohlcv, "2025-01-07", {"dead_money_days": 3})
        assert result.status == "open"

    def test_no_exit_before_3_days(self):
        """Position held only 1 day → not yet checked for dead money."""
        pos = _make_position(days_held=0)  # becomes 1 after increment
        ohlcv = _make_ohlcv(10.0, 10.05, 9.95, 10.0)  # flat
        result = check_exits(pos, ohlcv, "2025-01-03", {"dead_money_days": 3})
        assert result.status == "open"

    def test_stop_hit_takes_priority_over_dead_money(self):
        """Stop loss should trigger before dead money check."""
        pos = _make_position(days_held=2, current_stop=9.0)
        ohlcv = _make_ohlcv(8.80, 8.85, 8.70, 8.80)  # gap below stop
        result = check_exits(pos, ohlcv, "2025-01-07", {"dead_money_days": 3})
        assert result.status == "closed"
        assert result.exit_reason == "stop_hit"

    def test_custom_dead_money_threshold(self):
        """Custom dead_money_min_pct=5% — small move doesn't trigger exit."""
        pos = _make_position(days_held=2)
        ohlcv = _make_ohlcv(10.0, 10.30, 9.90, 10.25)  # +3% high, < 5%
        result = check_exits(pos, ohlcv, "2025-01-07", {
            "dead_money_days": 3,
            "dead_money_min_pct": 5.0,
        })
        assert result.status == "closed"
        assert result.exit_reason == "dead_money"


# ---------------------------------------------------------------------------
# Sector Boost in Confluence
# ---------------------------------------------------------------------------

class TestSectorBoost:
    """Tests for sector momentum boost/penalty in confluence."""

    def _config(self, **kw) -> ConfluenceConfig:
        defaults = dict(
            threshold_a=40, threshold_b=40, high_threshold=70,
            w_lens_a=0.40, w_lens_b=0.35, w_lens_c=0.25,
            max_daily_picks=5, min_composite_score=30.0,
            require_dtl_for_breakout=False,
        )
        defaults.update(kw)
        return ConfluenceConfig(**defaults)

    def test_hot_sector_boosts_score(self):
        """Stock in a hot sector gets +1.0 composite boost."""
        sig_a = _signal("600000.SH", "lens_a", 80)
        sig_b = _signal("600000.SH", "lens_b", 80)
        picks = run_confluence(
            [sig_a], [sig_b], [], "bull", 1.0, self._config(),
            hot_sectors=["银行"], cold_sectors=[],
            ticker_sector_map={"600000.SH": "银行"},
        )
        assert len(picks) == 1
        # Without boost: 0.40*80 + 0.35*80 = 60. With boost: 61.
        assert picks[0].composite_score == pytest.approx(60.0 + SECTOR_BOOST, abs=0.01)

    def test_cold_sector_penalizes_score(self):
        """Stock in a cold sector gets -1.0 composite penalty."""
        sig_a = _signal("600000.SH", "lens_a", 80)
        sig_b = _signal("600000.SH", "lens_b", 80)
        picks = run_confluence(
            [sig_a], [sig_b], [], "bull", 1.0, self._config(),
            hot_sectors=[], cold_sectors=["银行"],
            ticker_sector_map={"600000.SH": "银行"},
        )
        assert len(picks) == 1
        assert picks[0].composite_score == pytest.approx(60.0 + SECTOR_PENALTY, abs=0.01)

    def test_no_sector_map_no_boost(self):
        """Without ticker_sector_map, no boost or penalty applied."""
        sig_a = _signal("600000.SH", "lens_a", 80)
        sig_b = _signal("600000.SH", "lens_b", 80)
        picks = run_confluence(
            [sig_a], [sig_b], [], "bull", 1.0, self._config(),
            hot_sectors=["银行"],
        )
        assert len(picks) == 1
        assert picks[0].composite_score == pytest.approx(60.0, abs=0.01)

    def test_sector_stored_on_pick(self):
        """PickCandidate.sector should be populated from ticker_sector_map."""
        sig_a = _signal("600000.SH", "lens_a", 80)
        sig_b = _signal("600000.SH", "lens_b", 80)
        picks = run_confluence(
            [sig_a], [sig_b], [], "bull", 1.0, self._config(),
            ticker_sector_map={"600000.SH": "银行"},
        )
        assert picks[0].sector == "银行"


# ---------------------------------------------------------------------------
# LLM Accuracy Tracker
# ---------------------------------------------------------------------------

class TestLlmAccuracy:
    """Tests for LLM accuracy tracker (unit tests, no filesystem)."""

    def test_compute_penalties_all_good(self):
        from tracking.llm_accuracy import TierAccuracy, AccuracyReport, get_confidence_penalties

        report = AccuracyReport(
            tiers=[
                TierAccuracy("HIGH", 10, 7, 0.70, 0.30),
                TierAccuracy("MEDIUM", 20, 12, 0.60, 0.40),
            ],
            total_picks=30, total_hits=19, overall_hit_rate=0.63,
        )
        penalties = get_confidence_penalties(report, max_fp_rate=0.40)
        assert penalties["HIGH"] == 1.0
        assert penalties["MEDIUM"] == 1.0  # exactly at threshold → OK

    def test_compute_penalties_over_threshold(self):
        from tracking.llm_accuracy import TierAccuracy, AccuracyReport, get_confidence_penalties

        report = AccuracyReport(
            tiers=[
                TierAccuracy("HIGH", 10, 5, 0.50, 0.50),     # 10% over
                TierAccuracy("SPECULATIVE", 10, 2, 0.20, 0.80),  # 40% over
            ],
            total_picks=20, total_hits=7, overall_hit_rate=0.35,
        )
        penalties = get_confidence_penalties(report, max_fp_rate=0.40, penalty_step=0.15)
        assert penalties["HIGH"] == 0.85       # 1 bucket over
        assert penalties["SPECULATIVE"] == 0.5  # 4 buckets, clamped at 0.5

    def test_empty_report_returns_empty(self):
        from tracking.llm_accuracy import AccuracyReport, get_confidence_penalties

        report = AccuracyReport(tiers=[], total_picks=0, total_hits=0, overall_hit_rate=0)
        penalties = get_confidence_penalties(report)
        assert penalties == {}
