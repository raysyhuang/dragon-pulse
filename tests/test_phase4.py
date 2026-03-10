"""Tests for Phase 4: Risk Parity sizing and Pre-market Gap Validator."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from risk.risk_parity import compute_risk_parity_sizing, SizingResult
from scripts.morning_check import check_gap, check_volume_confirmation, run_preflight


# ---------------------------------------------------------------------------
# Risk Parity Sizing
# ---------------------------------------------------------------------------

class TestRiskParitySizing:
    """Tests for volatility-adjusted position sizing."""

    def test_high_vol_gets_smaller_size(self):
        """Stock with double the target ATR should get ~half the size."""
        picks = [
            {"ticker": "A", "position_size_mult": 1.0, "atr_pct": 6.0, "sector": "", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0)
        assert len(results) == 1
        # 3.0 / 6.0 = 0.5x
        assert results[0].final_mult == pytest.approx(0.5, abs=0.01)

    def test_low_vol_gets_larger_size(self):
        """Stock with half the target ATR should get ~2x size (capped at 2.0)."""
        picks = [
            {"ticker": "B", "position_size_mult": 1.0, "atr_pct": 1.5, "sector": "", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0)
        # 3.0 / 1.5 = 2.0x (at cap)
        assert results[0].final_mult == pytest.approx(2.0, abs=0.01)

    def test_normal_vol_unchanged(self):
        """Stock at target ATR gets 1.0x."""
        picks = [
            {"ticker": "C", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0)
        assert results[0].final_mult == pytest.approx(1.0, abs=0.01)

    def test_vol_ratio_clamped_at_minimum(self):
        """Extremely volatile stock gets clamped at 0.3x, not lower."""
        picks = [
            {"ticker": "D", "position_size_mult": 1.0, "atr_pct": 20.0, "sector": "", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0)
        # 3.0 / 20.0 = 0.15 → clamped to 0.3
        assert results[0].final_mult == pytest.approx(0.3, abs=0.01)

    def test_incoming_mult_preserved(self):
        """Regime/guardian multiplier is preserved and combined with vol adjustment."""
        picks = [
            {"ticker": "E", "position_size_mult": 0.6, "atr_pct": 6.0, "sector": "", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0)
        # 0.6 * (3.0/6.0) = 0.6 * 0.5 = 0.3
        assert results[0].final_mult == pytest.approx(0.3, abs=0.01)

    def test_sector_cap_first_two_ok(self):
        """First 2 picks in same sector are not penalized."""
        picks = [
            {"ticker": "F1", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "半导体", "entry_price": 10, "stop_price": 9},
            {"ticker": "F2", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "半导体", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0, max_sector_positions=2)
        assert results[0].sector_capped is False
        assert results[1].sector_capped is False

    def test_sector_cap_third_penalized(self):
        """Third pick in same sector gets 0.5x penalty."""
        picks = [
            {"ticker": "G1", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "银行", "entry_price": 10, "stop_price": 9},
            {"ticker": "G2", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "银行", "entry_price": 10, "stop_price": 9},
            {"ticker": "G3", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "银行", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(
            picks, target_atr_pct=3.0, max_sector_positions=2, sector_excess_penalty=0.5,
        )
        assert results[0].final_mult == pytest.approx(1.0, abs=0.01)
        assert results[1].final_mult == pytest.approx(1.0, abs=0.01)
        assert results[2].sector_capped is True
        assert results[2].final_mult == pytest.approx(0.5, abs=0.01)

    def test_different_sectors_no_penalty(self):
        """Picks in different sectors are all fine."""
        picks = [
            {"ticker": "H1", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "银行", "entry_price": 10, "stop_price": 9},
            {"ticker": "H2", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "半导体", "entry_price": 10, "stop_price": 9},
            {"ticker": "H3", "position_size_mult": 1.0, "atr_pct": 3.0, "sector": "军工", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0, max_sector_positions=2)
        assert all(not r.sector_capped for r in results)

    def test_zero_atr_uses_default(self):
        """Zero ATR should default to 1.0x multiplier."""
        picks = [
            {"ticker": "I", "position_size_mult": 1.0, "atr_pct": 0, "sector": "", "entry_price": 10, "stop_price": 9},
        ]
        results = compute_risk_parity_sizing(picks, target_atr_pct=3.0)
        assert results[0].final_mult == pytest.approx(1.0, abs=0.01)

    def test_empty_picks(self):
        """Empty list returns empty list."""
        assert compute_risk_parity_sizing([]) == []


# ---------------------------------------------------------------------------
# Pre-Market Gap Validator
# ---------------------------------------------------------------------------

class TestGapCheck:
    """Tests for the gap validation logic."""

    def test_small_gap_up_is_go(self):
        action, reasons = check_gap(10.0, 10.10)  # +1% (below 3%*0.6=1.8% warn threshold)
        assert action == "GO"
        assert reasons == []

    def test_large_gap_up_cancels(self):
        action, reasons = check_gap(10.0, 10.40)  # +4%
        assert action == "CANCEL"
        assert len(reasons) == 1
        assert "Gap up" in reasons[0]

    def test_moderate_gap_up_warns(self):
        action, reasons = check_gap(10.0, 10.20, max_gap_up_pct=3.0)  # +2% > 3%*0.6=1.8%
        assert action == "WARN"

    def test_gap_down_cancels(self):
        action, reasons = check_gap(10.0, 9.40)  # -6%
        assert action == "CANCEL"
        assert "Gap down" in reasons[0]

    def test_flat_open_is_go(self):
        action, _ = check_gap(10.0, 10.0)
        assert action == "GO"

    def test_custom_thresholds(self):
        # +2% gap up with 5% threshold → GO (below 5%*0.6=3% warn threshold)
        action, _ = check_gap(10.0, 10.20, max_gap_up_pct=5.0)
        assert action == "GO"


class TestVolumeConfirmation:
    """Tests for opening volume check."""

    def test_strong_volume_is_go(self):
        action, _ = check_volume_confirmation(1_000_000, 150_000)  # 15% > 10%
        assert action == "GO"

    def test_weak_volume_warns(self):
        action, reasons = check_volume_confirmation(1_000_000, 50_000)  # 5% < 10%
        assert action == "WARN"
        assert "Low opening volume" in reasons[0]

    def test_zero_prev_volume_is_go(self):
        action, _ = check_volume_confirmation(0, 100_000)
        assert action == "GO"


class TestPreFlight:
    """Integration tests for the full pre-flight check."""

    def test_all_clear(self):
        picks = [{"ticker": "600000.SH", "name_cn": "浦发银行", "entry_price": 10.0}]
        results = run_preflight(
            picks,
            open_prices={"600000.SH": 10.10},  # +1% gap
            prev_volumes={"600000.SH": 1_000_000},
            first_15m_volumes={"600000.SH": 200_000},  # 20% > 10%
        )
        assert len(results) == 1
        assert results[0].action == "GO"

    def test_gap_up_cancels(self):
        picks = [{"ticker": "600000.SH", "name_cn": "浦发银行", "entry_price": 10.0}]
        results = run_preflight(
            picks,
            open_prices={"600000.SH": 10.50},  # +5% gap
            prev_volumes={},
        )
        assert results[0].action == "CANCEL"

    def test_weak_volume_warns(self):
        picks = [{"ticker": "600000.SH", "name_cn": "浦发银行", "entry_price": 10.0}]
        results = run_preflight(
            picks,
            open_prices={"600000.SH": 10.05},  # +0.5% gap (OK)
            prev_volumes={"600000.SH": 1_000_000},
            first_15m_volumes={"600000.SH": 30_000},  # 3% < 10%
        )
        assert results[0].action == "WARN"

    def test_missing_open_price_warns(self):
        picks = [{"ticker": "600000.SH", "name_cn": "浦发银行", "entry_price": 10.0}]
        results = run_preflight(picks, open_prices={}, prev_volumes={})
        assert results[0].action == "WARN"
        assert "No opening price" in results[0].reasons[0]

    def test_multiple_picks(self):
        picks = [
            {"ticker": "A", "name_cn": "测试A", "entry_price": 10.0},
            {"ticker": "B", "name_cn": "测试B", "entry_price": 20.0},
        ]
        results = run_preflight(
            picks,
            open_prices={"A": 10.10, "B": 21.00},  # A: +1% (GO), B: +5% (CANCEL)
            prev_volumes={},
        )
        assert results[0].action == "GO"
        assert results[1].action == "CANCEL"
