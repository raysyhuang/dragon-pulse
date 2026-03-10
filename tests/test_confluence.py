"""Tests for confluence scoring and Dragon Tiger gating."""

import sys
from pathlib import Path

# strategy modules use bare imports (from core.xxx), so add src/ to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest
from strategy.base import StrategySignal
from strategy.confluence import ConfluenceConfig, run_confluence, _has_dtl_confirmation


def _make_signal(
    ticker: str, lens: str, score: float, triggered: bool = True, dtl_net_buy: float = 0,
) -> StrategySignal:
    return StrategySignal(
        ticker=ticker,
        name_cn=f"测试_{ticker}",
        lens=lens,
        score=score,
        triggered=triggered,
        evidence={"dtl_net_buy": dtl_net_buy},
        entry_price=10.0,
        target_price=10.5,
        stop_price=9.5,
        max_hold_days=5,
    )


class TestDtlConfirmation:
    def test_no_dtl(self):
        signals = [_make_signal("A", "lens_b", 60, dtl_net_buy=0)]
        assert _has_dtl_confirmation(signals) is False

    def test_above_threshold(self):
        signals = [_make_signal("A", "lens_b", 60, dtl_net_buy=15_000_000)]
        assert _has_dtl_confirmation(signals) is True

    def test_below_threshold(self):
        signals = [_make_signal("A", "lens_b", 60, dtl_net_buy=5_000_000)]
        assert _has_dtl_confirmation(signals) is False


class TestConfluence:
    def _default_config(self, **overrides) -> ConfluenceConfig:
        defaults = dict(
            threshold_a=50.0, threshold_b=50.0, high_threshold=70.0,
            w_lens_a=0.40, w_lens_b=0.35, w_lens_c=0.25,
            max_daily_picks=5, min_composite_score=30.0,
            require_dtl_for_breakout=True,
        )
        defaults.update(overrides)
        return ConfluenceConfig(**defaults)

    def test_double_confluence_no_dtl_required(self):
        """Double (A+B) should NOT require DTL — it's already a strong pattern."""
        cfg = self._default_config()
        sig_a = [_make_signal("X", "lens_a", 60, dtl_net_buy=0)]
        sig_b = [_make_signal("X", "lens_b", 60, dtl_net_buy=0)]
        picks = run_confluence(sig_a, sig_b, [], "bull", 1.0, cfg)
        assert len(picks) == 1
        assert picks[0].confluence_type == "double"

    def test_breakout_seal_without_dtl_rejected(self):
        """Breakout+Seal with breakout dominant and no DTL → rejected."""
        cfg = self._default_config()
        # score_b (70) > score_a (0) → breakout dominant
        sig_b = [_make_signal("X", "lens_b", 70, dtl_net_buy=0)]
        sig_c = [_make_signal("X", "lens_c", 50)]
        picks = run_confluence([], sig_b, sig_c, "bull", 1.0, cfg)
        assert len(picks) == 0

    def test_breakout_seal_with_dtl_accepted(self):
        """Breakout+Seal with DTL confirmation → accepted."""
        cfg = self._default_config()
        sig_b = [_make_signal("X", "lens_b", 70, dtl_net_buy=20_000_000)]
        sig_c = [_make_signal("X", "lens_c", 50)]
        picks = run_confluence([], sig_b, sig_c, "bull", 1.0, cfg)
        assert len(picks) == 1

    def test_dtl_gate_disabled(self):
        """When require_dtl_for_breakout=False, breakout without DTL passes."""
        cfg = self._default_config(require_dtl_for_breakout=False)
        sig_b = [_make_signal("X", "lens_b", 70, dtl_net_buy=0)]
        sig_c = [_make_signal("X", "lens_c", 50)]
        picks = run_confluence([], sig_b, sig_c, "bull", 1.0, cfg)
        assert len(picks) == 1

    def test_single_institution_requires_dtl(self):
        """Single institution type always requires DTL (existing behavior)."""
        cfg = self._default_config()
        sig_b = [_make_signal("X", "lens_b", 75, dtl_net_buy=0)]
        picks = run_confluence([], sig_b, [], "bull", 1.0, cfg)
        # No DTL → can't be "single_institution", and no other confluence type matches
        assert len(picks) == 0

    def test_single_institution_with_dtl(self):
        # Single lens at 90 → composite = 0.35*90 = 31.5, above min 30.0
        cfg = self._default_config()
        sig_b = [_make_signal("X", "lens_b", 90, dtl_net_buy=20_000_000)]
        picks = run_confluence([], sig_b, [], "bull", 1.0, cfg)
        assert len(picks) == 1
        assert picks[0].confluence_type == "single_institution"

    def test_max_daily_picks_enforced(self):
        cfg = self._default_config(max_daily_picks=1)
        sig_a = [
            _make_signal("X", "lens_a", 60),
            _make_signal("Y", "lens_a", 55),
        ]
        sig_b = [
            _make_signal("X", "lens_b", 60),
            _make_signal("Y", "lens_b", 55),
        ]
        picks = run_confluence(sig_a, sig_b, [], "bull", 1.0, cfg)
        assert len(picks) == 1
        assert picks[0].ticker == "X"  # Higher score

    def test_min_composite_score_enforced(self):
        cfg = self._default_config(min_composite_score=80.0)
        sig_a = [_make_signal("X", "lens_a", 60)]
        sig_b = [_make_signal("X", "lens_b", 60)]
        picks = run_confluence(sig_a, sig_b, [], "bull", 1.0, cfg)
        # Composite = 0.40*60 + 0.35*60 + 0.25*0 = 45.0 < 80.0
        assert len(picks) == 0

    def test_regime_sizing_applied(self):
        cfg = self._default_config()
        sig_a = [_make_signal("X", "lens_a", 60)]
        sig_b = [_make_signal("X", "lens_b", 60)]
        picks = run_confluence(sig_a, sig_b, [], "caution", 0.6, cfg)
        assert len(picks) == 1
        assert picks[0].position_size_mult == pytest.approx(0.6, abs=0.01)
