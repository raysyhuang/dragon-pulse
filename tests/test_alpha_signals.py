import pandas as pd

from src.pipelines.funnel import build_engine_candidates
from src.signals.alpha_candidates import score_rs_pullback_alpha, score_sniper_breakout_alpha


def _trend_df(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = [10 + i * 0.055 for i in range(n)]
    # Put the last bar into a controlled pullback from the recent high.
    close[-2] = 19.05
    close[-1] = 18.85
    high = [c * 1.015 for c in close]
    low = [c * 0.985 for c in close]
    volume = [6_000_000] * n
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": volume}, index=idx)


def _breakout_df(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = [10 + i * 0.04 for i in range(n)]
    close[-2] = 16.40
    close[-1] = 16.62
    high = [c * 1.01 for c in close]
    low = [c * 0.98 for c in close]
    # make prior high close enough for a near-breakout without a blowoff day
    high[-21:-1] = [16.55] * 20
    volume = [5_000_000] * (n - 6) + [4_000_000, 3_900_000, 3_800_000, 3_700_000, 3_600_000, 8_000_000]
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": volume}, index=idx)


def _flat_csi(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.DataFrame({"close": [100.0] * n}, index=idx)


def test_rs_pullback_alpha_emits_research_signal_with_no_chase_ceiling():
    signal = score_rs_pullback_alpha(
        ticker="600118.SH",
        df=_trend_df(),
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=70,
        max_entry_pct=0.02,
    )

    assert signal is not None
    assert signal.subtype == "rs_pullback_alpha"
    assert signal.max_entry_price == round(signal.entry_price * 1.02, 2)
    assert signal.holding_period == 5
    assert signal.target_1 > signal.entry_price > signal.stop_loss


def test_sniper_breakout_alpha_emits_research_signal_with_mas_style_ceiling():
    signal = score_sniper_breakout_alpha(
        ticker="601698.SH",
        df=_breakout_df(),
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        max_entry_pct=0.03,
        min_adv_cny=70_000_000,
    )

    assert signal is not None
    assert signal.subtype == "sniper_breakout_alpha"
    assert signal.max_entry_price == round(signal.entry_price * 1.03, 2)
    assert signal.target_1 > signal.entry_price > signal.stop_loss


def test_build_engine_candidates_keeps_alpha_disabled_by_default():
    feat_items = [("600118.SH", _trend_df(), {"rsi_2": 50})]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={"mean_reversion": {"rsi2_max": 5, "score_floor": 65}},
        csi300_df=_flat_csi(),
    )

    assert [engine for engine, _ in candidates] == []


def test_build_engine_candidates_adds_alpha_when_research_flag_enabled():
    feat_items = [("600118.SH", _trend_df(), {"rsi_2": 50})]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={
            "mean_reversion": {"rsi2_max": 5, "score_floor": 65},
            "alpha_candidates": {
                "enabled": True,
                "engines": ["rs_pullback"],
                "rs_pullback": {"score_floor": 70, "max_entry_pct": 0.02},
            },
        },
        csi300_df=_flat_csi(),
    )

    assert [(engine, sig.subtype) for engine, sig in candidates] == [
        ("alpha_rs_pullback", "rs_pullback_alpha")
    ]
