from __future__ import annotations

import pandas as pd

from scripts.backtest_1yr import resolve_mr_subtype_and_exit_params
from src.signals.mean_reversion import (
    classify_mean_reversion_subtype,
    score_mean_reversion,
)


def _build_df() -> pd.DataFrame:
    closes = [95.0] * 55 + [100.0, 100.0, 100.0, 100.0, 100.0]
    volumes = [2_000_000] * len(closes)
    return pd.DataFrame(
        {
            "close": closes,
            "volume": volumes,
            "gap_pct": [0.0] * len(closes),
        }
    )


def _build_features() -> dict:
    return {
        "rsi_2": 3.0,
        "pct_above_sma200": 5.0,
        "pct_above_sma50": 3.0,
        "sma_50": 102.0,
        "sma_200": 98.0,
        "streak": -3,
        "dist_from_5d_low": 0.5,
        "rvol": 0.8,
        "close": 100.0,
        "atr_14": 4.0,
    }


def test_mean_reversion_respects_configurable_exit_and_entry_params():
    signal = score_mean_reversion(
        ticker="300001.SZ",
        df=_build_df(),
        features=_build_features(),
        stop_atr_mult=1.0,
        target_1_atr_mult=2.0,
        target_2_atr_mult=3.5,
        max_entry_atr_mult=0.1,
        holding_period=5,
    )

    assert signal is not None
    assert signal.stop_loss == 96.0
    assert signal.target_1 == 108.0
    assert signal.max_entry_price == 100.4
    assert signal.holding_period == 5


def test_mean_reversion_atr_mode_uses_atr_targets():
    signal = score_mean_reversion(
        ticker="300001.SZ",
        df=_build_df(),
        features=_build_features(),
        target_mode="atr",
        target_1_atr_mult=2.0,
        target_2_atr_mult=3.5,
    )

    assert signal is not None
    assert signal.target_1 == 108.0
    assert signal.target_2 == 114.0


def test_mean_reversion_subtype_classifier_marks_extreme_snapbacks_as_bounce():
    assert classify_mean_reversion_subtype(_build_features()) == "bounce"


def test_mean_reversion_subtype_classifier_marks_milder_pullbacks_as_drift():
    features = {
        **_build_features(),
        "rsi_2": 4.5,
        "streak": -2,
        "dist_from_5d_low": 1.2,
    }

    assert classify_mean_reversion_subtype(features) == "drift"


def test_subtype_split_routes_drift_to_wider_exit_profile():
    mr_config = {
        "stop_atr_mult": 0.95,
        "target_1_atr_mult": 1.5,
        "target_2_atr_mult": 2.0,
        "max_entry_atr_mult": 0.2,
        "holding_period": 3,
        "subtype_split": {
            "enabled": True,
            "rsi2_bounce_max": 3.0,
            "streak_bounce_max": -3,
            "dist_from_5d_low_bounce_max": 0.75,
            "drift": {
                "stop_atr_mult": 1.0,
                "target_1_atr_mult": 2.0,
                "target_2_atr_mult": 3.0,
                "holding_period": 4,
            },
        },
    }
    features = {
        **_build_features(),
        "rsi_2": 4.5,
        "streak": -2,
        "dist_from_5d_low": 1.2,
    }

    subtype, params = resolve_mr_subtype_and_exit_params(mr_config, features)

    assert subtype == "drift"
    assert params["stop_atr_mult"] == 1.0
    assert params["target_1_atr_mult"] == 2.0
    assert params["target_2_atr_mult"] == 3.0
    assert params["holding_period"] == 4


def test_subtype_split_disabled_keeps_default_exit_profile():
    mr_config = {
        "stop_atr_mult": 0.95,
        "target_1_atr_mult": 1.5,
        "target_2_atr_mult": 2.0,
        "max_entry_atr_mult": 0.2,
        "holding_period": 3,
        "subtype_split": {
            "enabled": False,
            "drift": {
                "stop_atr_mult": 1.0,
                "target_1_atr_mult": 2.0,
                "target_2_atr_mult": 3.0,
                "holding_period": 4,
            },
        },
    }

    subtype, params = resolve_mr_subtype_and_exit_params(mr_config, _build_features())

    assert subtype == "default"
    assert params["stop_atr_mult"] == 0.95
    assert params["target_1_atr_mult"] == 1.5
    assert params["target_2_atr_mult"] == 2.0
    assert params["holding_period"] == 3
