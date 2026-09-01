"""Regression coverage for evaluate_pick exit semantics."""

from __future__ import annotations

import pandas as pd
import pytest

from scripts.backtest_1yr import _daily_outcome_bucket, _is_stop_exit, evaluate_pick


def _bars(rows: list[dict[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, index=pd.date_range("2026-01-05", periods=len(rows), freq="B"))


def test_same_bar_stop_first_on_exit_eligible_bar():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},  # signal
                {"open": 10.0, "high": 10.4, "low": 9.6, "close": 10.1},  # entry
                {"open": 10.0, "high": 11.2, "low": 8.8, "close": 10.0},  # both
            ]
        ),
    )

    assert result["exit_reason"] == "same_bar_stop_first"
    assert result["exit_price"] == 9.0
    assert result["exit_day"] == 2
    assert result["hit_stop"] is True
    assert result["same_bar_both_touched"] is True
    assert result["pnl_pct"] < 0


def test_entry_bar_target_touch_is_locked_until_day_two():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 10.0, "high": 11.2, "low": 9.6, "close": 10.1},
                {"open": 10.0, "high": 11.1, "low": 9.7, "close": 10.2},
            ]
        ),
    )

    assert result["exit_reason"] == "target_hit"
    assert result["exit_day"] == 2


def test_entry_bar_gap_below_stop_waits_for_t1_legal_exit_open():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 8.5, "high": 11.5, "low": 8.0, "close": 10.0},
                {"open": 8.2, "high": 8.5, "low": 8.0, "close": 8.3},
            ]
        ),
    )

    assert result["exit_reason"] == "entry_gap_below_stop_t1_exit"
    assert result["exit_price"] == 8.2
    assert result["exit_day"] == 2
    assert result["hit_stop"] is True
    assert result["pnl_pct"] < 0


def test_runner_entry_bar_stop_touch_is_locked_until_next_bar():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        runner_max_hold=3,
        exit_mode="runner",
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 10.0, "high": 10.2, "low": 8.8, "close": 9.4},
                {"open": 10.0, "high": 10.2, "low": 8.8, "close": 9.4},
            ]
        ),
    )

    assert result["exit_reason"] == "stop_hit"
    assert result["exit_day"] == 2


def test_minimum_horizon_contract_rejects_one_session_holds():
    forward_df = _bars(
        [
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
        ]
    )

    with pytest.raises(ValueError, match="holding_period must be >= 2"):
        evaluate_pick("600118.SH", 10.0, 9.0, 11.0, 1, forward_df)
    with pytest.raises(ValueError, match="runner_max_hold must be >= 2"):
        evaluate_pick(
            "600118.SH", 10.0, 9.0, 11.0, 2, forward_df,
            exit_mode="runner", runner_max_hold=1,
        )


def test_exit_eligible_gap_stop_fills_at_open():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 10.0, "high": 10.2, "low": 9.6, "close": 10.1},
                {"open": 8.5, "high": 8.8, "low": 8.2, "close": 8.6},
            ]
        ),
    )

    assert result["exit_reason"] == "stop_hit_gap"
    assert result["exit_price"] == 8.5
    assert result["exit_day"] == 2


def test_exit_eligible_gap_target_fills_at_open():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 10.0, "high": 10.2, "low": 9.6, "close": 10.1},
                {"open": 11.5, "high": 11.8, "low": 11.2, "close": 11.6},
            ]
        ),
    )

    assert result["exit_reason"] == "target_hit_gap"
    assert result["exit_price"] == 11.5
    assert result["exit_day"] == 2


def test_trailing_breakeven_activation_takes_effect_next_bar():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=20.0,
        holding_period=4,
        exit_mode="trailing",
        atr=1.0,
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 10.0, "high": 10.2, "low": 9.6, "close": 10.0},
                {"open": 10.0, "high": 11.1, "low": 9.5, "close": 10.2},
                {"open": 10.0, "high": 10.2, "low": 9.9, "close": 10.0},
            ]
        ),
    )

    assert result["exit_reason"] == "stop_hit"
    assert result["exit_day"] == 3
    assert result["exit_price"] == 10.0


def test_runner_exit_eligible_gap_stop_fills_at_open():
    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.0,
        stop_loss=9.0,
        target_1=11.0,
        holding_period=3,
        runner_max_hold=3,
        exit_mode="runner",
        forward_df=_bars(
            [
                {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
                {"open": 10.0, "high": 10.2, "low": 9.6, "close": 10.1},
                {"open": 8.5, "high": 8.8, "low": 8.2, "close": 8.6},
            ]
        ),
    )

    assert result["exit_reason"] == "stop_hit_gap"
    assert result["exit_price"] == 8.5
    assert result["exit_day"] == 2


def test_limit_touch_gap_below_stop_fills_then_exits_at_t1_open():
    result = evaluate_pick(
        ticker="600118.SH", entry_price=10.0, stop_loss=9.0, target_1=11.0,
        holding_period=3, max_entry_price=10.2, entry_mode="limit_touch",
        forward_df=_bars([
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            {"open": 8.5, "high": 10.3, "low": 8.2, "close": 9.0},
            {"open": 8.1, "high": 8.4, "low": 7.9, "close": 8.2},
        ]),
    )
    assert result["actual_entry_price"] == 8.5
    assert result["exit_reason"] == "entry_gap_below_stop_t1_exit"
    assert result["exit_price"] == 8.1
    assert result["exit_day"] == 2


def test_hold_expiry_keeps_inclusive_horizon_after_entry_lockout():
    result = evaluate_pick(
        ticker="600118.SH", entry_price=10.0, stop_loss=9.0, target_1=11.0,
        holding_period=3,
        forward_df=_bars([
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            {"open": 10.0, "high": 10.2, "low": 9.6, "close": 10.1},
            {"open": 10.0, "high": 10.3, "low": 9.7, "close": 10.2},
            {"open": 10.0, "high": 10.4, "low": 9.8, "close": 10.3},
        ]),
    )
    assert result["exit_reason"] == "hold_expired"
    assert result["exit_day"] == 3
    assert result["exit_price"] == 10.3


@pytest.mark.parametrize(
    ("exit_mode", "runner_max_hold"),
    [("target_stop", 10), ("runner", 3)],
)
def test_partial_forward_window_remains_unrealized(exit_mode, runner_max_hold):
    result = evaluate_pick(
        ticker="600118.SH", entry_price=10.0, stop_loss=9.0, target_1=11.0,
        holding_period=3, exit_mode=exit_mode, runner_max_hold=runner_max_hold,
        forward_df=_bars([
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            {"open": 10.0, "high": 10.2, "low": 9.6, "close": 10.1},
        ]),
    )

    assert result["exit_reason"] == "holding_window_incomplete"
    assert result["exit_price"] is None
    assert result["pnl_pct"] is None
    assert result["unrealized_pnl_pct"] == 1.0


def test_trailing_arms_on_entry_bar_for_next_session_stop():
    result = evaluate_pick(
        ticker="600118.SH", entry_price=10.0, stop_loss=9.0, target_1=20.0,
        holding_period=3, exit_mode="trailing", atr=1.0,
        forward_df=_bars([
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            {"open": 10.0, "high": 11.1, "low": 9.5, "close": 10.2},
            {"open": 10.0, "high": 10.2, "low": 9.8, "close": 10.0},
            {"open": 10.0, "high": 10.2, "low": 9.8, "close": 10.0},
        ]),
    )

    assert result["exit_reason"] == "stop_hit"
    assert result["exit_day"] == 2
    assert result["exit_price"] == 10.0


def test_runner_arms_on_entry_bar_for_next_session_trailing_stop():
    result = evaluate_pick(
        ticker="600118.SH", entry_price=10.0, stop_loss=9.0, target_1=20.0,
        holding_period=3, exit_mode="runner", runner_max_hold=3,
        atr=1.0, breakeven_at=3.0, trail_atr=1.5,
        forward_df=_bars([
            {"open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0},
            {"open": 10.0, "high": 10.6, "low": 9.5, "close": 10.5},
            {"open": 10.0, "high": 10.2, "low": 9.8, "close": 10.0},
            {"open": 10.0, "high": 10.2, "low": 9.8, "close": 10.0},
        ]),
    )

    assert result["exit_reason"] == "trail_stop"
    assert result["exit_day"] == 2
    assert result["exit_price"] == 10.0


@pytest.mark.parametrize(
    ("exit_reason", "expected"),
    [
        ("target_hit", "win"),
        ("target_hit_gap", "win"),
        ("stop_hit", "loss"),
        ("stop_hit_gap", "loss"),
        ("gap_stop_open", "loss"),
        ("same_bar_stop_first", "loss"),
        ("trail_stop", "loss"),
        ("trail_stop_gap", "loss"),
        ("hold_expired", "hold"),
        ("hold_expired_runner", "hold"),
        ("holding_window_incomplete", "no_data"),
    ],
)
def test_daily_outcome_bucket_includes_all_exit_variants(exit_reason, expected):
    assert _daily_outcome_bucket(exit_reason) == expected


@pytest.mark.parametrize(
    ("exit_reason", "expected"),
    [
        ("stop_hit", True),
        ("stop_hit_gap", True),
        ("gap_stop_open", True),
        ("same_bar_stop_first", True),
        ("trail_stop", True),
        ("trail_stop_gap", True),
        ("target_hit_gap", False),
        ("holding_window_incomplete", False),
    ],
)
def test_post_stop_cooldown_recognizes_all_stop_variants(exit_reason, expected):
    assert _is_stop_exit(exit_reason) is expected
