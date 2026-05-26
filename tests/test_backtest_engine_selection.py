"""Backtest-specific contract tests."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from scripts.backtest_1yr import (
    apply_score_floor,
    evaluate_pick,
    parse_regime_set,
    resolve_backtest_engines,
)


def test_backtest_respects_sniper_quarantine_when_all_requested():
    run_mr, run_sniper, sniper_requested = resolve_backtest_engines(
        "all",
        {"sniper": {"enabled": False}},
    )

    assert run_mr is True
    assert sniper_requested is True
    assert run_sniper is False


def test_backtest_can_run_enabled_sniper_when_explicitly_requested():
    run_mr, run_sniper, sniper_requested = resolve_backtest_engines(
        "sniper_only",
        {"sniper": {"enabled": True}},
    )

    assert run_mr is False
    assert sniper_requested is True
    assert run_sniper is True


def test_backtest_regime_filter_parser_normalizes_comma_separated_values():
    assert parse_regime_set(" Bear, CHOPPY ,, bull ") == {"bear", "choppy", "bull"}


def test_backtest_regime_filter_parser_accepts_config_lists():
    assert parse_regime_set([" Bear", "CHOPPY", ""]) == {"bear", "choppy"}


def test_backtest_global_score_floor_filters_candidates():
    candidates = [
        ("mean_reversion", SimpleNamespace(score=94.9)),
        ("mean_reversion", SimpleNamespace(score=95.0)),
        ("sniper", SimpleNamespace(score=99.0)),
    ]

    filtered = apply_score_floor(candidates, 95)

    assert [signal.score for _, signal in filtered] == [95.0, 99.0]


def test_backtest_no_chase_skips_when_next_open_exceeds_max_entry():
    forward_df = pd.DataFrame(
        [
            {"open": 10.00, "high": 10.10, "low": 9.90, "close": 10.00},
            {"open": 10.31, "high": 10.50, "low": 10.20, "close": 10.40},
        ],
        index=pd.to_datetime(["2026-05-25", "2026-05-26"]),
    )

    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.00,
        stop_loss=9.50,
        target_1=11.00,
        holding_period=3,
        forward_df=forward_df,
        max_entry_price=10.20,
        entry_mode="no_chase_next_open",
    )

    assert result["exit_reason"] == "entry_skipped_no_chase"
    assert result["pnl_pct"] is None
    assert result["entry_status"] == "skipped"
    assert result["max_entry_price"] == 10.20
    assert result["actual_entry_price"] is None


def test_backtest_no_chase_fills_at_next_open_when_within_max_entry():
    forward_df = pd.DataFrame(
        [
            {"open": 10.00, "high": 10.10, "low": 9.90, "close": 10.00},
            {"open": 10.15, "high": 10.80, "low": 10.00, "close": 10.40},
            {"open": 10.45, "high": 11.20, "low": 10.30, "close": 11.00},
        ],
        index=pd.to_datetime(["2026-05-25", "2026-05-26", "2026-05-27"]),
    )

    result = evaluate_pick(
        ticker="600118.SH",
        entry_price=10.00,
        stop_loss=9.50,
        target_1=11.00,
        holding_period=3,
        forward_df=forward_df,
        max_entry_price=10.20,
        entry_mode="no_chase_next_open",
    )

    assert result["entry_status"] == "filled"
    assert result["actual_entry_price"] == 10.15
    assert result["entry_price"] == 10.15
    assert result["exit_reason"] == "target_hit"
    assert result["pnl_pct"] == 8.37
