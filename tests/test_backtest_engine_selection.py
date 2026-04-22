"""Backtest-specific contract tests."""

from __future__ import annotations

from types import SimpleNamespace

from scripts.backtest_1yr import (
    apply_score_floor,
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


def test_backtest_global_score_floor_filters_candidates():
    candidates = [
        ("mean_reversion", SimpleNamespace(score=94.9)),
        ("mean_reversion", SimpleNamespace(score=95.0)),
        ("sniper", SimpleNamespace(score=99.0)),
    ]

    filtered = apply_score_floor(candidates, 95)

    assert [signal.score for _, signal in filtered] == [95.0, 99.0]
