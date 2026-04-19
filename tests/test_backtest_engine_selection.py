"""Backtest-specific contract tests."""

from __future__ import annotations

from scripts.backtest_1yr import resolve_backtest_engines


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
