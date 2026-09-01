"""Tests for board-aware daily price limits."""

from __future__ import annotations

import pytest

from src.core.cn_limits import get_daily_limit


@pytest.mark.parametrize("ticker,expected", [
    # SSE main board
    ("600519.SH", 0.10),
    ("601398.SH", 0.10),
    ("603259.SH", 0.10),
    # SZSE main board
    ("000001.SZ", 0.10),
    ("002236.SZ", 0.10),
    # SSE STAR Market
    ("688001.SH", 0.20),
    ("688981.SH", 0.20),
    ("689009.SH", 0.20),
    # SZSE ChiNext
    ("300750.SZ", 0.20),
    ("301269.SZ", 0.20),
    # BSE
    ("831799.BJ", 0.30),
    ("430047.BJ", 0.30),
    ("920001.BJ", 0.30),
])
def test_board_limits(ticker: str, expected: float):
    assert get_daily_limit(ticker) == expected


def test_st_override():
    """ST status always caps at 5% regardless of board."""
    assert get_daily_limit("600519.SH", is_st=True) == 0.05
    assert get_daily_limit("688001.SH", is_st=True) == 0.05
    assert get_daily_limit("300750.SZ", is_st=True) == 0.05


def test_unknown_ticker_defaults_to_10pct():
    assert get_daily_limit("INVALID") == 0.10
    assert get_daily_limit("") == 0.10
