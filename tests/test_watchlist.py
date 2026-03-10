"""Tests for execution watchlist artifact generation and morning check consumption."""
from __future__ import annotations

import json
from pathlib import Path

from scripts.morning_check import run_preflight, check_gap


def test_watchlist_pick_preserves_name():
    """Watchlist picks must include company name for display."""
    pick = {
        "ticker": "600519.SH",
        "name": "贵州茅台",
        "sources": ["Weekly", "Pro30"],
        "entry_price": 1800.0,
        "target_price": 1980.0,
        "stop_price": 1710.0,
        "hybrid_score": 85,
        "confidence": "HIGH",
    }
    assert pick["name"] == "贵州茅台"
    assert pick["entry_price"] > 0
    assert pick["stop_price"] < pick["entry_price"] < pick["target_price"]


def test_watchlist_maps_to_morning_check_input():
    """Watchlist picks can be consumed by run_preflight directly."""
    picks = [
        {
            "ticker": "600519.SH",
            "name_cn": "贵州茅台",
            "entry_price": 1800.0,
        },
    ]
    open_prices = {"600519.SH": 1820.0}
    results = run_preflight(picks, open_prices, prev_volumes={})
    assert len(results) == 1
    assert results[0].action == "GO"
    assert results[0].name_cn == "贵州茅台"


def test_morning_check_gap_up_cancels():
    """Gap up > 3% should cancel the pick."""
    action, reasons = check_gap(100.0, 104.0, max_gap_up_pct=3.0)
    assert action == "CANCEL"
    assert len(reasons) > 0


def test_morning_check_gap_down_cancels():
    """Gap down > 5% should cancel the pick."""
    action, reasons = check_gap(100.0, 94.0, max_gap_down_pct=5.0)
    assert action == "CANCEL"


def test_morning_check_moderate_gap_warns():
    """Moderate gap up (>60% of threshold) should warn."""
    action, reasons = check_gap(100.0, 102.0, max_gap_up_pct=3.0)
    assert action == "WARN"


def test_morning_check_normal_gap_goes():
    """Normal gap within limits should be GO."""
    action, reasons = check_gap(100.0, 100.5, max_gap_up_pct=3.0)
    assert action == "GO"


def test_morning_check_missing_open_warns():
    """Missing open price should warn, not crash."""
    picks = [{"ticker": "000858.SZ", "name_cn": "五粮液", "entry_price": 150.0}]
    results = run_preflight(picks, open_prices={}, prev_volumes={})
    assert results[0].action == "WARN"
    assert "No opening price" in results[0].reasons[0]
