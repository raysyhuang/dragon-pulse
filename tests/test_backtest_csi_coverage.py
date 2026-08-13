"""Regression guards for backtest CSI300/snapshot data adequacy."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from scripts.backtest_1yr import _assert_csi_regime_coverage, _snapshot_covers_range


def _frame(start: str, periods: int) -> pd.DataFrame:
    index = pd.bdate_range(start, periods=periods)
    return pd.DataFrame({"close": range(100, 100 + periods)}, index=index)


def test_snapshot_coverage_rejects_present_but_too_recent_file():
    frame = _frame("2025-04-28", 300)
    assert not _snapshot_covers_range(frame, date(2021, 1, 1), date(2026, 7, 22))


def test_snapshot_coverage_accepts_requested_bounds():
    frame = _frame("2020-10-01", 1600)
    assert _snapshot_covers_range(frame, date(2021, 1, 1), date(2026, 7, 22))


def test_csi_regime_coverage_fails_closed_for_short_pre_start_history():
    frame = _frame("2025-04-28", 300)
    with pytest.raises(ValueError, match="CSI300 regime coverage inadequate"):
        _assert_csi_regime_coverage(frame, [date(2021, 1, 4), date(2021, 1, 5)], sma_long=50)


def test_csi_regime_coverage_accepts_complete_history():
    frame = _frame("2020-10-01", 400)
    _assert_csi_regime_coverage(frame, [date(2021, 1, 4), date(2021, 6, 30)], sma_long=50)
