"""Regression guards for backtest CSI300/snapshot data adequacy."""
from __future__ import annotations

from datetime import date
import json

import pandas as pd
import pytest

from scripts.backtest_1yr import _assert_csi_regime_coverage, _snapshot_covers_range, get_cn_trading_days


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


def test_real_sse_calendar_excludes_a_known_lunar_new_year_week(monkeypatch):
    payload = {"code": 0, "data": {"fields": ["cal_date", "is_open"], "items": [
        ["20230120", "1"], ["20230123", "0"], ["20230124", "0"], ["20230125", "0"], ["20230126", "0"], ["20230127", "0"], ["20230130", "1"],
    ]}}

    class Response:
        def read(self):
            return json.dumps(payload).encode()

    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: Response())
    sessions = get_cn_trading_days(date(2023, 1, 20), date(2023, 1, 30))
    assert sessions == [date(2023, 1, 20), date(2023, 1, 30)]


def test_real_sse_calendar_fails_closed_when_provider_errors(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("down")))
    with pytest.raises(RuntimeError, match="authoritative SSE trading calendar"):
        get_cn_trading_days(date(2023, 1, 20), date(2023, 1, 30))
