import sys
from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from scripts import smoke_cn_provider
from src.core import cn_data


def _index_bars():
    return pd.DataFrame(
        {
            "date": ["2026-01-06", "2025-12-31", "2026-01-05", "2026-03-21"],
            "open": ["12", "10", "11", "13"],
            "high": [13, 11, 12, 14],
            "low": [11, 9, 10, 12],
            "close": [12, 10, 11, 13],
            "volume": [123, 456, 789, 102],
        }
    )


def test_index_fetch_supports_legacy_akshare(monkeypatch):
    def fetch_index(*, symbol):
        assert symbol == "sz399001"
        return _index_bars()

    monkeypatch.setitem(
        sys.modules, "akshare", SimpleNamespace(stock_zh_index_daily=fetch_index)
    )
    result = cn_data._ak_fetch_index_daily(
        "399001", "SZ", datetime(2026, 1, 1), datetime(2026, 3, 20)
    )

    assert result.index.tolist() == pd.to_datetime(["2026-01-05", "2026-01-06"]).tolist()
    assert result.columns.tolist() == ["Open", "High", "Low", "Close", "Volume"]
    assert result["Open"].tolist() == [11, 12]
    assert result["Volume"].tolist() == [789, 123]


def test_cn_provider_smoke_accepts_renamed_index_and_fails_closed(monkeypatch):
    seen = []
    equities = []

    def fetch_index(*, symbol):
        seen.append(symbol)
        return _index_bars()

    def legacy_stub(**_):
        raise RuntimeError("legacy AkShare endpoint was renamed")

    equity_bars = _index_bars().rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
        }
    )

    def fetch_equity(**kwargs):
        equities.append(kwargs)
        return equity_bars.copy()

    akshare = SimpleNamespace(
        index_zh_daily_sina=fetch_index,
        stock_zh_index_daily=legacy_stub,
        stock_zh_a_hist_em=fetch_equity,
        stock_zh_a_hist=legacy_stub,
    )
    monkeypatch.setitem(sys.modules, "akshare", akshare)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(cn_data, "_tushare_fetch_daily", lambda *a, **kw: pd.DataFrame())
    monkeypatch.setattr(
        sys, "argv", ["smoke_cn_provider.py", "--config", "config/default.yaml"]
    )

    assert smoke_cn_provider.main() == 0
    assert seen == ["sh000300"]
    assert [p["symbol"] for p in equities] == ["600519", "000858"]
    assert all(
        p["period"] == "daily"
        and p["start_date"] == "20260101"
        and p["end_date"] == "20260320"
        for p in equities
    )

    # A missing benchmark must still block the scheduled scan.
    akshare.index_zh_daily_sina = lambda **_: pd.DataFrame()
    assert smoke_cn_provider.main() == 1
