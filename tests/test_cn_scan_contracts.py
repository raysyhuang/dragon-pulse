from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from collections import Counter
import time

import pandas as pd
import pytest

import src.core.cn_data as cn_data
from src.core.universe import get_top_n_cn_by_market_cap
from src.pipelines.scanner import _sort_signal_candidates


def test_market_cap_universe_requires_tushare_ranking(monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    monkeypatch.setattr(
        cn_data,
        "_ak_fetch_basic_info",
        lambda: pd.DataFrame({"ticker": ["000001.SZ", "000002.SZ"]}),
    )

    with pytest.raises(RuntimeError, match="TUSHARE_TOKEN"):
        get_top_n_cn_by_market_cap(
            provider_config={"cache_file": "/tmp/does-not-exist-ranked-universe.csv"}
        )


def test_sort_signal_candidates_breaks_ties_by_adv_then_market_cap():
    candidates = [
        ("sniper", SimpleNamespace(ticker="AAA", score=80.0)),
        ("mean_reversion", SimpleNamespace(ticker="BBB", score=80.0)),
        ("sniper", SimpleNamespace(ticker="CCC", score=80.0)),
        ("mean_reversion", SimpleNamespace(ticker="DDD", score=75.0)),
    ]
    data_map = {
        "AAA": pd.DataFrame({"Close": [10] * 20, "Volume": [100] * 20}),
        "BBB": pd.DataFrame({"Close": [10] * 20, "Volume": [200] * 20}),
        "CCC": pd.DataFrame({"Close": [10] * 20, "Volume": [200] * 20}),
        "DDD": pd.DataFrame({"Close": [10] * 20, "Volume": [500] * 20}),
    }
    info_map = {
        "AAA": {"market_cap": 300},
        "BBB": {"market_cap": 100},
        "CCC": {"market_cap": 500},
        "DDD": {"market_cap": 1000},
    }

    ranked = _sort_signal_candidates(candidates, data_map, info_map)

    assert [sig.ticker for _, sig in ranked] == ["CCC", "BBB", "AAA", "DDD"]


def test_market_cap_universe_uses_ranked_cache_when_live_fetch_unavailable(
    monkeypatch, tmp_path
):
    cache_file = tmp_path / "ranked_cache.csv"
    fresh_asof = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    pd.DataFrame(
        {
            "Ticker": ["000002.SZ", "600519.SH", "000001.SZ"],
            "asof": [fresh_asof] * 3,
        }
    ).to_csv(cache_file, index=False)

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    ranked = get_top_n_cn_by_market_cap(
        n=2,
        provider_config={"cache_file": str(cache_file), "cache_max_age_days": 7},
    )

    assert ranked == ["000002.SZ", "600519.SH"]


def test_download_daily_range_disables_tushare_backup_after_repeated_timeouts(
    monkeypatch,
):
    calls = Counter()

    monkeypatch.setattr(
        cn_data,
        "_ak_fetch_daily",
        lambda *args, **kwargs: calls.update(["akshare"]) or pd.DataFrame(),
    )

    def fake_tushare_fetch(*args, **kwargs):
        calls.update(["tushare"])
        raise RuntimeError("HTTPConnectionPool(...): Read timed out. (read timeout=30)")

    monkeypatch.setattr(cn_data, "_tushare_fetch_daily", fake_tushare_fetch)

    tickers = [f"{i:06d}.SZ" for i in range(1, 7)]
    _, report = cn_data.download_daily_range(
        tickers=tickers,
        start="2025-01-01",
        end="2025-01-31",
        provider_config={"backup_timeout_trip_count": 2},
    )

    assert calls["akshare"] == len(tickers)
    assert calls["tushare"] == 2
    assert "__disabled_backups__" in report["reasons"]


def test_download_daily_range_times_out_stuck_primary_and_uses_backup(
    monkeypatch,
):
    calls = Counter()

    def fake_akshare_fetch(*args, **kwargs):
        calls.update(["akshare"])
        time.sleep(0.2)
        return pd.DataFrame()

    def fake_tushare_fetch(*args, **kwargs):
        calls.update(["tushare"])
        return pd.DataFrame(
            {
                "Open": [10.0],
                "High": [10.5],
                "Low": [9.8],
                "Close": [10.2],
                "Volume": [1000.0],
            },
            index=pd.to_datetime(["2025-01-02"]),
        )

    monkeypatch.setattr(cn_data, "_ak_fetch_daily", fake_akshare_fetch)
    monkeypatch.setattr(cn_data, "_tushare_fetch_daily", fake_tushare_fetch)

    data_map, report = cn_data.download_daily_range(
        tickers=["000001.SZ"],
        start="2025-01-01",
        end="2025-01-31",
        provider_config={"provider_timeout_seconds": 0.05},
    )

    assert calls["akshare"] == 1
    assert calls["tushare"] == 1
    assert "000001.SZ" in data_map
    assert report["bad_tickers"] == []
