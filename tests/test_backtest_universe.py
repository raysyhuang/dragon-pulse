from __future__ import annotations

import json

from src.features.performance.backtest import load_execution_watchlist_tickers_in_range


def test_load_execution_watchlist_tickers_in_range_dedups_in_date_and_pick_order(tmp_path):
    outputs_root = tmp_path / "outputs"
    day1 = outputs_root / "2026-04-15"
    day2 = outputs_root / "2026-04-16"
    day1.mkdir(parents=True)
    day2.mkdir(parents=True)

    (day1 / "execution_watchlist_2026-04-15.json").write_text(
        json.dumps(
            {
                "regime": "bear",
                "picks": [
                    {"ticker": "600001.SH"},
                    {"ticker": "000001.SZ"},
                    {"ticker": "600001.SH"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (day2 / "execution_watchlist_2026-04-16.json").write_text(
        json.dumps(
            {
                "regime": "choppy",
                "picks": [
                    {"ticker": "300001.SZ"},
                    {"ticker": "000001.SZ"},
                    {"ticker": "600002.SH"},
                ],
            }
        ),
        encoding="utf-8",
    )

    tickers = load_execution_watchlist_tickers_in_range(
        outputs_root,
        start_date="2026-04-15",
        end_date="2026-04-16",
    )

    assert tickers == [
        "600001.SH",
        "000001.SZ",
        "300001.SZ",
        "600002.SH",
    ]


def test_load_execution_watchlist_tickers_in_range_returns_empty_for_missing_files(tmp_path):
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True)

    tickers = load_execution_watchlist_tickers_in_range(
        outputs_root,
        start_date="2026-04-15",
        end_date="2026-04-16",
    )

    assert tickers == []
