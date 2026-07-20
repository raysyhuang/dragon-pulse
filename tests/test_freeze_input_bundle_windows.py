"""Unit contracts for freezer capture-window modes."""

from __future__ import annotations

import sys

import pandas as pd
import pytest


def _price_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {"Open": [10.0], "High": [11.0], "Low": [9.0], "Close": [10.5], "Volume": [100]},
        index=pd.to_datetime(["2026-03-16"]),
    )


def _run_freezer(monkeypatch, tmp_path, extra_args: list[str]) -> tuple[str, str]:
    from scripts import freeze_input_bundle

    captured: dict[str, str] = {}

    def download(tickers, start, end, provider_config):
        captured["start"] = start
        captured["end"] = end
        return (
            {ticker: _price_frame() for ticker in tickers},
            {"bad_tickers": [], "reasons": {}, "providers": {ticker: "akshare" for ticker in tickers}},
        )

    monkeypatch.setattr(freeze_input_bundle, "load_config", lambda _: {})
    monkeypatch.setattr(freeze_input_bundle, "get_data_functions", lambda _: (None, download, {"primary": "akshare"}, None))
    monkeypatch.setattr(freeze_input_bundle, "get_top_n_cn_by_market_cap", lambda *_: ["600519.SH"])
    monkeypatch.setattr(freeze_input_bundle, "get_cn_basic_info", lambda *_: {"600519.SH": {"name_cn": "Kweichow", "exchange": "SH"}})
    monkeypatch.setattr(freeze_input_bundle.subprocess, "check_output", lambda *_, **__: "test-commit")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "freeze_input_bundle.py", "--output", str(tmp_path / "bundle"),
            "--start", "2026-03-10", "--end", "2026-03-14", "--universe-n", "1",
            *extra_args,
        ],
    )

    assert freeze_input_bundle.main() == 0
    return captured["start"], captured["end"]


def test_strict_price_window_requests_exact_user_dates(monkeypatch, tmp_path):
    """The smoke-only mode must never expand the provider's requested window."""
    start, end = _run_freezer(monkeypatch, tmp_path, ["--strict-price-window"])

    assert (start, end) == ("2026-03-10", "2026-03-14")
    assert (pd.Timestamp(end).date() - pd.Timestamp(start).date()).days <= 5


def test_default_price_window_retains_420_day_lookback_and_extended_end(monkeypatch, tmp_path):
    """Production freezer behavior remains intentionally broader than the scan dates."""
    start, end = _run_freezer(monkeypatch, tmp_path, [])

    assert start == "2025-01-14"
    assert pd.Timestamp(end).date() >= pd.Timestamp("2026-04-13").date()
