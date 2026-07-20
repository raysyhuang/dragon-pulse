"""Opt-in live smoke contract for the input-bundle freezer."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest


pytestmark = pytest.mark.network


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _composite(files: dict[str, str]) -> str:
    payload = "".join(f"{name}  {digest}\n" for name, digest in sorted(files.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _require_live_tushare() -> None:
    if not os.environ.get("TUSHARE_TOKEN"):
        pytest.skip("network smoke test requires TUSHARE_TOKEN; credential is not configured")
    try:
        with socket.create_connection(("api.tushare.pro", 443), timeout=5):
            pass
    except OSError as exc:
        pytest.skip(f"network smoke test requires api.tushare.pro:443; network is unreachable ({exc.__class__.__name__})")


def test_freezer_creates_atomic_strict_bundle_without_provider_fallback(tmp_path, monkeypatch):
    """A two-name, <=5-day live freeze records every price provider used."""
    _require_live_tushare()
    from scripts import freeze_input_bundle

    output = tmp_path / "bundle"
    config = tmp_path / "smoke-config.yaml"
    config.write_text(
        """market:\n  region: CN\ndata:\n  china:\n    primary: akshare\n    backup: ''\n    adjust: qfq\n    tushare_token_env: TUSHARE_TOKEN\nmean_reversion:\n  regime:\n    csi300_symbol: '000300.SH'\n""",
        encoding="utf-8",
    )
    end = date.today() - timedelta(days=7)
    start = end - timedelta(days=4)
    assert (end - start).days <= 5
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "freeze_input_bundle.py",
            "--output", str(output),
            "--start", start.isoformat(),
            "--end", end.isoformat(),
            "--config", str(config),
            "--universe-n", "2",
            "--bundle-id", "network-smoke",
            "--acceptance-mode", "live_equivalent",
        ],
    )

    try:
        assert freeze_input_bundle.main() == 0
    except RuntimeError as exc:
        if str(exc).startswith(("Cannot build market-cap-ranked universe", "refusing incomplete bundle")):
            pytest.skip("network smoke test could not reach an authenticated live provider; check credentials and connectivity")
        raise

    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    tickers = pd.read_csv(output / "universe.csv")["ticker"].tolist()
    expected_files = {
        "universe.csv",
        "basic_info.json",
        *(f"prices/{ticker}.csv" for ticker in tickers),
        "prices/000300.SH.csv",
    }
    actual_files = {
        path.relative_to(output).as_posix()
        for path in output.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }

    assert len(tickers) == 2
    assert actual_files == expected_files == set(manifest["files"])
    assert manifest["pit_grade"] is False
    assert manifest["price_providers"] == {name: "akshare" for name in sorted(expected_files) if name.startswith("prices/")}
    assert manifest["provider_fallbacks"] == []
    from src.core.input_bundle import validate_input_bundle

    loaded = validate_input_bundle(
        output,
        acceptance_mode="live_equivalent",
        support_tickers=("000300.SH",),
    )
    assert loaded.tickers == tickers
    assert not list(tmp_path.glob(".bundle.freeze-*"))
    assert {name: _sha256(output / name) for name in expected_files} == manifest["files"]
    assert _composite(manifest["files"]) == manifest["composite_sha256"]
