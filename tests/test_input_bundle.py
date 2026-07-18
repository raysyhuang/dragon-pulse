"""Offline integrity contracts for frozen backtest input bundles."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from src.core import input_bundle


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _composite(files: dict[str, str]) -> str:
    payload = "".join(f"{path}  {digest}\n" for path, digest in sorted(files.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _make_bundle(tmp_path: Path, *, basic_info: bool = True) -> Path:
    bundle = tmp_path / "bundle"
    prices = bundle / "prices"
    prices.mkdir(parents=True)
    (bundle / "universe.csv").write_text(
        "ticker,name,board,is_st,cap_rank_at_freeze\n000001.SZ,Test,main,false,1\n",
        encoding="utf-8",
    )
    if basic_info:
        (bundle / "basic_info.json").write_text(
            json.dumps({"000001.SZ": {"name_cn": "Test", "industry": "Test", "is_st": False}}),
            encoding="utf-8",
        )
    else:
        (bundle / "basic_info.json").write_text("{}", encoding="utf-8")
    prices.joinpath("000001.SZ.csv").write_text(
        "Date,open,high,low,close,volume\n2025-01-02,10,11,9,10,1000\n",
        encoding="utf-8",
    )
    files = {
        "universe.csv": _sha256(bundle / "universe.csv"),
        "basic_info.json": _sha256(bundle / "basic_info.json"),
        "prices/000001.SZ.csv": _sha256(prices / "000001.SZ.csv"),
    }
    manifest = {
        "bundle_id": "test-bundle",
        "created_at": "2026-07-18T00:00:00Z",
        "freeze_tool_commit": "test",
        "pit_grade": False,
        "universe_count": 1,
        "files": files,
        "composite_sha256": _composite(files),
        "notes": "cap universe frozen at capture time; NOT historical PIT",
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return bundle


def test_validate_input_bundle_loads_offline_inputs_and_preserves_identity(tmp_path):
    bundle = _make_bundle(tmp_path)

    loaded = input_bundle.validate_input_bundle(bundle, acceptance_mode="live_equivalent")

    assert loaded.bundle_id == "test-bundle"
    assert loaded.pit_grade is False
    assert loaded.tickers == ["000001.SZ"]
    assert loaded.basic_info["000001.SZ"]["is_st"] is False
    assert list(loaded.data_map) == ["000001.SZ"]
    assert loaded.composite_sha256 == loaded.manifest["composite_sha256"]


def test_validate_input_bundle_reports_tamper_and_writes_no_artifacts(tmp_path):
    bundle = _make_bundle(tmp_path)
    bundle.joinpath("prices/000001.SZ.csv").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(input_bundle.InputBundleValidationError, match=r"prices/000001\.SZ\.csv"):
        input_bundle.validate_input_bundle(bundle)


def test_validate_input_bundle_rejects_extra_and_missing_price_csvs(tmp_path):
    bundle = _make_bundle(tmp_path)
    bundle.joinpath("prices/extra.csv").write_text("x", encoding="utf-8")

    with pytest.raises(input_bundle.InputBundleValidationError, match="extra.csv"):
        input_bundle.validate_input_bundle(bundle)


def test_validate_input_bundle_requires_basic_info_only_for_live_equivalent(tmp_path):
    bundle = _make_bundle(tmp_path, basic_info=False)
    manifest = json.loads(bundle.joinpath("manifest.json").read_text(encoding="utf-8"))
    manifest["files"]["basic_info.json"] = _sha256(bundle / "basic_info.json")
    manifest["composite_sha256"] = _composite(manifest["files"])
    bundle.joinpath("manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    assert input_bundle.validate_input_bundle(bundle, acceptance_mode="off").tickers == ["000001.SZ"]
    with pytest.raises(input_bundle.InputBundleValidationError, match="basic_info"):
        input_bundle.validate_input_bundle(bundle, acceptance_mode="live_equivalent")


def test_manifest_listed_non_layout_file_is_rejected(tmp_path):
    bundle = _make_bundle(tmp_path)
    extra = bundle / "notes.txt"
    extra.write_text("not allowed", encoding="utf-8")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["notes.txt"] = _sha256(extra)
    manifest["composite_sha256"] = _composite(manifest["files"])
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(input_bundle.InputBundleValidationError, match="unsupported bundle file"):
        input_bundle.validate_input_bundle(bundle)


def test_manifest_universe_count_mismatch_is_rejected(tmp_path):
    bundle = _make_bundle(tmp_path)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["universe_count"] = 2
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(input_bundle.InputBundleValidationError, match="universe_count"):
        input_bundle.validate_input_bundle(bundle)


def test_bundle_mode_guard_blocks_provider_calls():
    from src.core.universe import get_top_n_cn_by_market_cap

    input_bundle.set_input_mode("bundle")
    try:
        with pytest.raises(input_bundle.BundleModeViolation, match="get_top_n_cn_by_market_cap"):
            get_top_n_cn_by_market_cap()
    finally:
        input_bundle.set_input_mode("live")


def test_backtest_cli_rejects_bundle_with_price_snapshot(monkeypatch, capsys, tmp_path):
    from scripts import backtest_1yr

    monkeypatch.setattr(
        "sys.argv",
        [
            "backtest_1yr.py",
            "--input-bundle", str(tmp_path / "bundle"),
            "--price-snapshot-dir", str(tmp_path / "snapshot"),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        backtest_1yr.main()

    assert exc_info.value.code == 2
    assert "not allowed with argument --input-bundle" in capsys.readouterr().err
