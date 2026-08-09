"""Vertical-contract tests for the offline PIT universe schedule builder."""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.core.pit_bundle import validate_pit_bundle

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "build_pit_universe_schedule.py"
FIELDS = ["ts_code", "circ_mv", "list_date", "delist_date"]

_SPEC = importlib.util.spec_from_file_location("build_pit_universe_schedule", SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
builder = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(builder)


def _write_snapshot(directory: Path, as_of: str, rows: list[dict[str, str]], *, name: str | None = None, fields=FIELDS) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (name or f"daily_basic_{as_of.replace('-', '')}.csv")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    day = as_of.replace("-", "")
    (directory / f"daily_basic_{day}.capture.json").write_text(
        json.dumps({
            "schema_version": 1, "provider": "tushare", "endpoint": "daily_basic",
            "requested_trade_date": day, "snapshot_file": path.name,
            "snapshot_sha256": _sha256(path), "captured_at": "2025-01-03T16:00:00Z",
            "provenance_grade": "TRUSTED_HISTORICAL_ASSUMPTION",
            "caveat": "historical_tushare_trusted_assumption",
        }), encoding="utf-8"
    )
    return path


def _valid_sources(tmp_path: Path) -> Path:
    sources = tmp_path / "input"
    _write_snapshot(
        sources,
        "2025-01-02",
        [
            {"ts_code": "000002.SZ", "circ_mv": "100", "list_date": "2020-01-01", "delist_date": ""},
            {"ts_code": "000001.SZ", "circ_mv": "100", "list_date": "2025-01-02", "delist_date": ""},
        ],
    )
    _write_snapshot(
        sources,
        "2025-01-03",
        [
            {"ts_code": "000003.SZ", "circ_mv": "300", "list_date": "2025-01-03", "delist_date": ""},
            {"ts_code": "000001.SZ", "circ_mv": "200", "list_date": "2020-01-01", "delist_date": ""},
            {"ts_code": "000002.SZ", "circ_mv": "100", "list_date": "2020-01-01", "delist_date": ""},
        ],
    )
    return sources


def _run(sources: Path, output: Path, *, dates="2025-01-02,2025-01-03", n="2", label="fixture_daily_basic") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--sources-dir",
            str(sources),
            "--output",
            str(output),
            "--as-of-dates",
            dates,
            "--universe-n",
            n,
            "--source-label",
            label,
        ],
        cwd=REPO,
        text=True,
        capture_output=True,
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_builds_multi_date_validator_accepted_provenance_bundle(tmp_path):
    sources = _valid_sources(tmp_path)
    output = tmp_path / "bundle"

    completed = _run(sources, output)

    assert completed.returncode == 0, completed.stderr
    loaded = validate_pit_bundle(output)
    assert loaded.pit_grade is True
    assert loaded.as_of_dates == ("2025-01-02", "2025-01-03")
    assert [(row["as_of_date"], row["ticker"]) for row in loaded.schedule] == [
        ("2025-01-02", "000001.SZ"),
        ("2025-01-02", "000002.SZ"),
        ("2025-01-03", "000001.SZ"),
        ("2025-01-03", "000003.SZ"),
    ]
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["pit_grade"] is True
    assert manifest["universe_n"] == 2
    assert manifest["source_label"] == "fixture_daily_basic"
    assert manifest["provenance"]["source_label"] == "fixture_daily_basic"
    assert isinstance(manifest["builder_git_commit"], str) and manifest["builder_git_commit"]
    assert set(manifest["hashes"]) == {
        "attestations/daily_basic_20250102.capture.json",
        "attestations/daily_basic_20250103.capture.json",
        "sources/daily_basic_20250102.csv",
        "sources/daily_basic_20250103.csv",
        "universe_schedule.csv",
    }
    for as_of in ("2025-01-02", "2025-01-03"):
        copied = output / "sources" / f"daily_basic_{as_of.replace('-', '')}.csv"
        original = sources / copied.name
        assert copied.read_bytes() == original.read_bytes()
        assert manifest["hashes"][f"sources/{copied.name}"] == _sha256(copied)
        rows = [row for row in loaded.schedule if row["as_of_date"] == as_of]
        assert {row["source_file"] for row in rows} == {f"sources/{copied.name}"}
        assert {row["source_sha256"] for row in rows} == {_sha256(copied)}


def test_main_accepts_relative_sources_dir_and_builds_valid_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _valid_sources(tmp_path)
    output = tmp_path / "bundle"
    monkeypatch.chdir(tmp_path)

    assert builder.main([
        "--sources-dir", "input", "--output", str(output),
        "--as-of-dates", "2025-01-02,2025-01-03", "--universe-n", "2",
        "--source-label", "fixture_daily_basic",
    ]) == 0
    assert validate_pit_bundle(output).pit_grade is True


@pytest.mark.parametrize(("porcelain", "expected_dirty"), [(" M scripts/build_pit_universe_schedule.py\n", True), ("", False)])
def test_manifest_honestly_records_injected_builder_tree_status(tmp_path, monkeypatch, porcelain, expected_dirty):
    def fake_check_output(command, *, text, stderr):
        if command[-2:] == ["rev-parse", "HEAD"]:
            return "f" * 40 + "\n"
        assert command[-2:] == ["status", "--porcelain"]
        return porcelain

    monkeypatch.setattr(builder.subprocess, "check_output", fake_check_output)

    manifest = builder.build_bundle(
        _valid_sources(tmp_path),
        tmp_path / "bundle",
        builder._parse_as_of_dates("2025-01-02,2025-01-03"),
        2,
        "fixture_daily_basic",
    )

    assert manifest["builder_git_commit"] == "f" * 40
    assert manifest["builder_tree_dirty"] is expected_dirty


def test_tied_circ_mv_membership_is_ticker_ordered_regardless_of_snapshot_row_order(tmp_path):
    rows = [
        {"ts_code": "000002.SZ", "circ_mv": "100", "list_date": "2020-01-01", "delist_date": ""},
        {"ts_code": "000001.SZ", "circ_mv": "100", "list_date": "2020-01-01", "delist_date": ""},
    ]
    first_sources = tmp_path / "first-input"
    second_sources = tmp_path / "second-input"
    _write_snapshot(first_sources, "2025-01-02", rows)
    _write_snapshot(second_sources, "2025-01-02", list(reversed(rows)))

    first = builder.build_bundle(
        first_sources,
        tmp_path / "first-bundle",
        builder._parse_as_of_dates("2025-01-02"),
        1,
        "fixture_daily_basic",
    )
    second = builder.build_bundle(
        second_sources,
        tmp_path / "second-bundle",
        builder._parse_as_of_dates("2025-01-02"),
        1,
        "fixture_daily_basic",
    )

    assert [row["ticker"] for row in csv.DictReader((tmp_path / "first-bundle" / "universe_schedule.csv").open(encoding="utf-8"))] == ["000001.SZ"]
    assert [row["ticker"] for row in csv.DictReader((tmp_path / "second-bundle" / "universe_schedule.csv").open(encoding="utf-8"))] == ["000001.SZ"]
    assert first["universe_n"] == second["universe_n"] == 1


@pytest.mark.parametrize("malformed", ["BAD!", "000001.sz", "1.SZ"])
def test_rejects_noncanonical_ticker_format_without_output(tmp_path, malformed):
    sources = tmp_path / "input"
    _write_snapshot(
        sources,
        "2025-01-02",
        [
            {"ts_code": "000001.SZ", "circ_mv": "300", "list_date": "2020-01-01", "delist_date": ""},
            {"ts_code": "600000.SH", "circ_mv": "200", "list_date": "2020-01-01", "delist_date": ""},
            {"ts_code": "430001.BJ", "circ_mv": "100", "list_date": "2020-01-01", "delist_date": ""},
            {"ts_code": malformed, "circ_mv": "1", "list_date": "2020-01-01", "delist_date": ""},
        ],
    )
    output = tmp_path / "bundle"

    completed = _run(sources, output, dates="2025-01-02", n="2")

    assert completed.returncode != 0
    assert "row 5" in completed.stderr
    assert "ticker must be canonical A-share ts_code" in completed.stderr
    assert not output.exists()


@pytest.mark.parametrize(
    "invalid_row",
    [
        {"ts_code": "000003.SZ", "circ_mv": "999", "list_date": "2025-01-03", "delist_date": ""},
        {"ts_code": "000004.SZ", "circ_mv": "999", "list_date": "2020-01-01", "delist_date": "2025-01-02"},
    ],
)
def test_rejects_snapshot_eligibility_contradiction_even_with_enough_valid_candidates(tmp_path, invalid_row):
    sources = tmp_path / "input"
    _write_snapshot(
        sources,
        "2025-01-02",
        [
            {"ts_code": "000001.SZ", "circ_mv": "300", "list_date": "2020-01-01", "delist_date": ""},
            {"ts_code": "600000.SH", "circ_mv": "200", "list_date": "2020-01-01", "delist_date": ""},
            invalid_row,
        ],
    )
    output = tmp_path / "bundle"

    completed = _run(sources, output, dates="2025-01-02", n="2")

    assert completed.returncode != 0
    assert "row 4" in completed.stderr
    assert "source snapshot eligibility contradiction" in completed.stderr
    assert not output.exists()


@pytest.mark.parametrize(
    ("rows", "dates", "n", "name", "fields", "expected"),
    [
        (None, "2025-01-02", "1", None, FIELDS, "missing required snapshot"),
        ([{"ts_code": "", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "missing ticker"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": ""}, {"ts_code": "000001.SZ", "circ_mv": "2", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "duplicate ticker"),
        ([{"ts_code": "000001.SZ", "circ_mv": "", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "circ_mv"),
        ([{"ts_code": "000001.SZ", "circ_mv": "not-a-number", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "circ_mv"),
        ([{"ts_code": "000001.SZ", "circ_mv": "0", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "circ_mv"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "list_date"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2025/01/01", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "list_date"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": "2025/01/01"}], "2025-01-02", "1", None, FIELDS, "delist_date"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2025-01-03", "delist_date": ""}], "2025-01-02", "1", None, FIELDS, "source snapshot eligibility contradiction"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": "2025-01-02"}], "2025-01-02", "1", None, FIELDS, "source snapshot eligibility contradiction"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "2", None, FIELDS, "fewer eligible"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", "daily_basic_wrong.csv", FIELDS, "missing required snapshot"),
        ([{"ts_code": "000001.SZ", "circ_mv": "1", "list_date": "2020-01-01", "delist_date": ""}], "2025-01-02", "1", None, ["ts_code", "circ_mv"], "required columns"),
    ],
)
def test_rejects_invalid_supplied_snapshot_without_partial_output(tmp_path, rows, dates, n, name, fields, expected):
    sources = tmp_path / "input"
    sources.mkdir()
    if rows is not None:
        _write_snapshot(sources, "2025-01-02", rows, name=name, fields=fields)
    output = tmp_path / "bundle"

    completed = _run(sources, output, dates=dates, n=n)

    assert completed.returncode != 0
    assert expected in completed.stderr
    assert not output.exists()


@pytest.mark.parametrize("dates,n,label,expected", [("2025/01/02", "1", "label", "ISO YYYY-MM-DD"), ("2025-01-02", "0", "label", "positive integer"), ("2025-01-02", "1", "", "source-label")])
def test_rejects_invalid_cli_contract(tmp_path, dates, n, label, expected):
    completed = _run(_valid_sources(tmp_path), tmp_path / "bundle", dates=dates, n=n, label=label)

    assert completed.returncode != 0
    assert expected in completed.stderr


def test_refuses_existing_output_atomically(tmp_path):
    output = tmp_path / "bundle"
    output.mkdir()
    sentinel = output / "sentinel.txt"
    sentinel.write_text("preserve", encoding="utf-8")

    completed = _run(_valid_sources(tmp_path), output)

    assert completed.returncode != 0
    assert "already exists" in completed.stderr
    assert sentinel.read_text(encoding="utf-8") == "preserve"
    assert not (output / "manifest.json").exists()


def test_observed_and_trusted_inputs_copy_hash_bound_evidence_and_keep_trusted_caveat(tmp_path):
    sources = _valid_sources(tmp_path)
    raw = sources / "raw" / "daily_basic_20250102.response.json"
    raw.parent.mkdir()
    raw.write_text('{"observed": true}\n', encoding="utf-8")
    observed_receipt = sources / "daily_basic_20250102.capture.json"
    payload = json.loads(observed_receipt.read_text(encoding="utf-8"))
    payload.pop("caveat")
    payload.update({
        "provenance_grade": "OBSERVED_CAPTURE",
        "raw_response_file": "raw/daily_basic_20250102.response.json",
        "raw_response_sha256": _sha256(raw),
    })
    observed_receipt.write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "bundle"

    assert _run(sources, output).returncode == 0
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["capture_provenance_grade"] == "TRUSTED_HISTORICAL_ASSUMPTION"
    assert manifest["capture_provenance_caveat"] == "trusted history is not independently capture-proven"
    assert (output / "raw" / raw.name).read_bytes() == raw.read_bytes()
    assert manifest["hashes"]["raw/daily_basic_20250102.response.json"] == _sha256(output / "raw" / raw.name)
    assert manifest["hashes"]["attestations/daily_basic_20250102.capture.json"] == _sha256(output / "attestations" / observed_receipt.name)
    validate_pit_bundle(output)
    (output / "raw" / raw.name).write_text("tampered", encoding="utf-8")
    with pytest.raises(Exception, match="hash mismatch"):
        validate_pit_bundle(output)


def test_validator_rejects_unlisted_ancillary_payload(tmp_path):
    output = tmp_path / "bundle"
    assert _run(_valid_sources(tmp_path), output).returncode == 0
    (output / "attestations" / "stray.capture.json").write_text("{}", encoding="utf-8")

    with pytest.raises(Exception, match="unhashed ancillary payload"):
        validate_pit_bundle(output)
