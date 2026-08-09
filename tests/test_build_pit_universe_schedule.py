"""Vertical-contract tests for the offline PIT universe schedule builder."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.core.pit_bundle import validate_pit_bundle

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "build_pit_universe_schedule.py"
FIELDS = ["ts_code", "circ_mv", "list_date", "delist_date"]


def _write_snapshot(directory: Path, as_of: str, rows: list[dict[str, str]], *, name: str | None = None, fields=FIELDS) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (name or f"daily_basic_{as_of.replace('-', '')}.csv")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
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
