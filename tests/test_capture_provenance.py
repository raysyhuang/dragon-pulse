"""Task 2.5 capture-provenance attestation contract."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.core.capture_provenance import CaptureProvenanceError, validate_capture_attestations


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot(sources: Path, day: str = "20250102") -> Path:
    sources.mkdir(parents=True, exist_ok=True)
    path = sources / f"daily_basic_{day}.csv"
    path.write_text("ts_code,circ_mv,list_date,delist_date\n000001.SZ,1,2020-01-01,\n", encoding="utf-8")
    return path


def _receipt(snapshot: Path, *, grade: str = "TRUSTED_HISTORICAL_ASSUMPTION", raw: Path | None = None, **changes: object) -> Path:
    day = snapshot.stem.removeprefix("daily_basic_")
    payload: dict[str, object] = {
        "schema_version": 1,
        "provider": "tushare",
        "endpoint": "daily_basic",
        "requested_trade_date": day,
        "snapshot_file": snapshot.name,
        "snapshot_sha256": _sha256(snapshot),
        "captured_at": "2025-01-02T16:00:00Z",
        "provenance_grade": grade,
    }
    if grade == "TRUSTED_HISTORICAL_ASSUMPTION":
        payload["caveat"] = "historical_tushare_trusted_assumption"
    if raw is not None:
        payload["raw_response_file"] = f"raw/{raw.name}"
        payload["raw_response_sha256"] = _sha256(raw)
    payload.update(changes)
    receipt = snapshot.parent / f"daily_basic_{day}.capture.json"
    receipt.write_text(json.dumps(payload), encoding="utf-8")
    return receipt


def test_valid_trusted_receipt_is_accepted_before_builder_exists(tmp_path: Path):
    snapshot = _snapshot(tmp_path / "input")
    _receipt(snapshot, captured_at="2025-01-03T16:00:00Z")

    attestations = validate_capture_attestations(snapshot.parent, [snapshot])

    assert attestations[0].provenance_grade == "TRUSTED_HISTORICAL_ASSUMPTION"
    assert attestations[0].caveat == "historical_tushare_trusted_assumption"


def test_accepts_absolute_and_relative_snapshots_through_symlinked_ancestor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    physical_parent = tmp_path / "physical-parent"
    physical_parent.mkdir()
    lexical_parent = tmp_path / "lexical-parent"
    lexical_parent.symlink_to(physical_parent, target_is_directory=True)
    sources = lexical_parent / "input"
    snapshot = _snapshot(sources)
    _receipt(snapshot)

    assert validate_capture_attestations(sources, [snapshot])[0].snapshot_path == snapshot

    monkeypatch.chdir(sources)
    assert validate_capture_attestations(Path("."), [Path(snapshot.name)])[0].snapshot_path == Path(snapshot.name)


def test_rejects_symlinked_snapshot_child_below_sources_root(tmp_path: Path):
    sources = tmp_path / "input"
    external = tmp_path / "external"
    snapshot = _snapshot(external)
    receipt = _receipt(snapshot)
    sources.mkdir()
    receipt.rename(sources / receipt.name)
    (sources / "nested").symlink_to(external, target_is_directory=True)

    with pytest.raises(CaptureProvenanceError, match="symlinked snapshot"):
        validate_capture_attestations(sources, [sources / "nested" / snapshot.name])


def test_rejects_schema_version_other_than_one(tmp_path: Path):
    snapshot = _snapshot(tmp_path / "input")
    _receipt(snapshot, schema_version=2)

    with pytest.raises(CaptureProvenanceError, match="schema_version"):
        validate_capture_attestations(snapshot.parent, [snapshot])


def test_rejects_naive_captured_at_timestamp(tmp_path: Path):
    snapshot = _snapshot(tmp_path / "input")
    _receipt(snapshot, captured_at="2025-01-02T16:00:00")

    with pytest.raises(CaptureProvenanceError, match="captured_at"):
        validate_capture_attestations(snapshot.parent, [snapshot])


def test_rejects_capture_before_requested_trade_date(tmp_path: Path):
    snapshot = _snapshot(tmp_path / "input")
    _receipt(snapshot, captured_at="2025-01-01T23:59:59Z")

    with pytest.raises(CaptureProvenanceError, match="captured_at predates requested_trade_date"):
        validate_capture_attestations(snapshot.parent, [snapshot])


def test_valid_observed_receipt_requires_hash_bound_raw_payload(tmp_path: Path):
    sources = tmp_path / "input"
    snapshot = _snapshot(sources)
    raw = sources / "raw" / "daily_basic_20250102.response.json"
    raw.parent.mkdir()
    raw.write_text('{"data": []}\n', encoding="utf-8")
    _receipt(snapshot, grade="OBSERVED_CAPTURE", raw=raw)

    attestation = validate_capture_attestations(sources, [snapshot])[0]

    assert attestation.raw_response_file == "raw/daily_basic_20250102.response.json"


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"snapshot_sha256": "0" * 64}, "snapshot hash mismatch"),
        ({"provider": "other"}, "provider"),
        ({"endpoint": "daily"}, "endpoint"),
        ({"requested_trade_date": "20250103"}, "requested_trade_date"),
        ({"snapshot_file": "daily_basic_20250103.csv"}, "snapshot_file"),
        ({"provenance_grade": "PIT_CAPTURE_VERIFIED"}, "provenance_grade"),
        ({"caveat": "PIT_CAPTURE_VERIFIED"}, "caveat"),
    ],
)
def test_rejects_stale_or_mismatched_trusted_receipt(tmp_path: Path, changes: dict[str, object], expected: str):
    snapshot = _snapshot(tmp_path / "input")
    _receipt(snapshot, **changes)

    with pytest.raises(CaptureProvenanceError, match=expected):
        validate_capture_attestations(snapshot.parent, [snapshot])


@pytest.mark.parametrize(
    ("raw_path", "raw_hash", "expected"),
    [
        ("../outside.json", None, "raw_response_file"),
        ("raw/../outside.json", None, "raw_response_file"),
        ("raw/a\n.json", None, "raw_response_file"),
        ("raw/missing.json", "0" * 64, "missing raw response"),
        ("raw/daily_basic_20250102.response.json", "0" * 64, "raw response hash mismatch"),
    ],
)
def test_rejects_observed_raw_path_and_hash_errors(tmp_path: Path, raw_path: str, raw_hash: str | None, expected: str):
    sources = tmp_path / "input"
    snapshot = _snapshot(sources)
    raw = sources / "raw" / "daily_basic_20250102.response.json"
    raw.parent.mkdir()
    raw.write_text("raw", encoding="utf-8")
    _receipt(snapshot, grade="OBSERVED_CAPTURE", raw_response_file=raw_path,
             raw_response_sha256=raw_hash or _sha256(raw))

    with pytest.raises(CaptureProvenanceError, match=expected):
        validate_capture_attestations(sources, [snapshot])


def test_rejects_missing_duplicate_and_symlinked_receipts(tmp_path: Path):
    sources = tmp_path / "input"
    snapshot = _snapshot(sources)
    with pytest.raises(CaptureProvenanceError, match="missing capture receipt"):
        validate_capture_attestations(sources, [snapshot])
    receipt = _receipt(snapshot)
    replacement = sources / "receipt-source.json"
    receipt.rename(replacement)
    receipt.symlink_to(replacement)
    with pytest.raises(CaptureProvenanceError, match="symlink"):
        validate_capture_attestations(sources, [snapshot])


def test_rejects_symlinked_raw_escape(tmp_path: Path):
    sources = tmp_path / "input"
    snapshot = _snapshot(sources)
    outside = tmp_path / "outside.json"
    outside.write_text("raw", encoding="utf-8")
    raw = sources / "raw" / "daily_basic_20250102.response.json"
    raw.parent.mkdir()
    raw.symlink_to(outside)
    _receipt(snapshot, grade="OBSERVED_CAPTURE", raw=outside,
             raw_response_file="raw/daily_basic_20250102.response.json")

    with pytest.raises(CaptureProvenanceError, match="symlink"):
        validate_capture_attestations(sources, [snapshot])
