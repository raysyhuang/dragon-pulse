"""Independent adversarial verification for Task 2.5 (capture-provenance attestation gate).

Authored from the Task 2.5 spec on a separate machine from the builder, with fixtures
built independently of the first-party suite.

Primary targets:
  * grade integrity — OBSERVED_CAPTURE is the stronger claim, so it must be unforgeable
    from within the bundle, and a mixed set must never round up;
  * the permanent caveat on trusted history must not be strippable;
  * attestations/ and raw/ are new path surface introduced into a bundle format whose
    symlink containment was previously verified.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.build_pit_universe_schedule import BuildError, _parse_as_of_dates, build_bundle
from src.core.capture_provenance import CaptureProvenanceError, validate_capture_attestations
from src.core.pit_bundle import PitBundleValidationError, validate_pit_bundle

HEADER = "ts_code,circ_mv,list_date,delist_date"
ROWS = "600003.SH,5000,2010-01-01,\n600000.SH,3000,2010-01-01,\n"
TRUSTED = "TRUSTED_HISTORICAL_ASSUMPTION"
OBSERVED = "OBSERVED_CAPTURE"
CAVEAT = "historical_tushare_trusted_assumption"


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def make_snapshot(src: Path, day: str, *, grade: str = TRUSTED, captured_at: str | None = None,
                  receipt_overrides: dict | None = None, raw_body: str | None = None,
                  write_raw: bool = True) -> Path:
    """Write snapshot + capture receipt (+ raw payload for observed) into a sources dir."""
    csv_path = src / f"daily_basic_{day}.csv"
    csv_path.write_text(HEADER + "\n" + ROWS)

    receipt: dict = {
        "schema_version": 1,
        "provider": "tushare",
        "endpoint": "daily_basic",
        "requested_trade_date": day,
        "snapshot_file": csv_path.name,
        "snapshot_sha256": sha(csv_path),
        "captured_at": captured_at or f"{day[:4]}-{day[4:6]}-{day[6:]}T16:00:00Z",
        "provenance_grade": grade,
    }
    if grade == TRUSTED:
        receipt["caveat"] = CAVEAT
    if grade == OBSERVED:
        raw_dir = src / "raw"
        raw_dir.mkdir(exist_ok=True)
        raw_path = raw_dir / f"daily_basic_{day}.json"
        if write_raw:
            raw_path.write_text(raw_body if raw_body is not None else json.dumps({"data": day}))
        receipt["raw_response_file"] = f"raw/{raw_path.name}"
        receipt["raw_response_sha256"] = sha(raw_path) if write_raw else "0" * 64
    if receipt_overrides:
        receipt.update(receipt_overrides)
        receipt = {k: v for k, v in receipt.items() if v is not _DELETE}
    (src / f"daily_basic_{day}.capture.json").write_text(json.dumps(receipt, indent=2))
    return csv_path


class _Delete:
    pass


_DELETE = _Delete()


@pytest.fixture
def src(tmp_path: Path) -> Path:
    d = tmp_path / "snapshots"
    d.mkdir()
    return d


def build(src: Path, out: Path, dates: str = "2026-01-05", n: int = 2):
    return build_bundle(src, out, _parse_as_of_dates(dates), n, "tushare_daily_basic")


def assert_rejected(src: Path, out: Path, expect: str | tuple[str, ...], **kw):
    with pytest.raises((BuildError, CaptureProvenanceError)) as exc:
        build(src, out, **kw)
    msg = str(exc.value)
    wanted = (expect,) if isinstance(expect, str) else expect
    assert any(w.lower() in msg.lower() for w in wanted), (
        f"rejected, but not for the intended reason.\n  wanted one of: {wanted}\n  got: {msg}"
    )
    return msg


def manifest_of(out: Path) -> dict:
    return json.loads((out / "manifest.json").read_text())


# --------------------------------------------------------------------------------------
# Positive controls
# --------------------------------------------------------------------------------------


def test_trusted_bundle_builds_and_validates(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    validate_pit_bundle(out)
    m = manifest_of(out)
    assert m["capture_provenance_grade"] == TRUSTED
    assert m.get("capture_provenance_caveat"), "manifest carries no trusted-history caveat"
    receipt = json.loads((out / "attestations" / "daily_basic_20260105.capture.json").read_text())
    assert receipt["caveat"] == CAVEAT, "literal caveat lost when the receipt was copied"


def test_observed_bundle_builds_and_binds_raw(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105", grade=OBSERVED)
    out = tmp_path / "b"
    build(src, out)
    validate_pit_bundle(out)
    m = manifest_of(out)
    assert OBSERVED in json.dumps(m)
    raw = list((out / "raw").iterdir())
    assert raw, "observed capture did not copy a raw payload"
    assert any(k.startswith("raw/") for k in m["hashes"]), "raw payload not hash-bound"


def test_never_emits_pit_capture_verified(src: Path, tmp_path: Path):
    """The spec forbids this label outright; it must not appear for either grade."""
    for i, grade in enumerate((TRUSTED, OBSERVED)):
        s = tmp_path / f"s{i}"
        s.mkdir()
        make_snapshot(s, "20260105", grade=grade)
        out = tmp_path / f"o{i}"
        build(s, out)
        assert "PIT_CAPTURE_VERIFIED" not in json.dumps(manifest_of(out))


# --------------------------------------------------------------------------------------
# A. Grade integrity — the strong claim must be unforgeable
# --------------------------------------------------------------------------------------


def test_mixed_grades_must_not_round_up_to_observed(src: Path, tmp_path: Path):
    """One trusted receipt in the set must drag the whole bundle down to trusted."""
    make_snapshot(src, "20260105", grade=OBSERVED)
    make_snapshot(src, "20260106", grade=TRUSTED)
    out = tmp_path / "b"
    build(src, out, dates="2026-01-05,2026-01-06")
    m = manifest_of(out)
    assert m["capture_provenance_grade"] == TRUSTED, "mixed evidence was rounded up to observed"
    assert m.get("capture_provenance_caveat"), "mixed bundle lost the trusted-history caveat"


def test_observed_without_raw_payload_is_rejected(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105", grade=OBSERVED, write_raw=False)
    assert_rejected(src, tmp_path / "b", ("raw", "missing"))


def test_observed_with_raw_hash_mismatch_is_rejected(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105", grade=OBSERVED)
    (src / "raw" / "daily_basic_20260105.json").write_text("tampered")
    assert_rejected(src, tmp_path / "b", ("raw", "sha", "hash"))


@pytest.mark.parametrize("grade", ["PIT_CAPTURE_VERIFIED", "VERIFIED", "observed_capture", "", None, 1])
def test_rejects_unknown_grade(src: Path, tmp_path: Path, grade):
    make_snapshot(src, "20260105", receipt_overrides={"provenance_grade": grade})
    assert_rejected(src, tmp_path / "b", ("grade", "provenance"))


def test_trusted_receipt_missing_caveat_is_rejected(src: Path, tmp_path: Path):
    """The caveat is the whole point of the trusted tier; it must not be droppable."""
    make_snapshot(src, "20260105", receipt_overrides={"caveat": _DELETE})
    assert_rejected(src, tmp_path / "b", ("caveat",))


@pytest.mark.parametrize("bad", ["", "verified", "historical_tushare_trusted", "OK"])
def test_trusted_receipt_wrong_caveat_is_rejected(src: Path, tmp_path: Path, bad):
    make_snapshot(src, "20260105", receipt_overrides={"caveat": bad})
    assert_rejected(src, tmp_path / "b", ("caveat",))


# --------------------------------------------------------------------------------------
# B. Receipt <-> snapshot binding
# --------------------------------------------------------------------------------------


def test_rejects_snapshot_hash_mismatch(src: Path, tmp_path: Path):
    csv_path = make_snapshot(src, "20260105")
    csv_path.write_text(HEADER + "\n600003.SH,9999,2010-01-01,\n600000.SH,3000,2010-01-01,\n")
    assert_rejected(src, tmp_path / "b", ("sha", "hash", "snapshot"))


def test_rejects_receipt_naming_a_different_snapshot(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105", receipt_overrides={"snapshot_file": "daily_basic_20260106.csv"})
    assert_rejected(src, tmp_path / "b", ("snapshot_file", "snapshot"))


def test_rejects_trade_date_disagreeing_with_filename(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105", receipt_overrides={"requested_trade_date": "20260106"})
    assert_rejected(src, tmp_path / "b", ("trade_date", "date"))


@pytest.mark.parametrize("field,bad", [
    ("provider", "wind"), ("provider", "Tushare"), ("provider", ""),
    ("endpoint", "daily"), ("endpoint", "daily_basic_v2"),
    ("schema_version", 2), ("schema_version", "1"), ("schema_version", None),
])
def test_rejects_wrong_literal_fields(src: Path, tmp_path: Path, field, bad):
    make_snapshot(src, "20260105", receipt_overrides={field: bad})
    assert_rejected(src, tmp_path / "b", (field, "provider", "endpoint", "schema"))


def test_rejects_missing_receipt(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105")
    (src / "daily_basic_20260105.capture.json").unlink()
    assert_rejected(src, tmp_path / "b", ("receipt", "missing", "capture"))


def test_rejects_unparseable_receipt(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105")
    (src / "daily_basic_20260105.capture.json").write_text("{not json")
    assert_rejected(src, tmp_path / "b", ("unparseable", "receipt"))


def test_rejects_receipt_hash_not_lowercase(src: Path, tmp_path: Path):
    csv_path = src / "daily_basic_20260105.csv"
    make_snapshot(src, "20260105")
    make_snapshot(src, "20260105", receipt_overrides={"snapshot_sha256": sha(csv_path).upper()})
    assert_rejected(src, tmp_path / "b", ("sha", "lowercase"))


# --------------------------------------------------------------------------------------
# C. Capture time
# --------------------------------------------------------------------------------------


def test_rejects_capture_before_trade_date(src: Path, tmp_path: Path):
    """A snapshot cannot be captured before the session it describes."""
    make_snapshot(src, "20260105", captured_at="2026-01-04T16:00:00Z")
    assert_rejected(src, tmp_path / "b", ("captured_at", "before", "capture"))


@pytest.mark.parametrize("bad", [
    "2026-01-05 16:00:00", "2026-01-05T16:00:00", "not-a-time", "", None, 12345, "2026-13-45T00:00:00Z",
])
def test_rejects_malformed_capture_time(src: Path, tmp_path: Path, bad):
    """Naive timestamps must be rejected: without a zone the ordering claim is unfounded.
    Note "20260105T160000Z" is deliberately absent - it is valid ISO 8601 basic format and
    Python 3.11+ parses it, so rejecting it would be wrong."""
    make_snapshot(src, "20260105", receipt_overrides={"captured_at": bad})
    assert_rejected(src, tmp_path / "b", ("captured_at", "utc", "time"))


def test_probe_capture_time_boundary_and_future(src: Path, tmp_path: Path, capsys):
    """Boundary (midnight of the trade date) and absurd-future capture are reported,
    not asserted: the spec fixes only 'not before the trade date'."""
    results = {}
    for label, ts in (("midnight_of_trade_date", "2026-01-05T00:00:00Z"),
                      ("year_2999", "2999-01-01T00:00:00Z")):
        s = tmp_path / label
        s.mkdir()
        make_snapshot(s, "20260105", captured_at=ts)
        try:
            build(s, tmp_path / f"o_{label}")
            results[label] = "ACCEPTED"
        except (BuildError, CaptureProvenanceError):
            results[label] = "rejected"
    with capsys.disabled():
        print(f"\n[PROBE] capture-time edges: {results}")


# --------------------------------------------------------------------------------------
# D. Path and symlink surface (new directories in a previously verified format)
# --------------------------------------------------------------------------------------


def test_rejects_symlinked_receipt(src: Path, tmp_path: Path, monkeypatch):
    import os
    make_snapshot(src, "20260105")
    receipt = src / "daily_basic_20260105.capture.json"
    stash = tmp_path / "outside_receipt.json"
    stash.write_text(receipt.read_text())
    receipt.unlink()
    os.symlink(stash, receipt)
    assert_rejected(src, tmp_path / "b", ("symlink", "unsafe", "receipt"))


def test_rejects_symlinked_raw_payload(src: Path, tmp_path: Path):
    import os
    make_snapshot(src, "20260105", grade=OBSERVED)
    raw = src / "raw" / "daily_basic_20260105.json"
    stash = tmp_path / "outside_raw.json"
    stash.write_text(raw.read_text())
    raw.unlink()
    os.symlink(stash, raw)
    assert_rejected(src, tmp_path / "b", ("symlink", "unsafe", "raw"))


@pytest.mark.parametrize("bad_raw", [
    "../escape.json", "/etc/passwd", "raw/../../escape.json",
    "raw/nested/deep.json", "notraw/x.json", "",
])
def test_rejects_unsafe_raw_response_path(src: Path, tmp_path: Path, bad_raw):
    make_snapshot(src, "20260105", grade=OBSERVED,
                  receipt_overrides={"raw_response_file": bad_raw})
    assert_rejected(src, tmp_path / "b", ("raw", "path", "flat", "safe"))


# --------------------------------------------------------------------------------------
# E. Output tamper — the bundle must stay self-defending after the build
# --------------------------------------------------------------------------------------


def test_tampered_copied_receipt_is_rejected(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    receipt = out / "attestations" / "daily_basic_20260105.capture.json"
    receipt.write_text(receipt.read_text().replace(CAVEAT, "no_caveat_here"))
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


def test_grade_upgrade_in_output_is_rejected(src: Path, tmp_path: Path):
    """Rewriting a copied receipt to the stronger grade AND restamping every hash must
    still fail: the declared grade has to be re-derived from the receipts."""
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    receipt = out / "attestations" / "daily_basic_20260105.capture.json"
    payload = json.loads(receipt.read_text())
    payload["provenance_grade"] = OBSERVED
    payload.pop("caveat", None)
    receipt.write_text(json.dumps(payload, sort_keys=True) + "\n")

    m = manifest_of(out)
    m["hashes"]["attestations/daily_basic_20260105.capture.json"] = sha(receipt)
    from src.core.pit_bundle import composite_sha256
    m["composite_sha256"] = composite_sha256(m["hashes"])
    blob = json.dumps(m)
    m = json.loads(blob.replace(TRUSTED, OBSERVED))
    m["composite_sha256"] = composite_sha256(m["hashes"])
    (out / "manifest.json").write_text(json.dumps(m, indent=2))

    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


@pytest.mark.parametrize("subdir", ["attestations", "raw"])
def test_unlisted_ancillary_payload_is_rejected(src: Path, tmp_path: Path, subdir):
    make_snapshot(src, "20260105", grade=OBSERVED)
    out = tmp_path / "b"
    build(src, out)
    (out / subdir).mkdir(exist_ok=True)
    (out / subdir / "stowaway.json").write_text("{}")
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


def test_deleted_raw_payload_is_rejected(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105", grade=OBSERVED)
    out = tmp_path / "b"
    build(src, out)
    next((out / "raw").iterdir()).unlink()
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


def test_deleted_receipt_from_output_is_rejected(src: Path, tmp_path: Path):
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    (out / "attestations" / "daily_basic_20260105.capture.json").unlink()
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


# --------------------------------------------------------------------------------------
# F. Epistemic probe — what OBSERVED_CAPTURE can and cannot mean
# --------------------------------------------------------------------------------------


def test_probe_observed_capture_is_self_asserted(src: Path, tmp_path: Path, capsys):
    """A wholly invented 'raw provider response' plus a matching receipt yields a bundle
    graded OBSERVED_CAPTURE. The spec is explicit that there are no provider calls and no
    third-party notarization, so this is the designed boundary, not a defect. Reported so
    that OBSERVED_CAPTURE is never read as 'independently proven to come from Tushare'."""
    make_snapshot(src, "20260105", grade=OBSERVED,
                  raw_body=json.dumps({"totally": "fabricated", "by": "the operator"}))
    out = tmp_path / "b"
    build(src, out)
    validate_pit_bundle(out)
    graded_observed = OBSERVED in json.dumps(manifest_of(out))
    with capsys.disabled():
        print(f"\n[PROBE] fabricated raw payload graded OBSERVED_CAPTURE: {graded_observed}")


# --------------------------------------------------------------------------------------
# G. Validator-side grade re-derivation
#
# Found by mutation testing: the builder computes the grade, but the validator must
# INDEPENDENTLY re-derive it from the receipts. Without that, a hand-crafted bundle can
# simply declare the stronger grade. Neither suite covered this path.
# --------------------------------------------------------------------------------------


def _restamp(out: Path, manifest: dict) -> None:
    from src.core.pit_bundle import composite_sha256
    manifest["composite_sha256"] = composite_sha256(manifest["hashes"])
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))


def test_validator_rederives_grade_on_mixed_bundle(src: Path, tmp_path: Path):
    """A mixed bundle must still validate, and must still read TRUSTED after validation."""
    make_snapshot(src, "20260105", grade=OBSERVED)
    make_snapshot(src, "20260106", grade=TRUSTED)
    out = tmp_path / "b"
    build(src, out, dates="2026-01-05,2026-01-06")
    validate_pit_bundle(out)
    assert manifest_of(out)["capture_provenance_grade"] == TRUSTED


def test_manifest_grade_upgrade_with_valid_receipts_is_rejected(src: Path, tmp_path: Path):
    """Receipts stay internally valid and trusted; only the manifest claims OBSERVED.
    Every hash is restamped, so ONLY the validator's re-derivation can catch this."""
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    m = manifest_of(out)
    m["capture_provenance_grade"] = OBSERVED
    m["capture_provenance_caveat"] = None
    _restamp(out, m)
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


def test_manifest_caveat_removal_with_valid_receipts_is_rejected(src: Path, tmp_path: Path):
    """Grade left honest, caveat stripped. The permanent limitation must be undroppable."""
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    m = manifest_of(out)
    m["capture_provenance_caveat"] = "all good, fully verified"
    _restamp(out, m)
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)


def test_trusted_receipt_claiming_raw_response_is_rejected(src: Path, tmp_path: Path):
    """A trusted receipt must not smuggle observed-tier fields; that is grade laundering."""
    make_snapshot(src, "20260105", receipt_overrides={
        "raw_response_file": "raw/daily_basic_20260105.json",
        "raw_response_sha256": "0" * 64,
    })
    assert_rejected(src, tmp_path / "b", ("raw", "trusted", "observed"))


def test_manifest_grade_upgrade_alone_is_rejected(src: Path, tmp_path: Path):
    """Isolates the grade cross-check: the caveat is left exactly as a trusted bundle
    should have it, so the caveat check cannot fire and mask the grade check."""
    make_snapshot(src, "20260105")
    out = tmp_path / "b"
    build(src, out)
    m = manifest_of(out)
    assert m["capture_provenance_grade"] == TRUSTED
    m["capture_provenance_grade"] = OBSERVED   # caveat deliberately left untouched
    _restamp(out, m)
    with pytest.raises(PitBundleValidationError):
        validate_pit_bundle(out)
