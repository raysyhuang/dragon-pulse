"""Containment verification for the H1 relative-path repair (decf447).

The repair intentionally EXEMPTS symlinked ancestors at or above the sources directory
so that /tmp- and /var-style paths work. The risk of any such relaxation is that the
exemption leaks downward and starts trusting symlinks BELOW the root as well.

Every test here asserts the exemption stops exactly at the sources directory.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts.build_pit_universe_schedule import BuildError, _parse_as_of_dates, build_bundle
from src.core.capture_provenance import CaptureProvenanceError, validate_capture_attestations

HEADER = "ts_code,circ_mv,list_date,delist_date"
ROWS = "600003.SH,5000,2010-01-01,\n600000.SH,3000,2010-01-01,\n"


def populate(sources: Path, day: str = "20260105") -> Path:
    sources.mkdir(parents=True, exist_ok=True)
    csv_path = sources / f"daily_basic_{day}.csv"
    csv_path.write_text(HEADER + "\n" + ROWS)
    (sources / f"daily_basic_{day}.capture.json").write_text(json.dumps({
        "schema_version": 1, "provider": "tushare", "endpoint": "daily_basic",
        "requested_trade_date": day, "snapshot_file": csv_path.name,
        "snapshot_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "captured_at": "2026-01-05T16:00:00Z",
        "provenance_grade": "TRUSTED_HISTORICAL_ASSUMPTION",
        "caveat": "historical_tushare_trusted_assumption",
    }))
    return csv_path


def build(sources: Path, out: Path):
    return build_bundle(sources, out, _parse_as_of_dates("2026-01-05"), 2, "tushare_daily_basic")


def assert_rejected(sources: Path, out: Path, why: str):
    with pytest.raises((BuildError, CaptureProvenanceError)) as exc:
        build(sources, out)
    msg = str(exc.value).lower()
    assert any(w in msg for w in ("symlink", "outside", "unsafe", "missing")), \
        f"{why}: rejected but not on a containment ground -> {exc.value}"


# --------------------------------------------------------------------------------------
# The exemption must apply ABOVE the sources dir
# --------------------------------------------------------------------------------------


def test_symlinked_ancestor_is_accepted(tmp_path: Path):
    real = tmp_path / "real"
    populate(real / "snaps")
    os.symlink(real, tmp_path / "link")
    build(tmp_path / "link" / "snaps", tmp_path / "out")


def test_two_symlinked_ancestors_are_accepted(tmp_path: Path):
    """Depth of legitimate indirection must not matter."""
    real = tmp_path / "real"
    populate(real / "a" / "snaps")
    os.symlink(real, tmp_path / "l1")
    os.symlink(tmp_path / "l1" / "a", tmp_path / "l2")
    build(tmp_path / "l2" / "snaps", tmp_path / "out")


# --------------------------------------------------------------------------------------
# ...and must STOP at the sources dir. Everything below stays fail-closed.
# --------------------------------------------------------------------------------------


def test_symlinked_snapshot_file_still_rejected(tmp_path: Path):
    sources = tmp_path / "snaps"
    populate(sources)
    outside = tmp_path / "outside.csv"
    outside.write_text(HEADER + "\n" + ROWS)
    target = sources / "daily_basic_20260105.csv"
    target.unlink()
    os.symlink(outside, target)
    assert_rejected(sources, tmp_path / "out", "symlinked snapshot")


def test_symlinked_snapshot_pointing_inside_still_rejected(tmp_path: Path):
    """Inward symlinks are still symlinks: repointable after hashing."""
    sources = tmp_path / "snaps"
    populate(sources)
    target = sources / "daily_basic_20260105.csv"
    stash = sources / "_real.csv"
    target.rename(stash)
    os.symlink(stash, target)
    assert_rejected(sources, tmp_path / "out", "inward symlinked snapshot")


def test_symlinked_receipt_still_rejected(tmp_path: Path):
    sources = tmp_path / "snaps"
    populate(sources)
    receipt = sources / "daily_basic_20260105.capture.json"
    outside = tmp_path / "outside.json"
    outside.write_text(receipt.read_text())
    receipt.unlink()
    os.symlink(outside, receipt)
    assert_rejected(sources, tmp_path / "out", "symlinked receipt")


def test_symlinked_intermediate_dir_below_root_still_rejected(tmp_path: Path):
    """A symlinked directory BETWEEN the sources root and the snapshot must fail:
    this is the exact case the ancestor exemption must not swallow."""
    sources = tmp_path / "snaps"
    sources.mkdir()
    real_sub = tmp_path / "elsewhere"
    populate(real_sub)
    os.symlink(real_sub, sources / "sub")
    with pytest.raises((BuildError, CaptureProvenanceError)):
        validate_capture_attestations(sources, [sources / "sub" / "daily_basic_20260105.csv"])


def test_ancestor_symlink_plus_child_symlink_still_rejected(tmp_path: Path):
    """The combination case: a LEGITIMATE symlinked ancestor must not license an
    escaping symlink below the root."""
    real = tmp_path / "real"
    sources = real / "snaps"
    populate(sources)
    os.symlink(real, tmp_path / "link")
    outside = tmp_path / "outside.csv"
    outside.write_text(HEADER + "\n" + ROWS)
    target = sources / "daily_basic_20260105.csv"
    target.unlink()
    os.symlink(outside, target)
    assert_rejected(tmp_path / "link" / "snaps", tmp_path / "out", "ancestor + child symlink")


def test_symlinked_raw_payload_still_rejected(tmp_path: Path):
    sources = tmp_path / "snaps"
    csv_path = populate(sources)
    raw_dir = sources / "raw"
    raw_dir.mkdir()
    outside = tmp_path / "outside_raw.json"
    outside.write_text('{"x":1}')
    raw = raw_dir / "daily_basic_20260105.json"
    os.symlink(outside, raw)
    receipt = sources / "daily_basic_20260105.capture.json"
    payload = json.loads(receipt.read_text())
    payload.update({
        "provenance_grade": "OBSERVED_CAPTURE",
        "raw_response_file": "raw/daily_basic_20260105.json",
        "raw_response_sha256": hashlib.sha256(outside.read_bytes()).hexdigest(),
    })
    payload.pop("caveat", None)
    receipt.write_text(json.dumps(payload))
    assert_rejected(sources, tmp_path / "out", "symlinked raw payload")


def test_symlinked_raw_directory_still_rejected(tmp_path: Path):
    sources = tmp_path / "snaps"
    populate(sources)
    outside_raw = tmp_path / "outside_raw"
    outside_raw.mkdir()
    body = '{"x":1}'
    (outside_raw / "daily_basic_20260105.json").write_text(body)
    os.symlink(outside_raw, sources / "raw")
    receipt = sources / "daily_basic_20260105.capture.json"
    payload = json.loads(receipt.read_text())
    payload.update({
        "provenance_grade": "OBSERVED_CAPTURE",
        "raw_response_file": "raw/daily_basic_20260105.json",
        "raw_response_sha256": hashlib.sha256(body.encode()).hexdigest(),
    })
    payload.pop("caveat", None)
    receipt.write_text(json.dumps(payload))
    assert_rejected(sources, tmp_path / "out", "symlinked raw dir")


# --------------------------------------------------------------------------------------
# Traversal via the snapshot argument itself
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("relative", [
    "../outside/daily_basic_20260105.csv",
    "sub/../../outside/daily_basic_20260105.csv",
])
def test_traversal_snapshot_argument_rejected(tmp_path: Path, relative):
    """The escaping path must be FULLY VALID apart from the traversal - conforming
    filename, real file, matching receipt - or the request is rejected on a name or
    missing-file technicality and the containment guard is never exercised.

    Found by mutation testing: with a non-conforming filename this test passed while
    the `..` guard was disabled, i.e. the right answer for the wrong reason.
    """
    sources = tmp_path / "snaps"
    populate(sources)
    outside = tmp_path / "outside"
    populate(outside)

    with pytest.raises((CaptureProvenanceError, BuildError)) as exc:
        validate_capture_attestations(sources, [Path(relative)])
    msg = str(exc.value).lower()
    assert "outside allowed root" in msg or "symlink" in msg, (
        f"rejected, but not on containment grounds: {exc.value}"
    )


def test_absolute_snapshot_outside_root_rejected(tmp_path: Path):
    sources = tmp_path / "snaps"
    populate(sources)
    outside = tmp_path / "outside.csv"
    outside.write_text(HEADER + "\n" + ROWS)
    with pytest.raises((CaptureProvenanceError, BuildError)):
        validate_capture_attestations(sources, [outside])


def test_probe_symlinked_sources_root_passed_to_library(tmp_path: Path, capsys):
    """The builder rejects a symlinked sources dir outright. The library is a lower-level
    entry point where the sources dir IS the exempted ancestor. Reported, not asserted:
    callers other than the builder must not assume the library screens this."""
    real = tmp_path / "real"
    populate(real)
    link = tmp_path / "link"
    os.symlink(real, link)
    try:
        validate_capture_attestations(link, [link / "daily_basic_20260105.csv"])
        result = "ACCEPTED"
    except (CaptureProvenanceError, BuildError):
        result = "rejected"
    builder_result = "rejected"
    try:
        build(link, tmp_path / "out")
        builder_result = "ACCEPTED"
    except (BuildError, CaptureProvenanceError):
        pass
    with capsys.disabled():
        print(f"\n[PROBE] symlinked sources ROOT -> library: {result}, builder: {builder_result}")
