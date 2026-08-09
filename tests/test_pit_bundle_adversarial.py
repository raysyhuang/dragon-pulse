"""Independent adversarial verification suite for Task 1 (strict PIT manifest validator).

Test INTENT was authored before the implementation was pushed, from the Task 1 spec in
docs/plans/2026-08-09-evidence-grade-research-infrastructure.md, on a separate machine
from the builder. The fixture BUILDER was then adapted to the implementation's actual
manifest schema (`bundle_id` / `hashes` / `composite_sha256`), which deviates from the
spec's wording (`sources` list). That deviation is reported as a finding.

Every negative test asserts a SPECIFIC failure message. This matters: on the first run
against a schema-mismatched fixture, 47 negative tests "passed" trivially because the
control bundle was itself invalid. Substring assertions make a trivial pass impossible.

Run against a clean checkout of the pushed SHA:
    python -m pytest tests/test_pit_bundle_adversarial.py -q
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest

from src.core.pit_bundle import (
    PitBundle,
    PitBundleValidationError,
    composite_sha256,
    validate_pit_bundle,
)

SCHEDULE_FIELDS = [
    "as_of_date",
    "ticker",
    "listed_on_or_before",
    "delisted_after",
    "source_file",
    "source_sha256",
]

# 600000 eligible on both dates; 600001 lists exactly on the second date and delists
# later, exercising both eligibility boundaries.
BASE_ROWS = [
    ("2026-01-05", "600000.SH", "2015-01-01", "", "sources/daily_basic_20260105.csv"),
    ("2026-01-05", "600002.SH", "2015-01-01", "", "sources/daily_basic_20260105.csv"),
    ("2026-01-06", "600000.SH", "2015-01-01", "", "sources/daily_basic_20260106.csv"),
    ("2026-01-06", "600001.SH", "2026-01-06", "2027-01-01", "sources/daily_basic_20260106.csv"),
]

SOURCE_BODIES = {
    "sources/daily_basic_20260105.csv": "ts_code,circ_mv\n600000.SH,1000\n600002.SH,900\n",
    "sources/daily_basic_20260106.csv": "ts_code,circ_mv\n600000.SH,1010\n600001.SH,880\n",
}

SRC_A = "sources/daily_basic_20260105.csv"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --------------------------------------------------------------------------------------
# Fixture construction — the single place that knows the bundle schema
# --------------------------------------------------------------------------------------


def build_valid_bundle(root: Path) -> Path:
    bundle = root / "bundle"
    (bundle / "sources").mkdir(parents=True)
    for relpath, body in SOURCE_BODIES.items():
        (bundle / relpath).write_text(body)

    write_schedule(bundle, [list(r) for r in _rows_with_digests(bundle)], restamp=False)

    hashes = {relpath: sha256_file(bundle / relpath) for relpath in SOURCE_BODIES}
    hashes["universe_schedule.csv"] = sha256_file(bundle / "universe_schedule.csv")
    write_manifest(
        bundle,
        {
            "pit_grade": True,
            "bundle_id": "test-bundle-0001",
            "as_of_dates": ["2026-01-05", "2026-01-06"],
            "universe_n": 2,
            "source_label": "tushare_daily_basic",
            "hashes": hashes,
            "composite_sha256": composite_sha256(hashes),
        },
    )
    return bundle


def _rows_with_digests(bundle: Path) -> list[list[str]]:
    digests = {relpath: sha256_file(bundle / relpath) for relpath in SOURCE_BODIES}
    return [
        [as_of, ticker, listed, delisted, src, digests[src]]
        for as_of, ticker, listed, delisted, src in BASE_ROWS
    ]


def write_manifest(bundle: Path, manifest: dict) -> None:
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2))


def read_manifest(bundle: Path) -> dict:
    return json.loads((bundle / "manifest.json").read_text())


def set_hashes(bundle: Path, hashes: dict, *, restamp_composite: bool = True) -> None:
    """Replace manifest hashes, keeping composite honest so tests isolate one defect."""
    manifest = read_manifest(bundle)
    manifest["hashes"] = hashes
    if restamp_composite:
        manifest["composite_sha256"] = composite_sha256(hashes)
    write_manifest(bundle, manifest)


def write_schedule(bundle: Path, rows: list[list[str]], *, restamp: bool = True,
                   fields: list[str] | None = None) -> None:
    path = bundle / "universe_schedule.csv"
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(fields or SCHEDULE_FIELDS)
        writer.writerows(rows)
    if restamp:
        manifest = read_manifest(bundle)
        manifest["hashes"]["universe_schedule.csv"] = sha256_file(path)
        manifest["composite_sha256"] = composite_sha256(manifest["hashes"])
        write_manifest(bundle, manifest)


def current_rows(bundle: Path) -> list[list[str]]:
    with (bundle / "universe_schedule.csv").open() as fh:
        reader = csv.reader(fh)
        next(reader)
        return [row for row in reader]


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    return build_valid_bundle(tmp_path)


def assert_rejected(bundle_path: Path, expect: str | tuple[str, ...]) -> str:
    """Rejection must be the TYPED error and must cite the intended reason.

    A bare KeyError/OSError escaping the validator is itself a finding: fail-closed
    must be deliberate, not incidental.
    """
    with pytest.raises(PitBundleValidationError) as exc:
        validate_pit_bundle(bundle_path)
    message = str(exc.value)
    wanted = (expect,) if isinstance(expect, str) else expect
    assert any(w in message for w in wanted), (
        f"rejected, but not for the intended reason.\n  wanted one of: {wanted}\n  got: {message}"
    )
    return message


# --------------------------------------------------------------------------------------
# Control — if these fail, the fixtures are wrong, not the implementation
# --------------------------------------------------------------------------------------


def test_control_valid_bundle_loads(bundle: Path):
    result = validate_pit_bundle(bundle)
    assert isinstance(result, PitBundle)
    assert result.pit_grade is True
    assert len(result.schedule) == 4


def test_control_validation_is_deterministic(bundle: Path):
    first = validate_pit_bundle(bundle)
    second = validate_pit_bundle(bundle)
    assert first.composite_sha256 == second.composite_sha256
    assert [dict(r) for r in first.schedule] == [dict(r) for r in second.schedule]


# --------------------------------------------------------------------------------------
# A. Lookahead / survivorship
# --------------------------------------------------------------------------------------


def test_rejects_membership_before_listing(bundle: Path):
    rows = current_rows(bundle)
    rows[0][2] = "2026-06-01"
    write_schedule(bundle, rows)
    assert_rejected(bundle, "was not listed as of")


def test_rejects_membership_after_delisting(bundle: Path):
    rows = current_rows(bundle)
    rows[0][3] = "2025-01-01"
    write_schedule(bundle, rows)
    assert_rejected(bundle, "was already delisted as of")


def test_boundary_as_of_equals_listing_date_is_eligible(bundle: Path):
    """First eligible session must be INCLUSIVE of the listing date; an off-by-one here
    silently thins every historical universe."""
    rows = current_rows(bundle)
    rows[0][2] = "2026-01-05"
    write_schedule(bundle, rows)
    validate_pit_bundle(bundle)


def test_boundary_as_of_equals_delisted_after_is_ineligible(bundle: Path):
    rows = current_rows(bundle)
    rows[0][3] = "2026-01-05"
    write_schedule(bundle, rows)
    assert_rejected(bundle, "was already delisted as of")


def test_rejects_schedule_date_absent_from_manifest(bundle: Path):
    rows = current_rows(bundle)
    rows.append(["2026-01-07", "600000.SH", "2015-01-01", "", rows[0][4], rows[0][5]])
    write_schedule(bundle, rows)
    assert_rejected(bundle, "schedule date outside manifest.as_of_dates")


def test_rejects_manifest_date_with_no_schedule_rows(bundle: Path):
    manifest = read_manifest(bundle)
    manifest["as_of_dates"] = ["2026-01-05", "2026-01-06", "2026-01-07"]
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "schedule dates do not exactly match")


def test_rejects_empty_schedule(bundle: Path):
    write_schedule(bundle, [])
    assert_rejected(bundle, "schedule must not be empty")


def test_rejects_duplicate_as_of_ticker_pair(bundle: Path):
    rows = current_rows(bundle)
    rows.append(list(rows[0]))
    write_schedule(bundle, rows)
    assert_rejected(bundle, "duplicate schedule identity")


# --------------------------------------------------------------------------------------
# B. Tamper evidence
# --------------------------------------------------------------------------------------


def test_rejects_mutated_source_with_stale_manifest_hash(bundle: Path):
    src = bundle / SRC_A
    src.write_text(src.read_text() + "600003.SH,800\n")
    assert_rejected(bundle, "hash mismatch for")


def test_rejects_mutated_schedule_with_stale_hash(bundle: Path):
    rows = current_rows(bundle)
    rows[0][1] = "600999.SH"
    write_schedule(bundle, rows, restamp=False)
    assert_rejected(bundle, "hash mismatch for")


def test_rejects_row_digest_disagreeing_with_manifest_digest(bundle: Path):
    rows = current_rows(bundle)
    rows[0][5] = "0" * 64
    write_schedule(bundle, rows)
    assert_rejected(bundle, "schedule source digest does not match manifest hash")


def test_rejects_stale_composite_hash_alone(bundle: Path):
    """Composite must bind even when every individual file hash is correct."""
    manifest = read_manifest(bundle)
    manifest["composite_sha256"] = "0" * 64
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "composite hash mismatch")


def test_rejects_source_missing_on_disk(bundle: Path):
    (bundle / SRC_A).unlink()
    assert_rejected(bundle, "missing manifest-listed file")


def test_rejects_source_referenced_by_schedule_but_unhashed(bundle: Path):
    manifest = read_manifest(bundle)
    hashes = {k: v for k, v in manifest["hashes"].items() if k != SRC_A}
    set_hashes(bundle, hashes)
    assert_rejected(bundle, "schedule source missing manifest hash")


def test_unhashed_raw_source_is_named_as_such(bundle: Path):
    manifest = read_manifest(bundle)
    hashes = {k: v for k, v in manifest["hashes"].items() if k != SRC_A}
    set_hashes(bundle, hashes)
    assert_rejected(bundle, "unhashed raw source")


@pytest.mark.parametrize("bad", ["not-a-sha", "abc123", "A" * 64, "0" * 63, ""])
def test_rejects_malformed_manifest_hash(bundle: Path, bad):
    """Uppercase hex and truncated digests must not slip through a loose regex."""
    manifest = read_manifest(bundle)
    hashes = dict(manifest["hashes"])
    hashes[SRC_A] = bad
    set_hashes(bundle, hashes)
    assert_rejected(bundle, "invalid SHA-256 manifest hash")


def test_rejects_extra_untracked_file_in_bundle(bundle: Path):
    """A file inside the bundle that no hash covers breaks self-containment."""
    (bundle / "notes.txt").write_text("hello")
    assert_rejected(bundle, "unlisted PIT bundle file")


def test_rejects_unreferenced_manifest_source(bundle: Path):
    """A hashed source no schedule row uses is dead provenance weight."""
    extra = bundle / "sources" / "daily_basic_20260107.csv"
    extra.write_text("ts_code,circ_mv\n600000.SH,1020\n")
    manifest = read_manifest(bundle)
    hashes = dict(manifest["hashes"])
    hashes["sources/daily_basic_20260107.csv"] = sha256_file(extra)
    set_hashes(bundle, hashes)
    assert_rejected(bundle, "unreferenced manifest source hash")


# --------------------------------------------------------------------------------------
# C. Path safety
# --------------------------------------------------------------------------------------


def test_rejects_symlinked_source_pointing_outside_bundle(bundle: Path, tmp_path: Path):
    outside = tmp_path / "outside.csv"
    outside.write_text(SOURCE_BODIES[SRC_A])
    target = bundle / SRC_A
    target.unlink()
    os.symlink(outside, target)
    assert_rejected(bundle, ("symlinked bundle payload is forbidden",
                             "symlinked manifest payload is forbidden"))


def test_rejects_symlinked_source_pointing_inside_bundle(bundle: Path):
    """Even an inward symlink defeats byte provenance: it can be repointed after hashing."""
    real = bundle / SRC_A
    stash = bundle / "sources" / "_real.csv"
    shutil.move(str(real), str(stash))
    os.symlink(stash, real)
    assert_rejected(bundle, ("symlinked bundle payload is forbidden",
                             "symlinked manifest payload is forbidden"))


def test_rejects_symlinked_sources_directory(bundle: Path, tmp_path: Path):
    """Same escape one level up: linking sources/ sidesteps per-file link checks."""
    outside = tmp_path / "outside_sources"
    outside.mkdir()
    for relpath, body in SOURCE_BODIES.items():
        (outside / Path(relpath).name).write_text(body)
    shutil.rmtree(bundle / "sources")
    os.symlink(outside, bundle / "sources")
    assert_rejected(bundle, ("symlinked bundle payload is forbidden",
                             "symlinked manifest payload is forbidden"))


def test_rejects_stray_symlinked_directory_in_bundle(bundle: Path, tmp_path: Path):
    """Regression guard for the rglob symlink scan, which is UNIQUELY load-bearing here.

    A symlinked directory is invisible to the unlisted-file check (`is_file()` is False),
    and it is not manifest-listed so the per-payload symlink check never sees it. Found
    by mutation testing: deleting the scan broke nothing in either suite.
    """
    outside = tmp_path / "secret"
    outside.mkdir()
    (outside / "leak.csv").write_text("x")
    os.symlink(outside, bundle / "extra_dir")
    assert_rejected(bundle, "symlinked bundle payload is forbidden")


@pytest.mark.parametrize("bad_path", [
    "sources/../../etc/passwd",
    "/etc/passwd",
    "sources/./x.csv",
    "sources/sub/../x.csv",
])
def test_rejects_unsafe_manifest_hash_path(bundle: Path, bad_path):
    manifest = read_manifest(bundle)
    hashes = dict(manifest["hashes"])
    hashes[bad_path] = "0" * 64
    set_hashes(bundle, hashes)
    assert_rejected(bundle, ("invalid manifest hash path",
                             "manifest.hashes may contain only"))


def test_rejects_source_outside_sources_dir(bundle: Path):
    """Spec: raw sources are REQUIRED under sources/."""
    stray = bundle / "stray.csv"
    stray.write_text(SOURCE_BODIES[SRC_A])
    manifest = read_manifest(bundle)
    hashes = dict(manifest["hashes"])
    hashes["stray.csv"] = sha256_file(stray)
    set_hashes(bundle, hashes)
    assert_rejected(bundle, "manifest.hashes may contain only")


def test_rejects_schedule_source_outside_sources_dir(bundle: Path):
    rows = current_rows(bundle)
    rows[0][4] = "../evil.csv"
    write_schedule(bundle, rows)
    assert_rejected(bundle, "schedule source is not under sources/")


def test_rejects_directory_as_source(bundle: Path):
    target = bundle / SRC_A
    target.unlink()
    target.mkdir()
    assert_rejected(bundle, "missing manifest-listed file")


# --------------------------------------------------------------------------------------
# D. Declaration integrity — fail CLOSED, never truthy-coerce
# --------------------------------------------------------------------------------------


def test_rejects_pit_grade_false(bundle: Path):
    manifest = read_manifest(bundle)
    manifest["pit_grade"] = False
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "manifest.pit_grade must be literal true")


def test_rejects_missing_pit_grade(bundle: Path):
    manifest = read_manifest(bundle)
    manifest.pop("pit_grade")
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "manifest.pit_grade must be literal true")


@pytest.mark.parametrize("value", ["true", "True", 1, "yes", [], {}])
def test_rejects_non_boolean_pit_grade(bundle: Path, value):
    """`if manifest.get("pit_grade"):` accepts several of these. Only `is True` is right."""
    manifest = read_manifest(bundle)
    manifest["pit_grade"] = value
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "manifest.pit_grade must be literal true")


@pytest.mark.parametrize("value", ["", None, 123, []])
def test_rejects_bad_bundle_id(bundle: Path, value):
    manifest = read_manifest(bundle)
    manifest["bundle_id"] = value
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "manifest.bundle_id must be a non-empty string")


def test_rejects_duplicate_as_of_dates(bundle: Path):
    manifest = read_manifest(bundle)
    manifest["as_of_dates"] = ["2026-01-05", "2026-01-05", "2026-01-06"]
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "must not contain duplicates")


@pytest.mark.parametrize("bad_date", ["20260105", "2026-13-45", "2026-1-5", "2026/01/05"])
def test_rejects_malformed_manifest_as_of_date(bundle: Path, bad_date):
    manifest = read_manifest(bundle)
    manifest["as_of_dates"] = [bad_date, "2026-01-06"]
    write_manifest(bundle, manifest)
    assert_rejected(bundle, "must use ISO YYYY-MM-DD dates")


@pytest.mark.parametrize("bad_date", ["20260105", "2026-13-45", "2026-1-5", "2026/01/05"])
def test_rejects_malformed_schedule_as_of_date(bundle: Path, bad_date):
    rows = current_rows(bundle)
    rows[0][0] = bad_date
    write_schedule(bundle, rows)
    assert_rejected(bundle, ("dates must use ISO YYYY-MM-DD",
                             "schedule date outside manifest.as_of_dates"))


@pytest.mark.parametrize("bad_date", ["20150101", "2015-13-01", "not-a-date"])
def test_rejects_malformed_listing_date(bundle: Path, bad_date):
    rows = current_rows(bundle)
    rows[0][2] = bad_date
    write_schedule(bundle, rows)
    assert_rejected(bundle, "dates must use ISO YYYY-MM-DD")


def test_rejects_malformed_delisted_date(bundle: Path):
    """Non-empty-but-garbage delisting date must not be read as 'never delisted'."""
    rows = current_rows(bundle)
    rows[3][3] = "not-a-date"
    write_schedule(bundle, rows)
    assert_rejected(bundle, "dates must use ISO YYYY-MM-DD")


@pytest.mark.parametrize("field_idx", [0, 1, 2, 4, 5])
def test_rejects_blank_required_field(bundle: Path, field_idx):
    rows = current_rows(bundle)
    rows[0][field_idx] = "   "
    write_schedule(bundle, rows)
    assert_rejected(bundle, ("has blank required fields", "schedule dates do not exactly match"))


def test_rejects_missing_manifest(bundle: Path):
    (bundle / "manifest.json").unlink()
    assert_rejected(bundle, "manifest is missing")


def test_rejects_malformed_manifest_json(bundle: Path):
    (bundle / "manifest.json").write_text("{not json")
    assert_rejected(bundle, "unparseable")


def test_rejects_non_object_manifest(bundle: Path):
    (bundle / "manifest.json").write_text("[1,2,3]")
    assert_rejected(bundle, "must be an object")


def test_rejects_missing_schedule_file(bundle: Path):
    (bundle / "universe_schedule.csv").unlink()
    assert_rejected(bundle, ("missing manifest-listed file", "schedule must not be empty"))


def test_rejects_schedule_missing_required_column(bundle: Path):
    """Dropping `delisted_after` must not be read as 'never delisted'."""
    rows = current_rows(bundle)
    fields = [f for f in SCHEDULE_FIELDS if f != "delisted_after"]
    trimmed = [[r[0], r[1], r[2], r[4], r[5]] for r in rows]
    write_schedule(bundle, trimmed, fields=fields)
    assert_rejected(bundle, "missing required columns")


# --------------------------------------------------------------------------------------
# E. Bundle-root integrity
#
# PROVENANCE: the root-symlink case was RELAYED FROM THE BUILDER mid-cycle, not caught
# independently by this suite. Retained as a regression guard; not counted as an
# independent finding. The sibling root cases are mine.
# --------------------------------------------------------------------------------------


def test_rejects_symlinked_bundle_root(bundle: Path, tmp_path: Path):
    link = tmp_path / "bundle_link"
    os.symlink(bundle, link)
    assert_rejected(link, "must not be a symlink")


def test_rejects_nonexistent_bundle_root(tmp_path: Path):
    assert_rejected(tmp_path / "does_not_exist", ("is missing",))


def test_rejects_file_as_bundle_root(tmp_path: Path):
    target = tmp_path / "not_a_dir"
    target.write_text("{}")
    assert_rejected(target, ("is missing", "must be an object"))


def test_rejects_symlinked_manifest(bundle: Path, tmp_path: Path):
    outside = tmp_path / "evil_manifest.json"
    outside.write_text(json.dumps(read_manifest(bundle)))
    (bundle / "manifest.json").unlink()
    os.symlink(outside, bundle / "manifest.json")
    assert_rejected(bundle, "must not be a symlink")


# --------------------------------------------------------------------------------------
# F. Output contract
# --------------------------------------------------------------------------------------


def test_schedule_returned_sorted_by_date_then_ticker(bundle: Path):
    write_schedule(bundle, list(reversed(current_rows(bundle))))
    result = validate_pit_bundle(bundle)
    keys = [(r["as_of_date"], r["ticker"]) for r in result.schedule]
    assert keys == sorted(keys)


def test_returned_bundle_is_immutable(bundle: Path):
    result = validate_pit_bundle(bundle)
    with pytest.raises(Exception):
        result.pit_grade = False  # type: ignore[misc]


def test_returned_manifest_is_deeply_immutable(bundle: Path):
    """A mutable nested manifest would let a caller edit provenance after validation."""
    result = validate_pit_bundle(bundle)
    with pytest.raises(Exception):
        result.manifest["hashes"]["universe_schedule.csv"] = "0" * 64  # type: ignore[index]


def test_returned_schedule_rows_are_immutable(bundle: Path):
    result = validate_pit_bundle(bundle)
    with pytest.raises(Exception):
        result.schedule[0]["ticker"] = "000001.SZ"  # type: ignore[index]


# --------------------------------------------------------------------------------------
# G. Probe — reports a design question for Task 2, does not assert
# --------------------------------------------------------------------------------------


def test_probe_all_names_immortal_is_the_survivorship_signature(bundle: Path):
    """A schedule where NO name ever delists across every date is the fingerprint of a
    current-universe snapshot relabelled PIT — what Task 1 exists to prevent. Nothing in
    the spec requires detecting it, and a short sample can legitimately look like this,
    so this is an observation, not an assertion."""
    rows = current_rows(bundle)
    for row in rows:
        row[3] = ""
        row[2] = "2015-01-01"
    write_schedule(bundle, rows)
    try:
        validate_pit_bundle(bundle)
        detected = False
    except PitBundleValidationError:
        detected = True
    print(f"\n[PROBE] immortal-universe schedule rejected by validator: {detected}")
