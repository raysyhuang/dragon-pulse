"""Independent adversarial verification for Task 2 (PIT universe schedule builder).

Authored from the Task 2 spec on a separate machine from the builder, exercising the
real CLI entry point. Primary target is F5 from the Task 1 verification: the builder is
the provenance gate, so it must reject fabricated or self-contradictory membership
evidence rather than quietly filtering it.

Every rejection asserts a specific message so a test cannot pass because the fixture was
broken for an unrelated reason.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.build_pit_universe_schedule import (
    BuildError,
    _parse_as_of_dates,
    build_bundle,
    main,
)
from src.core.pit_bundle import PitBundleValidationError, validate_pit_bundle

HEADER = "ts_code,circ_mv,list_date,delist_date"

# Two dated snapshots. 600001 lists exactly on the 2nd date (lower boundary);
# 600002 delists after the 2nd date; 600003 is a large-cap present throughout.
SNAP_A = [
    ("600003.SH", "5000", "2010-01-01", ""),
    ("600000.SH", "3000", "2010-01-01", ""),
    ("600002.SH", "1000", "2010-01-01", "2026-06-01"),
]
SNAP_B = [
    ("600003.SH", "5100", "2010-01-01", ""),
    ("600000.SH", "3100", "2010-01-01", ""),
    ("600001.SH", "2000", "2026-01-06", ""),
]

AS_OF = [("2026-01-05", "20260105"), ("2026-01-06", "20260106")]


def write_snapshot(sources: Path, yyyymmdd: str, rows, header: str = HEADER) -> Path:
    path = sources / f"daily_basic_{yyyymmdd}.csv"
    body = header + "\n" + "".join(",".join(r) + "\n" for r in rows)
    path.write_text(body)
    return path


@pytest.fixture
def sources(tmp_path: Path) -> Path:
    d = tmp_path / "snapshots"
    d.mkdir()
    write_snapshot(d, "20260105", SNAP_A)
    write_snapshot(d, "20260106", SNAP_B)
    return d


def run_build(sources: Path, out: Path, *, n: int = 2, label: str = "tushare_daily_basic",
              dates: str = "2026-01-05,2026-01-06") -> int:
    """Call the library entry point directly.

    The CLI wraps BuildError in parser.error() -> SystemExit(2), which discards the typed
    exception. Asserting on the library boundary keeps the failure reason inspectable;
    the CLI wrapping is covered separately below.
    """
    build_bundle(sources, out, _parse_as_of_dates(dates), n, label)
    return 0


def run_cli(sources: Path, out: Path, *, n: int = 2, label: str = "tushare_daily_basic",
            dates: str = "2026-01-05,2026-01-06") -> int:
    return main([
        "--sources-dir", str(sources), "--output", str(out),
        "--as-of-dates", dates, "--universe-n", str(n), "--source-label", label,
    ])


def assert_build_rejected(sources: Path, out: Path, expect: str | tuple[str, ...], **kw):
    """Rejection must be the typed BuildError and cite the intended reason."""
    with pytest.raises(BuildError) as exc:
        run_build(sources, out, **kw)
    message = str(exc.value)
    wanted = (expect,) if isinstance(expect, str) else expect
    assert any(w.lower() in message.lower() for w in wanted), (
        f"rejected, but not for the intended reason.\n  wanted one of: {wanted}\n  got: {message}"
    )
    return message


def read_schedule(bundle: Path) -> list[dict[str, str]]:
    import csv
    with (bundle / "universe_schedule.csv").open() as fh:
        return list(csv.DictReader(fh))


# --------------------------------------------------------------------------------------
# Control + the end-to-end handoff that actually matters
# --------------------------------------------------------------------------------------


def test_control_builds_and_output_passes_task1_validator(sources: Path, tmp_path: Path):
    """The whole point of Task 2: its output must be accepted by the verified validator."""
    out = tmp_path / "bundle"
    assert run_build(sources, out) == 0
    bundle = validate_pit_bundle(out)
    assert bundle.pit_grade is True
    assert bundle.as_of_dates == ("2026-01-05", "2026-01-06")
    assert len(bundle.schedule) == 4


def test_cli_returns_zero_and_wraps_errors(sources: Path, tmp_path: Path):
    """CLI contract: success -> 0; BuildError -> SystemExit(2) via parser.error."""
    assert run_cli(sources, tmp_path / "ok") == 0
    write_snapshot(sources, "20260105", [*SNAP_A, ("600009.SH", "9000", "2026-03-01", "")])
    with pytest.raises(SystemExit) as exc:
        run_cli(sources, tmp_path / "bad")
    assert exc.value.code == 2


def test_sources_copied_byte_identical(sources: Path, tmp_path: Path):
    out = tmp_path / "bundle"
    run_build(sources, out)
    for snap in sources.glob("*.csv"):
        copied = out / "sources" / snap.name
        assert copied.exists(), f"source not preserved: {snap.name}"
        assert hashlib.sha256(copied.read_bytes()).hexdigest() == \
               hashlib.sha256(snap.read_bytes()).hexdigest()


def test_build_is_byte_deterministic(sources: Path, tmp_path: Path):
    """Same inputs must yield the same composite hash, or provenance is not reproducible."""
    a, b = tmp_path / "a", tmp_path / "b"
    run_build(sources, a)
    run_build(sources, b)
    ma = json.loads((a / "manifest.json").read_text())
    mb = json.loads((b / "manifest.json").read_text())
    assert ma["composite_sha256"] == mb["composite_sha256"]
    assert (a / "universe_schedule.csv").read_bytes() == (b / "universe_schedule.csv").read_bytes()


def test_manifest_records_provenance(sources: Path, tmp_path: Path):
    out = tmp_path / "bundle"
    run_build(sources, out, label="tushare_daily_basic")
    manifest = json.loads((out / "manifest.json").read_text())
    blob = json.dumps(manifest).lower()
    assert "tushare_daily_basic" in blob, "source label not recorded"
    assert manifest.get("bundle_id"), "bundle_id missing"
    assert manifest.get("pit_grade") is True


# --------------------------------------------------------------------------------------
# A. F5 — membership evidence must be real, and contradictions must FAIL not filter
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("drop", ["list_date", "delist_date", "circ_mv", "ts_code"])
def test_rejects_snapshot_missing_required_column(sources: Path, tmp_path: Path, drop):
    cols = [c for c in HEADER.split(",") if c != drop]
    rows = [tuple(v for c, v in zip(HEADER.split(","), r) if c != drop) for r in SNAP_A]
    write_snapshot(sources, "20260105", rows, header=",".join(cols))
    assert_build_rejected(sources, tmp_path / "b", "required columns")


def test_rejects_blank_list_date(sources: Path, tmp_path: Path):
    """A blank listing date is absent evidence; it must never mean 'listed forever'."""
    rows = [("600003.SH", "5000", "", ""), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("list_date", "listing"))


def test_rejects_row_listed_after_as_of_rather_than_filtering(sources: Path, tmp_path: Path):
    """Spec: contradictory membership FAILS the build. Silent filtering would let a
    current-universe snapshot be laundered into a historical bundle."""
    rows = [*SNAP_A, ("600009.SH", "9000", "2026-03-01", "")]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("eligibility contradiction", "not listed"))


def test_rejects_row_already_delisted_rather_than_filtering(sources: Path, tmp_path: Path):
    rows = [*SNAP_A, ("600008.SH", "8000", "2010-01-01", "2025-01-01")]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("eligibility contradiction", "delist"))


def test_boundary_list_date_equal_as_of_is_eligible(sources: Path, tmp_path: Path):
    """`list_date <= as_of` — the IPO session itself counts."""
    out = tmp_path / "bundle"
    assert run_build(sources, out, n=3) == 0
    rows = [r for r in read_schedule(out) if r["as_of_date"] == "2026-01-06"]
    assert "600001.SH" in {r["ticker"] for r in rows}, "name listing on the as_of date was dropped"


def test_boundary_delist_date_equal_as_of_is_ineligible(sources: Path, tmp_path: Path):
    """`as_of < delist_date` — equality means already gone."""
    rows = [("600003.SH", "5000", "2010-01-01", "2026-01-05"), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("eligibility contradiction", "delist"))


def test_blank_delist_date_is_accepted_for_live_names(sources: Path, tmp_path: Path):
    out = tmp_path / "bundle"
    assert run_build(sources, out) == 0
    assert all(r["delisted_after"] == "" for r in read_schedule(out)
               if r["ticker"] == "600000.SH")


@pytest.mark.parametrize("bad", ["20100101", "2010-13-45", "2010-1-1", "not-a-date", "2010/01/01"])
def test_rejects_malformed_list_date(sources: Path, tmp_path: Path, bad):
    rows = [("600003.SH", "5000", bad, ""), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("list_date", "date"))


@pytest.mark.parametrize("bad", ["20260601", "2026-13-45", "not-a-date"])
def test_rejects_malformed_delist_date(sources: Path, tmp_path: Path, bad):
    rows = [("600003.SH", "5000", "2010-01-01", bad), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("delist_date", "date"))


def test_rejects_delist_before_list(sources: Path, tmp_path: Path):
    """Incoherent lifecycle: delisted before it ever listed."""
    rows = [("600003.SH", "5000", "2020-01-01", "2015-01-01"), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("delist", "list", "membership"))


def test_rejects_membership_contradiction_on_second_date_only(sources: Path, tmp_path: Path):
    """A defect on any requested date must fail the whole build, not just that date."""
    rows = [*SNAP_B, ("600007.SH", "7000", "2027-01-01", "")]
    write_snapshot(sources, "20260106", rows)
    assert_build_rejected(sources, tmp_path / "b", ("eligibility contradiction", "not listed"))


# --------------------------------------------------------------------------------------
# B. Selection determinism
# --------------------------------------------------------------------------------------


def test_ranks_by_circ_mv_descending(sources: Path, tmp_path: Path):
    out = tmp_path / "bundle"
    run_build(sources, out, n=2)
    day1 = {r["ticker"] for r in read_schedule(out) if r["as_of_date"] == "2026-01-05"}
    assert day1 == {"600003.SH", "600000.SH"}, f"wrong top-2 by circ_mv: {day1}"


def test_ties_broken_by_ticker_ascending(sources: Path, tmp_path: Path):
    """Equal caps must resolve deterministically or bundles are not reproducible."""
    rows = [("600005.SH", "3000", "2010-01-01", ""),
            ("600004.SH", "3000", "2010-01-01", ""),
            ("600006.SH", "1000", "2010-01-01", "")]
    write_snapshot(sources, "20260105", rows)
    out = tmp_path / "bundle"
    run_build(sources, out, n=1)
    day1 = [r["ticker"] for r in read_schedule(out) if r["as_of_date"] == "2026-01-05"]
    assert day1 == ["600004.SH"], f"tie not broken by ascending ticker: {day1}"


def test_rejects_duplicate_ticker_in_snapshot(sources: Path, tmp_path: Path):
    rows = [*SNAP_A, ("600000.SH", "999", "2010-01-01", "")]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("duplicate", "ts_code"))


@pytest.mark.parametrize("bad_cap", ["abc", "", "-100", "nan", "inf"])
def test_rejects_invalid_circ_mv(sources: Path, tmp_path: Path, bad_cap):
    rows = [("600003.SH", bad_cap, "2010-01-01", ""), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("circ_mv", "cap", "value"))


@pytest.mark.parametrize("bad_ticker", ["", "   ", "600000", "600000.XX", "../evil"])
def test_rejects_malformed_ticker(sources: Path, tmp_path: Path, bad_ticker):
    rows = [(bad_ticker, "5000", "2010-01-01", ""), *SNAP_A[1:]]
    write_snapshot(sources, "20260105", rows)
    assert_build_rejected(sources, tmp_path / "b", ("ticker", "ts_code"))


def test_rejects_empty_snapshot(sources: Path, tmp_path: Path):
    write_snapshot(sources, "20260105", [])
    assert_build_rejected(sources, tmp_path / "b", ("empty", "no eligible", "no rows"))


def test_rejects_universe_n_larger_than_available(sources: Path, tmp_path: Path):
    """Silently returning a short cross-section would misstate the universe size."""
    assert_build_rejected(sources, tmp_path / "b", ("universe_n", "eligible", "fewer"), n=99)


@pytest.mark.parametrize("bad_n", ["0", "-1", "abc"])
def test_rejects_invalid_universe_n(sources: Path, tmp_path: Path, bad_n):
    with pytest.raises((SystemExit, BuildError, ValueError)):
        main(["--sources-dir", str(sources), "--output", str(tmp_path / "b"),
              "--as-of-dates", "2026-01-05", "--universe-n", bad_n,
              "--source-label", "x"])


# --------------------------------------------------------------------------------------
# C. Source-set integrity
# --------------------------------------------------------------------------------------


def test_rejects_missing_snapshot_for_requested_date(sources: Path, tmp_path: Path):
    (sources / "daily_basic_20260106.csv").unlink()
    assert_build_rejected(sources, tmp_path / "b", ("missing", "not found", "snapshot"))


@pytest.mark.parametrize("bad_dates", ["20260105", "2026-13-45", "2026-1-5", ""])
def test_rejects_malformed_as_of_dates(sources: Path, tmp_path: Path, bad_dates):
    with pytest.raises((BuildError, SystemExit, ValueError)):
        run_build(sources, tmp_path / "b", dates=bad_dates)


def test_rejects_duplicate_as_of_dates(sources: Path, tmp_path: Path):
    assert_build_rejected(sources, tmp_path / "b", ("duplicate", "date"),
                          dates="2026-01-05,2026-01-05")


def test_unrequested_snapshots_are_not_bundled(sources: Path, tmp_path: Path):
    """An unrequested file must not silently enter the evidence set."""
    write_snapshot(sources, "20260107", SNAP_A)
    out = tmp_path / "bundle"
    rc = run_build(sources, out, dates="2026-01-05,2026-01-06")
    if rc == 0:
        assert not (out / "sources" / "daily_basic_20260107.csv").exists(), \
            "unrequested snapshot was copied into the bundle"
        validate_pit_bundle(out)


# --------------------------------------------------------------------------------------
# D. Atomicity — a failed build must leave nothing usable behind
# --------------------------------------------------------------------------------------


def test_failed_build_leaves_no_partial_output(sources: Path, tmp_path: Path):
    out = tmp_path / "bundle"
    rows = [*SNAP_B, ("600007.SH", "7000", "2027-01-01", "")]
    write_snapshot(sources, "20260106", rows)
    with pytest.raises(BuildError):
        run_build(sources, out)
    assert not out.exists(), "partial bundle left behind after a failed build"


def test_write_phase_failure_leaves_no_temp_directory(sources: Path, tmp_path: Path,
                                                      monkeypatch: pytest.MonkeyPatch):
    """The rmtree in the except block is load-bearing but was untested by both suites.

    All validation runs before the temp dir is created, so only a write-phase failure
    (disk full, permissions, interrupt) reaches it. Injecting one proves the staging
    directory is cleaned up rather than leaked into the output parent.
    """
    out_parent = tmp_path / "out"
    out_parent.mkdir()
    out = out_parent / "bundle"

    real_write_text = Path.write_text

    def explode(self: Path, *args, **kwargs):
        if self.name == "manifest.json":
            raise RuntimeError("injected write-phase failure")
        return real_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", explode)
    with pytest.raises(RuntimeError):
        run_build(sources, out)
    monkeypatch.undo()

    assert not out.exists(), "partial bundle left at output path"
    assert list(out_parent.iterdir()) == [], \
        f"staging directory leaked: {[p.name for p in out_parent.iterdir()]}"


def test_refuses_to_overwrite_existing_output(sources: Path, tmp_path: Path):
    out = tmp_path / "bundle"
    out.mkdir()
    (out / "sentinel.txt").write_text("pre-existing")
    assert_build_rejected(sources, out, ("exists", "output"))
    assert (out / "sentinel.txt").read_text() == "pre-existing"


def test_partial_output_is_not_validator_acceptable(sources: Path, tmp_path: Path):
    """Belt and braces: even if a partial dir survived, it must not validate."""
    out = tmp_path / "bundle"
    write_snapshot(sources, "20260106", [*SNAP_B, ("600007.SH", "7000", "2027-01-01", "")])
    with pytest.raises(BuildError):
        run_build(sources, out)
    if out.exists():
        with pytest.raises(PitBundleValidationError):
            validate_pit_bundle(out)


# --------------------------------------------------------------------------------------
# E. F5 residual probe — reports, does not assert
# --------------------------------------------------------------------------------------


def test_probe_wellformed_fabricated_snapshot_still_builds(tmp_path: Path):
    """The residual hole after Task 2: a snapshot with plausible but INVENTED list_dates
    and blank delist_dates is internally consistent, so it builds and validates. Task 2
    closes fabrication that is self-contradictory; it cannot close fabrication that is
    coherent. That risk now sits at upstream capture provenance, which the spec names
    explicitly. Reported so the boundary stays visible, not asserted as a defect."""
    src = tmp_path / "snap"
    src.mkdir()
    write_snapshot(src, "20260105", [("600000.SH", "3000", "2010-01-01", ""),
                                     ("600003.SH", "5000", "2010-01-01", "")])
    out = tmp_path / "bundle"
    try:
        run_build(src, out, dates="2026-01-05")
        validate_pit_bundle(out)
        built = True
    except (BuildError, PitBundleValidationError):
        built = False
    print(f"\n[PROBE] coherent-but-fabricated snapshot builds+validates: {built}")
