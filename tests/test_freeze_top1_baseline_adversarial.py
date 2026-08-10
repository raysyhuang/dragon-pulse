"""Independent adversarial verification for Task 5 (top-1 baseline evidence freezer).

Task 5's job is to inventory saved evidence WITHOUT reconstructing anything. The two
failure modes that matter are therefore (a) inventing evidence that was never saved, and
(b) an artifact that reads as stronger than "the tracker once claimed this".

Written on a separate machine from the builder, driving the real CLI entry point.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "freeze_top1_baseline.py"
AS_OF = "2026-08-09"
OUT_NAME = f"baseline_inventory_{AS_OF}.json"

FORBIDDEN_CLAIMS = (
    "EXECUTION_VERIFIED", "EXECUTION_PROVEN", "PIT_VERIFIED", "PIT_CAPTURE_VERIFIED",
    "ALPHA", "PROMOTABLE", "PRODUCTION_READY", "VALIDATED_PERFORMANCE",
)


def leg(rank=0, ticker="600498.SH", filled=True, reason=None, ret=1.5) -> dict:
    row = {"rank": rank, "ticker": ticker, "entry_date": "2026-05-06",
           "exit_date": "2026-05-08", "ret_pct": ret, "filled": filled}
    if reason is not None:
        row["reason"] = reason
    return row


def ledger_row(scan_date="2026-05-04", *, regime="bull", evaluated=True,
               legs=None, artifact="2026-05-04/top1_paper_watchlist_2026-05-04.json",
               ticker="600498.SH") -> dict:
    pick = {"ticker": ticker, "name_cn": "X", "score": 88.0, "entry_price": 10.0,
            "max_entry_price": 10.3, "stop_loss": 9.0, "target_1": 11.0,
            "holding_period": 3}
    return {
        "scan_date": scan_date, "regime": regime, "n_live_picks": 1,
        "top1": pick, "top2": [pick], "evaluated": evaluated,
        "results": {"legs": legs if legs is not None else [leg(ticker=ticker)]} if evaluated else None,
        "native_artifact_path": artifact,
    }


def artifact_payload(row: dict) -> dict:
    """The artifact must bind to its ledger row on every identity field."""
    return {"date": row["scan_date"], "sleeve": "top1_paper", "paper_only": True,
            "status": "PAPER_TRACK_ONLY", "regime": row["regime"],
            "top1": row["top1"], "top2": row["top2"]}


def build_inputs(tmp: Path, rows=None, *, write_artifacts=True) -> tuple[Path, Path]:
    rows = rows if rows is not None else [ledger_row()]
    root = tmp / "outputs"
    root.mkdir(parents=True, exist_ok=True)
    ledger = tmp / "ledger.jsonl"
    ledger.write_text("".join(json.dumps(r) + "\n" for r in rows))
    if write_artifacts:
        for r in rows:
            rel = r.get("native_artifact_path")
            if not isinstance(rel, str):
                continue
            dest = root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(json.dumps(artifact_payload(r)))
    return ledger, root


def freeze(ledger: Path, root: Path, out_dir: Path, *, extra=()) -> subprocess.CompletedProcess:
    out_dir.mkdir(parents=True, exist_ok=True)
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--ledger", str(ledger), "--artifact-root", str(root),
         "--output", str(out_dir / OUT_NAME), "--as-of", AS_OF, *extra],
        capture_output=True, text=True, cwd=REPO,
    )


def load(out_dir: Path) -> dict:
    return json.loads((out_dir / OUT_NAME).read_text())


def assert_failed(proc: subprocess.CompletedProcess, expect: str | tuple[str, ...]):
    assert proc.returncode != 0, f"expected failure, got success:\n{proc.stdout}"
    blob = (proc.stderr + proc.stdout).lower()
    wanted = (expect,) if isinstance(expect, str) else expect
    assert any(w.lower() in blob for w in wanted), (
        f"failed, but not for the intended reason.\n  wanted one of: {wanted}\n  got: {proc.stderr[-400:]}"
    )


def _values(node):
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [v for x in node.values() for v in _values(x)]
    if isinstance(node, list):
        return [v for x in node for v in _values(x)]
    return []


# --------------------------------------------------------------------------------------
# A. No reconstruction — the single most important property
# --------------------------------------------------------------------------------------


def test_A_missing_artifact_is_recorded_never_invented(tmp_path: Path):
    ledger, root = build_inputs(tmp_path, [ledger_row()], write_artifacts=False)
    proc = freeze(ledger, root, tmp_path / "o")
    assert proc.returncode == 0, proc.stderr
    doc = load(tmp_path / "o")
    row = doc["rows"][0]
    assert row["status"] == "MISSING_NATIVE_ARTIFACT"
    assert row["resolvable"] is False
    assert row.get("artifact_sha256") in (None, "")
    assert doc["summary"]["ledger_rows"] == 1


def test_A_partial_evidence_keeps_every_row(tmp_path: Path):
    """A resolvable row must not mask an unresolvable one, or coverage is overstated.

    This is the realistic missing-evidence case: the artifact FILE is gone while its
    date directory remains - distinct from the whole directory being absent, which was
    always handled. Was M1; the strict xfail marker was removed once repaired.
    """
    rows = [ledger_row("2026-05-04"),
            ledger_row("2026-05-05", artifact="2026-05-05/top1_paper_watchlist_2026-05-05.json")]
    ledger, root = build_inputs(tmp_path, rows)
    (root / "2026-05-05" / "top1_paper_watchlist_2026-05-05.json").unlink()
    proc = freeze(ledger, root, tmp_path / "o")
    assert proc.returncode == 0, proc.stderr
    doc = load(tmp_path / "o")
    statuses = sorted(r["status"] for r in doc["rows"])
    assert statuses == ["MISSING_NATIVE_ARTIFACT", "RESOLVED_NATIVE_ARTIFACT"]
    assert doc["summary"]["ledger_rows"] == 2


def test_A_missing_file_and_missing_directory_agree(tmp_path: Path):
    """The two shapes of absence must produce the same status. They diverged under M1,
    so this pins them together rather than testing either alone."""
    import shutil
    outcomes = {}
    for label, remove in (("file", lambda r: (r / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json").unlink()),
                          ("directory", lambda r: shutil.rmtree(r / "2026-05-04"))):
        work = tmp_path / label
        work.mkdir()
        ledger, root = build_inputs(work, [ledger_row()])
        remove(root)
        proc = freeze(ledger, root, work / "o")
        assert proc.returncode == 0, f"{label}: {proc.stderr}"
        outcomes[label] = load(work / "o")["rows"][0]["status"]
    assert outcomes["file"] == outcomes["directory"] == "MISSING_NATIVE_ARTIFACT", outcomes


def test_A_inventory_row_count_equals_ledger_lines(tmp_path: Path):
    rows = [ledger_row(f"2026-05-0{i}", artifact=f"a{i}/w.json") for i in (4, 5, 6)]
    ledger, root = build_inputs(tmp_path, rows)
    proc = freeze(ledger, root, tmp_path / "o")
    assert proc.returncode == 0, proc.stderr
    doc = load(tmp_path / "o")
    assert len(doc["rows"]) == doc["summary"]["ledger_rows"] == 3


# --------------------------------------------------------------------------------------
# B. Read-only — a freezer that mutates its evidence is not a freezer
# --------------------------------------------------------------------------------------


def test_B_ledger_and_artifacts_are_untouched(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    before_ledger = ledger.read_bytes()
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    proc = freeze(ledger, root, tmp_path / "o")
    assert proc.returncode == 0, proc.stderr
    assert ledger.read_bytes() == before_ledger, "the freezer modified the ledger"
    after = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert after == before, "the freezer modified saved artifacts"


def test_B_nothing_written_into_the_artifact_root(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    before = {p.relative_to(root) for p in root.rglob("*")}
    freeze(ledger, root, tmp_path / "o")
    after = {p.relative_to(root) for p in root.rglob("*")}
    assert after == before, f"new paths appeared under the artifact root: {after - before}"


# --------------------------------------------------------------------------------------
# C. Determinism and non-overwrite
# --------------------------------------------------------------------------------------


def test_C_same_machine_runs_are_byte_identical(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    a, b = tmp_path / "a", tmp_path / "b"
    assert freeze(ledger, root, a).returncode == 0
    assert freeze(ledger, root, b).returncode == 0
    assert (a / OUT_NAME).read_bytes() == (b / OUT_NAME).read_bytes()
    assert load(a)["manifest_sha256"] == load(b)["manifest_sha256"]


def test_C_manifest_hash_is_portable_across_absolute_paths(tmp_path: Path):
    """L1: identical evidence at a different absolute location must hash identically.

    This is the whole point of a portable content identity - a third party on another
    machine has to be able to confirm the artifact was not altered.
    """
    digests = []
    for label in ("here", "elsewhere/deeper/still"):
        work = tmp_path / label
        work.mkdir(parents=True)
        ledger, root = build_inputs(work, [ledger_row()])
        proc = freeze(ledger, root, work / "o")
        assert proc.returncode == 0, proc.stderr
        digests.append(load(work / "o")["manifest_sha256"])
    assert digests[0] == digests[1], (
        "manifest hash still varies with absolute path; it cannot serve as portable "
        "tamper evidence"
    )


def test_C_manifest_hash_still_binds_substantive_evidence(tmp_path: Path):
    """The danger in excluding fields from a hash is excluding too much. Every change
    below is substantive and MUST move the hash, or the exclusion has created a hole."""
    def digest_for(rows, mutate=None):
        work = tmp_path / f"case{len(list(tmp_path.iterdir()))}"
        work.mkdir()
        ledger, root = build_inputs(work, rows)
        if mutate:
            mutate(root)
        proc = freeze(ledger, root, work / "o")
        assert proc.returncode == 0, proc.stderr
        return load(work / "o")["manifest_sha256"]

    baseline = digest_for([ledger_row()])

    # 1. artifact CONTENT changes (identity fields still agree with the ledger)
    def touch_artifact(root):
        target = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
        payload = json.loads(target.read_text())
        payload["extra_field"] = "changed"
        target.write_text(json.dumps(payload))
    assert digest_for([ledger_row()], touch_artifact) != baseline, \
        "artifact content is not covered by the manifest hash"

    # 2. a different regime recorded in the ledger (and artifact)
    assert digest_for([ledger_row(regime="choppy")]) != baseline, \
        "ledger content is not covered by the manifest hash"

    # 3. a row becoming unresolvable
    assert digest_for([ledger_row()], lambda r: (r / "2026-05-04" /
                      "top1_paper_watchlist_2026-05-04.json").unlink()) != baseline, \
        "artifact resolvability is not covered by the manifest hash"

    # 4. an additional ledger row
    assert digest_for([ledger_row("2026-05-04"),
                       ledger_row("2026-05-05", artifact="2026-05-05/w.json")]) != baseline, \
        "row count is not covered by the manifest hash"


def test_C_excluded_provenance_is_retained_and_declared(tmp_path: Path):
    """Excluded metadata must still be PRESENT in the artifact and identifiable as
    non-hash-covered. Silently dropping it would trade one opacity for another."""
    ledger, root = build_inputs(tmp_path)
    freeze(ledger, root, tmp_path / "o")
    doc = load(tmp_path / "o")
    assert "generator" in doc, "advisory generator provenance was dropped entirely"
    assert doc["generator"].get("git", {}).get("advisory_only") is True
    blob = json.dumps(doc)
    assert "artifact_root" in blob or "root" in blob, \
        "absolute-root provenance was dropped rather than retained as advisory"


def test_C_existing_output_is_not_overwritten(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    out = tmp_path / "o"
    assert freeze(ledger, root, out).returncode == 0
    original = (out / OUT_NAME).read_bytes()
    proc = freeze(ledger, root, out)
    assert proc.returncode != 0, "a second run overwrote the published inventory"
    assert (out / OUT_NAME).read_bytes() == original


# --------------------------------------------------------------------------------------
# D. Labelling — the inventory must not read as performance evidence
# --------------------------------------------------------------------------------------


def test_D_evidence_and_promotion_labels_are_literal(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    freeze(ledger, root, tmp_path / "o")
    doc = load(tmp_path / "o")
    assert doc["evidence_grade"] == "LEGACY_NON_PIT_BASELINE_INVENTORY"
    assert doc["promotion_status"] == "NON_EXECUTION_NON_PROMOTABLE"
    assert doc["capture_provenance_caveat"]


def test_D_no_output_value_overstates(tmp_path: Path):
    """Scan for unqualified claims only.

    Substring scanning repeatedly produces false positives here: the HONEST caveat text
    contains NON-PROMOTABLE and NON-EXECUTION, which embed the very words being banned.
    A negated occurrence is the correct output, so only bare claims count.
    """
    import re
    ledger, root = build_inputs(tmp_path)
    freeze(ledger, root, tmp_path / "o")
    values = [v.upper() for v in _values(load(tmp_path / "o"))]
    for claim in FORBIDDEN_CLAIMS:
        pattern = re.compile(r"(?<!NON-)(?<!NON_)(?<!NOT )" + re.escape(claim))
        offenders = [v for v in values if pattern.search(v)]
        assert not offenders, f"inventory value asserts an unqualified {claim}: {offenders}"


def test_D_accounting_is_frozen_tracker_claim_not_recomputation(tmp_path: Path):
    """The freezer must copy the tracker's own filled/no-fill labels, not re-derive them.
    A row the tracker called filled stays filled even with an implausible return."""
    rows = [ledger_row(legs=[leg(filled=True, ret=999.0)])]
    ledger, root = build_inputs(tmp_path, rows)
    freeze(ledger, root, tmp_path / "o")
    acct = load(tmp_path / "o")["summary"]["accounting"]
    assert acct["filled"] == 1 and acct["no_fill"] == 0


# --------------------------------------------------------------------------------------
# E. Accounting reconciliation
# --------------------------------------------------------------------------------------


def test_E_accounting_partitions_the_ledger(tmp_path: Path):
    rows = [
        ledger_row("2026-05-04", legs=[leg(filled=True)], artifact="a/1.json"),
        ledger_row("2026-05-05", legs=[leg(filled=False, reason="no_fill_chase")], artifact="a/2.json"),
        ledger_row("2026-05-06", legs=[leg(filled=False, reason="censored_missing_exit")], artifact="a/3.json"),
        ledger_row("2026-05-07", evaluated=False, artifact="a/4.json"),
    ]
    ledger, root = build_inputs(tmp_path, rows)
    proc = freeze(ledger, root, tmp_path / "o")
    assert proc.returncode == 0, proc.stderr
    a = load(tmp_path / "o")["summary"]["accounting"]
    assert a["selected"] == 4
    assert a["filled"] + a["no_fill"] + a["censored"] + a["unknown"] == a["selected"]
    assert a["filled"] == 1 and a["no_fill"] == 1 and a["censored"] == 1 and a["unknown"] == 1


def test_E_probe_censored_is_absent_from_legacy_vocabulary(tmp_path: Path, capsys):
    """The committed inventory reports censored=0. That must be read as 'the legacy
    tracker had no censoring concept', not as 'no data was missing'. Confirm a censored
    reason is representable at all, so zero is a real observation rather than a
    structural impossibility."""
    rows = [ledger_row(legs=[leg(filled=False, reason="censored_missing_entry")])]
    ledger, root = build_inputs(tmp_path, rows)
    freeze(ledger, root, tmp_path / "o")
    a = load(tmp_path / "o")["summary"]["accounting"]
    with capsys.disabled():
        print(f"\n[PROBE] censored representable in inventory schema: {a['censored'] == 1} -> {a}")


# --------------------------------------------------------------------------------------
# F. Path containment — FD-anchored O_NOFOLLOW must hold on every declared path
# --------------------------------------------------------------------------------------


def test_F_symlinked_ledger_rejected(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    real = tmp_path / "real_ledger.jsonl"
    ledger.rename(real)
    os.symlink(real, ledger)
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("symlink", "regular", "non-symlink"))


def test_F_symlinked_artifact_is_not_resolved_as_native(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    target = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    outside = tmp_path / "outside.json"
    outside.write_text(target.read_text())
    target.unlink()
    os.symlink(outside, target)
    proc = freeze(ledger, root, tmp_path / "o")
    if proc.returncode == 0:
        row = load(tmp_path / "o")["rows"][0]
        assert row["status"] != "RESOLVED_NATIVE_ARTIFACT", (
            "a symlinked artifact was accepted as native saved evidence"
        )


def test_F_symlinked_artifact_root_rejected(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    linked = tmp_path / "root_link"
    os.symlink(root, linked)
    assert_failed(freeze(ledger, linked, tmp_path / "o"), ("symlink", "declared path"))


def test_F_symlinked_intermediate_directory_rejected(tmp_path: Path):
    """Directory-level O_NOFOLLOW: a symlinked date directory between the root and the
    artifact must not be traversed. Found by mutation testing - the earlier version of
    this test tolerated success and so could not detect the guard being removed."""
    import shutil
    ledger, root = build_inputs(tmp_path)
    real = tmp_path / "real_date"
    shutil.move(str(root / "2026-05-04"), str(real))
    os.symlink(real, root / "2026-05-04")
    proc = freeze(ledger, root, tmp_path / "o")
    assert proc.returncode != 0, "a symlinked intermediate directory was traversed"
    if (tmp_path / "o" / OUT_NAME).exists():
        assert load(tmp_path / "o")["rows"][0]["status"] != "RESOLVED_NATIVE_ARTIFACT"


@pytest.mark.parametrize("bad", ["../escape.json", "/etc/passwd", "a/../../escape.json"])
def test_F_traversal_artifact_path_never_resolves(tmp_path: Path, bad):
    rows = [ledger_row(artifact=bad)]
    ledger, root = build_inputs(tmp_path, rows, write_artifacts=False)
    (tmp_path / "escape.json").write_text(json.dumps(artifact_payload(rows[0])))
    proc = freeze(ledger, root, tmp_path / "o")
    if proc.returncode == 0:
        assert load(tmp_path / "o")["rows"][0]["status"] != "RESOLVED_NATIVE_ARTIFACT"


# --------------------------------------------------------------------------------------
# G. Malformed ledger input
# --------------------------------------------------------------------------------------


def test_G_non_json_line_rejected(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    ledger.write_text(ledger.read_text() + "{not json\n")
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("line", "json", "object"))


def test_G_missing_required_fields_rejected(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    ledger.write_text(json.dumps({"scan_date": "2026-05-04"}) + "\n")
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("required", "tracker fields", "lacks"))


def test_G_evaluated_row_without_results_rejected(tmp_path: Path):
    row = ledger_row()
    row["results"] = None
    ledger, root = build_inputs(tmp_path, [row])
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("results", "legs", "evaluated"))


def test_G_unevaluated_row_with_results_rejected(tmp_path: Path):
    row = ledger_row(evaluated=False)
    row["results"] = {"legs": [leg()]}
    ledger, root = build_inputs(tmp_path, [row])
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("results", "unevaluated", "null"))


@pytest.mark.parametrize("bad_date", ["20260504", "2026-13-45", "2026-5-4", "", None])
def test_G_invalid_scan_date_rejected(tmp_path: Path, bad_date):
    row = ledger_row()
    row["scan_date"] = bad_date
    ledger, root = build_inputs(tmp_path, [row])
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("yyyy-mm-dd", "date", "tracker fields"))


def test_G_leg_not_bound_to_top1_rejected(tmp_path: Path):
    """A rank-0 leg naming a different ticker than top1 is a corrupt record, and
    accepting it would silently attribute one name's outcome to another."""
    rows = [ledger_row(ticker="600498.SH", legs=[leg(rank=0, ticker="000001.SZ")])]
    ledger, root = build_inputs(tmp_path, rows)
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("rank-0", "bind", "top1"))


def test_G_artifact_not_matching_its_ledger_row_is_rejected(tmp_path: Path):
    """Identity binding: an artifact whose picks differ from the ledger row must not be
    accepted as that row's evidence. Accepting it would attribute saved evidence to a
    record it does not describe."""
    ledger, root = build_inputs(tmp_path)
    target = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    payload = json.loads(target.read_text())
    payload["top1"] = dict(payload["top1"], ticker="000001.SZ")
    target.write_text(json.dumps(payload))
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("identity", "incompatible"))


def test_G_artifact_regime_disagreement_is_rejected(tmp_path: Path):
    ledger, root = build_inputs(tmp_path)
    target = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    payload = json.loads(target.read_text())
    payload["regime"] = "bear"
    target.write_text(json.dumps(payload))
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("identity", "incompatible"))


def test_G_artifact_not_marked_paper_only_is_rejected(tmp_path: Path):
    """paper_only=True is what keeps this evidence from reading as live execution."""
    ledger, root = build_inputs(tmp_path)
    target = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    payload = json.loads(target.read_text())
    payload["paper_only"] = False
    target.write_text(json.dumps(payload))
    assert_failed(freeze(ledger, root, tmp_path / "o"), ("identity", "incompatible"))


# --------------------------------------------------------------------------------------
# H. Torn-read detection (unit level)
#
# Added after mutation testing: removing the "changed while being read" guard was
# undetected by both suites, because a genuine race cannot be provoked from the CLI.
# --------------------------------------------------------------------------------------


def _load_freezer_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location("freeze_top1_baseline", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_H_content_changing_during_read_is_rejected(tmp_path: Path, monkeypatch):
    """Simulate the file being swapped mid-read by making the post-read stat identity
    differ. Without the guard the freezer would hash bytes it never validated."""
    mod = _load_freezer_module()
    target = tmp_path / "evidence.json"
    target.write_text(json.dumps({"a": 1}))

    handle = mod._open_absolute_regular(target)
    real_fstat = os.fstat
    calls = {"n": 0}

    class _Shifted:
        """A stat result whose identity differs from the original."""
        def __init__(self, base):
            self.st_dev, self.st_ino = base.st_dev, base.st_ino + 1
            self.st_mode, self.st_size, self.st_mtime_ns = base.st_mode, base.st_size, 0
            self.st_nlink = base.st_nlink

    def shifting_fstat(fd):
        base = real_fstat(fd)
        calls["n"] += 1
        return _Shifted(base) if calls["n"] > 1 else base

    monkeypatch.setattr(os, "fstat", shifting_fstat)
    try:
        with pytest.raises(mod.BaselineInventoryError) as exc:
            handle.read_stable_bytes("native artifact")
        assert "changed while being read" in str(exc.value)
    finally:
        monkeypatch.undo()
        handle.close()


# --------------------------------------------------------------------------------------
# I. Exactly which fields the portable hash covers
#
# Added after mutation testing: the portable view is an explicit INCLUDE list, and
# neither suite would notice a field being dropped from it. Excluding the evidence or
# promotion labels would be the worst case - the inventory could be relabelled
# PROMOTABLE while its manifest hash still validated.
#
# Rather than reimplement their canonicalisation, each field is tampered with in the
# emitted document and re-hashed through their own portable view: a covered field must
# move the hash.
# --------------------------------------------------------------------------------------


MUST_BE_COVERED = [
    ("evidence_grade", "VALIDATED_PERFORMANCE_BASELINE"),
    ("promotion_status", "PROMOTABLE"),
    ("capture_provenance_caveat", "fully verified"),
    ("as_of", "1999-01-01"),
    ("schema_version", 99),
]


@pytest.mark.parametrize("field,tampered", MUST_BE_COVERED)
def test_I_tampering_a_covered_field_moves_the_hash(tmp_path: Path, field, tampered):
    mod = _load_freezer_module()
    ledger, root = build_inputs(tmp_path)
    assert freeze(ledger, root, tmp_path / "o").returncode == 0
    doc = load(tmp_path / "o")
    recorded = doc["manifest_sha256"]

    baseline = mod._sha256_bytes(mod._canonical_json(mod._portable_manifest_view(doc)))
    assert baseline == recorded, "recomputing the portable view did not reproduce the hash"

    assert field in doc, f"{field} absent from the emitted document"
    doc[field] = tampered
    after = mod._sha256_bytes(mod._canonical_json(mod._portable_manifest_view(doc)))
    assert after != recorded, (
        f"{field} is NOT covered by manifest_sha256; it could be altered undetected"
    )


@pytest.mark.parametrize("section", ["rows", "summary", "inputs"])
def test_I_tampering_a_covered_section_moves_the_hash(tmp_path: Path, section):
    mod = _load_freezer_module()
    ledger, root = build_inputs(tmp_path)
    assert freeze(ledger, root, tmp_path / "o").returncode == 0
    doc = load(tmp_path / "o")
    recorded = doc["manifest_sha256"]

    assert section in doc, f"{section} absent from the emitted document"
    if section == "rows":
        doc["rows"][0]["status"] = "RESOLVED_NATIVE_ARTIFACT_TAMPERED"
    elif section == "summary":
        doc["summary"]["accounting"]["filled"] = 999
    else:
        doc["inputs"]["artifact_evidence"][0]["sha256"] = "0" * 64
    after = mod._sha256_bytes(mod._canonical_json(mod._portable_manifest_view(doc)))
    assert after != recorded, (
        f"{section} is NOT covered by manifest_sha256; evidence could be altered undetected"
    )


def test_I_advisory_fields_are_deliberately_not_covered(tmp_path: Path):
    """The converse: the excluded metadata must genuinely be excluded, or the hash is
    not portable. Confirms the exclusion is exactly the advisory set and nothing more."""
    mod = _load_freezer_module()
    ledger, root = build_inputs(tmp_path)
    assert freeze(ledger, root, tmp_path / "o").returncode == 0
    doc = load(tmp_path / "o")
    recorded = doc["manifest_sha256"]
    for advisory in ("generator", "origin_advisory"):
        if advisory in doc:
            doc[advisory] = {"mutated": True}
    after = mod._sha256_bytes(mod._canonical_json(mod._portable_manifest_view(doc)))
    assert after == recorded, "an advisory field is hash-covered; the hash is not portable"
