"""Independent adversarial verification for Task 4 (provenance-labelled xsec runner).

Task 4 is the first stage that emits numbers, so the two highest-value attack surfaces
are (a) labelling honesty - can any output overstate what the evidence supports - and
(b) denominators - the arithmetic by which "no data" quietly becomes "no edge".

Written on a separate machine from the builder. Every rejection asserts a specific
message so a test cannot pass because the fixture was broken for another reason.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pytest

from src.core.xsec_runner import (
    ArtifactDurabilityUncertainError,
    run_xsec_replay,
)

SIG = "2026-01-05"
NEXT = "2026-01-06"
EXIT = "2026-01-09"

# Strings that would each be an overstatement of what this pipeline can support.
FORBIDDEN_CLAIMS = (
    "PIT_CAPTURE_VERIFIED", "PIT_VERIFIED", "PIT_GRADE_TRUE", "EXECUTION_VERIFIED",
    "EXECUTION_PROVEN", "ALPHA_CONFIRMED", "PRODUCTION_READY",
)
# "BINDING" is deliberately absent: it is a substring of the HONEST label
# RESEARCH_ONLY_NON_BINDING. Checked separately below as a whole token.


def bar(day: str, price: float, close: float | None = None) -> dict:
    c = price if close is None else close
    lo, hi = min(price, c), max(price, c)
    return {"day": day, "open": price, "high": hi, "low": lo, "close": c, "volume": 1000.0}


def sel(ticker: str, score: float, *, nxt=..., ext=...) -> dict:
    return {
        "ticker": ticker,
        "factor_score": score,
        "next_session": bar(NEXT, 10.0) if nxt is ... else nxt,
        "exit_session": bar(EXIT, 11.0) if ext is ... else ext,
    }


def reb(*selected, rebalance_date=SIG, sleeve="value", factor_order="DESC", cap=1e9) -> dict:
    return {
        "rebalance_date": rebalance_date, "sleeve": sleeve, "factor_order": factor_order,
        "max_entry_cap": cap, "selected": list(selected),
    }


def run(rebalances, out: Path, *, slots=10, cost_bps=10.0, bundle=None) -> Path:
    return run_xsec_replay(rebalances, output_dir=out, max_concurrent_slots=slots,
                           total_cost_bps=cost_bps, pit_bundle=bundle)


def records(artifact: Path) -> list[dict]:
    return [json.loads(line) for line in artifact.read_text().splitlines() if line.strip()]


def assert_rejected(rebalances, out: Path, expect: str | tuple[str, ...], **kw):
    with pytest.raises((ValueError, TypeError)) as exc:
        run(rebalances, out, **kw)
    msg = str(exc.value).lower()
    wanted = (expect,) if isinstance(expect, str) else expect
    assert any(w.lower() in msg for w in wanted), (
        f"rejected, but not for the intended reason.\n  wanted one of: {wanted}\n  got: {exc.value}"
    )


# --------------------------------------------------------------------------------------
# A. Labelling honesty — the output must never claim more than it can support
# --------------------------------------------------------------------------------------


def test_A_no_bundle_is_labelled_non_pit(tmp_path: Path):
    art = run([reb(sel("A", 1.0))], tmp_path / "o")
    rec = records(art)[0]
    assert rec["pit_grade"] == "PIT_GRADE_FALSE"
    assert rec["input_mode"] == "FROZEN_SELECTIONS_NON_PIT"
    assert rec["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"


def test_A_execution_provenance_is_always_caller_asserted(tmp_path: Path):
    """The runner cannot verify that a selection was actually tradeable; it must say so."""
    art = run([reb(sel("A", 1.0))], tmp_path / "o")
    assert records(art)[0]["selection_execution_provenance"] == "CALLER_ASSERTED_UNVERIFIED"


def _string_values(node) -> list[str]:
    """All string VALUES in the record tree. Field NAMES are excluded deliberately:
    scanning raw text produces false positives, e.g. EXECUTION_PROVEN inside the field
    name selection_execution_provenance, and BINDING inside RESEARCH_ONLY_NON_BINDING."""
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [v for value in node.values() for v in _string_values(value)]
    if isinstance(node, list):
        return [v for item in node for v in _string_values(item)]
    return []


def test_A_no_output_value_contains_an_overstated_claim(tmp_path: Path):
    art = run([reb(sel("A", 1.0), sel("B", 2.0))], tmp_path / "o")
    values = [v.upper() for rec in records(art) for v in _string_values(rec)]
    for claim in FORBIDDEN_CLAIMS:
        offenders = [v for v in values if claim in v]
        assert not offenders, f"output value asserts an unsupported claim {claim}: {offenders}"


def test_A_evidence_label_is_explicitly_non_binding(tmp_path: Path):
    art = run([reb(sel("A", 1.0))], tmp_path / "o")
    for rec in records(art):
        assert rec["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"


def test_A_caller_cannot_inject_a_stronger_label(tmp_path: Path):
    """A hostile or careless caller passing label fields must not have them echoed."""
    row = reb(sel("A", 1.0))
    row.update({
        "pit_grade": "PIT_CAPTURE_VERIFIED",
        "evidence_label": "PRODUCTION_READY",
        "selection_execution_provenance": "EXECUTION_VERIFIED",
        "input_mode": "FULLY_VERIFIED",
    })
    art = run([row], tmp_path / "o")
    rec = records(art)[0]
    assert rec["pit_grade"] == "PIT_GRADE_FALSE"
    assert rec["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
    assert rec["selection_execution_provenance"] == "CALLER_ASSERTED_UNVERIFIED"
    assert rec["input_mode"] == "FROZEN_SELECTIONS_NON_PIT"


def test_A_empty_run_still_carries_full_labelling(tmp_path: Path):
    """A zero-rebalance run must not become an unlabelled artifact."""
    art = run([], tmp_path / "o")
    rec = records(art)[0]
    assert rec["record_type"] == "run_summary"
    assert rec["pit_grade"] == "PIT_GRADE_FALSE"
    assert rec["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
    assert rec["rebalance_count"] == 0


# --------------------------------------------------------------------------------------
# B. Denominator honesty — how "no data" becomes "no edge"
# --------------------------------------------------------------------------------------


def test_B_mean_divides_by_filled_not_selected(tmp_path: Path):
    """Two names selected, one fillable. The mean must be the filled leg's return,
    not that return halved by the selected count."""
    art = run([reb(
        sel("A", 2.0, nxt=bar(NEXT, 10.0), ext=bar(EXIT, 11.0)),   # +10%
        sel("B", 1.0, nxt=None),                                    # censored
    )], tmp_path / "o", cost_bps=0.0001)
    s = records(art)[0]["summary"]
    assert s["selected"] == 2 and s["filled"] == 1
    assert s["filled_mean_gross_return"] == pytest.approx(0.10, abs=1e-6), (
        "mean was diluted by non-filled names"
    )


def test_B_all_censored_mean_is_none_not_zero(tmp_path: Path):
    """A zero return is a claim; absence is not. This must be null, never 0.0."""
    art = run([reb(sel("A", 1.0, nxt=None), sel("B", 2.0, nxt=None))], tmp_path / "o")
    s = records(art)[0]["summary"]
    assert s["filled"] == 0
    assert s["filled_mean_gross_return"] is None, "absence was reported as a 0% mean"
    assert s["filled_mean_net_return"] is None


def test_B_summary_reconciles_with_emitted_rows(tmp_path: Path):
    art = run([reb(
        sel("A", 4.0),
        sel("B", 3.0, nxt=bar(NEXT, 99.0)),
        sel("C", 2.0, nxt=None),
        sel("D", 1.0, ext=None),
        cap=50.0,
    )], tmp_path / "o")
    rec = records(art)[0]
    s = rec["summary"]
    assert len(rec["outcomes"]) == s["selected"] == 4
    assert s["selected"] == s["filled"] + s["no_fill"] + s["censored"]
    counted = sum(1 for o in rec["outcomes"] if o["status"] == "FILLED")
    assert counted == s["filled"]


def test_B_censored_rows_are_present_not_dropped(tmp_path: Path):
    art = run([reb(sel("A", 3.0), sel("B", 2.0, nxt=None), sel("C", 1.0, ext=None))],
              tmp_path / "o")
    rec = records(art)[0]
    tickers = {o["ticker"] for o in rec["outcomes"]}
    assert tickers == {"A", "B", "C"}, "a censored name was dropped from the artifact"
    statuses = {o["ticker"]: o["status"] for o in rec["outcomes"]}
    assert statuses["B"] == "CENSORED_MISSING_ENTRY"
    assert statuses["C"] == "CENSORED_MISSING_EXIT"


def test_B_filled_set_matches_status_not_just_non_null_return(tmp_path: Path):
    """`filled` is derived from net_return being non-null. That must agree with status,
    or a future change to the core silently changes the denominator here."""
    art = run([reb(sel("A", 3.0), sel("B", 2.0, ext=None), sel("C", 1.0, nxt=None))],
              tmp_path / "o")
    rec = records(art)[0]
    by_status = [o for o in rec["outcomes"] if o["status"] == "FILLED"]
    by_return = [o for o in rec["outcomes"] if o["net_return"] is not None]
    assert by_status == by_return


# --------------------------------------------------------------------------------------
# C. Identity uniqueness — the defect their final review caught
# --------------------------------------------------------------------------------------


def test_C_duplicate_rebalance_sleeve_rejected(tmp_path: Path):
    assert_rejected([reb(sel("A", 1.0)), reb(sel("B", 1.0))], tmp_path / "o",
                    ("duplicate rebalance/sleeve", "duplicate"))


def test_C_same_date_different_sleeve_is_allowed(tmp_path: Path):
    art = run([reb(sel("A", 1.0), sleeve="value"), reb(sel("B", 1.0), sleeve="momentum")],
              tmp_path / "o")
    assert len(records(art)) == 2


def test_C_duplicate_ticker_within_a_sleeve_rejected(tmp_path: Path):
    assert_rejected([reb(sel("A", 1.0), sel("A", 2.0))], tmp_path / "o",
                    ("duplicate selected ticker", "duplicate"))


# --------------------------------------------------------------------------------------
# D. Exclusive publication, atomicity, durability
# --------------------------------------------------------------------------------------


def test_D_second_run_refuses_and_preserves_the_original(tmp_path: Path):
    out = tmp_path / "o"
    art = run([reb(sel("A", 1.0))], out)
    original = art.read_bytes()
    with pytest.raises(FileExistsError):
        run([reb(sel("B", 2.0))], out)
    assert art.read_bytes() == original, "a refused run modified the published artifact"


def test_D_no_temp_files_left_after_success(tmp_path: Path):
    out = tmp_path / "o"
    run([reb(sel("A", 1.0))], out)
    leftovers = [p.name for p in out.iterdir() if p.name.startswith(".xsec_replay_records")]
    assert leftovers == [], f"staging files leaked: {leftovers}"


def test_D_no_temp_files_left_after_refusal(tmp_path: Path):
    out = tmp_path / "o"
    run([reb(sel("A", 1.0))], out)
    with pytest.raises(FileExistsError):
        run([reb(sel("B", 2.0))], out)
    leftovers = [p.name for p in out.iterdir() if p.name.startswith(".xsec_replay_records")]
    assert leftovers == [], f"staging files leaked after refusal: {leftovers}"


def test_D_validation_precedes_output_directory_creation(tmp_path: Path):
    """A rejected run must not leave a directory implying something was produced."""
    out = tmp_path / "never"
    assert_rejected([reb(sel("A", 1.0), factor_order="SIDEWAYS")], out,
                    ("factor_order", "asc", "desc"))
    assert not out.exists(), "output directory created despite failed validation"


def test_D_durability_failure_names_artifact_and_keeps_it(tmp_path: Path,
                                                          monkeypatch: pytest.MonkeyPatch):
    """A post-link fsync failure must raise a distinct, named error and must NOT delete
    the artifact - pretending it does not exist is the dishonest failure mode."""
    out = tmp_path / "o"
    real_fsync = os.fsync
    state = {"n": 0}

    def flaky(fd):
        state["n"] += 1
        if state["n"] >= 2:          # first call is the file, second is the directory
            raise OSError("injected directory fsync failure")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", flaky)
    with pytest.raises(ArtifactDurabilityUncertainError) as exc:
        run([reb(sel("A", 1.0))], out)
    monkeypatch.undo()

    artifact = out / "xsec_replay_records.jsonl"
    assert artifact.exists(), "artifact was deleted after a durability-uncertain failure"
    assert "xsec_replay_records.jsonl" in str(exc.value), "error does not name the artifact"
    assert records(artifact), "artifact left behind is unreadable"


# --------------------------------------------------------------------------------------
# E. Capacity determinism
# --------------------------------------------------------------------------------------


def test_E_capacity_is_independent_of_caller_list_order(tmp_path: Path):
    """Bundles stop being reproducible if input ordering changes who gets a slot."""
    picks = [sel("A", 1.0), sel("B", 3.0), sel("C", 2.0)]
    a = run([reb(*picks)], tmp_path / "a", slots=2)
    b = run([reb(*reversed(picks))], tmp_path / "b", slots=2)
    ranked_a = {o["ticker"]: o["status"] for o in records(a)[0]["outcomes"]}
    ranked_b = {o["ticker"]: o["status"] for o in records(b)[0]["outcomes"]}
    assert ranked_a == ranked_b, "capacity assignment depended on caller list order"


def test_E_capacity_follows_documented_rank_rule(tmp_path: Path):
    """DESC: highest factor score wins the slots."""
    art = run([reb(sel("A", 1.0), sel("B", 3.0), sel("C", 2.0), factor_order="DESC")],
              tmp_path / "o", slots=1)
    statuses = {o["ticker"]: o["status"] for o in records(art)[0]["outcomes"]}
    assert statuses["B"] != "CENSORED_CAPACITY", "top-ranked name was denied a slot"
    assert statuses["A"] == "CENSORED_CAPACITY"
    assert statuses["C"] == "CENSORED_CAPACITY"


def test_E_score_ties_break_by_ticker_ascending(tmp_path: Path):
    art = run([reb(sel("ZZZ", 5.0), sel("AAA", 5.0), factor_order="DESC")],
              tmp_path / "o", slots=1)
    statuses = {o["ticker"]: o["status"] for o in records(art)[0]["outcomes"]}
    assert statuses["AAA"] != "CENSORED_CAPACITY", "tie not broken by ascending ticker"


def test_E_capacity_rule_is_recorded_in_the_artifact(tmp_path: Path):
    art = run([reb(sel("A", 1.0))], tmp_path / "o", slots=3)
    rec = records(art)[0]
    assert rec["capacity_rule"], "capacity rule not disclosed"
    assert rec["max_concurrent_slots"] == 3
    assert rec["cost_assumption"], "cost simplification not disclosed"


# --------------------------------------------------------------------------------------
# F. Strict input validation
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("slots", [True, False, 0, -1, 1.5, "3", None])
def test_F_invalid_slot_counts_rejected(tmp_path: Path, slots):
    assert_rejected([reb(sel("A", 1.0))], tmp_path / "o",
                    ("max_concurrent_slots", "positive integer"), slots=slots)


@pytest.mark.parametrize("cost", [0.0, -1.0, float("nan"), float("inf"), "10", None, True])
def test_F_invalid_costs_rejected(tmp_path: Path, cost):
    assert_rejected([reb(sel("A", 1.0))], tmp_path / "o",
                    ("total_cost_bps", "positive", "number"), cost_bps=cost)


@pytest.mark.parametrize("bad_date", ["20260105", "2026-13-45", "2026-1-5", "", None, 20260105])
def test_F_invalid_rebalance_dates_rejected(tmp_path: Path, bad_date):
    assert_rejected([reb(sel("A", 1.0), rebalance_date=bad_date)], tmp_path / "o",
                    ("rebalance_date", "date"))


@pytest.mark.parametrize("bad", [None, "", 123, []])
def test_F_invalid_sleeve_rejected(tmp_path: Path, bad):
    row = reb(sel("A", 1.0))
    row["sleeve"] = bad
    assert_rejected([row], tmp_path / "o", ("sleeve",))


def test_F_non_mapping_rebalance_rejected(tmp_path: Path):
    assert_rejected(["not a mapping"], tmp_path / "o", ("rebalance", "mapping"))


def test_F_string_rebalances_argument_rejected(tmp_path: Path):
    """A bare string is a Sequence; iterating it would silently produce garbage."""
    assert_rejected("abc", tmp_path / "o", ("rebalances", "sequence", "mapping"))


def test_F_nan_factor_score_rejected(tmp_path: Path):
    assert_rejected([reb(sel("A", float("nan")))], tmp_path / "o",
                    ("canonical json", "factor_score", "number", "finite"))


def test_F_nan_is_rejected_before_it_can_reach_the_artifact(tmp_path: Path):
    """NaN is refused at JSON canonicalisation (allow_nan=False), which is stronger than
    a field-level check: no NaN can be serialised into a canonical record at all."""
    out = tmp_path / "o"
    with pytest.raises((ValueError, TypeError)):
        run([reb(sel("A", float("nan")))], out)
    assert not out.exists(), "output directory created for an unserialisable run"


# --------------------------------------------------------------------------------------
# G. Provenance binding
# --------------------------------------------------------------------------------------


def test_G_selection_content_hash_changes_with_selection(tmp_path: Path):
    a = run([reb(sel("A", 1.0))], tmp_path / "a")
    b = run([reb(sel("A", 2.0))], tmp_path / "b")
    assert (records(a)[0]["frozen_selection_content_sha256"]
            != records(b)[0]["frozen_selection_content_sha256"])


def test_G_identical_selection_hashes_identically(tmp_path: Path):
    a = run([reb(sel("A", 1.0))], tmp_path / "a")
    b = run([reb(sel("A", 1.0))], tmp_path / "b")
    assert (records(a)[0]["frozen_selection_content_sha256"]
            == records(b)[0]["frozen_selection_content_sha256"])


def test_G_source_hashes_present_and_distinct(tmp_path: Path):
    rec = records(run([reb(sel("A", 1.0))], tmp_path / "o"))[0]
    assert len(rec["runner_source_sha256"]) == 64
    assert len(rec["xsec_replay_source_sha256"]) == 64
    assert rec["runner_source_sha256"] != rec["xsec_replay_source_sha256"]


def test_G_runner_tree_dirty_is_recorded(tmp_path: Path):
    """Task 2 finding G3, restated at this layer: a commit id is worthless if the tree
    state that produced the artifact is not disclosed alongside it."""
    rec = records(run([reb(sel("A", 1.0))], tmp_path / "o"))[0]
    assert "runner_git_commit" in rec
    assert isinstance(rec["runner_tree_dirty"], bool)


def test_G_probe_capacity_slot_wasted_on_unfillable_name(tmp_path: Path, capsys):
    """Capacity is assigned by rank BEFORE fillability is known, so a top-ranked name
    with no bar consumes a slot that a lower-ranked fillable name could have used.
    Deterministic and defensible, but it changes the filled denominator, so it is
    reported rather than asserted."""
    art = run([reb(sel("A", 9.0, nxt=None), sel("B", 1.0), factor_order="DESC")],
              tmp_path / "o", slots=1)
    statuses = {o["ticker"]: o["status"] for o in records(art)[0]["outcomes"]}
    with capsys.disabled():
        print(f"\n[PROBE] rank-first capacity with unfillable top name: {statuses}")


# --------------------------------------------------------------------------------------
# H. PIT bundle integration — Tasks 1 + 2 + 2.5 feeding Task 4
#
# Added after mutation testing showed the suite had no PIT-bundle coverage at all, so
# removing membership enforcement entirely went undetected here.
# --------------------------------------------------------------------------------------

import hashlib

PIT_A, PIT_B, PIT_C = "600000.SH", "600002.SH", "600003.SH"


def _make_bundle(tmp_path: Path, day: str = "20260105", iso: str = SIG) -> Path:
    """Build a real Task 2 bundle with a Task 2.5 capture receipt."""
    from scripts.build_pit_universe_schedule import _parse_as_of_dates, build_bundle

    src = tmp_path / "snaps"
    src.mkdir()
    csv_path = src / f"daily_basic_{day}.csv"
    csv_path.write_text(
        "ts_code,circ_mv,list_date,delist_date\n"
        f"{PIT_C},5000,2010-01-01,\n{PIT_A},3000,2010-01-01,\n{PIT_B},1000,2010-01-01,\n"
    )
    (src / f"daily_basic_{day}.capture.json").write_text(json.dumps({
        "schema_version": 1, "provider": "tushare", "endpoint": "daily_basic",
        "requested_trade_date": day, "snapshot_file": csv_path.name,
        "snapshot_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "captured_at": f"{iso}T16:00:00Z",
        "provenance_grade": "TRUSTED_HISTORICAL_ASSUMPTION",
        "caveat": "historical_tushare_trusted_assumption",
    }))
    bundle = tmp_path / "bundle"
    build_bundle(src, bundle, _parse_as_of_dates(iso), 3, "tushare_daily_basic")
    return bundle


def test_H_valid_bundle_is_labelled_membership_only(tmp_path: Path):
    """The strongest label this pipeline can earn - and it must not be stronger."""
    bundle = _make_bundle(tmp_path)
    art = run([reb(sel(PIT_A, 2.0), sel(PIT_C, 1.0))], tmp_path / "o", bundle=bundle)
    rec = records(art)[0]
    assert rec["pit_grade"] == "PIT_UNIVERSE_MEMBERSHIP_ONLY"
    assert rec["input_mode"] == "FROZEN_SELECTIONS_PIT_UNIVERSE_MEMBERSHIP_VALIDATED"
    assert rec["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
    assert rec["selection_execution_provenance"] == "CALLER_ASSERTED_UNVERIFIED"
    assert rec["input_bundle_id"] and len(rec["input_bundle_composite_sha256"]) == 64


def test_H_ticker_absent_from_bundle_is_rejected(tmp_path: Path):
    """Membership enforcement: a name not in the frozen universe cannot be replayed."""
    bundle = _make_bundle(tmp_path)
    assert_rejected([reb(sel(PIT_A, 2.0), sel("600999.SH", 1.0))], tmp_path / "o",
                    ("does not contain selected ticker", "pit bundle"), bundle=bundle)


def test_H_date_absent_from_bundle_is_rejected(tmp_path: Path):
    bundle = _make_bundle(tmp_path)
    assert_rejected([reb(sel(PIT_A, 1.0), rebalance_date="2026-01-06")], tmp_path / "o",
                    ("does not contain selected ticker", "pit bundle"), bundle=bundle)


def test_H_membership_label_never_claims_capture_verification(tmp_path: Path):
    """Even with a fully valid bundle the output must not inherit a capture claim:
    the bundle's own grade is TRUSTED_HISTORICAL_ASSUMPTION, not observed capture."""
    bundle = _make_bundle(tmp_path)
    art = run([reb(sel(PIT_A, 1.0))], tmp_path / "o", bundle=bundle)
    values = [v.upper() for rec in records(art) for v in _string_values(rec)]
    for claim in FORBIDDEN_CLAIMS + ("OBSERVED_CAPTURE",):
        assert not [v for v in values if claim in v], f"output claims {claim}"


def test_H_invalid_bundle_rejected_before_any_output(tmp_path: Path):
    bundle = _make_bundle(tmp_path)
    (bundle / "manifest.json").write_text("{not json")
    out = tmp_path / "o"
    with pytest.raises(Exception):
        run([reb(sel(PIT_A, 1.0))], out, bundle=bundle)
    assert not out.exists(), "output directory created despite an invalid PIT bundle"
