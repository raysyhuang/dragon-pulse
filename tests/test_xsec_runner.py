"""Task4 integration contract: canonical, fail-closed xsec replay artifacts."""
from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest

from src.core import xsec_runner
from src.core.pit_bundle import PitBundleValidationError


def _bar(day: str, opening: float, close: float) -> dict[str, object]:
    return {"day": day, "open": opening, "high": max(opening, close), "low": min(opening, close), "close": close, "volume": 1000}


_DEFAULT = object()


def _selection(ticker: str, score: float, *, entry: object = _DEFAULT, exit: object = _DEFAULT) -> dict[str, object]:
    return {
        "ticker": ticker,
        "factor_score": score,
        "next_session": _bar("2025-01-03", 100, 101) if entry is _DEFAULT else entry,
        "exit_session": _bar("2025-01-06", 101, 110) if exit is _DEFAULT else exit,
    }


def _rebalance(*selections: dict[str, object]) -> dict[str, object]:
    return {
        "rebalance_date": "2025-01-02",
        "sleeve": "momentum",
        "factor_order": "DESC",
        "max_entry_cap": 100.0,
        "selected": list(selections),
    }


def _records(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_runner_calls_core_for_every_frozen_selected_ticker_and_writes_non_pit_artifact(tmp_path, monkeypatch) -> None:
    calls = []
    real_replay = xsec_runner.replay_cross_section

    def counted(replay):
        calls.append(replay)
        return real_replay(replay)

    monkeypatch.setattr(xsec_runner, "replay_cross_section", counted)
    artifact = xsec_runner.run_xsec_replay(
        [_rebalance(_selection("000002.SZ", 2.0), _selection("000001.SZ", 2.0))],
        output_dir=tmp_path / "out",
        max_concurrent_slots=2,
        total_cost_bps=20.0,
    )

    assert len(calls) == 1
    rows = _records(artifact)
    assert len(rows) == 1
    row = rows[0]
    assert row["schema_version"] == 1
    assert row["input_mode"] == "FROZEN_SELECTIONS_NON_PIT"
    assert row["pit_grade"] == "PIT_GRADE_FALSE"
    assert row["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
    assert row["capacity_rule"] == "rank-first: factor order then ticker ASC; first max_concurrent_slots are capacity_available"
    assert row["max_concurrent_slots"] == 2
    assert row["cost_assumption"] == "total_cost_bps research simplification; not a leg-specific A-share commission/stamp-duty model"
    assert [outcome["ticker"] for outcome in row["outcomes"]] == ["000001.SZ", "000002.SZ"]
    assert row["summary"]["selected"] == 2
    assert row["summary"]["filled"] == 2
    assert row["summary"]["filled_mean_net_return"] == pytest.approx(0.098)


@pytest.mark.parametrize(
    ("path", "bad_value"),
    [
        ("max_entry_cap", "100.0"),
        ("factor_score", "2.0"),
        ("next_session.open", "100.0"),
        ("exit_session.volume", True),
    ],
)
def test_frozen_selection_numeric_fields_require_literal_finite_json_numbers(tmp_path, path: str, bad_value: object) -> None:
    rebalance = _rebalance(_selection("000001.SZ", 2.0))
    if path == "factor_score":
        rebalance["selected"][0]["factor_score"] = bad_value
    elif path.startswith(("next_session.", "exit_session.")):
        session, field = path.split(".")
        rebalance["selected"][0][session][field] = bad_value
    else:
        rebalance[path] = bad_value

    with pytest.raises(ValueError, match="literal JSON number"):
        xsec_runner.run_xsec_replay([rebalance], output_dir=tmp_path / "out", max_concurrent_slots=1, total_cost_bps=20.0)
    assert not (tmp_path / "out").exists()


def test_rank_first_capacity_is_order_independent_and_equality_cap_fills(tmp_path) -> None:
    selections = [_selection("000003.SZ", 1.0), _selection("000002.SZ", 2.0), _selection("000001.SZ", 2.0)]
    artifact = xsec_runner.run_xsec_replay(
        [_rebalance(*selections)], output_dir=tmp_path / "out", max_concurrent_slots=2, total_cost_bps=20.0,
    )
    outcomes = _records(artifact)[0]["outcomes"]
    assert [(row["ticker"], row["status"]) for row in outcomes] == [
        ("000001.SZ", "FILLED"), ("000002.SZ", "FILLED"), ("000003.SZ", "CENSORED_CAPACITY"),
    ]


def test_missing_data_is_emitted_and_all_censored_mean_is_null(tmp_path) -> None:
    artifact = xsec_runner.run_xsec_replay(
        [_rebalance(
            _selection("000001.SZ", 3.0, entry=None),
            _selection("000002.SZ", 2.0, exit=None),
        )], output_dir=tmp_path / "out", max_concurrent_slots=2, total_cost_bps=20.0,
    )
    row = _records(artifact)[0]
    assert [outcome["status"] for outcome in row["outcomes"]] == ["CENSORED_MISSING_ENTRY", "CENSORED_MISSING_EXIT"]
    assert row["summary"]["selected"] == 2
    assert row["summary"]["censored"] == 2
    assert row["summary"]["filled_mean_gross_return"] is None
    assert row["summary"]["filled_mean_net_return"] is None


def test_canonical_filled_means_use_filled_denominator_and_null_all_censored_records(tmp_path) -> None:
    mixed_artifact = xsec_runner.run_xsec_replay(
        [_rebalance(
            _selection("000001.SZ", 3.0, entry=_bar("2025-01-03", 100, 100), exit=_bar("2025-01-06", 100, 110)),
            _selection("000002.SZ", 2.0, entry=_bar("2025-01-03", 100, 100), exit=_bar("2025-01-06", 100, 80)),
            _selection("000003.SZ", 1.0, entry=None),
        )], output_dir=tmp_path / "mixed", max_concurrent_slots=3, total_cost_bps=20.0,
    )
    mixed_summary = _records(mixed_artifact)[0]["summary"]
    assert isinstance(mixed_summary, dict)
    assert mixed_summary["selected"] == 3
    assert mixed_summary["filled"] == 2
    assert mixed_summary["filled_mean_gross_return"] == pytest.approx(-0.05)
    assert mixed_summary["filled_mean_net_return"] == pytest.approx(-0.052)

    censored_artifact = xsec_runner.run_xsec_replay(
        [_rebalance(_selection("000001.SZ", 2.0, entry=None), _selection("000002.SZ", 1.0, exit=None))],
        output_dir=tmp_path / "all-censored", max_concurrent_slots=2, total_cost_bps=20.0,
    )
    censored_summary = _records(censored_artifact)[0]["summary"]
    assert isinstance(censored_summary, dict)
    assert censored_summary["selected"] == 2
    assert censored_summary["filled"] == 0
    assert censored_summary["filled_mean_gross_return"] is None
    assert censored_summary["filled_mean_net_return"] is None


def test_invalid_pit_bundle_fails_before_creating_artifact(tmp_path) -> None:
    output = tmp_path / "out"
    with pytest.raises(PitBundleValidationError):
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000001.SZ", 1.0))], output_dir=output, max_concurrent_slots=1,
            total_cost_bps=20.0, pit_bundle=tmp_path / "invalid",
        )
    assert not output.exists()


def _valid_pit_bundle(tmp_path: Path) -> Path:
    """Use the actual schedule builder so runner provenance is validator-accepted."""
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("pit_builder", repo / "scripts" / "build_pit_universe_schedule.py")
    assert spec is not None and spec.loader is not None
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    sources = tmp_path / "sources"
    sources.mkdir()
    snapshot = sources / "daily_basic_20250102.csv"
    with snapshot.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ts_code", "circ_mv", "list_date", "delist_date"])
        writer.writeheader()
        writer.writerow({"ts_code": "000001.SZ", "circ_mv": "100", "list_date": "2020-01-01", "delist_date": ""})
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    (sources / "daily_basic_20250102.capture.json").write_text(json.dumps({
        "schema_version": 1, "provider": "tushare", "endpoint": "daily_basic", "requested_trade_date": "20250102",
        "snapshot_file": snapshot.name, "snapshot_sha256": digest, "captured_at": "2025-01-03T00:00:00Z",
        "provenance_grade": "TRUSTED_HISTORICAL_ASSUMPTION", "caveat": "historical_tushare_trusted_assumption",
    }), encoding="utf-8")
    bundle = tmp_path / "bundle"
    builder.build_bundle(sources, bundle, builder._parse_as_of_dates("2025-01-02"), 1, "fixture")
    return bundle


def test_valid_pit_bundle_missing_selected_schedule_identity_fails_before_artifact(tmp_path) -> None:
    bundle = _valid_pit_bundle(tmp_path)
    output = tmp_path / "out"
    with pytest.raises(ValueError, match="does not contain selected ticker"):
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000099.SZ", 1.0))], output_dir=output, max_concurrent_slots=1,
            total_cost_bps=20.0, pit_bundle=bundle,
        )
    assert not output.exists()


def test_valid_tiny_pit_bundle_emits_labelled_artifact_with_provenance(tmp_path) -> None:
    bundle = _valid_pit_bundle(tmp_path)
    artifact = xsec_runner.run_xsec_replay(
        [_rebalance(_selection("000001.SZ", 1.0))], output_dir=tmp_path / "out", max_concurrent_slots=1,
        total_cost_bps=20.0, pit_bundle=bundle,
    )
    row = _records(artifact)[0]
    assert row["pit_grade"] == "PIT_UNIVERSE_MEMBERSHIP_ONLY"
    assert row["input_mode"] == "FROZEN_SELECTIONS_PIT_UNIVERSE_MEMBERSHIP_VALIDATED"
    assert row["input_bundle_id"]
    assert row["input_bundle_composite_sha256"]
    assert row["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
    assert row["selection_execution_provenance"] == "CALLER_ASSERTED_UNVERIFIED"
    selection_hash = row["frozen_selection_content_sha256"]
    assert isinstance(selection_hash, str)
    assert re.fullmatch(r"[0-9a-f]{64}", selection_hash)


def test_canonical_labels_ignore_caller_injections_for_non_pit_and_valid_pit_bundle(tmp_path) -> None:
    injected = {
        "pit_grade": "CALLER_INJECTED_PIT_GRADE",
        "evidence_label": "CALLER_INJECTED_EVIDENCE_LABEL",
        "input_mode": "CALLER_INJECTED_INPUT_MODE",
        "selection_execution_provenance": "CALLER_INJECTED_EXECUTION_PROVENANCE",
    }
    non_pit = _rebalance(_selection("000001.SZ", 1.0))
    non_pit.update(injected)
    pit = _rebalance(_selection("000001.SZ", 1.0))
    pit.update(injected)

    non_pit_record = _records(xsec_runner.run_xsec_replay(
        [non_pit], output_dir=tmp_path / "non-pit", max_concurrent_slots=1, total_cost_bps=20.0,
    ))[0]
    pit_record = _records(xsec_runner.run_xsec_replay(
        [pit], output_dir=tmp_path / "pit", max_concurrent_slots=1, total_cost_bps=20.0,
        pit_bundle=_valid_pit_bundle(tmp_path),
    ))[0]

    for record, expected in (
        (non_pit_record, ("PIT_GRADE_FALSE", "FROZEN_SELECTIONS_NON_PIT")),
        (pit_record, ("PIT_UNIVERSE_MEMBERSHIP_ONLY", "FROZEN_SELECTIONS_PIT_UNIVERSE_MEMBERSHIP_VALIDATED")),
    ):
        assert record["pit_grade"] == expected[0]
        assert record["input_mode"] == expected[1]
        assert record["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
        assert record["selection_execution_provenance"] == "CALLER_ASSERTED_UNVERIFIED"
        for field, supplied in injected.items():
            assert record[field] != supplied


@pytest.mark.parametrize("compact_date", ["20250102", "2025-1-2", "2025/01/02"])
def test_dates_require_literal_yyyy_mm_dd_before_creating_output(tmp_path, compact_date: str) -> None:
    rebalance = _rebalance(_selection("000001.SZ", 1.0))
    rebalance["rebalance_date"] = compact_date

    with pytest.raises(ValueError, match="ISO YYYY-MM-DD"):
        xsec_runner.run_xsec_replay([rebalance], output_dir=tmp_path / "out", max_concurrent_slots=1, total_cost_bps=20.0)
    assert not (tmp_path / "out").exists()


def test_duplicate_rebalance_sleeve_identity_fails_before_creating_output(tmp_path) -> None:
    duplicate = _rebalance(_selection("000001.SZ", 1.0))
    output = tmp_path / "duplicate"

    with pytest.raises(ValueError, match="duplicate rebalance/sleeve identity"):
        xsec_runner.run_xsec_replay(
            [duplicate, duplicate], output_dir=output, max_concurrent_slots=1, total_cost_bps=20.0,
        )
    assert not output.exists()


def test_rebalance_sleeve_identity_allows_distinct_sleeves_or_dates(tmp_path) -> None:
    same_date_different_sleeve = _rebalance(_selection("000001.SZ", 1.0))
    same_date_different_sleeve["sleeve"] = "value"
    same_sleeve_different_date = _rebalance(
        _selection("000001.SZ", 1.0, entry=_bar("2025-01-04", 100, 101), exit=_bar("2025-01-06", 101, 110)),
    )
    same_sleeve_different_date["rebalance_date"] = "2025-01-03"

    different_sleeves = xsec_runner.run_xsec_replay(
        [_rebalance(_selection("000001.SZ", 1.0)), same_date_different_sleeve],
        output_dir=tmp_path / "different-sleeves", max_concurrent_slots=1, total_cost_bps=20.0,
    )
    different_dates = xsec_runner.run_xsec_replay(
        [_rebalance(_selection("000001.SZ", 1.0)), same_sleeve_different_date],
        output_dir=tmp_path / "different-dates", max_concurrent_slots=1, total_cost_bps=20.0,
    )

    assert len(_records(different_sleeves)) == 2
    assert len(_records(different_dates)) == 2


def test_artifact_uses_normalized_signal_date_and_actual_source_hashes(tmp_path) -> None:
    rebalance = _rebalance(_selection("000001.SZ", 1.0))
    artifact = xsec_runner.run_xsec_replay(
        [rebalance], output_dir=tmp_path / "out", max_concurrent_slots=1, total_cost_bps=20.0,
    )
    row = _records(artifact)[0]

    assert row["rebalance_date"] == "2025-01-02"
    assert row["signal_date"] == "2025-01-02"
    assert row["frozen_selection_content_sha256"] == hashlib.sha256(
        json.dumps(rebalance["selected"], sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert row["runner_source_sha256"] == hashlib.sha256(Path(xsec_runner.__file__).read_bytes()).hexdigest()
    replay_source = Path(xsec_runner.__file__).with_name("xsec_replay.py")
    assert row["xsec_replay_source_sha256"] == hashlib.sha256(replay_source.read_bytes()).hexdigest()
    runner_hash = row["runner_source_sha256"]
    replay_hash = row["xsec_replay_source_sha256"]
    assert isinstance(runner_hash, str) and re.fullmatch(r"[0-9a-f]{64}", runner_hash)
    assert isinstance(replay_hash, str) and re.fullmatch(r"[0-9a-f]{64}", replay_hash)


def test_canonical_publish_is_exclusive_and_preserves_existing_destination(tmp_path) -> None:
    output = tmp_path / "out"
    output.mkdir()
    artifact = output / "xsec_replay_records.jsonl"
    artifact.write_bytes(b"pre-existing canonical evidence\n")

    with pytest.raises(FileExistsError):
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000001.SZ", 1.0))], output_dir=output, max_concurrent_slots=1, total_cost_bps=20.0,
        )
    assert artifact.read_bytes() == b"pre-existing canonical evidence\n"
    assert list(output.glob(".xsec_replay_records.*")) == []


def test_canonical_publish_cleans_temp_and_destination_when_publish_fails(tmp_path, monkeypatch) -> None:
    output = tmp_path / "out"

    def fail_link(source: str, destination: str, *args: object, **kwargs: object) -> None:
        raise OSError("injected exclusive publish failure")

    monkeypatch.setattr(xsec_runner.os, "link", fail_link)
    with pytest.raises(OSError, match="injected exclusive publish failure"):
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000001.SZ", 1.0))], output_dir=output, max_concurrent_slots=1, total_cost_bps=20.0,
        )
    assert not (output / "xsec_replay_records.jsonl").exists()
    assert list(output.glob(".xsec_replay_records.*")) == []


def test_canonical_publish_discloses_preserved_artifact_after_directory_sync_failure(tmp_path, monkeypatch) -> None:
    """Post-link fsync errors have uncertain durability; preserve the artifact for inspection."""
    output = tmp_path / "out"
    artifact = output / "xsec_replay_records.jsonl"
    real_link = xsec_runner.os.link
    real_sync = xsec_runner.os.fsync
    published_bytes: bytes | None = None
    sync_calls = 0

    def capture_link(source: str, destination: str, *args: object, **kwargs: object) -> None:
        nonlocal published_bytes
        published_bytes = Path(source).read_bytes()
        real_link(source, destination, *args, **kwargs)

    def fail_directory_sync(file_descriptor: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == 2:
            raise OSError("injected directory sync failure")
        real_sync(file_descriptor)

    monkeypatch.setattr(xsec_runner.os, "link", capture_link)
    monkeypatch.setattr(xsec_runner.os, "fsync", fail_directory_sync)
    with pytest.raises(xsec_runner.ArtifactDurabilityUncertainError) as raised:
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000001.SZ", 1.0))], output_dir=output, max_concurrent_slots=1, total_cost_bps=20.0,
        )
    assert raised.value.artifact == artifact
    assert str(artifact) in str(raised.value)
    assert "artifact may exist but durability is uncertain; inspect/reconcile manually" in str(raised.value)
    assert isinstance(raised.value.__cause__, OSError)
    assert "injected directory sync failure" in str(raised.value.__cause__)
    assert published_bytes is not None
    assert artifact.read_bytes() == published_bytes
    assert list(output.glob(".xsec_replay_records.*")) == []


def test_post_link_sync_failure_never_unlinks_a_concurrent_artifact_replacement(tmp_path, monkeypatch) -> None:
    """The destination is never auto-deleted after link, avoiding stat-to-unlink TOCTOU."""
    output = tmp_path / "out"
    artifact = output / "xsec_replay_records.jsonl"
    replacement = b"concurrent replacement"
    real_sync = xsec_runner.os.fsync
    real_unlink = Path.unlink
    sync_calls = 0
    artifact_unlink_calls = 0

    def fail_directory_sync(file_descriptor: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == 2:
            raise OSError("injected directory sync failure")
        real_sync(file_descriptor)

    def replace_immediately_before_unlink(path: Path, *args: object, **kwargs: object) -> None:
        nonlocal artifact_unlink_calls
        if path == artifact:
            artifact_unlink_calls += 1
            path.write_bytes(replacement)
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(xsec_runner.os, "fsync", fail_directory_sync)
    monkeypatch.setattr(Path, "unlink", replace_immediately_before_unlink)
    with pytest.raises(xsec_runner.ArtifactDurabilityUncertainError) as raised:
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000001.SZ", 1.0))], output_dir=output, max_concurrent_slots=1, total_cost_bps=20.0,
        )
    assert raised.value.artifact == artifact
    assert artifact_unlink_calls == 0
    assert _records(artifact)[0]["record_type"] == "rebalance_sleeve"
    assert list(output.glob(".xsec_replay_records.*")) == []


def test_canonical_publish_cleans_temp_and_destination_when_write_sync_fails(tmp_path, monkeypatch) -> None:
    output = tmp_path / "out"

    def fail_sync(file_descriptor: int) -> None:
        raise OSError("injected write sync failure")

    monkeypatch.setattr(xsec_runner.os, "fsync", fail_sync)
    with pytest.raises(OSError, match="injected write sync failure"):
        xsec_runner.run_xsec_replay(
            [_rebalance(_selection("000001.SZ", 1.0))], output_dir=output, max_concurrent_slots=1, total_cost_bps=20.0,
        )
    assert not (output / "xsec_replay_records.jsonl").exists()
    assert list(output.glob(".xsec_replay_records.*")) == []


def test_real_canonical_cli_replays_without_provider_secret(tmp_path) -> None:
    selection_file = tmp_path / "selections.json"
    selection_file.write_text(json.dumps([_rebalance(_selection("000001.SZ", 1.0))]), encoding="utf-8")
    output = tmp_path / "out"
    environment = os.environ.copy()
    environment.pop("TUSHARE_TOKEN", None)

    completed = subprocess.run(
        [sys.executable, "scripts/xsec_sleeves.py", "--frozen-selections", str(selection_file), "--output-dir", str(output)],
        cwd=Path(__file__).resolve().parents[1], env=environment, text=True, capture_output=True, check=False,
    )

    assert completed.returncode == 0, completed.stderr
    rows = _records(output / "xsec_replay_records.jsonl")
    assert rows[0]["input_mode"] == "FROZEN_SELECTIONS_NON_PIT"
    assert rows[0]["pit_grade"] == "PIT_GRADE_FALSE"
    assert rows[0]["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"


def test_canonical_cli_discloses_durability_uncertainty_without_false_failed_closed(tmp_path, monkeypatch, capsys) -> None:
    repo = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("xsec_sleeves", repo / "scripts" / "xsec_sleeves.py")
    assert spec is not None and spec.loader is not None
    sleeves = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sleeves)
    selection_file = tmp_path / "selections.json"
    selection_file.write_text(json.dumps([_rebalance(_selection("000001.SZ", 1.0))]), encoding="utf-8")
    artifact = tmp_path / "out" / "xsec_replay_records.jsonl"

    def durability_uncertain(*args: object, **kwargs: object) -> Path:
        raise xsec_runner.ArtifactDurabilityUncertainError(artifact)

    monkeypatch.setattr(sleeves, "run_xsec_replay", durability_uncertain)
    result = sleeves.main(["--frozen-selections", str(selection_file), "--output-dir", str(tmp_path / "out")])

    stderr = capsys.readouterr().err
    assert result != 0
    assert str(artifact) in stderr
    assert "artifact may exist but durability is uncertain; inspect/reconcile manually" in stderr
    assert "failed closed" not in stderr
