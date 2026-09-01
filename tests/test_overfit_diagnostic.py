"""Adversarial contracts for the research-only native overfit diagnostic."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import subprocess
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

import src.research.overfit_diagnostic as diagnostic
from src.research.overfit_diagnostic import DiagnosticError, _code_identity, run_diagnostic


STATUS = "RESEARCH_ONLY_NON_BINDING"


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def _code_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "docs").mkdir()
    (repo / "src" / "engine.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "scripts" / "run.py").write_text("print('ok')\n", encoding="utf-8")
    (repo / "docs" / "notes.md").write_text("clean\n", encoding="utf-8")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "fixture")
    return repo


def test_code_identity_binds_clean_head_bytes_and_refuses_dirty_executable(tmp_path):
    repo = _code_repo(tmp_path)

    identity = _code_identity(repo)

    assert identity["code_sha"] == _git(repo, "rev-parse", "HEAD")
    assert len(identity["code_bundle_sha256"]) == 64
    (repo / "src" / "engine.py").write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(DiagnosticError, match="executable project file differs from HEAD"):
        _code_identity(repo)


@pytest.mark.parametrize(
    "change",
    [
        "ignored", "staged_add", "staged_delete", "staged_modify", "mode", "symlink", "missing",
    ],
)
def test_code_identity_refuses_any_filesystem_or_index_delta_in_scope(tmp_path, change):
    repo = _code_repo(tmp_path)
    engine = repo / "src" / "engine.py"
    if change == "ignored":
        (repo / ".gitignore").write_text("src/ignored.py\n", encoding="utf-8")
        (repo / "src" / "ignored.py").write_text("IGNORED = True\n", encoding="utf-8")
    elif change == "staged_add":
        (repo / "scripts" / "added.py").write_text("ADDED = True\n", encoding="utf-8")
        _git(repo, "add", "scripts/added.py")
    elif change == "staged_delete":
        _git(repo, "rm", "src/engine.py")
    elif change == "staged_modify":
        engine.write_text("VALUE = 2\n", encoding="utf-8")
        _git(repo, "add", "src/engine.py")
        engine.write_text("VALUE = 1\n", encoding="utf-8")
    elif change == "mode":
        engine.chmod(0o755)
    elif change == "symlink":
        engine.unlink()
        engine.symlink_to("../scripts/run.py")
    else:
        engine.unlink()

    with pytest.raises(DiagnosticError):
        _code_identity(repo)


def test_code_identity_allows_dirty_files_outside_executable_scope(tmp_path):
    repo = _code_repo(tmp_path)
    (repo / "docs" / "notes.md").write_text("dirty but allowed\n", encoding="utf-8")

    assert _code_identity(repo)["code_sha"] == _git(repo, "rev-parse", "HEAD")


def test_code_identity_refuses_ignored_untracked_symlink_directory_without_following_it(tmp_path):
    repo = _code_repo(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    (external / "hidden.py").write_text("raise AssertionError('must not be read')\n", encoding="utf-8")
    (repo / ".gitignore").write_text("src/plugins\n", encoding="utf-8")
    (repo / "src" / "plugins").symlink_to(external, target_is_directory=True)

    with pytest.raises(DiagnosticError, match="symlinked directory"):
        _code_identity(repo)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _packet(tmp_path: Path, *, rows: int = 160, variants: int = 4, edge: bool = True) -> tuple[Path, Path]:
    matrix = tmp_path / "returns.csv"
    variant_ids = [f"variant_{index}" for index in range(variants)]
    rng = np.random.default_rng(20260831)
    values = rng.normal(0.0, 0.01, size=(rows, variants))
    if edge:
        values[:, 0] = 0.0015 + rng.normal(0.0, 0.003, size=rows)
    start = date(2025, 1, 2)
    with matrix.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["date", *variant_ids])
        for index, row in enumerate(values):
            writer.writerow([(start + timedelta(days=index)).isoformat(), *[f"{value:.12g}" for value in row]])
    manifest = {
        "schema_version": 1,
        "status": STATUS,
        "selected_variant": variant_ids[0],
        "variant_ids": variant_ids,
        "n_trials_total": variants,
        "date_start": start.isoformat(),
        "date_end": (start + timedelta(days=rows - 1)).isoformat(),
        "periods_per_year": 252,
        "input_bundle_sha256": "1" * 64,
        "experiment_config_sha256": "2" * 64,
        "execution_contract_hash": "3" * 64,
        "matrix_sha256": _sha256(matrix),
        "all_tested_and_abandoned_variants_counted": True,
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return matrix, manifest_path


def _run(tmp_path: Path, **kwargs):
    matrix, manifest = _packet(tmp_path, **kwargs)
    output = tmp_path / "evidence"
    result = run_diagnostic(matrix, manifest, output, n_blocks=4, min_block_observations=20)
    return output, result


def test_success_writes_complete_non_promoting_evidence(tmp_path):
    output, result = _run(tmp_path)

    assert result["status"] == STATUS
    assert result["verdict"] in {"VETO_FURTHER_PROMOTION", "NO_VETO_RESEARCH_ONLY_NOT_PROMOTION"}
    assert "pbo" in result["diagnostics"]
    assert result["diagnostics"]["pbo"] is not None
    assert {path.name for path in output.iterdir()} == {
        "summary.json", "pbo_splits.csv", "artifact_manifest.json"
    }
    artifact_manifest = json.loads((output / "artifact_manifest.json").read_text(encoding="utf-8"))
    assert set(artifact_manifest["files"]) == {"summary.json", "pbo_splits.csv"}


def _load_manifest(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_manifest(path: Path, manifest: dict) -> None:
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")


def _matrix_rows(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def _write_matrix(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv.writer(handle, lineterminator="\n").writerows(rows)


def _invoke_invalid(tmp_path: Path, mutate) -> None:
    matrix, manifest_path = _packet(tmp_path)
    mutate(matrix, manifest_path)
    output = tmp_path / "must_not_exist"
    with pytest.raises(DiagnosticError):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()
    assert not list(tmp_path.glob(".must_not_exist.tmp-*"))


def _mutate_manifest(field: str, value):
    def mutate(matrix: Path, manifest_path: Path) -> None:
        manifest = _load_manifest(manifest_path)
        manifest[field] = value
        _write_manifest(manifest_path, manifest)
    return mutate


def _mutate_matrix(change, *, refresh_hash: bool = True):
    def mutate(matrix: Path, manifest_path: Path) -> None:
        rows = _matrix_rows(matrix)
        change(rows)
        _write_matrix(matrix, rows)
        if refresh_hash:
            manifest = _load_manifest(manifest_path)
            manifest["matrix_sha256"] = _sha256(matrix)
            _write_manifest(manifest_path, manifest)
    return mutate


@pytest.mark.parametrize(
    "mutate",
    [
        _mutate_matrix(lambda rows: rows.__setitem__(slice(1, None), [])),
        _mutate_matrix(lambda rows: rows[1].__setitem__(0, "2025/01/02")),
        _mutate_matrix(lambda rows: rows[2].__setitem__(0, rows[1][0])),
        _mutate_matrix(lambda rows: rows.__setitem__(slice(1, 3), [rows[2], rows[1]])),
        _mutate_manifest("date_start", "2024-12-31"),
        _mutate_manifest("date_end", "2026-12-31"),
        _mutate_matrix(lambda rows: rows[1].__setitem__(1, "nan")),
        _mutate_matrix(lambda rows: rows[1].__setitem__(1, "")),
        _mutate_matrix(lambda rows: rows[1].pop()),
        _mutate_manifest("selected_variant", "not_present"),
        _mutate_matrix(lambda rows: rows[0].__setitem__(slice(1, None), list(reversed(rows[0][1:])))),
        _mutate_manifest("n_trials_total", True),
        _mutate_manifest("n_trials_total", 3),
        _mutate_manifest("all_tested_and_abandoned_variants_counted", False),
        _mutate_manifest("input_bundle_sha256", "abc"),
        _mutate_matrix(lambda rows: rows[1].__setitem__(1, "0.123"), refresh_hash=False),
        _mutate_manifest("schema_version", 999),
        _mutate_manifest("status", "PROMOTION_READY"),
    ],
    ids=[
        "empty_rows", "invalid_date", "duplicate_date", "unsorted_dates", "start_bound",
        "end_bound", "non_finite", "missing_cell", "ragged_row", "selected_absent",
        "column_order", "trials_bool", "trials_less_than_columns", "incomplete_trial_attestation",
        "malformed_hash", "matrix_hash_mismatch", "schema", "status",
    ],
)
def test_invalid_packets_fail_closed_without_artifacts(tmp_path, mutate):
    _invoke_invalid(tmp_path, mutate)


def test_malformed_csv_is_refused_even_when_lenient_parser_would_accept_numbers(tmp_path):
    def malformed(matrix: Path, manifest_path: Path) -> None:
        lines = matrix.read_text(encoding="utf-8").splitlines()
        cells = lines[1].split(",")
        cells[1] = f'"{cells[1]}" '
        lines[1] = ",".join(cells)
        matrix.write_text("\n".join(lines) + "\n", encoding="utf-8")
        manifest = _load_manifest(manifest_path)
        manifest["matrix_sha256"] = _sha256(matrix)
        _write_manifest(manifest_path, manifest)

    _invoke_invalid(tmp_path, malformed)


@pytest.mark.parametrize(
    "cell",
    [" 0.1", "0.1 ", "1_0e-3", "1,23"],
    ids=["leading_whitespace", "trailing_whitespace", "underscore", "locale_comma"],
)
def test_return_cells_require_strict_decimal_or_scientific_grammar(tmp_path, cell):
    matrix, manifest_path = _packet(tmp_path)
    rows = _matrix_rows(matrix)
    rows[1][1] = cell
    _write_matrix(matrix, rows)
    manifest = _load_manifest(manifest_path)
    manifest["matrix_sha256"] = _sha256(matrix)
    _write_manifest(manifest_path, manifest)

    with pytest.raises(DiagnosticError, match="missing/non-numeric return"):
        diagnostic._load_packet(matrix.read_bytes(), manifest_path.read_bytes(), 4, 20)


def test_matrix_change_before_publication_is_refused_without_output(tmp_path, monkeypatch):
    matrix, manifest = _packet(tmp_path)
    output = tmp_path / "must_not_exist"
    original = diagnostic._revalidate_inputs

    def replace_then_revalidate(expected):
        matrix.write_bytes(matrix.read_bytes() + b"\n")
        return original(expected)

    monkeypatch.setattr(diagnostic, "_revalidate_inputs", replace_then_revalidate)
    with pytest.raises(DiagnosticError, match="matrix changed during execution"):
        run_diagnostic(matrix, manifest, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def test_manifest_change_before_publication_is_refused_without_output(tmp_path, monkeypatch):
    matrix, manifest = _packet(tmp_path)
    output = tmp_path / "must_not_exist"
    original = diagnostic._revalidate_inputs

    def replace_then_revalidate(expected):
        manifest.write_bytes(manifest.read_bytes() + b"\n")
        return original(expected)

    monkeypatch.setattr(diagnostic, "_revalidate_inputs", replace_then_revalidate)
    with pytest.raises(DiagnosticError, match="manifest changed during execution"):
        run_diagnostic(matrix, manifest, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


@pytest.mark.parametrize("changed_input", ["matrix", "manifest"])
def test_input_mutated_during_final_code_identity_is_refused(tmp_path, monkeypatch, changed_input):
    matrix, manifest = _packet(tmp_path)
    output = tmp_path / "must_not_exist"
    original = diagnostic._code_identity
    calls = 0

    def mutate_during_second_identity(project_root):
        nonlocal calls
        calls += 1
        if calls == 2:
            path = matrix if changed_input == "matrix" else manifest
            path.write_bytes(path.read_bytes() + b"\n")
        return original(project_root)

    monkeypatch.setattr(diagnostic, "_code_identity", mutate_during_second_identity)
    with pytest.raises(DiagnosticError, match=f"{changed_input} changed during execution"):
        run_diagnostic(matrix, manifest, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()
    assert not list(tmp_path.glob(".must_not_exist.tmp-*"))


@pytest.mark.parametrize("value", [True, 1.0], ids=["boolean", "float"])
def test_schema_version_requires_exact_integer_without_artifacts(tmp_path, value):
    _invoke_invalid(tmp_path, _mutate_manifest("schema_version", value))


def test_manifest_rejects_unknown_promotion_looking_field(tmp_path):
    _invoke_invalid(tmp_path, _mutate_manifest("promotion_approved", True))


@pytest.mark.parametrize("n_trials", [10**17, 10**400], ids=["large", "astronomical"])
def test_n_trials_total_above_bound_is_a_clear_diagnostic_error(tmp_path, n_trials):
    matrix, manifest_path = _packet(tmp_path)
    manifest = _load_manifest(manifest_path)
    manifest["n_trials_total"] = n_trials
    _write_manifest(manifest_path, manifest)

    with pytest.raises(DiagnosticError, match="n_trials_total .* maximum 1000000000"):
        diagnostic._load_packet(matrix.read_bytes(), manifest_path.read_bytes(), 4, 20)


def test_requires_at_least_two_variants(tmp_path):
    matrix, manifest_path = _packet(tmp_path, variants=1)
    output = tmp_path / "must_not_exist"
    with pytest.raises(DiagnosticError, match="at least two"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def test_rejects_too_few_observations_for_requested_blocks(tmp_path):
    matrix, manifest_path = _packet(tmp_path, rows=79)
    output = tmp_path / "must_not_exist"
    with pytest.raises(DiagnosticError, match="block"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def test_rejects_excessive_cscv_split_count_before_combinations(tmp_path, monkeypatch):
    matrix, manifest_path = _packet(tmp_path, rows=36)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("combinations must not be constructed")

    monkeypatch.setattr(diagnostic.itertools, "combinations", forbidden)
    with pytest.raises(DiagnosticError, match="CSCV split count .* exceeds maximum 10000"):
        diagnostic._load_packet(matrix.read_bytes(), manifest_path.read_bytes(), 18, 2)


def test_no_overwrite_preserves_existing_directory(tmp_path):
    matrix, manifest_path = _packet(tmp_path)
    output = tmp_path / "evidence"
    output.mkdir()
    sentinel = output / "keep.txt"
    sentinel.write_text("do not mutate", encoding="utf-8")

    with pytest.raises(DiagnosticError, match="overwrite"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)

    assert sentinel.read_text(encoding="utf-8") == "do not mutate"
    assert {path.name for path in output.iterdir()} == {"keep.txt"}


def test_keyboard_interrupt_during_generation_removes_temporary_evidence(tmp_path, monkeypatch):
    matrix, manifest_path = _packet(tmp_path)
    output = tmp_path / "must_not_exist"
    identity = {
        "code_sha": "a" * 40,
        "code_bundle_sha256": "b" * 64,
        "code_bundle_rule": "test identity",
    }
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: identity)
    monkeypatch.setattr(
        diagnostic,
        "_sha256",
        lambda _path: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)

    assert not output.exists()
    assert not list(tmp_path.glob(".must_not_exist.tmp-*"))


def test_outputs_are_deterministic_and_each_file_is_provenance_stamped(tmp_path, monkeypatch):
    identity = {
        "code_sha": "a" * 40,
        "code_bundle_sha256": "b" * 64,
        "code_bundle_rule": "test identity",
    }
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: identity)
    matrix, manifest_path = _packet(tmp_path)
    outputs = [tmp_path / "evidence_a", tmp_path / "evidence_b"]
    for output in outputs:
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)

    first_manifest = json.loads((outputs[0] / "artifact_manifest.json").read_text(encoding="utf-8"))
    second_manifest = json.loads((outputs[1] / "artifact_manifest.json").read_text(encoding="utf-8"))
    assert first_manifest["files"] == second_manifest["files"]
    assert first_manifest["composite_sha256"] == second_manifest["composite_sha256"]
    for name, expected_hash in first_manifest["files"].items():
        assert _sha256(outputs[0] / name) == expected_hash
    summary = json.loads((outputs[0] / "summary.json").read_text(encoding="utf-8"))
    with (outputs[0] / "pbo_splits.csv").open(encoding="utf-8", newline="") as handle:
        split = next(csv.DictReader(handle))
    for field in (
        "status", "code_sha", "matrix_sha256", "input_bundle_sha256",
        "experiment_config_sha256", "execution_contract_hash", "date_start", "date_end",
        "periods_per_year", "n_blocks", "min_block_observations", "variant_ids",
        "matrix_variant_count", "n_trials_total", "selected_variant",
        "all_tested_and_abandoned_variants_counted", "assumptions", "thresholds",
    ):
        assert field in summary
        assert field in first_manifest
        assert field in split
    assert summary["variant_ids"] == ["variant_0", "variant_1", "variant_2", "variant_3"]
    assert json.loads(split["variant_ids"]) == summary["variant_ids"]
    assert summary["matrix_variant_count"] == 4
    assert split["matrix_variant_count"] == "4"
    assert any(
        "dispersion is estimated only from supplied matrix columns" in assumption
        and "cannot be reconstructed" in assumption
        for assumption in summary["assumptions"]
    )


@pytest.mark.parametrize(
    "daily_sharpes",
    [np.array([0.1, 0.1, 0.1, 0.1]), np.array([0.1, 0.1 + 1e-15, 0.1, 0.1 - 1e-15])],
    ids=["exact", "near"],
)
def test_deflated_sharpe_refuses_zero_or_near_zero_cross_trial_dispersion(daily_sharpes):
    with pytest.raises(DiagnosticError, match="cross-trial Sharpe dispersion is zero or nearly zero"):
        diagnostic._expected_max_daily_sharpe(daily_sharpes, 4)


def test_one_sided_normal_tail_remains_finite_and_nonzero_at_high_z():
    tail = diagnostic._one_sided_normal_tail(9.0)

    assert math.isfinite(tail)
    assert 0.0 < tail < 1e-18


def test_synthetic_noise_is_vetoed_while_stable_edge_clears_secondary_gate(tmp_path):
    noise_dir = tmp_path / "noise"
    edge_dir = tmp_path / "edge"
    noise_dir.mkdir()
    edge_dir.mkdir()
    _, noise = _run(noise_dir, edge=False)
    _, edge = _run(edge_dir, edge=True)

    assert noise["verdict"] == "VETO_FURTHER_PROMOTION"
    assert edge["verdict"] == "NO_VETO_RESEARCH_ONLY_NOT_PROMOTION"
    assert edge["diagnostics"]["selected_annualized_sharpe"] > noise["diagnostics"]["selected_annualized_sharpe"]
    assert edge["diagnostics"]["deflated_sharpe_probability"] > noise["diagnostics"]["deflated_sharpe_probability"]
    forbidden = {"PASS", "APPROVE", "CANDIDATE"}
    assert not forbidden.intersection(json.dumps(edge).upper().replace('"', "").split())


def test_cscv_refuses_exact_zero_variance_split_without_artifacts(tmp_path):
    matrix, manifest_path = _packet(tmp_path)
    rows = _matrix_rows(matrix)
    for row in rows[1:81]:
        row[1] = "0.001"
    _write_matrix(matrix, rows)
    manifest = _load_manifest(manifest_path)
    manifest["matrix_sha256"] = _sha256(matrix)
    _write_manifest(manifest_path, manifest)
    output = tmp_path / "must_not_exist"

    with pytest.raises(DiagnosticError, match="CSCV split .* unevaluable"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def test_cscv_refuses_decimal_near_constant_split_without_artifacts(tmp_path):
    matrix, manifest_path = _packet(tmp_path)
    rows = _matrix_rows(matrix)
    for index, row in enumerate(rows[1:81]):
        row[1] = "0.0010000000001" if index % 2 else "0.0010000000002"
    _write_matrix(matrix, rows)
    manifest = _load_manifest(manifest_path)
    manifest["matrix_sha256"] = _sha256(matrix)
    _write_manifest(manifest_path, manifest)
    output = tmp_path / "must_not_exist"

    with pytest.raises(DiagnosticError, match="CSCV split .* unevaluable"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def _set_tied_variant_columns(matrix: Path, manifest_path: Path, perturbation: float) -> None:
    rows = _matrix_rows(matrix)
    for index, row in enumerate(rows[1:]):
        row[2] = f"{float(row[1]) + perturbation * (-1 if index % 2 else 1):.17g}"
    _write_matrix(matrix, rows)
    manifest = _load_manifest(manifest_path)
    manifest["matrix_sha256"] = _sha256(matrix)
    _write_manifest(manifest_path, manifest)


def test_cscv_refuses_identical_nonconstant_variant_scores_without_artifacts(tmp_path):
    matrix, manifest_path = _packet(tmp_path)
    _set_tied_variant_columns(matrix, manifest_path, 0.0)
    output = tmp_path / "must_not_exist"

    with pytest.raises(DiagnosticError, match="CSCV split .* tied .* scores"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def test_cscv_refuses_numerically_near_tied_variant_scores_without_artifacts(tmp_path):
    matrix, manifest_path = _packet(tmp_path)
    _set_tied_variant_columns(matrix, manifest_path, 1e-15)
    output = tmp_path / "must_not_exist"

    with pytest.raises(DiagnosticError, match="CSCV split .* tied .* scores"):
        run_diagnostic(matrix, manifest_path, output, n_blocks=4, min_block_observations=20)
    assert not output.exists()


def test_rejects_unequal_cscv_blocks_instead_of_dropping_observations(tmp_path):
    matrix, manifest_path = _packet(tmp_path, rows=161)
    with pytest.raises(DiagnosticError, match="divisible"):
        run_diagnostic(
            matrix, manifest_path, tmp_path / "must_not_exist",
            n_blocks=4, min_block_observations=20,
        )


def test_cli_runs_offline_and_invalid_input_returns_nonzero_without_output(tmp_path, monkeypatch, capsys):
    from scripts import research_overfit_diagnostic

    matrix, manifest_path = _packet(tmp_path)
    output = tmp_path / "evidence"
    monkeypatch.setattr(
        "sys.argv",
        [
            "research_overfit_diagnostic.py", "--matrix", str(matrix), "--manifest",
            str(manifest_path), "--output", str(output), "--blocks", "4",
            "--min-block-observations", "20",
        ],
    )
    assert research_overfit_diagnostic.main() == 0
    assert output.is_dir()
    assert json.loads(capsys.readouterr().out)["status"] == STATUS

    bad_output = tmp_path / "bad_evidence"
    manifest = _load_manifest(manifest_path)
    manifest["status"] = "NOT_RESEARCH_ONLY"
    _write_manifest(manifest_path, manifest)
    monkeypatch.setattr(
        "sys.argv",
        [
            "research_overfit_diagnostic.py", "--matrix", str(matrix), "--manifest",
            str(manifest_path), "--output", str(bad_output), "--blocks", "4",
            "--min-block-observations", "20",
        ],
    )
    assert research_overfit_diagnostic.main() != 0
    assert not bad_output.exists()


@pytest.mark.parametrize("n_trials", [10**17, 10**400], ids=["large", "enormous"])
def test_cli_refuses_excessive_trial_count_without_traceback_or_output(
    tmp_path, monkeypatch, capsys, n_trials
):
    from scripts import research_overfit_diagnostic

    matrix, manifest_path = _packet(tmp_path)
    manifest = _load_manifest(manifest_path)
    manifest["n_trials_total"] = n_trials
    _write_manifest(manifest_path, manifest)
    output = tmp_path / "must_not_exist"
    monkeypatch.setattr(
        "sys.argv",
        [
            "research_overfit_diagnostic.py", "--matrix", str(matrix), "--manifest",
            str(manifest_path), "--output", str(output), "--blocks", "4",
            "--min-block-observations", "20",
        ],
    )

    assert research_overfit_diagnostic.main() == 2
    captured = capsys.readouterr()
    assert "exceeds maximum 1000000000" in captured.err
    assert "Traceback" not in captured.err
    assert not output.exists()


def test_threshold_policy_may_only_be_tightened(tmp_path):
    matrix, manifest_path = _packet(tmp_path)
    weakened = dict(
        min_selected_annualized_sharpe=-100.0,
        min_deflated_sharpe_probability=0.0,
        max_pbo=1.0,
        max_bonferroni_p_value=1.0,
    )
    with pytest.raises(DiagnosticError, match="weaken"):
        run_diagnostic(
            matrix, manifest_path, tmp_path / "must_not_exist",
            n_blocks=4, min_block_observations=20, thresholds=weakened,
        )
    assert not (tmp_path / "must_not_exist").exists()
