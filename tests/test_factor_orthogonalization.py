"""Adversarial contracts for the offline research-only factor orthogonalizer."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

import src.research.factor_orthogonalization as diagnostic
from src.research.factor_orthogonalization import DiagnosticError, _code_identity, run_diagnostic

STATUS = "RESEARCH_ONLY_NON_BINDING"
IDENTITY = {"code_sha": "a" * 40, "code_bundle_sha256": "b" * 64, "code_bundle_rule": "test"}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _packet(
    root: Path,
    *,
    dates: int = 2,
    per_date: int = 60,
    control_count: int = 5,
    factor_mode: str = "exposed",
) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    controls = ["size", "liquidity", "beta", "volatility"] + [
        f"existing_{index}" for index in range(control_count - 4)
    ]
    matrix = root / "matrix.csv"
    rng = np.random.default_rng(20260901)
    start = date(2025, 1, 2)
    rows = []
    for day_index in range(dates):
        values = rng.normal(size=(per_date, control_count))
        industries = np.asarray(["bank", "energy", "tech"] * ((per_date + 2) // 3))[:per_date]
        noise = rng.normal(scale=0.22, size=per_date)
        factor = 2.8 * values[:, 0] - 1.9 * values[:, 1] + 0.8 * values[:, 2] + noise
        factor += np.asarray([1.2 if item == "tech" else -0.7 if item == "bank" else 0 for item in industries])
        if factor_mode == "zero_factor":
            factor[:] = 1.0
        elif factor_mode == "near_factor":
            factor = 1.0 + np.arange(per_date) % 2 * 1e-15
        elif factor_mode == "zero_control":
            values[:, 0] = 1.0
        elif factor_mode == "near_control":
            values[:, 0] = 1.0 + np.arange(per_date) % 2 * 1e-15
        elif factor_mode == "rank_deficient":
            values[:, 1] = values[:, 0]
        elif factor_mode == "zero_residual":
            factor = values[:, 0].copy()
        elif factor_mode == "near_residual":
            factor = values[:, 0] + (np.arange(per_date) % 2) * 1e-15
        for index in range(per_date):
            rows.append([
                (start + timedelta(days=day_index)).isoformat(), f"S{index:04d}",
                f"{factor[index]:.17g}", industries[index],
                *[f"{value:.17g}" for value in values[index]],
            ])
    with matrix.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["date", "symbol", "factor_value", "industry", *controls])
        writer.writerows(rows)
    manifest = {
        "schema_version": 1,
        "status": STATUS,
        "factor_id": "synthetic_exposure",
        "market": "US_EQUITIES_PIT",
        "date_start": rows[0][0],
        "date_end": rows[-1][0],
        "row_count": len(rows),
        "date_count": dates,
        "industry_column": "industry",
        "continuous_controls": controls,
        "control_roles": {
            "size": "size", "liquidity": "liquidity", "beta": "beta",
            "volatility": "volatility", "existing_factors": controls[4:],
        },
        "point_in_time_industry_attestation": True,
        "controls_known_by_signal_cutoff_attestation": True,
        "factor_known_by_signal_cutoff_attestation": True,
        "winsor_lower": 0.01,
        "winsor_upper": 0.99,
        "min_cross_section": 50,
        "max_condition_number": 1e6,
        "max_abs_residual_exposure": 1e-8,
        "matrix_sha256": _sha(matrix),
        "input_bundle_sha256": "1" * 64,
        "experiment_config_sha256": "2" * 64,
        "research_contract_sha256": "3" * 64,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return matrix, manifest_path


def _manifest(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_manifest(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _rows(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.reader(handle))


def _write_rows(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        csv.writer(handle, lineterminator="\n").writerows(rows)


def _run(root: Path, monkeypatch, **kwargs):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(root, **kwargs)
    output = root / "evidence"
    result = run_diagnostic(matrix, manifest, output)
    return matrix, manifest, output, result


def test_synthetic_exposure_is_removed_and_outputs_are_exact(tmp_path, monkeypatch):
    matrix, _, output, summary = _run(tmp_path, monkeypatch)
    assert summary["verdict"] == "DIAGNOSTIC_ONLY_NOT_SELECTION"
    assert {path.name for path in output.iterdir()} == {
        "residuals.csv", "date_diagnostics.csv", "summary.json", "artifact_manifest.json"
    }
    with matrix.open(encoding="utf-8", newline="") as handle:
        source = list(csv.DictReader(handle))
    with (output / "residuals.csv").open(encoding="utf-8", newline="") as handle:
        residuals = list(csv.DictReader(handle))
    assert [(row["date"], row["symbol"]) for row in residuals] == [
        (row["date"], row["symbol"]) for row in source
    ]
    assert [float(row["raw_factor"]) for row in residuals] == [float(row["factor_value"]) for row in source]
    raw = np.asarray([float(row["raw_factor"]) for row in residuals[:60]])
    size = np.asarray([float(row["size"]) for row in source[:60]])
    preprocessed = np.asarray([float(row["preprocessed_factor"]) for row in residuals[:60]])
    residual = np.asarray([float(row["residual_factor"]) for row in residuals[:60]])
    preprocessed_size = diagnostic._winsor_zscore(size, "size")
    assert abs(np.corrcoef(raw, size)[0, 1]) > 0.6
    assert abs(np.corrcoef(preprocessed, preprocessed_size)[0, 1]) > 0.6
    assert abs(np.corrcoef(residual, preprocessed_size)[0, 1]) < 1e-8
    assert summary["diagnostics"]["max_abs_industry_residual_mean"] < 1e-8


def test_residual_is_exact_affine_normalization_of_ols_remainder_not_rewinsorized(tmp_path, monkeypatch):
    matrix, manifest_path, output, _ = _run(tmp_path, monkeypatch)
    manifest, parsed, dates, controls, limit, tolerance = diagnostic._load_packet(
        matrix.read_bytes(), manifest_path.read_bytes()
    )
    group = [item for item in parsed if item[0] == dates[0]]
    factor = diagnostic._winsor_zscore(np.asarray([item[2] for item in group]), "factor")
    raw_controls = np.asarray([item[4] for item in group])
    standardized = np.column_stack([
        diagnostic._winsor_zscore(raw_controls[:, index], name) for index, name in enumerate(controls)
    ])
    industries = sorted({item[3] for item in group})
    dummies = np.column_stack([[float(item[3] == category) for item in group] for category in industries[1:]])
    design = np.column_stack([np.ones(len(group)), standardized, dummies])
    expected = factor - design @ np.linalg.lstsq(design, factor, rcond=None)[0]
    expected = (expected - expected.mean()) / expected.std(ddof=1)
    with (output / "residuals.csv").open(encoding="utf-8", newline="") as handle:
        actual = np.asarray([float(row["residual_factor"]) for row in list(csv.DictReader(handle))[:60]])
    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-14)


def _invalid(root: Path, monkeypatch, mutate, **packet_kwargs):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest_path = _packet(root, **packet_kwargs)
    mutate(matrix, manifest_path)
    output = root / "must_not_exist"
    with pytest.raises(DiagnosticError):
        run_diagnostic(matrix, manifest_path, output)
    assert not os.path.lexists(output)
    assert not list(root.glob(".must_not_exist.tmp-*"))


def _manifest_change(field, value):
    def change(_matrix, path):
        manifest = _manifest(path)
        if value is _DELETE:
            del manifest[field]
        else:
            manifest[field] = value
        _write_manifest(path, manifest)
    return change


_DELETE = object()


@pytest.mark.parametrize("field,value", [
    ("schema_version", True), ("schema_version", 2), ("status", "PASS"), ("factor_id", ""),
    ("market", "CRYPTO"), ("date_start", "2025/01/02"), ("date_end", "2025-1-3"),
    ("row_count", True), ("date_count", 2.0), ("min_cross_section", 29),
    ("industry_column", ""), ("continuous_controls", ["size", "liquidity", "beta"]),
    ("continuous_controls", ["size", "liquidity", "beta", "size"]),
    ("continuous_controls", ["size", "liquidity", "beta", ""]),
    ("continuous_controls", ["size", "liquidity", "beta", "factor_value"]),
    ("winsor_lower", 0.02), ("winsor_upper", 0.98), ("max_condition_number", True),
    ("max_condition_number", float("inf")),
    ("max_condition_number", 1e9), ("max_abs_residual_exposure", 0.0),
    ("max_abs_residual_exposure", 1e-7), ("matrix_sha256", "ABC"),
    ("input_bundle_sha256", "A" * 64), ("experiment_config_sha256", "x" * 64),
    ("research_contract_sha256", "3" * 63),
    ("point_in_time_industry_attestation", False),
    ("controls_known_by_signal_cutoff_attestation", False),
    ("factor_known_by_signal_cutoff_attestation", False),
    ("factor_id", _DELETE), ("promotion_approved", True),
], ids=lambda item: str(item))
def test_manifest_exact_schema_types_policy_attestations_fail_closed(tmp_path, monkeypatch, field, value):
    _invalid(tmp_path, monkeypatch, _manifest_change(field, value))


@pytest.mark.parametrize("roles", [
    {"size": "size"},
    {"size": "size", "liquidity": "size", "beta": "beta", "volatility": "volatility", "existing_factors": ["existing_0"]},
    {"size": "missing", "liquidity": "liquidity", "beta": "beta", "volatility": "volatility", "existing_factors": ["existing_0"]},
    {"size": "size", "liquidity": "liquidity", "beta": "beta", "volatility": "volatility", "existing_factors": []},
    {"size": "size", "liquidity": "liquidity", "beta": "beta", "volatility": "volatility", "existing_factors": ["existing_0"], "other": "size"},
])
def test_control_roles_are_exact_distinct_present_and_ordered(tmp_path, monkeypatch, roles):
    _invalid(tmp_path, monkeypatch, _manifest_change("control_roles", roles))


@pytest.mark.parametrize("change", [
    "header", "unsorted", "duplicate", "bad_date", "empty_symbol", "empty_industry",
    "missing_cell", "whitespace", "underscore", "nan", "hash", "row_count", "date_count",
])
def test_matrix_contract_and_counts_fail_closed(tmp_path, monkeypatch, change):
    def mutate(matrix, manifest_path):
        rows = _rows(matrix)
        manifest = _manifest(manifest_path)
        if change == "header": rows[0][2] = "factor"
        elif change == "unsorted": rows[1], rows[2] = rows[2], rows[1]
        elif change == "duplicate": rows[2][1] = rows[1][1]
        elif change == "bad_date": rows[1][0] = "2025/01/02"
        elif change == "empty_symbol": rows[1][1] = ""
        elif change == "empty_industry": rows[1][3] = ""
        elif change == "missing_cell": rows[1].pop()
        elif change == "whitespace": rows[1][2] = " 0.1"
        elif change == "underscore": rows[1][2] = "1_0"
        elif change == "nan": rows[1][2] = "nan"
        elif change == "row_count": manifest["row_count"] += 1
        elif change == "date_count": manifest["date_count"] += 1
        if change not in {"row_count", "date_count", "hash"}:
            _write_rows(matrix, rows)
            manifest["matrix_sha256"] = _sha(matrix)
        elif change == "hash":
            matrix.write_bytes(matrix.read_bytes() + b"\n")
        _write_manifest(manifest_path, manifest)
    _invalid(tmp_path, monkeypatch, mutate)


@pytest.mark.parametrize("mode,match", [
    ("zero_factor", "factor.*dispersion"), ("near_factor", "factor.*dispersion"),
    ("zero_control", "control size.*dispersion"), ("near_control", "control size.*dispersion"),
    ("rank_deficient", "rank deficient"), ("zero_residual", "residual.*dispersion"),
    ("near_residual", "residual.*dispersion"),
])
def test_numerically_unevaluable_designs_are_refused(tmp_path, monkeypatch, mode, match):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(tmp_path, factor_mode=mode)
    with pytest.raises(DiagnosticError, match=match):
        run_diagnostic(matrix, manifest, tmp_path / "none")
    assert not (tmp_path / "none").exists()


def test_too_few_rows_and_industries_are_refused(tmp_path, monkeypatch):
    def too_few(_matrix, manifest_path):
        manifest = _manifest(manifest_path); manifest["min_cross_section"] = 61; _write_manifest(manifest_path, manifest)
    _invalid(tmp_path / "few", monkeypatch, too_few)
    def one_industry(matrix, manifest_path):
        rows = _rows(matrix)
        for row in rows[1:]: row[3] = "only"
        _write_rows(matrix, rows)
        manifest = _manifest(manifest_path); manifest["matrix_sha256"] = _sha(matrix); _write_manifest(manifest_path, manifest)
    _invalid(tmp_path / "industry", monkeypatch, one_industry)


def test_columns_must_be_fewer_than_rows(tmp_path, monkeypatch):
    def minimum(_matrix, manifest_path):
        manifest = _manifest(manifest_path); manifest["min_cross_section"] = 30; _write_manifest(manifest_path, manifest)
    _invalid(tmp_path, monkeypatch, minimum, per_date=30, dates=1, control_count=28)


def test_condition_number_threshold_is_enforced(tmp_path, monkeypatch):
    _invalid(tmp_path, monkeypatch, _manifest_change("max_condition_number", 1.0))


def test_deterministic_hashes_and_exact_verdict_vocabulary(tmp_path, monkeypatch):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(tmp_path)
    outputs = [tmp_path / "a", tmp_path / "b"]
    for output in outputs: run_diagnostic(matrix, manifest, output)
    manifests = [json.loads((item / "artifact_manifest.json").read_text()) for item in outputs]
    assert manifests[0]["files"] == manifests[1]["files"]
    assert manifests[0]["composite_sha256"] == manifests[1]["composite_sha256"]
    assert set(manifests[0]["files"]) == {"residuals.csv", "date_diagnostics.csv", "summary.json"}
    for name, digest in manifests[0]["files"].items(): assert _sha(outputs[0] / name) == digest
    combined = "".join(path.read_text() for path in outputs[0].iterdir())
    assert "DIAGNOSTIC_ONLY_NOT_SELECTION" in combined
    assert all(word not in combined for word in ['"PASS"', '"APPROVE"', '"CANDIDATE"'])
    summary = json.loads((outputs[0] / "summary.json").read_text())
    for field in ("factor_id", "market", "control_roles", "date_start", "date_end", "row_count", "date_count", "assumptions", "code_sha"):
        assert field in summary and field in manifests[0]
    assert "not validated alpha/OOS" in summary["restriction"]


@pytest.mark.parametrize("kind", ["directory", "file", "dangling_symlink", "symlink"])
def test_output_no_overwrite_preserves_every_existing_destination(tmp_path, monkeypatch, kind):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(tmp_path)
    output = tmp_path / "evidence"
    target = tmp_path / "target"
    if kind == "directory": output.mkdir(); (output / "keep").write_text("safe")
    elif kind == "file": output.write_text("safe")
    elif kind == "dangling_symlink": output.symlink_to(target, target_is_directory=True)
    else: target.mkdir(); (target / "keep").write_text("safe"); output.symlink_to(target, target_is_directory=True)
    with pytest.raises(DiagnosticError, match="overwrite"):
        run_diagnostic(matrix, manifest, output)
    assert os.path.lexists(output)
    assert not list(tmp_path.glob(".evidence.tmp-*"))


def test_keyboard_interrupt_cleans_temp_and_no_partial_output(tmp_path, monkeypatch):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(tmp_path)
    monkeypatch.setattr(diagnostic, "_sha256", lambda _path: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt): run_diagnostic(matrix, manifest, tmp_path / "evidence")
    assert not (tmp_path / "evidence").exists()
    assert not list(tmp_path.glob(".evidence.tmp-*"))


@pytest.mark.parametrize("which", ["matrix", "manifest"])
def test_input_toctou_at_final_boundary_is_refused(tmp_path, monkeypatch, which):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(tmp_path)
    original = diagnostic._revalidate_inputs
    def mutate(expected):
        path = matrix if which == "matrix" else manifest
        path.write_bytes(path.read_bytes() + b"\n")
        original(expected)
    monkeypatch.setattr(diagnostic, "_revalidate_inputs", mutate)
    with pytest.raises(DiagnosticError, match=f"{which} changed"):
        run_diagnostic(matrix, manifest, tmp_path / "evidence")
    assert not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("which", ["matrix", "manifest"])
def test_input_symlink_retarget_at_final_boundary_is_refused(tmp_path, monkeypatch, which):
    monkeypatch.setattr(diagnostic, "_code_identity", lambda _root: IDENTITY)
    matrix, manifest = _packet(tmp_path)
    original_path = matrix if which == "matrix" else manifest
    replacement = tmp_path / f"replacement-{which}"
    replacement.write_bytes(original_path.read_bytes() + b"\n")
    lexical = tmp_path / f"{which}-link"
    lexical.symlink_to(original_path)
    original_revalidate = diagnostic._revalidate_inputs

    def retarget(expected):
        lexical.unlink()
        lexical.symlink_to(replacement)
        original_revalidate(expected)

    monkeypatch.setattr(diagnostic, "_revalidate_inputs", retarget)
    matrix_arg = lexical if which == "matrix" else matrix
    manifest_arg = lexical if which == "manifest" else manifest
    with pytest.raises(DiagnosticError, match=f"{which} changed"):
        run_diagnostic(matrix_arg, manifest_arg, tmp_path / "evidence")
    assert lexical.resolve() == replacement.resolve()
    assert not (tmp_path / "evidence").exists()
    assert not list(tmp_path.glob(".evidence.tmp-*"))


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"; (repo / "src").mkdir(parents=True); (repo / "scripts").mkdir()
    (repo / "src" / "a.py").write_text("VALUE = 1\n"); (repo / "scripts" / "b.py").write_text("print('x')\n")
    _git(repo, "init", "-q"); _git(repo, "config", "user.email", "t@example.com"); _git(repo, "config", "user.name", "T")
    _git(repo, "add", "."); _git(repo, "commit", "-qm", "base"); return repo


def test_code_identity_binds_head_and_refuses_dirty_staged_ignored_and_symlink_dirs(tmp_path):
    for index, mutation in enumerate(("dirty", "staged", "ignored", "symlink_dir")):
        repo = _repo(tmp_path / str(index)); assert _code_identity(repo)["code_sha"] == _git(repo, "rev-parse", "HEAD")
        if mutation == "dirty": (repo / "src" / "a.py").write_text("VALUE = 2\n")
        elif mutation == "staged": (repo / "scripts" / "c.py").write_text("X=1\n"); _git(repo, "add", "scripts/c.py")
        elif mutation == "ignored": (repo / ".gitignore").write_text("src/x.py\n"); (repo / "src" / "x.py").write_text("X=1\n")
        else:
            external = tmp_path / f"external{index}"; external.mkdir(); (repo / "src" / "plugin").symlink_to(external, target_is_directory=True)
        with pytest.raises(DiagnosticError): _code_identity(repo)


def test_code_identity_refuses_committed_python_symlink(tmp_path):
    repo = _repo(tmp_path)
    external = tmp_path / "external.py"
    external.write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "src" / "linked.py").symlink_to(external)
    _git(repo, "add", "src/linked.py")
    _git(repo, "commit", "-qm", "add linked python")

    with pytest.raises(DiagnosticError, match="symlinked Python file"):
        _code_identity(repo)


def test_code_identity_mutation_at_final_boundary_is_refused(tmp_path, monkeypatch):
    matrix, manifest = _packet(tmp_path)
    calls = 0
    def identity(_root):
        nonlocal calls; calls += 1
        return IDENTITY if calls == 1 else {**IDENTITY, "code_bundle_sha256": "c" * 64}
    monkeypatch.setattr(diagnostic, "_code_identity", identity)
    with pytest.raises(DiagnosticError, match="executable project files changed"):
        run_diagnostic(matrix, manifest, tmp_path / "evidence")
    assert not (tmp_path / "evidence").exists()


def test_cli_returns_two_without_artifacts_on_refusal(tmp_path, monkeypatch, capsys):
    from scripts import research_factor_orthogonalization
    matrix, manifest = _packet(tmp_path); data = _manifest(manifest); data["status"] = "PASS"; _write_manifest(manifest, data)
    monkeypatch.setattr("sys.argv", ["orthogonalize", "--matrix", str(matrix), "--manifest", str(manifest), "--output", str(tmp_path / "out")])
    assert research_factor_orthogonalization.main() == 2
    assert "refused" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()
