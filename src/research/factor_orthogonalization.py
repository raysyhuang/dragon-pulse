"""Offline, non-binding cross-sectional factor orthogonalization diagnostic."""

from __future__ import annotations

import csv
import ctypes
import hashlib
import io
import json
import math
import os
import re
import shutil
import stat
import subprocess
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import numpy as np

STATUS = "RESEARCH_ONLY_NON_BINDING"
VERDICT = "DIAGNOSTIC_ONLY_NOT_SELECTION"
NUMERIC_PATTERN = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")
HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
REQUIRED_ROLES = {"size", "liquidity", "beta", "volatility", "existing_factors"}
ASSUMPTIONS = [
    "Industry labels are point-in-time only by operator attestation; this diagnostic does not independently prove them.",
    "Controls and factor values are known by signal cutoff only by operator attestation.",
    "Residual orthogonality is in-sample, same-date, and linear after the declared preprocessing.",
    "Residual factor is not validated alpha or out-of-sample evidence and cannot enter any selector without a separately preregistered outcome study, replay, independent review, and forward paper gate.",
    "Publication is namespace-atomic and no-overwrite, but does not claim fsync crash durability or cleanup after SIGKILL/machine loss.",
    "Intermediate output-parent symlinks are outside the final-component lexical no-symlink boundary.",
]


class DiagnosticError(ValueError):
    """The supplied packet cannot be evaluated under the frozen contract."""


def _strict_json(data: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise DiagnosticError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            data,
            object_pairs_hook=pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(
                DiagnosticError(f"non-finite JSON value: {token}")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise DiagnosticError(f"unparseable manifest: {exc}") from exc
    if not isinstance(value, dict):
        raise DiagnosticError("manifest must be a JSON object")
    return value


def _iso_date(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise DiagnosticError(f"{field} must be a canonical YYYY-MM-DD date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DiagnosticError(f"{field} must be a canonical YYYY-MM-DD date") from exc
    if parsed.isoformat() != value:
        raise DiagnosticError(f"{field} must be a canonical YYYY-MM-DD date")
    return value


def _finite_number(value: object, field: str, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise DiagnosticError(f"{field} must be a finite number")
    result = float(value)
    if result < minimum or result > maximum:
        raise DiagnosticError(f"{field} is outside the allowed range")
    return result


def _load_packet(matrix_bytes: bytes, manifest_bytes: bytes):
    manifest = _strict_json(manifest_bytes)
    required = {
        "schema_version", "status", "factor_id", "market", "date_start", "date_end",
        "row_count", "date_count", "industry_column", "continuous_controls", "control_roles",
        "point_in_time_industry_attestation", "controls_known_by_signal_cutoff_attestation",
        "factor_known_by_signal_cutoff_attestation", "winsor_lower", "winsor_upper",
        "min_cross_section", "max_condition_number", "max_abs_residual_exposure",
        "matrix_sha256", "input_bundle_sha256", "experiment_config_sha256",
        "research_contract_sha256",
    }
    missing = required - set(manifest)
    unknown = set(manifest) - required
    if missing:
        raise DiagnosticError(f"manifest missing required fields: {', '.join(sorted(missing))}")
    if unknown:
        raise DiagnosticError(f"manifest contains unknown fields: {', '.join(sorted(unknown))}")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise DiagnosticError("unsupported schema_version")
    if manifest["status"] != STATUS:
        raise DiagnosticError(f"unsupported status; required {STATUS}")
    if not isinstance(manifest["factor_id"], str) or not manifest["factor_id"]:
        raise DiagnosticError("factor_id must be a non-empty string")
    if manifest["market"] not in {"A_SHARE_TUSHARE", "US_EQUITIES_PIT"}:
        raise DiagnosticError("unsupported market")
    start = _iso_date(manifest["date_start"], "date_start")
    end = _iso_date(manifest["date_end"], "date_end")
    if start > end:
        raise DiagnosticError("date_start must not exceed date_end")
    for field in ("row_count", "date_count", "min_cross_section"):
        value = manifest[field]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise DiagnosticError(f"{field} must be a positive integer")
    if manifest["min_cross_section"] < 30:
        raise DiagnosticError("min_cross_section must be at least 30")
    industry = manifest["industry_column"]
    if not isinstance(industry, str) or not industry or industry in {"date", "symbol", "factor_value"}:
        raise DiagnosticError("industry_column must be a non-empty non-reserved string")
    controls = manifest["continuous_controls"]
    if (
        not isinstance(controls, list) or len(controls) < 4
        or any(not isinstance(item, str) or not item for item in controls)
        or len(set(controls)) != len(controls)
        or any(item in {"date", "symbol", "factor_value", industry} for item in controls)
    ):
        raise DiagnosticError("continuous_controls must be at least four unique non-empty non-reserved names")
    roles = manifest["control_roles"]
    if not isinstance(roles, dict) or set(roles) != REQUIRED_ROLES:
        raise DiagnosticError("control_roles must contain the exact schema 1 role keys")
    primary = [roles[name] for name in ("size", "liquidity", "beta", "volatility")]
    existing = roles["existing_factors"]
    if (
        any(not isinstance(item, str) or item not in controls for item in primary)
        or len(set(primary)) != 4
        or not isinstance(existing, list)
        or any(not isinstance(item, str) or item not in controls for item in existing)
        or len(set(existing)) != len(existing)
        or set(primary).intersection(existing)
        or existing != [item for item in controls if item not in primary]
    ):
        raise DiagnosticError("control_roles must map distinct controls and existing_factors in manifest order")
    for field in (
        "point_in_time_industry_attestation", "controls_known_by_signal_cutoff_attestation",
        "factor_known_by_signal_cutoff_attestation",
    ):
        if manifest[field] is not True:
            raise DiagnosticError(f"{field} must be true")
    if type(manifest["winsor_lower"]) not in (int, float) or manifest["winsor_lower"] != 0.01:
        raise DiagnosticError("schema 1 winsor_lower must equal 0.01")
    if type(manifest["winsor_upper"]) not in (int, float) or manifest["winsor_upper"] != 0.99:
        raise DiagnosticError("schema 1 winsor_upper must equal 0.99")
    max_condition = _finite_number(
        manifest["max_condition_number"], "max_condition_number", minimum=1.0, maximum=1e8
    )
    tolerance = _finite_number(
        manifest["max_abs_residual_exposure"], "max_abs_residual_exposure",
        minimum=np.nextafter(0.0, 1.0), maximum=1e-8,
    )
    for field in (
        "matrix_sha256", "input_bundle_sha256", "experiment_config_sha256",
        "research_contract_sha256",
    ):
        if not isinstance(manifest[field], str) or HASH_PATTERN.fullmatch(manifest[field]) is None:
            raise DiagnosticError(f"{field} must be a lowercase SHA-256 digest")
    actual_hash = hashlib.sha256(matrix_bytes).hexdigest()
    if actual_hash != manifest["matrix_sha256"]:
        raise DiagnosticError(f"matrix hash mismatch: expected {manifest['matrix_sha256']}, actual {actual_hash}")
    try:
        rows = list(csv.reader(io.StringIO(matrix_bytes.decode("utf-8"), newline=""), strict=True))
    except (UnicodeError, csv.Error) as exc:
        raise DiagnosticError(f"unparseable matrix CSV: {exc}") from exc
    expected_header = ["date", "symbol", "factor_value", industry, *controls]
    if not rows or rows[0] != expected_header:
        raise DiagnosticError("matrix header/order differs from manifest contract")
    if len(rows) == 1:
        raise DiagnosticError("matrix CSV has no observations")
    parsed = []
    keys = []
    for row_number, row in enumerate(rows[1:], 2):
        if len(row) != len(expected_header):
            raise DiagnosticError(f"matrix row {row_number} has missing or extra cells")
        row_date = _iso_date(row[0], f"matrix row {row_number} date")
        symbol = row[1]
        if not symbol or symbol.strip() != symbol:
            raise DiagnosticError(f"matrix row {row_number} symbol must be non-empty without surrounding whitespace")
        category = row[3]
        if not category:
            raise DiagnosticError(f"matrix row {row_number} industry must be non-empty")
        numbers = []
        for cell in [row[2], *row[4:]]:
            if NUMERIC_PATTERN.fullmatch(cell) is None:
                raise DiagnosticError(f"matrix row {row_number} has a non-strict numeric cell")
            number = float(cell)
            if not math.isfinite(number):
                raise DiagnosticError(f"matrix row {row_number} has a non-finite numeric cell")
            numbers.append(number)
        keys.append((row_date, symbol))
        parsed.append((row_date, symbol, numbers[0], category, numbers[1:]))
    if keys != sorted(keys) or len(set(keys)) != len(keys):
        raise DiagnosticError("matrix rows must be unique and strictly sorted by (date,symbol)")
    dates = sorted(set(item[0] for item in parsed))
    if dates[0] != start or dates[-1] != end:
        raise DiagnosticError("matrix date range does not match manifest bounds")
    if len(parsed) != manifest["row_count"] or len(dates) != manifest["date_count"]:
        raise DiagnosticError("matrix row/date counts do not match manifest")
    return manifest, parsed, dates, controls, max_condition, tolerance


def _near_zero(values: np.ndarray) -> bool:
    scale = max(1.0, float(np.max(np.abs(values))))
    return float(np.std(values, ddof=1)) <= 1e-12 * scale


def _winsor_zscore(values: np.ndarray, label: str) -> np.ndarray:
    lower, upper = np.quantile(values, [0.01, 0.99], method="linear")
    clipped = np.clip(values, lower, upper)
    if _near_zero(clipped):
        raise DiagnosticError(f"{label} has zero or nearly-zero dispersion after winsorization")
    return (clipped - np.mean(clipped)) / np.std(clipped, ddof=1)


def _orthogonalize(manifest, parsed, dates, controls, max_condition, tolerance):
    residual_rows = []
    diagnostics = []
    by_date = {day: [] for day in dates}
    for item in parsed:
        by_date[item[0]].append(item)
    for day in dates:
        group = by_date[day]
        n_obs = len(group)
        industries = sorted({item[3] for item in group})
        if n_obs < manifest["min_cross_section"]:
            raise DiagnosticError(f"{day} has fewer than min_cross_section observations")
        if len(industries) < 2:
            raise DiagnosticError(f"{day} must contain at least two industries")
        raw_factor = np.asarray([item[2] for item in group], dtype=float)
        factor = _winsor_zscore(raw_factor, f"{day} factor")
        raw_controls = np.asarray([item[4] for item in group], dtype=float)
        standardized = np.column_stack([
            _winsor_zscore(raw_controls[:, index], f"{day} control {name}")
            for index, name in enumerate(controls)
        ])
        dummies = np.column_stack([
            np.asarray([1.0 if item[3] == category else 0.0 for item in group])
            for category in industries[1:]
        ])
        design = np.column_stack([np.ones(n_obs), standardized, dummies])
        columns = design.shape[1]
        if columns >= n_obs:
            raise DiagnosticError(f"{day} design columns must be fewer than observations")
        if not np.all(np.isfinite(design)) or not np.all(np.isfinite(factor)):
            raise DiagnosticError(f"{day} design contains non-finite values")
        rank = int(np.linalg.matrix_rank(design))
        if rank != columns:
            raise DiagnosticError(f"{day} design is rank deficient")
        condition = float(np.linalg.cond(design))
        if not math.isfinite(condition) or condition > max_condition:
            raise DiagnosticError(f"{day} design condition number exceeds threshold")
        coefficients, _, lstsq_rank, _ = np.linalg.lstsq(design, factor, rcond=None)
        if int(lstsq_rank) != columns:
            raise DiagnosticError(f"{day} least-squares rank is deficient")
        fitted = design @ coefficients
        residual = factor - fitted
        if _near_zero(residual):
            raise DiagnosticError(f"{day} residual has zero or nearly-zero dispersion")
        residual = (residual - np.mean(residual)) / np.std(residual, ddof=1)
        correlations = np.corrcoef(np.column_stack([residual, standardized]), rowvar=False)[0, 1:]
        max_corr = float(np.max(np.abs(correlations)))
        group_means = [
            abs(float(np.mean(residual[[item[3] == category for item in group]])))
            for category in industries
        ]
        max_group_mean = max(group_means)
        if max_corr > tolerance or max_group_mean > tolerance:
            raise DiagnosticError(f"{day} residual exposure exceeds manifest tolerance")
        total_ss = float(np.sum((factor - np.mean(factor)) ** 2))
        residual_ss = float(np.sum((factor - fitted) ** 2))
        diagnostics.append({
            "date": day, "n_obs": n_obs, "n_industries": len(industries),
            "design_columns": columns, "rank": rank, "condition_number": condition,
            "r_squared": 1.0 - residual_ss / total_ss,
            "max_abs_continuous_correlation": max_corr,
            "max_abs_industry_residual_mean": max_group_mean,
        })
        for item, raw, preprocessed, value in zip(group, raw_factor, factor, residual):
            residual_rows.append({
                "date": day, "symbol": item[1], "raw_factor": float(raw),
                "preprocessed_factor": float(preprocessed), "residual_factor": float(value),
            })
    return residual_rows, diagnostics


def _read_input(path: Path, label: str) -> tuple[bytes, str]:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise DiagnosticError(f"cannot read {label}: {exc}") from exc
    return data, hashlib.sha256(data).hexdigest()


def _revalidate_inputs(expected: dict[str, tuple[Path, str]]) -> None:
    for label, (path, digest) in expected.items():
        try:
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            raise DiagnosticError(f"{label} changed during execution: {exc}") from exc
        if actual != digest:
            raise DiagnosticError(f"{label} changed during execution")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _composite(files: dict[str, str]) -> str:
    payload = "".join(f"{name}  {digest}\n" for name, digest in sorted(files.items()))
    return hashlib.sha256(payload.encode()).hexdigest()


def _code_identity(project_root: Path) -> dict[str, str]:
    try:
        code_sha = subprocess.check_output(
            ["git", "rev-parse", "--verify", "HEAD^{commit}"], cwd=project_root, text=True
        ).strip()
        head_listing = subprocess.check_output(
            ["git", "ls-tree", "-r", "-z", "HEAD", "--", "src", "scripts"], cwd=project_root
        )
        index_listing = subprocess.check_output(
            ["git", "ls-files", "--stage", "-z", "--", "src", "scripts"], cwd=project_root
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise DiagnosticError(f"cannot establish code identity: {exc}") from exc
    head = {}
    for record in head_listing.split(b"\0"):
        if record:
            metadata, relative = record.split(b"\t", 1)
            if relative.endswith(b".py"):
                mode, object_type, object_id = metadata.split(b" ")
                if mode == b"120000":
                    raise DiagnosticError(
                        f"symlinked Python file in executable project scope: {os.fsdecode(relative)}"
                    )
                head[relative] = (mode, object_type, object_id)
    if not head:
        raise DiagnosticError("cannot establish code identity: no tracked project Python files")
    index = {}
    for record in index_listing.split(b"\0"):
        if record:
            metadata, relative = record.split(b"\t", 1)
            if relative.endswith(b".py"):
                mode, object_id, stage = metadata.split(b" ")
                if stage != b"0" or relative in index:
                    raise DiagnosticError("staged executable project files differ from HEAD")
                index[relative] = (mode, object_id)
    expected_index = {path: (value[0], value[2]) for path, value in head.items()}
    if index != expected_index:
        raise DiagnosticError("staged executable project files differ from HEAD")
    root = os.fsencode(project_root)
    actual_paths = set()
    for scope in (b"src", b"scripts"):
        scope_path = os.path.join(root, scope)
        if not os.path.isdir(scope_path):
            continue
        for directory, directories, filenames in os.walk(scope_path, followlinks=False):
            for name in directories:
                path = os.path.join(directory, name)
                if stat.S_ISLNK(os.lstat(path).st_mode):
                    raise DiagnosticError(
                        f"symlinked directory in executable project scope: {os.fsdecode(os.path.relpath(path, root))}"
                    )
            for name in [*directories, *filenames]:
                relative = os.path.relpath(os.path.join(directory, name), root)
                if relative.endswith(b".py"):
                    actual_paths.add(relative)
    if actual_paths != set(head):
        differing = sorted(actual_paths ^ set(head))[0]
        raise DiagnosticError(f"executable project file set differs from HEAD: {os.fsdecode(differing)}")
    bundle = hashlib.sha256()
    for relative in sorted(head):
        expected_mode, expected_type, object_id = head[relative]
        path = os.path.join(root, relative)
        try:
            file_stat = os.lstat(path)
            if stat.S_ISREG(file_stat.st_mode):
                actual_mode, actual_type = (b"100755" if file_stat.st_mode & stat.S_IXUSR else b"100644"), b"blob"
                with open(path, "rb") as handle:
                    actual = handle.read()
            elif stat.S_ISLNK(file_stat.st_mode):
                actual_mode, actual_type, actual = b"120000", b"blob", os.readlink(path)
            elif stat.S_ISDIR(file_stat.st_mode):
                actual_mode, actual_type, actual = b"040000", b"tree", b""
            else:
                actual_mode, actual_type, actual = b"special", b"special", b""
            committed = subprocess.check_output(["git", "cat-file", expected_type, object_id], cwd=project_root)
        except (OSError, subprocess.CalledProcessError) as exc:
            raise DiagnosticError(f"cannot verify executable project file {os.fsdecode(relative)}: {exc}") from exc
        if (actual_mode, actual_type, actual) != (expected_mode, expected_type, committed):
            raise DiagnosticError(f"executable project file differs from HEAD: {os.fsdecode(relative)}")
        bundle.update(relative)
        bundle.update(b"\0" + expected_mode + b" " + expected_type + b"\0")
        bundle.update(str(len(actual)).encode() + b"\0" + actual)
    return {
        "code_sha": code_sha,
        "code_bundle_sha256": bundle.hexdigest(),
        "code_bundle_rule": "SHA256 of path-sorted raw path, HEAD mode/type, byte length, and exact HEAD blob bytes for exact filesystem/HEAD src/**/*.py and scripts/**/*.py set",
    }


def _publish_no_replace(temp: Path, output: Path, pre_publish_check: Callable[[], None]) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise DiagnosticError("atomic no-replace directory publication is unsupported")
    pre_publish_check()
    if renameat2(-100, os.fsencode(temp), -100, os.fsencode(output), 1) != 0:
        error_number = ctypes.get_errno()
        if error_number == 17:
            raise DiagnosticError(f"refusing to overwrite existing output directory: {output}")
        raise OSError(error_number, os.strerror(error_number), str(output))


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run_diagnostic(matrix_path: str | Path, manifest_path: str | Path, output_dir: str | Path) -> dict:
    """Validate, orthogonalize, and atomically publish a diagnostic-only directory."""
    # Preserve the caller's lexical input namespace. Hash and parse the bytes
    # reached through that path, then traverse the same path again at the final
    # publication boundary so a retargeted symlink cannot hide behind an early
    # resolve() of its original target.
    matrix_path = Path(os.path.abspath(os.fspath(matrix_path)))
    manifest_path = Path(os.path.abspath(os.fspath(manifest_path)))
    output = Path(os.path.abspath(os.fspath(output_dir)))
    project_root = Path(__file__).resolve().parents[2]
    code_identity = _code_identity(project_root)
    matrix_bytes, matrix_digest = _read_input(matrix_path, "matrix")
    manifest_bytes, manifest_digest = _read_input(manifest_path, "manifest")
    manifest, parsed, dates, controls, max_condition, tolerance = _load_packet(matrix_bytes, manifest_bytes)
    residuals, diagnostics = _orthogonalize(
        manifest, parsed, dates, controls, max_condition, tolerance
    )
    provenance = {
        "status": STATUS, **code_identity, "factor_id": manifest["factor_id"],
        "market": manifest["market"], "experiment_manifest_sha256": manifest_digest,
        "matrix_sha256": matrix_digest, "input_bundle_sha256": manifest["input_bundle_sha256"],
        "experiment_config_sha256": manifest["experiment_config_sha256"],
        "research_contract_sha256": manifest["research_contract_sha256"],
        "date_start": dates[0], "date_end": dates[-1], "row_count": len(parsed),
        "date_count": len(dates), "industry_column": manifest["industry_column"],
        "continuous_controls": controls, "control_roles": manifest["control_roles"],
        "point_in_time_industry_attestation": True,
        "controls_known_by_signal_cutoff_attestation": True,
        "factor_known_by_signal_cutoff_attestation": True,
        "winsor_lower": 0.01, "winsor_upper": 0.99,
        "min_cross_section": manifest["min_cross_section"],
        "max_condition_number": max_condition,
        "max_abs_residual_exposure": tolerance, "assumptions": ASSUMPTIONS,
    }
    summary = {
        **provenance, "schema_version": 1, "verdict": VERDICT,
        "diagnostics": {
            "max_condition_number_observed": max(row["condition_number"] for row in diagnostics),
            "max_abs_continuous_correlation": max(row["max_abs_continuous_correlation"] for row in diagnostics),
            "max_abs_industry_residual_mean": max(row["max_abs_industry_residual_mean"] for row in diagnostics),
            "min_r_squared": min(row["r_squared"] for row in diagnostics),
            "max_r_squared": max(row["r_squared"] for row in diagnostics),
        },
        "restriction": "Residual factor is not validated alpha/OOS and cannot enter a selector without a separately preregistered outcome study, replay, independent review, and forward paper gate.",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".{output.name}.tmp-", dir=output.parent))
    try:
        _write_csv(
            temp / "residuals.csv",
            ["date", "symbol", "raw_factor", "preprocessed_factor", "residual_factor"],
            residuals,
        )
        _write_csv(
            temp / "date_diagnostics.csv",
            ["date", "n_obs", "n_industries", "design_columns", "rank", "condition_number",
             "r_squared", "max_abs_continuous_correlation", "max_abs_industry_residual_mean"],
            diagnostics,
        )
        (temp / "summary.json").write_text(
            json.dumps(summary, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8"
        )
        files = {path.name: _sha256(path) for path in temp.iterdir() if path.is_file()}
        artifact_manifest = {
            **provenance, "schema_version": 1, "verdict": VERDICT, "files": files,
            "composite_rule": "SHA256 of sorted '<relative_path>  <sha256>\\n' UTF-8 lines",
            "composite_sha256": _composite(files),
        }
        (temp / "artifact_manifest.json").write_text(
            json.dumps(artifact_manifest, sort_keys=True, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )

        def pre_publish_check() -> None:
            if _code_identity(project_root) != code_identity:
                raise DiagnosticError("executable project files changed during execution")
            _revalidate_inputs({
                "matrix": (matrix_path, matrix_digest), "manifest": (manifest_path, manifest_digest)
            })

        _publish_no_replace(temp, output, pre_publish_check)
    finally:
        shutil.rmtree(temp, ignore_errors=True)
    return summary
