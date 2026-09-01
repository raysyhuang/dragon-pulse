"""Conservative, research-only overfit diagnostics for an immutable return matrix."""

from __future__ import annotations

import csv
import ctypes
import hashlib
import io
import itertools
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
from statistics import NormalDist

import numpy as np


STATUS = "RESEARCH_ONLY_NON_BINDING"
VERDICTS = {"VETO_FURTHER_PROMOTION", "NO_VETO_RESEARCH_ONLY_NOT_PROMOTION"}
ASSUMPTIONS = [
    "Input columns are aligned daily net portfolio returns supplied by the experiment packet.",
    "Returns and trials are treated as supplied; this diagnostic does not prove point-in-time provenance.",
    "Sharpe-style normal approximations can be unreliable under serial correlation, skew, and fat tails.",
    "CSCV uses contiguous blocks but does not remove dependence from overlapping holding-period returns.",
    "DSR cross-trial Sharpe dispersion is estimated only from supplied matrix columns; omitted declared trials affect the n_trials_total adjustment but their dispersion cannot be reconstructed.",
    "No result is untouched out-of-sample evidence or authorization for promotion, alerts, or orders.",
]
DEFAULT_THRESHOLDS = {
    "min_selected_annualized_sharpe": 0.5,
    "min_deflated_sharpe_probability": 0.95,
    "max_pbo": 0.20,
    "max_bonferroni_p_value": 0.05,
}
MAX_CSCV_SPLITS = 10_000
MAX_N_TRIALS_TOTAL = 1_000_000_000
RETURN_PATTERN = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")


class DiagnosticError(ValueError):
    """The immutable packet or requested diagnostic cannot be evaluated safely."""


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
        raise DiagnosticError(f"{field} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DiagnosticError(f"{field} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise DiagnosticError(f"{field} must use canonical YYYY-MM-DD format")
    return value


def _load_packet(matrix_bytes: bytes, manifest_bytes: bytes, n_blocks: int, min_block_observations: int):
    manifest = _strict_json(manifest_bytes)
    required = {
        "schema_version", "status", "selected_variant", "variant_ids", "n_trials_total",
        "date_start", "date_end", "periods_per_year", "input_bundle_sha256",
        "experiment_config_sha256", "execution_contract_hash", "matrix_sha256",
        "all_tested_and_abandoned_variants_counted",
    }
    missing = required - set(manifest)
    if missing:
        raise DiagnosticError(f"manifest missing required fields: {', '.join(sorted(missing))}")
    unknown = set(manifest) - (required | {"complete_historical_variant_count"})
    if unknown:
        raise DiagnosticError(f"manifest contains unknown fields: {', '.join(sorted(unknown))}")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise DiagnosticError("unsupported schema_version")
    if manifest["status"] != STATUS:
        raise DiagnosticError(f"unsupported status; required {STATUS}")
    variant_ids = manifest["variant_ids"]
    if (
        not isinstance(variant_ids, list)
        or len(variant_ids) < 2
        or any(not isinstance(item, str) or not item for item in variant_ids)
        or len(set(variant_ids)) != len(variant_ids)
    ):
        raise DiagnosticError("variant_ids must contain at least two unique non-empty strings")
    selected = manifest["selected_variant"]
    if not isinstance(selected, str) or selected not in variant_ids:
        raise DiagnosticError("selected_variant is absent from variant_ids")
    n_trials = manifest["n_trials_total"]
    if isinstance(n_trials, bool) or not isinstance(n_trials, int) or n_trials < len(variant_ids):
        raise DiagnosticError("n_trials_total must be an integer at least as large as matrix variants")
    if n_trials > MAX_N_TRIALS_TOTAL:
        raise DiagnosticError(
            f"n_trials_total {n_trials} exceeds maximum {MAX_N_TRIALS_TOTAL}"
        )
    if manifest["all_tested_and_abandoned_variants_counted"] is not True:
        raise DiagnosticError("all tested and abandoned variants must be counted")
    if "complete_historical_variant_count" in manifest:
        count = manifest["complete_historical_variant_count"]
        if isinstance(count, bool) or not isinstance(count, int) or count != n_trials:
            raise DiagnosticError("complete_historical_variant_count must equal n_trials_total")
    periods = manifest["periods_per_year"]
    if isinstance(periods, bool) or not isinstance(periods, int) or periods <= 0:
        raise DiagnosticError("periods_per_year must be a positive integer")
    start = _iso_date(manifest["date_start"], "date_start")
    end = _iso_date(manifest["date_end"], "date_end")
    hash_fields = (
        "input_bundle_sha256", "experiment_config_sha256", "execution_contract_hash", "matrix_sha256"
    )
    for field in hash_fields:
        value = manifest[field]
        if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise DiagnosticError(f"{field} must be a lowercase SHA-256 digest")
    actual_hash = hashlib.sha256(matrix_bytes).hexdigest()
    if actual_hash != manifest["matrix_sha256"]:
        raise DiagnosticError(
            f"matrix hash mismatch: expected {manifest['matrix_sha256']}, actual {actual_hash}"
        )
    if isinstance(n_blocks, bool) or not isinstance(n_blocks, int) or n_blocks < 2 or n_blocks % 2:
        raise DiagnosticError("n_blocks must be an even integer of at least two")
    split_count = math.comb(n_blocks, n_blocks // 2)
    if split_count > MAX_CSCV_SPLITS:
        raise DiagnosticError(
            f"CSCV split count {split_count} exceeds maximum {MAX_CSCV_SPLITS}"
        )
    if (
        isinstance(min_block_observations, bool)
        or not isinstance(min_block_observations, int)
        or min_block_observations < 2
    ):
        raise DiagnosticError("min_block_observations must be an integer of at least two")
    try:
        rows = list(csv.reader(io.StringIO(matrix_bytes.decode("utf-8"), newline=""), strict=True))
    except (UnicodeError, csv.Error) as exc:
        raise DiagnosticError(f"unparseable matrix CSV: {exc}") from exc
    if not rows:
        raise DiagnosticError("matrix CSV is empty")
    header, raw_rows = rows[0], rows[1:]
    if header != ["date", *variant_ids]:
        raise DiagnosticError("matrix columns/order differ from manifest variant_ids")
    if not raw_rows:
        raise DiagnosticError("matrix CSV has empty rows")
    if len(raw_rows) < n_blocks * min_block_observations:
        raise DiagnosticError("too few observations for requested blocks and minimum block observations")
    if len(raw_rows) % n_blocks:
        raise DiagnosticError("observation count must be divisible by n_blocks for equal contiguous CSCV blocks")
    dates: list[str] = []
    values: list[list[float]] = []
    for row_number, row in enumerate(raw_rows, 2):
        if len(row) != len(header):
            raise DiagnosticError(f"matrix row {row_number} has missing or extra cells")
        dates.append(_iso_date(row[0], f"matrix row {row_number} date"))
        parsed_row: list[float] = []
        for cell in row[1:]:
            if RETURN_PATTERN.fullmatch(cell) is None:
                raise DiagnosticError(f"matrix row {row_number} has a missing/non-numeric return")
            try:
                number = float(cell)
            except ValueError as exc:
                raise DiagnosticError(f"matrix row {row_number} has a missing/non-numeric return") from exc
            if not math.isfinite(number):
                raise DiagnosticError(f"matrix row {row_number} has a non-finite return")
            parsed_row.append(number)
        values.append(parsed_row)
    if dates != sorted(dates) or len(set(dates)) != len(dates):
        raise DiagnosticError("matrix dates must be unique and strictly increasing")
    if dates[0] != start or dates[-1] != end:
        raise DiagnosticError("matrix date range does not match manifest bounds")
    matrix = np.asarray(values, dtype=float)
    if np.any(np.std(matrix, axis=0, ddof=1) <= 0.0):
        raise DiagnosticError("required Sharpe diagnostics are unevaluable for zero-variance returns")
    return manifest, dates, variant_ids, matrix


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _composite(files: dict[str, str]) -> str:
    lines = "".join(f"{name}  {digest}\n" for name, digest in sorted(files.items()))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _code_identity(project_root: Path) -> dict[str, str]:
    try:
        code_sha = subprocess.check_output(
            ["git", "rev-parse", "--verify", "HEAD^{commit}"], cwd=project_root, text=True
        ).strip()
        head_listing = subprocess.check_output(
            ["git", "ls-tree", "-r", "-z", "HEAD", "--", "src", "scripts"],
            cwd=project_root,
        )
        index_listing = subprocess.check_output(
            ["git", "ls-files", "--stage", "-z", "--", "src", "scripts"],
            cwd=project_root,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise DiagnosticError(f"cannot establish code identity: {exc}") from exc

    head: dict[bytes, tuple[bytes, bytes, bytes]] = {}
    for record in head_listing.split(b"\0"):
        if not record:
            continue
        metadata, relative = record.split(b"\t", 1)
        if relative.endswith(b".py"):
            mode, object_type, object_id = metadata.split(b" ")
            head[relative] = (mode, object_type, object_id)
    if not head:
        raise DiagnosticError("cannot establish code identity: no tracked project Python files")

    index: dict[bytes, tuple[bytes, bytes]] = {}
    for record in index_listing.split(b"\0"):
        if not record:
            continue
        metadata, relative = record.split(b"\t", 1)
        if relative.endswith(b".py"):
            mode, object_id, stage = metadata.split(b" ")
            if stage != b"0" or relative in index:
                raise DiagnosticError("staged executable project files differ from HEAD")
            index[relative] = (mode, object_id)
    expected_index = {
        relative: (mode, object_id)
        for relative, (mode, _object_type, object_id) in head.items()
    }
    if index != expected_index:
        raise DiagnosticError("staged executable project files differ from HEAD")

    root = os.fsencode(project_root)
    actual_paths: set[bytes] = set()
    for scope in (b"src", b"scripts"):
        scope_path = os.path.join(root, scope)
        if not os.path.isdir(scope_path):
            continue
        for directory, directories, filenames in os.walk(scope_path, followlinks=False):
            for name in directories:
                path = os.path.join(directory, name)
                if stat.S_ISLNK(os.lstat(path).st_mode):
                    relative = os.path.relpath(path, root)
                    raise DiagnosticError(
                        f"symlinked directory in executable project scope: {os.fsdecode(relative)}"
                    )
            for name in [*directories, *filenames]:
                path = os.path.join(directory, name)
                relative = os.path.relpath(path, root)
                if relative.endswith(b".py"):
                    actual_paths.add(relative)
    if actual_paths != set(head):
        difference = sorted(actual_paths ^ set(head))[0]
        raise DiagnosticError(
            f"executable project file set differs from HEAD: {os.fsdecode(difference)}"
        )

    bundle = hashlib.sha256()
    for relative in sorted(head):
        expected_mode, expected_type, object_id = head[relative]
        path = os.path.join(root, relative)
        try:
            file_stat = os.lstat(path)
            if stat.S_ISREG(file_stat.st_mode):
                actual_mode, actual_type = (
                    b"100755" if file_stat.st_mode & stat.S_IXUSR else b"100644",
                    b"blob",
                )
                with open(path, "rb") as handle:
                    actual = handle.read()
            elif stat.S_ISLNK(file_stat.st_mode):
                actual_mode, actual_type = b"120000", b"blob"
                actual = os.readlink(path)
            elif stat.S_ISDIR(file_stat.st_mode):
                actual_mode, actual_type, actual = b"040000", b"tree", b""
            else:
                actual_mode, actual_type, actual = b"special", b"special", b""
            committed = subprocess.check_output(
                ["git", "cat-file", expected_type, object_id], cwd=project_root
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            name = os.fsdecode(relative)
            raise DiagnosticError(f"cannot verify executable project file {name}: {exc}") from exc
        if (actual_mode, actual_type, actual) != (expected_mode, expected_type, committed):
            raise DiagnosticError(
                f"executable project file differs from HEAD: {os.fsdecode(relative)}"
            )
        bundle.update(relative)
        bundle.update(b"\0" + expected_mode + b" " + expected_type + b"\0")
        bundle.update(str(len(actual)).encode("ascii") + b"\0" + actual)
    return {
        "code_sha": code_sha,
        "code_bundle_sha256": bundle.hexdigest(),
        "code_bundle_rule": (
            "SHA256 of concatenated path-sorted '<raw_path>\\0<HEAD_mode> <HEAD_type>\\0"
            "<byte_length>\\0<exact_HEAD_blob_bytes>' records for the exact filesystem/HEAD "
            "src/**/*.py and scripts/**/*.py set"
        ),
    }


def _sharpe(returns: np.ndarray, periods_per_year: int) -> float:
    std = float(np.std(returns, ddof=1))
    return float(np.mean(returns) / std * math.sqrt(periods_per_year))


def _one_sided_normal_tail(z_score: float) -> float:
    return 0.5 * math.erfc(z_score / math.sqrt(2.0))


def _probabilistic_sharpe(returns: np.ndarray, benchmark_daily_sharpe: float) -> float:
    n = len(returns)
    daily_sr = float(np.mean(returns) / np.std(returns, ddof=1))
    centered = returns - np.mean(returns)
    sigma = float(np.std(returns, ddof=0))
    skew = float(np.mean(centered**3) / sigma**3)
    kurtosis = float(np.mean(centered**4) / sigma**4)
    denominator = math.sqrt(
        max(1e-15, 1.0 - skew * daily_sr + ((kurtosis - 1.0) / 4.0) * daily_sr**2)
    )
    z_score = (daily_sr - benchmark_daily_sharpe) * math.sqrt(n - 1) / denominator
    return NormalDist().cdf(z_score)


def _expected_max_daily_sharpe(daily_sharpes: np.ndarray, n_trials: int) -> float:
    if n_trials <= 1:
        return 0.0
    trial_std = float(np.std(daily_sharpes, ddof=1))
    scale = max(1.0, float(np.max(np.abs(daily_sharpes))))
    if trial_std <= 1e-12 * scale:
        raise DiagnosticError("DSR cross-trial Sharpe dispersion is zero or nearly zero")
    normal = NormalDist()
    gamma = 0.5772156649015329
    return trial_std * (
        (1.0 - gamma) * normal.inv_cdf(1.0 - 1.0 / n_trials)
        + gamma * normal.inv_cdf(1.0 - 1.0 / (n_trials * math.e))
    )


def _cscv_scores(values: np.ndarray, split_index: int, side: str) -> np.ndarray:
    spread = np.ptp(values, axis=0)
    scale = np.maximum(1.0, np.max(np.abs(values), axis=0))
    if np.any(spread <= 1e-12 * scale):
        raise DiagnosticError(f"CSCV split {split_index} {side} scores are unevaluable")
    scores = np.mean(values, axis=0) / np.std(values, axis=0, ddof=1)
    if not np.all(np.isfinite(scores)):
        raise DiagnosticError(f"CSCV split {split_index} {side} scores are unevaluable")
    ordered = np.sort(scores)
    if np.any(np.isclose(ordered[1:], ordered[:-1], rtol=1e-12, atol=1e-12)):
        raise DiagnosticError(f"CSCV split {split_index} tied {side} scores are ambiguous")
    return scores


def _pbo_splits(
    matrix: np.ndarray,
    variant_ids: list[str],
    n_blocks: int,
    min_block_observations: int,
) -> tuple[float, list[dict]]:
    block_size = len(matrix) // n_blocks
    usable = block_size * n_blocks
    blocks = np.split(matrix[:usable], n_blocks)
    rows: list[dict] = []
    for split_index, train_blocks in enumerate(itertools.combinations(range(n_blocks), n_blocks // 2)):
        train_set = set(train_blocks)
        test_blocks = tuple(index for index in range(n_blocks) if index not in train_set)
        train = np.concatenate([blocks[index] for index in train_blocks])
        test = np.concatenate([blocks[index] for index in test_blocks])
        train_scores = _cscv_scores(train, split_index, "train")
        winner = int(np.argmax(train_scores))
        test_scores = _cscv_scores(test, split_index, "test")
        order = np.argsort(test_scores)
        rank = int(np.where(order == winner)[0][0]) + 1
        percentile = (rank - 0.5) / len(variant_ids)
        logit = math.log(percentile / (1.0 - percentile))
        rows.append({
            "split_index": split_index,
            "train_blocks": ";".join(map(str, train_blocks)),
            "test_blocks": ";".join(map(str, test_blocks)),
            "in_sample_winner": variant_ids[winner],
            "winner_out_of_sample_rank": rank,
            "winner_out_of_sample_percentile": percentile,
            "logit": logit,
            "is_overfit": logit <= 0.0,
            "block_size": block_size,
        })
    return float(np.mean([row["is_overfit"] for row in rows])), rows


def _publish_no_replace(temp: Path, output: Path, pre_publish_check: Callable[[], None]) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise DiagnosticError("atomic no-replace directory publication is unsupported")
    pre_publish_check()
    result = renameat2(-100, os.fsencode(temp), -100, os.fsencode(output), 1)
    if result != 0:
        errno = ctypes.get_errno()
        if errno == 17:
            raise DiagnosticError(f"refusing to overwrite existing output directory: {output}")
        raise OSError(errno, os.strerror(errno), str(output))


def run_diagnostic(
    matrix_path: str | Path,
    manifest_path: str | Path,
    output_dir: str | Path,
    *,
    n_blocks: int = 8,
    min_block_observations: int = 20,
    thresholds: dict[str, float] | None = None,
) -> dict:
    """Validate, evaluate, and atomically publish a non-promoting evidence directory."""
    matrix_path = Path(matrix_path).resolve()
    manifest_path = Path(manifest_path).resolve()
    output = Path(output_dir).resolve()
    project_root = Path(__file__).resolve().parents[2]
    code_identity = _code_identity(project_root)
    matrix_bytes, matrix_digest = _read_input(matrix_path, "matrix")
    manifest_bytes, manifest_digest = _read_input(manifest_path, "manifest")
    manifest, dates, variant_ids, matrix = _load_packet(
        matrix_bytes, manifest_bytes, n_blocks, min_block_observations
    )
    effective_thresholds = dict(DEFAULT_THRESHOLDS if thresholds is None else thresholds)
    if set(effective_thresholds) != set(DEFAULT_THRESHOLDS):
        raise DiagnosticError("thresholds must contain the exact policy fields")
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
        for value in effective_thresholds.values()
    ):
        raise DiagnosticError("thresholds must be finite numbers")
    weakened = (
        effective_thresholds["min_selected_annualized_sharpe"]
        < DEFAULT_THRESHOLDS["min_selected_annualized_sharpe"]
        or effective_thresholds["min_deflated_sharpe_probability"]
        < DEFAULT_THRESHOLDS["min_deflated_sharpe_probability"]
        or effective_thresholds["max_pbo"] > DEFAULT_THRESHOLDS["max_pbo"]
        or effective_thresholds["max_bonferroni_p_value"]
        > DEFAULT_THRESHOLDS["max_bonferroni_p_value"]
    )
    if weakened:
        raise DiagnosticError("threshold overrides may tighten but never weaken the conservative policy")
    periods = manifest["periods_per_year"]
    selected_index = variant_ids.index(manifest["selected_variant"])
    selected = matrix[:, selected_index]
    selected_sharpe = _sharpe(selected, periods)
    daily_sharpes = np.mean(matrix, axis=0) / np.std(matrix, axis=0, ddof=1)
    expected_max = _expected_max_daily_sharpe(daily_sharpes, manifest["n_trials_total"])
    dsr = _probabilistic_sharpe(selected, expected_max)
    z_score = float(np.mean(selected) / (np.std(selected, ddof=1) / math.sqrt(len(selected))))
    one_sided_p = _one_sided_normal_tail(z_score)
    bonferroni_p = min(1.0, one_sided_p * manifest["n_trials_total"])
    pbo, splits = _pbo_splits(matrix, variant_ids, n_blocks, min_block_observations)
    checks = {
        "selected_annualized_sharpe": selected_sharpe >= effective_thresholds["min_selected_annualized_sharpe"],
        "deflated_sharpe_probability": dsr >= effective_thresholds["min_deflated_sharpe_probability"],
        "pbo": pbo <= effective_thresholds["max_pbo"],
        "bonferroni_adjusted_p_value": bonferroni_p <= effective_thresholds["max_bonferroni_p_value"],
    }
    verdict = "NO_VETO_RESEARCH_ONLY_NOT_PROMOTION" if all(checks.values()) else "VETO_FURTHER_PROMOTION"
    provenance = {
        "status": STATUS,
        **code_identity,
        "experiment_manifest_sha256": manifest_digest,
        "matrix_sha256": matrix_digest,
        "input_bundle_sha256": manifest["input_bundle_sha256"],
        "experiment_config_sha256": manifest["experiment_config_sha256"],
        "execution_contract_hash": manifest["execution_contract_hash"],
        "date_start": dates[0],
        "date_end": dates[-1],
        "periods_per_year": periods,
        "n_blocks": n_blocks,
        "min_block_observations": min_block_observations,
        "variant_ids": variant_ids,
        "matrix_variant_count": len(variant_ids),
        "n_trials_total": manifest["n_trials_total"],
        "selected_variant": manifest["selected_variant"],
        "all_tested_and_abandoned_variants_counted": manifest[
            "all_tested_and_abandoned_variants_counted"
        ],
        "assumptions": ASSUMPTIONS,
        "thresholds": effective_thresholds,
    }
    summary = {
        **provenance,
        "schema_version": 1,
        "verdict": verdict,
        "checks": checks,
        "diagnostics": {
            "selected_annualized_sharpe": selected_sharpe,
            "deflated_sharpe_probability": dsr,
            "deflated_sharpe_benchmark_annualized": expected_max * math.sqrt(periods),
            "pbo": pbo,
            "bonferroni_adjusted_selected_p_value": bonferroni_p,
            "unadjusted_selected_one_sided_p_value": one_sided_p,
            "pbo_split_count": len(splits),
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".{output.name}.tmp-", dir=output.parent))
    try:
        (temp / "summary.json").write_text(
            json.dumps(summary, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8"
        )
        with (temp / "pbo_splits.csv").open("w", encoding="utf-8", newline="") as handle:
            fieldnames = [*splits[0], *provenance]
            writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            for split in splits:
                writer.writerow({
                    **split,
                    **provenance,
                    "variant_ids": json.dumps(variant_ids, separators=(",", ":")),
                    "assumptions": json.dumps(ASSUMPTIONS, separators=(",", ":")),
                    "thresholds": json.dumps(effective_thresholds, sort_keys=True, separators=(",", ":")),
                })
        files = {path.name: _sha256(path) for path in temp.iterdir() if path.is_file()}
        artifact_manifest = {
            **provenance,
            "schema_version": 1,
            "files": files,
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
                "matrix": (matrix_path, matrix_digest),
                "manifest": (manifest_path, manifest_digest),
            })

        _publish_no_replace(temp, output, pre_publish_check)
    finally:
        shutil.rmtree(temp, ignore_errors=True)
    return summary
