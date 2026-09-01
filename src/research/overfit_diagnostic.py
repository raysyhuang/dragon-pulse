"""Conservative, research-only overfit diagnostics for an immutable return matrix."""

from __future__ import annotations

import csv
import ctypes
import hashlib
import itertools
import json
import math
import os
import shutil
import subprocess
import tempfile
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
    "No result is untouched out-of-sample evidence or authorization for promotion, alerts, or orders.",
]
DEFAULT_THRESHOLDS = {
    "min_selected_annualized_sharpe": 0.5,
    "min_deflated_sharpe_probability": 0.95,
    "max_pbo": 0.20,
    "max_bonferroni_p_value": 0.05,
}


class DiagnosticError(ValueError):
    """The immutable packet or requested diagnostic cannot be evaluated safely."""


def _strict_json(path: Path) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise DiagnosticError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(
                DiagnosticError(f"non-finite JSON value: {token}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
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


def _load_packet(matrix_path: Path, manifest_path: Path, n_blocks: int, min_block_observations: int):
    manifest = _strict_json(manifest_path)
    required = {
        "schema_version", "status", "selected_variant", "variant_ids", "n_trials_total",
        "date_start", "date_end", "periods_per_year", "input_bundle_sha256",
        "experiment_config_sha256", "execution_contract_hash", "matrix_sha256",
        "all_tested_and_abandoned_variants_counted",
    }
    missing = required - set(manifest)
    if missing:
        raise DiagnosticError(f"manifest missing required fields: {', '.join(sorted(missing))}")
    if manifest["schema_version"] != 1:
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
    actual_hash = _sha256(matrix_path)
    if actual_hash != manifest["matrix_sha256"]:
        raise DiagnosticError(
            f"matrix hash mismatch: expected {manifest['matrix_sha256']}, actual {actual_hash}"
        )
    if isinstance(n_blocks, bool) or not isinstance(n_blocks, int) or n_blocks < 2 or n_blocks % 2:
        raise DiagnosticError("n_blocks must be an even integer of at least two")
    if (
        isinstance(min_block_observations, bool)
        or not isinstance(min_block_observations, int)
        or min_block_observations < 2
    ):
        raise DiagnosticError("min_block_observations must be an integer of at least two")
    try:
        with matrix_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))
    except (OSError, UnicodeError, csv.Error) as exc:
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


def _composite(files: dict[str, str]) -> str:
    lines = "".join(f"{name}  {digest}\n" for name, digest in sorted(files.items()))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _code_sha(project_root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=project_root, text=True
    ).strip()


def _sharpe(returns: np.ndarray, periods_per_year: int) -> float:
    std = float(np.std(returns, ddof=1))
    return float(np.mean(returns) / std * math.sqrt(periods_per_year))


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
    if trial_std == 0.0:
        return 0.0
    normal = NormalDist()
    gamma = 0.5772156649015329
    return trial_std * (
        (1.0 - gamma) * normal.inv_cdf(1.0 - 1.0 / n_trials)
        + gamma * normal.inv_cdf(1.0 - 1.0 / (n_trials * math.e))
    )


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
        train_scores = np.mean(train, axis=0) / np.std(train, axis=0, ddof=1)
        winner = int(np.argmax(train_scores))
        test_scores = np.mean(test, axis=0) / np.std(test, axis=0, ddof=1)
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
            "min_block_observations": min_block_observations,
        })
    return float(np.mean([row["is_overfit"] for row in rows])), rows


def _publish_no_replace(temp: Path, output: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise DiagnosticError("atomic no-replace directory publication is unsupported")
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
    manifest, dates, variant_ids, matrix = _load_packet(
        matrix_path, manifest_path, n_blocks, min_block_observations
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
    one_sided_p = 1.0 - NormalDist().cdf(z_score)
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
        "code_sha": _code_sha(Path(__file__).resolve().parents[2]),
        "experiment_manifest_sha256": _sha256(manifest_path),
        "matrix_sha256": _sha256(matrix_path),
        "input_bundle_sha256": manifest["input_bundle_sha256"],
        "experiment_config_sha256": manifest["experiment_config_sha256"],
        "execution_contract_hash": manifest["execution_contract_hash"],
        "date_start": dates[0],
        "date_end": dates[-1],
        "n_trials_total": manifest["n_trials_total"],
        "selected_variant": manifest["selected_variant"],
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
        _publish_no_replace(temp, output)
    except Exception:
        shutil.rmtree(temp, ignore_errors=True)
        raise
    return summary
