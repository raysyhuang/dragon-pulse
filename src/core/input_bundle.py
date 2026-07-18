"""Fail-closed loader for immutable backtest input bundles."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


INPUT_MODE = "live"


class BundleModeViolation(RuntimeError):
    """Raised when live acquisition is attempted during a bundle replay."""


class InputBundleValidationError(ValueError):
    """Raised when an input bundle does not match its manifest contract."""


@dataclass(frozen=True)
class InputBundle:
    path: Path
    manifest: dict
    bundle_id: str
    composite_sha256: str
    pit_grade: bool
    tickers: list[str]
    basic_info: dict[str, dict]
    data_map: dict[str, pd.DataFrame]


def set_input_mode(mode: str) -> None:
    global INPUT_MODE
    INPUT_MODE = mode


def assert_provider_access_allowed(function_name: str) -> None:
    if INPUT_MODE == "bundle":
        raise BundleModeViolation(f"Bundle mode forbids provider access: {function_name}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _composite_hash(files: dict[str, str]) -> str:
    payload = "".join(f"{relpath}  {digest}\n" for relpath, digest in sorted(files.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _bundle_files(path: Path) -> set[str]:
    return {
        item.relative_to(path).as_posix()
        for item in path.rglob("*")
        if item.is_file()
    }


def _failures_text(failures: list[str]) -> str:
    return "Input bundle validation failed:\n- " + "\n- ".join(failures)


def validate_input_bundle(
    bundle_dir: str | Path,
    *,
    acceptance_mode: str = "off",
    support_tickers: tuple[str, ...] = (),
) -> InputBundle:
    """Validate and load a bundle without consulting providers or cache state.

    ``support_tickers`` covers required non-tradable series such as CSI 300.
    They must be manifest-listed price files but are not returned as universe members.
    """
    path = Path(bundle_dir).resolve()
    manifest_path = path / "manifest.json"
    failures: list[str] = []
    if not path.is_dir():
        raise InputBundleValidationError(f"Input bundle directory is missing: {path}")
    if not manifest_path.is_file():
        raise InputBundleValidationError(f"Input bundle manifest is missing: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InputBundleValidationError(f"Input bundle manifest is unparseable: {manifest_path}: {exc}") from exc

    required_manifest = {
        "bundle_id", "created_at", "freeze_tool_commit", "pit_grade", "universe_count",
        "files", "composite_sha256", "notes",
    }
    missing_manifest = required_manifest - set(manifest)
    if missing_manifest:
        failures.append(f"manifest is missing required field(s): {', '.join(sorted(missing_manifest))}")
    if not isinstance(manifest.get("universe_count"), int) or isinstance(manifest.get("universe_count"), bool):
        failures.append("manifest.universe_count must be an integer")
    for field in ("created_at", "freeze_tool_commit", "notes"):
        if not isinstance(manifest.get(field), str) or not manifest[field]:
            failures.append(f"manifest.{field} must be a non-empty string")

    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        failures.append("manifest.files must be a non-empty object")
        files = {}
    elif not all(isinstance(key, str) and isinstance(value, str) for key, value in files.items()):
        failures.append("manifest.files must map string paths to SHA-256 strings")

    if manifest.get("pit_grade") is not False:
        failures.append("manifest.pit_grade must be literal false for an input bundle")
    if not isinstance(manifest.get("bundle_id"), str) or not manifest["bundle_id"]:
        failures.append("manifest.bundle_id must be a non-empty string")
    if not isinstance(manifest.get("composite_sha256"), str):
        failures.append("manifest.composite_sha256 must be a string")

    required = {"universe.csv", "basic_info.json"}
    missing_required = required - set(files)
    for relpath in sorted(missing_required):
        failures.append(f"manifest is missing required file: {relpath}")
    for relpath, expected in sorted(files.items()):
        candidate = path / relpath
        if Path(relpath).is_absolute() or ".." in Path(relpath).parts:
            failures.append(f"invalid manifest path: {relpath}")
            continue
        if not candidate.is_file():
            failures.append(f"missing manifest-listed file: {relpath}")
            continue
        actual = _sha256(candidate)
        if actual != expected:
            failures.append(f"hash mismatch for {relpath}: expected {expected}, actual {actual}")

    actual_files = _bundle_files(path) - {"manifest.json"}
    extra_files = actual_files - set(files)
    for relpath in sorted(extra_files):
        failures.append(f"unlisted bundle file: {relpath}")
    composite = _composite_hash(files)
    if manifest.get("composite_sha256") != composite:
        failures.append(
            f"composite hash mismatch: expected {manifest.get('composite_sha256')}, actual {composite}"
        )

    universe_path = path / "universe.csv"
    basic_info_path = path / "basic_info.json"
    tickers: list[str] = []
    basic_info: dict[str, dict] = {}
    if universe_path.is_file() and "universe.csv" in files:
        try:
            universe_df = pd.read_csv(universe_path)
            if "ticker" not in universe_df.columns:
                failures.append("universe.csv is missing required ticker column")
            else:
                tickers = [str(value).upper() for value in universe_df["ticker"].dropna().tolist()]
                if not tickers or len(set(tickers)) != len(tickers):
                    failures.append("universe.csv must contain unique non-empty tickers")
        except Exception as exc:
            failures.append(f"unparseable universe.csv: {exc}")
    if basic_info_path.is_file() and "basic_info.json" in files:
        try:
            raw_basic_info = json.loads(basic_info_path.read_text(encoding="utf-8"))
            if not isinstance(raw_basic_info, dict):
                failures.append("basic_info.json must contain a ticker-to-metadata object")
            else:
                basic_info = {str(ticker).upper(): value for ticker, value in raw_basic_info.items() if isinstance(value, dict)}
        except (OSError, json.JSONDecodeError) as exc:
            failures.append(f"unparseable basic_info.json: {exc}")

    support = {str(ticker).upper() for ticker in support_tickers}
    expected_prices = {f"prices/{ticker}.csv" for ticker in set(tickers) | support}
    expected_files = {"universe.csv", "basic_info.json"} | expected_prices
    unexpected_manifest_files = set(files) - expected_files
    for relpath in sorted(unexpected_manifest_files):
        failures.append(f"manifest contains unsupported bundle file: {relpath}")
    if isinstance(manifest.get("universe_count"), int) and manifest["universe_count"] != len(tickers):
        failures.append(f"manifest.universe_count={manifest['universe_count']} does not match universe.csv={len(tickers)}")
    actual_prices = {relpath for relpath in files if relpath.startswith("prices/")}
    for relpath in sorted(expected_prices - actual_prices):
        failures.append(f"required ticker lacks price CSV: {relpath}")
    for relpath in sorted(actual_prices - expected_prices):
        failures.append(f"price CSV has no universe/support ticker: {relpath}")
    if acceptance_mode == "live_equivalent":
        for ticker in tickers:
            if ticker not in basic_info:
                failures.append(f"basic_info.json lacks live_equivalent metadata for {ticker}")

    if failures:
        raise InputBundleValidationError(_failures_text(failures))

    data_map: dict[str, pd.DataFrame] = {}
    for ticker in sorted(set(tickers) | support):
        price_path = path / f"prices/{ticker}.csv"
        try:
            data = pd.read_csv(price_path, index_col=0, parse_dates=True)
        except Exception as exc:  # hashes already passed; retain a useful parse error.
            raise InputBundleValidationError(f"unparseable price CSV {price_path.relative_to(path)}: {exc}") from exc
        if data.empty:
            raise InputBundleValidationError(f"empty price CSV: {price_path.relative_to(path)}")
        data.index.name = "Date"
        data_map[ticker] = data

    return InputBundle(
        path=path,
        manifest=manifest,
        bundle_id=manifest["bundle_id"],
        composite_sha256=composite,
        pit_grade=False,
        tickers=tickers,
        basic_info=basic_info,
        data_map=data_map,
    )
