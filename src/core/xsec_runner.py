"""Canonical, provider-free runner for frozen cross-sectional replay evidence.

This module deliberately accepts already-frozen factor selections and execution bars.
It never calls a selector or market-data provider.  Canonical accounting is delegated
one rebalance/sleeve at a time to :func:`src.core.xsec_replay.replay_cross_section`.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.core.pit_bundle import PitBundleValidationError, validate_pit_bundle
from src.core.xsec_replay import Bar, ReplayInput, TickerReplay, replay_cross_section

SCHEMA_VERSION = 1
CAPACITY_RULE = "rank-first: factor order then ticker ASC; first max_concurrent_slots are capacity_available"
COST_ASSUMPTION = "total_cost_bps research simplification; not a leg-specific A-share commission/stamp-duty model"


class ArtifactDurabilityUncertainError(OSError):
    """The canonical artifact was linked but its directory entry was not synced."""

    def __init__(self, artifact: Path) -> None:
        self.artifact = artifact
        super().__init__(
            f"canonical xsec replay artifact {artifact}: "
            "artifact may exist but durability is uncertain; inspect/reconcile manually"
        )


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise ValueError(f"{label} must be ISO YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be ISO YYYY-MM-DD") from exc


def _parse_bar(value: object, label: str) -> Bar | None:
    if value is None:
        return None
    row = _require_mapping(value, label)
    try:
        return Bar(
            day=_parse_date(row.get("day"), f"{label}.day"),
            open=_number(row["open"], f"{label}.open"), high=_number(row["high"], f"{label}.high"),
            low=_number(row["low"], f"{label}.low"), close=_number(row["close"], f"{label}.close"),
            volume=_number(row["volume"], f"{label}.volume"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, ValueError) and "literal JSON number" in str(exc):
            raise
        raise ValueError(f"{label} must contain valid OHLCV fields") from exc


def _number(value: object, label: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite literal JSON number")
    return float(value)


def _git_provenance() -> tuple[str, bool]:
    root = Path(__file__).resolve().parents[2]
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip())
    except (OSError, subprocess.CalledProcessError):
        return "UNAVAILABLE", True
    return commit or "UNAVAILABLE", dirty


def _outcome_record(outcome: object) -> dict[str, object]:
    # Outcome is intentionally converted at this boundary: accounting stays core-owned.
    return {
        "ticker": outcome.ticker,
        "status": outcome.status.value,
        "entry_price": outcome.entry_price,
        "exit_price": outcome.exit_price,
        "gross_return": outcome.gross_return,
        "net_return": outcome.net_return,
    }


def _validated_pit_metadata(pit_bundle: str | Path | None) -> tuple[str, str, str | None, str | None, set[tuple[str, str]] | None]:
    if pit_bundle is None:
        return "FROZEN_SELECTIONS_NON_PIT", "PIT_GRADE_FALSE", None, None, None
    bundle = validate_pit_bundle(pit_bundle)
    if not bundle.pit_grade:
        raise PitBundleValidationError("validated PIT bundle did not have PIT grade")
    identities = {(row["as_of_date"], row["ticker"]) for row in bundle.schedule}
    return (
        "FROZEN_SELECTIONS_PIT_UNIVERSE_MEMBERSHIP_VALIDATED",
        "PIT_UNIVERSE_MEMBERSHIP_ONLY",
        bundle.bundle_id,
        bundle.composite_sha256,
        identities,
    )


def _source_sha256(filename: str) -> str:
    return hashlib.sha256(Path(__file__).with_name(filename).read_bytes()).hexdigest()


def _canonical_selection_sha256(selected: list[object]) -> str:
    try:
        encoded = json.dumps(selected, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("selected must be canonical JSON-compatible objects") from exc
    return hashlib.sha256(encoded).hexdigest()


def _publish_exclusively(output: Path, artifact: Path, serialized: bytes) -> None:
    """Publish a fully serialized artifact with link's no-replace guarantee.

    If a post-link directory fsync fails, durability is uncertain.  The linked
    destination is preserved for manual inspection and is never auto-deleted.
    """
    output.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=output, prefix=".xsec_replay_records.", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, artifact)
        directory_fd = os.open(output, os.O_RDONLY)
        try:
            try:
                os.fsync(directory_fd)
            except OSError as exc:
                raise ArtifactDurabilityUncertainError(artifact) from exc
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def run_xsec_replay(
    rebalances: Sequence[Mapping[str, Any]], *, output_dir: str | Path,
    max_concurrent_slots: int, total_cost_bps: float, pit_bundle: str | Path | None = None,
) -> Path:
    """Write non-overwriting JSONL canonical records for frozen cross-sectional inputs.

    Validation, including any PIT bundle validation, completes before creating the
    caller-supplied output directory.  A valid bundle is provenance evidence only;
    input rows must still be frozen by the caller and conform to this runner schema.
    """
    if isinstance(max_concurrent_slots, bool) or not isinstance(max_concurrent_slots, int) or max_concurrent_slots < 1:
        raise ValueError("max_concurrent_slots must be a positive integer")
    total_cost_bps = _number(total_cost_bps, "total_cost_bps")
    if total_cost_bps <= 0:
        raise ValueError("total_cost_bps must be positive")
    input_mode, pit_grade, bundle_id, bundle_composite, pit_identities = _validated_pit_metadata(pit_bundle)
    if not isinstance(rebalances, Sequence) or isinstance(rebalances, (str, bytes)):
        raise ValueError("rebalances must be a sequence")

    prepared: list[tuple[Mapping[str, Any], date, float, str, tuple[TickerReplay, ...]]] = []
    rebalance_sleeve_identities: set[tuple[str, str]] = set()
    for rebalance_value in rebalances:
        rebalance = _require_mapping(rebalance_value, "rebalance")
        signal_date = _parse_date(rebalance.get("rebalance_date"), "rebalance_date")
        sleeve = rebalance.get("sleeve")
        factor_order = rebalance.get("factor_order")
        if not isinstance(sleeve, str) or not sleeve:
            raise ValueError("sleeve must be non-empty")
        identity = (signal_date.isoformat(), sleeve)
        if identity in rebalance_sleeve_identities:
            raise ValueError(f"duplicate rebalance/sleeve identity: {identity[0]}, {identity[1]}")
        rebalance_sleeve_identities.add(identity)
        if factor_order not in {"ASC", "DESC"}:
            raise ValueError("factor_order must be ASC or DESC")
        cap = _number(rebalance.get("max_entry_cap"), "max_entry_cap")
        if cap <= 0:
            raise ValueError("max_entry_cap must be positive")
        selected = rebalance.get("selected")
        if not isinstance(selected, list):
            raise ValueError("selected must be a list")
        selection_content_sha256 = _canonical_selection_sha256(selected)
        parsed: list[tuple[float, str, TickerReplay]] = []
        seen: set[str] = set()
        for raw_selection in selected:
            item = _require_mapping(raw_selection, "selected item")
            ticker = item.get("ticker")
            if not isinstance(ticker, str) or not ticker:
                raise ValueError("selected ticker must be non-empty")
            if ticker in seen:
                raise ValueError(f"duplicate selected ticker: {ticker}")
            if pit_identities is not None and (signal_date.isoformat(), ticker) not in pit_identities:
                raise ValueError(f"PIT bundle does not contain selected ticker/date: {ticker}, {signal_date.isoformat()}")
            seen.add(ticker)
            score = _number(item.get("factor_score"), "factor_score")
            parsed.append((score, ticker, TickerReplay(
                ticker=ticker, signal_date=signal_date,
                next_session=_parse_bar(item.get("next_session"), f"{ticker}.next_session"),
                exit_session=_parse_bar(item.get("exit_session"), f"{ticker}.exit_session"),
                capacity_available=False,
            )))
        # Tie-breakers are always ticker ASC, independent of caller list/set order.
        if factor_order == "DESC":
            parsed.sort(key=lambda row: (-row[0], row[1]))
        else:
            parsed.sort(key=lambda row: (row[0], row[1]))
        capacity_ranked = tuple(
            TickerReplay(item.ticker, item.signal_date, item.next_session, item.exit_session, index < max_concurrent_slots)
            for index, (_, _, item) in enumerate(parsed)
        )
        prepared.append((rebalance, signal_date, cap, selection_content_sha256, capacity_ranked))

    output = Path(output_dir)
    artifact = output / "xsec_replay_records.jsonl"
    commit, dirty = _git_provenance()
    runner_source_sha256 = _source_sha256("xsec_runner.py")
    replay_source_sha256 = _source_sha256("xsec_replay.py")
    records: list[dict[str, object]] = []
    for rebalance, signal_date, cap, selection_content_sha256, selected in prepared:
        result = replay_cross_section(ReplayInput(
            selected=selected, max_entry_cap=cap, total_cost_bps=total_cost_bps,
        ))
        filled = [outcome for outcome in result.outcomes if outcome.net_return is not None]
        records.append({
            "schema_version": SCHEMA_VERSION, "record_type": "rebalance_sleeve",
            "runner_git_commit": commit, "runner_tree_dirty": dirty,
            "runner_source_sha256": runner_source_sha256, "xsec_replay_source_sha256": replay_source_sha256,
            "signal_convention": "frozen selection at rebalance_date; entry next-session open; exit later-session close",
            "input_mode": input_mode, "pit_grade": pit_grade,
            "evidence_label": "RESEARCH_ONLY_NON_BINDING",
            "selection_execution_provenance": "CALLER_ASSERTED_UNVERIFIED",
            "frozen_selection_content_sha256": selection_content_sha256,
            "input_bundle_id": bundle_id, "input_bundle_composite_sha256": bundle_composite,
            "capacity_rule": CAPACITY_RULE, "max_concurrent_slots": max_concurrent_slots,
            "cost_assumption": COST_ASSUMPTION, "total_cost_bps": total_cost_bps,
            "rebalance_date": signal_date.isoformat(), "signal_date": signal_date.isoformat(),
            "sleeve": rebalance["sleeve"], "factor_order": rebalance["factor_order"],
            "outcomes": [_outcome_record(outcome) for outcome in result.outcomes],
            "summary": {
                "selected": result.totals.selected, "eligible": result.totals.eligible,
                "filled": result.totals.filled, "no_fill": result.totals.no_fill,
                "censored": result.totals.censored, "capacity_censored": result.totals.capacity_censored,
                "filled_gross_return_total": result.totals.gross_return_total,
                "filled_net_return_total": result.totals.net_return_total,
                "filled_mean_gross_return": (sum(outcome.gross_return for outcome in filled if outcome.gross_return is not None) / len(filled)) if filled else None,
                "filled_mean_net_return": (sum(outcome.net_return for outcome in filled if outcome.net_return is not None) / len(filled)) if filled else None,
            },
        })
    if not records:
        records.append({
            "schema_version": SCHEMA_VERSION, "record_type": "run_summary", "input_mode": input_mode,
            "pit_grade": pit_grade, "evidence_label": "RESEARCH_ONLY_NON_BINDING", "rebalance_count": 0,
            "selection_execution_provenance": "CALLER_ASSERTED_UNVERIFIED",
            "frozen_selection_content_sha256": _canonical_selection_sha256([]),
            "runner_git_commit": commit, "runner_tree_dirty": dirty,
            "runner_source_sha256": runner_source_sha256, "xsec_replay_source_sha256": replay_source_sha256,
        })
    serialized = "".join(json.dumps(record, sort_keys=True, allow_nan=False) + "\n" for record in records).encode("utf-8")
    _publish_exclusively(output, artifact, serialized)
    return artifact
