"""
LLM Accuracy Tracker
====================

Compares LLM confidence labels (HIGH / MEDIUM / SPECULATIVE) against actual
pick outcomes.  When a confidence tier's false-positive rate exceeds a
configurable threshold (default 40%), a penalty multiplier is emitted so
future composite scores for that tier are discounted.

Usage:
    from src.tracking.llm_accuracy import compute_accuracy, get_confidence_penalties

    acc = compute_accuracy("outputs")
    penalties = get_confidence_penalties(acc, max_fp_rate=0.40)
    # penalties = {"HIGH": 1.0, "MEDIUM": 0.85, "SPECULATIVE": 0.70}
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TierAccuracy:
    """Accuracy statistics for a single confidence tier."""
    tier: str
    n_picks: int
    n_hits: int
    hit_rate: float
    false_positive_rate: float  # 1 - hit_rate


@dataclass(frozen=True)
class AccuracyReport:
    """Full accuracy report across all tiers."""
    tiers: list[TierAccuracy]
    total_picks: int
    total_hits: int
    overall_hit_rate: float


def compute_accuracy(
    outputs_root: str | Path = "outputs",
    perf_detail_path: Optional[str | Path] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> AccuracyReport:
    """
    Load LLM confidence labels from top5 JSONs and join with perf outcomes.

    Returns an AccuracyReport with per-tier hit rates.
    """
    root = Path(outputs_root)
    rows = []

    for date_dir in sorted(root.iterdir()):
        if not date_dir.is_dir() or len(date_dir.name) != 10:
            continue
        date_str = date_dir.name
        if start_date and date_str < start_date:
            continue
        if end_date and date_str > end_date:
            continue

        top5_file = date_dir / f"weekly_scanner_top5_{date_str}.json"
        if not top5_file.exists():
            continue

        try:
            data = json.loads(top5_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        for entry in data.get("top5", []):
            if not isinstance(entry, dict):
                continue
            rows.append({
                "baseline_date": date_str,
                "ticker": str(entry.get("ticker", "")).upper(),
                "confidence": str(entry.get("confidence", "UNKNOWN")).upper(),
            })

    if not rows:
        return AccuracyReport(tiers=[], total_picks=0, total_hits=0, overall_hit_rate=0)

    labels_df = pd.DataFrame(rows)

    # Load performance outcomes
    perf_path = (
        Path(perf_detail_path) if perf_detail_path
        else root / "performance" / "perf_detail.csv"
    )
    if not perf_path.exists():
        logger.warning(f"perf_detail.csv not found at {perf_path}")
        return AccuracyReport(tiers=[], total_picks=0, total_hits=0, overall_hit_rate=0)

    perf_df = pd.read_csv(perf_path)
    perf_df["ticker"] = perf_df["ticker"].astype(str).str.upper()

    merged = labels_df.merge(
        perf_df[["baseline_date", "ticker", "hit10"]],
        on=["baseline_date", "ticker"],
        how="inner",
    )

    if merged.empty:
        return AccuracyReport(tiers=[], total_picks=0, total_hits=0, overall_hit_rate=0)

    # Per-tier stats
    tiers = []
    for tier, group in merged.groupby("confidence"):
        n = len(group)
        hits = int(group["hit10"].sum())
        hr = hits / n if n > 0 else 0
        tiers.append(TierAccuracy(
            tier=str(tier),
            n_picks=n,
            n_hits=hits,
            hit_rate=round(hr, 4),
            false_positive_rate=round(1 - hr, 4),
        ))

    tiers.sort(key=lambda t: t.tier)

    total = len(merged)
    total_hits = int(merged["hit10"].sum())

    return AccuracyReport(
        tiers=tiers,
        total_picks=total,
        total_hits=total_hits,
        overall_hit_rate=round(total_hits / total, 4) if total else 0,
    )


def get_confidence_penalties(
    report: AccuracyReport,
    max_fp_rate: float = 0.40,
    penalty_step: float = 0.15,
) -> dict[str, float]:
    """
    Compute composite-score penalty multipliers per confidence tier.

    If a tier's false-positive rate exceeds `max_fp_rate`, apply a discount:
      multiplier = max(0.5, 1.0 - penalty_step * excess_buckets)

    where excess_buckets = ceil((fp_rate - max_fp_rate) / 0.10).

    Returns dict like {"HIGH": 1.0, "MEDIUM": 0.85, "SPECULATIVE": 0.70}.
    """
    import math

    penalties: dict[str, float] = {}
    for tier in report.tiers:
        if tier.false_positive_rate <= max_fp_rate:
            penalties[tier.tier] = 1.0
        else:
            excess = tier.false_positive_rate - max_fp_rate
            buckets = math.ceil(excess / 0.10)
            mult = max(0.5, 1.0 - penalty_step * buckets)
            penalties[tier.tier] = round(mult, 2)
            logger.info(
                f"LLM accuracy penalty: {tier.tier} FP={tier.false_positive_rate:.0%} "
                f"-> multiplier={mult:.2f}"
            )

    return penalties


def write_accuracy_report(
    report: AccuracyReport,
    output_path: str | Path = "outputs/calibration/llm_accuracy.md",
) -> None:
    """Write a human-readable accuracy report."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# LLM Confidence Accuracy Report",
        f"\nTotal picks: {report.total_picks}, Hits: {report.total_hits}, "
        f"Overall hit rate: {report.overall_hit_rate:.0%}",
        "\n## Per-Tier Breakdown",
        "| Tier | Picks | Hits | Hit Rate | FP Rate |",
        "|------|-------|------|----------|---------|",
    ]
    for t in report.tiers:
        lines.append(
            f"| {t.tier} | {t.n_picks} | {t.n_hits} | "
            f"{t.hit_rate:.0%} | {t.false_positive_rate:.0%} |"
        )

    penalties = get_confidence_penalties(report)
    lines.append("\n## Applied Penalties")
    for tier, mult in sorted(penalties.items()):
        status = "OK" if mult >= 1.0 else f"PENALIZED x{mult}"
        lines.append(f"- **{tier}**: {status}")

    path.write_text("\n".join(lines) + "\n")
    logger.info(f"LLM accuracy report written to {path}")
