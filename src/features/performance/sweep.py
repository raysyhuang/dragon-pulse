"""
Parameter Sweep Engine for Calibration
=======================================

Sweeps key selection thresholds against historical pick outcomes to find
the parameter set that maximizes hit-rate and profit factor jointly.

Extends the existing calibration.py with composite_score sweeping
and a balanced objective function.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SweepResult:
    """Result of a single parameter sweep point."""
    param_name: str
    param_value: float
    n_picks: int
    n_hits: int
    hit_rate: float
    avg_return_pct: float
    profit_factor: float


@dataclass(frozen=True)
class Recommendation:
    """Final recommendation from the sweep."""
    param_name: str
    current_value: float
    recommended_value: float
    expected_hit_rate: float
    expected_picks_per_month: float
    rationale: str


def load_top5_with_scores(
    outputs_root: str | Path = "outputs",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load composite scores and confidence from weekly_scanner_top5 JSON files.

    Returns DataFrame with: baseline_date, ticker, composite_score, confidence,
    technical_score, catalyst_score, market_activity_score
    """
    root = Path(outputs_root)
    rows = []

    for date_dir in sorted(root.iterdir()):
        if not date_dir.is_dir():
            continue
        date_str = date_dir.name
        if len(date_str) != 10:
            continue
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
            scores = entry.get("scores", {})
            rows.append({
                "baseline_date": date_str,
                "ticker": str(entry.get("ticker", "")).upper(),
                "composite_score": float(entry.get("composite_score", 0)),
                "confidence": str(entry.get("confidence", "UNKNOWN")),
                "technical_score": float(scores.get("technical", 0)),
                "catalyst_score": float(scores.get("catalyst", 0)),
                "market_activity_score": float(scores.get("market_activity", 0)),
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def sweep_threshold(
    merged: pd.DataFrame,
    param_col: str,
    thresholds: list[float],
    hit_col: str = "hit10",
    return_col: str = "max_return_pct",
) -> list[SweepResult]:
    """Sweep a numeric threshold and compute metrics at each level."""
    results = []
    for thresh in sorted(thresholds):
        subset = merged[merged[param_col] >= thresh].copy()
        n = len(subset)
        if n == 0:
            results.append(SweepResult(
                param_name=param_col, param_value=thresh,
                n_picks=0, n_hits=0, hit_rate=0, avg_return_pct=0, profit_factor=0,
            ))
            continue

        hits = subset[hit_col].dropna()
        n_hits = int(hits.sum()) if not hits.empty else 0
        hit_rate = float(hits.mean()) if not hits.empty else 0

        returns = subset[return_col].dropna()
        avg_ret = float(returns.mean()) if not returns.empty else 0
        pos_sum = float(returns[returns > 0].sum())
        neg_sum = abs(float(returns[returns <= 0].sum()))
        pf = pos_sum / neg_sum if neg_sum > 0 else (10.0 if pos_sum > 0 else 0)

        results.append(SweepResult(
            param_name=param_col, param_value=thresh,
            n_picks=n, n_hits=n_hits, hit_rate=round(hit_rate, 4),
            avg_return_pct=round(avg_ret, 2), profit_factor=round(pf, 2),
        ))

    return results


def find_optimal(
    results: list[SweepResult],
    min_picks_per_month: int = 8,
    total_months: float = 1.0,
) -> Optional[SweepResult]:
    """
    Find the threshold that maximizes a balanced objective:
    0.45 * hit_rate + 0.35 * profit_factor + 0.20 * volume
    """
    candidates = [r for r in results if r.n_picks / max(total_months, 0.1) >= min_picks_per_month]
    if not candidates:
        return None

    max_hr = max(c.hit_rate for c in candidates) or 1
    max_pf = max(c.profit_factor for c in candidates) or 1
    max_n = max(c.n_picks for c in candidates) or 1

    best = max(candidates, key=lambda c: (
        0.45 * (c.hit_rate / max_hr)
        + 0.35 * (c.profit_factor / max_pf)
        + 0.20 * (c.n_picks / max_n)
    ))
    return best


def run_sweep(
    outputs_root: str | Path = "outputs",
    perf_detail_path: Optional[str | Path] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: str | Path = "outputs/calibration",
) -> dict[str, Any]:
    """
    Run composite_score + technical_score sweeps and produce recommendations.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    scores_df = load_top5_with_scores(outputs_root, start_date, end_date)
    if scores_df.empty:
        logger.warning("No top5 scores found — nothing to calibrate")
        return {"status": "no_data"}

    perf_path = Path(perf_detail_path) if perf_detail_path else Path(outputs_root) / "performance" / "perf_detail.csv"
    if not perf_path.exists():
        logger.warning(f"perf_detail.csv not found at {perf_path} — run `python main.py performance` first")
        return {"status": "no_perf_data"}

    perf_df = pd.read_csv(perf_path)
    perf_df["ticker"] = perf_df["ticker"].astype(str).str.upper()

    merged = scores_df.merge(
        perf_df[["baseline_date", "ticker", "hit10", "max_return_pct"]],
        on=["baseline_date", "ticker"],
        how="inner",
    )

    if merged.empty:
        logger.warning("No matching picks between scores and outcomes")
        return {"status": "no_matches"}

    logger.info(f"Sweeping on {len(merged)} picks across {merged['baseline_date'].nunique()} days")

    dates = sorted(merged["baseline_date"].unique())
    total_days = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days + 1
    total_months = max(total_days / 30.0, 0.1)

    # Sweeps
    composite_results = sweep_threshold(
        merged, "composite_score",
        [round(x, 1) for x in np.arange(4.0, 8.5, 0.5)],
    )
    tech_results = sweep_threshold(
        merged, "technical_score",
        [round(x, 1) for x in np.arange(5.0, 9.0, 0.5)],
    )

    optimal_composite = find_optimal(composite_results, total_months=total_months)
    optimal_tech = find_optimal(tech_results, total_months=total_months)

    recommendations = []
    if optimal_composite:
        recommendations.append(Recommendation(
            param_name="quality_filters_weekly.min_composite_score",
            current_value=6.5, recommended_value=optimal_composite.param_value,
            expected_hit_rate=optimal_composite.hit_rate,
            expected_picks_per_month=optimal_composite.n_picks / max(total_months, 0.1),
            rationale=f"hit={optimal_composite.hit_rate:.0%}, PF={optimal_composite.profit_factor:.2f}, n={optimal_composite.n_picks}",
        ))
    if optimal_tech:
        recommendations.append(Recommendation(
            param_name="quality_filters_weekly.min_technical_score",
            current_value=7.0, recommended_value=optimal_tech.param_value,
            expected_hit_rate=optimal_tech.hit_rate,
            expected_picks_per_month=optimal_tech.n_picks / max(total_months, 0.1),
            rationale=f"hit={optimal_tech.hit_rate:.0%}, PF={optimal_tech.profit_factor:.2f}, n={optimal_tech.n_picks}",
        ))

    # Write artifacts
    all_results = composite_results + tech_results
    sweep_df = pd.DataFrame([{
        "param_name": r.param_name, "threshold": r.param_value,
        "n_picks": r.n_picks, "n_hits": r.n_hits, "hit_rate": r.hit_rate,
        "avg_return_pct": r.avg_return_pct, "profit_factor": r.profit_factor,
    } for r in all_results])
    sweep_path = out_dir / "sweep_results.csv"
    sweep_df.to_csv(sweep_path, index=False)

    config_lines = [
        "# Auto-generated calibration recommendations",
        f"# Based on {len(merged)} picks, {dates[0]} to {dates[-1]}",
        "", "quality_filters_weekly:",
    ]
    for rec in recommendations:
        key = rec.param_name.split(".")[-1]
        config_lines.append(f"  {key}: {rec.recommended_value}  # was {rec.current_value}, hit_rate={rec.expected_hit_rate:.0%}")
    config_path = out_dir / "recommended_config.yaml"
    config_path.write_text("\n".join(config_lines) + "\n")

    report_lines = [
        "# Calibration Sweep Report",
        f"\nDate range: {dates[0]} to {dates[-1]} ({len(merged)} picks, {total_months:.1f} months)",
        "\n## Recommendations",
    ]
    for rec in recommendations:
        report_lines.append(f"- **{rec.param_name}**: {rec.current_value} → {rec.recommended_value}")
        report_lines.append(f"  Hit rate: {rec.expected_hit_rate:.0%}, Picks/month: {rec.expected_picks_per_month:.0f}")

    report_lines.append("\n## Composite Score Sweep")
    report_lines.append("| Threshold | Picks | Hits | Hit Rate | Avg Ret | PF |")
    report_lines.append("|-----------|-------|------|----------|---------|-----|")
    for r in composite_results:
        report_lines.append(f"| {r.param_value:.1f} | {r.n_picks} | {r.n_hits} | {r.hit_rate:.0%} | {r.avg_return_pct:+.1f}% | {r.profit_factor:.2f} |")

    report_lines.append("\n## Technical Score Sweep")
    report_lines.append("| Threshold | Picks | Hits | Hit Rate | Avg Ret | PF |")
    report_lines.append("|-----------|-------|------|----------|---------|-----|")
    for r in tech_results:
        report_lines.append(f"| {r.param_value:.1f} | {r.n_picks} | {r.n_hits} | {r.hit_rate:.0%} | {r.avg_return_pct:+.1f}% | {r.profit_factor:.2f} |")

    report_path = out_dir / "calibration_report.md"
    report_path.write_text("\n".join(report_lines) + "\n")

    logger.info(f"Calibration complete → {out_dir}")
    for rec in recommendations:
        logger.info(f"  {rec.param_name}: {rec.current_value} → {rec.recommended_value}")

    return {
        "status": "ok",
        "sweep_csv": str(sweep_path),
        "config_yaml": str(config_path),
        "report_md": str(report_path),
        "recommendations": recommendations,
    }
