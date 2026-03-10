"""Calibrate command handler — run parameter sweep + LLM accuracy analysis."""

from __future__ import annotations
import logging
from src.features.performance.sweep import run_sweep
from src.tracking.llm_accuracy import compute_accuracy, write_accuracy_report

logger = logging.getLogger(__name__)


def cmd_calibrate(args) -> int:
    """Run parameter sweep calibration and LLM accuracy analysis."""
    outputs_root = getattr(args, "outputs_root", "outputs")
    start_date = getattr(args, "start", None)
    end_date = getattr(args, "end", None)
    out_dir = getattr(args, "out_dir", "outputs/calibration")

    # --- Part 1: Parameter sweep ---
    logger.info("Running calibration sweep...")

    result = run_sweep(
        outputs_root=outputs_root,
        start_date=start_date,
        end_date=end_date,
        output_dir=out_dir,
    )

    status = result.get("status", "unknown")
    if status == "no_data":
        logger.error("No top5 score data found in outputs/. Run `python main.py all` first.")
        return 1
    if status == "no_perf_data":
        logger.error("No perf_detail.csv found. Run `python main.py performance` first.")
        return 1
    if status == "no_matches":
        logger.error("No matching picks between scores and performance data.")
        return 1

    logger.info(f"Sweep CSV:  {result.get('sweep_csv')}")
    logger.info(f"Config:     {result.get('config_yaml')}")
    logger.info(f"Report:     {result.get('report_md')}")

    for rec in result.get("recommendations", []):
        logger.info(f"  {rec.param_name}: {rec.current_value} -> {rec.recommended_value} ({rec.rationale})")

    # --- Part 2: LLM accuracy analysis ---
    logger.info("\nRunning LLM confidence accuracy analysis...")

    acc_report = compute_accuracy(
        outputs_root=outputs_root,
        start_date=start_date,
        end_date=end_date,
    )

    if acc_report.total_picks > 0:
        report_path = f"{out_dir}/llm_accuracy.md"
        write_accuracy_report(acc_report, output_path=report_path)
        logger.info(f"LLM accuracy: {acc_report.overall_hit_rate:.0%} overall ({acc_report.total_picks} picks)")
        for tier in acc_report.tiers:
            logger.info(f"  {tier.tier}: {tier.hit_rate:.0%} hit rate ({tier.n_picks} picks, FP={tier.false_positive_rate:.0%})")
    else:
        logger.warning("No LLM confidence data matched with outcomes — skipping accuracy report")

    return 0
