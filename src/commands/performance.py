"""Performance / backtest command handler."""

from __future__ import annotations

import logging
from pathlib import Path

from src.features.performance.backtest import (
    compute_watchlist_backtest,
    has_execution_watchlists_in_range,
    compute_hit10_backtest,
    load_execution_watchlists_in_range,
    load_picks_in_range,
    write_backtest_outputs,
    write_watchlist_backtest_outputs,
)
from src.features.performance.calibration import (
    build_calibration_suggestions,
    write_calibration_report,
)

logger = logging.getLogger(__name__)


def cmd_performance(args) -> int:
    """
    Backtest picks from existing `outputs/YYYY-MM-DD/` artifacts.

    KPI:
    - Entry = baseline close (first valid close on/after baseline date)
    - Success = hit +10% within next 7 trading days (max High by default)
    """
    logger.info("=" * 60)
    logger.info("PERFORMANCE BACKTEST")
    logger.info("=" * 60)

    outputs_root = getattr(args, "outputs_root", "outputs")
    start_date = getattr(args, "start", None)
    end_date = getattr(args, "end", None)
    source = str(getattr(args, "source", "auto") or "auto").lower()
    out_dir = getattr(args, "out_dir", "outputs/performance")

    use_watchlist = source == "watchlist" or (
        source == "auto"
        and has_execution_watchlists_in_range(
            outputs_root,
            start_date=start_date,
            end_date=end_date,
        )
    )

    if use_watchlist:
        watchlist_picks = load_execution_watchlists_in_range(
            outputs_root,
            start_date=start_date,
            end_date=end_date,
        )
        if watchlist_picks.empty:
            logger.warning("No execution watchlists found for the requested range.")
            return 1

        perf_detail, perf_by_date, perf_by_group, summary = compute_watchlist_backtest(
            watchlist_picks,
            outputs_root=outputs_root,
            auto_adjust=bool(getattr(args, "auto_adjust", False)),
            threads=not bool(getattr(args, "no_threads", False)),
        )
        paths = write_watchlist_backtest_outputs(
            perf_detail,
            perf_by_date,
            perf_by_group,
            summary,
            output_dir=out_dir,
        )

        logger.info(
            "Watchlist summary: total=%s matured=%s cancelled=%s open=%s avg_pnl=%s",
            summary.get("total_picks"),
            summary.get("matured_picks"),
            summary.get("cancelled_picks"),
            summary.get("open_or_partial_picks"),
            summary.get("avg_pnl_pct"),
        )
        if summary.get("target_hit_rate") is not None:
            logger.info(
                "Matured picks: target_hit_rate=%.3f stop_hit_rate=%.3f positive_pnl_rate=%.3f",
                float(summary.get("target_hit_rate")),
                float(summary.get("stop_hit_rate")),
                float(summary.get("positive_pnl_rate")),
            )

        logger.info("\nArtifacts written:")
        for k, v in paths.items():
            logger.info(f"  - {k}: {v}")
        return 0

    picks = load_picks_in_range(outputs_root, start_date=start_date, end_date=end_date)
    if not picks:
        logger.warning("No output dates found for the requested range.")
        return 1

    perf_detail, perf_by_date, perf_by_component, perf_by_feature = compute_hit10_backtest(
        picks,
        outputs_root=outputs_root,
        forward_trading_days=int(getattr(args, "forward_days", 7)),
        hit_threshold_pct=float(getattr(args, "threshold", 10.0)),
        use_high=not bool(getattr(args, "use_close_only", False)),
        exclude_entry_day=not bool(getattr(args, "include_entry_day", False)),
        auto_adjust=bool(getattr(args, "auto_adjust", False)),
        threads=not bool(getattr(args, "no_threads", False)),
    )

    paths = write_backtest_outputs(
        perf_detail,
        perf_by_date,
        perf_by_component,
        perf_by_feature,
        output_dir=out_dir,
    )

    # Calibration suggestions (writes markdown + yaml snippet)
    try:
        suggestions, artifacts = build_calibration_suggestions(
            perf_detail,
            outputs_root=outputs_root,
            min_rows=15,
        )
        cal_paths = write_calibration_report(
            perf_by_component,
            perf_by_date,
            suggestions,
            output_dir=out_dir,
        )
        paths.update(cal_paths)

        # Persist threshold tables for inspection
        for k, df in artifacts.items():
            if df is None or df.empty:
                continue
            p = Path(out_dir) / f"{k}.csv"
            df.to_csv(p, index=False)
            paths[k] = str(p)
    except Exception:
        # don't fail the run for calibration
        pass

    # Log a compact summary
    try:
        overall = perf_by_component[perf_by_component["component"] == "all"].iloc[0]
        logger.info(
            f"Overall: n={int(overall['n'])} hit_rate={float(overall['hit_rate']):.3f}"
        )
    except Exception:
        pass

    logger.info("\nArtifacts written:")
    for k, v in paths.items():
        logger.info(f"  - {k}: {v}")

    return 0
