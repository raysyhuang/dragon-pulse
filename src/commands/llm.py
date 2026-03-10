"""LLM ranking command handler."""

from __future__ import annotations
import json
import logging
from pathlib import Path
from src.core.helpers import get_ny_date, get_trading_date
from src.core.config import load_config, get_config_value
from src.core.io import get_run_dir, save_json
from src.core.llm import rank_weekly_candidates

logger = logging.getLogger(__name__)


def cmd_llm(args) -> int:
    """Run LLM ranking on weekly scanner packets."""
    # Use trading date for outputs (excludes weekends)
    if args.date:
        from datetime import datetime as dt
        date_obj = dt.strptime(args.date, "%Y-%m-%d").date()
        date_obj = get_trading_date(date_obj)  # Convert to trading date if weekend
        date_str = date_obj.strftime("%Y-%m-%d")
    else:
        date_obj = get_trading_date()
        date_str = date_obj.strftime("%Y-%m-%d")

    # Load config for quality gate thresholds
    config = load_config(getattr(args, "config", None))
    min_composite = float(get_config_value(
        config, "quality_filters_weekly", "min_composite_score", default=6.5
    ))
    min_confidence = str(get_config_value(
        config, "quality_filters_weekly", "min_confidence", default="MEDIUM"
    ))
    method_version = str(get_config_value(
        config, "runtime", "method_version", default="v3.2-CN"
    ))

    # Load packets
    run_dir = get_run_dir(date_obj, "outputs")
    packets_file = run_dir / f"weekly_scanner_packets_{date_str}.json"

    if not packets_file.exists():
        logger.error(f"Packets file not found: {packets_file}")
        logger.info("Run weekly scanner first to generate packets.")
        return 1

    with open(packets_file, "r", encoding="utf-8") as f:
        packets_data = json.load(f)

    packets = packets_data.get("packets", [])
    if not packets:
        logger.error("No packets found in file")
        return 1

    logger.info(f"Loaded {len(packets)} packets")
    logger.info(f"Using {args.provider} / {args.model or 'default'}")
    logger.info(f"Post-LLM gate: min_composite={min_composite}, min_confidence={min_confidence}")

    # Rank candidates
    logger.info(f"Calling {args.provider} API (this may take a minute)...")
    try:
        result = rank_weekly_candidates(
            packets=packets,
            provider=args.provider,
            model=args.model,
            api_key=args.api_key,
            min_composite_score=min_composite,
            min_confidence=min_confidence,
            method_version=method_version,
        )
    except Exception as e:
        logger.error(f"Error calling API: {e}", exc_info=True)
        return 1

    # Save output
    output_file = run_dir / f"weekly_scanner_top5_{date_str}.json"
    save_json(result, output_file)

    top5 = result.get("top5", [])
    gate_rejections = result.get("gate_rejections", [])

    logger.info(f"\nSaved results: {output_file}")
    if gate_rejections:
        logger.info(f"Gate rejected {len(gate_rejections)} picks:")
        for r in gate_rejections:
            logger.info(f"  ✗ {r}")

    logger.info(f"\nFinal picks ({len(top5)}):")
    for item in top5:
        ticker = item.get("ticker", "Unknown")
        composite = item.get("composite_score", 0)
        confidence = item.get("confidence", "Unknown")
        logger.info(f"  {item.get('rank', '?')}. {ticker} (score: {composite:.2f}, confidence: {confidence})")

    if not top5:
        logger.warning("No picks survived the quality gate — this is by design. Better 0 picks than bad picks.")

    return 0

