"""Scan command handler — runs the deterministic scanner pipeline."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def cmd_scan(args) -> int:
    """Run the deterministic scanner and save artifacts + send Telegram alert."""
    from src.core.config import load_config
    from src.core.io import save_json
    from src.pipelines.scanner import run_scan

    config_path = getattr(args, "config", "config/default.yaml")
    config = load_config(config_path)
    asof_date = getattr(args, "date", None)

    result = run_scan(config, asof_date=asof_date)
    scan_date = result["date"]

    # --- Output directory ---
    out_dir = Path("outputs") / scan_date
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Save scan_results ---
    scan_results = {
        "date": scan_date,
        "generated_utc": datetime.utcnow().isoformat(),
        "regime": result["regime"],
        "regime_detail": result.get("regime_detail", {}),
        "universe_size": result["universe_size"],
        "downloaded": result.get("downloaded", 0),
        "signals_total": result.get("signals_total", 0),
        "picks": result["picks"],
        "errors": result.get("errors", []),
    }
    save_json(scan_results, out_dir / f"scan_results_{scan_date}.json")
    logger.info("Saved scan_results_%s.json", scan_date)

    # --- Save regime ---
    regime_out = {
        "date": scan_date,
        "regime": result["regime"],
        **result.get("regime_detail", {}),
    }
    save_json(regime_out, out_dir / f"regime_{scan_date}.json")

    # --- Save execution_watchlist ---
    watchlist = {
        "date": scan_date,
        "regime": result["regime"],
        "universe_size": result["universe_size"],
        "picks": result["picks"],
    }
    save_json(watchlist, out_dir / f"execution_watchlist_{scan_date}.json")
    logger.info("Saved execution_watchlist_%s.json", scan_date)

    # --- Telegram alert ---
    _send_scan_alert(result)

    picks_count = len(result["picks"])
    logger.info("Scan complete: %d picks for %s", picks_count, scan_date)
    return 0


def _send_scan_alert(result: dict) -> None:
    """Send Telegram alert with scan results."""
    try:
        from src.core.alerts import AlertConfig, AlertManager, _regime_emoji, _ticker_display

        alert_config = AlertConfig(enabled=True, channels=["telegram"])
        if not alert_config.telegram_bot_token or not alert_config.telegram_chat_id:
            return

        scan_date = result["date"]
        regime = result["regime"]
        picks = result["picks"]

        emoji = _regime_emoji(regime)
        rd = result.get("regime_detail", {})
        acc_mode = rd.get("acceptance_mode", "—")
        dq_score = rd.get("day_quality_score", 0)
        eligible = rd.get("acceptance_eligible_count", 0)
        breadth = rd.get("market_breadth_pct_above_sma20", 0)

        lines = [
            f"<b>🐉 Dragon Pulse — {scan_date}</b>",
            f"Regime: {emoji} <b>{regime.upper()}</b> | Breadth: {breadth:.0%}",
            f"Signals: {result.get('signals_total', 0)} MR | Eligible: {eligible} | DQ: {dq_score:.0f}/100 → <b>{acc_mode.upper()}</b>",
            "",
        ]

        if not picks:
            if acc_mode == "breadth_suppressed":
                lines.append("📉 Breadth suppressed — no picks today.")
            elif acc_mode == "abstain":
                lines.append("⏸ Day quality too low — abstained.")
            else:
                lines.append("No picks today.")
        else:
            for i, p in enumerate(picks, 1):
                display = _ticker_display(p["ticker"], p.get("name_cn", ""))
                lines.append(
                    f"<b>{i}. {display}</b> "
                    f"Score: {p['score']:.0f}"
                )
                max_entry_str = f" max=¥{p['max_entry_price']:.2f}" if p.get("max_entry_price") else ""
                lines.append(
                    f"   Entry: ¥{p['entry_price']:.2f}{max_entry_str} | "
                    f"Stop: ¥{p['stop_loss']:.2f} | T1: ¥{p['target_1']:.2f} | "
                    f"Hold: {p['holding_period']}d"
                )
                if p.get("reason_summary"):
                    lines.append(f"   {p['reason_summary']}")
                lines.append("")

        mgr = AlertManager(alert_config)
        mgr.send_alert(
            title=f"Dragon Pulse Scan: {scan_date}",
            message="\n".join(lines),
            data={"asof": scan_date},
            priority="high" if picks else "low",
        )
        logger.info("Telegram alert sent")
    except Exception as e:
        logger.warning("Failed to send Telegram alert: %s", e)
