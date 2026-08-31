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
        "download_failed": result.get("download_failed", 0),
        "download_health": result.get("download_health", "ok"),
        "circuit_breaker": result.get("circuit_breaker"),
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

    # --- Telegram alert (skipped when morning workflow handles it) ---
    import os
    if not os.environ.get("DRAGON_PULSE_SKIP_TELEGRAM"):
        _send_scan_alert(result)
    else:
        logger.info("Telegram alert skipped (DRAGON_PULSE_SKIP_TELEGRAM set)")

    picks_count = len(result["picks"])
    logger.info("Scan complete: %d picks for %s", picks_count, scan_date)
    return 0


def _send_scan_alert(result: dict) -> None:
    """Send Telegram alert with scan results."""
    try:
        from src.core.alerts import AlertConfig, AlertManager, _regime_emoji, _regime_cn, _ticker_display, _translate_reason_summary

        alert_config = AlertConfig(enabled=True, channels=["telegram"])
        if not alert_config.telegram_bot_token or not alert_config.telegram_chat_id:
            return

        scan_date = result["date"]
        regime = result["regime"]
        regime_label = _regime_cn(regime)
        picks = result["picks"]

        emoji = _regime_emoji(regime)
        rd = result.get("regime_detail", {})
        selection_error = rd.get("selection_error")
        acc_mode = rd.get("acceptance_mode", "—")
        dq_score = rd.get("day_quality_score", 0)
        eligible = rd.get("acceptance_eligible_count", 0)
        breadth = rd.get("market_breadth_pct_above_sma20", 0)

        ACC_MODE_CN = {
            "breadth_suppressed": "宽度受限",
            "abstain": "放弃",
            "normal": "正常",
            "relaxed": "宽松",
        }
        acc_label = ACC_MODE_CN.get(acc_mode, acc_mode.upper())

        from src.core.io import count_abstention_streak
        from src.core.message_format import (
            RULE,
            abstention_note,
            append_footer,
            meta_line,
            pick_block,
            title_line,
        )

        alert_title = f"\U0001f409 龙脉扫描 — {scan_date}"
        lines = [
            title_line("", alert_title),
            meta_line(
                f"{emoji} <b>{regime_label}</b>",
                f"宽度 {breadth:.0%}",
                f"选股 <b>{len(picks)}</b>",
            ),
            RULE,
        ]

        if not picks:
            if selection_error:
                lines.append("⚠️ <b>选股中止</b> — 行业元数据不完整，未生成可执行候选")
            elif acc_mode == "breadth_suppressed":
                lines.append("今日无选股 — 市场宽度受限")
            elif acc_mode == "abstain":
                lines.append("今日无选股 — 日质量过低，主动放弃")
            else:
                lines.append("今日无选股 — 无信号通过筛选")
            if not selection_error:
                streak, since = count_abstention_streak(scan_date, current_picks=0)
                note = abstention_note(streak, since)
                if note:
                    lines.append(note)
        else:
            for i, p in enumerate(picks, 1):
                reason = _translate_reason_summary(p.get("reason_summary", ""))
                lines += pick_block(
                    index=i,
                    display=_ticker_display(p["ticker"], p.get("name_cn", "")),
                    entry=p.get("entry_price"),
                    stop=p.get("stop_loss"),
                    target=p.get("target_1"),
                    max_entry=p.get("max_entry_price"),
                    holding=p.get("holding_period"),
                    score=p.get("score"),
                    sub=reason,
                )
                lines.append("")

        append_footer(
            lines,
            [
                "<i>"
                + meta_line(
                    f"信号 {result.get('signals_total', 0)}",
                    f"入选 {eligible}",
                    f"日质量 {dq_score:.0f}/100 → {acc_label}",
                )
                + "</i>"
            ],
        )

        mgr = AlertManager(alert_config)
        mgr.send_alert(
            title=alert_title,
            message="\n".join(lines),
            data={"asof": scan_date},
            priority="high" if picks or selection_error else "low",
        )
        logger.info("Telegram alert sent")
    except Exception as e:
        logger.warning("Failed to send Telegram alert: %s", e)
