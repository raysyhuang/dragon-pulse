#!/usr/bin/env python3
"""
Pre-Market Gap Validator (Morning Check)
=========================================

Run at 9:25-9:35 AM Shanghai time to validate yesterday's picks
against the opening auction.

Rules:
1. Gap Rule: If a pick gaps up >3% at the open, it's "too far from entry" → CANCEL.
2. Volume Confirmation: If first-15-min volume < 10% of previous day's volume → WARN.
3. Limit-Down Rule: If a pick gaps down >5%, stop-loss is already breached → CANCEL.

Usage:
    python scripts/morning_check.py
    python scripts/morning_check.py --date 2025-12-28
    python scripts/morning_check.py --picks-file outputs/2025-12-28/dragon_pulse_picks.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add project root and src to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

logger = logging.getLogger(__name__)


@dataclass
class PreFlightResult:
    """Result of pre-flight check for a single pick."""
    ticker: str
    name_cn: str
    entry_price: float
    open_price: float
    gap_pct: float
    prev_volume: int
    open_volume_15m: int       # First 15 min volume (0 if unavailable)
    volume_ratio: float        # open_volume_15m / (prev_volume * 0.10)
    action: str                # "GO", "WARN", "CANCEL"
    reasons: list[str]


def check_gap(
    entry_price: float,
    open_price: float,
    max_gap_up_pct: float = 3.0,
    max_gap_down_pct: float = 5.0,
    stop_loss: float | None = None,
) -> tuple[str, list[str]]:
    """
    Check if the opening gap invalidates the entry.

    If stop_loss is provided, cancel when open < stop_loss (per-pick precision).
    Otherwise fall back to flat max_gap_down_pct threshold.

    Returns (action, reasons).
    """
    gap_pct = (open_price / entry_price - 1) * 100
    reasons = []

    if gap_pct > max_gap_up_pct:
        reasons.append(
            f"高开 +{gap_pct:.1f}% 超过{max_gap_up_pct}%上限 — 追高风险，取消入场"
        )
        return "CANCEL", reasons

    # Per-pick stop check: if open is below the stop_loss, cancel immediately
    if stop_loss is not None and open_price < stop_loss:
        reasons.append(
            f"开盘 ¥{open_price:.2f} < 止损 ¥{stop_loss:.2f} — 开盘即触发止损"
        )
        return "CANCEL", reasons

    if gap_pct < -max_gap_down_pct:
        reasons.append(
            f"低开 {gap_pct:.1f}% 超过{max_gap_down_pct}%下限 — 止损已触发"
        )
        return "CANCEL", reasons

    if gap_pct > max_gap_up_pct * 0.6:
        reasons.append(
            f"高开 +{gap_pct:.1f}% 偏高 — 建议减仓"
        )
        return "WARN", reasons

    return "GO", []


def check_volume_confirmation(
    prev_day_volume: int,
    first_15m_volume: int,
    min_volume_ratio: float = 0.10,
) -> tuple[str, list[str]]:
    """
    Check if opening volume confirms the breakout.

    If first 15 minutes of trading has < 10% of previous full-day volume,
    the breakout lacks conviction.
    """
    if prev_day_volume <= 0:
        return "GO", []

    ratio = first_15m_volume / prev_day_volume if prev_day_volume > 0 else 0

    if ratio < min_volume_ratio:
        return "WARN", [
            f"开盘量不足: {first_15m_volume:,} = 前日{ratio:.1%}"
            f"(需>{min_volume_ratio:.0%}) — 动能偏弱"
        ]

    return "GO", []


def run_preflight(
    picks: list[dict],
    open_prices: dict[str, float],
    prev_volumes: dict[str, int],
    first_15m_volumes: Optional[dict[str, int]] = None,
    config: Optional[dict] = None,
) -> list[PreFlightResult]:
    """
    Run pre-flight checks on a list of picks.

    Args:
        picks: List of dicts with ticker, name_cn, entry_price
        open_prices: Dict of ticker → opening price
        prev_volumes: Dict of ticker → previous day's total volume
        first_15m_volumes: Dict of ticker → first 15-min volume (optional)
        config: Override thresholds (max_gap_up_pct, max_gap_down_pct, min_volume_ratio)

    Returns:
        List of PreFlightResult for each pick.
    """
    if config is None:
        config = {}
    if first_15m_volumes is None:
        first_15m_volumes = {}

    max_gap_up = config.get("max_gap_up_pct", 3.0)
    max_gap_down = config.get("max_gap_down_pct", 5.0)
    min_vol_ratio = config.get("min_volume_ratio", 0.10)

    results = []

    for pick in picks:
        ticker = pick.get("ticker", "")
        name_cn = pick.get("name_cn", ticker)
        entry_price = float(pick.get("entry_price", 0))

        open_price = open_prices.get(ticker, 0)
        prev_vol = prev_volumes.get(ticker, 0)
        open_vol_15m = first_15m_volumes.get(ticker, 0)

        if not open_price or not entry_price:
            results.append(PreFlightResult(
                ticker=ticker, name_cn=name_cn,
                entry_price=entry_price, open_price=0,
                gap_pct=0, prev_volume=prev_vol,
                open_volume_15m=0, volume_ratio=0,
                action="WARN", reasons=["无开盘价"],
            ))
            continue

        gap_pct = (open_price / entry_price - 1) * 100

        # Check gap (with per-pick stop_loss and max_entry_price if available)
        pick_stop_loss = pick.get("stop_loss")
        if pick_stop_loss is not None:
            pick_stop_loss = float(pick_stop_loss)
        pick_max_entry = pick.get("max_entry_price")
        if pick_max_entry is not None:
            pick_max_entry = float(pick_max_entry)

        if pick_max_entry is not None and pick_max_entry > 0 and open_price > pick_max_entry:
            gap_action, gap_reasons = "CANCEL", [
                f"开盘 ¥{open_price:.2f} > 最高可买价 ¥{pick_max_entry:.2f} — no-chase取消入场"
            ]
        else:
            gap_action, gap_reasons = check_gap(
                entry_price, open_price, max_gap_up, max_gap_down,
                stop_loss=pick_stop_loss,
            )

        # Check volume
        vol_action, vol_reasons = check_volume_confirmation(
            prev_vol, open_vol_15m, min_vol_ratio,
        )

        # Combine: CANCEL > WARN > GO
        all_reasons = gap_reasons + vol_reasons
        if gap_action == "CANCEL":
            final_action = "CANCEL"
        elif vol_action == "WARN" or gap_action == "WARN":
            final_action = "WARN"
        else:
            final_action = "GO"

        vol_ratio = open_vol_15m / (prev_vol * min_vol_ratio) if prev_vol > 0 else 0

        results.append(PreFlightResult(
            ticker=ticker, name_cn=name_cn,
            entry_price=entry_price, open_price=open_price,
            gap_pct=round(gap_pct, 2), prev_volume=prev_vol,
            open_volume_15m=open_vol_15m, volume_ratio=round(vol_ratio, 2),
            action=final_action, reasons=all_reasons,
        ))

    return results


def _shanghai_now() -> datetime:
    """Current time in Asia/Shanghai. Module-level so tests can monkeypatch."""
    from zoneinfo import ZoneInfo
    return datetime.now(tz=ZoneInfo("Asia/Shanghai"))


def fetch_open_prices(tickers: list[str]) -> dict[str, float]:
    """Fetch real-time opening prices from AkShare (best effort).

    Before 09:25 Asia/Shanghai (auction close), return {} without calling
    AkShare. The `今开` field can still hold the prior session's open
    pre-market, which would produce false gap signals.
    """
    prices: dict[str, float] = {}
    code_map = {
        ticker.split(".")[0]: ticker
        for ticker in tickers
        if ticker and "." in ticker
    }
    if not code_map:
        return prices

    now_sh = _shanghai_now()
    gate = now_sh.replace(hour=9, minute=25, second=0, microsecond=0)
    if now_sh < gate:
        logger.info(
            "Skipping live open fetch: %s Shanghai is before 09:25 auction close.",
            now_sh.strftime("%H:%M:%S"),
        )
        return prices

    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return prices

        snapshot = df[df["代码"].isin(code_map)]
        for _, row in snapshot.iterrows():
            code = str(row.get("代码", "") or "")
            try:
                open_price = float(row.get("今开", 0) or 0)
            except (TypeError, ValueError):
                continue

            # AkShare commonly reports 0 before the live auction/open.
            if open_price > 0 and code in code_map:
                prices[code_map[code]] = open_price
    except ImportError:
        logger.warning("AkShare not installed — cannot fetch live prices")
    except Exception as exc:
        logger.warning(f"Failed to fetch live opening prices: {exc}")
    return prices


def _read_marker(marker: Path) -> dict | None:
    """Parse a dedup marker into a dict, or None if missing/unreadable.

    Returns a dict like ``{"sent": ..., "signature": ..., "kind": ...,
    "date": ..., "legacy": bool}``. Legacy text markers (`sent=YYYY-MM-DD`)
    parse to ``{"sent": "<date>", "legacy": True}``.
    """
    if not marker.exists():
        return None
    try:
        content = marker.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if not content:
        return None
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "sent" in data:
            return data
    except json.JSONDecodeError:
        pass
    if content.startswith("sent="):
        return {"sent": content.split("=", 1)[1].strip(), "legacy": True}
    return None


def _marker_is_for_today(marker: Path, today_str: str) -> bool:
    """Check if a (pending) dedup marker was written for today's session.

    Supports both the legacy `sent=YYYY-MM-DD` text and the new JSON form.
    """
    data = _read_marker(marker)
    if data is None:
        return False
    return data.get("sent") == today_str


def _compute_verdict_signature(results: list[PreFlightResult]) -> str:
    """Stable hash over (ticker, action, open_price, gap_pct) for each pick.

    Used to detect when a later morning-check run produces a different
    execution verdict (e.g., real opens replacing stale pre-market values).
    """
    import hashlib

    items = sorted(
        [
            r.ticker,
            r.action,
            round(float(r.open_price or 0), 4),
            round(float(r.gap_pct or 0), 2),
        ]
        for r in results
    )
    payload = json.dumps(items, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _should_send_final(
    marker: Path,
    today_str: str,
    signature: str,
) -> tuple[bool, str]:
    """Decide whether to (re)send the final morning alert.

    Returns (should_send, reason). The marker is verdict-aware: same signature
    on the same calendar day → skip; different signature → send a corrected
    alert. Legacy text markers cannot be compared and are treated as already
    sent for today.
    """
    data = _read_marker(marker)
    if data is None:
        return True, "no prior marker"
    if data.get("sent") != today_str:
        return True, f"marker from {data.get('sent')!r} is stale"
    if data.get("legacy"):
        return False, "legacy marker present (no signature to compare)"
    prior = data.get("signature")
    if prior == signature:
        return False, "verdict unchanged"
    return True, f"verdict changed (prior {prior!r})"


def _write_final_marker(
    marker: Path,
    *,
    today_str: str,
    date_str: str,
    kind: str,
    signature: str,
) -> None:
    payload = {
        "sent": today_str,
        "date": date_str,
        "kind": kind,
        "signature": signature,
    }
    marker.write_text(
        json.dumps(payload, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def send_open_pending_alert(
    *,
    today_str: str,
    date_str: str,
    watchlist_path: Path,
    picks: list[dict],
    pending_marker: Path,
) -> bool:
    """Send a watchlist-only alert when opening prices are not live yet."""
    if date_str != today_str:
        logger.info(
            "Latest watchlist is from %s, not %s. Skipping stale waiting-for-open alert.",
            date_str,
            today_str,
        )
        return False

    if _marker_is_for_today(pending_marker, today_str):
        logger.info(f"Waiting-for-open alert already sent for {today_str} (marker: {pending_marker}). Skipping.")
        return True

    try:
        from src.core.alerts import AlertConfig, AlertManager, _ticker_display, _regime_emoji, _regime_cn, _translate_reason_summary

        alert_config = AlertConfig(enabled=True, channels=["telegram"])
        if not alert_config.telegram_bot_token or not alert_config.telegram_chat_id:
            logger.info("Telegram not configured — skipping waiting-for-open alert.")
            return False

        wl_data = {}
        if watchlist_path.exists():
            wl_data = json.loads(watchlist_path.read_text(encoding="utf-8"))

        wl_picks = wl_data.get("picks") or picks
        regime = wl_data.get("regime", "unknown")
        regime_label = _regime_cn(regime)
        universe_size = wl_data.get("universe_size", 0)
        emoji = _regime_emoji(regime)
        scan_label = f" (扫描: {date_str})" if date_str != today_str else ""

        from src.core.io import read_reconciliation
        from src.core.message_format import (RULE, append_footer, meta_line, pick_block, 
                                             reconciliation_line, title_line)

        alert_title = f"\U0001f409 龙脉扫描 — {today_str} 开盘检查{scan_label}"
        lines = [
            title_line("", alert_title),
            meta_line(
                f"{emoji} <b>{regime_label}</b>",
                f"选股 <b>{len(wl_picks)}</b>",
                f"股池 {universe_size}",
            ),
            reconciliation_line(read_reconciliation(date_str)),
            RULE,
        ]

        for i, pick in enumerate(wl_picks, 1):
            reason = _translate_reason_summary(pick.get("reason_summary", ""))
            lines += pick_block(
                index=i,
                display=_ticker_display(
                    pick.get("ticker", "?"),
                    pick.get("name_cn", pick.get("name", "")),
                ),
                entry=pick.get("entry_price"),
                stop=pick.get("stop_loss"),
                target=pick.get("target_1"),
                max_entry=pick.get("max_entry_price"),
                holding=pick.get("holding_period"),
                score=pick.get("score"),
                icon="\u23f3",
                tag="待定",
                sub=reason,
            )
            lines.append("")

        append_footer(
            lines,
            ["<i>\u26a0\ufe0f 开盘价尚未公布 — 跳空检查将在 09:25(上海) 后执行</i>"],
        )

        mgr = AlertManager(alert_config)
        mgr.send_alert(
            title=alert_title,
            message="\n".join(lines),
            data={"asof": date_str},
            priority="low",
        )
        pending_marker.write_text(f"sent={today_str}\n", encoding="utf-8")
        logger.info("Waiting-for-open alert sent to Telegram")
        return True
    except Exception as exc:
        logger.warning(f"Failed to send waiting-for-open alert: {exc}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Pre-market gap validator for Dragon Pulse picks")
    parser.add_argument("--date", help="Date for picks (YYYY-MM-DD), defaults to today")
    parser.add_argument("--picks-file", help="Path to picks JSON file")
    parser.add_argument("--max-gap-up", type=float, default=3.0, help="Max gap-up %% to proceed")
    parser.add_argument("--max-gap-down", type=float, default=5.0, help="Max gap-down %% before cancel")
    parser.add_argument("--dry-run", action="store_true", help="Show picks without fetching live data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Calendar date for the message header (the day the trader reads it).
    # Always use Shanghai timezone regardless of process TZ setting.
    from zoneinfo import ZoneInfo
    today_str = datetime.now(tz=ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")

    # Resolve date and output directory
    # When no --date given, find the most recent watchlist (nightly runs
    # the evening before, so "today" at 09:26 has no watchlist yet).
    date_str = args.date
    if not date_str and not args.picks_file:
        import glob
        # Sort by date in filename, not mtime (mtime unreliable after git checkout)
        candidates = sorted(glob.glob("outputs/*/execution_watchlist_*.json"), reverse=True)
        if candidates:
            # Extract date from filename
            fname = Path(candidates[0]).stem  # execution_watchlist_YYYY-MM-DD
            date_str = fname.replace("execution_watchlist_", "")
            logger.info(f"Auto-detected latest watchlist date: {date_str}")
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = Path("outputs") / date_str

    # Priority: load from execution watchlist artifact (new scan schema)
    watchlist_path = output_dir / f"execution_watchlist_{date_str}.json"

    # Shared dedup marker — prevents duplicate Telegram sends from CI + local fallback
    morning_marker = output_dir / ".morning_alert_sent"
    pending_marker = output_dir / ".morning_open_pending_sent"

    picks = None
    if not args.picks_file and watchlist_path.exists():
        wl_data = json.loads(watchlist_path.read_text(encoding="utf-8"))
        picks = wl_data.get("picks", [])
        # Map new watchlist fields to morning check expected fields
        for p in picks:
            p.setdefault("name_cn", p.get("name_cn", p.get("name", p.get("ticker", ""))))
        logger.info(f"Loaded {len(picks)} picks from execution watchlist: {watchlist_path}")

    # Fallback: load picks from explicit file or glob
    if picks is None:
        if args.picks_file:
            picks_path = Path(args.picks_file)
        else:
            picks_path = Path("data/dragon_pulse.db")
            # Try loading from yesterday's output
            candidates = list(output_dir.glob("*top5*.json")) + list(output_dir.glob("*picks*.json"))
            if candidates:
                picks_path = candidates[0]
            else:
                logger.error(f"No picks found for {date_str}")
                return 1

        if not picks_path.exists():
            logger.error(f"File not found: {picks_path}")
            return 1

        data = json.loads(picks_path.read_text(encoding="utf-8"))
        picks = data.get("top5", data.get("picks", []))

    if not picks:
        logger.info("No picks to validate.")
        # Still send a compact Telegram message for zero-picks days
        no_picks_signature = "no_picks"
        should_send, reason = _should_send_final(morning_marker, today_str, no_picks_signature)
        if not should_send:
            logger.info(f"Morning alert already sent for {today_str} ({reason}). Skipping.")
            return 0

        # Check scan health to distinguish "quiet market" from "broken scan"
        scan_results_path = output_dir / f"scan_results_{date_str}.json"
        scan_health = None
        if scan_results_path.exists():
            scan_data = json.loads(scan_results_path.read_text(encoding="utf-8"))
            dl_health = scan_data.get("download_health", "ok")
            circuit_breaker = scan_data.get("circuit_breaker")
            downloaded = scan_data.get("downloaded", 0)
            universe = scan_data.get("universe_size", 0)
            signals = scan_data.get("signals_total", 0)
            if dl_health != "ok" or circuit_breaker:
                scan_health = "degraded"
            else:
                scan_health = "healthy"

        try:
            from src.core.alerts import AlertConfig, AlertManager, _regime_emoji
            alert_config = AlertConfig(enabled=True, channels=["telegram"])
            if alert_config.telegram_bot_token and alert_config.telegram_chat_id:
                wl_data = {}
                if watchlist_path.exists():
                    wl_data = json.loads(watchlist_path.read_text(encoding="utf-8"))
                regime = wl_data.get("regime", "unknown")
                emoji = _regime_emoji(regime)
                scan_label = f" (scan: {date_str})" if date_str != today_str else ""
                from src.core.alerts import _regime_cn
                regime_label = _regime_cn(regime)
                from src.core.io import read_reconciliation
                from src.core.message_format import (RULE, append_footer, meta_line, 
                                                     reconciliation_line, title_line)

                alert_title = f"\U0001f409 龙脉扫描 — {today_str} 开盘检查{scan_label}"
                lines = [
                    title_line("", alert_title),
                    meta_line(f"{emoji} <b>{regime_label}</b>", "选股 <b>0</b>"),
                    reconciliation_line(read_reconciliation(date_str)),
                    RULE,
                ]

                if scan_health == "degraded":
                    lines.append("\u26a0\ufe0f <b>数据异常 — 扫描不完整，请检查数据源</b>")
                    lines.append(RULE)
                    detail = [f"<i>已下载 {downloaded}/{universe} · 状态 {dl_health}</i>"]
                    if circuit_breaker:
                        detail.append(f"<i>熔断 {circuit_breaker}</i>")
                    lines.extend(detail)
                    priority = "high"
                else:
                    regime_detail = {}
                    if scan_results_path.exists():
                        regime_detail = scan_data.get("regime_detail", {})
                    acceptance_mode = regime_detail.get("acceptance_mode", "")
                    breadth = regime_detail.get("market_breadth_pct_above_sma20")

                    if acceptance_mode == "breadth_suppressed" and breadth is not None:
                        lines.append("今日无选股 — 市场宽度受限")
                        lines.append(RULE)
                        lines.append(f"<i>宽度 {breadth:.1%} · {signals} 个信号已被过滤</i>")
                    else:
                        lines.append("今日无选股 — 无信号通过筛选")
                    priority = "low"

                mgr = AlertManager(alert_config)
                mgr.send_alert(
                    title=alert_title,
                    message="\n".join(lines),
                    data={"asof": date_str},
                    priority=priority,
                )
                _write_final_marker(
                    morning_marker,
                    today_str=today_str,
                    date_str=date_str,
                    kind="no_picks",
                    signature=no_picks_signature,
                )
                logger.info("No-picks morning alert sent to Telegram (health=%s)", scan_health)
        except Exception as e:
            logger.warning(f"Failed to send no-picks alert: {e}")
        return 0

    logger.info(f"Validating {len(picks)} picks...")

    if args.dry_run:
        for p in picks:
            ticker = p.get("ticker", "?")
            entry = p.get("entry_price", "?")
            logger.info(f"  {ticker}: entry={entry}")
        logger.info("\n(dry run — no live data fetched)")
        return 0

    # Fetch live data
    tickers = [p.get("ticker", "") for p in picks]
    open_prices = fetch_open_prices(tickers)

    if not open_prices:
        logger.warning(
            "Could not fetch opening prices yet. Waiting for live auction/open data "
            "(9:25-9:35 AM Shanghai)."
        )
        send_open_pending_alert(
            today_str=today_str,
            date_str=date_str,
            watchlist_path=watchlist_path,
            picks=picks,
            pending_marker=pending_marker,
        )
        return 0

    # Run checks
    results = run_preflight(
        picks=picks,
        open_prices=open_prices,
        prev_volumes={},  # Would need yesterday's volume
        config={"max_gap_up_pct": args.max_gap_up, "max_gap_down_pct": args.max_gap_down},
    )

    # Display results
    logger.info("\n" + "=" * 60)
    logger.info("  PRE-FLIGHT CHECK RESULTS")
    logger.info("=" * 60)

    for r in results:
        icon = {"GO": "✅", "WARN": "⚠️", "CANCEL": "❌"}.get(r.action, "?")
        logger.info(f"\n{icon} {r.ticker} {r.name_cn}")
        logger.info(f"   Entry: ¥{r.entry_price:.2f} → Open: ¥{r.open_price:.2f} (gap: {r.gap_pct:+.1f}%)")
        logger.info(f"   Action: {r.action}")
        for reason in r.reasons:
            logger.info(f"   → {reason}")

    go_count = sum(1 for r in results if r.action == "GO")
    warn_count = sum(1 for r in results if r.action == "WARN")
    cancel_count = sum(1 for r in results if r.action == "CANCEL")
    logger.info(f"\nSummary: {go_count} GO, {warn_count} WARN, {cancel_count} CANCEL")

    # Save machine-readable results
    check_output = {
        "date": date_str,
        "generated_utc": datetime.utcnow().isoformat(),
        "results": [],
    }
    for r in results:
        check_output["results"].append({
            "ticker": r.ticker,
            "name": r.name_cn,
            "action": r.action,
            "entry_price": r.entry_price,
            "open_price": r.open_price,
            "gap_pct": r.gap_pct,
            "reasons": r.reasons,
        })

    check_file = output_dir / f"execution_check_{date_str}.json"
    check_file.parent.mkdir(parents=True, exist_ok=True)
    check_file.write_text(json.dumps(check_output, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Saved execution check to {check_file}")

    # Send combined Telegram alert (watchlist details + execution verdicts).
    # Verdict-aware dedup: if a later run produces a different (ticker, action,
    # open, gap) signature than the marker, re-send a corrected alert.
    verdict_signature = _compute_verdict_signature(results)
    should_send, reason = _should_send_final(morning_marker, today_str, verdict_signature)
    if not should_send:
        logger.info(f"Morning alert already sent for {today_str} ({reason}). Skipping Telegram.")
        return 0
    try:
        from src.core.alerts import AlertConfig, AlertManager, _ticker_display, _regime_emoji, _regime_cn, _translate_reason_summary

        alert_config = AlertConfig(enabled=True, channels=["telegram"])
        if alert_config.telegram_bot_token and alert_config.telegram_chat_id:
            # Load watchlist for full pick details
            wl_data = {}
            if watchlist_path.exists():
                wl_data = json.loads(watchlist_path.read_text(encoding="utf-8"))
            regime = wl_data.get("regime", "unknown")
            regime_label = _regime_cn(regime)
            universe_size = wl_data.get("universe_size", 0)
            wl_picks = wl_data.get("picks", [])
            pick_map = {p.get("ticker"): p for p in wl_picks}

            emoji = _regime_emoji(regime)
            scan_label = f" (扫描: {date_str})" if date_str != today_str else ""
            from src.core.io import read_reconciliation
            from src.core.message_format import (RULE, append_footer, meta_line, pick_block, 
                                                 reconciliation_line, title_line)

            alert_title = f"\U0001f409 龙脉扫描 — {today_str} 开盘检查{scan_label}"
            ACTION_CN = {"GO": "执行", "WARN": "注意", "CANCEL": "取消"}
            ICON = {"GO": "\u2705", "WARN": "\u26a0\ufe0f", "CANCEL": "\u274c"}
            go_picks = [r for r in results if r.action == "GO"]
            warn_picks = [r for r in results if r.action == "WARN"]
            cancel_picks = [r for r in results if r.action == "CANCEL"]

            lines = [
                title_line("", alert_title),
                meta_line(
                    f"{emoji} <b>{regime_label}</b>",
                    f"执行 <b>{len(go_picks)}</b>",
                    f"注意 {len(warn_picks)}",
                    f"取消 {len(cancel_picks)}",
                ),
                reconciliation_line(read_reconciliation(date_str)),
                RULE,
            ]

            for i, r in enumerate(results, 1):
                wp = pick_map.get(r.ticker, {})
                extras = []
                if r.open_price:
                    extras.append(f"开盘 \u00a5{r.open_price:.2f}（跳空 {r.gap_pct:+.1f}%）")
                reason = (
                    _translate_reason_summary(wp["reason_summary"])
                    if wp.get("reason_summary") and r.action == "GO"
                    else ""
                )
                lines += pick_block(
                    index=i,
                    display=_ticker_display(r.ticker, r.name_cn),
                    entry=r.entry_price,
                    stop=wp.get("stop_loss"),
                    target=wp.get("target_1"),
                    max_entry=wp.get("max_entry_price"),
                    holding=wp.get("holding_period"),
                    score=wp.get("score"),
                    icon=ICON.get(r.action, "\u2022"),
                    tag=ACTION_CN.get(r.action, r.action),
                    extras=extras,
                    notes=list(r.reasons),
                    sub=reason,
                )
                lines.append("")

            append_footer(
                lines,
                [f"<i>股池 {universe_size} · 昨夜选股 {len(wl_picks)}</i>"],
            )

            mgr = AlertManager(alert_config)
            mgr.send_alert(
                title=alert_title,
                message="\n".join(lines),
                data={"asof": date_str},
                priority="high" if go_picks else "low",
            )
            _write_final_marker(
                morning_marker,
                today_str=today_str,
                date_str=date_str,
                kind="final",
                signature=verdict_signature,
            )
            pending_marker.unlink(missing_ok=True)
            logger.info("Combined morning alert sent to Telegram (sig=%s)", verdict_signature[:8])
    except Exception as e:
        logger.warning(f"Failed to send morning alert: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
