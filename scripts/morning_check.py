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
            f"Gap up +{gap_pct:.1f}% exceeds {max_gap_up_pct}% limit — "
            f"entry invalidated (chasing risk)"
        )
        return "CANCEL", reasons

    # Per-pick stop check: if open is below the stop_loss, cancel immediately
    if stop_loss is not None and open_price < stop_loss:
        reasons.append(
            f"Open ¥{open_price:.2f} < stop ¥{stop_loss:.2f} — "
            f"stop already breached at open"
        )
        return "CANCEL", reasons

    if gap_pct < -max_gap_down_pct:
        reasons.append(
            f"Gap down {gap_pct:.1f}% exceeds {max_gap_down_pct}% limit — "
            f"stop already breached"
        )
        return "CANCEL", reasons

    if gap_pct > max_gap_up_pct * 0.6:
        reasons.append(
            f"Gap up +{gap_pct:.1f}% is elevated — consider reduced size"
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
            f"Low opening volume: {first_15m_volume:,} = {ratio:.1%} of prev day "
            f"(need >{min_volume_ratio:.0%}) — weak conviction"
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
                action="WARN", reasons=["No opening price available"],
            ))
            continue

        gap_pct = (open_price / entry_price - 1) * 100

        # Check gap (with per-pick stop_loss if available)
        pick_stop_loss = pick.get("stop_loss")
        if pick_stop_loss is not None:
            pick_stop_loss = float(pick_stop_loss)
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


def fetch_open_prices(tickers: list[str]) -> dict[str, float]:
    """Fetch real-time opening prices from AkShare (best effort)."""
    prices = {}
    try:
        import akshare as ak
        for ticker in tickers:
            try:
                code = ticker.split(".")[0]
                df = ak.stock_zh_a_spot_em()
                if df is not None and not df.empty:
                    row = df[df["代码"] == code]
                    if not row.empty:
                        prices[ticker] = float(row.iloc[0].get("今开", 0) or 0)
            except Exception:
                continue
    except ImportError:
        logger.warning("AkShare not installed — cannot fetch live prices")
    return prices


def main():
    parser = argparse.ArgumentParser(description="Pre-market gap validator for Dragon Pulse picks")
    parser.add_argument("--date", help="Date for picks (YYYY-MM-DD), defaults to today")
    parser.add_argument("--picks-file", help="Path to picks JSON file")
    parser.add_argument("--max-gap-up", type=float, default=3.0, help="Max gap-up %% to proceed")
    parser.add_argument("--max-gap-down", type=float, default=5.0, help="Max gap-down %% before cancel")
    parser.add_argument("--dry-run", action="store_true", help="Show picks without fetching live data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    # Resolve date and output directory
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    output_dir = Path("outputs") / date_str

    # Priority: load from execution watchlist artifact (new scan schema)
    watchlist_path = output_dir / f"execution_watchlist_{date_str}.json"
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
        logger.warning("Could not fetch opening prices. Run this during market hours (9:25-9:35 AM).")
        return 1

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

    # Send Telegram execution alert
    try:
        from src.core.alerts import AlertConfig, AlertManager, _ticker_display, _section_line

        alert_config = AlertConfig(enabled=True, channels=["telegram"])
        if alert_config.telegram_bot_token and alert_config.telegram_chat_id:
            lines = [f"<b>\U0001f3af Dragon Pulse — Execution Check {date_str}</b>", ""]

            go_picks = [r for r in results if r.action == "GO"]
            cancel_picks = [r for r in results if r.action == "CANCEL"]
            warn_picks = [r for r in results if r.action == "WARN"]

            if go_picks:
                lines.append(f"\u2705 <b>GO ({len(go_picks)})</b>")
                for r in go_picks:
                    display = _ticker_display(r.ticker, r.name_cn)
                    lines.append(f"  {display}")
                    lines.append(f"  Entry \u00a5{r.entry_price:.2f} \u2192 Open \u00a5{r.open_price:.2f} ({r.gap_pct:+.1f}%)")
                lines.append("")

            if warn_picks:
                lines.append(f"\u26a0\ufe0f <b>WARN ({len(warn_picks)})</b>")
                for r in warn_picks:
                    display = _ticker_display(r.ticker, r.name_cn)
                    reason = r.reasons[0] if r.reasons else "elevated risk"
                    lines.append(f"  {display} — {reason}")
                lines.append("")

            if cancel_picks:
                lines.append(f"\u274c <b>CANCEL ({len(cancel_picks)})</b>")
                for r in cancel_picks:
                    display = _ticker_display(r.ticker, r.name_cn)
                    reason = r.reasons[0] if r.reasons else "threshold breached"
                    lines.append(f"  {display} — {reason}")
                lines.append("")

            lines.append(f"\U0001f4ca GO: {len(go_picks)} | WARN: {len(warn_picks)} | CANCEL: {len(cancel_picks)}")

            mgr = AlertManager(alert_config)
            mgr.send_alert(
                title=f"Execution Check: {date_str}",
                message="\n".join(lines),
                data={"asof": date_str},
                priority="high" if go_picks else "low",
            )
            logger.info("Execution alert sent to Telegram")
    except Exception as e:
        logger.warning(f"Failed to send execution alert: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
