#!/usr/bin/env python3
"""
Track 2 Validation: Scanner Policy Layer
==========================================

Runs the full scanner pipeline on a set of historical dates and records
what each filter stage removes. This validates breadth gating, quality-gated
book sizing, sector concentration limits, and limit-down veto — none of
which are exercised by the backtest script.

Usage:
    python scripts/validate_scanner_policy.py --dates 2026-03-10,2026-03-11
    python scripts/validate_scanner_policy.py --sample 30
    python scripts/validate_scanner_policy.py --sample 30 --out-dir outputs/track2
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

load_dotenv(project_root / ".env")

from src.core.config import load_config, get_config_value
from src.core.data import get_data_functions
from src.core.universe import get_top_n_cn_by_market_cap
from src.features.technical import (
    compute_all_technical_features,
    compute_rsi2_features,
    latest_features,
)
from src.core.acceptance import run_acceptance
from src.pipelines.scanner import _classify_regime, _sort_signal_candidates, _compute_adv_cny
from src.signals.mean_reversion import score_mean_reversion
from src.signals.sniper import score_sniper

logger = logging.getLogger(__name__)


def get_cn_trading_days(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _slice_up_to(df: pd.DataFrame, scan_date: date) -> pd.DataFrame:
    if df.empty:
        return df
    idx = df.index
    if hasattr(idx, 'date'):
        mask = idx.date <= scan_date
    else:
        mask = pd.to_datetime(idx).date <= scan_date
    return df.loc[mask]


def main():
    parser = argparse.ArgumentParser(description="Scanner policy validation (Track 2)")
    parser.add_argument("--dates", default="", help="Comma-separated dates (YYYY-MM-DD)")
    parser.add_argument("--sample", type=int, default=0,
                        help="Sample N trading days from last year (spread across regimes)")
    parser.add_argument("--config", default="config/default.yaml")
    parser.add_argument("--out-dir", default="outputs/track2")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")

    config = load_config(args.config)

    # Resolve dates
    if args.dates:
        scan_dates = [datetime.strptime(d.strip(), "%Y-%m-%d").date() for d in args.dates.split(",")]
    elif args.sample > 0:
        end = date.today()
        start = end - timedelta(days=365)
        all_days = get_cn_trading_days(start, end)
        # Spread evenly
        step = max(1, len(all_days) // args.sample)
        scan_dates = all_days[::step][:args.sample]
    else:
        logger.error("Specify --dates or --sample")
        return 1

    logger.info("Validating scanner policy on %d dates", len(scan_dates))

    # --- Setup ---
    _, download_daily_range_fn, provider_config, _ = get_data_functions(config)

    logger.info("Building universe...")
    universe = get_top_n_cn_by_market_cap(n=1000, provider_config=provider_config)
    logger.info("Universe: %d tickers", len(universe))

    # Download range
    dl_start = min(scan_dates) - timedelta(days=400)
    dl_end = max(scan_dates) + timedelta(days=10)
    dl_start_str = dl_start.strftime("%Y-%m-%d")
    dl_end_str = dl_end.strftime("%Y-%m-%d")

    csi_cfg = config.get("mean_reversion", {}).get("regime", {}) or {}
    csi_symbol = csi_cfg.get("csi300_symbol", "000300.SH")
    logger.info("Downloading CSI 300...")
    csi300_data, _ = download_daily_range_fn(
        tickers=[csi_symbol], start=dl_start_str, end=dl_end_str,
        provider_config=provider_config,
    )
    csi300_full = csi300_data.get(csi_symbol, pd.DataFrame())
    if not csi300_full.empty:
        csi300_full = csi300_full.rename(
            columns={c: c.lower() for c in csi300_full.columns if c in ("Open", "High", "Low", "Close", "Volume")}
        )

    logger.info("Bulk downloading OHLCV for %d tickers...", len(universe))
    t0 = time.time()
    data_map, report = download_daily_range_fn(
        tickers=universe, start=dl_start_str, end=dl_end_str,
        provider_config=provider_config,
    )
    logger.info("Download: %d OK (%.1f min)", len(data_map), (time.time() - t0) / 60)

    # Fetch basic info for ST/sector
    from src.core.cn_data import get_cn_basic_info
    info_map = get_cn_basic_info(universe, provider_config=provider_config)

    # --- Config ---
    mr_config = config.get("mean_reversion", {})
    sniper_config = config.get("sniper", {})
    sma_short = int(get_config_value(config, "mean_reversion", "regime", "sma_short", default=20))
    sma_long = int(get_config_value(config, "mean_reversion", "regime", "sma_long", default=50))

    from src.core.cn_limits import get_daily_limit

    # --- Run each date ---
    results = []

    for scan_date in scan_dates:
        date_str = scan_date.strftime("%Y-%m-%d")
        csi_slice = _slice_up_to(csi300_full, scan_date)
        regime = _classify_regime(csi_slice, sma_short, sma_long)

        # Score all tickers
        all_signals = []
        for ticker, full_df in data_map.items():
            try:
                hist_df = _slice_up_to(full_df, scan_date)
                if len(hist_df) < 60:
                    continue
                feat_df = compute_all_technical_features(hist_df)
                feat_df = compute_rsi2_features(feat_df)
                feats = latest_features(feat_df)
                if not feats:
                    continue

                mr_signal = score_mean_reversion(
                    ticker=ticker, df=feat_df, features=feats, regime=regime,
                    rsi2_max=float(mr_config.get("rsi2_max", 5)),
                    adv_min_cny=float(mr_config.get("adv_min_cny", 100_000_000)),
                    score_floor=float(mr_config.get("score_floor", 65)),
                    min_bars=int(mr_config.get("min_bars", 60)),
                    max_single_day_move=float(mr_config.get("max_single_day_move", 0.11)),
                    stop_atr_mult=float(mr_config.get("stop_atr_mult", 0.75)),
                    target_1_atr_mult=float(mr_config.get("target_1_atr_mult", 1.5)),
                    target_2_atr_mult=float(mr_config.get("target_2_atr_mult", 2.0)),
                    max_entry_atr_mult=float(mr_config.get("max_entry_atr_mult", 0.2)),
                    holding_period=int(mr_config.get("holding_period", 3)),
                )
                if mr_signal:
                    all_signals.append(("mean_reversion", mr_signal))

                sniper_signal = score_sniper(
                    ticker=ticker, df=feat_df, features=feats, regime=regime,
                    csi300_df=csi_slice,
                    atr_pct_floor=float(sniper_config.get("atr_pct_floor", 3.5)),
                    min_avg_volume=int(sniper_config.get("min_avg_volume", 500_000)),
                    stop_atr_mult=float(sniper_config.get("stop_atr_mult", 2.0)),
                    target_atr_mult=float(sniper_config.get("target_atr_mult", 3.0)),
                    target_2_atr_mult=float(sniper_config.get("target_2_atr_mult", 5.0)),
                    holding_period=int(sniper_config.get("holding_period", 7)),
                )
                if sniper_signal:
                    all_signals.append(("sniper", sniper_signal))
            except Exception as e:
                logger.debug("Skipped %s: %s", ticker, e)
                continue

        # Dedupe
        best: dict[str, tuple[str, object]] = {}
        for engine, sig in all_signals:
            existing = best.get(sig.ticker)
            if existing is None or sig.score > existing[1].score:
                best[sig.ticker] = (engine, sig)

        sorted_picks = _sort_signal_candidates(list(best.values()), data_map, info_map)
        raw_count = len(sorted_picks)
        raw_tickers = [(e, s.ticker, s.score) for e, s in sorted_picks[:20]]

        # --- Stage 1: Breadth gate ---
        above_count = 0
        for bdf in data_map.values():
            sliced = _slice_up_to(bdf, scan_date)
            if len(sliced) < 20:
                continue
            close_col = "close" if "close" in sliced.columns else "Close"
            if float(sliced[close_col].iloc[-1]) > float(sliced[close_col].tail(20).mean()):
                above_count += 1
        above_sma20 = above_count / max(len(data_map), 1)
        breadth_floor = float(config.get("book_size", {}).get("breadth_floor", 0.30))
        breadth_suppressed = above_sma20 < breadth_floor
        after_breadth = [] if breadth_suppressed else sorted_picks

        # --- Stage 2: Score floor ---
        book_cfg = config.get("book_size", {}).get(regime, {})
        max_picks = int(book_cfg.get("max_picks", 5)) if isinstance(book_cfg, dict) else 5
        min_score = float(book_cfg.get("min_score", 0)) if isinstance(book_cfg, dict) else 0
        score_rejected = [
            {"ticker": s.ticker, "score": round(s.score, 1), "reason": f"score {s.score:.1f} < floor {min_score}"}
            for _, s in after_breadth if s.score < min_score
        ]
        after_score = [p for p in after_breadth if p[1].score >= min_score]
        score_removed = len(score_rejected)

        # --- Stage 3: Limit-down veto ---
        after_limit = []
        limit_rejected = []
        for engine, sig in after_score:
            raw_df = data_map.get(sig.ticker, pd.DataFrame())
            if not raw_df.empty and len(raw_df) > 1:
                close_col = "close" if "close" in raw_df.columns else "Close"
                c = float(raw_df[close_col].iloc[-1])
                pc = float(raw_df[close_col].iloc[-2])
                pct_chg = (c / pc - 1)
                limit_pct = get_daily_limit(sig.ticker,
                                            is_st=info_map.get(sig.ticker, {}).get("is_st", False))
                if pct_chg <= (-limit_pct + 0.001):
                    limit_rejected.append({
                        "ticker": sig.ticker, "pct_chg": round(pct_chg * 100, 2),
                        "reason": f"closed at limit-down ({pct_chg*100:.1f}%)"
                    })
                    continue
            after_limit.append((engine, sig))

        # --- Stage 4: Sector cap ---
        max_per_sector = int(config.get("book_size", {}).get("max_per_sector", 2))
        sector_counts: dict[str, int] = {}
        after_sector: list[tuple[str, object]] = []
        sector_rejected = []
        for engine, sig in after_limit:
            industry = (info_map.get(sig.ticker, {}) or {}).get("industry", "unknown") or "unknown"
            if sector_counts.get(industry, 0) < max_per_sector:
                after_sector.append((engine, sig))
                sector_counts[industry] = sector_counts.get(industry, 0) + 1
            else:
                sector_rejected.append({
                    "ticker": sig.ticker, "industry": industry,
                    "reason": f"sector cap ({max_per_sector}) exceeded for {industry}"
                })

        # --- Stage 5: Acceptance allocator ---
        acceptance_cfg = config.get("acceptance", {})
        acceptance_enabled = acceptance_cfg.get("enabled", True)

        if acceptance_enabled:
            acceptance_result = run_acceptance(
                candidates=after_sector,
                breadth_pct=above_sma20,
                regime=regime,
                universe_size=len(data_map),
                config=acceptance_cfg,
                info_map=info_map,
            )
            final = [(e, s) for e, s, _tier in acceptance_result.accepted]
            acceptance_rejected = [
                {"ticker": s.ticker, "score": round(s.score, 1), "tier": tier, "reason": reason}
                for e, s, tier, reason in acceptance_result.rejected
            ]
            acceptance_info = {
                "mode": acceptance_result.mode,
                "eligible_count": acceptance_result.eligible_count,
                "day_quality_score": acceptance_result.day_quality.score,
                "day_quality_components": acceptance_result.day_quality.components,
                "accepted": [{"ticker": s.ticker, "score": round(s.score, 1), "tier": tier}
                             for _, s, tier in acceptance_result.accepted],
                "rejected": acceptance_rejected,
            }
        else:
            final = after_sector[:max_picks]
            acceptance_info = {"mode": "disabled"}

        # Picks removed by max_picks truncation (only when acceptance disabled)
        truncated = []
        if not acceptance_enabled:
            truncated = [
                {"ticker": s.ticker, "score": round(s.score, 1), "reason": f"below top-{max_picks} cutoff"}
                for _, s in after_sector[max_picks:]
            ]

        day_result = {
            "date": date_str,
            "regime": regime,
            "breadth_pct": round(above_sma20, 4),
            "breadth_floor": breadth_floor,
            "breadth_suppressed": breadth_suppressed,
            "raw_candidates": raw_count,
            "raw_top20": raw_tickers,
            "filter_chain": {
                "after_breadth": len(after_breadth),
                "score_floor": min_score,
                "score_rejected": score_rejected,
                "after_score": len(after_score),
                "limit_down_rejected": limit_rejected,
                "after_limit": len(after_limit),
                "sector_rejected": sector_rejected,
                "after_sector": len(after_sector),
                "max_picks": max_picks,
                "truncated": truncated,
                "acceptance": acceptance_info,
            },
            "final_count": len(final),
            "final_picks": [(e, s.ticker, round(s.score, 1)) for e, s in final],
        }
        results.append(day_result)

        logger.info("[%s] %s  breadth=%.1f%%  raw=%d -> score=%d -> limit=%d -> sector=%d -> accept=%d  final=%d",
                     date_str, regime, above_sma20 * 100,
                     raw_count, len(after_score), len(after_limit),
                     len(after_sector), len(final), len(final))

    # --- Summary ---
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "scanner_policy_validation.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Saved: %s", out_path)

    # Print aggregate stats
    regime_days = {}
    for r in results:
        reg = r["regime"]
        regime_days.setdefault(reg, []).append(r)

    logger.info("\n" + "=" * 60)
    logger.info("SCANNER POLICY VALIDATION SUMMARY")
    logger.info("=" * 60)
    for reg, days in sorted(regime_days.items()):
        avg_raw = sum(d["raw_candidates"] for d in days) / len(days)
        avg_final = sum(d["final_count"] for d in days) / len(days)
        zero_days = sum(1 for d in days if d["final_count"] == 0)
        breadth_kills = sum(1 for d in days if d["breadth_suppressed"])
        logger.info("  %s (%d days): avg_raw=%.1f avg_final=%.1f zero_days=%d breadth_kills=%d",
                     reg, len(days), avg_raw, avg_final, zero_days, breadth_kills)

    return 0


if __name__ == "__main__":
    sys.exit(main())
