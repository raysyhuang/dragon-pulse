#!/usr/bin/env python3
"""
Portfolio simulator v2 — fixes three issues Codex flagged in v1:

1. Uses explicit entry_date and exit_date columns from the detail CSV
   (instead of inferring from signal_date + exit_day, which broke when the
   experiment shifted the entry bar to T+2).
2. Sells at the row's exit_price (target / stop / hold-expired close as
   determined by the original trade evaluation), not the day's actual close.
3. Takes position_pct as an explicit input, distinct from the concurrent-
   position cap. Old code derived sizing from cap, so 40% and 50% both
   collapsed to cap=2 / 50% sizing.

Required columns in the detail CSV:
    ticker, entry_date, exit_date, entry_price, exit_price

Optional: score (used for selection priority), engine, regime, subtype.

Usage:
    python scripts/portfolio_sim_v2.py \\
        outputs/backtest/backtest_detail_<window>__confirmation.csv \\
        --position-pct 20 25 30 40 50 \\
        --max-concurrent 5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from src.core.config import load_config
from src.core.data import get_data_functions

logger = logging.getLogger(__name__)


def _download_price_data(
    tickers: list[str], start: date, end: date, config_path: str = "config/default.yaml"
) -> dict[str, pd.DataFrame]:
    config = load_config(config_path)
    _, download_range_fn, provider_config, _ = get_data_functions(config)
    start_str = (start - timedelta(days=10)).strftime("%Y-%m-%d")
    end_str = (end + timedelta(days=30)).strftime("%Y-%m-%d")
    logger.info("Downloading price data for %d tickers (%s to %s)...",
                len(tickers), start_str, end_str)
    t0 = time.time()
    data_map, report = download_range_fn(
        tickers=tickers, start=start_str, end=end_str,
        provider_config=provider_config,
    )
    logger.info("Download: %d OK, %d failed (%.1f min)",
                len(data_map), len(report.get("bad_tickers", [])),
                (time.time() - t0) / 60)
    return data_map


def _calendar_from_data(data_map: dict[str, pd.DataFrame]) -> list[date]:
    all_dates: set[date] = set()
    for df in data_map.values():
        if df.empty:
            continue
        idx = df.index
        d_idx = idx.date if hasattr(idx, "date") else pd.to_datetime(idx).date
        all_dates.update(d_idx)
    return sorted(all_dates)


def _ticker_close_map(data_map: dict, calendar: list[date]) -> dict:
    """Build {ticker: {date: close}} for fast mark-to-market."""
    close_map: dict[str, dict[date, float]] = {}
    for ticker, df in data_map.items():
        if df.empty:
            continue
        close_col = "Close" if "Close" in df.columns else "close"
        idx = df.index
        d_idx = idx.date if hasattr(idx, "date") else pd.to_datetime(idx).date
        close_map[ticker] = {
            d: float(c) for d, c in zip(d_idx, df[close_col].astype(float).values)
        }
    return close_map


def simulate(
    detail_csv: str,
    data_map: dict,
    calendar: list[date],
    position_pct: float,
    max_concurrent: int,
    initial_capital: float = 100_000.0,
) -> dict:
    """Mark-to-market portfolio sim with explicit dates and exit prices."""
    df = pd.read_csv(detail_csv)
    df = df[df["pnl_pct"].notna()].copy()
    if df.empty:
        return {"error": "no valid trades"}

    # Required columns
    required = {"ticker", "entry_price", "exit_price"}
    missing = required - set(df.columns)
    if missing:
        return {"error": f"missing required columns: {sorted(missing)}"}

    # Derive entry_date / exit_date from legacy schema if missing.
    # Legacy backtest_1yr.py rows have `date` (signal day T) and `exit_day` (1-indexed
    # offset into bars starting at T+1). Entry = T+1, exit = T + exit_day.
    cal_idx = {d: i for i, d in enumerate(calendar)}
    if "entry_date" not in df.columns or "exit_date" not in df.columns:
        if not {"date", "exit_day"}.issubset(df.columns):
            return {"error": "missing entry_date/exit_date AND missing legacy date/exit_day"}
        sig_dates = pd.to_datetime(df["date"]).dt.date
        entry_dates, exit_dates = [], []
        for sd, ed in zip(sig_dates, df["exit_day"].astype(int).values):
            pos = cal_idx.get(sd)
            if pos is None or pos + 1 >= len(calendar):
                entry_dates.append(None); exit_dates.append(None); continue
            entry_dates.append(calendar[pos + 1])
            exit_dates.append(calendar[min(pos + int(ed), len(calendar) - 1)])
        df["entry_date"] = entry_dates
        df["exit_date"] = exit_dates
    else:
        df["entry_date"] = pd.to_datetime(df["entry_date"]).dt.date
        df["exit_date"] = pd.to_datetime(df["exit_date"]).dt.date
    df = df.dropna(subset=["entry_date", "exit_date"]).copy()
    if "score" in df.columns:
        df["score"] = df["score"].astype(float)
    else:
        df["score"] = 0.0

    # Trim trades whose dates fall outside the available calendar
    cal_set = set(calendar)
    valid_mask = df["entry_date"].isin(cal_set) & df["exit_date"].isin(cal_set)
    df = df.loc[valid_mask].sort_values(["entry_date", "score"], ascending=[True, False])
    if df.empty:
        return {"error": "no trades with both entry and exit dates in calendar"}

    close_map = _ticker_close_map(data_map, calendar)

    # Bucket trades by entry_date for fast lookup
    by_entry: dict[date, list[pd.Series]] = {}
    for _, row in df.iterrows():
        by_entry.setdefault(row["entry_date"], []).append(row)

    cash = initial_capital
    open_positions: list[dict] = []  # {ticker, shares, entry_price, exit_date, exit_price, last_value}
    sim_start = df["entry_date"].min()
    sim_end = df["exit_date"].max()
    sim_days = [d for d in calendar if sim_start <= d <= sim_end]

    equity_curve = []
    skipped_capacity = 0
    skipped_cash = 0
    peak_equity = initial_capital
    max_dd = 0.0
    max_dd_date = None

    pct = position_pct / 100.0

    for day in sim_days:
        # 1) Close positions whose exit_date == today, at the row's exit_price.
        remaining: list[dict] = []
        for pos in open_positions:
            if pos["exit_date"] <= day:
                cash += pos["exit_price"] * pos["shares"]
            else:
                remaining.append(pos)
        open_positions = remaining

        # 2) Open new positions for today's entries, highest score first.
        for row in sorted(by_entry.get(day, []), key=lambda r: -r["score"]):
            if len(open_positions) >= max_concurrent:
                skipped_capacity += 1
                continue
            total_equity = cash + sum(p["last_value"] for p in open_positions)
            alloc = total_equity * pct
            alloc = min(alloc, cash)  # cannot spend more than available cash
            if alloc <= 0 or float(row["entry_price"]) <= 0:
                skipped_cash += 1
                continue
            shares = alloc / float(row["entry_price"])
            cost = shares * float(row["entry_price"])
            cash -= cost
            open_positions.append({
                "ticker": str(row["ticker"]),
                "shares": shares,
                "entry_price": float(row["entry_price"]),
                "exit_date": row["exit_date"],
                "exit_price": float(row["exit_price"]),
                "last_value": cost,
            })

        # 3) Mark-to-market open positions using today's close.
        for pos in open_positions:
            c = close_map.get(pos["ticker"], {}).get(day)
            if c is not None:
                pos["last_value"] = c * pos["shares"]
            # else: leave last_value as-is (no trading day for this ticker)

        total_equity = cash + sum(p["last_value"] for p in open_positions)
        equity_curve.append({
            "date": day.isoformat(),
            "equity": round(total_equity, 2),
            "cash": round(cash, 2),
            "positions": len(open_positions),
        })

        if total_equity > peak_equity:
            peak_equity = total_equity
        dd = (peak_equity - total_equity) / peak_equity if peak_equity > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            max_dd_date = day

    # Close any still-open positions at their exit_price even if exit_date is
    # past sim_end — shouldn't happen due to sim_end definition, but defensive.
    for pos in open_positions:
        cash += pos["exit_price"] * pos["shares"]
    final_equity = cash if not equity_curve else equity_curve[-1]["equity"]

    total_return = (final_equity / initial_capital - 1) * 100
    years = max(len(sim_days) / 252.0, 1e-9)
    if final_equity > 0:
        annual_return = ((final_equity / initial_capital) ** (1 / years) - 1) * 100
    else:
        annual_return = 0.0

    equities = pd.Series([e["equity"] for e in equity_curve])
    daily_returns = equities.pct_change().dropna()
    if daily_returns.std() > 0:
        sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252))
    else:
        sharpe = 0.0

    positions_series = [e["positions"] for e in equity_curve]
    active_days = [p for p in positions_series if p > 0]

    return {
        "detail_csv": str(detail_csv),
        "position_pct": position_pct,
        "max_concurrent": max_concurrent,
        "initial_capital": initial_capital,
        "trades_in_file": int(len(df)),
        "trades_executed": int(len(df) - skipped_capacity - skipped_cash),
        "skipped_capacity": int(skipped_capacity),
        "skipped_cash": int(skipped_cash),
        "sim_days": len(sim_days),
        "final_equity": round(final_equity, 2),
        "equity_multiple": round(final_equity / initial_capital, 3),
        "total_return_pct": round(total_return, 2),
        "annualized_return_pct": round(annual_return, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "max_drawdown_date": max_dd_date.isoformat() if max_dd_date else None,
        "sharpe_ratio": round(sharpe, 3),
        "avg_positions_when_active": round(float(np.mean(active_days)), 1) if active_days else 0.0,
        "max_concurrent_observed": max(positions_series) if positions_series else 0,
        "utilization_pct": round(len(active_days) / max(len(sim_days), 1) * 100, 1),
        # Returned so a reviewer can recompute drawdown and Sharpe rather than
        # trust the two scalars. Sharpe here is mean/std of DAILY equity returns
        # annualised by sqrt(252); drawdown is peak-to-trough on this same curve.
        "equity_curve": equity_curve,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", help="Detail CSV(s) with entry_date/exit_date/exit_price")
    parser.add_argument("--position-pct", nargs="+", type=float, default=[20, 25, 30, 40, 50])
    parser.add_argument("--max-concurrent", type=int, default=5,
                        help="Max simultaneously-open positions (independent of position_pct)")
    parser.add_argument("--capital", type=float, default=100_000.0)
    parser.add_argument("--config", default="config/default.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)-8s | %(message)s")

    all_tickers: set[str] = set()
    min_date = date.max
    max_date = date.min
    for f in args.files:
        df = pd.read_csv(f)
        df = df[df["pnl_pct"].notna()].copy()
        all_tickers.update(df["ticker"].astype(str).unique())
        # Cover both schemas: explicit entry/exit dates or legacy date+exit_day
        for col in ("entry_date", "exit_date", "date"):
            if col not in df.columns:
                continue
            ds = pd.to_datetime(df[col]).dt.date
            if len(ds):
                min_date = min(min_date, ds.min())
                max_date = max(max_date, ds.max())

    data_map = _download_price_data(sorted(all_tickers), min_date, max_date, args.config)
    calendar = _calendar_from_data(data_map)
    logger.info("Trading calendar: %d days (%s → %s)", len(calendar), calendar[0], calendar[-1])

    summary: dict = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "max_concurrent": args.max_concurrent,
        "files": {},
    }

    for f in args.files:
        rows = {}
        for pct in args.position_pct:
            res = simulate(f, data_map, calendar,
                           position_pct=float(pct),
                           max_concurrent=args.max_concurrent,
                           initial_capital=args.capital)
            rows[f"{pct}%"] = res
        summary["files"][f] = rows

    # Curves go to CSV; keeping them in the summary would bloat it past readability.
    for fname, rows in summary["files"].items():
        for alloc, res in rows.items():
            curve = res.pop("equity_curve", None)
            if curve:
                dest = Path(fname).with_name(
                    Path(fname).stem + f"__equity_curve_{str(alloc).replace('%','pct')}.csv")
                pd.DataFrame(curve).to_csv(dest, index=False)
                res["equity_curve_csv"] = dest.name

    out = Path(args.files[0]).with_name("portfolio_sim_v2_summary.json")
    out.write_text(json.dumps(summary, indent=2, default=str))
    logger.info("Wrote %s", out)

    print()
    print("=" * 72)
    print(f"PORTFOLIO SIM v2 (max_concurrent={args.max_concurrent}, capital=${args.capital:,.0f})")
    print("=" * 72)
    for f, rows in summary["files"].items():
        print(f"\n## {Path(f).stem}")
        print(f"{'Alloc':<8}{'Trades':>8}{'Skip':>8}{'Eq':>8}{'Ret%':>10}{'Ann%':>8}{'DD%':>8}{'Sharpe':>8}{'Util%':>8}")
        for alloc, r in rows.items():
            if "error" in r:
                print(f"  {alloc:<6}  ERROR: {r['error']}")
                continue
            print(f"{alloc:<8}{r['trades_executed']:>8}{r['skipped_capacity']:>8}"
                  f"{r['equity_multiple']:>8.2f}{r['total_return_pct']:>10.1f}"
                  f"{r['annualized_return_pct']:>8.1f}{r['max_drawdown_pct']:>8.1f}"
                  f"{r['sharpe_ratio']:>8.2f}{r['utilization_pct']:>8.1f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
