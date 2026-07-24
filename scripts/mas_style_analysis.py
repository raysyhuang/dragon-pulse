#!/usr/bin/env python3
"""
MAS-style analysis on existing Dragon Pulse backtest detail CSVs.

One-shot script: produces a side-by-side comparison against the MAS reference
numbers Hermes generated on S&P500/yfinance.

For each input detail CSV (1Y, 3Y) and each filter variant (baseline / strict
no-chase), it reports:
  - Trades, win rate, avg trade %
  - Trade-level Sharpe and profit factor
  - Max drawdown of the sequential equity curve (treats each trade in date
    order, all-in)
  - Allocation table via portfolio_sim at 20/25/30/40/50% per pick

The strict no-chase rule = skip trades whose T+1 open is above the signal
entry_price (i.e. price gapped above the alert before we could buy).

Usage:
    python scripts/mas_style_analysis.py \
        outputs/backtest/backtest_detail_1yr_live_equiv.csv \
        outputs/backtest/backtest_detail_3yr_live_equiv.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
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
from scripts.portfolio_sim import (
    _build_calendar_from_data,
    simulate_portfolio,
)

logger = logging.getLogger(__name__)


def load_t1_opens(
    detail_df: pd.DataFrame,
    config_path: str = "config/default.yaml",
) -> tuple[dict[tuple[str, date], float], dict[str, pd.DataFrame], list[date]]:
    """Download OHLCV once, then build a (ticker, signal_date) -> T+1 open map.

    Returns (open_map, data_map, calendar).
    """
    tickers = sorted(detail_df["ticker"].astype(str).unique())
    sig_dates = pd.to_datetime(detail_df["date"]).dt.date
    start = sig_dates.min() - timedelta(days=10)
    end = sig_dates.max() + timedelta(days=30)

    config = load_config(config_path)
    _, download_range_fn, provider_config, _ = get_data_functions(config)

    logger.info(
        "Downloading OHLCV for %d tickers (%s to %s) — uses cache when warm",
        len(tickers), start, end,
    )
    data_map, report = download_range_fn(
        tickers=tickers, start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"), provider_config=provider_config,
    )
    logger.info(
        "Downloaded: %d OK, %d failed", len(data_map), len(report.get("bad_tickers", [])),
    )

    calendar = _build_calendar_from_data(data_map)
    cal_idx = {d: i for i, d in enumerate(calendar)}

    open_map: dict[tuple[str, date], float] = {}
    miss = 0
    for ticker, df in data_map.items():
        if df.empty:
            continue
        open_col = "Open" if "Open" in df.columns else "open"
        if open_col not in df.columns:
            continue
        idx = df.index
        d_index = idx.date if hasattr(idx, "date") else pd.to_datetime(idx).date
        ticker_dates = list(d_index)
        ticker_opens = df[open_col].astype(float).values
        for sd in sig_dates.unique():
            pos = cal_idx.get(sd)
            if pos is None or pos + 1 >= len(calendar):
                continue
            t1 = calendar[pos + 1]
            # Find this ticker's row for t1
            try:
                local_pos = ticker_dates.index(t1)
            except ValueError:
                miss += 1
                continue
            open_map[(ticker, sd)] = float(ticker_opens[local_pos])

    logger.info(
        "Built T+1 open map: %d entries (%d (ticker,date) pairs without data)",
        len(open_map), miss,
    )
    return open_map, data_map, calendar


def apply_no_chase(detail_df: pd.DataFrame, open_map: dict) -> pd.DataFrame:
    """Return a copy of detail_df keeping only rows where T+1 open <= entry."""
    sig_dates = pd.to_datetime(detail_df["date"]).dt.date.values
    keep_mask = np.zeros(len(detail_df), dtype=bool)
    missing = 0
    for i, row in enumerate(detail_df.itertuples(index=False)):
        key = (str(row.ticker), sig_dates[i])
        op = open_map.get(key)
        if op is None:
            missing += 1
            continue
        if op <= float(row.entry_price):
            keep_mask[i] = True
    logger.info(
        "No-chase filter: kept %d / %d (%d had no T+1 open)",
        int(keep_mask.sum()), len(detail_df), missing,
    )
    return detail_df.loc[keep_mask].reset_index(drop=True)


def trade_level_metrics(df: pd.DataFrame, label: str) -> dict:
    """MAS-style trade-level metrics: WR, avg, Sharpe, PF, MaxDD on sequential
    all-in compounding (one position at a time, chronological)."""
    valid = df[df["pnl_pct"].notna()].copy()
    n = len(valid)
    if n == 0:
        return {"label": label, "trades": 0}

    pnl = valid["pnl_pct"].astype(float)
    wins = pnl > 0
    win_rate = float(wins.mean())
    avg = float(pnl.mean())
    avg_win = float(pnl[wins].mean()) if wins.any() else 0.0
    avg_loss = float(pnl[~wins].mean()) if (~wins).any() else 0.0

    # Profit factor: gross win % / |gross loss %|
    gross_win = float(pnl[wins].sum())
    gross_loss = float(-pnl[~wins].sum())
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # Trade-level Sharpe — sample mean / std, annualized by assuming the
    # trades themselves are independent observations. To match Hermes's
    # numbers we use the simple sqrt(N) annualizer.
    ret_decimal = pnl / 100.0
    if ret_decimal.std(ddof=1) > 0:
        sharpe_per_trade = float(ret_decimal.mean() / ret_decimal.std(ddof=1))
        sharpe_annualized = float(sharpe_per_trade * np.sqrt(252))
    else:
        sharpe_per_trade = 0.0
        sharpe_annualized = 0.0

    # Sequential all-in compounded equity & drawdown
    valid = valid.sort_values("date").reset_index(drop=True)
    eq = (1 + valid["pnl_pct"].astype(float) / 100.0).cumprod()
    peak = eq.cummax()
    dd = (peak - eq) / peak
    max_dd = float(dd.max()) * 100
    total_return = float(eq.iloc[-1] - 1) * 100

    return {
        "label": label,
        "trades": n,
        "win_rate_pct": round(win_rate * 100, 2),
        "avg_trade_pct": round(avg, 3),
        "avg_win_pct": round(avg_win, 3),
        "avg_loss_pct": round(avg_loss, 3),
        "profit_factor": round(pf, 3) if pf != float("inf") else None,
        "sharpe_per_trade": round(sharpe_per_trade, 3),
        "sharpe_annualized_sqrt252": round(sharpe_annualized, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "all_in_total_return_pct": round(total_return, 2),
    }


def run_portfolio_alloc_table(
    detail_path: Path,
    filtered_df: pd.DataFrame,
    data_map: dict,
    calendar: list[date],
    allocs: list[int],
    initial_capital: float = 100_000.0,
) -> dict:
    """Run portfolio_sim at multiple per-pick allocation %s.

    20%→max_positions=5, 25→4, 33→3, 50→2, 100→1. We approximate the user's
    requested set (20/25/30/40/50) by the nearest integer cap.

    The temp CSV path is PID-scoped so parallel invocations don't race.
    """
    import os
    tmp = detail_path.with_name(
        f"{detail_path.stem}__filtered_pid{os.getpid()}.csv"
    )
    filtered_df.to_csv(tmp, index=False)

    table = {}
    for alloc in allocs:
        # Position cap = round(100 / alloc), floor at 1
        cap = max(1, round(100 / alloc))
        res = simulate_portfolio(
            str(tmp), data_map, calendar,
            initial_capital=initial_capital, max_positions=cap,
        )
        table[f"{alloc}%"] = {
            "max_positions_cap": cap,
            "trades_simulated": res.get("total_trades_simulated"),
            "skipped_over_cap": res.get("skipped_trades_capacity"),
            "equity_multiple": res.get("equity_multiple"),
            "total_return_pct": res.get("total_return_pct"),
            "annualized_return_pct": res.get("annualized_return_pct"),
            "max_drawdown_pct": res.get("max_drawdown_pct"),
            "sharpe_ratio": res.get("sharpe_ratio"),
            "utilization_pct": res.get("utilization_pct"),
        }

    tmp.unlink(missing_ok=True)
    return table


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", help="Backtest detail CSV(s)")
    parser.add_argument("--allocs", default="20,25,30,40,50",
                        help="Per-pick allocation %s to compound (CSV)")
    parser.add_argument(
        "--out-json", default="outputs/backtest/mas_style_summary.json",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    allocs = [int(x) for x in args.allocs.split(",") if x.strip()]

    full_summary: dict = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "filters": ["baseline", "strict_no_chase"],
        "allocs_pct": allocs,
        "windows": {},
    }

    for f in args.files:
        path = Path(f)
        logger.info("=" * 70)
        logger.info("Window: %s", path.stem)
        logger.info("=" * 70)
        detail = pd.read_csv(f)
        detail = detail[detail["pnl_pct"].notna()].copy()

        open_map, data_map, calendar = load_t1_opens(detail)

        baseline = detail
        no_chase = apply_no_chase(detail, open_map)

        win_summary = {}
        for label, df in [("baseline", baseline), ("strict_no_chase", no_chase)]:
            logger.info("-- %s: %d trades", label, len(df))
            trade_metrics = trade_level_metrics(df, label)
            alloc_table = run_portfolio_alloc_table(
                detail_path=path,
                filtered_df=df,
                data_map=data_map,
                calendar=calendar,
                allocs=allocs,
            )
            win_summary[label] = {
                "trade_metrics": trade_metrics,
                "portfolio_alloc": alloc_table,
            }

        full_summary["windows"][path.stem] = win_summary

    out = Path(args.out_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as fh:
        json.dump(full_summary, fh, indent=2, default=str)
    logger.info("Wrote %s", out)

    # Pretty print
    print()
    print("=" * 70)
    print("MAS-STYLE COMPARISON SUMMARY")
    print("=" * 70)
    for win_name, win_data in full_summary["windows"].items():
        print(f"\n## {win_name}")
        for filt, blob in win_data.items():
            tm = blob["trade_metrics"]
            print(f"\n  [{filt}]  trades={tm['trades']}  WR={tm['win_rate_pct']}%  "
                  f"avg={tm['avg_trade_pct']:+.2f}%  PF={tm['profit_factor']}  "
                  f"Sharpe={tm['sharpe_annualized_sqrt252']}  "
                  f"MaxDD={tm['max_drawdown_pct']}%  "
                  f"AllInRet={tm['all_in_total_return_pct']:+.1f}%")
            print("    Portfolio (mark-to-market, capital-recycled):")
            for alloc, row in blob["portfolio_alloc"].items():
                print(f"      {alloc:>4} per pick (cap={row['max_positions_cap']}): "
                      f"eq={row['equity_multiple']}x  ret={row['total_return_pct']:+.1f}%  "
                      f"DD={row['max_drawdown_pct']}%  Sharpe={row['sharpe_ratio']}  "
                      f"util={row['utilization_pct']}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
