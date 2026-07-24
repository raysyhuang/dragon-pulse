#!/usr/bin/env python3
"""
Experiment 2: Wait-for-confirmation entry.

Hypothesis: dragon-pulse MR's edge is being diluted by trades where the bounce
never starts (open below entry, continues down). Instead of buying at T+1 open
@ entry_price, wait one more day: only enter if T+1 close > entry_price
(intraday confirmed a bounce). Hold N more days, exit on the same absolute
stop/target levels.

Compare against the baseline (existing detail CSV — T+1 open @ entry_price).

Usage:
    python scripts/experiment_wait_for_confirmation.py \
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
from scripts.mas_style_analysis import (
    load_t1_opens,  # we'll also walk forward bars from data_map
    trade_level_metrics,
    run_portfolio_alloc_table,
)

logger = logging.getLogger(__name__)


def _ticker_dataframe(data_map: dict, ticker: str) -> pd.DataFrame | None:
    df = data_map.get(ticker)
    if df is None or df.empty:
        return None
    cols_lower = {c: c.lower() for c in df.columns}
    return df.rename(columns=cols_lower)


def reevaluate_with_confirmation(
    detail_df: pd.DataFrame,
    data_map: dict,
    calendar: list,
    limit_up_pct: float = 0.10,
    entry_bar: str = "t1_close",
    score_floor: float | None = None,
) -> pd.DataFrame:
    """For each trade, simulate wait-for-confirmation entry.

    Returns a new DataFrame with the same columns as the input detail CSV but:
      - entry_price = T+1 close OR T+2 open (depending on entry_bar)
      - exit_price/exit_day/exit_reason/pnl_pct recomputed from the bar AFTER entry
      - rows skipped if T+1 close <= original entry_price or > entry × (1+limit_up_pct)
      - rows skipped if score < score_floor (when set)

    entry_bar:
      "t1_close" — enter at T+1 close on confirmation day (assumes EOD fill;
                   unrealistic but useful for the principle test).
      "t2_open"  — enter at T+2 open after confirmation visible at T+1 close
                   (realistic: you decide after market close on T+1, fill at
                   the opening auction on T+2).
    """
    if entry_bar not in {"t1_close", "t2_open"}:
        raise ValueError(f"entry_bar must be t1_close or t2_open, got {entry_bar}")

    cal_idx = {d: i for i, d in enumerate(calendar)}
    out_rows = []
    skip_no_confirm = 0
    skip_limit_up = 0
    skip_insufficient = 0
    skip_score = 0

    for _, row in detail_df.iterrows():
        ticker = str(row["ticker"])
        sig_date = pd.to_datetime(row["date"]).date()
        original_entry = float(row["entry_price"])
        stop = float(row["stop_loss"])
        target = float(row["target_1"])
        hold = int(row["holding_period"])

        if score_floor is not None:
            try:
                if float(row.get("score", 0)) < score_floor:
                    skip_score += 1
                    continue
            except (TypeError, ValueError):
                skip_score += 1
                continue

        df = _ticker_dataframe(data_map, ticker)
        if df is None:
            skip_insufficient += 1
            continue

        # Map ticker dates → row positions
        idx = df.index
        ddates = idx.date if hasattr(idx, "date") else pd.to_datetime(idx).date
        date_pos = {d: i for i, d in enumerate(ddates)}

        # T+1 = next trading day in calendar
        cal_pos = cal_idx.get(sig_date)
        if cal_pos is None or cal_pos + 1 >= len(calendar):
            skip_insufficient += 1
            continue
        t1_date = calendar[cal_pos + 1]
        t1_pos = date_pos.get(t1_date)
        if t1_pos is None:
            skip_insufficient += 1
            continue

        t1_close = float(df["close"].iloc[t1_pos])

        # Confirmation gate (uses T+1 close in both modes — confirmation is
        # only observable after T+1 EOD)
        if t1_close <= original_entry:
            skip_no_confirm += 1
            continue
        if t1_close > original_entry * (1 + limit_up_pct):
            # Effectively limit-up on T+1 — assume we'd have missed the fill
            skip_limit_up += 1
            continue

        # Determine entry price + the bar after which forward evaluation begins
        if entry_bar == "t1_close":
            entry_price = t1_close
            forward_start = t1_pos + 1  # walk from T+2 onward
        else:  # t2_open
            if cal_pos + 2 >= len(calendar):
                skip_insufficient += 1
                continue
            t2_date = calendar[cal_pos + 2]
            t2_pos = date_pos.get(t2_date)
            if t2_pos is None:
                skip_insufficient += 1
                continue
            t2_open = float(df["open"].iloc[t2_pos]) if "open" in df.columns else None
            if t2_open is None or t2_open <= 0:
                skip_insufficient += 1
                continue
            entry_price = t2_open
            # A-share T+1 rule: no same-day exits. Start walking from T+3
            # (the first day a sell is legal).
            forward_start = t2_pos + 1

        bars = df.iloc[forward_start : forward_start + hold]
        if bars.empty:
            skip_insufficient += 1
            continue

        exit_price = None
        exit_day = None
        exit_reason = None
        hit_target = False
        hit_stop = False
        for day_idx, (_, bar) in enumerate(bars.iterrows(), start=1):
            hi = float(bar["high"])
            lo = float(bar["low"])
            cl = float(bar["close"])
            if hi >= target:
                exit_price = target
                exit_day = day_idx
                exit_reason = "target_hit"
                hit_target = True
                break
            if lo <= stop:
                exit_price = stop
                exit_day = day_idx
                exit_reason = "stop_hit"
                hit_stop = True
                break
        else:
            exit_price = float(bars.iloc[-1]["close"])
            exit_day = len(bars)
            exit_reason = "hold_expired"

        pnl_pct = round((exit_price / entry_price - 1) * 100, 2)

        # Explicit entry_date and exit_date so portfolio_sim_v2 (and any
        # downstream consumer) doesn't have to infer dates from offsets.
        #   t1_close: entry at T+1 close, first walked bar at T+2.
        #             exit_day=N → exit_date = T+1+N = entry_date + N
        #   t2_open : entry at T+2 open,  first walked bar at T+3.
        #             exit_day=N → exit_date = T+2+N = entry_date + N
        # In both modes the formula collapses to entry_date + exit_day.
        entry_date = t1_date if entry_bar == "t1_close" else calendar[cal_pos + 2]
        exit_date = calendar[min(cal_idx[entry_date] + exit_day, len(calendar) - 1)]

        out_rows.append({
            "ticker": ticker,
            "entry_price": entry_price,
            "stop_loss": stop,
            "target_1": target,
            "holding_period": hold,
            "exit_price": exit_price,
            "exit_day": exit_day,
            "exit_reason": exit_reason,
            "pnl_pct": pnl_pct,
            "hit_target": hit_target,
            "hit_stop": hit_stop,
            # Keep legacy 'date' for v1 sim compatibility, but the v2 sim
            # consumes entry_date/exit_date directly.
            "date": t1_date.isoformat(),
            "entry_date": entry_date.isoformat(),
            "exit_date": exit_date.isoformat(),
            "engine": row.get("engine"),
            "score": row.get("score"),
            "regime": row.get("regime"),
            "subtype": row.get("subtype"),
        })

    logger.info(
        "Confirmation experiment (entry_bar=%s, score_floor=%s): kept %d / %d "
        "(skip_score=%d, skip_no_confirm=%d, skip_limit_up=%d, skip_insufficient=%d)",
        entry_bar, score_floor,
        len(out_rows), len(detail_df),
        skip_score, skip_no_confirm, skip_limit_up, skip_insufficient,
    )
    return pd.DataFrame(out_rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", help="Backtest detail CSV(s)")
    parser.add_argument("--allocs", default="20,25,30,40,50")
    parser.add_argument(
        "--entry-bar", default="t1_close", choices=["t1_close", "t2_open"],
        help="Where to mark entry on confirmation day",
    )
    parser.add_argument(
        "--score-floor", type=float, default=None,
        help="Skip signals with score below this floor",
    )
    parser.add_argument(
        "--label", default="",
        help="Suffix added to output JSON / detail CSV filenames",
    )
    parser.add_argument(
        "--out-json", default="outputs/backtest/experiment_wait_for_confirmation.json",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )
    allocs = [int(x) for x in args.allocs.split(",") if x.strip()]

    label_suffix = f"__{args.label}" if args.label else ""
    full_summary: dict = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "experiment": "wait_for_confirmation",
        "entry_bar": args.entry_bar,
        "score_floor": args.score_floor,
        "label": args.label,
        "allocs_pct": allocs,
        "windows": {},
    }

    for f in args.files:
        path = Path(f)
        logger.info("=" * 70)
        logger.info("Window: %s", path.stem)
        logger.info("=" * 70)
        baseline_df = pd.read_csv(f)
        baseline_df = baseline_df[baseline_df["pnl_pct"].notna()].copy()

        # Reuse the same downloader as mas_style_analysis (cached)
        _, data_map, calendar = load_t1_opens(baseline_df)

        confirm_df = reevaluate_with_confirmation(
            baseline_df, data_map, calendar,
            entry_bar=args.entry_bar, score_floor=args.score_floor,
        )
        # Save the rebuilt detail CSV so portfolio_sim can consume it
        confirm_path = path.with_name(path.stem + f"__confirmation{label_suffix}.csv")
        confirm_df.to_csv(confirm_path, index=False)
        logger.info("Wrote rebuilt detail: %s", confirm_path)

        variant_label = f"wait_for_confirmation_{args.entry_bar}"
        if args.score_floor is not None:
            variant_label += f"_score{int(args.score_floor)}"

        win = {}
        for label, df in [("baseline", baseline_df), (variant_label, confirm_df)]:
            logger.info("-- %s: %d trades", label, len(df))
            tm = trade_level_metrics(df, label)
            alloc_tbl = run_portfolio_alloc_table(
                detail_path=path if label == "baseline" else confirm_path,
                filtered_df=df,
                data_map=data_map,
                calendar=calendar,
                allocs=allocs,
            )
            win[label] = {"trade_metrics": tm, "portfolio_alloc": alloc_tbl}
        full_summary["windows"][path.stem] = win

    out_default = Path(args.out_json)
    if args.label and out_default.stem.endswith("wait_for_confirmation"):
        out = out_default.with_name(out_default.stem + f"_{args.label}" + out_default.suffix)
    else:
        out = out_default
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(full_summary, indent=2, default=str))
    logger.info("Wrote %s", out)

    print()
    print("=" * 70)
    print(f"EXPERIMENT — WAIT-FOR-CONFIRMATION ({args.entry_bar}"
          f"{', score≥' + str(args.score_floor) if args.score_floor is not None else ''})")
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
            print("    Portfolio:")
            for alloc, row in blob["portfolio_alloc"].items():
                print(f"      {alloc:>4} (cap={row['max_positions_cap']}): "
                      f"eq={row['equity_multiple']}x  ret={row['total_return_pct']:+.1f}%  "
                      f"DD={row['max_drawdown_pct']}%  Sharpe={row['sharpe_ratio']}  "
                      f"util={row['utilization_pct']}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
