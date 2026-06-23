"""Backtest alternative exit logic on execution-watchlist picks.

Re-simulates each pick from the SAME entry (next-session open, same cancellation
rules as the live engine) but replaces the fixed target/stop/3-day-hold exit with
a configurable scale-out + breakeven + ATR-trailing-stop ruleset.

Outputs a portfolio_sim_v2-compatible detail CSV so realized compounded returns
can be compared directly against the production baseline.

Intrabar convention is deliberately CONSERVATIVE: within a bar the stop is checked
against the Low before any profit target is checked against the High, so we never
overstate the new logic. Trailing stops update on the bar Close.

Usage:
    python scripts/exit_logic_backtest.py --start 2026-04-01 --end 2026-06-22 \
        --t1 3 --t2 5 --trail-atr 1.5 --max-hold 10 --out-dir outputs/performance/exit_v2
"""

from __future__ import annotations

import argparse
import logging
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.features.performance.backtest import (
    load_execution_watchlists_in_range,
    _to_dt,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("exit_backtest")


def _f(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _atr_at_entry(hist: pd.DataFrame, entry_ts: pd.Timestamp, period: int = 14) -> float | None:
    """ATR(period) using bars up to and including the entry bar."""
    df = hist[hist.index <= entry_ts].copy()
    if len(df) < period + 1:
        return None
    high = pd.to_numeric(df["High"], errors="coerce")
    low = pd.to_numeric(df["Low"], errors="coerce")
    close = pd.to_numeric(df["Close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    return float(atr) if np.isfinite(atr) else None


def simulate_pick(pick: dict, price_df: pd.DataFrame, p: argparse.Namespace) -> dict:
    """Apply alternative exit logic to a single pick. Returns a detail row.

    Modes (informed by the MAS Sniper grid: don't take winners off early, cut
    laggards early, let runners run):
      scaleout         scale out 1/3 at t1, 1/3 at t2, trail the rest
      runner           whole position; breakeven after +breakeven_at; trail trail_atr*ATR
      laggard          original fixed target_1/stop, but cut if flat/red at laggard_day close
      runner_laggard   runner + laggard cut
    """
    out = {
        "baseline_date": pick.get("baseline_date"),
        "regime": pick.get("regime"),
        "ticker": pick.get("ticker"),
        "name_cn": pick.get("name_cn"),
        "score": pick.get("score"),
        "status": None,
        "entry_date": None,
        "entry_price": None,
        "exit_date": None,
        "exit_price": None,
        "pnl_pct": None,
        "exit_reason": None,
        "n_tranches_filled": 0,
        "max_return_pct": None,
        "atr_pct": None,
    }
    if price_df is None or price_df.empty:
        out["status"] = "no_data"
        return out

    df = price_df.sort_index()
    baseline_ts = pd.Timestamp(str(pick.get("baseline_date")))
    fwd = df[df.index > baseline_ts]
    if fwd.empty:
        out["status"] = "open_or_partial"
        return out

    entry_ts = fwd.index[0]
    entry = _f(fwd.iloc[0].get("Open"))
    if entry is None or entry <= 0:
        out["status"] = "no_data"
        return out

    max_entry = _f(pick.get("max_entry_price"))
    stop0 = _f(pick.get("stop_loss"))
    if stop0 is None:
        out["status"] = "unsupported_schema"
        return out

    # Gap below stop -> skip in every entry policy.
    if entry < stop0:
        out.update(status="cancelled", exit_reason="open_below_stop_loss")
        return out

    # Entry policy (default 'nochase' preserves live behavior):
    #   nochase     fill at T+1 open iff open <= max_entry, else skip
    #   chase_band  fill at T+1 open iff open <= max_entry*(1+band%), else skip
    #   limit_touch fill at open if <= max_entry; else fill at max_entry if T+1
    #               low touches it (intraday pullback); else skip
    entry_mode = getattr(p, "entry_mode", "nochase")
    band = getattr(p, "chase_band", 0.0) / 100.0
    if max_entry is not None:
        if entry_mode == "limit_touch":
            first_low = _f(fwd.iloc[0].get("Low"))
            if entry <= max_entry:
                pass
            elif first_low is not None and first_low <= max_entry:
                entry = max_entry
            else:
                out.update(status="cancelled", exit_reason="no_touch")
                return out
        else:  # nochase (band=0) or chase_band
            if entry > max_entry * (1.0 + band):
                out.update(status="cancelled", exit_reason="open_above_max_entry")
                return out

    atr = _atr_at_entry(df, entry_ts, period=14)
    if atr is None:
        # Fallback: infer ATR-equivalent from the engine's own stop distance.
        atr = entry - stop0
    out["atr_pct"] = round(atr / entry * 100.0, 2)

    out["entry_date"] = entry_ts.strftime("%Y-%m-%d")
    out["entry_price"] = round(entry, 4)

    bars = fwd.iloc[: p.max_hold].copy()
    highs = pd.to_numeric(bars["High"], errors="coerce")
    if highs.notna().any():
        out["max_return_pct"] = round((float(highs.max()) / entry - 1.0) * 100.0, 2)

    target_1 = _f(pick.get("target_1"))
    holding_period = int(_f(pick.get("holding_period")) or p.max_hold)

    # ---- fixed: production baseline (target-first / stop / hold-expired). ----
    if p.mode == "fixed":
        fixed_bars = fwd.iloc[1 : holding_period + 1] if len(fwd) > 1 else fwd.iloc[:0]
        if fixed_bars.empty:
            out["status"] = "open_or_partial"
            return out
        for day_idx, (dt, row) in enumerate(fixed_bars.iterrows(), start=1):
            high, low, close = _f(row.get("High")), _f(row.get("Low")), _f(row.get("Close"))
            if high is None or low is None or close is None:
                continue
            if target_1 is not None and high >= target_1:  # target first (matches prod)
                out.update(status="matured", exit_date=dt.strftime("%Y-%m-%d"),
                           exit_price=round(target_1, 4), exit_reason="target_hit",
                           pnl_pct=round((target_1 / entry - 1) * 100, 2))
                return out
            if low <= stop0:
                out.update(status="matured", exit_date=dt.strftime("%Y-%m-%d"),
                           exit_price=round(stop0, 4), exit_reason="stop_hit",
                           pnl_pct=round((stop0 / entry - 1) * 100, 2))
                return out
        last_close = _f(fixed_bars.iloc[-1].get("Close"))
        out.update(status="matured", exit_date=fixed_bars.index[-1].strftime("%Y-%m-%d"),
                   exit_price=round(last_close, 4), exit_reason="hold_expired",
                   pnl_pct=round((last_close / entry - 1) * 100, 2))
        return out

    t1_price = entry * (1 + p.t1 / 100.0)
    t2_price = entry * (1 + p.t2 / 100.0)
    use_scaleout = p.mode == "scaleout"
    use_laggard = p.mode in ("laggard", "runner_laggard")
    use_fixed_target = p.mode == "laggard" and target_1 is not None

    remaining = 1.0
    realized = 0.0  # sum of frac * fractional_return
    stop = stop0
    sold_t1 = sold_t2 = False
    engaged = False  # breakeven/trail engaged (price showed strength)
    highest_close = entry
    last_reason = None
    last_dt = entry_ts

    n_bars = len(bars)
    for day_idx, (dt, row) in enumerate(bars.iterrows(), start=1):
        high = _f(row.get("High"))
        low = _f(row.get("Low"))
        close = _f(row.get("Close"))
        if high is None or low is None or close is None:
            continue
        last_dt = dt

        # 1) CONSERVATIVE: stop checked first, against the current stop level.
        if low <= stop:
            realized += remaining * (stop / entry - 1.0)
            last_reason = "trail_stop" if engaged else "initial_stop"
            remaining = 0.0
            break

        # 2a) scaleout mode: take partials at t1/t2.
        if use_scaleout:
            if not sold_t1 and high >= t1_price:
                realized += p.t1_frac * (t1_price / entry - 1.0)
                remaining -= p.t1_frac
                sold_t1 = True
                engaged = True
                out["n_tranches_filled"] += 1
                if p.t1 >= p.breakeven_at:
                    stop = max(stop, entry)
            if not sold_t2 and high >= t2_price:
                realized += p.t2_frac * (t2_price / entry - 1.0)
                remaining -= p.t2_frac
                sold_t2 = True
                out["n_tranches_filled"] += 1

        # 2b) laggard mode: original fixed target = full exit.
        if use_fixed_target and high >= target_1:
            realized += remaining * (target_1 / entry - 1.0)
            last_reason = "target_hit"
            remaining = 0.0
            break

        # 3) Runner: engage breakeven + trailing once price shows strength.
        highest_close = max(highest_close, close)
        if not use_fixed_target:
            if not engaged and close >= entry * (1 + p.breakeven_at / 100.0):
                engaged = True
            if engaged:
                stop = max(stop, entry, highest_close - p.trail_atr * atr)

        # 4) Laggard cut: flat/red at the cut bar's close -> abandon early.
        if use_laggard and remaining > 1e-9 and day_idx == p.laggard_day:
            if close <= entry * (1 + p.laggard_thresh / 100.0):
                realized += remaining * (close / entry - 1.0)
                last_reason = "laggard_cut"
                remaining = 0.0
                break

        # 5) Time stop.
        if day_idx >= p.max_hold:
            realized += remaining * (close / entry - 1.0)
            last_reason = "max_hold_runner" if engaged else "max_hold"
            remaining = 0.0
            break

    if remaining > 1e-9:
        # Window ran out before max_hold and no stop -> still open.
        if n_bars < p.max_hold:
            last_close = _f(bars.iloc[-1].get("Close"))
            out["status"] = "open_or_partial"
            out["exit_reason"] = "holding_window_incomplete"
            if last_close is not None:
                out["pnl_pct"] = round(
                    (realized + remaining * (last_close / entry - 1.0)) * 100.0, 2
                )
            return out
        # Shouldn't happen, but close at last bar.
        last_close = _f(bars.iloc[-1].get("Close"))
        realized += remaining * (last_close / entry - 1.0)
        last_reason = last_reason or "max_hold"
        remaining = 0.0

    pnl = realized * 100.0
    out["status"] = "matured"
    out["pnl_pct"] = round(pnl, 2)
    out["exit_price"] = round(entry * (1 + realized), 4)
    out["exit_date"] = last_dt.strftime("%Y-%m-%d")
    out["exit_reason"] = last_reason or ("partial_exit" if out["n_tranches_filled"] else "exit")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--picks-csv", default="",
                    help="Replay from a pinned pick CSV (from backtest_1yr --dump-picks) "
                         "instead of execution watchlists. Overrides --start/--end.")
    ap.add_argument("--start", default="")
    ap.add_argument("--end", default="")
    ap.add_argument("--outputs-root", default="outputs")
    ap.add_argument("--out-dir", default="outputs/performance/exit_v2")
    ap.add_argument("--mode", default="scaleout",
                    choices=["fixed", "scaleout", "runner", "laggard", "runner_laggard"])
    ap.add_argument("--laggard-day", type=int, default=2,
                    help="bar index whose close decides the laggard cut")
    ap.add_argument("--laggard-thresh", type=float, default=0.0,
                    help="cut if return-to-close <= this pct at laggard-day")
    ap.add_argument("--t1", type=float, default=3.0, help="first scale-out target pct")
    ap.add_argument("--t2", type=float, default=5.0, help="second scale-out target pct")
    ap.add_argument("--t1-frac", type=float, default=1 / 3)
    ap.add_argument("--t2-frac", type=float, default=1 / 3)
    ap.add_argument("--breakeven-at", type=float, default=3.0)
    ap.add_argument("--trail-atr", type=float, default=1.5)
    ap.add_argument("--max-hold", type=int, default=10)
    args = ap.parse_args()

    if args.picks_csv:
        picks = pd.read_csv(args.picks_csv)
        picks["ticker"] = picks["ticker"].astype(str).str.upper()
        if args.start:
            picks = picks[picks["baseline_date"].astype(str) >= args.start]
        if args.end:
            picks = picks[picks["baseline_date"].astype(str) <= args.end]
        logger.info("Loaded %d pinned picks from %s", len(picks), args.picks_csv)
    else:
        if not args.start or not args.end:
            logger.error("Provide --picks-csv OR both --start and --end.")
            return 1
        picks = load_execution_watchlists_in_range(
            args.outputs_root, start_date=args.start, end_date=args.end
        )
    if picks.empty:
        logger.error("No picks to evaluate.")
        return 1

    from src.core.cn_data import download_daily_range

    tickers = sorted(picks["ticker"].dropna().astype(str).str.upper().unique().tolist())
    baseline_dates = sorted(picks["baseline_date"].astype(str).tolist())
    # Lookback for ATR, forward buffer for max_hold.
    dl_start = (_to_dt(baseline_dates[0]) - timedelta(days=45)).strftime("%Y-%m-%d")
    dl_end = (_to_dt(baseline_dates[-1]) + timedelta(days=max(30, args.max_hold * 3))).strftime(
        "%Y-%m-%d"
    )
    logger.info("Downloading %d tickers %s -> %s", len(tickers), dl_start, dl_end)
    data_map, _ = download_daily_range(tickers=tickers, start=dl_start, end=dl_end)

    rows = []
    for pick in picks.to_dict(orient="records"):
        tk = str(pick.get("ticker") or "").strip().upper()
        rows.append(simulate_pick(pick, data_map.get(tk, pd.DataFrame()), args))
    detail = pd.DataFrame(rows)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    detail_path = out_dir / "exit_v2_detail.csv"
    detail.to_csv(detail_path, index=False)

    matured = detail[detail["status"] == "matured"]
    win = matured[matured["pnl_pct"] > 0]["pnl_pct"]
    loss = matured[matured["pnl_pct"] <= 0]["pnl_pct"]
    logger.info("=" * 64)
    logger.info("ALT-EXIT RESULTS  (t1=%s t2=%s trail=%sxATR max_hold=%s)",
                args.t1, args.t2, args.trail_atr, args.max_hold)
    logger.info("=" * 64)
    logger.info("picks=%d matured=%d cancelled=%d open=%d",
                len(detail), len(matured),
                (detail["status"] == "cancelled").sum(),
                (detail["status"] == "open_or_partial").sum())
    if len(matured):
        logger.info("avg_pnl=%.2f%% median=%.2f%% win_rate=%.0f%%",
                    matured["pnl_pct"].mean(), matured["pnl_pct"].median(),
                    (matured["pnl_pct"] > 0).mean() * 100)
        if len(win) and len(loss):
            logger.info("winners n=%d avg=+%.2f%% | losers n=%d avg=%.2f%% | payoff=%.2f",
                        len(win), win.mean(), len(loss), loss.mean(),
                        win.mean() / abs(loss.mean()))
        logger.info("exit reasons: %s", dict(matured["exit_reason"].value_counts()))
    logger.info("detail -> %s", detail_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
