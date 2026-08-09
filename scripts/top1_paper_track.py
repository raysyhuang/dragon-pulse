#!/usr/bin/env python3
"""Top-1/day paper track — forward A/B of concentration vs the live top-2.

Research lead (2026-07-26): on point-in-time alpha_rs_pullback picks, cutting the
daily book from 2 picks to 1 (by score) ~halved cross-cycle drawdown (2021-2024
maxDD 22%->9.5%) and flipped full-cycle return -10%->+9%. Converges with the prior
mean-reversion selection finding. This is a PAPER tracker to confirm it forward
before any live change: it post-processes the existing execution_watchlist (already
score-ranked) and does NOT touch live ranking, execution_watchlist, or alerts book.

Each scan day it records the top-1 pick and the top-2 set. Matured positions are
scored with a conservative bracket (stop-first on same-bar touch) and compared to
CSI 300 buy-hold over the same window. `report` prints/telegrams cumulative
top1 vs top2 vs CSI300 (return, win rate, maxDD).

Modes:
  record   --date YYYY-MM-DD   append today's watchlist top-1/top-2 to the ledger
  evaluate --date YYYY-MM-DD   score any matured, unevaluated ledger rows as-of date
  report                       aggregate cumulative comparison (+ optional --alert)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.core.cn_data import download_daily_range  # noqa: E402
from src.core.alerts import AlertConfig, AlertManager, _ticker_display  # noqa: E402

logger = logging.getLogger(__name__)

CSI300 = "000300.SH"
LEDGER = PROJECT_ROOT / "outputs" / "top1_paper" / "ledger.jsonl"
SUMMARY = PROJECT_ROOT / "outputs" / "top1_paper" / "summary.json"
PICK_FIELDS = ("ticker", "name_cn", "score", "entry_price", "max_entry_price",
               "stop_loss", "target_1", "holding_period")


def _read_ledger() -> list[dict]:
    if not LEDGER.exists():
        return []
    return [json.loads(l) for l in LEDGER.read_text(encoding="utf-8").splitlines() if l.strip()]


def _write_ledger(rows: list[dict]) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")


def _slim(pick: dict) -> dict:
    return {k: pick.get(k) for k in PICK_FIELDS}


def record(date: str) -> int:
    wl = PROJECT_ROOT / "outputs" / date / f"execution_watchlist_{date}.json"
    if not wl.exists():
        logger.info("No execution_watchlist for %s; nothing to record.", date)
        return 0
    payload = json.loads(wl.read_text(encoding="utf-8"))
    picks = sorted(payload.get("picks", []), key=lambda p: -float(p.get("score", 0)))
    row = {
        "scan_date": date,
        "regime": payload.get("regime"),
        "n_live_picks": len(picks),
        "top1": _slim(picks[0]) if picks else None,
        "top2": [_slim(p) for p in picks[:2]],
        "evaluated": False,
        "results": None,
    }
    rows = [r for r in _read_ledger() if r.get("scan_date") != date]  # idempotent
    rows.append(row)
    rows.sort(key=lambda r: r["scan_date"])
    _write_ledger(rows)

    art = PROJECT_ROOT / "outputs" / date / f"top1_paper_watchlist_{date}.json"
    art.write_text(json.dumps({
        "date": date, "sleeve": "top1_paper", "paper_only": True,
        "status": "PAPER_TRACK_ONLY", "note": "top-1/day concentration A/B vs live top-2 + CSI300",
        "regime": row["regime"], "top1": row["top1"], "top2": row["top2"],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Recorded top-1 paper for %s (%d live picks).", date, len(picks))
    return 0


def _simulate_bracket(df: pd.DataFrame, scan_date: str, cap: float,
                      stop: float, target: float, hold: int) -> dict | None:
    """Conservative bracket from next open after scan_date. Stop wins same-bar ties."""
    df = df.rename(columns={c: c.lower() for c in df.columns})
    df = df.sort_index()
    fwd = df[df.index > pd.Timestamp(scan_date)]
    if len(fwd) < 1:
        return None
    entry_date = fwd.index[0]
    entry_open = float(fwd.iloc[0]["open"])
    if cap and entry_open > float(cap):
        return {"filled": False, "ret_pct": 0.0, "entry_date": str(entry_date.date()),
                "exit_date": str(entry_date.date()), "reason": "no_fill_chase"}
    window = fwd.iloc[:hold]
    for dt, bar in window.iterrows():
        if float(bar["low"]) <= stop:
            return {"filled": True, "ret_pct": (stop / entry_open - 1) * 100,
                    "entry_date": str(entry_date.date()), "exit_date": str(dt.date()), "reason": "stop"}
        if float(bar["high"]) >= target:
            return {"filled": True, "ret_pct": (target / entry_open - 1) * 100,
                    "entry_date": str(entry_date.date()), "exit_date": str(dt.date()), "reason": "target"}
    last = window.iloc[-1]
    return {"filled": True, "ret_pct": (float(last["close"]) / entry_open - 1) * 100,
            "entry_date": str(entry_date.date()), "exit_date": str(window.index[-1].date()), "reason": "hold_expiry"}


def _csi_return(csi: pd.DataFrame, entry_date: str, exit_date: str) -> float | None:
    if csi is None or csi.empty:
        return None
    c = csi.rename(columns={col: col.lower() for col in csi.columns}).sort_index()
    try:
        o = float(c[c.index >= pd.Timestamp(entry_date)].iloc[0]["open"])
        x = float(c[c.index <= pd.Timestamp(exit_date)].iloc[-1]["close"])
    except (IndexError, KeyError):
        return None
    return (x / o - 1) * 100


def evaluate(date: str) -> int:
    rows = _read_ledger()
    asof = pd.Timestamp(date)
    pending = []
    for r in rows:
        if r.get("evaluated") or not r.get("top2"):
            continue
        hold = int((r["top1"] or {}).get("holding_period", 5) or 5)
        # matured if enough trading room has passed (approx: hold+2 calendar-safe = hold*2+4 days)
        if asof < pd.Timestamp(r["scan_date"]) + pd.Timedelta(days=hold * 2 + 5):
            continue
        pending.append(r)
    if not pending:
        logger.info("No matured, unevaluated rows as of %s.", date)
        return 0

    tickers = sorted({p["ticker"] for r in pending for p in r["top2"]})
    start = (pd.Timestamp(min(r["scan_date"] for r in pending)) - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
    end = date
    data, _ = download_daily_range(
        tickers=tickers + [CSI300],
        start=start,
        end=end,
        provider_config={
            "primary": "tushare",
            "backup": "akshare",
            "tushare_token_env": "TUSHARE_TOKEN",
        },
    )
    csi = data.get(CSI300, pd.DataFrame())

    for r in pending:
        legs = []
        for rank, p in enumerate(r["top2"]):
            df = data.get(p["ticker"], pd.DataFrame())
            sim = _simulate_bracket(df, r["scan_date"], p.get("max_entry_price"),
                                    float(p["stop_loss"]), float(p["target_1"]),
                                    int(p.get("holding_period", 5) or 5)) if not df.empty else None
            if sim:
                legs.append({"rank": rank, "ticker": p["ticker"], "entry_date": sim["entry_date"],
                             "exit_date": sim["exit_date"], "ret_pct": round(sim["ret_pct"], 2),
                             "filled": sim["filled"], "reason": sim["reason"]})
        if not legs:
            continue  # data gap; retry next run
        csi_ret = _csi_return(csi, legs[0]["entry_date"], legs[0]["exit_date"])
        r["results"] = {
            "legs": legs,                                  # per-position, for capital-constrained sim
            "top1_ret_pct": legs[0]["ret_pct"],            # rank-0 pick
            "top2_ret_pct": round(sum(l["ret_pct"] for l in legs) / len(legs), 2),
            "csi300_ret_pct": round(csi_ret, 2) if csi_ret is not None else None,
            "entry_date": legs[0]["entry_date"], "exit_date": legs[0]["exit_date"],
        }
        r["evaluated"] = True
    _write_ledger(rows)
    logger.info("Evaluated %d matured rows as of %s.", len(pending), date)
    return 0


def _portfolio_sim(legs: list[dict], pos_pct: float = 0.20, max_conc: int = 5) -> dict:
    """Capital-constrained event-driven sim (fixed fraction/position, concurrency cap).

    Fair across top-1 (fewer positions) and top-2 (more): identical sizing, so the
    difference is pure selection — same methodology as the validated lite sim.
    """
    legs = [l for l in legs if l.get("filled") and l.get("entry_date")]
    if not legs:
        return {"trades": 0, "cum_return_pct": 0.0, "win_pct": None, "max_dd_pct": None}
    dates = sorted({d for l in legs for d in (l["entry_date"], l["exit_date"])})
    by_entry: dict[str, list] = {}
    for l in legs:
        by_entry.setdefault(l["entry_date"], []).append(l)
    cash = 1.0
    open_pos: list[dict] = []
    curve, wins, taken = [], [], 0
    for day in dates:
        open_pos_next = []
        for pos in open_pos:
            if pos["exit_date"] == day:
                cash += pos["alloc"] * (1 + pos["ret_pct"] / 100.0)
                wins.append(pos["ret_pct"] > 0)
            else:
                open_pos_next.append(pos)
        open_pos = open_pos_next
        eq_now = cash + sum(p["alloc"] for p in open_pos)
        for l in by_entry.get(day, []):
            if len(open_pos) >= max_conc:
                break
            alloc = pos_pct * eq_now
            if cash >= alloc:
                cash -= alloc
                open_pos.append({"alloc": alloc, "exit_date": l["exit_date"], "ret_pct": l["ret_pct"]})
                taken += 1
        curve.append(cash + sum(p["alloc"] for p in open_pos))
    peak, dd = curve[0], 0.0
    for v in curve:
        peak = max(peak, v)
        dd = max(dd, 1 - v / peak)
    return {"trades": taken, "cum_return_pct": round((curve[-1] - 1) * 100, 1),
            "win_pct": round(sum(wins) / len(wins) * 100, 0) if wins else None,
            "max_dd_pct": round(dd * 100, 1)}


def _benchmark_stats(returns: list[float]) -> dict:
    """CSI300 buy-hold benchmark over the same windows (avg per-window + win)."""
    if not returns:
        return {"windows": 0, "avg_window_ret_pct": None, "win_pct": None}
    return {"windows": len(returns), "avg_window_ret_pct": round(sum(returns) / len(returns), 2),
            "win_pct": round(sum(1 for r in returns if r > 0) / len(returns) * 100, 0)}


def report(alert: bool = False) -> int:
    rows = [r for r in _read_ledger() if r.get("evaluated") and r.get("results", {}).get("legs")]
    rows.sort(key=lambda r: r["results"]["entry_date"])
    top1_legs = [r["results"]["legs"][0] for r in rows if r["results"]["legs"]]
    top2_legs = [l for r in rows for l in r["results"]["legs"][:2]]
    csi = [r["results"]["csi300_ret_pct"] for r in rows if r["results"].get("csi300_ret_pct") is not None]
    summary = {
        "generated": "paper A/B — top-1 (challenger) vs top-2 (live) vs CSI300",
        "evaluated_days": len(rows),
        "sizing": "capital-constrained, 20%/position, max 5 concurrent (identical across tracks)",
        "top1": _portfolio_sim(top1_legs), "top2_live": _portfolio_sim(top2_legs),
        "csi300_benchmark": _benchmark_stats(csi),
        "note": "PAPER ONLY. Promote top-1 to live only after it beats top-2 AND CSI300 net of costs.",
    }
    SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if alert:
        cfg = AlertConfig(enabled=True, channels=["telegram"])
        if cfg.telegram_bot_token and cfg.telegram_chat_id:
            t1, t2, c = summary["top1"], summary["top2_live"], summary["csi300_benchmark"]
            msg = "\n".join([
                "<b>🧪 Top-1/day 纸面A/B — 累计 (20%/仓, 资金约束)</b>",
                "状态: <b>PAPER ONLY / 不进实盘</b>",
                f"评估天数: {summary['evaluated_days']}",
                f"<b>Top-1 (挑战)</b>: 累计{t1['cum_return_pct']}% 胜率{t1['win_pct']}% 回撤{t1['max_dd_pct']}%",
                f"<b>Top-2 (现行)</b>: 累计{t2['cum_return_pct']}% 胜率{t2['win_pct']}% 回撤{t2['max_dd_pct']}%",
                f"<b>CSI300</b>(基准): 均窗{c['avg_window_ret_pct']}% 胜率{c['win_pct']}%",
                "论点: 集中到top-1 应降回撤; 升实盘前须同时跑赢top-2与CSI300。",
            ])
            AlertManager(cfg).send_alert(title="Top-1 paper A/B", message=msg,
                                         data={"days": summary["evaluated_days"]}, priority="low")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Top-1/day paper track (A/B vs live top-2 + CSI300)")
    ap.add_argument("mode", choices=["record", "evaluate", "report"])
    ap.add_argument("--date", default=pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d"))
    ap.add_argument("--alert", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.mode == "record":
        return record(args.date)
    if args.mode == "evaluate":
        return evaluate(args.date)
    return report(alert=args.alert)


if __name__ == "__main__":
    raise SystemExit(main())
