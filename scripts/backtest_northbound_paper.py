#!/usr/bin/env python3
"""Deterministic replay for the northbound active-rank paper sleeve.

This script consumes a cached event file produced by the June 2026 northbound
research pass and replays the current paper rule without touching live Dragon
Pulse ranking or execution artifacts.

Rule of record:
- Source: hsgt_top10 active list / turnover rank, not confirmed net-buy flow.
- Conservative T+1 no-chase fill already encoded by `cons_fill`.
- 5D bracket replay return already encoded by `bracket_ret` / `bracket_reason`.
- Current paper sleeve: rank <= 5 and stop-risk between 4% and 7%.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EVENTS = Path("/tmp/dp_alpha_event_search_20260629/northbound_bracket_replay/northbound_bracket_events.csv")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return None


def _profit_factor(returns: pd.Series) -> float:
    gains = returns[returns > 0].sum()
    losses = -returns[returns < 0].sum()
    if losses <= 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def summarize(label: str, frame: pd.DataFrame) -> dict:
    ret = pd.to_numeric(frame["bracket_ret"], errors="coerce").dropna()
    if ret.empty:
        return {
            "label": label,
            "events": int(len(frame)),
            "n": 0,
            "wr": 0.0,
            "avg": 0.0,
            "med": 0.0,
            "pf": 0.0,
            "p10": 0.0,
            "p90": 0.0,
            "target_rate": 0.0,
            "stop_rate": 0.0,
            "time_rate": 0.0,
        }
    reasons = frame.loc[ret.index, "bracket_reason"].astype(str)
    return {
        "label": label,
        "events": int(len(frame)),
        "n": int(len(ret)),
        "wr": round(float((ret > 0).mean() * 100), 2),
        "avg": round(float(ret.mean()), 3),
        "med": round(float(ret.median()), 3),
        "pf": round(_profit_factor(ret), 3),
        "p10": round(float(ret.quantile(0.10)), 3),
        "p90": round(float(ret.quantile(0.90)), 3),
        "target_rate": round(float((reasons == "target").mean() * 100), 2),
        "stop_rate": round(float((reasons == "stop").mean() * 100), 2),
        "time_rate": round(float((reasons == "time").mean() * 100), 2),
    }


def add_variant_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    rank = pd.to_numeric(df.get("rank"), errors="coerce")
    stop_risk = pd.to_numeric(df.get("stop_risk"), errors="coerce")
    change = pd.to_numeric(df.get("change"), errors="coerce")
    ret20 = pd.to_numeric(df.get("ret20"), errors="coerce")
    ret60 = pd.to_numeric(df.get("ret60"), errors="coerce")
    cons_fill = df.get("cons_fill", True)
    if not isinstance(cons_fill, pd.Series):
        cons_fill = pd.Series(True, index=df.index)
    cons_fill = cons_fill.astype(bool)

    top5 = cons_fill & rank.le(5)
    masks: dict[str, pd.Series] = {
        "nb_top5": top5,
        "top5_lowrisk_4_7": top5 & stop_risk.ge(4.0) & stop_risk.le(7.0),
        "top5_lowrisk_lt4": top5 & stop_risk.lt(4.0),
        "top5_change_pos": top5 & change.gt(0),
        "top5_change_neg": top5 & change.lt(0),
        "top5_rs20_pos": top5 & ret20.gt(0),
        "top5_rs60_pos": top5 & ret60.gt(0),
    }

    # One-pick-per-day variants: pick the lowest stop-risk name per signal date.
    for base_label in ["nb_top5", "top5_lowrisk_4_7", "top5_lowrisk_lt4", "top5_rs20_pos", "top5_change_pos", "top5_change_neg"]:
        base = df[masks[base_label]].copy()
        if base.empty:
            masks[f"{base_label}_top1_lowrisk"] = pd.Series(False, index=df.index)
            continue
        selected = (
            base.assign(_stop_risk=stop_risk.loc[base.index])
            .sort_values(["trade_date", "_stop_risk", "rank", "ts_code"])
            .groupby("trade_date", as_index=False)
            .head(1)
            .index
        )
        masks[f"{base_label}_top1_lowrisk"] = df.index.isin(selected)
    return masks


def _parse_trade_dates(values) -> pd.Series:
    """Parse Tushare-style YYYYMMDD trade dates without nanosecond misparse."""
    return pd.to_datetime(values.astype(str).str.replace(r"\.0$", "", regex=True), format="%Y%m%d", errors="coerce")


def split_summaries(df: pd.DataFrame, mask: pd.Series, label: str) -> list[dict]:
    selected = df[mask].copy()
    if selected.empty:
        return []
    selected["trade_date"] = _parse_trade_dates(selected["trade_date"])
    selected = selected.dropna(subset=["trade_date"]).sort_values("trade_date")
    dates = selected["trade_date"]
    start = dates.min().strftime("%Y-%m-%d")
    end = dates.max().strftime("%Y-%m-%d")
    out: list[dict] = []
    full = summarize(label, selected)
    full.update({"split": "full", "start": start, "end": end})
    out.append(full)

    # Chronological 70/30 split by date boundary, not row index, so one signal
    # date cannot leak into both train and holdout.
    unique_dates = sorted(selected["trade_date"].drop_duplicates().tolist())
    cut_date_idx = int(len(unique_dates) * 0.70)
    if cut_date_idx > 0 and cut_date_idx < len(unique_dates):
        cut_date = unique_dates[cut_date_idx]
        train = selected[selected["trade_date"] < cut_date]
        holdout = selected[selected["trade_date"] >= cut_date]
        for split, part in [("train_70pct", train), ("holdout_30pct", holdout)]:
            if part.empty:
                continue
            row = summarize(label, part)
            row.update({
                "split": split,
                "start": part["trade_date"].min().strftime("%Y-%m-%d"),
                "end": part["trade_date"].max().strftime("%Y-%m-%d"),
            })
            out.append(row)

    selected["year_month"] = selected["trade_date"].dt.strftime("%Y-%m")
    for month, part in selected.groupby("year_month"):
        row = summarize(label, part)
        row.update({"split": f"month_{month}", "start": month, "end": month})
        out.append(row)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Northbound active-rank paper sleeve bracket backtest")
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS, help="Cached northbound bracket events CSV")
    parser.add_argument("--outdir", type=Path, default=PROJECT_ROOT / "outputs" / "research" / "northbound_backtest")
    parser.add_argument("--label", default=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    args = parser.parse_args()

    if not args.events.exists():
        raise SystemExit(f"Missing events CSV: {args.events}")

    df = pd.read_csv(args.events)
    required = {"trade_date", "ts_code", "rank", "cons_fill", "stop_risk", "bracket_ret", "bracket_reason"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"Events CSV missing required columns: {missing}")

    masks = add_variant_masks(df)
    summary_rows = [summarize(label, df[mask]) for label, mask in masks.items()]
    summary = pd.DataFrame(summary_rows).sort_values(["avg", "pf", "wr"], ascending=[False, False, False])

    current_label = "top5_lowrisk_4_7"
    capacity_label = "top5_lowrisk_4_7_top1_lowrisk"
    split_rows = split_summaries(df, masks[current_label], current_label)
    split_rows.extend(split_summaries(df, masks[capacity_label], capacity_label))
    splits = pd.DataFrame(split_rows)

    outdir = args.outdir / args.label
    outdir.mkdir(parents=True, exist_ok=True)
    summary_path = outdir / "northbound_paper_backtest_summary.csv"
    splits_path = outdir / "northbound_paper_backtest_splits.csv"
    events_path = outdir / "northbound_paper_current_rule_events.csv"
    manifest_path = outdir / "manifest.json"

    summary.to_csv(summary_path, index=False)
    splits.to_csv(splits_path, index=False)
    df[masks[current_label]].to_csv(events_path, index=False)

    current_daily_counts = df[masks[current_label]].groupby("trade_date").size()
    manifest = {
        "generated_utc": utc_now_iso(),
        "git_commit": git_commit(),
        "source_events": str(args.events),
        "source_events_rows": int(len(df)),
        "source_events_sha256": file_sha256(args.events),
        "outdir": str(outdir),
        "rule_of_record": current_label,
        "capacity_realistic_rule": capacity_label,
        "paper_only": True,
        "n_variants_tested": int(len(summary)),
        "selection_bias_warning": "Rule/variant metrics are event-study outputs; current rule was selected after variant search, so full-sample and holdout numbers are not promotion evidence.",
        "concurrency_warning": "Rule-of-record takes every matching name and can hold overlapping 5D positions; use capacity_realistic_rule for one-pick-per-day sanity.",
        "mean_events_per_signal_day": round(float(current_daily_counts.mean()), 3) if not current_daily_counts.empty else 0.0,
        "max_events_per_signal_day": int(current_daily_counts.max()) if not current_daily_counts.empty else 0,
        "data_label": "active-rank/turnover only; not confirmed northbound net-buy flow",
        "summary_csv": str(summary_path),
        "splits_csv": str(splits_path),
        "current_rule_events_csv": str(events_path),
        "current_rule_summary": summary[summary["label"] == current_label].iloc[0].to_dict(),
        "current_rule_holdout": splits[(splits["label"] == current_label) & (splits["split"] == "holdout_30pct")].iloc[0].to_dict() if not splits.empty else {},
        "capacity_rule_summary": summary[summary["label"] == capacity_label].iloc[0].to_dict(),
        "capacity_rule_holdout": splits[(splits["label"] == capacity_label) & (splits["split"] == "holdout_30pct")].iloc[0].to_dict() if not splits.empty else {},
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {summary_path}")
    print(f"Wrote {splits_path}")
    print(f"Wrote {events_path}")
    print(f"Wrote {manifest_path}")
    print("\nTop variants:")
    print(summary.head(12).to_string(index=False))
    print("\nRule-of-record splits:")
    print(splits.head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
