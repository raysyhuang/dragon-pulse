#!/usr/bin/env python3
"""Compare score-floor filters from saved Dragon Pulse backtest detail files."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_LABELS = [
    "mr_new_default_1y",
    "mr_new_default_3y",
    "mr_new_default_5y",
]
DEFAULT_THRESHOLDS = [0, 75, 80, 85, 90, 95]


def parse_number_list(value: str) -> list[float]:
    return [float(part.strip()) for part in value.split(",") if part.strip()]


def load_detail(out_dir: Path, label: str) -> pd.DataFrame:
    path = out_dir / f"backtest_detail_{label}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing detail file: {path}")

    df = pd.read_csv(path)
    required = {"date", "score", "pnl_pct", "exit_reason", "hit_target", "hit_stop"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {', '.join(sorted(missing))}")

    df = df.copy()
    df["pnl_pct"] = pd.to_numeric(df["pnl_pct"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    return df[df["pnl_pct"].notna() & df["score"].notna()].copy()


def summarize_threshold(label: str, df: pd.DataFrame, threshold: float) -> dict:
    subset = df[df["score"] >= threshold].copy()
    total = len(subset)
    if total == 0:
        return {
            "label": label,
            "score_floor": threshold,
            "picks": 0,
            "active_days": 0,
            "win_rate": 0.0,
            "target_hit_rate": 0.0,
            "stop_hit_rate": 0.0,
            "hold_expired_rate": 0.0,
            "expectancy_pct": 0.0,
            "median_pct": 0.0,
            "daily_cumulative_pct": 0.0,
            "daily_max_drawdown_pct": 0.0,
        }

    daily_returns = subset.groupby("date")["pnl_pct"].mean() / 100.0
    equity = (1.0 + daily_returns).cumprod()
    drawdown = (equity - equity.cummax()) / equity.cummax()

    return {
        "label": label,
        "score_floor": threshold,
        "picks": total,
        "active_days": int(subset["date"].nunique()),
        "win_rate": float((subset["pnl_pct"] > 0).mean()),
        "target_hit_rate": float(subset["hit_target"].astype(bool).mean()),
        "stop_hit_rate": float(subset["hit_stop"].astype(bool).mean()),
        "hold_expired_rate": float((subset["exit_reason"] == "hold_expired").mean()),
        "expectancy_pct": float(subset["pnl_pct"].mean()),
        "median_pct": float(subset["pnl_pct"].median()),
        "daily_cumulative_pct": float((equity.iloc[-1] - 1.0) * 100.0),
        "daily_max_drawdown_pct": float(-drawdown.min() * 100.0),
    }


def format_markdown(summary: pd.DataFrame) -> str:
    display = summary.copy()
    pct_cols = [
        "win_rate",
        "target_hit_rate",
        "stop_hit_rate",
        "hold_expired_rate",
    ]
    for col in pct_cols:
        display[col] = (display[col] * 100).round(1).astype(str) + "%"
    for col in [
        "expectancy_pct",
        "median_pct",
        "daily_cumulative_pct",
        "daily_max_drawdown_pct",
    ]:
        display[col] = display[col].round(2)

    warning = (
        "> ⚠️ **Diagnostic only — not promotion-grade.** This filters saved trades "
        "post-hoc, so it cannot model how a higher floor changes which picks the engine "
        "selects on competitive days, acceptance-mode transitions, or capital allocation "
        "across days. Empirically the filter overstated the score≥90 candidate's 3Y cum "
        "return by ~4x vs a true paired rerun (see commit history 2026-04-23). Use a "
        "full backtest with the candidate config before promoting any floor.\n\n"
    )
    return "# Score Floor Comparison\n\n" + warning + display.to_markdown(index=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare score floors from saved backtest detail CSVs."
    )
    parser.add_argument("--out-dir", default="outputs/backtest")
    parser.add_argument("--labels", default=",".join(DEFAULT_LABELS))
    parser.add_argument(
        "--thresholds",
        default=",".join(str(value) for value in DEFAULT_THRESHOLDS),
    )
    parser.add_argument("--output-label", default="score_floor_comparison")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    labels = [part.strip() for part in args.labels.split(",") if part.strip()]
    thresholds = parse_number_list(args.thresholds)

    rows = []
    for label in labels:
        detail = load_detail(out_dir, label)
        for threshold in thresholds:
            rows.append(summarize_threshold(label, detail, threshold))

    summary = pd.DataFrame(rows)
    csv_path = out_dir / f"{args.output_label}.csv"
    md_path = out_dir / f"{args.output_label}.md"
    summary.to_csv(csv_path, index=False)
    md_path.write_text(format_markdown(summary), encoding="utf-8")

    print(f"Wrote {csv_path}")
    print(f"Wrote {md_path}")
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
