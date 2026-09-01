#!/usr/bin/env python3
"""Fail-closed verifier for the committed bull+choppy evidence manifest."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _star_market_tickers(tickers) -> list[str]:
    """Return STAR ordinary shares/CDRs without matching embedded digits."""
    found = []
    for ticker in tickers:
        code = str(ticker).strip().upper().split(".", 1)[0]
        if len(code) == 6 and code[:3] in {"688", "689"} and code.isdigit():
            found.append(str(ticker))
    return found


def _csv_tickers(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as fh:
        return [row["ticker"] for row in csv.DictReader(fh)]


def _git_replay_code_unchanged(root: Path, commit: str, relpaths) -> bool:
    """Require this checkout's replay code to match the pinned replay commit."""
    try:
        subprocess.run(
            ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
            cwd=root,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        result = subprocess.run(
            ["git", "diff", "--quiet", commit, "--", *relpaths],
            cwd=root,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", default=str(Path(__file__).resolve().parent))
    ap.add_argument("--archive", default="")
    args = ap.parse_args()
    run = Path(args.run_dir).resolve()
    root = run.parents[2]
    manifest = json.loads((run / "evidence_manifest.json").read_text(encoding="utf-8"))
    failures: list[str] = []

    def check(ok: bool, label: str) -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {label}")
        if not ok:
            failures.append(label)

    for rel, expected in manifest["outputs_sha256"].items():
        path = run / rel
        check(path.exists() and sha256(path) == expected, f"output hash {rel}")
    for rel, expected in manifest["inputs_sha256"].items():
        path = root / rel
        check(path.exists() and sha256(path) == expected, f"input hash {rel}")
    for rel, expected in manifest["code_sha256"].items():
        path = root / rel
        check(path.exists() and sha256(path) == expected, f"code hash {rel}")
    check(
        _git_replay_code_unchanged(
            root, manifest["replay_code_commit"], manifest["code_sha256"].keys()
        ),
        "checkout replay code matches pinned replay commit",
    )

    summary = json.loads((run / "backtest_summary_pit5y_bull_choppy.json").read_text())
    portfolio = json.loads((run / "portfolio_sim_v2_summary.json").read_text())
    p = portfolio["result"]
    check(summary["picks_emitted"] == summary["picks_filled"] + summary["picks_skipped"],
          "pick decomposition closes")
    check(sum(row["total"] for row in summary["per_regime"].values()) == summary["picks_filled"],
          "per-regime filled decomposition closes")
    check(p["trades_in_file"] == p["trades_executed"] + p["skipped_capacity"] + p["skipped_cash"],
          "portfolio ledger closes")
    check(portfolio["detail_sha256"] == manifest["outputs_sha256"][portfolio["detail_file"]],
          "portfolio detail binding")
    check(portfolio["equity_curve_sha256"] == manifest["outputs_sha256"][portfolio["equity_curve_file"]],
          "portfolio curve binding")
    for name in ("picks.csv", "backtest_detail_pit5y_bull_choppy.csv"):
        check(not _star_market_tickers(_csv_tickers(run / name)),
              f"no STAR Market symbols in {name}")

    sys.path.insert(0, str(root))
    from src.core.config import load_config
    config = load_config(str(root / "config/default.yaml"))
    check(config["alpha_candidates"]["rs_pullback"]["regimes"] == ["bull", "choppy"],
          "engine gate opens only bull+choppy")
    check(config["acceptance"]["excluded_regimes"] == ["bear"],
          "acceptance gate keeps bear blocked")
    check(config["book_size"]["bear"] == {"max_picks": 0, "min_score": 999},
          "book defense keeps bear blocked")

    if args.archive:
        check(sha256(Path(args.archive)) == manifest["snapshot_archive_sha256"],
              "snapshot archive hash")

    print(f"RESULT: {'FAIL' if failures else 'PASS'} ({len(failures)} failures)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
