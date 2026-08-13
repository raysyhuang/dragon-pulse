#!/usr/bin/env python3
"""Package a backtest replay so a clean checkout can recompute it, not just read it.

Three replays of the same window this week produced three different answers,
each of which looked finished. The first was starved of CSI300 history and
reported four bull years as calm. The second iterated weekdays and emitted 40
picks on days the exchange was shut. The third is believed correct — and that
belief is worth exactly nothing while the evidence sits untracked on one laptop.

A composite hash proves two people hold the same cache. It does not give the
second person the cache. So this bundle records identity for everything that
shaped the numbers AND states plainly where the inputs can be fetched; if they
cannot be fetched, it labels itself VERIFY_ARTIFACT_ONLY_NOT_RECOMPUTABLE rather
than implying a reproducibility it does not have.

Usage:
    python scripts/build_replay_bundle.py --run-dir outputs/backtest/pit5y_final \\
        --label pit5y_final --cache-url <release-asset-url>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HASH_SPEC = ("sha256 over file bytes; a composite is sha256 over the newline-joined "
             "'name:sha256' pairs of its members, sorted by name")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def composite(pairs: dict[str, str]) -> str:
    return hashlib.sha256(
        "\n".join(f"{k}:{v}" for k, v in sorted(pairs.items())).encode()
    ).hexdigest()


def snapshot_manifest(snap_dir: Path) -> dict:
    """Per-file identity: a bare composite cannot tell you which file drifted."""
    files, hashes = [], {}
    for p in sorted(snap_dir.glob("*.csv")):
        try:
            df = pd.read_csv(p, usecols=["Date"])
            lo, hi, rows = (df.Date.min(), df.Date.max(), len(df)) if len(df) else ("", "", 0)
        except Exception:
            lo, hi, rows = "", "", -1
        h = sha256_file(p)
        hashes[p.name] = h
        files.append({"name": p.name, "rows": rows, "first": lo, "last": hi, "sha256": h})
    return {
        "count": len(files),
        "schema": ["Date", "Open", "High", "Low", "Close", "Volume"],
        "composite_sha256": composite(hashes),
        "files": files,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--cache-url", default="",
                    help="Where the snapshot archive can be downloaded. Without it the "
                         "bundle is verifiable but not recomputable, and says so.")
    args = ap.parse_args()

    run = Path(args.run_dir)
    lab = args.label
    daily = run / f"backtest_daily_{lab}.csv"
    detail = run / f"backtest_detail_{lab}.csv"
    summary = run / f"backtest_summary_{lab}.json"

    outputs = {}
    for p in (daily, detail, summary, run / "picks.json"):
        if p.exists():
            outputs[p.name] = sha256_file(p)
    for p in sorted(run.glob("*equity_curve*.csv")) + [run / "portfolio_sim_v2_summary.json"]:
        if p.exists():
            outputs[p.name] = sha256_file(p)

    sessions = sorted(pd.read_csv(daily)["date"].astype(str).tolist())
    snaps = snapshot_manifest(run / "snapshots")

    bundle = {
        "artifact": f"DP_REPLAY_BUNDLE_{lab.upper()}",
        "status": "RESEARCH_ONLY_NON_BINDING",
        "ratified": False,
        "recomputable": bool(args.cache_url) or "VERIFY_ARTIFACT_ONLY_NOT_RECOMPUTABLE",
        "claim_boundary": [
            "Corrected-but-unattested diagnostic measurement.",
            "Signal generation IS now offline-reproducible from the frozen schedule, "
            "calendar receipt and price cache; membership correctness remains a "
            "TRUSTED_HISTORICAL_ASSUMPTION, not an attested fact.",
            "Supports only: the live 30-session drought has historical precedent.",
            "Does NOT update the playbook, endorse the annualised return, alter the "
            "selector, or reopen the PR #16 breadth conclusion.",
        ],
        "hash_algorithm": HASH_SPEC,
        # The live-acquisition form was published here, which would send a reviewer
        # to a provider and rebuild the universe rather than replay it. What follows
        # is the command that actually reproduces this bundle offline.
        "command": (
            f"python scripts/backtest_1yr.py --start 2021-08-12 --end 2026-08-11 "
            f"--universe-mode point_in_time --engines alpha --acceptance-mode live_equivalent "
            f"--label {lab} --out-dir <OUT> --price-snapshot-dir <EXTRACTED>/snapshots "
            f"--pit-schedule-in <BUNDLE>/pit_membership_schedule.json "
            f"--calendar-in <BUNDLE>/trade_cal_receipt.json "
            f"--dump-picks <OUT>/picks.json"
        ),
        "clean_machine_reproduction": [
            "git clone <repo> && git checkout <replay_code_commit>",
            f"curl -L -o snapshots.tgz {args.cache_url or '<cache_url>'}",
            "shasum -a 256 snapshots.tgz   # must equal snapshot_archive.sha256",
            "tar -xzf snapshots.tgz -C <EXTRACTED>",
            "python scripts/verify_replay_bundle.py --run-dir <BUNDLE>   # hashes + offline replay",
            "# or run the command above directly with TUSHARE_TOKEN unset and no network",
        ],
        "sim_command": (
            f"python scripts/portfolio_sim_v2.py {detail} --position-pct 20 --max-concurrent 5"
        ),
        "code": {
            # A bundle cannot name the commit that contains it: writing the bundle
            # creates the commit. Recording HEAD here therefore always lags by one,
            # which is exactly the mismatch review caught (git_base dc5b646 against
            # head edc8a11). The content hashes below are self-consistent and are
            # the authoritative identity; this SHA is the commit whose code RAN the
            # replay, which is a different and weaker claim.
            "identity_note": ("replay_code_commit is the commit whose code produced this "
                              "replay. It is NOT the PR head and cannot be: committing this "
                              "file advances head. Verify against the *_sha256 fields."),
            "authoritative_identity": "runner_sha256 + simulator_sha256 + config_sha256",
            "replay_code_commit": subprocess.run(["git", "rev-parse", "HEAD"],
                                                 capture_output=True, text=True).stdout.strip(),
            "runner_sha256": sha256_file(PROJECT_ROOT / "scripts" / "backtest_1yr.py"),
            "simulator_sha256": sha256_file(PROJECT_ROOT / "scripts" / "portfolio_sim_v2.py"),
            "config_sha256": sha256_file(PROJECT_ROOT / "config" / "default.yaml"),
        },
        "environment": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "platform": platform.platform(),
        },
        "calendar": {
            "source": "tushare trade_cal exchange=SSE is_open=1 over the run window",
            "sessions": len(sessions),
            "first": sessions[0],
            "last": sessions[-1],
            "session_list_sha256": hashlib.sha256("\n".join(sessions).encode()).hexdigest(),
            "receipt_file": "trade_cal_receipt.json",
        },
        "inputs": {
            "pit_membership_schedule": {
                "file": "pit_membership_schedule.json",
                "sha256": sha256_file(run / "pit_membership_schedule.json")
                          if (run / "pit_membership_schedule.json").exists() else None,
                "provenance": "TRUSTED_HISTORICAL_ASSUMPTION",
                "note": ("Membership is provider-CURRENT as-of resolution, not provider-as-of; "
                         "no receipt attests to the ranking on the rebalance date. Freezing makes "
                         "signal generation reproducible, which is weaker than making it correct."),
                "consume_with": "--pit-schedule-in",
            },
            "trade_cal_receipt": {
                "file": "trade_cal_receipt.json",
                "sha256": sha256_file(run / "trade_cal_receipt.json")
                          if (run / "trade_cal_receipt.json").exists() else None,
                "consume_with": "--calendar-in",
            },
            "snapshot_cache": {
                **{k: v for k, v in snaps.items() if k != "files"},
                "download_url": args.cache_url or None,
                "note": ("Per-file manifest is in snapshot_manifest.json. Re-downloading from "
                         "the provider will NOT reproduce these hashes byte-for-byte; only the "
                         "archived cache will."),
            }
        },
        "accounting_semantics": {
            "sizing": "20% of equity per position",
            "max_concurrent": 5,
            "cash_constraint": "allocation capped at available cash; otherwise skipped_cash",
            "marking": "daily mark-to-market on close; halted names carry last_value",
            "exit": "row exit_price at exit_date (target/stop/hold-expiry from the runner)",
            "sharpe": "mean/std of daily equity returns, annualised by sqrt(252)",
            "drawdown": "peak-to-trough on the marked-to-market equity curve",
            "costs": "NOT modelled in this simulator — returns are gross",
        },
        "outputs": outputs,
    }
    bundle["bundle_composite_sha256"] = composite(outputs)

    (run / "snapshot_manifest.json").write_text(json.dumps(snaps, indent=2))
    (run / "replay_bundle.json").write_text(json.dumps(bundle, indent=2))
    print(json.dumps({k: v for k, v in bundle.items() if k not in ("outputs",)}, indent=2)[:1600])
    print(f"\nsnapshot_manifest.json: {snaps['count']} files, composite {snaps['composite_sha256'][:24]}...")
    print(f"replay_bundle.json:     composite {bundle['bundle_composite_sha256'][:24]}...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
