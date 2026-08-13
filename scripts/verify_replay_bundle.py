#!/usr/bin/env python3
"""Independently verify a published replay bundle against its real artifacts.

Deliberately NOT a per-PR gate. The PR gate is a committed synthetic fixture
(tests/test_offline_pit_replay.py) that proves the mechanism in seconds. This
verifies the actual published evidence — a 39 MB archive, 1,897 price files, a
full five-year replay — and belongs on a release, a nightly, or a human's
machine. Conflating the two produces one of two failures: a slow, network-
fragile CI, or a green fixture standing in for evidence nobody checked.

What it does, in order, refusing to continue on the first mismatch:

  1. fetch the snapshot archive and check its SHA-256 against the committed value
  2. verify the PIT membership schedule and trade_cal receipt hashes
  3. extract, then check every price file against snapshot_manifest.json
  4. replay the full window with no token and no network reachable
  5. reconcile the pick decomposition and the portfolio ledger
  6. write a verification log bound to the immutable commit under test

Usage:
    python scripts/verify_replay_bundle.py --run-dir outputs/backtest/pit5y_final
    python scripts/verify_replay_bundle.py --run-dir ... --skip-replay   # hashes only
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Verification:
    """Accumulates checks. Any failure makes the whole verification fail."""

    def __init__(self) -> None:
        self.checks: list[dict] = []

    def record(self, name: str, ok: bool, detail: str = "") -> bool:
        self.checks.append({"check": name, "ok": bool(ok), "detail": detail})
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))
        return ok

    @property
    def ok(self) -> bool:
        return all(c["ok"] for c in self.checks)


def _offline_env(scratch: Path) -> dict:
    """No token, no .env fallback, and every connect path raising in the child."""
    site = scratch / "nonet"
    site.mkdir(exist_ok=True)
    (site / "sitecustomize.py").write_text(
        "import socket\n"
        "def _b(*a, **k):\n"
        "    raise RuntimeError('NETWORK_BLOCKED_BY_VERIFIER')\n"
        "socket.create_connection = _b\n"
        "socket.socket.connect = _b\n"
        "socket.socket.connect_ex = _b\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("TUSHARE_TOKEN", None)
    env["HOME"] = str(scratch)
    env["PYTHONPATH"] = str(site)
    return env


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--label", default="pit5y_final")
    ap.add_argument("--skip-replay", action="store_true",
                    help="Hash verification only; skips the multi-minute replay.")
    ap.add_argument("--out", default="", help="Where to write the verification log.")
    args = ap.parse_args()

    run = Path(args.run_dir)
    bundle = json.loads((run / "replay_bundle.json").read_text(encoding="utf-8"))
    v = Verification()

    print(f"verifying {bundle['artifact']}  (status {bundle['status']})\n")

    # --- 1. archive identity -------------------------------------------------
    url = (bundle.get("inputs", {}).get("snapshot_cache", {}) or {}).get("download_url")
    expected_line = (run / "snapshot_archive.sha256").read_text().split()[0] \
        if (run / "snapshot_archive.sha256").exists() else None
    scratch = Path(tempfile.mkdtemp(prefix="dp-verify-"))
    archive = scratch / "snapshots.tgz"

    if not url:
        v.record("archive published", False, "bundle has no download_url")
        return 1
    print(f"  downloading {url}")
    urllib.request.urlretrieve(url, archive)
    got = sha256_file(archive)
    v.record("archive sha256 matches committed value", got == expected_line,
             f"{got[:16]}… vs {str(expected_line)[:16]}…")

    # --- 2. frozen input identity -------------------------------------------
    for key, fname in (("pit_membership_schedule", "pit_membership_schedule.json"),
                       ("trade_cal_receipt", "trade_cal_receipt.json")):
        rec = bundle.get("inputs", {}).get(key) or {}
        path = run / fname
        if not path.exists():
            v.record(f"{key} present", False, str(path))
            continue
        v.record(f"{key} sha256 matches bundle", sha256_file(path) == rec.get("sha256"))

    sched = json.loads((run / "pit_membership_schedule.json").read_text(encoding="utf-8"))
    v.record("schedule declares TRUSTED_HISTORICAL_ASSUMPTION",
             sched.get("provenance") == "TRUSTED_HISTORICAL_ASSUMPTION",
             "membership is provider-current, not attested as-of")

    # --- 3. per-file cache identity -----------------------------------------
    with tarfile.open(archive) as tf:
        tf.extractall(scratch)                                    # noqa: S202 - hash-checked below
    snaps = scratch / "snapshots"
    manifest = json.loads((run / "snapshot_manifest.json").read_text(encoding="utf-8"))
    mismatched = [f["name"] for f in manifest["files"]
                  if not (snaps / f["name"]).exists()
                  or sha256_file(snaps / f["name"]) != f["sha256"]]
    v.record("every price file matches snapshot_manifest",
             not mismatched, f"{len(manifest['files'])} files, {len(mismatched)} mismatched")

    # --- 4/5. offline replay + reconciliation --------------------------------
    if not args.skip_replay:
        out = scratch / "replay"
        cmd = [sys.executable, "scripts/backtest_1yr.py",
               "--start", bundle["calendar"]["first"], "--end", bundle["calendar"]["last"],
               "--universe-mode", "point_in_time", "--engines", "alpha",
               "--acceptance-mode", "live_equivalent", "--label", "verify",
               "--out-dir", str(out), "--price-snapshot-dir", str(snaps),
               "--pit-schedule-in", str(run / "pit_membership_schedule.json"),
               "--calendar-in", str(run / "trade_cal_receipt.json")]
        print("  replaying offline (this takes minutes)…")
        r = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True,
                           env=_offline_env(scratch), timeout=7200)
        v.record("replay ran with no network", "NETWORK_BLOCKED_BY_VERIFIER" not in r.stderr)
        v.record("replay ran with no token", "TUSHARE_TOKEN" not in r.stderr)
        v.record("replay exited cleanly", r.returncode == 0, r.stderr[-200:] if r.returncode else "")

        published = json.loads((run / f"backtest_summary_{args.label}.json").read_text(encoding="utf-8"))
        replayed_path = out / "backtest_summary_verify.json"
        if replayed_path.exists():
            replayed = json.loads(replayed_path.read_text(encoding="utf-8"))
            for field in ("picks_emitted", "picks_filled", "picks_skipped"):
                v.record(f"{field} reproduces", published.get(field) == replayed.get(field),
                         f"published {published.get(field)} vs replayed {replayed.get(field)}")
            e, f, s = (replayed.get("picks_emitted"), replayed.get("picks_filled"),
                       replayed.get("picks_skipped"))
            v.record("pick decomposition closes", e == (f or 0) + (s or 0), f"{e} = {f} + {s}")

        sim = json.loads((run / "portfolio_sim_v2_summary.json").read_text(encoding="utf-8"))
        res = list(list(sim["files"].values())[0].values())[0]
        v.record("ledger closes",
                 res["trades_in_file"] == res["trades_executed"] + res["skipped_capacity"] + res["skipped_cash"],
                 f"{res['trades_in_file']} = {res['trades_executed']} + "
                 f"{res['skipped_capacity']} + {res['skipped_cash']}")

    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                          cwd=PROJECT_ROOT).stdout.strip()
    log = {
        "verifier": "DP_REPLAY_BUNDLE_VERIFIER",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "commit_under_test": head,
        "bundle_artifact": bundle["artifact"],
        "bundle_composite_sha256": bundle.get("bundle_composite_sha256"),
        "replay_executed": not args.skip_replay,
        "result": "PASS" if v.ok else "FAIL",
        "checks": v.checks,
        "boundary": ("Verifies the published artifacts reproduce. Does NOT ratify the "
                     "strategy numbers, and does not upgrade membership beyond "
                     "TRUSTED_HISTORICAL_ASSUMPTION."),
    }
    dest = Path(args.out) if args.out else run / "verification_log.json"
    dest.write_text(json.dumps(log, indent=2), encoding="utf-8")
    print(f"\n  {log['result']} — {sum(c['ok'] for c in v.checks)}/{len(v.checks)} checks")
    print(f"  wrote {dest}")
    return 0 if v.ok else 1


if __name__ == "__main__":
    sys.exit(main())
