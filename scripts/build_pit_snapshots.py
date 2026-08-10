#!/usr/bin/env python3
"""Fetch dated A-share snapshots that the verified Task 2 builder can consume.

This is the missing upstream link. Tasks 1-2.5 validate supplied evidence; nothing so
far produced any. This emits, per requested trade date, the exact pair those tools
require:

    sources/daily_basic_YYYYMMDD.csv      ts_code,circ_mv,list_date,delist_date
    sources/daily_basic_YYYYMMDD.capture.json   the Task 2.5 capture receipt

SURVIVORSHIP: listing dates are joined from stock_basic across list_status L, D and P,
so delisted and suspended issuers are present. Querying only L would silently drop 339
delisted names (~5.8% of the universe), concentrated in recent years — precisely the
bias measured at 19% of picks in this repository's survivorship audit.

HONESTY OF GRADE: these snapshots are fetched today, not captured on their trade date.
They are therefore TRUSTED_HISTORICAL_ASSUMPTION and can never be OBSERVED_CAPTURE or
PIT_CAPTURE_VERIFIED. That limitation is permanent and is written into every receipt.

Membership contradictions are REPORTED, never silently dropped: if a name traded on a
date its listing record says it could not have, that is a data-quality fact worth seeing.

Usage:
    python scripts/build_pit_snapshots.py --dates 2026-01-05,2026-01-06 --out /tmp/snaps
    python scripts/build_pit_snapshots.py --dates-file dates.txt --out /tmp/snaps
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timezone

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
API = "http://api.tushare.pro"
TRUSTED_CAVEAT = "historical_tushare_trusted_assumption"


def _load_env() -> str:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            m = re.match(r"^\s*([A-Z_]+)\s*=\s*(.*?)\s*$", line)
            if m:
                os.environ.setdefault(m.group(1), m.group(2).strip("\"'"))
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set")
    return token


def api(token: str, name: str, fields: str = "", **params):
    resp = requests.post(API, timeout=90, json={
        "api_name": name, "token": token, "params": params, "fields": fields}).json()
    if resp.get("code") != 0:
        raise SystemExit(f"tushare {name} failed: {resp.get('msg')}")
    data = resp["data"]
    return [dict(zip(data["fields"], row)) for row in data["items"]]


def listing_table(token: str) -> dict[str, tuple[str, str]]:
    """ts_code -> (list_date, delist_date). Includes delisted and paused issuers."""
    table: dict[str, tuple[str, str]] = {}
    counts = {}
    for status in ("L", "D", "P"):
        rows = api(token, "stock_basic", fields="ts_code,list_date,delist_date",
                   list_status=status)
        counts[status] = len(rows)
        for r in rows:
            table[r["ts_code"]] = (r.get("list_date") or "", r.get("delist_date") or "")
    print(f"  listing table: {counts} -> {len(table)} unique issuers", flush=True)
    return table


def iso(compact: str) -> str:
    return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}" if compact else ""


def sha256_file(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_one(token: str, listings, day: str, out_dir: pathlib.Path) -> dict:
    """day is YYYYMMDD."""
    rows = api(token, "daily_basic", fields="ts_code,circ_mv", trade_date=day)
    as_of = iso(day)
    kept, dropped, contradictions = [], 0, []
    for r in rows:
        code, cap = r["ts_code"], r["circ_mv"]
        if cap is None or float(cap) <= 0:
            dropped += 1
            continue
        listed, delisted = listings.get(code, ("", ""))
        if not listed:
            dropped += 1
            continue
        l_iso, d_iso = iso(listed), iso(delisted)
        if l_iso > as_of or (d_iso and d_iso <= as_of):
            contradictions.append((code, l_iso, d_iso))
            continue
        kept.append((code, f"{float(cap):.4f}", l_iso, d_iso))
    kept.sort(key=lambda x: (-float(x[1]), x[0]))

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"daily_basic_{day}.csv"
    csv_path.write_text("ts_code,circ_mv,list_date,delist_date\n"
                        + "".join(",".join(r) + "\n" for r in kept))
    receipt = {
        "schema_version": 1, "provider": "tushare", "endpoint": "daily_basic",
        "requested_trade_date": day, "snapshot_file": csv_path.name,
        "snapshot_sha256": sha256_file(csv_path),
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "provenance_grade": "TRUSTED_HISTORICAL_ASSUMPTION",
        "caveat": TRUSTED_CAVEAT,
    }
    (out_dir / f"daily_basic_{day}.capture.json").write_text(json.dumps(receipt, indent=2))
    return {"day": day, "kept": len(kept), "dropped": dropped,
            "contradictions": contradictions, "raw": len(rows)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dates", help="comma-separated YYYY-MM-DD or YYYYMMDD")
    ap.add_argument("--dates-file", type=pathlib.Path)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    args = ap.parse_args()

    raw = []
    if args.dates:
        raw += [d.strip() for d in args.dates.split(",") if d.strip()]
    if args.dates_file:
        raw += [l.strip() for l in args.dates_file.read_text().splitlines() if l.strip()]
    days = [d.replace("-", "") for d in raw]
    if not days:
        raise SystemExit("no dates given")

    token = _load_env()
    listings = listing_table(token)

    total_contra = 0
    for day in days:
        res = build_one(token, listings, day, args.out)
        total_contra += len(res["contradictions"])
        note = ""
        if res["contradictions"]:
            sample = ", ".join(f"{c}({l}->{d})" for c, l, d in res["contradictions"][:3])
            note = f"  CONTRADICTIONS {len(res['contradictions'])}: {sample}"
        print(f"  {iso(day)}  universe {res['kept']:>5}  "
              f"(raw {res['raw']}, dropped {res['dropped']}){note}", flush=True)

    print(f"\n  wrote {len(days)} snapshot(s) + receipts to {args.out}")
    print(f"  grade TRUSTED_HISTORICAL_ASSUMPTION — fetched now, not captured on the trade date")
    if total_contra:
        print(f"  {total_contra} membership contradiction(s) reported and EXCLUDED from the "
              f"snapshot; they are data-quality facts, not silently dropped rows")
    print("\n  next: python scripts/build_pit_universe_schedule.py \\")
    print(f"          --sources-dir {args.out} --output <bundle> \\")
    print(f"          --as-of-dates {','.join(iso(d) for d in days)} \\")
    print("          --universe-n <N> --source-label tushare_daily_basic")
    return 0


if __name__ == "__main__":
    sys.exit(main())
