#!/usr/bin/env python3
"""Paper sleeve for the one confirmed edge: ChiNext 50/200 trend timing.

Records, daily and append-only, exactly the rule studied in
docs/research/2026-08-10-edge-test-trend-timing.md. It places no orders, sends no
alerts, and touches no selector, cron, or production artifact.

THE RULE (identical to the backtest, deliberately):
    signal[t] = close[t] > SMA50[t] > SMA200[t]
    position held over session t+1 = signal[t]
The signal is computed from a completed session's close and governs the NEXT session,
so the live sleeve and the backtest cannot diverge in their timing convention.

Ledger discipline: append-only, one row per session, idempotent by trade_date. A rerun
never rewrites history; it only adds sessions that are not already recorded. Every row
carries its evidence labels so a row read in isolation cannot be mistaken for execution.

Usage:
    python scripts/chinext_timing_paper_sleeve.py            # update ledger to latest session
    python scripts/chinext_timing_paper_sleeve.py --status   # print current state, no writes
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timedelta, timezone

import hashlib
from zoneinfo import ZoneInfo

import pandas as pd
import requests


class ProviderError(RuntimeError):
    """The provider failed. Distinct from a market holiday or from stale data."""


_CAPTURE: dict[str, str] = {}

ROOT = pathlib.Path(__file__).resolve().parent.parent
LEDGER = ROOT / "outputs" / "paper_lab" / "chinext_timing_paper_ledger.jsonl"
INCEPTION_FILE = ROOT / "outputs" / "paper_lab" / "chinext_timing_paper_inception.txt"
CAPTURE_FILE = ROOT / "outputs" / "paper_lab" / "chinext_timing_last_capture.json"
MARKER = ROOT / "outputs" / "paper_lab" / "chinext_timing_state.json"
TS_CODE = "399006.SZ"
FAST, SLOW = 50, 200
SIDE_BPS = 5.0
CASH_ANNUAL = 0.018
TRADING_DAYS = 243.0
EVIDENCE_LABEL = "RESEARCH_ONLY_NON_BINDING"
EXECUTION_STATUS = "PAPER_ONLY_NO_ORDERS"
SLEEVE = "chinext_50_200_trend_timing"


def _load_env() -> None:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            m = re.match(r"^\s*([A-Z_]+)\s*=\s*(.*?)\s*$", line)
            if m:
                os.environ.setdefault(m.group(1), m.group(2).strip("\"'"))


def fetch_index() -> pd.DataFrame:
    """Full ChiNext history. Uses the REST endpoint directly; the tushare package is optional."""
    _load_env()
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set")

    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    frames, digests = [], []
    # Windows are CLOSED at today's Shanghai date: an open-ended or future end date
    # would make the query non-reproducible and could admit unsettled rows.
    for start, end in (("20100101", "20180101"), ("20180101", "20260101"), ("20260101", today)):
        body = {"api_name": "index_daily", "token": token,
                "params": {"ts_code": TS_CODE, "start_date": start, "end_date": end},
                "fields": "trade_date,open,close"}
        raw = requests.post("https://api.tushare.pro", timeout=60, json=body).content
        digests.append(hashlib.sha256(raw).hexdigest())
        try:
            resp = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProviderError(f"unparseable response for {start}..{end}: {exc}") from exc
        if not isinstance(resp, dict) or resp.get("code") != 0:
            raise ProviderError(f"tushare error for {start}..{end}: "
                                f"{resp.get('msg') if isinstance(resp, dict) else 'bad shape'}")
        data = resp.get("data")
        if (not isinstance(data, dict) or not isinstance(data.get("items"), list)
                or not isinstance(data.get("fields"), list)
                or not {"trade_date", "open", "close"}.issubset(data["fields"])):
            raise ProviderError(f"malformed response shape for {start}..{end}")
        frames.append(pd.DataFrame(data["items"], columns=data["fields"]))
    _CAPTURE["response_sha256"] = hashlib.sha256("".join(digests).encode()).hexdigest()
    _CAPTURE["captured_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _CAPTURE["query_end_date"] = today
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df["close"] = df["close"].astype(float)
    df["open"] = df["open"].astype(float)
    df = df.sort_values("trade_date").drop_duplicates("trade_date").reset_index(drop=True)
    # Never admit a bar for a session that has not closed. A manual pre-close dispatch
    # would otherwise append an incomplete intraday bar as if it were final.
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    if now.hour < 15:
        df = df[df["trade_date"] < pd.Timestamp(now.date())].reset_index(drop=True)
    return df


def build_rows(df: pd.DataFrame, inception: str | None = None) -> list[dict]:
    """One row per session, with the position that the PRIOR session's signal dictates.

    `inception` is the LAST SESSION OBSERVED at first run. Rows up to and including it
    are BACKFILLED_FROM_HISTORY — a reconstruction. Only sessions appended AFTER that
    point are FORWARD_PAPER. This repository's evidence hierarchy ranks live above replay
    above reconstruction, so a backfilled row must never read as forward evidence, and
    the inception session itself is backfill: it was reconstructed, not tracked.
    """
    c, o = df["close"], df["open"]
    sma_f, sma_s = c.rolling(FAST).mean(), c.rolling(SLOW).mean()
    signal = ((c > sma_f) & (sma_f > sma_s))
    pos = signal.shift(1).astype(float)              # held over the NEXT session
    ret = c.pct_change()
    turn = pos.diff().abs()
    # EXECUTABLE FILL: on the day a position opens you earn open->close, not the full
    # close-to-close move, because the order can only be placed after the signal.
    entering = ((pos == 1) & (pos.shift(1) == 0)).astype(float)
    entry_adj = entering * ((c / o - 1) - ret)
    cash_daily = CASH_ANNUAL / TRADING_DAYS
    net = (pos * ret + entry_adj + (1 - pos) * cash_daily
           - turn * (SIDE_BPS / 10_000.0))
    position = signal.shift(1)

    rows, equity = [], 1.0
    for i in range(len(df)):
        if pd.isna(sma_s.iloc[i]) or pd.isna(position.iloc[i]) or pd.isna(net.iloc[i]):
            continue
        equity *= (1 + float(net.iloc[i]))
        date_str = df["trade_date"].iloc[i].strftime("%Y-%m-%d")
        rows.append({
            "sleeve": SLEEVE,
            "record_origin": ("FORWARD_PAPER" if inception and date_str > inception
                              else "BACKFILLED_FROM_HISTORY"),
            "trade_date": date_str,
            "close": round(float(c.iloc[i]), 4),
            "sma_fast": round(float(sma_f.iloc[i]), 4),
            "sma_slow": round(float(sma_s.iloc[i]), 4),
            "signal_today": bool(signal.iloc[i]),
            "position_held_today": bool(position.iloc[i]),
            "position_next_session": bool(signal.iloc[i]),
            "traded_today": bool(turn.iloc[i] and turn.iloc[i] > 0),
            "net_return": round(float(net.iloc[i]), 8),
            "paper_equity": round(equity, 8),
            "rule": f"close > SMA{FAST} > SMA{SLOW}; signal[t] governs session t+1",
            "fill_convention": "entry earns open->close on the switching session",
            "capture_sha256": _CAPTURE.get("response_sha256", ""),
            "cost_bps_per_side": SIDE_BPS,
            "cash_annual_rate": CASH_ANNUAL,
            "evidence_label": EVIDENCE_LABEL,
            "execution_status": EXECUTION_STATUS,
        })
    return rows


def _atomic_write(path: pathlib.Path, data: str) -> None:
    """Durable single-file replace: temp, fsync, rename, fsync directory."""
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def ledger_state() -> dict:
    """Observable state of the ledger, for comparison against the commit marker."""
    if not LEDGER.exists():
        return {"rows": 0, "latest": None}
    lines = [l for l in LEDGER.read_text().splitlines() if l.strip()]
    return {"rows": len(lines),
            "latest": json.loads(lines[-1])["trade_date"] if lines else None}


def check_marker() -> None:
    """Detect a partially completed previous run.

    Each file is individually durable, but three files cannot be replaced in one atomic
    step. Rather than claim atomicity we do not have, the run records a commit marker
    LAST; a marker that disagrees with the ledger means the previous run died mid-write,
    and that must be reconciled by a human rather than silently appended to.
    """
    if not MARKER.exists():
        return
    want = json.loads(MARKER.read_text())
    have = ledger_state()
    if want.get("ledger") != have:
        raise SystemExit(
            f"previous run did not complete: marker records {want.get('ledger')} but the "
            f"ledger shows {have}. Inspect and reconcile before rerunning.")


def _fsync_dir(path: pathlib.Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def existing_rows() -> dict[str, dict]:
    if not LEDGER.exists():
        return {}
    out: dict[str, dict] = {}
    for n, line in enumerate(LEDGER.read_text().splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        missing = [k for k in ROW_SCHEMA if k not in row]
        if missing:
            raise SystemExit(f"ledger row {n} does not match the row schema "
                             f"(missing {', '.join(missing)}); refusing to append to a "
                             f"ledger whose rows cannot be fully compared")
        day = row["trade_date"]
        if day in out and out[day] != row:
            raise SystemExit(f"ledger already contains conflicting rows for {day}; "
                             f"refusing to append to a corrupt ledger")
        out[day] = row
    return out


# Every field a row carries. A recorded row must match this schema exactly: a partial
# row would let missing fields skip comparison, so corruption could evade detection.
ROW_SCHEMA = ("sleeve", "record_origin", "trade_date", "close", "sma_fast", "sma_slow",
              "signal_today", "position_held_today", "position_next_session",
              "traded_today", "net_return", "paper_equity", "rule", "fill_convention",
              "capture_sha256", "cost_bps_per_side", "cash_annual_rate",
              "evidence_label", "execution_status")
# Compared on re-observation. capture_sha256 is excluded because the provider response
# digest legitimately differs between runs; everything else must be reproducible.
_COMPARED = tuple(k for k in ROW_SCHEMA if k != "capture_sha256")


def conflicts(existing: dict, fresh: dict) -> list[str]:
    """Fields that changed for a date already recorded. A restated close is a real event
    and must surface, not be silently discarded by a date-only dedupe.

    A field MISSING from the recorded row counts as a conflict rather than being skipped;
    otherwise a truncated row would silently pass every comparison.
    """
    return [k for k in _COMPARED if k not in existing or existing[k] != fresh.get(k)]


def latest_expected_session(token: str) -> str | None:
    """Most recent CLOSED trading session in Shanghai, from the exchange calendar.

    Without this, a market holiday, a provider outage and silently stale data all look
    identical: zero new rows. They are different events and must be reported differently.
    """
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    today = now.strftime("%Y%m%d")
    body = {"api_name": "trade_cal", "token": token,
            "params": {"exchange": "SZSE",   # ChiNext is a Shenzhen index
                       "start_date": (now.replace(day=1) - timedelta(days=40)).strftime("%Y%m%d"),
                       "end_date": today},
            "fields": "cal_date,is_open"}
    resp = requests.post("https://api.tushare.pro", timeout=60, json=body).json()
    if resp.get("code") != 0:
        raise ProviderError(f"trade_cal failed: {resp.get('msg')}")
    data = resp.get("data")
    if (not isinstance(data, dict) or not isinstance(data.get("items"), list)
            or not isinstance(data.get("fields"), list)):
        raise ProviderError("malformed trade_cal response shape")
    try:
        opens = sorted(r[0] for r in data["items"] if int(r[1]) == 1)
    except (TypeError, ValueError, IndexError) as exc:
        raise ProviderError(f"unusable trade_cal rows: {exc}") from exc
    if not opens:
        # Syntactically valid but empty: we cannot tell a holiday from an outage, so we
        # must not write. Returning None here previously still appended and exited 0.
        raise ProviderError("trade_cal returned no open sessions in the lookback window")
    # A session only counts as closed once the 15:00 Shanghai close has passed.
    if opens[-1] == today and now.hour < 15:
        opens = opens[:-1]
    return opens[-1] if opens else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--status", action="store_true", help="print state without writing")
    args = ap.parse_args()

    _load_env()
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set")

    # Corrupt or partial state must fail BEFORE any provider call: cheaper, and it keeps
    # a broken ledger from being masked by an unrelated network failure.

    # Corrupt or partial state must fail BEFORE any provider call: cheaper, and it keeps
    # a broken ledger from being masked by an unrelated network failure.
    check_marker()

    try:
        expected = latest_expected_session(token)
        frame = fetch_index()
    except (ProviderError, requests.RequestException) as exc:
        print(f"status              PROVIDER_ERROR\n  {exc}")
        return 2

    have_before = existing_rows()
    inception = INCEPTION_FILE.read_text().strip() if INCEPTION_FILE.exists() else None
    rows = build_rows(frame, inception=inception)
    if not rows:
        print("status              PROVIDER_ERROR\n  no rows produced")
        return 2
    latest = rows[-1]
    provider_latest = latest["trade_date"].replace("-", "")

    print(f"ChiNext 50/200 trend-timing paper sleeve — {EVIDENCE_LABEL} / {EXECUTION_STATUS}")
    print(f"  latest session      {latest['trade_date']}   close {latest['close']:.2f}")
    print(f"  SMA{FAST} {latest['sma_fast']:.2f}   SMA{SLOW} {latest['sma_slow']:.2f}")
    print(f"  signal today        {'BULL (in)' if latest['signal_today'] else 'no signal (cash)'}")
    print(f"  position NEXT sess. {'HOLD ChiNext ETF' if latest['position_next_session'] else 'CASH'}")
    trailing = rows[-243:] if len(rows) >= 243 else rows
    print(f"  trailing 1y exposure {sum(r['position_held_today'] for r in trailing)/len(trailing):.0%}"
          f"   paper equity since {rows[0]['trade_date']}: {latest['paper_equity']:.3f}x")
    print(f"  capture             {_CAPTURE.get('captured_at')} "
          f"sha256={_CAPTURE.get('response_sha256', '')[:16]} "
          f"query_end={_CAPTURE.get('query_end_date')}")

    # freshness: holiday, stale provider, or current — three distinct outcomes
    if expected is None:
        print("status              PROVIDER_ERROR\n  calendar unavailable; refusing to write")
        return 2
    if provider_latest >= expected:
        state = "CURRENT"
    else:
        state = "STALE_PROVIDER_DATA"
    print(f"  expected session    {expected}   provider latest {provider_latest}   -> {state}")

    conflicting = {}
    for r in rows:
        prior = have_before.get(r["trade_date"])
        if prior:
            bad = conflicts(prior, r)
            if bad:
                conflicting[r["trade_date"]] = bad
    if conflicting:
        for day, fields in sorted(conflicting.items())[:5]:
            print(f"  CONFLICT {day}: {', '.join(fields)} differ from the recorded row")
        print("::error::provider values changed for sessions already recorded; "
              "refusing to append. Inspect and reconcile before rerunning.")
        return 3

    if args.status:
        return 0 if state != "STALE_PROVIDER_DATA" else 1

    new = [r for r in rows if r["trade_date"] not in have_before]
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    if inception is None:
        _atomic_write(INCEPTION_FILE, latest["trade_date"] + "\n")
        print(f"  inception observed at {latest['trade_date']}; sessions AFTER it are FORWARD_PAPER")
    # Durability contract, stated exactly: each file is individually durable (fsync on
    # file and directory), but three files CANNOT be replaced in one atomic step. A crash
    # between them leaves partial state. That is why the commit marker is written LAST and
    # checked FIRST: partial state is detectable and fails closed on the next run, rather
    # than being silently appended to.
    with LEDGER.open("a", encoding="utf-8") as fh:
        for r in new:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    _fsync_dir(LEDGER.parent)
    _atomic_write(CAPTURE_FILE, json.dumps(
        {**_CAPTURE, "ts_code": TS_CODE, "latest_session": latest["trade_date"],
         "expected_session": expected, "freshness": state,
         "rows_appended": len(new), "ledger_rows": len(have_before) + len(new),
         "evidence_label": EVIDENCE_LABEL, "execution_status": EXECUTION_STATUS},
        indent=2, sort_keys=True) + "\n")
    # marker LAST: its presence and agreement is what makes a completed run identifiable
    _atomic_write(MARKER, json.dumps(
        {"ledger": ledger_state(), "inception": inception or latest["trade_date"],
         "capture_sha256": _CAPTURE.get("response_sha256", ""),
         "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
        indent=2, sort_keys=True) + "\n")

    forward = sum(1 for r in rows if r["record_origin"] == "FORWARD_PAPER")
    print(f"  ledger {LEDGER.relative_to(ROOT)}: +{len(new)} rows "
          f"(total {len(have_before) + len(new)}; forward-paper {forward})")

    if state == "STALE_PROVIDER_DATA":
        print(f"::error::provider data is stale: expected a session at {expected}, "
              f"provider's latest is {provider_latest}. This is NOT a market holiday.")
        return 1
    if not new:
        print("  no new session (market closed or already recorded) — not an error")
    return 0


if __name__ == "__main__":
    sys.exit(main())
