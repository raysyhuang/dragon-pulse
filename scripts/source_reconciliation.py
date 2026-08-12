#!/usr/bin/env python3
"""Cross-check Tushare against iFinD so a corrupt feed cannot pass as a quiet day.

Dragon Pulse's recurring failure mode is not a wrong number — it is a wrong
number that looks like silence. The akshare outage, the deleted TUSHARE_TOKEN,
and 31 sessions of a northbound sleeve returning exit 0 all presented as
"nothing to report". A second, independent provider is the cheapest way to tell
a quiet market from a broken pipe.

This is a HEALTH CHECK, not a data source. Nothing here feeds the scanner:
Tushare stays canonical, and a divergence means "go look", not "use iFinD".

Four hazards were measured on the iFinD MCP before this was written, and each
one is defended against explicitly rather than trusted away:

1. TICKER RESOLUTION IS UNSTABLE. The same natural-language query returned
   000300.SH once and 399300.SZ minutes later. Codes are pinned in the request
   AND validated on the response against a declared alias set. A code outside
   the set fails; it is never accepted because the price happened to match.
2. NON-TRADING DAYS ARE FABRICATED. iFinD emits forward-filled rows for
   weekends carrying the previous session's close. Its 交易日期 field ECHOES
   the requested date, so it cannot attest to anything on its own — verified
   live: asking for Saturday 2026-08-08 returns Friday's 4694.4365 stamped
   20260808. The real defence is ordering: the session is resolved from settled
   Tushare bars, and Tushare is queried first, so a fabricated iFinD bar has
   nothing to be compared against and the row fails before it can agree.
3. PRICES ARE UNADJUSTED BY DEFAULT (复权方式: 不复权). Tushare's `daily.close`
   is likewise unadjusted, so the comparison is like-for-like. Never compare
   these against an adjusted series — every ex-dividend date would fire.
4. NUMBERS ARRIVE AS PROSE. Values come back inside a markdown table, with
   Chinese magnitude units (1.6832万亿). Parsing is explicit and fails loudly.

Exit codes: 0 only when every instrument was fetched from both sources and
agreed. Any unavailability, mismatch, staleness or parse failure is non-zero.

Usage:
    python scripts/source_reconciliation.py                 # last trading day
    python scripts/source_reconciliation.py --date 2026-08-11
    python scripts/source_reconciliation.py --json out.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

IFIND_BASE = "https://api-mcp.51ifind.com:8643/ds-mcp-servers/"

# Named deliberately. A pass means four anchor instruments' unadjusted EOD closes
# matched — nothing about financials, adjusted series, full-market coverage, or
# whether iFinD could substitute for Tushare. The label travels with the artifact
# so downstream readers cannot quietly upgrade it into "data verified".
CHECK_NAME = "SOURCE_RECONCILIATION_HEALTHCHECK_NON_BINDING"

# Instruments to reconcile. Index first — it is the regime gate's own input, so
# a divergence there changes what the scanner does. The equities widen coverage
# enough to catch provider-level corruption that a single symbol would miss.
#
# `aliases` exists because 000300.SH and 399300.SZ are the same index carried on
# two exchanges. Treating them as equivalent is a deliberate, reviewable decision
# recorded here — not a silent acceptance at comparison time.
@dataclass(frozen=True)
class Instrument:
    code: str
    name: str
    kind: str  # "index" | "equity"
    aliases: tuple[str, ...] = ()

    @property
    def accepted_codes(self) -> set[str]:
        return {self.code, *self.aliases}


INSTRUMENTS = (
    Instrument("000300.SH", "沪深300", "index", aliases=("399300.SZ",)),
    Instrument("600519.SH", "贵州茅台", "equity"),
    Instrument("000001.SZ", "平安银行", "equity"),
    Instrument("601318.SH", "中国平安", "equity"),
)

# 2dp rounding on a ~¥10 stock is 5e-4 relative, so a pure relative tolerance
# would be too tight for cheap names and a pure absolute one too loose for the
# index. Take whichever is larger.
REL_TOL = 5e-4
ABS_TOL = 0.01


class ReconError(Exception):
    """Typed failure. Every path that cannot produce a comparison raises."""

    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


# --------------------------------------------------------------------------
# Credentials
# --------------------------------------------------------------------------

def _secret(name: str) -> str:
    val = os.environ.get(name)
    if val:
        return val.strip()
    env = PROJECT_ROOT / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.strip().startswith(f"{name}="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise ReconError("CONFIG", f"{name} not set")


# --------------------------------------------------------------------------
# Tushare (canonical)
# --------------------------------------------------------------------------

def tushare_call(token: str, api: str, params: dict, fields: str = "") -> list[dict]:
    body = json.dumps({"api_name": api, "token": token, "params": params, "fields": fields}).encode()
    req = urllib.request.Request(
        "http://api.tushare.pro", data=body, headers={"Content-Type": "application/json"}
    )
    try:
        r = json.loads(urllib.request.urlopen(req, timeout=60).read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ReconError("TUSHARE_UNAVAILABLE", f"{api}: {type(exc).__name__}") from exc
    if r.get("code") != 0:
        raise ReconError("TUSHARE_ERROR", f"{api}: {r.get('msg')}")
    d = r["data"]
    return [dict(zip(d["fields"], row)) for row in d["items"]]


def tushare_close(token: str, inst: Instrument, yyyymmdd: str) -> float:
    """Unadjusted close, matching iFinD's 不复权 default."""
    api = "index_daily" if inst.kind == "index" else "daily"
    rows = tushare_call(token, api, {"ts_code": inst.code, "trade_date": yyyymmdd}, "ts_code,trade_date,close")
    if not rows:
        raise ReconError("TUSHARE_NO_DATA", f"{inst.code} has no bar on {yyyymmdd}")
    row = rows[0]
    if row["trade_date"] != yyyymmdd:
        raise ReconError("TUSHARE_STALE", f"{inst.code} returned {row['trade_date']}, asked {yyyymmdd}")
    return float(row["close"])


def resolve_session(token: str, on_or_before: str) -> str:
    """Newest session that actually has a settled bar, not the newest calendar session.

    The trading calendar marks today open from the moment it starts, but the EOD
    bar does not exist until after the close. Anchoring on the calendar makes a
    pre-close run report a total blackout across every instrument — a false
    alarm indistinguishable from the real outage this script hunts for. Ask the
    data what the last settled session was instead.
    """
    anchor = INSTRUMENTS[0]
    rows = tushare_call(
        token, "index_daily" if anchor.kind == "index" else "daily",
        {"ts_code": anchor.code, "start_date": _shift_days(on_or_before, -20), "end_date": on_or_before},
        "trade_date,close",
    )
    days = sorted(r["trade_date"] for r in rows)
    if not days:
        raise ReconError("TUSHARE_NO_DATA", f"{anchor.code} has no bar in the 20d before {on_or_before}")
    return days[-1]


def _shift_days(yyyymmdd: str, delta: int) -> str:
    import datetime as _dt
    d = _dt.date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8])) + _dt.timedelta(days=delta)
    return d.strftime("%Y%m%d")


# --------------------------------------------------------------------------
# iFinD (independent check)
# --------------------------------------------------------------------------

_MAGNITUDE = (("万亿", 1e12), ("亿", 1e8), ("万", 1e4))


def parse_cn_number(raw: str) -> float:
    """Turn iFinD's prose numerals into a float. '1.6832万亿' -> 1.6832e12."""
    s = str(raw).strip().replace(",", "").replace("￥", "").replace("¥", "")
    if not s or s in {"-", "—", "\t"}:
        raise ReconError("PARSE_FAILURE", f"empty numeric cell: {raw!r}")
    for suffix, mult in _MAGNITUDE:
        if s.endswith(suffix):
            head = s[: -len(suffix)]
            try:
                return float(head) * mult
            except ValueError as exc:
                raise ReconError("PARSE_FAILURE", f"bad number {raw!r}") from exc
    try:
        return float(s)
    except ValueError as exc:
        raise ReconError("PARSE_FAILURE", f"bad number {raw!r}") from exc


def parse_markdown_table(text: str) -> list[dict]:
    """iFinD answers are markdown tables; pull them into dicts."""
    rows = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("|")]
    cells = [[c.strip() for c in ln.strip("|").split("|")] for ln in rows]
    cells = [c for c in cells if not all(set(x) <= {"-", ""} for x in c)]
    if len(cells) < 2:
        raise ReconError("PARSE_FAILURE", "no markdown table in iFinD answer")
    header, *body = cells
    return [dict(zip(header, r)) for r in body if len(r) == len(header)]


class IFindClient:
    def __init__(self, token: str, timeout: int = 90):
        self.token = token
        self.timeout = timeout

    def _post(self, url: str, payload: dict, sid: str | None = None):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": self.token,
        }
        if sid:
            headers["Mcp-Session-Id"] = sid
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers)
        # verify defaults to on: the iFinD certificate validates, and the bundled
        # vendor client's verify=False would ship this bearer token unprotected.
        r = urllib.request.urlopen(req, timeout=self.timeout)
        return r.status, dict(r.headers), r.read().decode("utf-8", "replace")

    @staticmethod
    def _decode(body: str) -> dict:
        if "data:" in body[:200]:
            for line in body.splitlines():
                if line.startswith("data:"):
                    try:
                        return json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        continue
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ReconError("IFIND_BAD_RESPONSE", body[:120]) from exc

    def call(self, server: str, tool: str, arguments: dict) -> dict:
        url = IFIND_BASE + server
        try:
            status, headers, _ = self._post(url, {
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                           "clientInfo": {"name": "dp-reconciliation", "version": "1.0"}},
            })
            if status != 200:
                raise ReconError("IFIND_UNAVAILABLE", f"initialize HTTP {status}")
            sid = headers.get("Mcp-Session-Id") or headers.get("mcp-session-id")
            self._post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, sid)
            _, _, body = self._post(url, {
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            }, sid)
        except ReconError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ReconError("IFIND_UNAVAILABLE", f"{type(exc).__name__}: {exc}") from exc

        result = (self._decode(body).get("result") or {})
        text = "".join(c.get("text", "") for c in (result.get("content") or []))
        if not text:
            raise ReconError("IFIND_BAD_RESPONSE", "empty tool content")
        try:
            outer = json.loads(text)
            inner = outer.get("data")
            payload = json.loads(inner) if isinstance(inner, str) else inner
        except (json.JSONDecodeError, AttributeError) as exc:
            raise ReconError("IFIND_BAD_RESPONSE", text[:120]) from exc
        if not isinstance(payload, dict) or "answer" not in payload:
            raise ReconError("IFIND_BAD_RESPONSE", str(payload)[:120])
        return payload


def ifind_close(client: IFindClient, inst: Instrument, yyyymmdd: str) -> tuple[float, str]:
    """Fetch the unadjusted close, verifying the code and date that came back.

    ``yyyymmdd`` MUST be a settled Tushare session (see ``resolve_session``).
    iFinD will happily invent a bar for any date handed to it, so this function
    cannot be used to discover whether a session exists — only to cross-check a
    session Tushare has already settled.
    """
    server, tool = (
        ("hexin-ifind-ds-index-mcp", "index_data") if inst.kind == "index"
        else ("hexin-ifind-ds-stock-mcp", "get_stock_performance")
    )
    date_cn = f"{yyyymmdd[:4]}年{int(yyyymmdd[4:6])}月{int(yyyymmdd[6:8])}日"
    payload = client.call(server, tool, {"query": f"{inst.code} 在 {date_cn} 的收盘价（不复权）"})

    rows = parse_markdown_table(payload["answer"])
    code_key = next((k for k in rows[0] if "代码" in k), None)
    close_key = next((k for k in rows[0] if "收盘" in k), None)
    if not code_key or not close_key:
        raise ReconError("PARSE_FAILURE", f"missing 代码/收盘价 columns: {list(rows[0])}")

    matched = [r for r in rows if r[code_key].strip() in inst.accepted_codes]
    if not matched:
        got = sorted({r[code_key].strip() for r in rows})
        raise ReconError(
            "CODE_MISMATCH",
            f"asked {inst.code} (accept {sorted(inst.accepted_codes)}), iFinD returned {got}",
        )
    returned_code = matched[0][code_key].strip()

    # The date lives in indicators_params, not the table. Treat it as a weak
    # signal only: iFinD echoes whatever date was requested, so a match proves
    # nothing (verified live against a Saturday). It still catches the case where
    # iFinD answers with a *different* date than asked. Absence is a hard failure.
    params = payload.get("indicators_params") or {}
    stamped = None
    for key, meta in params.items():
        if "收盘" in key and isinstance(meta, dict):
            stamped = meta.get("交易日期")
            break
    if stamped is None:
        raise ReconError("IFIND_UNDATED", f"{inst.code}: no 交易日期 in indicators_params")
    if str(stamped) != yyyymmdd:
        raise ReconError("IFIND_STALE", f"{inst.code}: iFinD dated {stamped}, asked {yyyymmdd}")

    return parse_cn_number(matched[0][close_key]), returned_code


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------

def within_tolerance(a: float, b: float) -> bool:
    return abs(a - b) <= max(ABS_TOL, REL_TOL * max(abs(a), abs(b)))


@dataclass
class Row:
    code: str
    name: str
    kind: str
    tushare: float | None = None
    ifind: float | None = None
    ifind_code: str | None = None
    agree: bool = False
    failure: str | None = None
    detail: str | None = None


def reconcile(date: str, tushare_token: str, ifind_token: str) -> tuple[list[Row], str]:
    yyyymmdd = date.replace("-", "")
    session = resolve_session(tushare_token, yyyymmdd)
    client = IFindClient(ifind_token)

    rows: list[Row] = []
    for inst in INSTRUMENTS:
        row = Row(code=inst.code, name=inst.name, kind=inst.kind)
        try:
            row.tushare = tushare_close(tushare_token, inst, session)
            row.ifind, row.ifind_code = ifind_close(client, inst, session)
            row.agree = within_tolerance(row.tushare, row.ifind)
            if not row.agree:
                row.failure = "DIVERGENCE"
                row.detail = (
                    f"tushare={row.tushare} ifind={row.ifind} "
                    f"diff={abs(row.tushare - row.ifind):.4f}"
                )
        except ReconError as exc:
            row.failure, row.detail = exc.kind, exc.detail
        rows.append(row)
        time.sleep(0.2)
    return rows, session


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--json", default=None, help="artifact path")
    args = ap.parse_args()

    import datetime as dt
    asof = args.date or dt.date.today().strftime("%Y-%m-%d")

    try:
        tushare_token = _secret("TUSHARE_TOKEN")
        ifind_token = _secret("IFIND_API_KEY")
    except ReconError as exc:
        print(f"FAIL {exc}")
        return 2

    try:
        rows, session = reconcile(asof, tushare_token, ifind_token)
    except ReconError as exc:
        print(f"FAIL {exc}")
        return 2

    print(f"{CHECK_NAME}\nsession {session} (asof {asof})")
    print(f"{'code':<12}{'name':<10}{'tushare':>12}{'ifind':>12}{'':>3}{'note'}")
    for r in rows:
        ts = f"{r.tushare:.4f}" if r.tushare is not None else "—"
        fi = f"{r.ifind:.4f}" if r.ifind is not None else "—"
        mark = "OK " if r.agree else "!! "
        note = ""
        if r.failure:
            note = f"{r.failure}: {r.detail}"
        elif r.ifind_code and r.ifind_code != r.code:
            note = f"alias accepted: iFinD returned {r.ifind_code}"
        print(f"{r.code:<12}{r.name:<10}{ts:>12}{fi:>12}{mark:>3}{note}")

    agreed = sum(1 for r in rows if r.agree)
    failed = [r for r in rows if not r.agree]
    print(f"\n{agreed}/{len(rows)} instruments agreed")

    artifact = {
        "check": CHECK_NAME,
        "binding": False,
        "asof": asof,
        "session": session,
        "tolerance": {"relative": REL_TOL, "absolute": ABS_TOL},
        "agreed": agreed,
        "total": len(rows),
        "rows": [asdict(r) for r in rows],
    }
    dest = Path(args.json) if args.json else PROJECT_ROOT / "outputs" / asof / f"source_reconciliation_{asof}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(artifact, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {dest}")

    if failed:
        # Never exit 0 on partial coverage. An unreachable provider is an
        # unchecked pipeline, which is the state this script exists to surface.
        print(f"\nFAILED: {', '.join(f'{r.code}[{r.failure}]' for r in failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
