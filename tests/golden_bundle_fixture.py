"""Deterministic sealed input bundle used by the bundle-mode CLI positive control."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


GOLDEN_BUNDLE_ID = "golden-mr-liveness-v1"
GOLDEN_TICKER = "000001.SZ"
CSI300_TICKER = "000300.SH"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _composite(files: dict[str, str]) -> str:
    payload = "".join(f"{path}  {digest}\n" for path, digest in sorted(files.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _prices(closes: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": closes,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": 1_000_000,
        },
        index=closes.index,
    )


def make_golden_mr_bundle(tmp_path: Path) -> Path:
    """Build a local, hash-locked bundle with one RSI(2) MR signal on 2025-12-31."""
    bundle = tmp_path / "golden-bundle"
    prices_dir = bundle / "prices"
    prices_dir.mkdir(parents=True)

    dates = pd.bdate_range(end="2026-01-06", periods=214)
    signal_date = pd.Timestamp("2025-12-31")
    # A liquid long-term uptrend followed by three modest down days: price remains
    # above SMA200 while RSI(2), streak, and 5-day-low gates all signal exactly once.
    stock_closes = pd.Series(range(len(dates)), index=dates, dtype=float) * 0.1 + 100.0
    stock_closes.loc[pd.Timestamp("2025-12-29")] = 118.0
    stock_closes.loc[pd.Timestamp("2025-12-30")] = 116.0
    stock_closes.loc[signal_date] = 114.0
    # The next session opens within the generated no-chase ceiling, so the
    # positive control is an evaluated pick rather than a skipped candidate.
    stock_closes.loc[pd.Timestamp("2026-01-01")] = 114.0
    stock_closes.loc[pd.Timestamp("2026-01-02")] = 116.0
    stock_closes.loc[pd.Timestamp("2026-01-05")] = 117.0
    stock_closes.loc[pd.Timestamp("2026-01-06")] = 118.0
    csi_closes = pd.Series(range(len(dates)), index=dates, dtype=float) * 0.2 + 4000.0

    (bundle / "universe.csv").write_text(
        "ticker,name,board,is_st,cap_rank_at_freeze\n000001.SZ,Golden,main,false,1\n",
        encoding="utf-8",
    )
    (bundle / "basic_info.json").write_text(
        json.dumps({GOLDEN_TICKER: {"name_cn": "Golden", "industry": "Test", "is_st": False}}),
        encoding="utf-8",
    )
    _prices(stock_closes).to_csv(prices_dir / f"{GOLDEN_TICKER}.csv", index_label="Date", float_format="%.2f")
    _prices(csi_closes).to_csv(prices_dir / f"{CSI300_TICKER}.csv", index_label="Date", float_format="%.2f")

    files = {
        "universe.csv": _sha256(bundle / "universe.csv"),
        "basic_info.json": _sha256(bundle / "basic_info.json"),
        f"prices/{GOLDEN_TICKER}.csv": _sha256(prices_dir / f"{GOLDEN_TICKER}.csv"),
        f"prices/{CSI300_TICKER}.csv": _sha256(prices_dir / f"{CSI300_TICKER}.csv"),
    }
    manifest = {
        "bundle_id": GOLDEN_BUNDLE_ID,
        "created_at": "2026-07-18T00:00:00Z",
        "freeze_tool_commit": "golden-fixture",
        "pit_grade": False,
        "universe_count": 1,
        "files": files,
        "composite_sha256": _composite(files),
        "notes": "Deterministic local MR positive-control; NOT historical PIT.",
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return bundle
