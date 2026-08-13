"""A committed miniature of the published replay bundle.

The real bundle's price cache is a 39 MB release asset, so a test gated on its
presence skips in CI — and a skipped acceptance gate reads as a passing one,
which is the failure this whole thread has been about. This fixture is small
enough to live in the repo and exercises the same mechanism: a frozen PIT
membership schedule, a frozen trade_cal receipt, and a matching price snapshot
directory, consumed with no provider.

It proves the mechanism, not the published numbers. Verifying the real 1,895
ticker archive is a separate job (scripts/verify_replay_bundle.py) and must not
be conflated with this gate.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

MINI_TICKER = "000001.SZ"
MINI_TICKER_B = "000002.SZ"
CSI300 = "000300.SH"
SIGNAL_DATE = "2025-12-31"


def _ohlcv(closes: pd.Series) -> pd.DataFrame:
    """Snapshot-cache schema: Date,Open,High,Low,Close,Volume."""
    return pd.DataFrame(
        {
            "Open": closes,
            "High": closes + 1.0,
            "Low": closes - 1.0,
            "Close": closes,
            "Volume": 90_000_000.0,
        },
        index=closes.index,
    )


def make_offline_replay_fixture(tmp_path: Path) -> dict[str, Path]:
    """Build snapshots + frozen schedule + frozen calendar for an offline replay.

    The price path mirrors the golden MR fixture: a long uptrend above SMA200 with
    three modest down days, so RSI(2), streak and 5-day-low all fire exactly once
    and the next open sits inside the no-chase ceiling. A fixture that completed
    with zero picks would prove the runner ran, not that signals rebuild.
    """
    root = tmp_path / "offline_fixture"
    snapshots = root / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)

    # LOOKBACK_DAYS is 400 CALENDAR days, and the runner refuses a replay whose
    # cached CSI300 does not span start-400d..end — Neo's coverage guard, working
    # as intended. A 214-session fixture fell short and triggered a refetch, which
    # read as "the replay went to the network". 600 sessions clears it with margin.
    dates = pd.bdate_range(end="2026-01-06", periods=600)
    # Flat then rising, rather than one long ramp. A 600-session ramp lifts SMA200
    # above the pullback and the trend gate stops firing; a flat base keeps SMA200
    # well under the dip so RSI(2) can trigger while price is still in an uptrend.
    closes = pd.Series(100.0, index=dates, dtype=float)
    ramp = pd.Series(range(150), dtype=float) * 0.12 + 100.0
    closes.iloc[-150:] = ramp.to_numpy()
    for day, px in {
        "2025-12-29": 118.0, "2025-12-30": 116.0, SIGNAL_DATE: 114.0,
        "2026-01-01": 114.0, "2026-01-02": 116.0, "2026-01-05": 117.0, "2026-01-06": 118.0,
    }.items():
        closes.loc[pd.Timestamp(day)] = px
    csi = pd.Series(range(len(dates)), index=dates, dtype=float) * 0.2 + 4000.0

    # Two candidates in DIFFERENT sectors. A one-ticker fixture cannot exercise a
    # per-sector cap at all, which is exactly why it stayed green while the real
    # replay was silently halving every day's picks to one.
    _ohlcv(closes).to_csv(snapshots / f"{MINI_TICKER}.csv", index_label="Date", float_format="%.2f")
    _ohlcv(closes * 1.01).to_csv(snapshots / f"{MINI_TICKER_B}.csv", index_label="Date", float_format="%.2f")
    _ohlcv(csi).to_csv(snapshots / f"{CSI300}.csv", index_label="Date", float_format="%.2f")

    # Frozen industry metadata: absent, the sector cap has nothing to enforce on.
    basic_info = root / "basic_info.json"
    basic_info.write_text(json.dumps({
        MINI_TICKER:   {"name_cn": "MiniA", "industry": "SectorAlpha", "is_st": False},
        MINI_TICKER_B: {"name_cn": "MiniB", "industry": "SectorBeta",  "is_st": False},
    }, indent=2, ensure_ascii=False), encoding="utf-8")

    # Frozen membership: one rebalance covering the window, one member.
    schedule = root / "pit_membership_schedule.json"
    schedule.write_text(json.dumps({
        "artifact": "DP_PIT_MEMBERSHIP_SCHEDULE",
        "provenance": "TRUSTED_HISTORICAL_ASSUMPTION",
        "provenance_note": (
            "Membership is provider-CURRENT as-of resolution, not provider-as-of. "
            "Synthetic fixture: membership is asserted, not sourced."
        ),
        "universe_n": 2,
        "rebalance_months": 1,
        "rebalances": [{"date": "2025-01-01", "members": [MINI_TICKER, MINI_TICKER_B]}],
    }, indent=2), encoding="utf-8")

    # Frozen calendar in raw trade_cal response shape, covering the fixture's days.
    cal = root / "trade_cal_receipt.json"
    cal.write_text(json.dumps({
        "code": 0,
        "data": {
            "fields": ["cal_date", "is_open"],
            "items": [[d.strftime("%Y%m%d"), 1] for d in dates],
        },
    }), encoding="utf-8")

    return {"root": root, "snapshots": snapshots, "schedule": schedule,
            "calendar": cal, "basic_info": basic_info}
