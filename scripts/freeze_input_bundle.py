#!/usr/bin/env python3
"""Freeze a non-PIT offline input bundle for reproducible backtest A/B runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
import sys
sys.path.insert(0, str(project_root))

from src.core.config import load_config
from src.core.data import get_data_functions
from src.core.cn_data import get_cn_basic_info
from src.core.universe import get_top_n_cn_by_market_cap


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _composite(files: dict[str, str]) -> str:
    return hashlib.sha256("".join(f"{name}  {digest}\n" for name, digest in sorted(files.items())).encode()).hexdigest()


def _write_price(path: Path, frame: pd.DataFrame) -> None:
    data = frame.copy()
    data.index = pd.to_datetime(data.index).strftime("%Y-%m-%d")
    data.index.name = "Date"
    data.to_csv(path, float_format="%.10g")


def _freeze_data_end(backtest_end: date) -> date:
    """Cover later-listed current-universe names without changing scan dates."""
    return max(backtest_end + timedelta(days=30), date.today())


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze a non-PIT Dragon Pulse input bundle")
    parser.add_argument("--output", required=True, help="New bundle directory; must not exist")
    parser.add_argument("--start", default="2025-03-14")
    parser.add_argument("--end", default="2026-03-13")
    parser.add_argument("--config", default="config/default.yaml")
    parser.add_argument("--universe-n", type=int, default=1000)
    parser.add_argument("--bundle-id", default="")
    parser.add_argument("--acceptance-mode", choices=["off", "engine_only", "live_equivalent"], default="live_equivalent")
    args = parser.parse_args()

    output = Path(args.output).resolve()
    if output.exists():
        parser.error(f"refusing to overwrite existing bundle: {output}")
    config = load_config(args.config)
    _, download_range, provider_config, _ = get_data_functions(config)
    universe = get_top_n_cn_by_market_cap(args.universe_n, provider_config)
    basic_info = get_cn_basic_info(universe, provider_config)
    if args.acceptance_mode == "live_equivalent":
        missing_basic = [ticker for ticker in universe if ticker not in basic_info]
        if missing_basic:
            raise RuntimeError(f"refusing live_equivalent bundle; basic info missing for: {', '.join(missing_basic[:10])}")
    csi_cfg = config.get("mean_reversion", {}).get("regime", {}) or config.get("sniper", {}).get("regime", {}) or {}
    csi_symbol = csi_cfg.get("csi300_symbol", "000300.SH")
    start = datetime.strptime(args.start, "%Y-%m-%d").date() - timedelta(days=420)
    backtest_end = datetime.strptime(args.end, "%Y-%m-%d").date()
    end = _freeze_data_end(backtest_end)
    data_map, report = download_range(
        universe + [csi_symbol], start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"),
        provider_config=provider_config,
    )
    missing = [ticker for ticker in universe + [csi_symbol] if data_map.get(ticker, pd.DataFrame()).empty]
    if missing:
        raise RuntimeError(f"refusing incomplete bundle; missing price data for: {', '.join(missing[:10])}")

    temp = Path(tempfile.mkdtemp(prefix=f".{output.name}.freeze-", dir=output.parent))
    try:
        prices = temp / "prices"
        prices.mkdir()
        pd.DataFrame([
            {"ticker": ticker, "name": basic_info.get(ticker, {}).get("name_cn", ""), "board": basic_info.get(ticker, {}).get("exchange", ""),
             "is_st": bool(basic_info.get(ticker, {}).get("is_st", False)), "cap_rank_at_freeze": rank}
            for rank, ticker in enumerate(universe, 1)
        ]).to_csv(temp / "universe.csv", index=False)
        (temp / "basic_info.json").write_text(json.dumps(basic_info, sort_keys=True, ensure_ascii=False, indent=2), encoding="utf-8")
        for ticker in sorted(universe + [csi_symbol]):
            _write_price(prices / f"{ticker}.csv", data_map[ticker])
        files = {item.relative_to(temp).as_posix(): _sha256(item) for item in sorted(temp.rglob("*")) if item.is_file()}
        price_providers = {
            f"prices/{ticker}.csv": report["providers"][ticker]
            for ticker in sorted(universe + [csi_symbol])
        }
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=project_root, text=True).strip()
        manifest = {
            "bundle_id": args.bundle_id or f"dp-ab-{datetime.utcnow().strftime('%Y-%m-%dT%H%M%SZ')}",
            "created_at": datetime.utcnow().isoformat() + "Z",
            "freeze_tool_commit": commit,
            "pit_grade": False,
            "universe_count": len(universe),
            "files": files,
            "composite_sha256": _composite(files),
            "price_providers": price_providers,
            "provider_fallbacks": sorted(
                path for path, provider in price_providers.items()
                if provider != str(provider_config.get("primary", "akshare")).lower()
            ),
            "notes": (
                "cap universe frozen at capture time; NOT historical PIT; "
                f"price coverage through {end.isoformat()} to retain later-listed current-universe names"
            ),
        }
        (temp / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
        os.replace(temp, output)
    except Exception:
        shutil.rmtree(temp, ignore_errors=True)
        raise
    print(json.dumps({"bundle": str(output), "bundle_id": manifest["bundle_id"], "composite_sha256": manifest["composite_sha256"], "pit_grade": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
