#!/usr/bin/env python3
"""Run Dragon Pulse alpha-candidate tournament variants.

This orchestrates existing backtest_1yr.py runs with isolated temp configs so
research sleeves can be compared against the RS Pullback baseline without
changing production defaults.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import yaml


VARIANTS: dict[str, dict] = {
    "rs_pullback_baseline": {
        "engines": ["rs_pullback"],
        "overrides": {"rs_pullback": {"score_floor": 90, "max_entry_pct": 0.02}},
    },
    "sniper_breakout_v2_strict": {
        "engines": ["sniper_breakout"],
        "overrides": {"sniper_breakout": {"score_floor": 82, "min_adv_cny": 120_000_000}},
    },
    "sniper_breakout_v2_loose": {
        "engines": ["sniper_breakout"],
        "overrides": {"sniper_breakout": {"score_floor": 70, "min_adv_cny": 80_000_000}},
    },
    "sniper_breakout_mas_top1": {
        "engines": ["sniper_breakout"],
        "overrides": {
            "sniper_breakout": {
                "score_floor": 70,
                "min_adv_cny": 80_000_000,
                "max_entry_pct": 0.03,
                "stop_atr_mult": 1.6,
                "target_atr_mult": 3.2,
                "target_2_atr_mult": 4.7,
                "holding_period": 5,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 70}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
    },
    "sniper_breakout_mas_top2": {
        "engines": ["sniper_breakout"],
        "overrides": {
            "sniper_breakout": {
                "score_floor": 70,
                "min_adv_cny": 80_000_000,
                "max_entry_pct": 0.03,
                "stop_atr_mult": 1.6,
                "target_atr_mult": 3.2,
                "target_2_atr_mult": 4.7,
                "holding_period": 5,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 2, "min_score": 70}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 2, "max_selective": 1},
        },
        "top_n": 2,
    },
    "sniper_breakout_mas_rr_top1": {
        "engines": ["sniper_breakout"],
        "overrides": {
            "sniper_breakout": {
                "score_floor": 70,
                "min_adv_cny": 80_000_000,
                "max_entry_pct": 0.03,
                "stop_atr_mult": 1.6,
                "target_atr_mult": 3.2,
                "target_2_atr_mult": 4.7,
                "holding_period": 5,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 70}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "rr",
    },
    "dp_sniper_breakout_lowrisk_rr_top1": {
        "engines": ["sniper_breakout"],
        "overrides": {
            "sniper_breakout": {
                "score_floor": 70,
                "min_adv_cny": 80_000_000,
                "max_entry_pct": 0.03,
                "stop_atr_mult": 1.6,
                "target_atr_mult": 3.2,
                "target_2_atr_mult": 4.7,
                "holding_period": 5,
                # Iteration-7 DP Sniper failure bucket: winners had lower stop risk
                # (~4.7% avg) while losers averaged ~6.1%; keep the R:R ranker but
                # reject wide-stop breakouts that drove most drawdown.
                "max_stop_risk_pct": 4.75,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 70}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "rr",
    },
    "limitup_continuation": {
        "engines": ["limitup_continuation"],
        "overrides": {"limitup_continuation": {"score_floor": 70, "min_adv_cny": 80_000_000}},
    },
    "limitup_continuation_strict": {
        "engines": ["limitup_continuation"],
        "overrides": {"limitup_continuation": {"score_floor": 78, "min_adv_cny": 120_000_000}},
    },
    "limitup_continuation_top1": {
        "engines": ["limitup_continuation"],
        "overrides": {"limitup_continuation": {"score_floor": 70, "min_adv_cny": 80_000_000}},
        "top_n": 1,
    },
    "limitup_continuation_top1_acceptance_v1": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
    },
    "limitup_continuation_top1_lowrisk": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "lowrisk",
    },
    "limitup_continuation_top1_rr": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "rr",
    },
    "limitup_continuation_top1_rr_cd3": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "rr",
        "ticker_cooldown_days": 3,
    },
    "limitup_continuation_top1_rr_stopcd10": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "rr",
        "post_stop_cooldown_days": 10,
    },
    "limitup_continuation_top1_rr_cd3_stopcd10": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "rr",
        "ticker_cooldown_days": 3,
        "post_stop_cooldown_days": 10,
    },
    "limitup_continuation_top1_lowrisk_rr": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 72,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                "max_atr_pct": 6.5,
                "min_rs20": 4.0,
                "min_post_impulse_hold": -0.055,
                "max_post_impulse_extension": 0.045,
            }
        },
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 72}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
        "pick_rank_mode": "lowrisk_rr",
    },
    "limitup_continuation_acceptance_top1": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 70,
                "min_adv_cny": 80_000_000,
                "max_entry_pct": 0.02,
                "stop_atr_mult": 1.15,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
            }
        },
        "root_overrides": {
            # Research-only acceptance profile for post-board continuation.
            # Production RS Pullback requires score >=90; that previously
            # reduced live-equivalent continuation picks to zero.
            "book_size": {
                "bull": {"max_picks": 1, "min_score": 70},
                "max_per_sector": 1,
            },
            "acceptance": {
                "dq_full_threshold": 45,
                "dq_selective_threshold": 15,
                "max_full": 1,
                "max_selective": 1,
            },
        },
        "top_n": 1,
    },
    "limitup_continuation_riskband_top1": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 78,
                "min_adv_cny": 80_000_000,
                "max_entry_pct": 0.02,
                "stop_atr_mult": 1.15,
                "target_atr_mult": 2.2,
                "target_2_atr_mult": 3.4,
                "holding_period": 4,
                # Iteration-3 failure bucket: very tight stops were mostly weak/fake
                # continuations, while >7.5% stop-risk names drove drawdown.
                "min_stop_risk_pct": 6.5,
                "max_stop_risk_pct": 7.5,
            }
        },
        "root_overrides": {
            "book_size": {
                "bull": {"max_picks": 1, "min_score": 78},
                "max_per_sector": 1,
            },
            "acceptance": {
                "dq_full_threshold": 45,
                "dq_selective_threshold": 15,
                "max_full": 1,
                "max_selective": 1,
            },
        },
        "top_n": 1,
    },
    "limitup_continuation_fast_exit": {
        "engines": ["limitup_continuation"],
        "overrides": {
            "limitup_continuation": {
                "score_floor": 70,
                "min_adv_cny": 80_000_000,
                "stop_atr_mult": 1.05,
                "target_atr_mult": 1.8,
                "target_2_atr_mult": 2.8,
                "holding_period": 3,
            }
        },
    },
    "accumulation_breakout": {
        "engines": ["accumulation_breakout"],
        "overrides": {"accumulation_breakout": {"score_floor": 68, "min_adv_cny": 80_000_000}},
    },
    "accumulation_breakout_top1_acceptance": {
        "engines": ["accumulation_breakout"],
        "overrides": {"accumulation_breakout": {"score_floor": 68, "min_adv_cny": 80_000_000}},
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 68}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
    },
    "accumulation_breakout_loose_top1_acceptance": {
        "engines": ["accumulation_breakout"],
        "overrides": {"accumulation_breakout": {"score_floor": 62, "min_adv_cny": 60_000_000}},
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 62}},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
        "top_n": 1,
    },
    "momentum_combo": {
        "engines": ["sniper_breakout", "limitup_continuation", "accumulation_breakout"],
        "overrides": {
            "sniper_breakout": {"score_floor": 70, "min_adv_cny": 80_000_000},
            "limitup_continuation": {"score_floor": 70, "min_adv_cny": 80_000_000},
            "accumulation_breakout": {"score_floor": 68, "min_adv_cny": 80_000_000},
        },
    },
    "momentum_combo_top1": {
        "engines": ["sniper_breakout", "limitup_continuation", "accumulation_breakout"],
        "overrides": {
            "sniper_breakout": {"score_floor": 70, "min_adv_cny": 80_000_000},
            "limitup_continuation": {"score_floor": 70, "min_adv_cny": 80_000_000},
            "accumulation_breakout": {"score_floor": 68, "min_adv_cny": 80_000_000},
        },
        "top_n": 1,
    },
}

PATTERNS = {
    "picks": re.compile(r"Total picks evaluated: (\d+) \| No-chase skipped: (\d+)"),
    "win": re.compile(r"PnL win rate: ([\d.]+)% \| Target hit rate: ([\d.]+)%"),
    "expectancy": re.compile(r"True expectancy: ([+-]?[\d.]+)% \| Weighted: ([+-]?[\d.]+)%"),
    "risk": re.compile(r"Max drawdown: ([\d.]+)% \| Cumulative PnL: ([+-]?[\d.]+)% \| Equity: ([\d.]+)x"),
}


def parse_metrics(log_text: str) -> dict:
    metrics: dict[str, float | int | str | None] = {}
    for key, pattern in PATTERNS.items():
        match = pattern.search(log_text)
        if not match:
            continue
        if key == "picks":
            metrics["picks"] = int(match.group(1))
            metrics["skipped"] = int(match.group(2))
        elif key == "win":
            metrics["win_rate_pct"] = float(match.group(1))
            metrics["target_hit_rate_pct"] = float(match.group(2))
        elif key == "expectancy":
            metrics["true_expectancy_pct"] = float(match.group(1))
            metrics["weighted_expectancy_pct"] = float(match.group(2))
        elif key == "risk":
            metrics["max_drawdown_pct"] = float(match.group(1))
            metrics["cumulative_pnl_pct"] = float(match.group(2))
            metrics["equity_multiple"] = float(match.group(3))
    if "picks" not in metrics and "No results to summarize" in log_text:
        metrics.update({"picks": 0, "skipped": 0})
    return metrics


def _sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_summary_metrics(summary_path: Path, log_text: str) -> dict:
    """Prefer the machine-readable backtest summary over regex log parsing."""
    if not summary_path.exists():
        return parse_metrics(log_text)

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    return {
        "picks": int(summary.get("total_picks", 0)),
        "skipped": int(summary.get("entry_skipped_no_chase", 0)),
        "win_rate_pct": round(float(summary.get("pnl_win_rate", 0.0)) * 100, 1),
        "target_hit_rate_pct": round(float(summary.get("target_hit_rate", 0.0)) * 100, 1),
        "true_expectancy_pct": summary.get("true_expectancy_pct"),
        "weighted_expectancy_pct": summary.get("weighted_expectancy_pct"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
        "cumulative_pnl_pct": summary.get("cumulative_pnl_pct"),
        "equity_multiple": summary.get("final_equity_multiple"),
        "summary": str(summary_path),
        "price_data_fingerprint": summary.get("price_data_fingerprint"),
        "price_snapshot_mode": summary.get("price_snapshot_mode"),
        "price_snapshot_dir": summary.get("price_snapshot_dir"),
        "download_bad_ticker_count": summary.get("download_bad_ticker_count"),
        "download_bad_tickers": summary.get("download_bad_tickers"),
    }


def detail_hash(detail_path: str | Path) -> str | None:
    """Return a stable hash for a backtest detail CSV, or None if absent."""
    return _sha256_file(Path(detail_path))


def deep_update(base: dict, updates: dict) -> dict:
    """Recursively update mapping values, preserving unspecified defaults."""
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_update(base[key], value)
        else:
            base[key] = value
    return base


def build_config(base: dict, variant_name: str, variant: dict) -> dict:
    cfg = deepcopy(base)
    cfg.setdefault("mean_reversion", {})["enabled"] = False
    cfg.setdefault("sniper", {})["enabled"] = False
    alpha = cfg.setdefault("alpha_candidates", {})
    alpha["enabled"] = True
    alpha["engines"] = list(variant["engines"])
    for block in variant["engines"]:
        alpha.setdefault(block, {})
        alpha[block]["enabled"] = True
    for block, overrides in variant.get("overrides", {}).items():
        alpha.setdefault(block, {})
        alpha[block].update(overrides)
    deep_update(cfg, deepcopy(variant.get("root_overrides", {})))
    cfg.setdefault("runtime", {})["method_version"] = f"alpha-tournament-{variant_name}"
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/default.yaml")
    parser.add_argument("--start", default="2026-04-01")
    parser.add_argument("--end", default="2026-06-24")
    parser.add_argument("--variants", default="all", help="Comma-separated variant names or all")
    parser.add_argument("--acceptance-mode", default="live_equivalent", choices=["off", "engine_only", "live_equivalent"])
    parser.add_argument("--entry-mode", default="limit_touch")
    parser.add_argument("--exit-mode", default="target_stop")
    parser.add_argument("--universe-source", default="watchlist", choices=["watchlist", "market_cap"])
    parser.add_argument("--universe-mode", default="static", choices=["static", "point_in_time"])
    parser.add_argument("--universe-n", default="1000")
    parser.add_argument("--outputs-root", default="outputs")
    parser.add_argument("--price-snapshot-dir", default="")
    parser.add_argument("--workdir", default="/tmp/dp-alpha-tournament")
    parser.add_argument("--max-days", default="0")
    parser.add_argument("--top-n", default="5")
    parser.add_argument("--pick-rank-mode", default="score", choices=["score", "lowrisk", "rr", "lowrisk_rr"])
    parser.add_argument("--ticker-cooldown-days", default="0")
    parser.add_argument("--post-stop-cooldown-days", default="0")
    args = parser.parse_args()

    requested = list(VARIANTS) if args.variants == "all" else [v.strip() for v in args.variants.split(",") if v.strip()]
    unknown = [v for v in requested if v not in VARIANTS]
    if unknown:
        parser.error(f"Unknown variant(s): {', '.join(unknown)}")

    base = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    results = []

    for name in requested:
        variant = VARIANTS[name]
        cfg = build_config(base, name, variant)
        cfg_path = workdir / f"{name}.yaml"
        log_path = workdir / f"{name}.log"
        variant_out_dir = workdir / name / "backtest"
        variant_out_dir.mkdir(parents=True, exist_ok=True)
        cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
        cmd = [
            sys.executable,
            "scripts/backtest_1yr.py",
            "--config", str(cfg_path),
            "--start", args.start,
            "--end", args.end,
            "--engines", "alpha_only",
            "--acceptance-mode", args.acceptance_mode,
            "--entry-mode", args.entry_mode,
            "--exit-mode", args.exit_mode,
            "--universe-source", args.universe_source,
            "--outputs-root", args.outputs_root,
            "--out-dir", str(variant_out_dir),
            "--price-snapshot-dir", args.price_snapshot_dir or str(workdir / "price_snapshot"),
            "--max-days", str(args.max_days),
            "--top-n", str(variant.get("top_n", args.top_n)),
            "--pick-rank-mode", str(variant.get("pick_rank_mode", args.pick_rank_mode)),
            "--ticker-cooldown-days", str(variant.get("ticker_cooldown_days", args.ticker_cooldown_days)),
            "--post-stop-cooldown-days", str(variant.get("post_stop_cooldown_days", args.post_stop_cooldown_days)),
        ]
        if args.universe_source == "market_cap":
            cmd += ["--universe-mode", args.universe_mode, "--universe-n", str(args.universe_n)]
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=1800)
        log_text = proc.stdout + proc.stderr
        log_path.write_text(log_text, encoding="utf-8")
        summary_file = variant_out_dir / "backtest_summary.json"
        detail_file = variant_out_dir / "backtest_detail.csv"
        daily_file = variant_out_dir / "backtest_daily.csv"
        metrics = _load_summary_metrics(summary_file, log_text)
        if proc.returncode != 0 and "picks" not in metrics:
            metrics["error"] = log_text[-2000:]
        metrics.update({
            "variant": name,
            "returncode": proc.returncode,
            "log": str(log_path),
            "config": str(cfg_path),
            "out_dir": str(variant_out_dir),
            "detail": str(detail_file) if detail_file.exists() else None,
            "detail_sha256": detail_hash(detail_file),
            "daily": str(daily_file) if daily_file.exists() else None,
            "daily_sha256": detail_hash(daily_file),
        })
        results.append(metrics)
        print(json.dumps(metrics, ensure_ascii=False, sort_keys=True))

    summary_path = workdir / "summary.json"
    summary_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"summary={summary_path}")
    return 0 if all(r.get("returncode") == 0 for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
