#!/usr/bin/env python3
"""Northbound active risk-band paper sleeve.

This is a quarantined forward paper tracker for the June 2026 northbound
research lead. It deliberately writes separate paper artifacts and sends a
clearly-labelled paper alert; it does not feed Dragon Pulse live ranking,
execution_watchlist, or production alpha engines.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.core.alerts import AlertConfig, AlertManager, _ticker_display  # noqa: E402
from src.core.cn_data import get_cn_basic_info  # noqa: E402
from scripts.morning_check import fetch_open_prices, run_preflight  # noqa: E402

logger = logging.getLogger(__name__)

# Last trading day with per-stock northbound holdings. The exchanges ended daily
# per-stock disclosure from 2024-08-19; both vendors we can reach stop on the same
# day (Eastmoney RPT_MUTUAL_HOLDSTOCKNDATE_STA and Tushare hk_hold, checked
# 2026-08-11), so this is a disclosure change, not a provider outage. Nothing in
# this repo can recover holding deltas for a later date.
NORTHBOUND_HOLDING_LAST_DATE = "2024-08-16"


@dataclass
class PaperPick:
    ticker: str
    name_cn: str
    rank: int
    signal_date: str
    source_trade_date: str
    entry_price: float
    max_entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    holding_period: int
    score: float
    stop_risk_pct: float
    net_amount: float | None = None
    amount: float | None = None
    holding_delta_vol: float | None = None
    holding_delta_ratio: float | None = None
    holding_delta_source: str | None = None
    regime_at_signal: str | None = None
    breadth_at_signal: float | None = None

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "name_cn": self.name_cn,
            "engine": "northbound_active_riskband_paper",
            "paper_only": True,
            "status": "PAPER_TRACK_ONLY",
            "rank": self.rank,
            "signal_date": self.signal_date,
            "source_trade_date": self.source_trade_date,
            "entry_price": self.entry_price,
            "max_entry_price": self.max_entry_price,
            "stop_loss": self.stop_loss,
            "target_1": self.target_1,
            "target_2": self.target_2,
            "holding_period": self.holding_period,
            "score": self.score,
            "stop_risk_pct": self.stop_risk_pct,
            "net_amount": self.net_amount,
            "amount": self.amount,
            "holding_delta_vol": self.holding_delta_vol,
            "holding_delta_ratio": self.holding_delta_ratio,
            "holding_delta_source": self.holding_delta_source,
            "regime_at_signal": self.regime_at_signal,
            "breadth_at_signal": self.breadth_at_signal,
            "northbound_flow_confirmed": bool(
                (self.net_amount is not None and self.net_amount > 0)
                or (self.holding_delta_vol is not None and self.holding_delta_vol > 0)
            ),
            "promotion_eligible": False,
            "promotion_blocker": "Paper-only active-rank lead until net_amount or holding-delta works and PIT/publish-lag gates pass",
            "reason_summary": f"北向活跃榜rank={self.rank}, stop风险={self.stop_risk_pct:.1f}%",
        }


def _yyyymmdd(date_str: str) -> str:
    return pd.to_datetime(date_str).strftime("%Y%m%d")


def _iso_date(value) -> str:
    return pd.to_datetime(str(value)).strftime("%Y-%m-%d")


def _token() -> str | None:
    return os.environ.get("TUSHARE_TOKEN")


def fetch_hsgt_top10_latest(asof_date: str, lookback_days: int = 10) -> tuple[str | None, pd.DataFrame, str]:
    """Fetch latest available hsgt_top10 rows at or before asof_date.

    Tushare endpoint behavior varies by account/version, so try both generic
    and market_type-specific calls, then union/dedupe rows.
    """
    import tushare as ts  # pyright: ignore[reportMissingModuleSource]

    token = _token()
    if not token:
        return None, pd.DataFrame(), "missing TUSHARE_TOKEN"

    pro = ts.pro_api(token=token)
    end = pd.to_datetime(asof_date)
    errors: list[str] = []

    for offset in range(0, lookback_days + 1):
        day = end - timedelta(days=offset)
        trade_date = day.strftime("%Y%m%d")
        frames: list[pd.DataFrame] = []
        calls = [({}, "generic"), ({"market_type": "1"}, "沪股通"), ({"market_type": "3"}, "深股通")]
        for kwargs, label in calls:
            try:
                df = pro.hsgt_top10(trade_date=trade_date, **kwargs)
                if df is not None and not df.empty:
                    df = df.copy()
                    df["_call_label"] = label
                    frames.append(df)
            except Exception as exc:  # keep trying other dates/call shapes
                errors.append(f"{trade_date}/{label}: {exc}")

        if not frames:
            continue

        out = pd.concat(frames, ignore_index=True)
        if "ts_code" not in out.columns:
            errors.append(f"{trade_date}: missing ts_code column")
            continue
        out["ts_code"] = out["ts_code"].astype(str).str.upper()
        if "rank" in out.columns:
            out["rank"] = pd.to_numeric(out["rank"], errors="coerce")
        else:
            out["rank"] = 999
        if "net_amount" in out.columns:
            out["net_amount"] = pd.to_numeric(out["net_amount"], errors="coerce")
        if "amount" in out.columns:
            out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
        out = out.sort_values(["rank", "ts_code"]).drop_duplicates("ts_code", keep="first")
        return trade_date, out, "ok"

    return None, pd.DataFrame(), "; ".join(errors[-5:]) or "no rows"



def _normalize_cn_code(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if "." in raw:
        code, exch = raw.split(".", 1)
        if exch in {"SH", "SZ"}:
            return f"{code.zfill(6)}.{exch}"
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 6:
        code = digits[-6:]
        exch = "SH" if code.startswith("6") else "SZ"
        return f"{code}.{exch}"
    return raw


def _numeric_or_none(value: object) -> float | None:
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return val if pd.notna(val) else None


def assess_data_quality(
    *,
    asof_date: str,
    source_trade_date: str | None,
    source_status: str,
    net_amount_non_null: int,
    holding_delta_status: str,
    holding_delta_rows: int,
) -> dict:
    """Grade whether northbound evidence is flow-confirmed or active-rank-only."""
    gaps: list[str] = []
    if source_status != "ok":
        gaps.append(f"hsgt_top10 unavailable/degraded: {source_status}")
    if source_trade_date and source_trade_date < asof_date:
        gaps.append(f"latest hsgt_top10 trade_date {source_trade_date} is older than asof {asof_date}")
    if net_amount_non_null <= 0:
        gaps.append("hsgt_top10 net_amount/buy/sell are not populated; active-rank turnover only")
    holding_delta_discontinued = "discontinued_after" in holding_delta_status
    if holding_delta_discontinued:
        gaps.append(
            "holding-delta replacement is permanently unavailable: per-stock northbound "
            f"holdings ended {NORTHBOUND_HOLDING_LAST_DATE}"
        )
    elif holding_delta_rows <= 0 or holding_delta_status != "ok":
        gaps.append(f"holding-delta replacement unavailable: {holding_delta_status}")

    flow_confirmed = net_amount_non_null > 0 or (holding_delta_status == "ok" and holding_delta_rows > 0)
    if source_status != "ok":
        grade = "unavailable"
    elif flow_confirmed and not any("older than" in g for g in gaps):
        grade = "flow_confirmed"
    elif flow_confirmed:
        grade = "flow_confirmed_but_lagged"
    else:
        grade = "active_rank_only"
    return {
        "data_quality": grade,
        "flow_confirmed": flow_confirmed,
        "data_gaps": gaps,
        "label": "北向活跃榜/成交活跃纸面跟踪" if not flow_confirmed else "北向资金流确认纸面跟踪",
        # Both flow-confirmation routes are gone: hsgt_top10 stopped populating
        # net_amount and per-stock holdings ended. Recorded so the readout cannot
        # mistake a permanent ceiling for a gap that might close on its own.
        "flow_confirmation_possible": not (holding_delta_discontinued and net_amount_non_null <= 0),
    }


def load_regime_stamp(market_date: str) -> dict:
    """Load the latest observable Dragon Pulse market context for a paper entry.

    The feed runs before the T+1 open, so it uses the prior completed market
    date rather than same-day post-close data. A missing artifact is non-blocking.
    """
    path = PROJECT_ROOT / "outputs" / market_date / f"regime_{market_date}.json"
    stamp = {
        "regime_at_signal": None,
        "breadth_at_signal": None,
        "regime_market_date": market_date,
        "regime_stamp_source": "saved_regime_artifact",
    }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("regime artifact must be a JSON object")
        regime = payload.get("regime")
        breadth = payload.get("market_breadth_pct_above_sma20")
        stamp["regime_at_signal"] = str(regime) if regime else None
        stamp["breadth_at_signal"] = round(float(breadth), 4) if breadth is not None else None
        stamp["regime_stamp_status"] = "ok"
    except FileNotFoundError:
        stamp["regime_stamp_status"] = "regime_artifact_missing"
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        stamp["regime_stamp_status"] = f"regime_artifact_unavailable: {type(exc).__name__}: {str(exc)[:120]}"
    return stamp


def _compute_true_atr14(high_s: pd.Series, low_s: pd.Series, close_s: pd.Series) -> float:
    """Compute Wilder-style true range mean over 14 bars.

    The bracket replay used ATR-style stop risk. A simple high-low average
    understates gap risk, so include previous close in true range.
    """
    prev_close = close_s.shift(1)
    tr = pd.concat([
        high_s - low_s,
        (high_s - prev_close).abs(),
        (low_s - prev_close).abs(),
    ], axis=1).max(axis=1)
    return float(tr.tail(14).mean())


def fetch_holding_delta_map(source_date: str) -> tuple[dict[str, dict], str]:
    """Best-effort northbound holding-delta fallback via Eastmoney/AkShare.

    This is a replacement candidate for missing hsgt_top10 net_amount fields.
    If the public Eastmoney wrapper changes or fails, the paper sleeve still
    emits active-rank picks and records the gap.

    Per-stock northbound holdings stop at NORTHBOUND_HOLDING_LAST_DATE, so for
    any later date this returns the discontinuation status without a network
    call. Earlier dates still hit the provider for backfill and research.
    """
    if _iso_date(source_date) > NORTHBOUND_HOLDING_LAST_DATE:
        return {}, f"northbound_holding_delta_discontinued_after_{NORTHBOUND_HOLDING_LAST_DATE}"

    try:
        import akshare as ak  # pyright: ignore[reportMissingModuleSource]
    except Exception as exc:
        return {}, f"akshare unavailable: {exc}"

    end = pd.to_datetime(source_date)
    start = end - timedelta(days=7)
    try:
        df = ak.stock_hsgt_stock_statistics_em(
            symbol="北向持股",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
    except (TypeError, KeyError, IndexError, AttributeError) as exc:
        # AkShare/Eastmoney wrapper parsing failed before a DataFrame returned.
        # Keep the sleeve fail-open, but distinguish this from provider absence.
        return {}, (
            f"eastmoney_holding_delta_parser_error: {type(exc).__name__}: "
            f"{str(exc)[:120]}"
        )
    except Exception as exc:
        return {}, f"eastmoney_holding_delta_unavailable: {type(exc).__name__}: {str(exc)[:120]}"

    if df is None or df.empty:
        return {}, "eastmoney_holding_delta_empty"

    df = df.copy()
    code_col = next((c for c in df.columns if str(c) in {"代码", "股票代码", "SECURITY_CODE", "code", "ts_code"}), None)
    date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()), None)
    vol_col = next((c for c in df.columns if any(k in str(c) for k in ["持股数", "持股数量", "持股股数"])), None)
    ratio_col = next((c for c in df.columns if "持股占" in str(c) or "持股比例" in str(c)), None)
    if not code_col or not date_col or not vol_col:
        return {}, "eastmoney_holding_delta_schema_gap:" + ",".join(map(str, df.columns[:20]))

    df["_ticker"] = df[code_col].map(_normalize_cn_code)
    df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["_vol"] = pd.to_numeric(df[vol_col], errors="coerce")
    if ratio_col:
        df["_ratio"] = pd.to_numeric(df[ratio_col], errors="coerce")
    else:
        df["_ratio"] = pd.NA
    df = df.dropna(subset=["_ticker", "_date", "_vol"]).sort_values(["_ticker", "_date"])
    latest = df[df["_date"] <= end]
    if latest.empty:
        return {}, "eastmoney_holding_delta_no_rows_before_source_date"

    out: dict[str, dict] = {}
    for ticker, g in latest.groupby("_ticker"):
        g = g.tail(2)
        if len(g) < 2:
            continue
        prev, cur = g.iloc[0], g.iloc[-1]
        delta_vol = _numeric_or_none(cur["_vol"] - prev["_vol"])
        delta_ratio = _numeric_or_none(cur["_ratio"] - prev["_ratio"]) if pd.notna(cur.get("_ratio")) and pd.notna(prev.get("_ratio")) else None
        out[str(ticker)] = {
            "holding_delta_vol": delta_vol,
            "holding_delta_ratio": delta_ratio,
            "holding_delta_source": "eastmoney_hsgt_stock_statistics",
            "holding_delta_latest_date": _iso_date(cur["_date"]),
            "holding_delta_prev_date": _iso_date(prev["_date"]),
        }
    return out, "ok" if out else "eastmoney_holding_delta_no_pair_rows"


def save_source_probe(date_str: str) -> Path:
    """Record current HSGT availability/publish-lag evidence for forward tracking."""
    raw_date, rows, status = fetch_hsgt_top10_latest(date_str, lookback_days=10)
    source_date = _iso_date(raw_date) if raw_date else None
    holding_map, holding_status = fetch_holding_delta_map(source_date or date_str)
    out = PROJECT_ROOT / "outputs" / date_str / f"northbound_source_probe_{date_str}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    rank_le_5 = 0
    if rows is not None and not rows.empty and "rank" in rows.columns:
        rank_le_5 = int(pd.to_numeric(rows["rank"], errors="coerce").le(5).sum())
    net_non_null = 0
    if rows is not None and not rows.empty and "net_amount" in rows.columns:
        net_non_null = int(rows["net_amount"].notna().sum())
    payload = {
        "date": date_str,
        "generated_utc": datetime.utcnow().isoformat(),
        "probe_time_shanghai": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "hsgt_top10_status": status,
        "hsgt_top10_latest_trade_date": source_date,
        "hsgt_top10_rows": int(len(rows)) if rows is not None else 0,
        "hsgt_top10_rank_le_5_rows": rank_le_5,
        "hsgt_top10_columns": list(rows.columns) if rows is not None and not rows.empty else [],
        "hsgt_top10_net_amount_non_null": net_non_null,
        "holding_delta_status": holding_status,
        "holding_delta_rows": len(holding_map),
        "holding_delta_sample": dict(list(holding_map.items())[:5]),
    }
    payload.update(assess_data_quality(
        asof_date=date_str,
        source_trade_date=source_date,
        source_status=status,
        net_amount_non_null=net_non_null,
        holding_delta_status=holding_status,
        holding_delta_rows=len(holding_map),
    ))
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved northbound source probe: %s", out)
    return out

def _fetch_ohlcv(tickers: Iterable[str], start: str, end: str) -> dict[str, pd.DataFrame]:
    from src.core.cn_data import download_daily_range

    data_map, report = download_daily_range(
        list(tickers),
        start=start,
        end=end,
        # Use unadjusted prices for forward paper alerts; qfq-adjusted prices can
        # be split/dividend-adjusted and are not the executable screen price.
        provider_config={"primary": "akshare", "backup": "tushare", "adjust": "none", "tushare_token_env": "TUSHARE_TOKEN"},
    )
    if report.get("bad_tickers"):
        logger.info("OHLCV unavailable for %d northbound names", len(report.get("bad_tickers", [])))
    return data_map


def build_picks(asof_date: str, top_n: int = 5) -> tuple[list[PaperPick], dict]:
    source_trade_date_raw, hsgt, status = fetch_hsgt_top10_latest(asof_date)
    meta = {
        "asof_date": asof_date,
        "source_trade_date": _iso_date(source_trade_date_raw) if source_trade_date_raw else None,
        "source_status": status,
        "paper_only": True,
        "rule": "northbound active rank<=5, stop-risk 4-7%, conservative T+1 no-chase, 5D bracket replay tracker",
    }
    if hsgt.empty or source_trade_date_raw is None:
        return [], meta

    # Research rule: northbound active rank<=5. Keep both Shanghai/Shenzhen active lists,
    # deduped by code. Later filters enforce risk band and tradability.
    rank_raw = hsgt["rank"] if "rank" in hsgt.columns else pd.Series(999, index=hsgt.index)
    rank_series = pd.Series(pd.to_numeric(rank_raw, errors="coerce"), index=hsgt.index)
    hsgt = hsgt[rank_series.le(5)].copy()
    if hsgt.empty:
        meta["source_status"] = "no rank<=5 rows"
        return [], meta

    candidates = pd.Series(hsgt["ts_code"]).dropna().astype(str).str.upper().drop_duplicates().tolist()
    source_date = _iso_date(source_trade_date_raw)
    start = (pd.to_datetime(source_date) - timedelta(days=90)).strftime("%Y-%m-%d")
    data_map = _fetch_ohlcv(candidates, start=start, end=source_date)
    info_map = get_cn_basic_info(candidates, provider_config={"tushare_token_env": "TUSHARE_TOKEN"})
    holding_delta_map, holding_delta_status = fetch_holding_delta_map(source_date)
    regime_stamp = load_regime_stamp(source_date)
    meta["holding_delta_status"] = holding_delta_status
    meta["holding_delta_rows"] = len(holding_delta_map)
    meta.update(regime_stamp)
    net_amount_non_null = int(pd.Series(hsgt["net_amount"]).notna().sum()) if "net_amount" in hsgt.columns else 0
    meta.update(assess_data_quality(
        asof_date=asof_date,
        source_trade_date=source_date,
        source_status=status,
        net_amount_non_null=net_amount_non_null,
        holding_delta_status=holding_delta_status,
        holding_delta_rows=len(holding_delta_map),
    ))

    picks: list[PaperPick] = []
    for _, row in hsgt.iterrows():
        ticker = str(row.get("ts_code", "")).upper()
        df = data_map.get(ticker, pd.DataFrame())
        if df.empty or len(df) < 20:
            continue
        close_col = "Close" if "Close" in df.columns else "close"
        high_col = "High" if "High" in df.columns else "high"
        low_col = "Low" if "Low" in df.columns else "low"
        close_s = pd.to_numeric(df[close_col], errors="coerce").dropna()
        high_s = pd.to_numeric(df[high_col], errors="coerce").dropna()
        low_s = pd.to_numeric(df[low_col], errors="coerce").dropna()
        if len(close_s) < 20 or len(high_s) < 20 or len(low_s) < 20:
            continue
        close = float(close_s.iloc[-1])
        atr14 = _compute_true_atr14(high_s, low_s, close_s)
        if close <= 0 or atr14 <= 0:
            continue
        stop_risk_pct = 1.1 * atr14 / close * 100.0
        if stop_risk_pct < 4.0 or stop_risk_pct > 7.0:
            continue

        rank = int(row.get("rank") or 999)
        net_amount = row.get("net_amount") if "net_amount" in row else None
        amount = row.get("amount") if "amount" in row else None
        net_val = float(net_amount) if pd.notna(net_amount) else 0.0
        amount_val = float(amount) if pd.notna(amount) else 0.0
        risk_center_bonus = max(0.0, 20.0 - abs(stop_risk_pct - 5.5) * 8.0)
        holding_delta = holding_delta_map.get(ticker, {})
        holding_delta_vol = holding_delta.get("holding_delta_vol")
        holding_delta_ratio = holding_delta.get("holding_delta_ratio")
        net_bonus = 8.0 if net_val > 0 else 0.0
        holding_bonus = 6.0 if holding_delta_vol is not None and holding_delta_vol > 0 else 0.0
        score = max(1.0, 100.0 - (rank - 1) * 7.0 + risk_center_bonus + net_bonus + holding_bonus)
        info = info_map.get(ticker, {})
        name = str(row.get("name") or info.get("name_cn") or ticker)
        picks.append(PaperPick(
            ticker=ticker,
            name_cn=name,
            rank=rank,
            signal_date=asof_date,
            source_trade_date=source_date,
            entry_price=round(close, 2),
            max_entry_price=round(close * 1.02, 2),
            stop_loss=round(close - 1.1 * atr14, 2),
            target_1=round(close + 2.1 * atr14, 2),
            target_2=round(close + 3.6 * atr14, 2),
            holding_period=5,
            score=round(score, 2),
            stop_risk_pct=round(stop_risk_pct, 2),
            net_amount=float(net_val) if pd.notna(net_amount) else None,
            amount=float(amount_val) if pd.notna(amount) else None,
            holding_delta_vol=_numeric_or_none(holding_delta_vol),
            holding_delta_ratio=_numeric_or_none(holding_delta_ratio),
            holding_delta_source=holding_delta.get("holding_delta_source"),
            regime_at_signal=regime_stamp["regime_at_signal"],
            breadth_at_signal=regime_stamp["breadth_at_signal"],
        ))

    picks = sorted(picks, key=lambda p: (-p.score, p.rank, p.ticker))[:top_n]
    meta["source_rows"] = int(len(hsgt))
    meta["riskband_picks"] = int(len(picks))
    return picks, meta


def artifact_path(date_str: str) -> Path:
    return PROJECT_ROOT / "outputs" / date_str / f"northbound_paper_watchlist_{date_str}.json"


def save_watchlist(date_str: str, picks: list[PaperPick], meta: dict) -> Path:
    out = artifact_path(date_str)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_str,
        "generated_utc": datetime.utcnow().isoformat(),
        "sleeve": "northbound_active_riskband_paper",
        "paper_only": True,
        "status": "PAPER_TRACK_ONLY",
        "meta": meta,
        "picks": [p.to_dict() for p in picks],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def send_preopen_alert(date_str: str, picks: list[PaperPick], meta: dict, path: Path) -> None:
    alert_config = AlertConfig(enabled=True, channels=["telegram"])
    if not alert_config.telegram_bot_token or not alert_config.telegram_chat_id:
        logger.info("Telegram not configured; saved artifact only: %s", path)
        return

    from src.core.message_format import (
        RULE,
        append_footer,
        expandable,
        humanize_gap,
        humanize_status,
        meta_line,
        pick_block,
        title_line,
    )

    QUALITY_CN = {
        "flow_confirmed": "北向资金流已确认",
        "flow_confirmed_but_lagged": "资金流已确认（数据滞后）",
        "active_rank_only": "仅活跃榜排名（无资金流）",
        "unavailable": "数据源不可用",
    }
    source_date = meta.get("source_trade_date") or "无"
    quality = QUALITY_CN.get(meta.get("data_quality"), str(meta.get("data_quality", "未知")))
    alert_title = f"🧪 北向纸面袖珍盘 — {date_str}"

    lines = [
        title_line("", alert_title),
        meta_line("<b>PAPER ONLY</b>", "不进实盘龙脉排名", f"候选 <b>{len(picks)}</b>"),
        RULE,
    ]
    if not picks:
        lines.append(f"今日无北向纸面候选 — {humanize_status(meta.get('source_status'))}")
    else:
        for i, p in enumerate(picks, 1):
            extras = [f"北向排名 #{p.rank}"]
            if p.holding_delta_vol is not None:
                extras.append(f"北向持股变化 {p.holding_delta_vol:+,.0f}股")
            lines += pick_block(
                index=i,
                display=_ticker_display(p.ticker, p.name_cn),
                entry=p.entry_price,
                stop=p.stop_loss,
                target=p.target_1,
                max_entry=p.max_entry_price,
                holding=p.holding_period,
                score=p.score,
                extras=extras,
            )
            lines.append("")

    append_footer(
        lines,
        [expandable(
            "数据与规则",
            [
                f"数据质量 · {quality}",
                f"榜单来源 · hsgt_top10 {source_date}",
                "筛选规则 · 北向排名≤5 且 止损风险 4–7%",
                f"增持替代源 · {humanize_status(meta.get('holding_delta_status'))}",
                *[f"• {humanize_gap(gap)}" for gap in meta.get("data_gaps", [])[:4]],
                "用途 · 2–4周 forward paper，验证成交、开盘追高、5日 bracket、发布滞后。",
            ],
        )],
    )

    AlertManager(alert_config).send_alert(
        title=alert_title,
        message="\n".join(lines),
        data={"asof": date_str},
        priority="low",
    )


def send_open_check(date_str: str) -> int:
    path = artifact_path(date_str)
    if not path.exists():
        logger.error("Missing northbound paper watchlist: %s", path)
        return 1
    payload = json.loads(path.read_text(encoding="utf-8"))
    picks = payload.get("picks", [])
    if not picks:
        logger.info("No northbound paper picks to open-check.")
        return 0

    open_prices = fetch_open_prices([p.get("ticker", "") for p in picks])
    if not open_prices:
        logger.warning("Open prices unavailable; skip northbound paper open check for now.")
        return 0
    results = run_preflight(picks=picks, open_prices=open_prices, prev_volumes={})

    out = PROJECT_ROOT / "outputs" / date_str / f"northbound_paper_execution_check_{date_str}.json"
    out.write_text(json.dumps({
        "date": date_str,
        "generated_utc": datetime.utcnow().isoformat(),
        "sleeve": "northbound_active_riskband_paper",
        "paper_only": True,
        "results": [r.__dict__ for r in results],
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    alert_config = AlertConfig(enabled=True, channels=["telegram"])
    if not alert_config.telegram_bot_token or not alert_config.telegram_chat_id:
        return 0

    result_map = {r.ticker: r for r in results}
    go = [r for r in results if r.action == "GO"]
    warn = [r for r in results if r.action == "WARN"]
    cancel = [r for r in results if r.action == "CANCEL"]
    from src.core.message_format import RULE, append_footer, meta_line, pick_block, title_line

    alert_title = f"🧪 北向纸面袖珍盘 — {date_str} 开盘检查"
    lines = [
        title_line("", alert_title),
        meta_line(
            "<b>PAPER ONLY</b>",
            f"执行 {len(go)}",
            f"注意 {len(warn)}",
            f"取消 {len(cancel)}",
        ),
        RULE,
    ]
    action_cn = {"GO": "纸面执行", "WARN": "纸面注意", "CANCEL": "纸面取消"}
    icon = {"GO": "✅", "WARN": "⚠️", "CANCEL": "❌"}
    for i, pick in enumerate(picks, 1):
        r = result_map.get(pick.get("ticker"))
        if r is None:
            continue
        gap = f"开盘 ¥{r.open_price:.2f}（跳空 {r.gap_pct:+.1f}%）" if r.open_price else "开盘 —"
        lines += pick_block(
            index=i,
            display=_ticker_display(r.ticker, r.name_cn),
            entry=r.entry_price,
            stop=pick.get("stop_loss"),
            target=pick.get("target_1"),
            max_entry=pick.get("max_entry_price"),
            holding=pick.get("holding_period"),
            score=pick.get("score"),
            icon=icon.get(r.action, "•"),
            tag=action_cn.get(r.action, r.action),
            extras=[gap],
            notes=list(r.reasons),
        )
        lines.append("")

    append_footer(lines, [f"<i>纸面跟踪 · 共 {len(results)} 只</i>"])

    AlertManager(alert_config).send_alert(
        title=alert_title,
        message="\n".join(lines),
        data={"asof": date_str},
        priority="low",
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Northbound active risk-band paper sleeve")
    parser.add_argument("--date", default=pd.Timestamp.now(tz="Asia/Shanghai").strftime("%Y-%m-%d"))
    parser.add_argument("--mode", choices=["preopen", "open_check", "source_probe"], default="preopen")
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--no-alert", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.mode == "open_check":
        return send_open_check(args.date)
    if args.mode == "source_probe":
        save_source_probe(args.date)
        return 0

    save_source_probe(args.date)
    picks, meta = build_picks(args.date, top_n=args.top_n)
    path = save_watchlist(args.date, picks, meta)
    logger.info("Saved northbound paper watchlist: %s (%d picks)", path, len(picks))
    if not args.no_alert:
        send_preopen_alert(args.date, picks, meta, path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
