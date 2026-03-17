"""
Regime Gate Functions

Market regime checking (SPY/VIX filters) enriched with FRED macro data.
"""

from __future__ import annotations
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from typing import Optional

logger = logging.getLogger(__name__)


def check_regime(params: dict, asof_date: Optional[str] = None, **kwargs) -> dict:
    """
    Check market regime: SPY above MA20 AND VIX <= threshold.
    
    Args:
        params: Dict with regime gate parameters:
            - spy_symbol: SPY ticker (default: "SPY")
            - vix_symbol: VIX ticker (default: "^VIX")
            - spy_ma_days: MA period (default: 20)
            - vix_max: Maximum VIX threshold (default: 25.0)
    
    Returns:
        Dict with:
            - ok: bool (True if regime is OK)
            - spy_last: float
            - spy_ma: float
            - vix_last: float
            - spy_above_ma: bool
            - vix_ok: bool
            - message: str
    """
    out = {
        "ok": True,
        "spy_last": np.nan,
        "spy_ma": np.nan,
        "vix_last": np.nan,
        "spy_above_ma": None,
        "vix_ok": None,
        "message": ""
    }
    
    try:
        spy_symbol = params.get("spy_symbol", "SPY")
        vix_symbol = params.get("vix_symbol", "^VIX")
        spy_ma_days = params.get("spy_ma_days", 20)
        vix_max = params.get("vix_max", 25.0)

        download_daily_fn = kwargs.get("download_daily_fn")
        download_daily_range_fn = kwargs.get("download_daily_range_fn")

        if download_daily_range_fn:
            # Use market-aware download function (CN or US)
            end_dt = pd.to_datetime(asof_date).to_pydatetime() if asof_date else pd.Timestamp.now()
            start_dt = end_dt - pd.Timedelta(days=120)
            start_str = pd.Timestamp(start_dt).strftime("%Y-%m-%d")
            end_str = pd.Timestamp(end_dt).strftime("%Y-%m-%d")
            spy_result = download_daily_range_fn(tickers=[spy_symbol], start=start_str, end=end_str)
            # download_daily_range returns (data_dict, report) tuple
            if isinstance(spy_result, tuple):
                spy_data = spy_result[0]
            else:
                spy_data = spy_result
            spy = spy_data.get(spy_symbol, pd.DataFrame()) if isinstance(spy_data, dict) else spy_data
            if vix_symbol:
                vix_result = download_daily_range_fn(tickers=[vix_symbol], start=start_str, end=end_str)
                if isinstance(vix_result, tuple):
                    vix_data = vix_result[0]
                else:
                    vix_data = vix_result
                vix = vix_data.get(vix_symbol, pd.DataFrame()) if isinstance(vix_data, dict) else vix_data
            else:
                vix = pd.DataFrame()
        elif asof_date:
            end_dt = pd.to_datetime(asof_date).to_pydatetime()
            start_dt = end_dt - pd.Timedelta(days=120)
            spy = yf.download(spy_symbol, start=start_dt, end=end_dt + pd.Timedelta(days=1), interval="1d", progress=False)
            if vix_symbol:
                vix = yf.download(vix_symbol, start=start_dt, end=end_dt + pd.Timedelta(days=1), interval="1d", progress=False)
            else:
                vix = pd.DataFrame()
        else:
            spy = yf.download(spy_symbol, period="3mo", interval="1d", progress=False)
            if vix_symbol:
                vix = yf.download(vix_symbol, period="3mo", interval="1d", progress=False)
            else:
                vix = pd.DataFrame()

        spy_close = spy["Close"].dropna() if not spy.empty and "Close" in spy.columns else pd.Series(dtype=float)
        vix_close = vix["Close"].dropna() if not vix.empty and "Close" in vix.columns else pd.Series(dtype=float)

        if len(spy_close) < spy_ma_days + 1:
            out["message"] = "Regime data insufficient; skipping gate."
            return out

        # Extract scalar values properly
        spy_last_val = spy_close.iloc[-1]
        if isinstance(spy_last_val, pd.Series):
            spy_last_val = spy_last_val.iloc[0]
        out["spy_last"] = float(spy_last_val)

        spy_ma_val = spy_close.tail(spy_ma_days).mean()
        if isinstance(spy_ma_val, pd.Series):
            spy_ma_val = spy_ma_val.iloc[0]
        out["spy_ma"] = float(spy_ma_val)

        out["spy_above_ma"] = out["spy_last"] >= out["spy_ma"]

        # VIX check — skip if no VIX symbol configured (e.g. CN market)
        if len(vix_close) >= 5:
            vix_last_val = vix_close.iloc[-1]
            if isinstance(vix_last_val, pd.Series):
                vix_last_val = vix_last_val.iloc[0]
            out["vix_last"] = float(vix_last_val)
            out["vix_ok"] = out["vix_last"] <= vix_max
        else:
            out["vix_ok"] = True  # No VIX data = skip VIX gate

        out["ok"] = bool(out["spy_above_ma"] and out["vix_ok"])
        # Build message with actual symbol names
        msg = (
            f"{spy_symbol}={out['spy_last']:.2f} vs MA{spy_ma_days}={out['spy_ma']:.2f} "
            f"({'OK' if out['spy_above_ma'] else 'RISK-OFF'})"
        )
        if not np.isnan(out["vix_last"]):
            msg += (
                f"; VIX={out['vix_last']:.2f} "
                f"(<= {vix_max:.2f} is {'OK' if out['vix_ok'] else 'RISK-OFF'})"
            )
        msg += "."
        out["message"] = msg

        return out
    except Exception as e:
        out["message"] = f"Regime gate error; skipping gate. ({e})"
        return out



