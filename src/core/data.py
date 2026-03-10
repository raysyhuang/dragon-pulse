"""
Data adapter router.

Resolves which download helpers to use based on config/market:
- US: yfinance (existing helpers)
- CN: AkShare primary with Tushare backup (cn_data)
"""

from __future__ import annotations

from typing import Tuple

from .config import get_config_value
from . import yf as yf_data
from . import cn_data


def resolve_market(config: dict) -> str:
    """Return normalized market region code."""
    return str(get_config_value(config, "market", "region", default="US")).upper()


def resolve_market_settings(config: dict) -> dict:
    market = resolve_market(config)
    if market == "CN":
        default_tz = "Asia/Shanghai"
        default_close_hour = 15
        default_close_minute = 0
    else:
        default_tz = "America/New_York"
        default_close_hour = 16
        default_close_minute = 0

    return {
        "market": market,
        "timezone": get_config_value(config, "market", "timezone", default=default_tz),
        "close_hour_local": int(get_config_value(config, "market", "close_hour_local", default=default_close_hour)),
        "close_minute_local": int(get_config_value(config, "market", "close_minute_local", default=default_close_minute)),
    }


def get_data_functions(config: dict) -> Tuple:
    """
    Returns (download_daily_fn, download_daily_range_fn, provider_config, market).
    """
    market = resolve_market(config)
    data_cfg = config.get("data", {})

    if market == "CN":
        cn_cfg = data_cfg.get("china", {}) if isinstance(data_cfg.get("china", {}), dict) else {}
        return (
            cn_data.download_daily,
            cn_data.download_daily_range,
            cn_cfg,
            market,
        )

    # Default to US/yfinance
    us_cfg = data_cfg.get("us", {}) if isinstance(data_cfg.get("us", {}), dict) else {}
    return (
        yf_data.download_daily,
        yf_data.download_daily_range,
        us_cfg,
        market,
    )


# Re-export for convenience
get_ticker_df = yf_data.get_ticker_df

