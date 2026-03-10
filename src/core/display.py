"""
Ticker display formatting: Always show company names with ticker numbers.
"""
from __future__ import annotations
from typing import Optional

_NAME_CACHE: dict[str, str] = {}

def format_ticker(ticker: str, name_cn: Optional[str] = None) -> str:
    """Format ticker as 公司名（代码）. E.g., 中国石油（600028.SH）"""
    if name_cn:
        _NAME_CACHE[ticker] = name_cn
        return f"{name_cn}（{ticker}）"
    cached = _NAME_CACHE.get(ticker)
    if cached:
        return f"{cached}（{ticker}）"
    return ticker

def load_name_cache(tickers: list[str], provider_config: Optional[dict] = None) -> None:
    """Bulk load company names from AkShare/Tushare into cache."""
    try:
        from core.cn_data import get_cn_basic_info
        info = get_cn_basic_info(tickers, provider_config)
        for ticker, data in info.items():
            _NAME_CACHE[ticker] = data.get("name_cn", ticker)
    except ImportError:
        pass

def get_name(ticker: str) -> str:
    """Get cached Chinese name for a ticker."""
    return _NAME_CACHE.get(ticker, ticker)
