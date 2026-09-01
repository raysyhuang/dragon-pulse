"""Board-aware daily price limit for China A-shares.

Official exchange rules:
  - SSE/SZSE main board: 10%
  - ST / risk-warning shares: 5%
  - SSE STAR Market ordinary shares/CDRs (688xxx, 689xxx): 20%
  - SZSE ChiNext (300xxx, 301xxx): 20%
  - BSE (4xxxxx, 8xxxxx, 92xxxx): 30%

When board cannot be determined, defaults to conservative 10% (main board).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def get_daily_limit(ticker: str, is_st: bool = False) -> float:
    """Return the daily price limit as a decimal (e.g. 0.10 for 10%).

    Parameters
    ----------
    ticker : str
        A-share ticker like "688001.SH", "300750.SZ", "600519.SH", "831799.BJ"
    is_st : bool
        Whether the stock has ST / *ST / risk-warning status.

    Returns
    -------
    float
        Daily price limit as a fraction (0.05, 0.10, 0.20, or 0.30).
    """
    if is_st:
        return 0.05

    code = _extract_code(ticker)
    if not code or len(code) != 6:
        logger.debug("Cannot determine board for %s — using 10%% default", ticker)
        return 0.10

    # SSE STAR Market ordinary shares/CDRs: 688xxx, 689xxx
    if code.startswith(("688", "689")):
        return 0.20

    # SZSE ChiNext: 300xxx, 301xxx
    if code.startswith(("300", "301")):
        return 0.20

    # BSE: 4xxxxx, 8xxxxx (not STAR), 92xxxx
    if code.startswith("4") or code.startswith("92"):
        return 0.30
    if code.startswith("8") and not code.startswith(("688", "689")):
        return 0.30

    # SSE/SZSE main board: 600xxx, 601xxx, 603xxx, 000xxx, 001xxx, 002xxx
    return 0.10


def _extract_code(ticker: str) -> str:
    """Extract the 6-digit code from a ticker string."""
    raw = str(ticker).strip().upper()
    if "." in raw:
        return raw.split(".")[0]
    return raw
