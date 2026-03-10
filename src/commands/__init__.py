"""
Command Handlers

Command implementations for CLI interface.
"""

from .weekly import cmd_weekly
from .pro30 import cmd_pro30
from .llm import cmd_llm
from .movers import cmd_movers
from .all import cmd_all
from .performance import cmd_performance
from .replay import cmd_replay
from .cmd_scan import cmd_scan
from .cmd_backtest import cmd_backtest
from .cmd_evolve import cmd_evolve
from .cmd_calibrate import cmd_calibrate

__all__ = [
    "cmd_weekly", "cmd_pro30", "cmd_llm", "cmd_movers", "cmd_all",
    "cmd_performance", "cmd_replay", "cmd_scan", "cmd_backtest", "cmd_evolve",
    "cmd_calibrate",
]

