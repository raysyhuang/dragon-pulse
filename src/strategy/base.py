from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
import pandas as pd
from core.display import format_ticker

@dataclass
class StrategySignal:
    """A signal from one strategy lens."""
    ticker: str
    name_cn: str
    lens: str                    # "lens_a", "lens_b", "lens_c"
    score: float                 # 0-100
    triggered: bool              # Whether signal conditions are met
    evidence: dict = field(default_factory=dict)  # Detailed scoring breakdown
    entry_price: float = 0.0
    target_price: float = 0.0
    stop_price: float = 0.0
    position_size_mult: float = 1.0  # e.g., 0.5 for Lens C (asymmetric risk)
    max_hold_days: int = 5

    def display_ticker(self) -> str:
        return format_ticker(self.ticker, self.name_cn)

@dataclass
class PickCandidate:
    """A final pick candidate after confluence."""
    ticker: str
    name_cn: str
    composite_score: float       # Weighted combination of lens scores
    confluence_type: str         # "double", "pullback_seal", "breakout_seal", "single_institution"
    signals: list[StrategySignal]
    entry_price: float
    target_price: float
    stop_price: float
    position_size_mult: float    # Final sizing multiplier
    max_hold_days: int
    regime: str                  # "bull", "caution", "bear"
    sector: str = ""

    def display_ticker(self) -> str:
        return format_ticker(self.ticker, self.name_cn)

    def risk_reward_ratio(self) -> float:
        risk = abs(self.entry_price - self.stop_price)
        reward = abs(self.target_price - self.entry_price)
        return reward / risk if risk > 0 else 0.0

class StrategyLens(ABC):
    """Base class for all strategy lenses."""

    def __init__(self, params: dict):
        self.params = params  # Evolvable parameters

    @abstractmethod
    def scan(self, ticker: str, name_cn: str, ohlcv: pd.DataFrame,
             technicals: dict, context: dict) -> StrategySignal:
        """Evaluate a single stock. Returns signal with score and triggered flag."""
        ...

    @abstractmethod
    def get_param_ranges(self) -> dict[str, tuple[float, float]]:
        """Return (min, max) ranges for each evolvable parameter."""
        ...
