# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Deterministic A-share scanner with two MAS-derived engines: **Mean Reversion** (RSI(2) oversold bounces, 3-day hold) and **Sniper** (BB squeeze + volume compression breakouts, 7-day hold). No LLM, no debate/confluence. Top 1000 A-shares by market cap.

## Commands

```bash
# Install
pip install -r requirements.txt

# Run scan (primary command)
python main.py scan
python main.py scan --date 2026-03-13    # Specific date
python main.py scan --config config/default.yaml --debug

# Alias
python main.py all                       # Same as scan

# Backtest
python main.py performance              # Backtest picks from outputs/

# Tests
pytest tests/ -v
```

## Architecture

### Data Flow

```
main.py (CLI dispatcher)
  -> src/commands/scan.py (command handler)
    -> src/pipelines/scanner.py (unified pipeline)
      -> src/core/universe.py (top 1000 by market cap)
      -> src/core/cn_data.py (OHLCV download)
      -> src/features/technical.py (pandas_ta indicators)
      -> src/signals/mean_reversion.py (RSI(2) engine)
      -> src/signals/sniper.py (BB squeeze engine)
      -> src/core/alerts.py (Telegram notification)
```

### Key Directories

- **src/core/**: Foundation — config, data fetching, universe, regime, alerts, I/O
- **src/commands/**: CLI command handlers (`scan.py`, `performance.py`)
- **src/pipelines/**: `scanner.py` — unified scan pipeline
- **src/signals/**: `mean_reversion.py`, `sniper.py` — scoring engines
- **src/features/**: `technical.py` — pandas_ta feature engineering
- **config/default.yaml**: All parameters for both engines
- **outputs/YYYY-MM-DD/**: Date-stamped output directories

### Engines

**Mean Reversion** — active in ALL regimes (bull, choppy, bear)
- RSI(2) ≤ 10 trigger, weights: RSI(2) 40%, trend 25%, streak 15%, 5d-low 10%, volume 10%
- Stop: entry - 0.75×ATR, targets capped at entry×1.10 (A-share limit)
- Gates: ≥60 bars, ADV ≥50M CNY, no >11% single-day moves

**Sniper** — bear regime hard block, score floors: 60 bull, 65 choppy
- BB squeeze + volume compression + CSI 300 relative strength
- Stop: entry - 2.0×ATR, targets capped at entry×1.10
- Gates: ATR% ≥3.5, avg volume ≥500K shares

### Output Artifacts

- `scan_results_{date}.json` — full ranked picks + metadata
- `execution_watchlist_{date}.json` — execution-ready top picks
- `regime_{date}.json` — regime decision + evidence

### Configuration Pattern

```python
from src.core.config import load_config, get_config_value
config = load_config("config/default.yaml")
value = get_config_value(config, "mean_reversion", "holding_period", default=3)
```

## Environment Variables

- `TUSHARE_TOKEN` — required for Tushare market-cap ranking and backup data
- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` — for scan alerts

## China A-Share Specifics

- 10% daily price limit (涨跌停) — all targets capped at entry×1.10
- T+1 constraint — no same-day exits after entry
- ST stocks (退市警示) excluded
- CSI 300 (`000300.SH`) used for regime classification and sniper relative strength
- Trading calendar follows Shanghai timezone
- Liquidity thresholds in CNY (default: 50M avg daily volume)
