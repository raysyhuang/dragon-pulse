# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-strategy momentum screener for China A-shares (with US market fallback) that identifies stocks with high probability of significant moves. Combines technical screening, LLM-powered ranking, and cross-system hybrid analysis.

Three core systems: **Weekly Scanner** (7-day aggressive momentum), **30-Day Screener** (conservative positions), and **Daily Movers** (quarantined idea funnel). The `all` command runs all three plus hybrid cross-referencing.

## Commands

```bash
# Install
pip install -r requirements.txt

# Run everything (recommended)
python main.py all

# Individual components
python main.py weekly          # Weekly scanner only
python main.py pro30           # 30-day screener only
python main.py llm             # LLM ranking only (needs weekly packets first)
python main.py movers          # Daily movers discovery
python main.py performance     # Backtest historical picks
python main.py replay          # Regenerate past outputs

# Useful flags
python main.py all --debug                    # Debug logging
python main.py all --date 2025-12-28          # Specific date
python main.py all --provider anthropic       # Use Claude instead of GPT
python main.py all --model gpt-5.2            # Explicit model
python main.py all --log-file logs/app.log    # Log to file
python main.py all --open                     # Open HTML report in browser

# Tests
pytest tests/ -v
pytest tests/test_filters.py -v    # Single test file
```

## Architecture

### Data Flow

```
main.py (CLI dispatcher)
  -> src/commands/*.py (command handlers)
    -> src/pipelines/*.py (orchestration)
      -> src/core/*.py (business logic)
      -> src/features/*.py (optional features)
      -> src/reporting/*.py (HTML/artifact generation)
```

### Key Directories

- **src/core/**: Foundation — config loading, data fetching, filtering, scoring, LLM integration, I/O
- **src/commands/**: CLI command handlers (thin wrappers around pipelines)
- **src/pipelines/**: Orchestration workflows for weekly and pro30 scanners
- **src/features/**: Optional modules — daily movers, dragon tiger list, sector rotation, backtesting
- **src/reporting/**: HTML report generation with dark theme, plus CSV/JSON/Markdown artifacts
- **config/default.yaml**: Single source of truth for all parameters (market, universe, liquidity, technicals, etc.)
- **outputs/YYYY-MM-DD/**: Date-stamped output directories (trading days only)

### Market Abstraction

The system supports both China A-shares and US markets via a market abstraction layer:

- `src/core/data.py` routes to the correct data provider based on config (`market.region`)
- `src/core/cn_data.py` handles China data (AkShare primary, Tushare backup)
- `src/core/yf.py` handles US data via Yahoo Finance
- `src/core/universe.py` manages ticker universes (`CHINA_A`, `SP500`, `SP500+NASDAQ100`, etc.)
- `get_data_functions(config)` returns the appropriate download/provider functions for the configured market

### Pipeline Pattern

All pipelines follow the same structure: load config -> build universe -> download data -> apply hard filters -> compute scores -> build LLM packets -> save outputs. The `src/pipelines/weekly.py` and `src/pipelines/pro30.py` are the main orchestrators.

### Scoring

Technical scores (0-10) are computed in `src/core/scoring.py` using 5 factors: proximity to 52W high, volume spike, RSI range, moving average alignment, and realized volatility. The LLM then applies a 3-factor model: Technical (40%, locked from pre-computation), Catalyst/News (40%), Market Activity (20%).

### Configuration Pattern

```python
from src.core.config import load_config, get_config_value
config = load_config("config/default.yaml")
value = get_config_value(config, "universe", "mode", default="SP500")
```

## Environment Variables

- `OPENAI_API_KEY` — required for OpenAI models (default provider)
- `ANTHROPIC_API_KEY` — required for Anthropic models
- `TUSHARE_TOKEN` — required for Tushare backup data provider (China)

## China A-Share Specifics

- 10% daily price limit (涨跌停) constrains realistic targets
- ST stocks (退市警示) excluded via `universe.exclude_st` config
- CSI 300 (`000300.SH`) used as market regime benchmark
- Dragon Tiger List (龙虎榜) tracks institutional activity in `src/features/dragon_tiger/`
- Trading calendar follows Shanghai timezone with CN holidays
- Liquidity thresholds in CNY (default: 50M avg daily volume)
