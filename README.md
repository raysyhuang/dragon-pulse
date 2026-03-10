# Momentum Trading System

**Simple, consolidated momentum trading system with progress indicators.**

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# ⭐ RECOMMENDED: Run everything + hybrid analysis (one command!)
python main.py all                    # Uses gpt-5.2 by default
python main.py all --model gpt-5.2    # Explicitly use gpt-5.2
python main.py all --date 2025-12-28  # Scan specific date
python main.py all --debug            # Enable debug logging
python main.py all --log-file logs/app.log  # Write logs to file

# Or run individual components:
python main.py weekly      # Weekly scanner only
python main.py pro30       # 30-day screener only
python main.py llm          # LLM ranking only
python main.py movers       # Daily movers only
```

**The `all` command runs everything in sequence:**
1. Daily Movers discovery
2. Weekly Scanner (technical screening with progress bars)
3. 30-Day Screener (conservative screening)
4. LLM ranking (Top 5 from weekly using GPT-5.2)
5. **Hybrid analysis** - Cross-references all results and highlights overlaps:
   - ⭐ **ALL THREE** (Weekly + 30-Day + Movers) - Highest conviction
   - 🔥 **Weekly + 30-Day** - Strong technical + conservative
   - 📈 **Weekly + Movers** - Momentum + recent activity
   - 💎 **30-Day + Movers** - Conservative + recent activity

**Output**: `outputs/YYYY-MM-DD/hybrid_analysis_YYYY-MM-DD.json` with all overlaps and recommendations

## What It Does

**Weekly Scanner**: Finds top 5 stocks for ≥10% move in next 7 days using 4-factor model (Technical, Catalyst, Options, Sentiment)

**30-Day Screener**: Conservative screening for 30-day positions with dual-horizon analysis (7-10d + 30d)

**Daily Movers**: Quarantined idea funnel for daily gainers/losers (+7% to +15% or -15% to -7%)

## Configuration

All settings in `config/default.yaml`. Key options:
- `universe.mode`: now supports `"CHINA_A"` (AkShare primary, Tushare backup) in addition to `"SP500"`, `"SP500+NASDAQ100"`, `"SP500+NASDAQ100+R2000"`
- `market.region` + `market.timezone`: defaults to China (`Asia/Shanghai`) in this fork
- `data.china`: primary/backup provider and Tushare token env (`TUSHARE_TOKEN`)
- `movers.enabled`: true/false to enable daily movers
- `liquidity.min_avg_dollar_volume_20d`: Minimum liquidity (default: 50M)

## Outputs

All outputs go to `outputs/YYYY-MM-DD/` (trading dates only):
- Output directories are only created for **trading days** (weekdays)
- If run on a **weekend**, outputs use the previous **Friday's date**
- If run **before market close** (before 4 PM ET), outputs use the **previous trading day**
- This ensures outputs always correspond to completed trading sessions

Output files:
- `weekly_scanner_candidates_*.csv` - Technical candidates
- `weekly_scanner_packets_*.json` - LLM packets
- `weekly_scanner_top5_*.json` - Final Top 5 (after LLM ranking)
- `30d_momentum_candidates_*.csv` - 30-day candidates
- `llm_packets_*.txt` - LLM analysis packets
- `hybrid_analysis_*.json` - Cross-referenced results
- `report_*.html` - Comprehensive HTML report

## File Structure

```
├── main.py                    # Unified entry point (USE THIS)
├── config/
│   └── default.yaml           # All settings
├── src/
│   ├── core/                 # Core utilities
│   │   ├── filters.py        # Hard filter logic
│   │   ├── packets.py        # Packet building
│   │   ├── analysis.py       # Headline analysis
│   │   └── llm.py            # LLM integration
│   ├── commands/             # Command handlers
│   │   ├── weekly.py         # Weekly scanner command
│   │   ├── pro30.py          # 30-day screener command
│   │   ├── llm.py            # LLM ranking command
│   │   ├── movers.py         # Daily movers command
│   │   └── all.py            # Complete scan command
│   ├── features/movers/       # Daily movers
│   └── pipelines/            # Weekly & Pro30 pipelines
└── requirements.txt          # Dependencies
```

**Note**: The LLM ranking is now integrated into the core pipeline (`src/core/llm.py` and `src/commands/llm.py`).

## Which System to Use?

- **Weekly Scanner**: High-velocity 7-day momentum bursts, catalyst-driven (Top 5 JSON output)
- **30-Day Screener**: Conservative positions, capital preservation, larger sizes (Top 15-25 CSV output)
- **Both**: Run both and cross-reference for higher conviction

**For Investing**: Use 30-Day Screener as primary (better for capital preservation). Weekly Scanner can complement for tactical positions.

## Daily Movers

Daily movers are a quarantined idea funnel - they don't affect scoring, just add candidates to the universe. Enable in `config/default.yaml`:
```yaml
movers:
  enabled: true
  top_n: 50
  cooling_days_required: 1
```

## Progress Indicators

The system now shows progress percentages:
- `[1/4] Building universe...`
- `[2/4] Screening X tickers... Progress: 500/2498 (20.0%)`
- `[3/4] Fetching company info... Info: 15/30 (50.0%)`
- `[4/4] Building LLM packets... Packets: 25/30 (83.3%)`

## System Effectiveness

**For 7-day, 10% moves:**
- **Methodology**: Solid multi-factor approach (Technical + Catalyst + Options + Sentiment)
- **Limitations**: Options & Sentiment data missing (50% of model handicapped)
- **Realistic Expectation**: 20-40% hit rate (1-2 out of 5 picks reach 10%)
- **Key Differentiator**: Catalyst identification via LLM (most important factor)

**Recommendation**: 
- Use as a **filter**, not the sole signal
- Track performance (see `PERFORMANCE_TRACKING.md`)
- Focus on Rank 1-2 picks (highest conviction)
- Always use stop-losses and risk management

**For more conservative trading**: Use `python main.py pro30` (30-day screener) instead.

## Troubleshooting

**SSL/TLS Errors**: yfinance sometimes fails due to SSL issues. The system will retry and continue.

**"Unknown" Exchange/Sector**: Company info fetching may fail for some tickers. This doesn't affect scoring.

**No Candidates**: Try lowering filters in `config/default.yaml` or expanding universe mode.

**Recommended**: Use `python main.py` for everything.
