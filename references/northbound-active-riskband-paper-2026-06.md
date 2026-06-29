# Northbound Active Risk-Band Paper Sleeve — 2026-06

Status: **paper-track only**. Do not route into live ranking, alerts, trade plans, or production picks.

## Rule under review

Research label: `northbound_active_riskband_paper`

Candidate definition from the June 2026 A-share alpha search:

1. Source: Tushare `hsgt_top10` daily northbound active list plus daily OHLCV.
2. Signal date: stock appears in northbound active top 5 by rank.
3. Entry realism: conservative no-chase fill only — next trading day's open must be at or below signal close × 1.02.
4. Risk band: 1.1 × ATR14 stop distance must be between 4% and 7% of signal close.
5. Test exit: 5 trading-day close exit.

## Evidence snapshot

Artifact: `/tmp/dp_alpha_event_search_20260629/northbound_composite/northbound_composite_summary.csv`

Best close-exit row observed:

| sleeve | fills/events | horizon | WR | avg | PF | median | p10 | p90 |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| top5_lowrisk_4_7 | 986 / 1012 | 5D close | 56.69% | +2.573% | 2.086 | +1.572% | -7.626% | +14.419% |

Apples-to-apples bracket replay follow-up:

Artifact: `/tmp/dp_alpha_event_search_20260629/northbound_bracket_replay/northbound_bracket_summary.csv`

| sleeve | fills/events | horizon | WR | avg | PF | median | target | stop | time |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| top5_lowrisk_4_7 | 1012 / 1012 | 5D bracket | 51.98% | +1.681% | 1.728 | +0.605% | 30.43% | 38.34% | 31.23% |
| nb_top5 | 1975 / 1975 | 5D bracket | 48.91% | +1.243% | 1.583 | -0.286% | 25.72% | 38.94% | 35.34% |

Standalone northbound top5 comparison:

Artifact: `/tmp/dp_alpha_event_search_20260629/northbound_active/northbound_active_summary.csv`

| sleeve | fills/events | horizon | WR | avg | PF |
|---|---:|---|---:|---:|---:|
| rank_top5 conservative | 1937 / 2270 | 5D close | 53.90% | +1.861% | 1.802 |

The risk-band composite improves the standalone northbound signal under both close-exit and bracket replay, but bracket replay materially reduces the headline and remains well below MAS Sniper-class quality.

## Benchmark comparison

MAS Sniper 3Y model-level rerun:

Artifact: `/srv/workspaces/multi-agentic-screener/outputs/research/alpha_tournament/mas_3y_rerun_20260628_full_cached_bg/2026-06-29/alpha_tournament_results.json`

- Sniper core: ~80.8% WR, +3.93% avg, PF 3.61.
- Selective-bear Sniper: ~80.7% WR, +3.93% avg, PF 3.60.

Conclusion: northbound risk-band is a legitimate positive A-share research sleeve, not a MAS Sniper replacement.

## Known caveats / blockers

- Endpoint reproducibility: confirm real-world publication timing and availability for `hsgt_top10`; do not assume it is live-deployable without source-lag verification.
- Survivorship/PIT: current fast research used available Tushare/OHLCV panels. It needs a point-in-time universe and out-of-sample split before promotion.
- Exit model: the original headline was 5D close exit; bracket replay reduced the best risk-band row from 56.69% WR / +2.573% / PF 2.086 to 51.98% WR / +1.681% / PF 1.728. Treat close-exit numbers as optimistic leads, not promotion metrics.
- Overfit risk: the 4–7% stop-risk band was selected after searching variants; require walk-forward validation.
- Benchmark gap: WR/PF remain far below MAS Sniper.

## Required validation before promotion

1. Rebuild as a deterministic script under `scripts/` using cached input manifests, not ad-hoc `/tmp` state.
2. Validate endpoint lag: signal must only use data known after market close on signal date.
3. Add walk-forward / holdout windows, including non-bull regimes.
4. Compare weekly against RS Pullback and CSI300.
5. Keep it quarantined from `execution_watchlist`, Telegram/Discord alerts, and live trade plans until all above pass.
