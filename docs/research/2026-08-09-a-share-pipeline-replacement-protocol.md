# A-Share Pipeline Reliability Scan — Preregistered Protocol (2026-08-09)

**Status:** research / paper-only. This document does **not** change `execution_watchlist`, execution rules, or order authority.

## Decision question

Should Dragon Pulse retain its current one-name RS-pullback paper selector, improve it, or replace it with a more reliable A-share sleeve?

The purpose is not to maximize an attractive backtest. A candidate must demonstrate reproducible, point-in-time, cost-aware and capacity-aware performance under the same execution conventions before it can become a *parallel paper* sleeve. Human approval is required before any production change.

## Evidence already known (not a new discovery)

The repository's 2026-07-26 alpha-hunt report found no liquid-A-share stock-selection factor that survived its full-history, cost-aware checks. The reported survivor was ChiNext 50/200 index trend timing, a modest market-timing rule rather than stock selection. Treat that as the incumbent **reliability benchmark**, not proof it will continue to work.

The existing daily top-1 paper ledger is insufficient to promote the current selector: its current committed summary contains 35 evaluated scans and its last evaluated scan is 2026-07-02. It is a stale, small forward sample.

## Frozen research arms

No extra arm may be added after outcome inspection without a versioned amendment and a new holdout/forward clock.

| ID | Arm | Purpose | Source tier | Promotion status |
|---|---|---|---|---|
| B0 | CSI 300 / ChiNext ETF buy-and-hold comparators | Market beta controls | Tushare structured data | Benchmark only |
| B1 | Existing RS-pullback top-1 | Current selector benchmark | Dragon Pulse artifacts + Tushare | Frozen incumbent |
| C1 | ChiNext 50/200 with VT20 | Existing robust timing candidate | Existing repo implementation + Tushare | Parallel-paper candidate only |
| C2 | Quality-value basket | Cheapness only when paired with profitability, cash conversion and balance-sheet filters | Tushare `daily_basic` + financial statements/indicators | New candidate, untested |
| C3 | Event/revision proxy | Disclosure-timed earnings/announcement reaction, only if point-in-time timestamps and an independently frozen matched control are available | Tushare disclosure data; any broker/opinion feed is context only | New candidate, untested |

**Explicitly excluded:** raw social/KOL calls, webpage scraping, unsourced “undervalued” lists, post-close information in a pre-open decision, and same-sample parameter fishing.

## Common replay rules

These rules bind every arm where applicable:

1. **Point in time:** universe membership, financial record and disclosure fields must have an available/published timestamp no later than the signal timestamp. `end_date` alone is never evidence of availability.
2. **Signal clock:** close-derived data on D may first create an entry at D+1 open; no same-close fills.
3. **Tradability:** China T+1, suspension, limit-up/limit-down, no-chase cap, liquidity and corporate-action treatment are explicit. An unfilled signal is a `NO_FILL`, not P&L.
4. **Economics:** identical commission, stamp duty where applicable, slippage, finite capital, maximum concurrent positions and one-pick/day accounting are applied across comparable arms.
5. **Controls:** every candidate is compared with the correct buy-and-hold benchmark and an equal-weight/top-bottom/universe control when cross-sectional ranking is used. A higher return alone is not enough.
6. **Reporting:** preserve all signals, eligibility, fills, no-fills/censors, open positions, source/provider, data freshness, config hash, code commit and raw-input manifest hash.

## Tushare source audit — 2026-08-07 close

Artifact: `outputs/source_data_audit_2026-08-07.json`.

- `daily_basic`: 5,535 records. It returned `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `dv_ratio`, market-cap and liquidity fields; availability varies by field and negative/undefined earnings are not represented as usable positive P/E.
- `fina_indicator`: the endpoint is ticker-scoped. A deterministic top-30-market-cap connectivity sample returned 30/30 records for 2026-Q1, but individual fields have missing values and filings appeared across 2026-04-08 to 2026-05-17.
- The audit proves current endpoint access and field availability only. It does **not** prove point-in-time historical completeness, vendor revision behavior, or alpha.

### Required quality-value join

For every signal date and ticker:

1. take pricing/valuation fields from that market date;
2. select the most recent financial record with `ann_date <= signal_date`;
3. retain `ann_date`, `end_date`, response hash, requested fields and source;
4. reject or explicitly bucket missing cash-flow / leverage / profitability fields; do not silently impute a favorable value.

## C2 quality-value hypothesis (not an investment recommendation)

A stock can be labelled a *research candidate* only when it is cheap **and** financially viable. The preliminary deterministic inputs are:

- valuation: positive `pe_ttm`, `pb`, `ps_ttm`, and dividend yield where applicable;
- profitability: `roe` / `roa`, net-profit and gross-profit margins;
- cash conversion: `ocf_to_or` and available operating-cash-flow fields;
- balance sheet: `debt_to_assets` plus any sector-specific exclusions;
- risk controls: liquidity, ST/suspension/listing-age, concentration and sector neutrality.

Exact percentiles, rebalance frequency, sector treatment and score weights remain **unsealed** until the data-availability/PIT implementation review is complete. They must be frozen before any candidate result is opened.

## C3 event/revision hypothesis (not an investment recommendation)

Public research opinions and broker ratings are leads only. A mechanical event sleeve may be tested only when we can preserve the original disclosure timestamp, issuer record, and a conservative tradable session convention. Its binding test will include a matched random-entry long control with the same calendar, universe, liquidity and bracket assumptions. No NLP/document scoring or analyst-opinion interpretation will be added until the mechanical base-rate gate clears.

## External-source register

External materials are classified, never treated as execution authority:

| Tier | Allowed use | Examples |
|---|---|---|
| Structured primary/vendor | Research inputs after timestamp/provenance validation | Tushare daily/financial/disclosure endpoints |
| Public research/opinion | Theme discovery and independent qualitative context | Broker reports indexed by Eastmoney research center |
| Social/web narrative | Lead only; must be converted to a testable hypothesis | Forum/KOL/“undervalued” lists |

The source scan should attach source URL, publication date, quoted claim, fields used, and why the claim is not itself evidence of return.

## Binding decision gates

A new stock-selection arm is **REJECTED** unless all are true:

1. clean, reproducible run from a frozen input bundle, with no unresolved PIT or data-quality failure;
2. full-sample and date-separated holdout results are positive relative to the appropriate benchmark **after** costs and realistic fills;
3. rank/control evidence shows the declared signal, rather than equal-weight, survivorship, market beta or a single regime, explains the result;
4. no catastrophic concentration, drawdown or turnover failure; and
5. it then completes a new forward paper sample without rule changes.

Passing historical gates permits only a labelled **parallel paper** sleeve. It never silently replaces the daily selector.

## Next deliverables

1. Source-grounded strategy and valuation-opinion register with dates/URLs and evidence tier.
2. Tushare PIT financial-join implementation specification and coverage report.
3. Exact frozen C2/C3 configs and test fixtures before outcome runs.
4. Identical capital-constrained replay + controls, then independent Hawk review.
5. Forward paper ledger from a named effective date for any historical pass.
