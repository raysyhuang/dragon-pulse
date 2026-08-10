# A-Share Pipeline Reliability Scan — Preregistered Protocol (2026-08-09)

**Status:** research / paper-only. This document does **not** change `execution_watchlist`, execution rules, or order authority.

## Decision question

Should Dragon Pulse retain its current one-name RS-pullback paper selector, improve it, or replace it with a more reliable A-share sleeve?

The purpose is not to maximize an attractive backtest. A candidate must demonstrate reproducible, point-in-time, cost-aware and capacity-aware performance under the same execution conventions before it can become a *parallel paper* sleeve. Human approval is required before any production change.

## Evidence already known (not a new discovery)

The repository's 2026-07-26 alpha-hunt report found no liquid-A-share stock-selection factor that survived its full-history, cost-aware checks. The reported survivor was ChiNext 50/200 index trend timing, a modest market-timing rule rather than stock selection. Treat that as the incumbent **reliability benchmark**, not proof it will continue to work.

The existing daily top-1 paper ledger is insufficient to promote the current selector: its current committed summary contains 35 evaluated scans and its last evaluated scan is 2026-07-02. It is a stale, small forward sample. Task 5 freezes its legacy inventory at `outputs/top1_paper/baseline_inventory_2026-08-09.json` (portable SHA-256 `4200f732444a2efae5fde11bc04bcfc61cd9c86bd3cf4820e23ce6a4825c4e02`): 35 ledger rows / 35 resolved artifacts, 24 tracker-labelled filled and 11 tracker-labelled no-fill. These are saved tracker labels, not verified execution.

## Frozen research arms

No extra arm may be added after outcome inspection without a versioned amendment and a new holdout/forward clock.

| ID | Arm | Purpose | Source tier | Promotion status |
|---|---|---|---|---|
| B0 | CSI 300 / ChiNext ETF buy-and-hold comparators | Market beta controls | Tushare structured data | Benchmark only |
| B1 | Existing RS-pullback top-1 | Current selector benchmark | Dragon Pulse artifacts + Tushare | Frozen incumbent |
| C1 | ChiNext 50/200 with VT20 | Existing robust timing candidate | Existing repo implementation + Tushare | Parallel-paper candidate only |
| C2 | Quality-value ablation | Test whether a properly PIT, announcement-timed quality overlay changes the result of the already-failed price-only value sleeve | Tushare `daily_basic` + financial statements/indicators | Blocked pending PIT-grade data and execution-harness repairs |
| C3 | Event/revision feasibility probe | First prove pre-open disclosure availability and versioning; do not evaluate returns yet | Tushare disclosure data; any broker/opinion feed is context only | Blocked / not a return sleeve |

**Explicitly excluded:** raw social/KOL calls, webpage scraping, unsourced “undervalued” lists, post-close information in a pre-open decision, and same-sample parameter fishing.

## Common replay rules

These rules bind every arm where applicable:

1. **Point in time:** universe membership, financial record and disclosure fields must have an available/published timestamp no later than the signal timestamp. `end_date` alone is never evidence of availability.
2. **Signal clock:** close-derived data on D may first create an entry at D+1 open; no same-close fills.
3. **Tradability:** China T+1, suspension, limit-up/limit-down, no-chase cap, liquidity and corporate-action treatment are explicit. An unfilled signal is a `NO_FILL`, not P&L.
4. **Economics:** identical commission, stamp duty where applicable, slippage, finite capital, maximum concurrent positions and one-pick/day accounting are applied across comparable arms.
5. **Controls:** every candidate is compared with the correct buy-and-hold benchmark and an equal-weight/top-bottom/universe control when cross-sectional ranking is used. A higher return alone is not enough.
6. **Reporting:** preserve all signals, eligibility, fills, no-fills/censors, open positions, source/provider, data freshness, config hash, code commit and raw-input manifest hash.

### Capability blockers discovered by independent review

The current input freezer is explicitly `pit_grade: false`: it freezes a current capitalisation universe and says it is **not historical PIT**. The present cross-sectional sleeve script also uses rebalance-date-close to next-rebalance-date-close returns and drops missing held-name returns. It does not yet implement T+1/open fill, limit-up/no-fill, suspension/delist treatment or finite-capital constraints.

Therefore, **no new historical C2 result is admissible** until a PIT-grade universe/data-bundle design and a production-like replay harness exist. A date-separated factor curve from the current harness is a lead only, not a promotion test.

### PIT evidence and provenance gate

Tasks 1–2 establish **self-consistency and source membership only**. Task 1's `validate_pit_bundle` is an integrity validator, not an oracle for historical membership: it accepts only a supplied bundle with a nonempty `bundle_id`, flat file `hashes`, matching `composite_sha256`, `pit_grade: true`, hash-valid copied sources, and schedule rows tied to those sources. It can prove those supplied artifacts are self-consistent; it cannot prove a vendor snapshot was genuinely captured on its named date or contains every historically eligible issuer.

Task 2 accepts no provider calls and requires a supplied `daily_basic_YYYYMMDD.csv` for every requested date, preserves each source under `sources/`, rejects fabricated/malformed listing information and invalid caps, applies `list_date <= as_of < delist_date` (where a delisting date exists), and deterministically ranks eligible names by circulating market value (`circ_mv`) then ticker.

Task 2.5 is the required **capture-provenance gate** before Task 3. Every supplied CSV needs a hash-bound receipt. Where `OBSERVED_CAPTURE` is admitted, its receipt must bind a safe, hash-valid raw provider response copied under `raw/`; this is a conditional contract, not an assertion that a historical raw response exists today. Existing historical Tushare snapshots may instead be admitted solely as `TRUSTED_HISTORICAL_ASSUMPTION`, with literal caveat `historical_tushare_trusted_assumption`: trusted history is not independently capture-proven. This is a permanent evidence limitation, never `PIT_CAPTURE_VERIFIED`. Receipts are copied under `attestations/`, all ancillary payloads are manifest-hashed, and a mixed observed/trusted bundle remains `TRUSTED_HISTORICAL_ASSUMPTION`.

## Tushare source audit — 2026-08-07 close

Artifact: `outputs/source_data_audit_2026-08-07.json`.

- `daily_basic`: 5,535 records. It returned `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `dv_ratio`, market-cap and liquidity fields; availability varies by field and negative/undefined earnings are not represented as usable positive P/E.
- `fina_indicator`: the current token rejected a period-wide request without `ts_code`; this is an access/endpoint-feasibility constraint, not proof that the endpoint is intrinsically ticker-scoped. A deterministic top-30-market-cap connectivity sample returned 30/30 rows, but individual fields have structural missingness, one stored row had a null `ann_date`, and filings ranged 2026-04-08 to 2026-05-17.
- The audit proves current endpoint access and field availability only. It does **not** prove point-in-time historical completeness, vendor revision behavior, or alpha.

### Required quality-value join

For every signal date and ticker:

1. take pricing/valuation fields from that market date;
2. select the originally available record with `ann_date <= signal_date`, retain vendor update/version metadata and prevent a later restatement from overwriting the historical panel;
3. use a declared common-period or TTM convention—never rank Q1 and H1 values in one cross-section;
4. retain `ann_date`, `end_date`, update/version metadata, response hash, requested fields and source; hard-reject null `ann_date` for PIT use;
5. report missingness by sector and board, use a separate financial-sector specification, and state the non-random deletion caused by every filter. Do not silently impute a favorable value.

## C2 quality-value hypothesis (not an investment recommendation)

A stock can be labelled a *research candidate* only when it is cheap **and** financially viable. The preliminary deterministic inputs are:

- valuation: positive `pe_ttm`, `pb`, `ps_ttm`, and dividend yield where applicable;
- profitability: `roe` / `roa`, net-profit and gross-profit margins;
- cash conversion: `ocf_to_or` and available operating-cash-flow fields;
- balance sheet: `debt_to_assets` plus any sector-specific exclusions;
- risk controls: liquidity, ST/suspension/listing-age, concentration and sector neutrality.

This is **not a new factor discovery**: the stored price-only `xsec:value` control already trailed CSI300 over its full sample. C2 asks only whether an announcement-timed quality overlay changes that answer. Exact percentiles, rebalance frequency, sector treatment and score weights remain **unsealed** until the PIT/execution implementation review is complete. Before outcomes are opened, an amendment must freeze a calendar holdout, effect-size/Sharpe thresholds, a cap on specifications and a multiple-testing adjustment across arms.

## C3 event/revision hypothesis (not an investment recommendation)

Public research opinions and broker ratings are leads only. This is initially a **feasibility probe**, not a return sleeve: it must prove an actual pre-open availability timestamp rather than date-only `ann_date`, preserve issuer/version/revision state, and quantify discrepancies between planned and actual disclosure timing. Only then may a mechanical event sleeve be preregistered. Its later binding test would include a matched random-entry long control with the same calendar, universe, liquidity and bracket assumptions. No NLP/document scoring or analyst-opinion interpretation will be added until the mechanical base-rate gate clears.

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

1. clean, reproducible run from a PIT-grade frozen input bundle, with no unresolved PIT or data-quality failure;
2. full-sample and a predeclared calendar holdout clear the preregistered net excess-return and Sharpe-delta thresholds relative to the appropriate benchmark, after costs and realistic fills;
3. rank/control evidence shows the declared signal, rather than equal-weight, survivorship, market beta or a single regime, explains the result;
4. no catastrophic concentration, drawdown or turnover failure; and
5. it then completes a new forward paper sample without rule changes.

Passing historical gates permits only a labelled **parallel paper** sleeve. It never silently replaces the daily selector.

## Delivery status and remaining prerequisites

**Completed infrastructure tasks:**

- **Tasks 1–2.5:** self-consistent, hash-bound supplied bundles/schedules, deterministic membership construction, and receipt-bound capture provenance. These do not prove a vendor snapshot is authentic or complete; `OBSERVED_CAPTURE` is operator asserted, and existing historical receipts are never `PIT_CAPTURE_VERIFIED`.
- **Tasks 3–4:** membership-checked, provenance-labelled replay accounting with explicit fills/no-fills/censoring and honest denominators; this is not live execution P&L or alpha evidence.
- **Task 5:** portable, hash-bound B1 inventory (`outputs/top1_paper/baseline_inventory_2026-08-09.json`, SHA-256 `4200f732444a2efae5fde11bc04bcfc61cd9c86bd3cf4820e23ce6a4825c4e02`). It remains legacy, non-PIT, non-execution and non-promotable.
- **Task 6:** infrastructure-gate documentation is recorded in `docs/research/2026-08-10-infrastructure-verification.md`. Its independent Hawk/Opus review passed after the documentation draft: `claude -p … --model claude-opus-5` returned **APPROVE** after checking citation/provenance, the capture limitation, B1's regime limitation, and the absence of production/trading scope. That approval clears only the review prerequisite; it does not authorize challenger-outcome interpretation.

The Task 5 inventory's mechanical regime composition is 33/35 bull (94.2857%), 2/35 choppy and no bear. This does not invalidate or reclassify the inventory, but B1 can support only bull-heavy comparator descriptive claims—not cross-regime alpha, replacement or promotion.

**Exact remaining prerequisites:**

1. Source-grounded strategy and valuation-opinion register with dates/URLs and evidence tier.
2. PIT-grade universe/data-bundle design, including delisted/suspended issuer treatment; do not repurpose the current non-PIT freezer.
3. Repair/specify the replay harness: next-session executable fills, limits/suspensions/delists/no-fill and finite capital.
4. Tushare financial-join coverage report by sector/board, common-period rule, `ann_date`/revision provenance and request-rate budget.
5. Freeze/backfill the incumbent only through a named date, retaining its legacy/non-execution and regime limitations, **before comparing any challenger**.
6. Run the C3 availability feasibility probe. Only after steps 2–4 are complete, seal C2's valid input fit, parameters, full sample, calendar holdout, and thresholds before opening outcomes.

Hawk approval is complete. C1/C2 or later challenger results remain **blocked from interpretation** until the applicable remaining prerequisites—especially valid input fit plus sealed preregistered parameters, full sample, calendar holdout, and thresholds—are complete under the binding decision gates.
