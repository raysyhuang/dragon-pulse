# Hawk Review — A-Share Reliability Scan (2026-08-09)

**Reviewer:** Hawk via Claude Code `opus` (Claude Code 2.1.226)  
**Verdict:** **MODIFY** — do not build C2/C3 or interpret a new factor result yet.

## Verified blockers

1. `scripts/freeze_input_bundle.py` explicitly emits `pit_grade: false` and says its cap universe is frozen at capture time, not historical PIT. It cannot satisfy a historical PIT gate.
2. `scripts/xsec_sleeves.py` currently derives returns from the signal/rebalance-date close to the next rebalance-date close and silently drops missing held-name returns. It lacks T+1/open fill, price-limit no-fill, suspension/delist treatment and finite-capital accounting.
3. `ann_date <= signal_date` alone is insufficient: financial records may be revised, `ann_date` can be null, and mixing Q1/H1 values makes cross-sectional quality ranking invalid.
4. Existing `xsec:value` is the mandatory price-only control: its stored full-cycle result is below CSI300 (75.0% total / Sharpe 0.35 versus CSI300 109.2% / 0.39). A quality-value proposal must be framed as an ablation of that failed ancestor, not a fresh factor discovery.
5. The daily baseline must be frozen/backfilled with immutable source artifacts before it can act as a comparator; it currently has 35 evaluated rows through 2026-07-02 while later dated scans exist.
6. C3 is a **data-feasibility probe** only until an actual pre-open disclosure timestamp (not a date-only field) and revision/version handling are proven.

## Required amendments adopted

- No historical return run until a PIT-grade universe/data-bundle plan exists.
- Financial-quality specification must state update/version logic, common-period/TTM convention, sector/board coverage and the cost of missing-field exclusions.
- A future C2 protocol must freeze a calendar holdout, economic effect threshold, multiple-testing scope and forward sample length before outcomes are opened.
- Literature citations remain bibliographic leads when full text was not inspected; do not claim their exact factor construction or findings as verified.

## Result

The current evidence supports improving **measurement infrastructure first**, not modifying the selector and not adding an “undervalued” ranker.
