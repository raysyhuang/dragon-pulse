# PIT Selection Test v2 — Corrected After External Audit (2026-08-10)

**Supersedes** `2026-08-10-pit-selection-test-null.md`, whose conclusion was correctly
rejected. That note's *direction* survived re-testing; its *evidentiary standing* did not,
and its closure claim was withdrawn.

**Status:** research / paper-only. Changes no selector, cron, execution rule, or order authority.

## What the audit found, and what was done

| Blocker | Status |
|---|---|
| Not PIT — runner called without a validated bundle, output `PIT_GRADE_FALSE` | **Fixed.** Universe is now read *from* a validated bundle and the bundle is passed to the runner. Artifacts read `PIT_UNIVERSE_MEMBERSHIP_ONLY`. |
| No immutable evidence artifact | **Fixed.** Hashed `analysis.json`, non-overwriting publication, nothing deleted. |
| Headline statistics not computed or persisted by the script | **Fixed.** Beta, alpha, t, up/down attribution and half-splits are computed in-script and hashed. |
| Execution realism disabled | **Bounded, not fixed** — see the sensitivity below, which changed the conclusion about this blocker. |
| Sample not frozen (`date.today()`) | **Fixed.** `FROZEN_END = 20260630`, explicit. |
| Conclusion overreaches | **Accepted and withdrawn.** See "What this does not close". |

The bundle: `pit-universe-2021-01-29-to-2026-06-30-n1000`, composite
`fd59dfa40eb183df620d15c4fb7e739456244cf4acfb187c318cd29d7f993f0d`, 66 dates,
66,000 schedule rows, **604 carrying a real delisting date**, capture grade
`TRUSTED_HISTORICAL_ASSUMPTION`.

## Results — artifact-bound, PIT-bound, frozen

<!-- BEGIN GENERATED: results -->
| Sleeve | mo | filled | cens | CAGR | beta | annual alpha | t | p | H1 (post-hoc) | H2 (post-hoc) |
|---|---|---|---|---|---|---|---|---|---|---|
| turnover_low | 65 | 3245 | 5 | +0.24% | 0.52 | +3.55% | 0.68 | 0.50 | +6.12% | -5.16% |
| size_small | 65 | 3244 | 6 | -3.97% | 0.98 | +2.23% | 0.61 | 0.54 | -4.54% | -3.43% |
| value_pb | 65 | 3244 | 6 | -5.17% | 0.63 | -1.05% | -0.17 | 0.86 | -2.89% | -7.33% |
| momentum_12_1 | 53 | 2648 | 2 | -5.38% | 1.00 | +6.24% | 0.50 | 0.62 | -19.96% | +11.17% |
| dividend | 65 | 3249 | 1 | -5.41% | 0.62 | -1.27% | -0.21 | 0.84 | -1.74% | -8.84% |
| **control_spread** | 65 | 3246 | 4 | -5.87% | 1.00 | +0.00% | — | — | -8.61% | -3.14% |
| value_pe | 65 | 3249 | 1 | -6.36% | 0.44 | -3.42% | -0.57 | 0.57 | -10.81% | -1.83% |
| reversal_1m | 64 | 3199 | 1 | -12.81% | 1.22 | -3.98% | -0.77 | 0.44 | -19.09% | -6.04% |
<!-- END GENERATED: results -->

<!-- BEGIN GENERATED: verdict -->
**No factor is statistically significant.** The largest intercept t-statistic in the study is **0.68375** (`turnover_low`, p = 0.49664, df = 63); the smallest two-sided p-value across all sleeves is **0.44446**. The half-sample columns are a POST-HOC split of a single sample and are not an out-of-sample holdout.
<!-- END GENERATED: verdict -->

<!-- BEGIN GENERATED: turnover_attribution -->
`turnover_low` remains the only non-negative sleeve and remains a low-beta portfolio (0.52) that earns -2.75% in up months and +2.74% in down months. Its post-hoc half-split is **positive then negative** (+6.12% → -5.16%).
<!-- END GENERATED: turnover_attribution -->

## Execution sensitivity — and a correction

The frictionless case was defended in v1 as "conservative for a null," on the reasoning
that constraints only reduce factor returns. **That reasoning was wrong and the persisted
evidence below refutes it.**

<!-- BEGIN GENERATED: sensitivity -->
| Config | slots | turnover_low fills | control fills | cap-censored | turnover_low | control | excess |
|---|---|---|---|---|---|---|---|
| no_cap_50_slots | 50 | 3245 | 3246 | 0 | +0.24% | -5.87% | +6.11% |
| cap_1.02x_20_slots | 20 | 598 | 629 | 1950 | +0.18% | +0.49% | -0.30% |
| cap_1.00x_10_slots | 10 | 298 | 165 | 2600 | +3.46% | -5.61% | +9.07% |

Constraints do not move the estimate monotonically. Relative to the no-cap turnover_low fill count (3245), constrained configurations fill 598, 298 positions; excess flips sign across the persisted configurations. Under these execution constraints the effect is not identified here; this is exploratory evidence, not a promotion basis.
<!-- END GENERATED: sensitivity -->

## What this does and does not support

<!-- BEGIN GENERATED: evidence_summary -->
**Supports:** across these 7 sorts, on a bundle-validated survivorship-controlled universe over 65 monthly rebalances, no factor produced significant alpha. The non-negative sleeve(s) are turnover_low; sleeves beating the control in more than half their observed months have beta between 0.44 and 0.63. `turnover_low` reverses in the POST-HOC second half and is unidentified under the execution constraints above.
<!-- END GENERATED: evidence_summary -->

<!-- BEGIN GENERATED: scope -->
**Does not support**, and the v1 claim to the contrary is withdrawn: this does **not** close "standard A-share factor selection." Exploratory evidence about these seven sorts over this window; does not close a-share factor selection. 7 hand-chosen sorts at a single parameterisation over a 5.4-year window cannot settle that question. Limitations remain: none: no sector, industry or size neutralisation; POST-HOC split of one sample; NOT a sealed out-of-sample holdout; multiple testing: none applied across seven sleeves. It remains research / paper-only and must not alter a selector, cron, execution rule, or order authority.
<!-- END GENERATED: scope -->

It also does not test `alpha_rs_pullback`, and it says nothing about the timing sleeve,
which stands or falls on its own evidence.

## Reproduce

```
python scripts/pit_selection_test.py <work_dir>
```
Expects a validated bundle at `<work_dir>/pit_bundle_66` and cached panels at
`<work_dir>/seltest_cache`. Refuses to overwrite an existing `analysis.json`.

<!-- BEGIN GENERATED: provenance -->
- analysis self-hash `60a245ebfb8dbc73284dd2aa855b6a299d3e788798276a42cda15af7fd99719f`
- bundle `pit-universe-2021-01-29-to-2026-06-30-n1000` composite `fd59dfa40eb183df620d15c4fb7e739456244cf4acfb187c318cd29d7f993f0d`
- study script `bce1a1772c844a01c786bf977a55727d5f7695bbdbd5a7ccc07239e62d7feb27`
- trade calendar `23dca116e2d25935b77ef2b88ecc4fa74a87753cbdb38305029cc76116d919ed` (tushare trade_cal, exchange=SSE, schema `cal_date,is_open`)
- frozen plan `b100bdd223a0a0d1664ee77f1cc26749425c3ec7ebad3d48b364791ed3c5e5ac` — 65 rebalances, 20210129 .. 20260529
- raw inputs hash-bound but **not committed**: 131 daily panels, 66 daily_basic panels
- capture grade `TRUSTED_HISTORICAL_ASSUMPTION`; replayability: ACCOUNTABLE OUTCOMES, NON-REPLAYABLE SELECTION INPUTS.
<!-- END GENERATED: provenance -->

---

## Addendum — second audit round (`dfe240a` → this commit)

A second audit rejected `dfe240a`. All six findings were valid and are addressed here.

| Finding | Resolution |
|---|---|
| Only the manifest committed; schedule, snapshots, receipts, canonical JSONL absent | **Fixed.** `outputs/pit_selection_v2/bundle/` now holds the full bundle — `universe_schedule.csv`, all 66 source snapshots, all 66 capture receipts, manifest. `outputs/pit_selection_v2/canonical/` holds the canonical JSONL for all 8 sleeves and all 6 sensitivity runs. 19 MB total. |
| `analysis.json` not strict JSON (bare `NaN`) | **Fixed.** Serialised with `allow_nan=False`; undefined values are `null`. Verified to parse under a strict constant handler. |
| Publishing is check-then-write, not atomic | **Fixed.** Writes to a temp file, `fsync`s, then `os.link` — no-replace with no TOCTOU window, matching the Task 4 pattern. |
| Sensitivity table absent from the executable script | **Fixed.** The grid is computed inside `main()` and persisted to `execution_sensitivity`, with its own canonical JSONL per configuration. It was previously an ad-hoc shell session — the same error the first audit flagged, repeated. |
| Reported t is not an OLS intercept t-statistic | **Fixed.** Proper OLS with `s² = SSR/(n−2)` and `SE(α) = s·√(1/n + x̄²/Sxx)`; specification, SE, df and two-sided p are persisted. The prior figure omitted both the df correction and the leverage term. |
| Documentation overclaims | **Fixed.** "survivorship-free" → "survivorship-controlled"; the half-split is labelled post-hoc, explicitly not out-of-sample; the stale "frictionless is conservative" header is replaced with the correction; the v1 banner no longer claims the note is "unaltered" while carrying a prepended banner. |

<!-- BEGIN GENERATED: addendum_statistics -->
**Corrected statistics** (OLS; the largest available regression df is 63) are generated from `analysis.json`, not hand-written. No two-sided p-value in the study is below 0.44; the smallest is 0.44446. **Status is unchanged by these fixes:** exploratory; this does not close the selection question.
<!-- END GENERATED: addendum_statistics -->

**Known remaining limitation, stated rather than omitted:** the raw cached provider panels
(~38 MB of daily OHLCV and `daily_basic` parquet) are **not** committed. The bundle and
canonical JSONL make the universe construction validatable and every entry/exit price and
outcome auditable, so all accounting can be checked without them. Re-deriving the
*selection* step from raw vendor data requires re-fetching. That is a deliberate
size/traceability trade-off and it is an auditor's call whether it is acceptable.

The generated block above states the statistics and exploratory status derived from the artifact.

---

## Addendum — third audit round

A third audit returned MODIFY on three blockers. All were confirmed independently and all are repaired.

**Provenance was stale.** Canonical rows recorded `runner_git_commit=dfe240a` with a dirty tree, and nothing bound the study script or the raw panels. The script is now committed *before* the run, so the regenerated artifacts record `runner_git_commit=e31aa7f` with `runner_tree_dirty=false`. An `input_binding` block now carries the study script's own SHA-256 and the SHA-256 of every raw panel consumed, so inputs are cryptographically bound even though the bytes are not committed.

**The document misreported its own evidence.** It cited an obsolete analysis hash, and the results table was worse than misaligned: a header edit added a `p` column without updating the rows, so it displayed the superseded t of 0.70 and printed an up-month excess of −2.75% under a column headed `p`. Both tables are now **generated from `analysis.json`** rather than written by hand, which removes the class of error rather than the instance.

**One overclaim survived.** A line-wrapped "survivorship-free" escaped the previous pass and is corrected.

### Scope, stated as the audit requires

**Accountable outcomes, non-replayable selection inputs.** Canonical rows carry every executed entry and exit price, so all accounting is auditable from this repository alone. Selection construction is *not* reproducible here, because the raw provider panels are hash-bound but not committed. The runner's `CALLER_ASSERTED_UNVERIFIED` label is correct and remains limiting. No factor conclusion and no selection closure follows from this study.

### Execution sensitivity (generated)

The generated execution-sensitivity block above is the sole artifact-owned statement of its rows and numerical interpretation.