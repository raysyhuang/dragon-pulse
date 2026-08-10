# Task 6 — Infrastructure Verification Gate (2026-08-10)

**Scope:** documentation-only evidence gate for the A-share replacement protocol. It changes no pipeline, selector, cron, production setting, provider call, execution rule, or order authority.

## Gate result

| Item | Status | Evidence / boundary |
|---|---|---|
| Task 1 — PIT bundle integrity validator | **PASSED** | External verification recorded for `104bdf5`; it validates the supplied bundle's hashes, schedule/source membership, and stated PIT grade—not vendor authenticity or completeness. |
| Task 2 — PIT universe schedule builder | **PASSED** | External verification recorded for `0ae670c`; deterministic supplied-snapshot schedule construction, without provider calls. |
| Task 2.5 — capture-provenance receipts | **PASSED** | Initial external gate `4173788` passed with H1 identified; H1 containment repair was re-verified at `decf447`. A receipt admitted at `OBSERVED_CAPTURE` must bind a supplied raw, hash-bound response, but the grade remains operator-asserted. |
| Task 3 — replay accounting | **PASSED** | External verification recorded for `c410d20`; executable-fill/no-fill/censor accounting is implemented and denominators are not presented as observations when all rows are censored. |
| Task 4 — provenance-labelled replay runner | **PASSED** | External verification recorded in `2026-08-09-external-verification-cddcf07.md` for `cddcf07`; frozen-bundle membership is enforced and replay outputs label provenance and non-promotable evidence. `8872008` added the verification note; it is not the reviewed implementation SHA. |
| Task 5 — incumbent baseline inventory | **PASSED** | M1/L1 repair re-verification is recorded in `2026-08-10-external-verification-52a3287.md` for `52a3287`, committed via `969246a`; `11a2078` adds first-party N1 coverage. The portable manifest SHA is `4200f732444a2efae5fde11bc04bcfc61cd9c86bd3cf4820e23ce6a4825c4e02`. |
| Task 6 — independent Hawk review of this documentation gate | **PASSED** | Completed after the documentation draft: an Opus review returned **APPROVE** for the Task 6 documentation scope. |
| Task 6 — authorization to interpret challenger outcomes | **BLOCKED** | Hawk approval clears only the independent-review prerequisite. No C1/C2/C3 or other challenger outcome may be interpreted until the arm also satisfies its remaining input-fit and preregistration prerequisites below. |

### Evidence provenance

“External verification recorded” refers to the committed review notes in `docs/research/` (not a new review in this task): Task 1 is `2026-08-09-external-verification-104bdf5.md` (`104bdf5`); Task 2 is `2026-08-09-external-verification-0ae670c.md` (`0ae670c`); Task 2.5's initial gate is `2026-08-09-external-verification-4173788.md` (`4173788`, PASS with H1 identified) and its H1 repair is `2026-08-09-external-verification-decf447.md` (`decf447`); Task 3 is `2026-08-09-external-verification-c410d20.md` (`c410d20`); and Task 4 is `2026-08-09-external-verification-cddcf07.md` (`cddcf07`). Task 5's original verification, `2026-08-10-external-verification-1a50c1e.md` (`1a50c1e`), passed with M1/L1 defects, leading to the repair and re-verification recorded in `2026-08-10-external-verification-52a3287.md` (`52a3287`), committed via `969246a`. That external record reports **358 passed** across all seven verification suites at `52a3287` / `969246a`. `11a2078` is a test-only N1 addition; the user reports the verifier files unchanged and results still green after it, but no externally run seven-suite result is attributed here to the `11a2078` state. The user separately reports a local full-suite run of **746 passed, 1 deselected**, with the expected pytest configuration warning. These are distinct claims: no local test or provider call was run for this documentation-only gate.

**Task 6 review evidence:** after this documentation draft, Hawk was invoked as `claude -p … --model claude-opus-5` and returned **APPROVE**. The Opus review checked citation/provenance boundaries, the capture limitation, B1's regime limitation, and the absence of production/trading scope; it identified no blockers for the Task 6 documentation scope. This records the command/model and verdict only—no new external review artifact is asserted. Lower-severity legacy cross-sectional compatibility concerns remain separately labelled and non-promotable outside Task 6/B1 scope.

## What the completed infrastructure does—and does not—establish

Tasks 1–5 provide an auditable capture-and-replay foundation:

- self-consistent, hash-bound supplied bundles and schedules;
- membership-checked replay inputs;
- operator-attested capture receipts: where `OBSERVED_CAPTURE` is admitted, its receipt must bind a supplied raw response by hash;
- executable replay accounting with explicit fills, no-fills, censoring and honest denominators; and
- a portable, hash-bound inventory of the incumbent baseline.

They **do not** certify vendor-snapshot authenticity, historical completeness, third-party notarization, live/execution P&L, or alpha. In particular, where `OBSERVED_CAPTURE` is admitted, the contract requires a raw, hash-bound response; this documentation does not assert that such a raw response exists in any historical record today. No existing historical record is `PIT_CAPTURE_VERIFIED`; the `TRUSTED_HISTORICAL_ASSUMPTION` receipt grade remains permanently caveated. A portable inventory hash detects changes to the covered inventory representation; it does not independently authenticate the historical sources or the tracker labels.

## Baseline inventory and preregistered regime limitation

The frozen B1 inventory is `outputs/top1_paper/baseline_inventory_2026-08-09.json` (`LEGACY_NON_PIT_BASELINE_INVENTORY`, `NON_EXECUTION_NON_PROMOTABLE`). Its portable SHA-256 is:

```text
4200f732444a2efae5fde11bc04bcfc61cd9c86bd3cf4820e23ce6a4825c4e02
```

It contains 35 ledger rows and 35 resolved native artifacts: 24 tracker-labelled `filled`, 11 tracker-labelled `no_fill`, and 0 recorded `censored`/`unknown`. These are saved tracker labels, **not verified execution**. Its last evaluated scan is 2026-07-02, so the baseline is stale under the protocol.

**Mechanical regime record:** 33/35 bull (**94.2857%**), 2/35 choppy, and 0 bear. This is a preregistered limitation, not a reason to alter the inventory or reclassify any record. Comparisons to B1 may support only **bull-heavy comparator descriptive claims**. They cannot decide robust cross-regime alpha, replacement, or promotion.

## Preregistered Task 6 interpretation rule

1. The independent Hawk review has approved this infrastructure gate; that approval alone does not authorize interpretation of any challenger outcome.
2. For every C1/C2 or later return-bearing arm, require valid input fit and the protocol's preregistered thresholds on the full sample and calendar holdout before interpreting comparative results.
3. Apply the protocol's common replay rules, costs, fills, capacity, provenance and evidence grades. A clean inventory or replay receipt does not waive a PIT, execution, or alpha requirement.
4. B1's bull-heavy composition may be reported descriptively, but cannot be used to infer cross-regime robustness or to promote/replace the incumbent.

## Remaining prerequisites after Tasks 1–6

Before an admissible challenger comparison or any promotion discussion:

1. complete the source-grounded strategy and valuation-opinion register;
2. create a PIT-grade universe/data-bundle design with delisted and suspended issuer treatment—without repurposing the current non-PIT freezer;
3. complete/specify the production-like replay harness for next-session executable fills, limits, suspensions, delists, no-fills and finite capital;
4. produce financial-join coverage by sector/board with common-period, `ann_date`, revision provenance and request-rate budget;
5. freeze/backfill the incumbent only through a named date, retaining its legacy/non-execution and regime limitations;
6. complete the C3 availability feasibility probe; only then seal C2 parameters/thresholds, full sample and calendar holdout before opening outcomes; and

Passing these gates could permit only a labelled parallel-paper sleeve under the protocol; it does not authorize a production-selector change.
