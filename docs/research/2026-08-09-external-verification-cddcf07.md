# External Verification — Task 4 Provenance-Labelled Replay Runner

**SHA:** `cddcf07cb954f863d9cfae8ca2a48f4470422501`
**Method:** clean `git clone` on a separate machine from the builder.
**Suite:** `tests/test_xsec_runner_adversarial.py` — 61 cases.

## Verdict: PASS

| Run | Result |
|---|---|
| Adversarial suite vs SHA | **61 passed** |
| All six verification suites vs SHA | 312 passed |
| Mutation testing — adversarial suite | **12 / 13** after closing a gap (11/13 initially; 1 equivalent mutant) |
| Mutation testing — first-party suite | 10 / 13 |

My five earlier adversarial files are byte-identical to `8def742`.

Task 4 is the first stage that emits numbers, so verification concentrated on the two ways a clean pipeline still produces a misleading result: **labelling that overstates**, and **denominators that convert absence into evidence**. Both hold.

## Labelling honesty

- No bundle → `PIT_GRADE_FALSE` / `FROZEN_SELECTIONS_NON_PIT`. Valid bundle → `PIT_UNIVERSE_MEMBERSHIP_ONLY`, which is the strongest label this pipeline can earn.
- `evidence_label` is always `RESEARCH_ONLY_NON_BINDING`; `selection_execution_provenance` is always `CALLER_ASSERTED_UNVERIFIED`.
- **A caller cannot inject a stronger label.** Passing `pit_grade`, `evidence_label`, `input_mode` and `selection_execution_provenance` in the input row leaves the output labels unchanged.
- Even with a fully valid bundle, no output value contains `OBSERVED_CAPTURE` — the bundle's own grade is trusted-historical, and the runner does not launder it upward.
- A zero-rebalance run still emits a fully labelled `run_summary` rather than an unlabelled artifact.

## Denominator honesty

- `filled_mean_*` divides by the **filled** count, verified against a two-name/one-fill case: the mean is the filled leg's return, not that return halved.
- An all-censored rebalance reports `null` means, **not `0.0`**. This was my flagged Task 3 carry-forward and it is correctly implemented.
- Summary counts reconcile with emitted rows: `selected == filled + no_fill + censored`, and censored names appear in the artifact rather than being dropped.

## Other verified properties

- **Exclusive publication.** `os.link` refuses to overwrite; a second run raises `FileExistsError` and leaves the original artifact byte-identical. No staging files leak on success or refusal.
- **Durability honesty.** An injected post-link directory-fsync failure raises `ArtifactDurabilityUncertainError`, names the artifact, and **does not delete it** — the dishonest failure mode would be to erase it and imply nothing was written.
- **Validation precedes side effects.** A rejected run does not create the output directory.
- **Capacity determinism.** Assignment is independent of caller list order, follows the documented rank rule, and breaks score ties by ticker ascending. `capacity_rule` and `cost_assumption` are both disclosed in every record — my two Task 3 scope observations are now explicit machine-readable constants.
- **PIT membership enforcement.** A ticker or date absent from the frozen bundle is rejected. Verified end to end against a real Task 2 bundle carrying a Task 2.5 capture receipt.

## Findings

### K1 — First-party suite misses caller label injection (MEDIUM)
Mutating `"pit_grade": pit_grade` to echo `rebalance.get("pit_grade", pit_grade)` passes the entire first-party suite. The implementation is correct, but nothing there pins it. This is the single highest-consequence regression available in this file: it would let an input row silently relabel its own evidence grade, and every downstream reader trusts that field.

### K2 — First-party suite misses the mean-denominator swap (MEDIUM)
Mutating `filled_mean_gross_return` to divide by `result.totals.selected` instead of `len(filled)` passes the first-party suite. That is precisely the arithmetic by which missing data dilutes toward zero and "no data" is read as "no edge". Worth a dedicated test, because it is the mistake most likely to be reintroduced by a well-meaning refactor.

### K3 — Legacy compatibility view is labelled, but not for the right defect (LOW/MEDIUM)
`scripts/xsec_sleeves.py` retains `fwd.reindex(...).dropna().mean()` and `0.0 if pd.isna(r) else r` (lines 232–240). Per spec the legacy path is permitted to remain as a labelled compatibility view, and the labelling is better than expected: output is opt-in via `--legacy-output-dir`, filenames carry `_legacy_non_pit`, and a `LEGACY_NON_PIT_RESEARCH_ONLY` banner prints to stdout.

Two gaps remain. The CSV **contents** carry no provenance column, so the label survives only as long as the filename does. And the label describes the input (`non_pit`) rather than the arithmetic: a period in which every held name has a missing forward return contributes a flat 0% to the equity curve rather than being censored, which biases the curve toward flat and understates realised volatility and drawdown.

Recommend an in-file provenance column and a label that names the actual defect — `SILENT_DROP_BIASED` rather than only `non_pit`. Anyone benchmarking new canonical output against the legacy curve is comparing against a number produced by the exact method Task 3 exists to replace.

### K4 — My own suite had no PIT-bundle coverage (INFO, closed)
Removing membership enforcement entirely went undetected by my suite, because I had built no bundle fixture. Closed with five cases that construct a real Task 2 bundle with a Task 2.5 receipt and drive it through the runner — which also gives the first genuine Tasks 1+2+2.5→4 integration coverage.

### Equivalent mutant (INFO)
Removing the runner's duplicate-ticker guard changes nothing: the replay core rejects it independently. Redundant layering, not a hole.

## Recommendation

**Approve Task 4.** Carry K1 and K2 into the first-party suite before Task 5, since both are silent-corruption regressions in the highest-trust fields. K3 is a labelling improvement, not a blocker.

Standing status: Tasks 1–4 now make an eventual result *auditable* — point-in-time universe membership, capture provenance, executable fills, honest denominators, non-overwriting artifacts, and labels that refuse to overstate. None of it is evidence of an edge. The first number this pipeline emits should be read as a correctly-measured number, nothing more.
