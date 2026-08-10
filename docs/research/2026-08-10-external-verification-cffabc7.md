# External Verification — Task 6 Infrastructure Verification Gate

**SHA:** `cffabc7a4703bb4e409725407503688037fdb30e`
**Method:** clean `git clone` on a separate machine from the builder. Documentation-only commit; `src/`, `scripts/`, `tests/` and `outputs/` are byte-identical to `11a2078`.

## Verdict: PASS

A documentation gate is verified by auditing its citations and numbers rather than by mutating code. Everything checkable resolved.

| Audit | Result |
|---|---|
| Cited SHAs resolving to the commit the text names | **11 / 11** |
| Cited verification documents present in `docs/research/` | **8 / 8** |
| Numeric claims recomputed from source data | **14 / 14** |
| Seven verification suites re-run at the gate SHA | **358 passed** |

### Numeric claims independently recomputed

From `outputs/top1_paper/ledger.jsonl` and the frozen inventory, not from the gate text: 35 ledger rows; 35 resolved native artifacts; 24 tracker-labelled filled; 11 no-fill; 0 censored; 0 unknown; 33 bull; 2 choppy; **0 bear**; 33/35 = **94.2857%**; last evaluated scan **2026-07-02**; portable manifest `4200f732…`; `LEGACY_NON_PIT_BASELINE_INVENTORY`; `NON_EXECUTION_NON_PROMOTABLE`.

### Characterisations checked against the underlying verifications

No row overstates what was verified. Three are worth confirming explicitly because they are the easiest places to drift:

- **Task 2.5** — "a receipt admitted at `OBSERVED_CAPTURE` must bind a supplied raw, hash-bound response, but the grade remains operator-asserted" matches the probe result exactly: a fabricated raw payload with a matching receipt still grades `OBSERVED_CAPTURE`.
- **Task 3** — "denominators are not presented as observations when all rows are censored" matches the verified behaviour: means are `null`, not `0.0`.
- **Task 4** — the note correctly distinguishes `8872008` (the commit adding the verification note) from `cddcf07` (the reviewed implementation). That distinction was one of the citation defects the internal review caught, and it is now right.

No open finding is concealed. The remaining lower-severity legacy cross-sectional item (K3, in-file provenance labelling of the legacy CSV) is acknowledged as separately labelled and out of Task 6 scope rather than omitted.

## Findings

### P1 — The `11a2078` state was externally verified; the gate understates it (LOW, corrected by this note)
The gate states that "no externally run seven-suite result is attributed here to the `11a2078` state." That was accurate when drafted but is now stale: the `11a2078` state **was** externally verified on a separate machine, with the seven verification suites at **358 passed**, and N1 closure confirmed by mutation — the four portable-view exclusions that previously passed the first-party suite (dropping `summary`, the evidence/promotion labels, the artifact-evidence hashes, and the capture caveat) are now caught, taking it from 2/6 to **6/6**.

The error direction is conservative — the gate claims less external verification than exists — but the trail is meant to be resolvable, and that result lived only in conversation. This note records it. The same seven suites also pass at the gate SHA `cffabc7` itself, at 358.

### P2 — Task 6's own review row is the only non-resolvable citation in the trail (OBSERVATION)
Every other row cites a committed document that a third party can open and check. The Task 6 review row cites a `claude -p … --model claude-opus-5` invocation and its verdict, with no artifact. The gate says so plainly — "this records the command/model and verdict only—no new external review artifact is asserted" — which is the honest framing rather than a defect.

It is still the weakest link in a trail whose value is resolvability. If the Hawk review is to carry gate-level weight, its output should be committed like the others. Recorded as an observation, not a blocker.

## Assessment of the gate's central claims

Both load-bearing statements are accurate and correctly bounded:

1. **Tasks 1–5 establish auditable capture-and-replay mechanics, not authenticity or alpha.** This matches every verification in the trail. Capture provenance is operator-asserted by design; no historical record is `PIT_CAPTURE_VERIFIED`; a portable inventory hash detects representation changes and authenticates nothing.
2. **B1 cannot support a cross-regime verdict.** 94.2857% bull, 0 bear, 35 scans, stale since 2026-07-02. Preregistering this *before* any challenger outcome exists is the correct sequencing, and it is the single most consequential line in the document — it is much harder to accept a comparator's limits after a favourable number has appeared than before.

## Recommendation

**Task 6 approved.** The infrastructure track is externally verified end to end: Tasks 1–6, seven adversarial suites, 358 cases, with every gate defect either repaired and re-verified or explicitly recorded as an accepted limitation.

The blocked status on challenger interpretation is correct and should hold. Nothing in Tasks 1–6 is evidence about returns; the achievement is that a future result can now be trusted to be measured correctly, which is a different and smaller claim than the one that will be tempting to make when the first number appears.
