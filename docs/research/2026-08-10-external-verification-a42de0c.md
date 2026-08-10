# External Verification — Selection Study Evidence Boundaries (`a42de0c`)

**Method:** clean `git clone` on a separate machine from the builder.

## Verdict: PASS — cleared

| Check | Result |
|---|---|
| Renderer `--check` | passes |
| Focused integrity tests | **8 passed** |
| Seven external verifier suites | **358 passed**, byte-identical to `aaf93c4` |
| Mutation testing — new contracts | **6 / 6 caught** |
| End-to-end drift proof | tampered `analysis.json` → `--check` exit 1 → restore → pass |

## Mutation results

| Mutant | Caught |
|---|---|
| `--check` always reports success | ✅ |
| renderer skips block replacement | ✅ |
| publisher drops directory fsync | ✅ |
| publisher swallows the durability error | ✅ |
| publisher allows replace (`link` → `replace`) | ✅ |
| artifact deleted on durability failure | ✅ |

The publication contract now matches the runner's on every dimension tested: exclusive
`os.link`, directory fsync, a named durability-uncertain error, and the artifact preserved
rather than removed when durability is in doubt.

## The claim that went further than required

The audit asked that material evidence blocks be renderer-owned. The document now carries
**eight** generated blocks — `results`, `verdict`, `turnover_attribution`, `sensitivity`,
`evidence_summary`, `scope`, `provenance`, `addendum_statistics` — where the external
verifier had introduced four.

I checked specifically for live current-study statistics left outside renderer-owned
regions, since that is the class of defect that recurred across four rounds. The only
statistic-shaped text outside a generated block is the historical passage describing the
superseded t of 0.70 and the mislabelled p-column. That passage **should not** be
generated: it is a record of a past error, and regenerating it would erase the audit
trail. Correct as written.

The unsupported v1-bias inference is removed. It asserted that v1 carried "mild optimistic
bias," which was never established — only that the numbers differed under a corrected
method. Removing it is right.

## Status

The selection study may now be frozen as **exploratory / non-promotable**, with scope
**accountable outcomes, non-replayable selection inputs**. Its substantive result is
unchanged and unchanged by any round of this audit: no factor significant, largest
intercept t = 0.68375 (p = 0.49664, df 63), smallest p-value 0.44446.

Four audit rounds altered no number. They changed what the document is entitled to claim
and whether a reader can check it — which was the point.

**Gate cleared.** Nothing in the selection track blocks scorecard or timing work.
