# External Verification — Task 2.5 Capture-Provenance Attestation Gate

**SHA:** `4173788a12260919eb2341591dedafd79949002d` (confirmed tip via GitHub API)
**Method:** clean `git clone` on a separate machine from the builder.
**Suite:** `tests/test_capture_provenance_adversarial.py` — 60 cases, independent fixtures.

## Verdict: PASS with one confirmed defect (H1), which the builder already has a repair pending

| Run | Result |
|---|---|
| Adversarial suite vs SHA | **60 passed** |
| All Task 1 / 2 / 2.5 suites vs SHA | 287 passed |
| Mutation testing — adversarial suite | **14 / 14** after closing gaps (10/14 initially) |
| Mutation testing — first-party 2.5 suite | 8 / 14 |

**Regression:** all 79 Task 1 and 56 Task 2 adversarial cases still pass. Symlink containment did not weaken; the path rule became stricter (`_is_flat_ancillary_path` forbids nesting).

**G3 from Task 2 is fixed** — the manifest now records `builder_tree_dirty`, verified `true` against a dirty tree.

## What the gate genuinely enforces

Confirmed by mutation: snapshot↔receipt hash binding, literal `provider`/`endpoint`/`schema_version`, filename↔`requested_trade_date` agreement, capture-time ≥ trade date, UTC-only timestamps, the literal trusted caveat, symlink rejection on receipts and raw payloads, unsafe/nested `raw_response_file` paths, and post-build tampering of `attestations/` and `raw/`.

Grade integrity holds: a mixed observed/trusted set does **not** round up, and `PIT_CAPTURE_VERIFIED` is never emitted.

## Findings

### H1 — Legitimate sources directory rejected when an ancestor is a symlink (MEDIUM, repair pending)
`validate_capture_attestations` compares a **lexical** path against a **physical** root:

- `source_root = source_dir.resolve()` — symlinks resolved
- `snapshot = Path(os.path.abspath(snapshot))` — symlinks *not* resolved
- `_safe_file` then calls `path.relative_to(root)`

Any sources directory reached through a symlinked ancestor is rejected as `snapshot resolves outside allowed root`. Reproduced from the CLI with a plain symlinked parent directory: the physical path builds, the symlinked-ancestor path fails. This is not exotic — on macOS `/tmp` and `/var` are symlinks, so `--sources-dir /tmp/...` never works.

Direction of failure is **closed** (valid input rejected), so it is not a containment bypass. But mixing `abspath` and `resolve()` in a single comparison is the same inconsistency that becomes a bypass in the other direction, so the repair must be re-verified rather than assumed. The builder reports a relative-path repair already validated locally and in safety review; it is **not** in this SHA.

Re-verification on the repair should confirm both directions: legitimate symlinked-ancestor paths accepted, and mutation P13 (`_safe_file` symlink guard) plus the Task 1 symlink cases still caught.

### H2 — Validator-side grade re-derivation was untested by both suites (MEDIUM, now closed)
Four mutants survived both suites: rounding a mixed set up to `OBSERVED_CAPTURE` (P1), deleting the manifest grade cross-check (P2), deleting the caveat cross-check (P3), and letting a trusted receipt carry observed-tier `raw_response_*` fields (P10).

This is the most consequential gap found. The builder computes the grade, but the validator's **independent re-derivation** is what stops a hand-crafted bundle from simply declaring the stronger grade — and none of it was exercised. Closed by five added cases, including one that isolates the grade check so the caveat check cannot mask it.

### H3 — First-party suite misses two literal-field regressions (LOW)
Mutations P8 (`schema_version` literal) and P14 (naive-timestamp rejection) pass the first-party 2.5 suite. Implementation is correct in both.

### Process note — builder edited the verifier's test file
`tests/test_build_pit_universe_schedule_adversarial.py` was modified in `5f7b857` to emit capture receipts from the fixture helper. The change is legitimate plumbing — no assertion was altered or removed, and the Task 2 mutation catch rate is unchanged at 11/12 — but it was not flagged. A builder silently editing the verifier's suite is the one edit that can quietly disarm the gate. Convention worth adopting: fixture-only changes to verifier files get called out explicitly in the handoff message.

## Probes — designed boundaries, reported not asserted

- **`OBSERVED_CAPTURE` is self-asserted.** A wholly fabricated `raw/` payload plus a matching receipt yields a bundle graded `OBSERVED_CAPTURE`. The spec is explicit that there are no provider calls and no third-party notarization, so this is the intended scope. It binds a raw response to a snapshot; it does **not** prove the response came from Tushare. `OBSERVED_CAPTURE` must never be read as "independently proven".
- **Capture-time edges accepted:** midnight of the trade date, and a year-2999 timestamp. Only "not before the trade date" is specified, so neither is a defect. A far-future capture time is nonsense evidence, though, and an upper bound (capture ≤ bundle build time) would be cheap.

## Recommendation

**Approve the Task 2.5 design and gate.** Land the H1 relative-path repair and re-verify it before Task 3 consumes the builder — H1 makes the CLI unusable on default macOS temp paths today. H2 is closed. H3 and the future-timestamp bound are optional hardening.

The honest status line for everything downstream: Tasks 1, 2 and 2.5 together establish that supplied evidence is self-consistent, hash-bound, membership-checked, reproducible, and correctly graded — **and that the strongest available grade is still operator-asserted.** No result built on these bundles may be described as independently capture-verified.
