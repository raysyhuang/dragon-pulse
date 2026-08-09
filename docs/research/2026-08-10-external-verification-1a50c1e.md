# External Verification — Task 5 Top-1 Baseline Evidence Freezer

**SHA:** `1a50c1e10bc31a67ff4a06f256514aafee8bc51f`
**Method:** clean `git clone` on a separate machine from the builder.
**Suite:** `tests/test_freeze_top1_baseline_adversarial.py` — 34 cases (33 pass, 1 strict xfail documenting M1).

## Verdict: PASS with one open defect (M1) and one reproducibility gap (L1)

| Run | Result |
|---|---|
| Adversarial suite vs SHA | **33 passed, 1 xfailed** |
| All seven verification suites vs SHA | **345 passed, 1 xfailed** |
| Mutation testing — adversarial suite | **11 / 11** after closing gaps (9/11 initially) |
| Mutation testing — first-party suite | 7 / 11 |

Task 4 findings **K1 and K2 are both closed** in the first-party suite. My six earlier adversarial files are byte-identical to `8872008`.

## Independently reproduced the real inventory

I regenerated the committed inventory from the same tracked ledger and artifacts on this machine:

- **All 35 artifact SHA-256 values match exactly.** The ledger hash matches. Every substantive row field — `accounting`, `artifact_path`, `artifact_sha256`, `identity`, `ledger_record_sha256`, `reason`, `regime`, `resolvable`, `scan_date`, `status` — is identical.
- The `summary` block reproduces exactly: 35 rows, 24 filled / 11 no-fill / 0 censored / 0 unknown, 33 bull / 2 choppy, 35/35 `RESOLVED_NATIVE_ARTIFACT`.

The evidence is real and verifiable. See L1 for the one thing that is not.

## What the freezer genuinely enforces

Verified by mutation, not assumed: a missing artifact cannot be fabricated into a resolved one; the artifact↔ledger identity binding covers `date`, `sleeve`, `paper_only`, `status`, `regime`, `top1` and `top2`; `paper_only` cannot be dropped; the evidence grade and promotion status cannot be upgraded; rank-0 legs must bind to `top1.ticker`; evaluated/unevaluated contradictions are rejected; the manifest hash covers the emitted document; and FD-anchored `O_NOFOLLOW` rejects symlinked ledgers, artifacts, roots and intermediate directories.

Read-only behaviour holds: neither the ledger nor any artifact is modified, and nothing new appears under the artifact root. A second run refuses to overwrite.

## Findings

### M1 — A missing artifact *file* hard-fails instead of being inventoried (MEDIUM, open)
Behaviour is inconsistent by shape of absence:

| Case | Result |
|---|---|
| Whole date directory absent | `MISSING_NATIVE_ARTIFACT` — correct |
| Artifact file deleted, directory remains | **hard failure, entire inventory aborts** |

Root cause: `_open_relative_regular` returns `None` on `FileNotFoundError`, which is how missing evidence becomes `MISSING_NATIVE_ARTIFACT`. Intermediate directory opens raise `FileNotFoundError` and are caught. The leaf goes through `_open_regular`, which wraps every `OSError` into `BaselineInventoryError` — no longer a `FileNotFoundError`, so that handler cannot match it and the error propagates.

The Task 5 spec requires "a ledger row with no corresponding artifact becomes `MISSING_NATIVE_ARTIFACT`, never reconstructed." The deleted-file case is the realistic form of missing evidence and it is precisely what a partial-evidence inventory exists to record. The committed inventory resolved 35/35, so this path was never exercised on real data.

Fails closed, so no false evidence is produced. Encoded as a strict xfail; remove the marker once leaf `ENOENT` is passed through.

### L1 — The manifest hash is not reproducible off the originating machine (MEDIUM)
`manifest_sha256` covers environment metadata: the absolute `artifact_root` (`/home/agent/dragon-pulse/outputs`) and the git commit/dirty state. Regenerating from identical inputs elsewhere yields a different manifest hash while every content hash matches.

A manifest hash exists so a third party can confirm an artifact was not altered. As it stands, only the originating machine can do that. The fix is already this codebase's own idiom: `generator.git` is marked `advisory_only: true` — apply the same treatment to absolute roots, or canonicalise them to relative paths before hashing.

Also noted: the committed inventory was produced with `git.dirty: true`. Disclosed rather than hidden, which is the Task 2 G3 lesson correctly applied, and `generator.source_sha256` binds the actual generator bytes — that value does reproduce exactly.

### M2 — Symlinked intermediate directory raises an untyped error (LOW)
Containment holds, but the failure surfaces as a raw `NotADirectoryError` traceback rather than a typed `BaselineInventoryError`. Inconsistent with the module's own error discipline, where every other rejection is wrapped.

### M3 — First-party suite gaps (LOW/MEDIUM)
Four mutants pass the first-party suite: dropping `O_NOFOLLOW` on files, dropping it on directories, stubbing the manifest hash, and removing torn-read detection. The implementation is correct in all four; the guards are simply unpinned. Torn-read detection cannot be provoked from the CLI, so I covered it with a unit-level test that shifts the post-read stat identity.

## How the numbers must be read

These are not defects — they are the interpretive terms of the artifact, and they matter more than any of the findings above.

1. **`24 filled / 11 no-fill` are the tracker's own labels, frozen — not verified execution.** The freezer copies `filled`/`reason` from the ledger and deliberately does not recompute them. Correctly labelled `NON_EXECUTION_NON_PROMOTABLE`.
2. **`censored: 0` does not mean no data was missing.** I confirmed the schema *can* represent censoring: a row with a `censored_*` reason produces `censored: 1`. Zero means the legacy tracker had no censoring concept, so absence was never recorded as such — the very gap Task 3 was built to fix.
3. **The baseline is ~94% bull regime** (33 bull / 2 choppy) across 35 scans. Any challenger compared against this baseline is being compared on bull-regime evidence almost exclusively. Given the repository's own finding that the strategy's apparent edge is bull-flattered, this is the single most important caveat attached to the artifact, and it should be restated at every point where the baseline is used as a comparator.

## Recommendation

**Approve Task 5 with M1 tracked as an open defect.** M1 should be fixed before the inventory is regenerated on any dataset with genuinely missing artifacts, since that is exactly when it will fire. L1 should be fixed before the manifest hash is cited as independent tamper evidence. M2 and M3 are hygiene.

Standing status: Tasks 1–5 make a future comparison auditable. The frozen baseline is a record of what a tracker claimed over 35 mostly-bull scans, correctly labelled as non-execution and non-promotable. It is not evidence of an edge, and its regime composition means it is a weak comparator for anything intended to work outside a bull market.
