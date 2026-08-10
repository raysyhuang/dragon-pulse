# External Re-Verification — Task 5 M1/L1 Repair

**SHA:** `52a3287dba1f76d53e8bab0cfe38fd437df417f6`
**Method:** clean `git clone` on a separate machine from the builder.

## Verdict: PASS — both findings closed

| Check | Result |
|---|---|
| Verifier suite vs SHA | **46 passed** (was 33 pass + 1 xfail + 4 red) |
| All seven verification suites | **358 passed** |
| Mutation — core battery (mine / theirs) | **10/10** / 7/10 |
| Mutation — portable-view battery (mine / theirs) | **6/6** / 2/6 |

The seven verifier files are byte-identical to `9fa85a9`; the builder did not touch them.

## L1 closed — proven, not asserted

The committed manifest hash reproduced **exactly** on this machine, from the same tracked ledger and artifacts, written to a completely different absolute path:

```
committed:    4200f732444a2efae5fde11bc04bcfc61cd9c86bd3cf4820e23ce6a4825c4e02
regenerated:  4200f732444a2efae5fde11bc04bcfc61cd9c86bd3cf4820e23ce6a4825c4e02
```

Under the previous contract this was impossible by construction. The manifest hash is now genuine third-party tamper evidence rather than a machine-local checksum.

The artifact also self-documents the contract. `manifest_identity` declares the algorithm, the exact canonicalisation, the hash-covered field list, the excluded advisory fields, and an interpretation line stating it is *"a self-consistent legacy inventory hash, not an independent source-authenticity proof."* That last sentence is the correct standard: the hash proves the document was not altered, not that the evidence is authentic.

Advisory provenance is retained rather than dropped — both `generator` and `origin_advisory` remain present, carry `advisory_only: true`, and are named in `excluded_advisory_fields`.

## M1 closed

Both shapes of absence now agree. A deleted artifact file and a removed date directory each inventory as `MISSING_NATIVE_ARTIFACT` instead of aborting the freeze, and a partial-evidence run keeps every row.

## The hash still binds

Portability is only half the property; the other half is that excluding fields must not create a hole. Verified by mutating the portable view directly: dropping `rows`, `summary`, the evidence/promotion labels, the artifact-evidence hashes, or the capture caveat each moves the hash and is caught. Adding an advisory field back into the covered set — which would silently break portability again — is also caught.

## Finding

### N1 — First-party suite does not pin the hash-covered set (MEDIUM)
Four of six portable-view mutations pass the first-party suite: dropping `summary`, dropping the evidence and promotion labels, dropping the artifact-evidence hashes, and dropping the capture caveat. The implementation is correct in all four.

The label case is the one that matters. If `evidence_grade` and `promotion_status` ever left the covered set, the inventory could be relabelled `PROMOTABLE` while its manifest hash continued to validate — a tamper-evidence failure in exactly the field a reader trusts most. The verifier suite now pins the covered set by tampering with each field and re-hashing through the implementation's own portable view; the first-party suite should do likewise.

## Unchanged interpretive terms

The repair did not alter what the evidence means, and these still govern any use of the baseline:

1. `24 filled / 11 no-fill` are the tracker's own frozen labels, not verified execution.
2. `censored: 0` means the legacy tracker had no censoring concept, not that nothing was missing.
3. The baseline is ~94% bull regime (33 bull / 2 choppy across 35 scans), making it a weak comparator for anything intended to work outside a bull market.

## Recommendation

**Task 5 fully approved; M1 and L1 closed.** Carry N1 into the first-party suite. Cleared for the Task 6 infrastructure gate.
