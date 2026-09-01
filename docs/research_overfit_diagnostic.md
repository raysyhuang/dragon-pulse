# Native research-only overfit diagnostic

`src/research/overfit_diagnostic.py` is a secondary evidence gate for an already
completed Dragon Pulse variant experiment. It is isolated from scan, selection,
configuration, alerts, schedulers, and order paths. It reads local files only.

## Evidence boundary

The command checks the internal integrity of a supplied packet and computes
secondary overfit diagnostics. It does **not** prove that the returns were
constructed point-in-time, that every historical trial was honestly disclosed,
that execution assumptions are realistic, or that any observation is untouched
out-of-sample. Even the favorable verdict is named
`NO_VETO_RESEARCH_ONLY_NOT_PROMOTION`; the only other verdict is
`VETO_FURTHER_PROMOTION`. Neither authorizes a production change.

The CSV must contain canonical, strictly increasing `YYYY-MM-DD` dates and one
finite **daily net portfolio return** column per variant, in exactly the manifest
order. All variants must share every date. The command deliberately has no
provider or network fallback. CSV parsing is strict; malformed quoting is refused.

Schema 1 requires this manifest shape (digests are lowercase SHA-256):

```json
{
  "schema_version": 1,
  "status": "RESEARCH_ONLY_NON_BINDING",
  "selected_variant": "variant_a",
  "variant_ids": ["variant_a", "variant_b"],
  "n_trials_total": 2,
  "date_start": "2025-01-02",
  "date_end": "2025-12-31",
  "periods_per_year": 252,
  "input_bundle_sha256": "<64 lowercase hex characters>",
  "experiment_config_sha256": "<64 lowercase hex characters>",
  "execution_contract_hash": "<64 lowercase hex characters>",
  "matrix_sha256": "<SHA-256 of the exact CSV bytes>",
  "all_tested_and_abandoned_variants_counted": true
}
```

`n_trials_total` must include every tested or abandoned variant, including trials
not retained as matrix columns. If optional `complete_historical_variant_count`
is supplied, it must equal `n_trials_total`. The boolean attestation is an
explicit operator assertion, not independent proof of search-history
completeness. No other manifest keys are allowed under schema 1.

## Exact usage

The defaults require eight equal contiguous blocks with at least 20 observations
per block, so the row count must be divisible by eight and at least 160.

```bash
python scripts/research_overfit_diagnostic.py \
  --matrix /immutable/experiment/daily_net_returns.csv \
  --manifest /immutable/experiment/manifest.json \
  --output outputs/research/overfit/experiment-001
```

Useful explicit form:

```bash
python scripts/research_overfit_diagnostic.py \
  --matrix daily_net_returns.csv --manifest manifest.json --output evidence-001 \
  --blocks 8 --min-block-observations 20 \
  --min-selected-sharpe 0.5 \
  --min-deflated-sharpe-probability 0.95 \
  --max-pbo 0.20 --max-bonferroni-p-value 0.05
```

The output path must not exist. Publication uses Linux `renameat2(...,
RENAME_NOREPLACE)` after all files are complete in a sibling temporary directory.
On any validation, computation, or publication error the command exits nonzero,
preserves an existing destination, and removes its temporary directory.

## Diagnostics and assumptions

- **Selected annualized Sharpe** uses sample standard deviation and the manifest's
  `periods_per_year`.
- **Deflated Sharpe probability** applies the probabilistic Sharpe ratio's
  skew/kurtosis adjustment against an expected maximum daily Sharpe benchmark.
  The benchmark uses the observed cross-variant Sharpe dispersion and
  `n_trials_total`.
- **CSCV/PBO** partitions the aligned observations into equal contiguous blocks,
  enumerates all half-block training combinations, selects the best in-sample
  mean/std strategy, ranks it on complementary blocks, and reports the fraction
  whose out-of-sample rank logit is non-positive. Every split is retained in
  `pbo_splits.csv`. Exact or numerically near-equal train/test strategy scores
  are refused because winner selection or rank would be ambiguous.
- **Bonferroni-adjusted selected p-value** multiplies a one-sided normal-approximate
  mean-return p-value by `n_trials_total`, capped at one.

These are screening diagnostics, not proofs. Daily returns can remain serially
correlated; contiguous blocks do not eliminate dependence. Overlapping holding
periods can materially overstate effective sample size. Normal approximations
are sensitive to skew, fat tails, heteroskedasticity, and small samples. PBO can
only rank variants present in the matrix; omitted abandoned trials affect the
trial-count adjustments but cannot be reconstructed. Thresholds are conservative
policy defaults rather than universal statistical constants.

## Artifacts

A successful no-overwrite directory contains:

- `summary.json`: metrics, threshold checks, and the non-promoting verdict;
- `pbo_splits.csv`: complete split-level CSCV/PBO evidence;
- `artifact_manifest.json`: SHA-256 for every other artifact and the deterministic
  composite `SHA256(sorted "<relative_path>  <sha256>\n" UTF-8 lines)`.

Every file stamps the research-only status, code SHA, packet identities, date
range, total trial count, selected variant, assumptions, and thresholds.

The code bundle is accepted only when the actual filesystem `src/**/*.py` and
`scripts/**/*.py` set exactly matches HEAD, the index has no scoped delta, and
every file's type, executable mode, and bytes match its HEAD object. Ignored and
staged Python additions are therefore refused; dirty non-Python documentation is
outside this identity boundary. Its digest is SHA-256 over concatenated,
raw-path-sorted records of
`<raw_path>\0<HEAD_mode> <HEAD_type>\0<byte_length>\0<exact_HEAD_blob_bytes>`.
