# Offline research-only factor orthogonalization

`src/research/factor_orthogonalization.py` implements **Serial Orthogonalization Gate 1** as a clean-room, local-only diagnostic. It is isolated from scans, providers, selection, configuration, alerts, scheduling, portfolios, and orders. It computes no returns, IC, promotion score, or forward outcome.

## Frozen input contract

The CSV header is exactly:

```text
date,symbol,factor_value,<industry_column>,<ordered continuous_controls>
```

The industry/control names and order come from the manifest. Rows must be unique and strictly sorted by `(date,symbol)`. Dates are canonical `YYYY-MM-DD`; symbols and industries are nonempty; there are no missing/extra cells. Factor/control values use strict finite decimal/scientific grammar with no whitespace or underscores.

Schema 1 accepts no extra keys:

```json
{
  "schema_version": 1,
  "status": "RESEARCH_ONLY_NON_BINDING",
  "factor_id": "factor_name",
  "market": "A_SHARE_TUSHARE",
  "date_start": "2025-01-02",
  "date_end": "2025-01-03",
  "row_count": 100,
  "date_count": 2,
  "industry_column": "industry",
  "continuous_controls": ["size", "liquidity", "beta", "volatility", "factor_a"],
  "control_roles": {
    "size": "size",
    "liquidity": "liquidity",
    "beta": "beta",
    "volatility": "volatility",
    "existing_factors": ["factor_a"]
  },
  "point_in_time_industry_attestation": true,
  "controls_known_by_signal_cutoff_attestation": true,
  "factor_known_by_signal_cutoff_attestation": true,
  "winsor_lower": 0.01,
  "winsor_upper": 0.99,
  "min_cross_section": 50,
  "max_condition_number": 1000000.0,
  "max_abs_residual_exposure": 1e-8,
  "matrix_sha256": "<64 lowercase hex>",
  "input_bundle_sha256": "<64 lowercase hex>",
  "experiment_config_sha256": "<64 lowercase hex>",
  "research_contract_sha256": "<64 lowercase hex>"
}
```

`market` is `A_SHARE_TUSHARE` or `US_EQUITIES_PIT`. `continuous_controls` has at least four ordered unique names. The four primary roles name distinct controls; `existing_factors` is exactly the remaining controls in manifest order and may be empty. `min_cross_section` is at least 30 (50 is the documented default). The condition threshold is finite in `[1, 1e8]`; exposure tolerance is finite in `(0, 1e-8]`.

## Algorithm

For each date, the command:

1. Requires the declared minimum cross-section and at least two industries.
2. Winsorizes the factor and every continuous control at 0.01/0.99 using NumPy's deterministic linear quantiles, then sample-z-scores (`ddof=1`). Zero/nearly-zero dispersion is refused.
3. Builds `intercept + standardized controls in manifest order + sorted industry dummies dropping the first category`. Nonfinite designs, columns greater than or equal to rows, rank deficiency, and excessive/nonfinite condition numbers are refused.
4. Uses `numpy.linalg.lstsq` to residualize the preprocessed factor. The residual is **not winsorized**. It is only mean-centered and sample-z-scored, preserving orthogonality; zero/nearly-zero residual dispersion is refused.
5. Recalculates maximum absolute Pearson correlation to the preprocessed continuous controls and maximum absolute industry-group residual mean. Both must satisfy the manifest tolerance.

Raw and preprocessed factor values are retained alongside the normalized residual.

## Run

The executable code must already be committed at `HEAD`; dirty, ignored, staged, missing, mode-changed, or symlinked Python files under `src/**/*.py` and `scripts/**/*.py` are refused. This includes Python symlinks committed in `HEAD`: link text is not accepted as a substitute for binding the executable target bytes.

```bash
python scripts/research_factor_orthogonalization.py \
  --matrix /immutable/input/factor_matrix.csv \
  --manifest /immutable/input/manifest.json \
  --output outputs/research/orthogonalization/experiment-001
```

The output path must not exist. A successful directory contains exactly:

- `residuals.csv`
- `date_diagnostics.csv`
- `summary.json`
- `artifact_manifest.json`, with SHA-256 for every other output and the deterministic composite over sorted `"<relative_path>  <sha256>\n"` lines.

The matrix and manifest are read once through the caller's lexical absolute paths for parsing/hashing, and those same lexical paths are revalidated at the final publication boundary; retargeting an input symlink during execution is refused. Executable identity binds the exact filesystem/index/HEAD Python file set, types, modes, and blob bytes. Publication uses Linux `renameat2(RENAME_NOREPLACE)` from a complete sibling temporary directory. The caller's absolute final component stays lexical, so dangling and non-dangling output symlinks are refused rather than followed. Python `BaseException` paths, including `KeyboardInterrupt`, clean unrenamed temporary directories.

The boundary is namespace-atomic, not crash-durable: there is no file/directory `fsync` claim, and cleanup after `SIGKILL` or machine loss is not promised. Parent directories requested by the caller can remain, and intermediate parent symlinks are outside the final-component lexical boundary.

## Interpretation limits

The only verdict is `DIAGNOSTIC_ONLY_NOT_SELECTION`. The artifacts never say `PASS`, `APPROVE`, or `CANDIDATE`. Orthogonality is an in-sample linear property after the declared same-date preprocessing; it does not establish nonlinear independence, stability, investability, causal validity, point-in-time truth beyond attestations, or alpha.

**The residual factor is not validated alpha or out-of-sample evidence and cannot enter any selector without a separately preregistered outcome study, replay, independent review, and forward paper gate.**
