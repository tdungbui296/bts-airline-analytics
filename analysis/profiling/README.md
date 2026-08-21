# Profiling and Validation Evidence

This directory contains compact, tracked evidence produced during source validation, schema projection, type auditing, storage estimation, and semantic-quality refinement.

Large raw, processed, and BigQuery-ready datasets are intentionally excluded from Git. The files here preserve the checks, summaries, exceptions, and engineering decisions used to build the warehouse.

## Evidence Categories

The directory contains three categories of artifacts:

1. Annual pipeline evidence for the supported 2024 and 2025 datasets.
2. Diagnostic evidence for specific anomalies, type issues, and storage decisions.
3. Historical exploratory evidence from the January 2025 pilot.

The supported annual pipeline is documented in `scripts/README.md`.

## Annual Validation Evidence

Annual validation checks the 12 monthly BTS source ZIP files before projection.

### 2024

- `2024_monthly_validation_summary.csv`
- `2024_batch_validation_summary.json`

The 2024 batch contains 7,079,061 rows. Ten months passed automatically. July and August required explicit review of known source-data conditions before approval.

### 2025

- `2025_monthly_validation_summary.csv`
- `2025_batch_validation_summary.json`

The 2025 batch contains 7,001,619 rows, with all 12 months passing validation.

## Annual Projection Evidence

Projection standardizes each validated annual batch into the 51-column core schema.

- `2024_projection_summary.json`
- `2025_projection_summary.json`

The detailed processed-file manifests are stored separately:

- `evidence/source/2024_processed_manifest.csv`
- `evidence/source/2025_processed_manifest.csv`

Those manifests record monthly inputs, outputs, hashes, row counts, and reconciliation results.

## Annual Type-Audit Evidence

Type audits verify the projected data against the proposed BigQuery type contract.

### 2024

- `2024_type_audit_by_column.csv`
- `2024_type_audit_summary.json`

### 2025

- `2025_type_audit_by_column.csv`
- `2025_type_audit_summary.json`

Both audits cover 51 columns and reconcile against their annual source totals.

## BigQuery Load-File Evidence

Load-file summaries describe the deterministic BigQuery-ready outputs generated from the projected core data.

- `2024_bq_load_files_summary.json`
- `2025_bq_load_files_summary.json`

The corresponding monthly load manifests are stored separately:

- `evidence/source/2024_bq_load_manifest.csv`
- `evidence/source/2025_bq_load_manifest.csv`

The load generator uses deterministic gzip metadata. Identical source inputs therefore produce stable compressed SHA-256 hashes.

Validated load totals:

| Year | Monthly files | Columns | Rows |
|---:|---:|---:|---:|
| 2024 | 12 | 51 | 7,079,061 |
| 2025 | 12 | 51 | 7,001,619 |
| Total | 24 | 51 | 14,080,680 |

## Diagnostic Evidence

These artifacts support specific engineering investigations. They are not separate production datasets.

### 2024 anomaly review

- `2024_07_missing_delay_cause_rows.csv`
- `2024_08_candidate_key_null_rows.csv`
- `2024_review_anomaly_rows.csv`

These files document the source conditions reviewed before the 2024 batch was approved.

Their diagnostic generator is:

- `scripts/diagnostics/inspect_2024_review_rows.py`

### 2025 type anomalies

- `2025_type_anomaly_rows.csv`

This file records rows examined during the 2025 type investigation.

Its diagnostic generator is:

- `scripts/diagnostics/inspect_2025_type_anomalies.py`

### 2025 storage estimation

- `2025_logical_storage_by_column.csv`
- `2025_logical_storage_estimate.json`

These files support the BigQuery Sandbox storage assessment. They are estimates from local processed data, not live BigQuery billing records.

Their diagnostic generator is:

- `scripts/diagnostics/estimate_2025_bq_storage.py`

### Cross-year quality-rule refinement

- `historical_2024_2025_quality_rule_refinement.csv`

This artifact records historical evidence used to refine semantic eligibility rules across the 2024 and 2025 data.

## Historical January 2025 Pilot

The January 2025 artifacts preserve the exploratory work that preceded the annual parameterized pipeline.

They are retained to show how source behavior, conditional nulls, status fields, diversion behavior, delay causes, and candidate keys were investigated before annualization.

### Pilot projection

- `january_2025_projection_summary.json`

Generator:

- `scripts/exploratory/project_january_2025.py`

### Basic profile

- `january_2025_basic_profile_summary.json`
- `january_2025_column_profile.csv`
- `january_2025_status_value_counts.csv`

Generator:

- `scripts/exploratory/profile_january_2025_basic.py`

### Conditional profile

- `january_2025_conditional_profile_summary.json`
- `january_2025_delay_cause_population.csv`
- `january_2025_diversion_landing_crosstab.csv`
- `january_2025_operational_nulls_by_status.csv`

Generator:

- `scripts/exploratory/profile_january_2025_conditional.py`

These pilot files are historical evidence, not the supported annual workflow.

## Interpretation Guidelines

- JSON summary files provide compact run-level results.
- CSV files provide column-level, month-level, or exception-level detail.
- `generated_at_utc` values identify when an artifact was regenerated.
- Row-count reconciliation fields should be checked before accepting generated outputs.
- Files under `data/` are local generated datasets and are intentionally excluded from Git.
- Credentials and local dbt profiles must never be stored in this directory.

For routine reproduction, use the supported parameterized scripts documented in `scripts/README.md`.