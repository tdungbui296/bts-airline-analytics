# Data Pipeline Scripts

This directory contains the Python pipeline used to validate, standardize, audit, and prepare the BTS On-Time Performance data for BigQuery.

The supported pipeline covers reporting years 2024 and 2025. Reusable annual scripts remain directly under `scripts/`, while diagnostic and historical exploratory scripts are separated into dedicated subdirectories.

## Environment

The project was validated with:

- Python 3.12.10
- pandas 3.0.5
- NumPy 2.5.1
- dbt Core 1.12.0
- dbt BigQuery 1.12.0

From the repository root, create a project-specific virtual environment:

```powershell
py -3.12 -m venv .venv
```

Activate it:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

Install the pinned dependencies:

```powershell
python -m pip install -r requirements.txt
```

After activation, `python` refers to the repository-local interpreter. All commands below should be run from the repository root with that environment active.

To verify the active interpreter:

```powershell
python -c "import sys; print(sys.executable)"
```

## Pipeline Overview

The annual preprocessing workflow is:

1. Validate the 12 monthly source ZIP files.
2. Project the validated source data into the standardized core schema.
3. Audit the proposed BigQuery data types.
4. Generate normalized BigQuery load files and reconciliation evidence.
5. Load the prepared files into BigQuery.
6. Build and test the analytical warehouse with dbt.

The scripts in this directory cover steps 1 through 4. BigQuery loading and dbt execution are documented separately at the project level.

## Supported Pipeline Scripts

### `validate_batch.py`

Validates the 12 monthly BTS source ZIP files for one reporting year.

Run for 2024:

```powershell
python .\scripts\validate_batch.py --year 2024
```

Run for 2025:

```powershell
python .\scripts\validate_batch.py --year 2025
```

Key outputs:

- `analysis/profiling/<year>_monthly_validation_summary.csv`
- `analysis/profiling/<year>_batch_validation_summary.json`

Validated totals:

| Year | Monthly files | Rows | Result |
|---:|---:|---:|---|
| 2024 | 12 | 7,079,061 | 10 pass, 2 manually reviewed |
| 2025 | 12 | 7,001,619 | 12 pass |

The July and August 2024 source anomalies were explicitly reviewed and approved before projection.

### `project_batch.py`

Projects one validated annual source batch into the standardized 51-column core schema.

Run for 2024:

```powershell
python .\scripts\project_batch.py --year 2024
```

Run for 2025:

```powershell
python .\scripts\project_batch.py --year 2025
```

Key outputs:

- `data/processed/<year>/bts_<year>_<month>_core.csv.gz`
- `evidence/source/<year>_processed_manifest.csv`
- `analysis/profiling/<year>_projection_summary.json`

The processed datasets are generated locally and excluded from Git. Their manifests and profiling summaries provide tracked reconciliation evidence.

### `audit_types.py`

Audits the standardized annual data against the proposed BigQuery type contract.

Run for 2024:

```powershell
python .\scripts\audit_types.py --year 2024
```

Run for 2025:

```powershell
python .\scripts\audit_types.py --year 2025
```

The audit evaluates all 51 projected columns and writes year-specific profiling evidence under `analysis/profiling/`.

### `build_bq_load_files.py`

Normalizes the standardized annual data into deterministic gzip-compressed CSV files suitable for BigQuery ingestion.

Run for 2024:

```powershell
python .\scripts\build_bq_load_files.py --year 2024
```

Run for 2025:

```powershell
python .\scripts\build_bq_load_files.py --year 2025
```

Key outputs:

- `data/load/<year>/bts_<year>_<month>_bq.csv.gz`
- `evidence/source/<year>_bq_load_manifest.csv`
- `analysis/profiling/<year>_bq_load_files_summary.json`

The gzip writer uses a fixed modification timestamp so identical source inputs produce byte-identical compressed outputs and stable SHA-256 hashes.

Validated totals:

| Year | Monthly files | Columns | Rows |
|---:|---:|---:|---:|
| 2024 | 12 | 51 | 7,079,061 |
| 2025 | 12 | 51 | 7,001,619 |
| Total | 24 | 51 | 14,080,680 |

## Diagnostic Scripts

The `diagnostics/` directory contains targeted investigations used to explain source anomalies, key construction, type irregularities, and storage characteristics.

These scripts are retained as engineering evidence but are not required for routine pipeline execution:

- `check_2024_fallback_flight_key.py`
- `estimate_2025_bq_storage.py`
- `inspect_2024_august_flight_number_fields.py`
- `inspect_2024_review_rows.py`
- `inspect_2025_type_anomalies.py`

They may depend on specific local raw or processed datasets.

## Exploratory Scripts

The `exploratory/` directory contains historical January 2025 pilot scripts created before the annual pipeline was generalized:

- `profile_january_2025_basic.py`
- `profile_january_2025_conditional.py`
- `project_january_2025.py`

These files preserve the project’s investigation history. They are not the supported production workflow and should not replace the parameterized annual scripts.

## Generated Data and Credentials

The following local or generated content is intentionally excluded from Git:

- `data/raw/`
- `data/processed/`
- `data/load/`
- Python virtual environments and caches
- dbt build artifacts
- `profiles.yml`
- environment files
- Google Cloud and service-account credentials

Never commit local BigQuery credentials, service-account keys, or generated source datasets.