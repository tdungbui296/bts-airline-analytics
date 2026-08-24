# BigQuery Ingestion and Warehouse Validation Runbook

This runbook defines the supported boundary between the local BTS data-preparation pipeline, the BigQuery landing table, and the dbt analytical warehouse.

It documents:

- the tracked inputs and evidence used to approve a load;
- the expected BigQuery destination;
- the required ingestion configuration;
- post-load validation queries;
- dbt build and test procedures;
- reconciliation expectations; and
- BigQuery Sandbox constraints.

The runbook deliberately does not claim the exact historical `bq load` command used during initial development because those command-line flags were not preserved in the repository. The tracked manifests, schema contract, row counts, hashes, dbt source configuration, and warehouse validation evidence remain the authoritative proof of the implemented data boundary.

## Supported Scope

| Property | Value |
|---|---|
| Source | US Bureau of Transportation Statistics Reporting Carrier On-Time Performance |
| Reporting years | 2024 and 2025 |
| Monthly source files | 24 |
| BigQuery project | `tdb-dbt-bigquery-portfolio` |
| BigQuery location | `asia-southeast1` |
| Landing dataset | `raw_bts` |
| Landing table | `flights` |
| Fully qualified source | `tdb-dbt-bigquery-portfolio.raw_bts.flights` |
| BigQuery load columns | 51 |
| 2024 rows | 7,079,061 |
| 2025 rows | 7,001,619 |
| Total rows | 14,080,680 |

The project currently uses BigQuery Sandbox. Cost, storage retention, table expiration, and historical partition limitations must therefore be considered before rebuilding large warehouse relations.

## Data Flow

```text
Monthly BTS ZIP files
        |
        v
Annual source validation
        |
        v
51-column processed core files
        |
        v
Type-contract audit
        |
        v
Deterministic BigQuery-ready CSV.GZ files
        |
        v
raw_bts.flights
        |
        v
dbt staging and quality classification
        |
        v
dimensional facts and dimensions
        |
        v
monthly analytical marts
```

## Repository Components

### Supported pipeline scripts

| Script | Purpose |
|---|---|
| `scripts/validate_batch.py` | Validate the 12 monthly source ZIP files for one reporting year |
| `scripts/project_batch.py` | Project validated source data into the standardized 51-column core format |
| `scripts/audit_types.py` | Audit projected values against the proposed BigQuery type contract |
| `scripts/build_bq_load_files.py` | Generate deterministic BigQuery-ready compressed CSV files |

Detailed script usage is documented in `../scripts/README.md`.

### Configuration contracts

| File | Purpose |
|---|---|
| `config/bts_core_columns.txt` | Ordered 51-column core field contract |
| `config/bq_proposed_types.csv` | Proposed BigQuery data-type contract |
| `config/bq_flights_schema.json` | Explicit BigQuery landing-table schema |

The BigQuery schema contains:

- 51 uniquely named fields;
- 25 required fields; and
- 26 nullable fields.

### Tracked source and load evidence

| File | Purpose |
|---|---|
| `evidence/source/2024_source_manifest.csv` | 2024 source-file identity and validation evidence |
| `evidence/source/2025_source_manifest.csv` | 2025 source-file identity and validation evidence |
| `evidence/source/2024_processed_manifest.csv` | 2024 processed-file hashes and reconciliation |
| `evidence/source/2025_processed_manifest.csv` | 2025 processed-file hashes and reconciliation |
| `evidence/source/2024_bq_load_manifest.csv` | 2024 BigQuery-ready output hashes and reconciliation |
| `evidence/source/2025_bq_load_manifest.csv` | 2025 BigQuery-ready output hashes and reconciliation |

Each annual manifest contains 12 monthly records.

### Warehouse validation evidence

| File | Purpose |
|---|---|
| `evidence/dbt/dbt_build_2026-08-17.log` | Preserved complete dbt build output |
| `evidence/profiling/fact_flights_derivability_audit.csv` | Proof that flight-fact analytical fields are derivable |
| `evidence/profiling/quality_classification_reconciliation_2024_2025.csv` | Cross-year semantic quality-classification reconciliation |
| `evidence/profiling/negative_scheduled_elapsed_summary_2024_2025.csv` | Summary of invalid negative scheduled-duration records |
| `evidence/profiling/negative_scheduled_elapsed_details_2024_2025.csv` | Record-level supporting detail for the duration anomaly |

Additional profiling evidence is classified in `../analysis/profiling/README.md`.

## Local Environment

From the repository root, create and activate the project environment if it does not already exist:

```powershell
py -3.12 -m venv .venv

Set-ExecutionPolicy `
    -Scope Process `
    -ExecutionPolicy RemoteSigned

.\.venv\Scripts\Activate.ps1

python -m pip install `
    -r .\requirements.txt
```

Confirm the main tools:

```powershell
python --version
dbt --version
```

The validated environment uses:

- Python 3.12.10
- pandas 3.0.5
- NumPy 2.5.1
- dbt Core 1.12.0
- dbt BigQuery 1.12.0

## Stage 1 — Validate the Annual Source Batches

Run validation separately for each supported reporting year:

```powershell
python .\scripts\validate_batch.py `
    --year 2024

python .\scripts\validate_batch.py `
    --year 2025
```

Expected results:

| Year | Total rows | Automatic passes | Review months |
|---:|---:|---:|---|
| 2024 | 7,079,061 | 10 | July and August |
| 2025 | 7,001,619 | 12 | None |

The 2024 July and August conditions were manually reviewed and approved using the tracked diagnostic evidence. A review status must not be silently converted into a pass without examining its supporting records.

Do not continue if:

- a monthly source file is missing;
- the annual row total changes unexpectedly;
- an unreviewed month returns a review condition;
- an approved month returns a new condition; or
- source identity differs from the tracked source manifest without an intentional refresh.

## Stage 2 — Project the 51-Column Core Files

Run projection separately for each year:

```powershell
python .\scripts\project_batch.py `
    --year 2024

python .\scripts\project_batch.py `
    --year 2025
```

Expected results:

| Year | Monthly outputs | Total rows | Row reconciliation |
|---:|---:|---:|---|
| 2024 | 12 | 7,079,061 | True |
| 2025 | 12 | 7,001,619 | True |

The generated processed data is stored locally under:

```text
data/processed/2024/
data/processed/2025/
```

These large generated files are intentionally excluded from Git.

Accept projection only when:

- 12 monthly outputs exist for the year;
- the annual total matches the approved source total;
- every monthly row count reconciles;
- the output contract contains 51 columns; and
- the processed manifest matches the generated files.

## Stage 3 — Audit BigQuery Type Compatibility

Run the annual type audits:

```powershell
python .\scripts\audit_types.py `
    --year 2024

python .\scripts\audit_types.py `
    --year 2025
```

Expected results:

- 51 of 51 columns assessed for 2024;
- 51 of 51 columns assessed for 2025;
- annual row totals preserved; and
- no unexplained type-contract failures.

The type audit protects the explicit BigQuery schema from malformed values before warehouse ingestion.

## Stage 4 — Generate Deterministic BigQuery Load Files

Run:

```powershell
python .\scripts\build_bq_load_files.py `
    --year 2024

python .\scripts\build_bq_load_files.py `
    --year 2025
```

Expected results:

| Year | Files | Columns | Rows | Approximate compressed size |
|---:|---:|---:|---:|---:|
| 2024 | 12 | 51 | 7,079,061 | 265.12 MB |
| 2025 | 12 | 51 | 7,001,619 | 266.09 MB |

The outputs are written under:

```text
data/load/2024/
data/load/2025/
```

The load generator uses deterministic gzip metadata with an MTIME value of zero. Regenerating a file from identical processed input therefore produces the same compressed SHA-256 hash.

Before ingestion, verify:

- 12 load files exist for each year;
- no `.tmp` files remain;
- every manifest output path exists;
- recorded file sizes match;
- recorded SHA-256 hashes match;
- every monthly row count reconciles;
- every output contains 51 columns; and
- annual totals equal 7,079,061 and 7,001,619.

The tracked `output_sha256` values in the annual load manifests are the accepted reproducible-output evidence.

## Stage 5 — Prepare the BigQuery Landing Dataset

The required destination is:

```text
tdb-dbt-bigquery-portfolio.raw_bts.flights
```

The dataset must use:

```text
asia-southeast1
```

If the dataset does not exist, create `raw_bts` through the BigQuery console:

1. Open Google Cloud Console.
2. Select project `tdb-dbt-bigquery-portfolio`.
3. Open BigQuery.
4. Select the project in the Explorer panel.
5. Choose **Create dataset**.
6. Enter dataset ID `raw_bts`.
7. Set the location to `asia-southeast1`.
8. Review expiration settings in the context of BigQuery Sandbox.
9. Create the dataset.

Do not create the landing dataset in another region. BigQuery datasets and dbt targets used together must be region-compatible.

## Stage 6 — Configure the Landing Table

The landing-table contract is:

```text
Project: tdb-dbt-bigquery-portfolio
Dataset: raw_bts
Table: flights
Schema: config/bq_flights_schema.json
Columns: 51
```

The schema must be explicit. Do not rely on automatic schema detection because:

- source fields include encoded time values;
- operational flags use controlled integer representations;
- nullability has already been audited;
- type consistency must remain stable across all 24 months; and
- silent inference changes would weaken reproducibility.

### Historical-command limitation

The repository does not contain the exact historical `bq load` command or its original flags. Therefore, this runbook does not present an unverified command as historical fact.

For a future reload, the operator must select and record one supported method:

- BigQuery console upload;
- Google Cloud Storage followed by a BigQuery load job; or
- a reviewed `bq load` command.

Regardless of method, the job must preserve this contract:

- CSV input;
- gzip-compressed files;
- explicit 51-field schema;
- header row skipped;
- destination `raw_bts.flights`;
- location `asia-southeast1`;
- append behavior only for distinct monthly files;
- no automatic field addition;
- no automatic field relaxation; and
- no duplicate month ingestion.

The exact command, job configuration, execution timestamp, and resulting BigQuery job ID should be captured during the next controlled reload.

## Stage 7 — Load-Safety Procedure

Before loading any monthly file:

1. Confirm the active GCP project.
2. Confirm the destination dataset and table.
3. Confirm the BigQuery location.
4. Confirm the input filename and reporting month.
5. Compare the file’s SHA-256 hash with the annual load manifest.
6. Confirm the expected monthly row count.
7. Determine whether that month already exists in the landing table.
8. Confirm whether the intended operation is creation, append, or controlled replacement.
9. Record the planned operation before execution.

Never blindly append all 24 files to an existing table. That can duplicate the complete historical dataset.

For a controlled full reload, prefer loading into a new temporary or versioned table, validating it, and only then promoting it to the supported landing-table boundary.

## Expected Monthly Row Counts

### 2024

| Month | Expected rows |
|---:|---:|
| January | 547,271 |
| February | 519,221 |
| March | 591,767 |
| April | 582,185 |
| May | 609,743 |
| June | 611,132 |
| July | 634,613 |
| August | 619,025 |
| September | 582,622 |
| October | 615,497 |
| November | 575,404 |
| December | 590,581 |
| Total | 7,079,061 |

### 2025

| Month | Expected rows |
|---:|---:|
| January | 539,747 |
| February | 504,884 |
| March | 600,872 |
| April | 583,950 |
| May | 605,648 |
| June | 611,575 |
| July | 631,428 |
| August | 602,378 |
| September | 562,439 |
| October | 605,844 |
| November | 570,550 |
| December | 582,304 |
| Total | 7,001,619 |

## Stage 8 — Validate the BigQuery Landing Table

Run these queries in the BigQuery SQL workspace after ingestion.

### Confirm total row count

```sql
select
    count(*) as total_rows
from `tdb-dbt-bigquery-portfolio.raw_bts.flights`;
```

Expected:

```text
14,080,680
```

### Confirm annual row counts

```sql
select
    Year as reporting_year,
    count(*) as row_count
from `tdb-dbt-bigquery-portfolio.raw_bts.flights`
group by Year
order by Year;
```

Expected:

| reporting_year | row_count |
|---:|---:|
| 2024 | 7,079,061 |
| 2025 | 7,001,619 |

### Confirm monthly row counts

```sql
select
    Year as reporting_year,
    Month as reporting_month,
    count(*) as row_count
from `tdb-dbt-bigquery-portfolio.raw_bts.flights`
group by
    Year,
    Month
order by
    Year,
    Month;
```

Compare all 24 results with the monthly tables in this runbook and the annual BigQuery load manifests.

### Confirm year and month coverage

```sql
select
    min(FlightDate) as minimum_flight_date,
    max(FlightDate) as maximum_flight_date,
    count(
        distinct format_date(
            '%Y-%m',
            FlightDate
        )
    ) as represented_months
from `tdb-dbt-bigquery-portfolio.raw_bts.flights`;
```

Expected represented months:

```text
24
```

### Confirm schema column count

```sql
select
    count(*) as column_count
from `tdb-dbt-bigquery-portfolio.raw_bts.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'flights';
```

Expected:

```text
51
```

### Confirm schema nullability counts

```sql
select
    is_nullable,
    count(*) as column_count
from `tdb-dbt-bigquery-portfolio.raw_bts.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'flights'
group by is_nullable
order by is_nullable;
```

Expected schema contract:

| is_nullable | column_count |
|---|---:|
| NO | 25 |
| YES | 26 |

### Inspect the complete deployed schema

```sql
select
    ordinal_position,
    column_name,
    data_type,
    is_nullable
from `tdb-dbt-bigquery-portfolio.raw_bts.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'flights'
order by ordinal_position;
```

Compare this result with `config/bq_flights_schema.json`.

### Detect unsupported years or months

```sql
select
    Year as reporting_year,
    Month as reporting_month,
    count(*) as row_count
from `tdb-dbt-bigquery-portfolio.raw_bts.flights`
where
    Year not in (2024, 2025)
    or Month not between 1 and 12
group by
    Year,
    Month
order by
    Year,
    Month;
```

Expected:

```text
Zero rows
```

### Check source date consistency

```sql
select
    countif(
        extract(year from FlightDate) != Year
    ) as year_mismatch_rows,
    countif(
        extract(month from FlightDate) != Month
    ) as month_mismatch_rows
from `tdb-dbt-bigquery-portfolio.raw_bts.flights`;
```

Expected:

```text
year_mismatch_rows = 0
month_mismatch_rows = 0
```

## Stage 9 — Configure dbt

Create the local profile at:

```text
%USERPROFILE%\.dbt\profiles.yml
```

Example structure:

```yaml
bts_airline_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: tdb-dbt-bigquery-portfolio
      dataset: dbt_dev
      location: asia-southeast1
      threads: 4
```

The profile is local configuration and must not be committed.

The repository `.gitignore` protects:

- `profiles.yml`;
- `dbt/profiles.yml`;
- environment-variable files;
- service-account JSON files;
- Google credential files;
- PEM files; and
- private-key files.

## Stage 10 — Parse Before Building

Run the cheapest structural check first:

```powershell
dbt parse `
    --project-dir .\dbt `
    --no-partial-parse
```

Parsing validates:

- project configuration;
- YAML structure;
- model references;
- source declarations;
- tests;
- macros; and
- dependency resolution.

It does not build the warehouse relations.

## Stage 11 — Inspect the Planned dbt Resources

Run:

```powershell
dbt ls `
    --project-dir .\dbt
```

The supported project contains:

- 11 models;
- 214 data tests;
- 1 seed;
- 1 source; and
- 561 macros, including installed and built-in macros.

The models are:

- `stg_bts__flights`
- `int_flights__quality_classified`
- `dim_airport`
- `dim_carrier`
- `dim_date`
- `fact_flights`
- `fact_flight_delay_cause`
- `mart_airport_monthly_performance`
- `mart_carrier_monthly_delay_cause`
- `mart_carrier_monthly_performance`
- `mart_route_monthly_performance`

The seed is:

- `dim_delay_cause`

## Stage 12 — Build and Test the Warehouse

For a complete controlled build:

```powershell
dbt build `
    --project-dir .\dbt
```

The latest preserved successful result was:

```text
PASS=226 WARN=0 ERROR=0 SKIP=0 TOTAL=226
```

This consisted of:

- 1 seed;
- 8 table models;
- 3 view models; and
- 214 data tests.

The complete historical build output is tracked at:

```text
evidence/dbt/dbt_build_2026-08-17.log
```

### Targeted development build

When changing one mart, use targeted selection to reduce BigQuery processing:

```powershell
dbt build `
    --project-dir .\dbt `
    --select mart_route_monthly_performance
```

The preserved targeted route-mart build completed with:

```text
PASS=37 WARN=0 ERROR=0 SKIP=0 TOTAL=37
```

Avoid repeatedly rebuilding all warehouse relations when a targeted build can validate the change.

## Stage 13 — Validate the dbt Source Boundary

The staging source is declared in:

```text
dbt/models/staging/bts/_bts__sources.yml
```

The expected source is:

```text
database: tdb-dbt-bigquery-portfolio
schema: raw_bts
table: flights
```

The staging model must preserve the landing-table row count.

The singular test:

```text
dbt/tests/assert_stg_bts__flights_preserves_row_count.sql
```

enforces this boundary.

The expected staging count is:

```text
14,080,680
```

## Stage 14 — Validate Warehouse Reconciliation

The test suite validates more than simple null and uniqueness checks.

Key reconciliation areas include:

- landing source to staging row preservation;
- staging to intermediate row preservation;
- intermediate semantic eligibility rules;
- intermediate to flight-fact row preservation;
- sparse positive delay-cause records;
- dimension-key relationships;
- carrier-month population preservation;
- route-month population preservation;
- airport-month population preservation;
- monthly volume reconciliation;
- valid rate ranges;
- delay-cause fact-to-mart reconciliation; and
- delay-cause shares summing to approximately one.

A dbt data test passes only when its query returns zero violating rows.

## Stage 15 — Preserve New Execution Evidence

For any future full reload, preserve an evidence package containing:

- execution date and time in UTC;
- operator or environment;
- GCP project;
- BigQuery location;
- destination dataset and table;
- ingestion method;
- exact load-job configuration or command;
- BigQuery job IDs;
- input filenames;
- input SHA-256 hashes;
- expected and actual monthly row counts;
- deployed schema comparison;
- post-load SQL results;
- dbt version;
- complete `dbt build` output; and
- final Git commit identifying the code version.

Do not store credentials, access tokens, local profiles, or service-account keys in evidence files.

## Failure and Recovery Guidance

### Hash mismatch

Stop the load.

A mismatch means the local output differs from the tracked approved artifact. Determine whether the difference results from:

- changed processed inputs;
- changed transformation code;
- incomplete generation;
- file corruption; or
- an intentional refresh.

Do not update a manifest merely to make the check pass.

### Row-count mismatch before ingestion

Stop before BigQuery loading.

Re-run validation and projection for the affected year and compare the affected month with:

- the source manifest;
- the validation summary;
- the processed manifest; and
- the BigQuery load manifest.

### Row-count mismatch after ingestion

Do not start dbt.

Identify:

- missing months;
- duplicated months;
- partial load jobs;
- incorrectly appended files; or
- an incorrect destination table.

Prefer rebuilding a controlled replacement table over manually deleting uncertain subsets.

### Schema mismatch

Do not relax or add fields automatically.

Compare:

- `config/bq_flights_schema.json`;
- `config/bq_proposed_types.csv`;
- the type-audit output; and
- `INFORMATION_SCHEMA.COLUMNS`.

Any contract change must be reviewed, tested, documented, and committed separately.

### dbt parse failure

Do not run a build.

Correct project, YAML, SQL, source, profile, or dependency errors first.

### dbt test failure

Do not treat partially built marts as approved analytical outputs.

Review the failing model and its upstream dependencies, correct the problem, and rerun the appropriate selection.

## Sandbox Constraints

BigQuery Sandbox influenced several current materialization decisions.

In particular:

- `fact_flights` is currently a view to avoid storing another full copy of the historical flight-grain dataset;
- `fact_flight_delay_cause` is a non-partitioned clustered table because historical partitions are subject to Sandbox expiration behavior;
- the four monthly analytical marts are physical tables;
- targeted builds are preferred during development; and
- `INFORMATION_SCHEMA.TABLE_STORAGE` access is not required for the supported validation procedure.

A future production version may use:

- an incremental flight fact;
- date partitioning;
- clustering;
- automated orchestration;
- source freshness;
- snapshots;
- continuous integration; and
- controlled deployment environments.

These are future enhancements and are not represented as currently implemented.

## Acceptance Checklist

A warehouse refresh is approved only when all applicable items below pass.

### Local pipeline

- [ ] Twelve monthly source files exist for each supported year.
- [ ] Annual validation completes.
- [ ] Reviewed source anomalies remain understood and approved.
- [ ] Projection produces 12 monthly files per year.
- [ ] Every projected row count reconciles.
- [ ] Type audits cover all 51 fields.
- [ ] BigQuery-ready outputs contain 51 fields.
- [ ] No temporary output files remain.
- [ ] All output hashes match their manifests.
- [ ] Deterministic gzip outputs reproduce stable hashes.

### BigQuery landing table

- [ ] Destination project is correct.
- [ ] Dataset is `raw_bts`.
- [ ] Table is `flights`.
- [ ] Location is `asia-southeast1`.
- [ ] Total row count is 14,080,680.
- [ ] The 2024 total is 7,079,061.
- [ ] The 2025 total is 7,001,619.
- [ ] All 24 monthly counts reconcile.
- [ ] Exactly 24 reporting months are represented.
- [ ] The deployed table contains 51 columns.
- [ ] Required and nullable field counts match the schema.
- [ ] Flight dates agree with source year and month fields.
- [ ] No unsupported reporting years or months exist.

### dbt warehouse

- [ ] `dbt parse` succeeds.
- [ ] The source resolves to `raw_bts.flights`.
- [ ] The delay-cause seed loads successfully.
- [ ] All intended models build.
- [ ] All generic tests pass.
- [ ] All singular tests pass.
- [ ] Source-to-staging volume reconciles.
- [ ] Flight-fact volume reconciles.
- [ ] Analytical mart grains remain unique.
- [ ] Mart volumes reconcile to their underlying facts.
- [ ] Rates remain within valid boundaries.
- [ ] Delay-cause shares reconcile.
- [ ] Final execution evidence is preserved.