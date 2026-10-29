# BTS Airline Analytics — Airflow Orchestration Evidence

## Execution summary

- Date: 2026-10-29
- DAG: `bts_2026_02_integrated_incremental`
- Apache Airflow: 3.3.2, Dockerized
- dbt Core / dbt-bigquery: 1.12.0
- BigQuery project: `tdb-dbt-bigquery-portfolio`
- Region: `asia-southeast1`
- Execution: manual, not scheduled
- DAG result: SUCCESS
- Tasks: 8/8 successful
- Duration: approximately 66 seconds

## Orchestrated workflow

1. Validate the existing February ingestion receipt.
2. Check explicit manual execution authorization.
3. Reconcile the January-only target and raw monthly source.
4. Execute the existing 2026 dbt upstream transformations.
5. Perform a representative BigQuery MERGE dry run.
6. Execute the genuine February dbt incremental MERGE.
7. Reconcile January and February flight records and unique keys.
8. Release the verified 2026 fact-table handoff.

## Verified results

| Reporting month | Flight records |
| --- | ---: |
| January 2026 | 544,003 |
| February 2026 | 515,037 |
| Total | 1,059,040 |

Independent post-run BigQuery metadata verification confirmed:

- Raw table row count: 1,059,040
- Incremental target row count: 1,059,040
- Incremental target columns: 42
- Daily partitioning on `flight_date`
- Clustering by origin airport, destination airport, and airline

The Airflow post-MERGE validation also passed monthly row-count
and flight-key reconciliation checks.

## Architecture and limitations

The February raw data was already loaded into BigQuery before
the Airflow run. The DAG validates ingestion rather than
re-uploading the February source files.

The transformation tasks reuse existing dbt models.

The MERGE cost gate uses representative SQL rather than an
exact copy of the dbt-generated MERGE statement.

The post-run metadata verification was read-only.

The 2024-2025 analytical marts were not rebuilt.

The 2026 analytical mart extension and Power BI reporting
integration remain pending.

Automatic Airflow scheduling is disabled.

## Supporting files

- `airflow/Dockerfile`
- `airflow/compose.yaml`
- `airflow/dags/bts_2026_02_integrated_incremental.py`
- `airflow/dags/bts_2026_02_offline_preflight.py`
- `evidence/bigquery/B3_50O_2026_02_airflow_postrun.json`

Airflow task logs, Graph View screenshots, and BigQuery
execution job IDs should be retained as additional evidence.

Do not commit OAuth credentials, tokens, local authentication
overrides, or private billing-account information.