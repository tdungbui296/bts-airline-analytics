"""February 2026 BTS incremental orchestration.

Manual execution only. Reuses existing BigQuery ingestion and dbt models.
No historical re-ingestion and no automatic scheduling.
"""

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task, get_current_context


PROJECT = "tdb-dbt-bigquery-portfolio"
MONTH = "2026-02"

DBT = "/home/airflow/dbt-venv/bin/dbt"
PYTHON = "/home/airflow/dbt-venv/bin/python"
PROJECT_DIR = "/opt/bts/dbt"
PROFILES_DIR = "/opt/bts/dbt-profile"

RECEIPT = Path(
    "/opt/bts/evidence/bigquery/B3_38_2026_02_load_receipt.json"
)

DBT_VARS = {
    "enable_incremental_demo": True,
    "enable_2026_incremental_demo": True,
    "incremental_2026_mode": "monthly",
    "reprocess_month": MONTH,
}


def run_command(arguments, timeout=1800):
    """Execute fixed arguments without a shell."""
    completed = subprocess.run(
        arguments,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout)

    if completed.returncode != 0:
        raise RuntimeError(
            "Subprocess failed with exit code "
            + str(completed.returncode)
        )


def run_dbt(selector, label):
    """Reuse the checked-in dbt project, not duplicated SQL."""
    target = "/tmp/bts-airflow-dbt/" + label + "/target"
    logs = "/tmp/bts-airflow-dbt/" + label + "/logs"

    Path(target).mkdir(parents=True, exist_ok=True)
    Path(logs).mkdir(parents=True, exist_ok=True)

    arguments = [
        DBT,
        "run",
        "--project-dir", PROJECT_DIR,
        "--profiles-dir", PROFILES_DIR,
        "--target", "dev",
        "--target-path", target,
        "--log-path", logs,
        "--vars", json.dumps(DBT_VARS),
        "--select", selector,
    ]

    run_command(arguments)
    return label + "_PASS"


BIGQUERY_CHECK = r"""
import re
import sys
from datetime import datetime, timezone
from google.cloud import bigquery

stage = sys.argv[1]
if stage not in ("before", "after"):
    raise RuntimeError("Invalid reconciliation stage")

project = "tdb-dbt-bigquery-portfolio"
location = "asia-southeast1"
limit = 1000000000

client = bigquery.Client(project=project, location=location)

raw = client.get_table(project + ".raw_bts.flights_2026")
target = client.get_table(
    project + ".dbt_dev.fact_flights_2026_monthly_incremental"
)

if raw.num_rows != 1059040 or len(raw.schema) != 51:
    raise RuntimeError("Raw 2026 baseline mismatch")

if raw.expires and raw.expires <= datetime.now(timezone.utc):
    raise RuntimeError("Raw source has expired")

if stage == "before" and target.num_rows != 544003:
    raise RuntimeError("January-only target baseline mismatch")

date_field = next(
    (
        f for f in raw.schema
        if f.name.lower() in ("fl_date", "flightdate", "flight_date")
    ),
    None,
)

if date_field is None:
    raise RuntimeError("Raw source date field not recognized")

if date_field.field_type not in (
    "DATE", "DATETIME", "TIMESTAMP", "STRING"
):
    raise RuntimeError("Unsupported source date type")

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", date_field.name):
    raise RuntimeError("Invalid source date identifier")


def read_rows(sql, label):
    dry = client.query(
        sql,
        location=location,
        job_config=bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
            maximum_bytes_billed=limit,
        ),
    )

    estimate = int(dry.total_bytes_processed or 0)
    print(label + "_DRY_RUN_BYTES=" + str(estimate))

    if estimate > limit:
        raise RuntimeError(label + " exceeds query byte limit")

    job = client.query(
        sql,
        location=location,
        job_config=bigquery.QueryJobConfig(
            use_query_cache=False,
            maximum_bytes_billed=limit,
            labels={
                "portfolio": "bts_airline",
                "purpose": "airflow_reconcile",
            },
        ),
    )

    result = list(job.result(timeout=180))
    if job.statement_type != "SELECT":
        raise RuntimeError("Unexpected SQL statement type")

    print(label + "_QUERY_JOB_ID=" + job.job_id)
    print(label + "_BILLED_BYTES=" + str(
        int(job.total_bytes_billed or 0)
    ))
    return result


raw_date = "SAFE_CAST(`" + date_field.name + "` AS DATE)"

raw_sql = (
    "SELECT FORMAT_DATE('%Y-%m', " + raw_date + ") AS month, "
    "COUNT(*) AS n FROM `" + project + ".raw_bts.flights_2026` "
    "GROUP BY month ORDER BY month"
)

raw_counts = {
    row.month: int(row.n)
    for row in read_rows(raw_sql, "RAW")
}

if raw_counts != {"2026-01": 544003, "2026-02": 515037}:
    raise RuntimeError("Raw month counts mismatch")

print("RAW_MONTHS_VERIFIED=" + str(raw_counts))

target_sql = (
    "SELECT FORMAT_DATE('%Y-%m', flight_date) AS month, "
    "COUNT(*) AS n, COUNT(DISTINCT flight_key) AS unique_keys, "
    "COUNTIF(flight_key IS NULL) AS null_keys "
    "FROM `" + project
    + ".dbt_dev.fact_flights_2026_monthly_incremental` "
    "GROUP BY month ORDER BY month"
)

target_rows = read_rows(target_sql, "TARGET")
target_counts = {}

for row in target_rows:
    count = int(row.n)
    if count != int(row.unique_keys) or int(row.null_keys) != 0:
        raise RuntimeError("Duplicate or null flight keys in month")
    target_counts[row.month] = count

expected = (
    {"2026-01": 544003}
    if stage == "before"
    else {"2026-01": 544003, "2026-02": 515037}
)

if target_counts != expected:
    raise RuntimeError("Target month reconciliation mismatch")

if stage == "after":
    global_sql = (
        "SELECT COUNT(*) AS n, "
        "COUNT(DISTINCT flight_key) AS unique_keys, "
        "COUNTIF(flight_key IS NULL) AS null_keys "
        "FROM `" + project
        + ".dbt_dev.fact_flights_2026_monthly_incremental`"
    )
    total = read_rows(global_sql, "GLOBAL_KEYS")[0]

    if (
        int(total.n) != 1059040
        or int(total.unique_keys) != 1059040
        or int(total.null_keys) != 0
    ):
        raise RuntimeError("Global flight key integrity failed")

print("TARGET_MONTHS_VERIFIED=" + str(target_counts))
print("RECONCILIATION_" + stage.upper() + "_PASS")
"""


def reconcile_warehouse(stage):
    """Run read-only BigQuery checks in isolated dbt Python environment."""
    run_command(
        [PYTHON, "-c", BIGQUERY_CHECK, stage],
        timeout=600,
    )
    return "RECONCILIATION_" + stage.upper() + "_PASS"


@dag(
    dag_id="bts_2026_02_integrated_incremental",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bts", "bigquery", "dbt", "incremental"],
)
def bts_2026_02_integrated_incremental():

    @task(retries=0)
    def validate_existing_ingestion():
        if not RECEIPT.is_file():
            raise RuntimeError("February ingestion receipt missing")

        with RECEIPT.open("r", encoding="utf-8-sig") as f:
            receipt = json.load(f)

        required = {
            "month": "2026-02",
            "destination": "raw_bts.flights_2026",
            "job_status": "DONE",
            "imported_rows": 515037,
            "bad_records": 0,
            "verified_total_rows": 1059040,
        }

        for field, expected in required.items():
            if receipt.get(field) != expected:
                raise RuntimeError(
                    "February ingestion receipt mismatch: " + field
                )

        print("FEBRUARY_INGESTION_ALREADY_COMPLETE")
        print("NO_RAW_DATA_REUPLOADED")
        return "INGESTION_RECEIPT_PASS"

    @task(retries=0)
    def authorize_manual_execution(previous):
        configuration = (
            get_current_context()["dag_run"].conf or {}
        )

        required = {
            "month": "2026-02",
            "authorize_february_merge": True,
        }

        if configuration != required:
            raise RuntimeError(
                "Manual authorization required. "
                "February MERGE was not executed."
            )

        print("EXPLICIT_FEBRUARY_AUTHORIZATION_PASS")
        return previous

    @task(retries=0)
    def validate_bigquery_baseline(previous):
        reconcile_warehouse("before")
        return previous

    @task(retries=0)
    def transform_2026_upstream(previous):
        # Reuse 2026 staging, intermediate and fact views.
        # Do not rebuild 2024-2025 historical marts.
        return run_dbt("+fact_flights_2026", "upstream")

    @task(retries=0)
    def estimate_february_merge_cost(previous):
        """Estimate February MERGE using a full-column representative SQL statement."""

        script = r'''
from google.cloud import bigquery
import re

project = "tdb-dbt-bigquery-portfolio"
location = "asia-southeast1"
limit = 1_000_000_000

client = bigquery.Client(project=project, location=location)

target_id = project + ".dbt_dev.fact_flights_2026_monthly_incremental"
source_id = project + ".dbt_dev.fact_flights_2026"

target = client.get_table(target_id)
source = client.get_table(source_id)

if target.table_type != "TABLE" or source.table_type != "VIEW":
    raise RuntimeError("Unexpected BigQuery target or source type")

if target.num_rows != 544003:
    raise RuntimeError("January target baseline changed")

target_fields = list(target.schema)
source_fields = list(source.schema)

if len(target_fields) != 42:
    raise RuntimeError("Unexpected target schema size")

if [(f.name, f.field_type) for f in target_fields] != [
    (f.name, f.field_type) for f in source_fields
]:
    raise RuntimeError("Source and target schemas differ")

columns = [f.name for f in target_fields]

if any(
    not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
    for name in columns
):
    raise RuntimeError("Unexpected column identifier")

updates = ", ".join(
    "`" + c + "` = S.`" + c + "`"
    for c in columns
)

insert_columns = ", ".join(
    "`" + c + "`"
    for c in columns
)

insert_values = ", ".join(
    "S.`" + c + "`"
    for c in columns
)

sql = f"""
MERGE `{target_id}` AS T
USING (
    SELECT *
    FROM `{source_id}`
    WHERE flight_date >= DATE '2026-02-01'
      AND flight_date < DATE '2026-03-01'
) AS S
ON T.flight_key = S.flight_key
   AND T.flight_date >= DATE '2026-02-01'
   AND T.flight_date < DATE '2026-03-01'
WHEN MATCHED THEN
    UPDATE SET {updates}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns})
    VALUES ({insert_values})
"""

job = client.query(
    sql,
    location=location,
    job_config=bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        maximum_bytes_billed=limit,
    ),
)

estimated = int(job.total_bytes_processed or 0)

print("REPRESENTATIVE_MERGE_ESTIMATED_BYTES:", estimated)

if estimated <= 0 or estimated > limit:
    raise RuntimeError("MERGE estimate requires review")

print("REPRESENTATIVE_MERGE_DRY_RUN_PASS")
print("NOT_EXACT_DBT_COMPILED_SQL")
print("NO_MERGE_EXECUTED")
'''

        run_command([PYTHON, "-c", script], timeout=180)
        return previous

    @task(retries=0)
    def merge_february(previous):
        return run_dbt(
            "fact_flights_2026_monthly_incremental",
            "february_merge",
        )

    @task(retries=0)
    def verify_incremental_result(previous):
        reconcile_warehouse("after")
        return "FEBRUARY_TARGET_VALIDATED"

    @task(retries=0)
    def release_fact_reporting_handoff(previous):
        print("FEBRUARY_FACT_TABLE_READY")
        print("EXPECTED_TOTAL_FLIGHTS=1059040")
        print("JANUARY_ROWS=544003")
        print("FEBRUARY_ROWS=515037")
        print("HISTORICAL_2024_2025_MARTS_NOT_REBUILT")
        print("POWER_BI_2026_EXTENSION_STILL_PENDING")
        return "2026_FACT_HANDOFF_PASS"

    receipt = validate_existing_ingestion()
    authorized = authorize_manual_execution(receipt)
    baseline = validate_bigquery_baseline(authorized)
    upstream = transform_2026_upstream(baseline)
    estimated = estimate_february_merge_cost(upstream)
    merged = merge_february(estimated)
    verified = verify_incremental_result(merged)
    release_fact_reporting_handoff(verified)


dag = bts_2026_02_integrated_incremental()