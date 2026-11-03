"""Readiness-only gate for recurring BTS monthly processing.

No ingestion, SQL jobs, dbt runs or Power BI refresh.
Monthly ingestion must first publish validated receipts.
"""

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import dag, task


INGESTION_DIR = Path(
    "/opt/bts/evidence/bigquery/monthly_ingestion"
)
COMPLETION_DIR = Path(
    "/opt/bts/evidence/bigquery/monthly_processing"
)
MANIFEST_DIR = Path("/opt/bts/evidence/source")

# Historical baseline, not a list of authorized future months.
BASELINE_MONTH = "2026-02"

MONTH_PATTERN = re.compile(r"2026-(0[1-9]|1[0-2])\Z")
HASH_PATTERN = re.compile(r"[0-9a-fA-F]{64}\Z")


def validate_month(value):
    if not isinstance(value, str):
        raise ValueError("Month must be a string")
    if not MONTH_PATTERN.fullmatch(value):
        raise ValueError("Month outside 2026 source contract")
    return value


def next_month(month):
    validate_month(month)
    year, number = map(int, month.split("-"))
    if number == 12:
        return None
    return f"{year}-{number + 1:02d}"


def read_receipt(path):
    if not path.is_file():
        raise ValueError("Expected receipt is not a regular file")
    if path.stat().st_size > 32768:
        raise ValueError("Oversized receipt")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("Receipt must be a JSON object")
    return data


def validate_ingestion_receipt(path, month):
    """Validate the existing BigQuery load-receipt contract.

    This checks receipt structure only. A successful receipt
    does not independently authorize a BigQuery MERGE.
    """
    validate_month(month)
    record = read_receipt(path)

    if record.get("month") != month:
        raise ValueError("Ingestion receipt month mismatch")

    if record.get("project") != "tdb-dbt-bigquery-portfolio":
        raise ValueError("Unexpected BigQuery project")

    if record.get("destination") != "raw_bts.flights_2026":
        raise ValueError("Unexpected ingestion destination")

    if record.get("job_status") != "DONE":
        raise ValueError("Load job not reported complete")

    if type(record.get("bad_records")) is not int:
        raise ValueError("Invalid rejected-row count")

    if record["bad_records"] != 0:
        raise ValueError("Rejected records found")

    if type(record.get("imported_rows")) is not int:
        raise ValueError("Invalid imported-row count")

    if record["imported_rows"] <= 0:
        raise ValueError("Empty monthly ingestion")

    if type(record.get("verified_total_rows")) is not int:
        raise ValueError("Invalid cumulative row count")

    if record["verified_total_rows"] < record["imported_rows"]:
        raise ValueError("Inconsistent cumulative row count")

    if type(record.get("input_files")) is not int:
        raise ValueError("Invalid input-file count")

    if record["input_files"] <= 0:
        raise ValueError("No input files recorded")

    job_id = record.get("job_id")

    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("Missing BigQuery load job ID")

    source_hash = record.get("source_sha256")

    if not isinstance(source_hash, str):
        raise ValueError("Missing source SHA256")

    if not HASH_PATTERN.fullmatch(source_hash):
        raise ValueError("Invalid source SHA256 format")

    return record


def validate_ingestion_manifest(path, receipt, month):
    """Validate monthly manifest against the existing load receipt.

    This is an offline evidence check, not an independent
    verification of the BigQuery load job or source file bytes.
    """
    validate_month(month)

    if not path.is_file():
        raise ValueError("Monthly ingestion manifest missing")

    if path.stat().st_size > 32768:
        raise ValueError("Oversized monthly manifest")

    expected_columns = [
        "reporting_year",
        "reporting_month",
        "input_file",
        "input_sha256",
        "expected_row_count",
        "output_file",
        "output_row_count",
        "row_count_reconciled",
        "output_column_count",
        "output_size_bytes",
        "output_size_mb",
        "output_sha256",
    ]

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != expected_columns:
            raise ValueError("Unexpected manifest columns")

        rows = list(reader)

    if len(rows) != 1:
        raise ValueError("Expected exactly one monthly manifest row")

    row = rows[0]

    def positive_integer(field):
        value = row.get(field)

        if not isinstance(value, str):
            raise ValueError("Missing manifest field: " + field)

        if not re.fullmatch(r"[0-9]+", value.strip()):
            raise ValueError("Noninteger manifest field: " + field)

        number = int(value.strip())

        if number <= 0:
            raise ValueError("Nonpositive manifest field: " + field)

        return number

    year, number = map(int, month.split("-"))

    if positive_integer("reporting_year") != year:
        raise ValueError("Manifest year mismatch")

    if positive_integer("reporting_month") != number:
        raise ValueError("Manifest month mismatch")

    expected_count = positive_integer("expected_row_count")
    output_count = positive_integer("output_row_count")

    if expected_count != output_count:
        raise ValueError("Manifest row counts do not reconcile")

    if output_count != receipt["imported_rows"]:
        raise ValueError("Manifest and load receipt row counts differ")

    if str(row["row_count_reconciled"]).strip().lower() != "true":
        raise ValueError("Manifest reconciliation not passed")

    if positive_integer("output_column_count") != 51:
        raise ValueError("BigQuery-ready file must have 51 columns")

    positive_integer("output_size_bytes")

    expected_prefix = month.replace("-", "_")

    for field, suffix in (
        ("input_file", "_core.csv.gz"),
        ("output_file", "_bq.csv.gz"),
    ):
        filename = str(row.get(field) or "")
        basename = filename.replace("\\", "/").split("/")[-1]

        if basename != "bts_" + expected_prefix + suffix:
            raise ValueError("Unexpected monthly filename: " + field)

    for field in ("input_sha256", "output_sha256"):
        value = row.get(field)

        if not isinstance(value, str):
            raise ValueError("Missing manifest hash: " + field)

        if not HASH_PATTERN.fullmatch(value):
            raise ValueError("Invalid manifest hash: " + field)

    # The existing BigQuery receipt hashes the BQ-ready output,
    # not the earlier processed input file.
    if row["output_sha256"].lower() != (
        receipt["source_sha256"].lower()
    ):
        raise ValueError("Loaded output SHA256 mismatch")

    return row


def verify_bigquery_load_metadata(client, month, receipt):
    """Independently check an existing BigQuery load job.

    Metadata-only. Does not execute SQL or modify data.
    """
    validate_month(month)

    project = "tdb-dbt-bigquery-portfolio"
    location = "asia-southeast1"

    if receipt.get("month") != month:
        raise ValueError("Load receipt reporting month mismatch")

    if receipt.get("project") != project:
        raise ValueError("Unexpected BigQuery project")

    if receipt.get("region") != location:
        raise ValueError("Unexpected load-job region")

    job_id = receipt.get("job_id")

    if not isinstance(job_id, str):
        raise ValueError("Missing load job ID")

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,1024}", job_id):
        raise ValueError("Invalid load job ID")

    job = client.get_job(
        job_id,
        project=project,
        location=location,
    )

    if job.job_id != job_id:
        raise ValueError("Returned job identifier mismatch")

    if job.job_type != "load":
        raise ValueError("Expected BigQuery load job")

    if job.state != "DONE" or job.error_result is not None:
        raise ValueError("BigQuery load did not succeed")

    if str(job.location).lower() != location:
        raise ValueError("Load-job location mismatch")

    destination = job.destination

    if destination is None:
        raise ValueError("Load destination missing")

    if (
        destination.project != project
        or destination.dataset_id != "raw_bts"
        or destination.table_id != "flights_2026"
    ):
        raise ValueError("Unexpected load destination")

    if job.output_rows != receipt["imported_rows"]:
        raise ValueError("Loaded row count differs from receipt")

    if job.input_files != receipt["input_files"]:
        raise ValueError("Input-file count differs from receipt")

    table = client.get_table(
        project + ".raw_bts.flights_2026"
    )

    if table.table_type != "TABLE":
        raise ValueError("Raw destination is not a table")

    if len(table.schema) != 51:
        raise ValueError("Raw BTS column contract mismatch")

    # The raw table may include later months.
    # Never require it to equal an older receipt's row total.
    if (
        type(table.num_rows) is not int
        or table.num_rows < receipt["verified_total_rows"]
    ):
        raise ValueError("Raw cumulative row baseline mismatch")

    return {
        "month": month,
        "job_id": job_id,
        "output_rows": job.output_rows,
        "raw_table_rows": table.num_rows,
        "raw_columns": len(table.schema),
    }


def verify_monthly_source_quality(client, month, receipt):
    """Bounded, month-scoped transformed-source quality check."""
    from datetime import date
    from google.cloud import bigquery

    validate_month(month)

    if receipt.get("month") != month:
        raise ValueError("Source receipt month mismatch")

    if receipt.get("job_status") != "DONE":
        raise ValueError("Source load not complete")

    if type(receipt.get("imported_rows")) is not int:
        raise ValueError("Invalid expected source count")

    if receipt["imported_rows"] <= 0:
        raise ValueError("Empty expected source")

    project = "tdb-dbt-bigquery-portfolio"
    location = "asia-southeast1"
    cap = 200_000_000

    year, number = map(int, month.split("-"))
    start = date(year, number, 1)
    end = date(year + (number == 12), number % 12 + 1, 1)

    source_id = project + ".dbt_dev.fact_flights_2026"
    source = client.get_table(source_id)

    if source.table_type not in ("VIEW", "TABLE"):
        raise ValueError("Unexpected transformed source type")

    columns = {
        column.name: column.field_type
        for column in source.schema
    }

    if columns.get("flight_date") != "DATE":
        raise ValueError("Source flight_date is not DATE")

    if columns.get("flight_key") != "STRING":
        raise ValueError("Source flight_key is not STRING")

    sql = """
SELECT
    COUNT(*) AS source_rows,
    COUNT(DISTINCT flight_key) AS distinct_flight_keys,
    COUNTIF(flight_key IS NULL OR flight_key = '')
        AS invalid_flight_keys,
    MIN(flight_date) AS first_flight_date,
    MAX(flight_date) AS last_flight_date
FROM `tdb-dbt-bigquery-portfolio.dbt_dev.fact_flights_2026`
WHERE flight_date >= @month_start
  AND flight_date < @next_month_start
"""

    parameters = [
        bigquery.ScalarQueryParameter("month_start", "DATE", start),
        bigquery.ScalarQueryParameter(
            "next_month_start", "DATE", end
        ),
    ]

    dry_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_legacy_sql=False,
        use_query_cache=False,
        maximum_bytes_billed=cap,
        query_parameters=parameters,
    )

    dry_job = client.query(
        sql, job_config=dry_config, location=location
    )

    estimated = int(dry_job.total_bytes_processed or 0)

    if estimated <= 0 or estimated > cap:
        raise ValueError("Quality query exceeds safety cap")

    config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        use_query_cache=False,
        maximum_bytes_billed=cap,
        query_parameters=parameters,
        labels={
            "project": "bts_portfolio",
            "checkpoint": "monthly_quality",
        },
    )

    job = client.query(
        sql, job_config=config, location=location
    )

    rows = list(job.result(timeout=120))

    if len(rows) != 1:
        raise ValueError("Unexpected quality result cardinality")

    row = rows[0]
    total = int(row["source_rows"])
    distinct = int(row["distinct_flight_keys"])
    invalid = int(row["invalid_flight_keys"])
    first = row["first_flight_date"]
    last = row["last_flight_date"]

    if total != receipt["imported_rows"]:
        raise ValueError("Monthly source row count mismatch")

    if invalid != 0 or distinct != total:
        raise ValueError("Duplicate, null or empty flight keys")

    if (
        first is None
        or last is None
        or not (start <= first < end)
        or not (start <= last < end)
    ):
        raise ValueError("Source reporting dates are invalid")

    billed = job.total_bytes_billed

    if billed is not None and int(billed) > cap:
        raise ValueError("Quality query exceeded billing cap")

    return {
        "month": month,
        "job_id": job.job_id,
        "source_rows": total,
        "distinct_flight_keys": distinct,
        "invalid_flight_keys": invalid,
        "first_flight_date": str(first),
        "last_flight_date": str(last),
        "dry_run_bytes": estimated,
        "bytes_billed": billed,
        "quality_status": "PASS",
    }


def validate_completion_receipt(path, month):
    record = read_receipt(path)

    if record.get("month") != month:
        raise ValueError("Completion receipt month mismatch")
    if record.get("status") != "DONE":
        raise ValueError("Month has not completed")
    if not isinstance(record.get("dbt_invocation_id"), str):
        raise ValueError("Missing dbt invocation ID")
    if not record["dbt_invocation_id"].strip():
        raise ValueError("Empty dbt invocation ID")

    return record


def choose_next_ready_month(
    ingestion_dir=INGESTION_DIR,
    completion_dir=COMPLETION_DIR,
    manifest_dir=MANIFEST_DIR,
):
    """Select next consecutive month; never skip an unprocessed month."""
    month = next_month(BASELINE_MONTH)

    while month is not None:
        completion = completion_dir / f"{month}.json"

        if not completion.exists():
            break

        validate_completion_receipt(completion, month)
        month = next_month(month)

    if month is None:
        return None

    ingestion = ingestion_dir / f"{month}.json"

    if not ingestion.exists():
        return None

    receipt = validate_ingestion_receipt(ingestion, month)
    manifest = manifest_dir / (
        month.replace("-", "_") + "_bq_load_manifest.csv"
    )
    validate_ingestion_manifest(manifest, receipt, month)
    return month


@dag(
    dag_id="bts_monthly_readiness_gate",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bts", "monthly", "readiness", "no-merge"],
)
def bts_monthly_readiness_gate():

    @task(retries=0)
    def check_monthly_ingestion():
        month = choose_next_ready_month()

        if month is None:
            raise AirflowSkipException(
                "No consecutive month with validated ingestion evidence"
            )

        print("CANDIDATE_MONTH:", month)
        print("RECEIPT_VALIDATION_ONLY")
        return month

    @task(retries=0)
    def verify_bigquery_load(month):
        """Revalidate evidence and check real load metadata."""
        from google.cloud import bigquery

        receipt_path = INGESTION_DIR / (month + ".json")
        receipt = validate_ingestion_receipt(
            receipt_path, month
        )

        manifest_path = MANIFEST_DIR / (
            month.replace("-", "_") + "_bq_load_manifest.csv"
        )

        validate_ingestion_manifest(
            manifest_path, receipt, month
        )

        client = bigquery.Client(
            project="tdb-dbt-bigquery-portfolio",
            location="asia-southeast1",
        )

        result = verify_bigquery_load_metadata(
            client, month, receipt
        )

        print("BIGQUERY_LOAD_METADATA_VERIFIED:", month)
        print("VERIFIED_LOADED_ROWS:", result["output_rows"])
        print("NO_BIGQUERY_SQL_EXECUTED")
        print("MERGE_NOT_AUTHORIZED")

        return month

    @task(retries=0)
    def check_monthly_source_quality(month):
        from google.cloud import bigquery

        receipt = validate_ingestion_receipt(
            INGESTION_DIR / (month + ".json"), month
        )

        client = bigquery.Client(
            project="tdb-dbt-bigquery-portfolio",
            location="asia-southeast1",
        )

        quality = verify_monthly_source_quality(
            client, month, receipt
        )

        print("MONTHLY_SOURCE_QUALITY_PASS:", month)
        print("QUALITY_JOB_ID:", quality["job_id"])
        print("QUALITY_BYTES_BILLED:", quality["bytes_billed"])
        print("MERGE_NOT_AUTHORIZED")

        return quality

    @task(retries=0)
    def reconcile_raw_months(quality):
        """Require complete raw-table reconciliation before release."""
        import subprocess
        from pathlib import Path

        month = quality["month"]
        validate_month(month)

        validator = Path(
            "/opt/bts/scripts/verify_2026_raw_months.py"
        )

        if not validator.is_file():
            raise RuntimeError("Raw reconciliation script missing")

        command = [
            "/home/airflow/dbt-venv/bin/python",
            str(validator),
            "--verify",
            "--month",
            month,
        ]

        completed = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=900,
        )

        output = completed.stdout or ""
        print(output[-16000:], flush=True)

        if completed.returncode != 0:
            raise RuntimeError(
                "Raw reconciliation failed with exit code "
                + str(completed.returncode)
            )

        expected_marker = (
            "RAW_MONTH_RECONCILIATION_PASS: " + month
        )

        if expected_marker not in output:
            raise RuntimeError(
                "Raw reconciliation completion marker missing"
            )

        print("MONTHLY_RAW_DISTRIBUTION_VERIFIED:", month)
        print("DBT_MERGE_NOT_AUTHORIZED")
        return quality

    @task(retries=0)
    def report_candidate(quality):
        month = quality["month"]
        print("QUALITY_QUERY_JOB_ID:", quality["job_id"])
        print("READY_CANDIDATE:", month)
        print("RAW_MONTH_RECONCILIATION_GATE_PASS")
        print("MERGE_NOT_AUTHORIZED")
        print("NO_DBT_EXECUTION")
        print("NO_POWER_BI_REFRESH")
        return month

    report_candidate(
        reconcile_raw_months(
            check_monthly_source_quality(
                verify_bigquery_load(check_monthly_ingestion())
            )
        )
    )


bts_monthly_readiness_gate()
