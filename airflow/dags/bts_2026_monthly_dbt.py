"""BTS 2026 month-aware, explicitly authorized dbt processing.

Default fixture performs no BigQuery calls or dbt execution.
Live MERGE requires ingestion, readiness and raw reconciliation.
No automatic scheduling, retries, or reporting publication.
"""

import importlib.util
import json
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from airflow.sdk import dag, get_current_context, task


ROOT = Path("/opt/bts")
DBT_PYTHON = "/home/airflow/dbt-venv/bin/python"
GATE_PATH = Path("/opt/airflow/dags/bts_monthly_readiness_gate.py")
FACT_TABLE = "tdb-dbt-bigquery-portfolio.dbt_dev.fact_flights_2026_monthly_incremental"
PROJECT = "tdb-dbt-bigquery-portfolio"
LOCATION = "asia-southeast1"
DBT_QUERY_CAP = 1_000_000_000


def validate_configuration(configuration):
    if configuration is None:
        configuration = {}

    if not isinstance(configuration, dict):
        raise ValueError("DAG configuration must be an object")

    mode = configuration.get("mode", "fixture")

    if mode == "fixture":
        if set(configuration) - {"mode"}:
            raise ValueError("Fixture cannot authorize writes")
        return {"mode": "fixture", "month": "2026-02"}

    if mode != "merge":
        raise ValueError("Unsupported dbt execution mode")

    required = {
        "mode",
        "month",
        "authorize_dbt_merge",
        "accept_per_query_cap_bytes",
    }

    if set(configuration) != required:
        raise ValueError("Exact MERGE authorization fields required")

    month = configuration["month"]

    if not isinstance(month, str) or not re.fullmatch(
        r"2026-(0[3-9]|1[0-2])", month
    ):
        raise ValueError("Historical or unsupported month")

    if configuration["authorize_dbt_merge"] is not True:
        raise ValueError("MERGE authorization missing")

    if (
        type(configuration["accept_per_query_cap_bytes"]) is not int
        or configuration["accept_per_query_cap_bytes"] != 1_000_000_000
    ):
        raise ValueError("Per-query cost cap not acknowledged")

    return {"mode": "merge", "month": month}


def import_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError("Required module unavailable: " + name)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def plan_for_request(request):
    planner = import_module(
        ROOT / "scripts/plan_2026_monthly_dbt.py",
        "bts_dynamic_dbt_planner",
    )

    return planner.plan_month(
        request["month"],
        fixture=request["mode"] == "fixture",
    )


def check_dbt_profile_cap():
    import yaml

    project = yaml.safe_load(
        (ROOT / "dbt/dbt_project.yml").read_text(
            encoding="utf-8-sig"
        )
    )

    profile_name = project["profile"]

    profiles = yaml.safe_load(
        (ROOT / "dbt-profile/profiles.yml").read_text(
            encoding="utf-8-sig"
        )
    )

    target = profiles[profile_name]["outputs"]["dev"]

    cap = target.get("maximum_bytes_billed")

    if type(cap) is not int or not 0 < cap <= DBT_QUERY_CAP:
        raise RuntimeError("dbt BigQuery cost cap is not acceptable")

    if target.get("type") != "bigquery":
        raise RuntimeError("Unexpected dbt adapter")

    print("DBT_PER_QUERY_COST_CAP_VERIFIED:", cap, flush=True)


def run_command(command, timeout=3600):
    completed = subprocess.run(
        command,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        check=False,
    )

    output = completed.stdout or ""
    print(output[-16000:], flush=True)

    if completed.returncode != 0:
        raise RuntimeError(
            "Command failed, exit code " + str(completed.returncode)
        )

    return output


def bigquery_client():
    from google.cloud import bigquery

    return bigquery.Client(
        project=PROJECT,
        location=LOCATION,
    )


def bounded_fact_count(month, with_distinct=False):
    from google.cloud import bigquery

    client = bigquery_client()
    start = date.fromisoformat(month + "-01")

    if with_distinct:
        expressions = (
            "COUNT(*) AS n, "
            "COUNT(DISTINCT flight_key) AS unique_keys, "
            "COUNTIF(flight_key IS NULL OR flight_key = '') AS invalid_keys"
        )
    else:
        expressions = "COUNT(*) AS n"

    sql = (
        "SELECT " + expressions
        + " FROM `" + FACT_TABLE + "` "
        + "WHERE flight_date >= @month_start "
        + "AND flight_date < DATE_ADD(@month_start, INTERVAL 1 MONTH)"
    )

    parameters = [
        bigquery.ScalarQueryParameter(
            "month_start", "DATE", start
        )
    ]

    options = dict(
        use_query_cache=False,
        maximum_bytes_billed=DBT_QUERY_CAP,
        query_parameters=parameters,
    )

    dry = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            dry_run=True, **options
        ),
        location=LOCATION,
    )

    estimated = dry.total_bytes_processed

    if (
        type(estimated) is not int
        or estimated < 0
        or estimated > DBT_QUERY_CAP
    ):
        raise RuntimeError("Fact verification exceeds query cap")

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(**options),
        location=LOCATION,
    )

    results = list(job.result(timeout=300))

    if len(results) != 1:
        raise RuntimeError("Unexpected fact validation result")

    row = results[0]

    return {
        "rows": int(row.n),
        "unique_keys": (
            int(row.unique_keys) if with_distinct else None
        ),
        "invalid_keys": (
            int(row.invalid_keys) if with_distinct else None
        ),
        "job_id": job.job_id,
    }


def validate_readiness(request):
    gate = import_module(
        GATE_PATH, "bts_existing_monthly_readiness"
    )

    month = request["month"]

    if request["mode"] == "fixture":
        receipt = gate.validate_ingestion_receipt(
            ROOT / "evidence/bigquery/B3_38_2026_02_load_receipt.json",
            month,
        )

        gate.validate_ingestion_manifest(
            ROOT / "evidence/source/2026_02_bq_load_manifest.csv",
            receipt,
            month,
        )

        if receipt["imported_rows"] != 515037:
            raise RuntimeError("February fixture row-count mismatch")

        plan_for_request(request)

        print("FEBRUARY_DBT_FIXTURE_PASS", flush=True)
        print("NO_BIGQUERY_API_CALLS")
        print("NO_DBT_RUN")
        return {"month": month, "mode": "fixture"}

    if gate.choose_next_ready_month() != month:
        raise RuntimeError(
            "Requested month has not passed sequential receipt selection"
        )

    receipt_path = (
        ROOT / "evidence/bigquery/monthly_ingestion"
        / (month + ".json")
    )

    receipt = gate.validate_ingestion_receipt(
        receipt_path, month
    )

    manifest = (
        ROOT / "evidence/source"
        / (month.replace("-", "_") + "_bq_load_manifest.csv")
    )

    gate.validate_ingestion_manifest(
        manifest, receipt, month
    )

    completion = (
        ROOT / "evidence/bigquery/monthly_processing"
        / (month + ".json")
    )

    if completion.exists():
        raise RuntimeError("Month already marked complete")

    guard = import_module(
        ROOT / "scripts/guard_2026_monthly_merge.py",
        "bts_2026_monthly_merge_guard",
    )

    merge_state = guard.inspect_month(month)["state"]

    if merge_state != "NEVER_ATTEMPTED":
        raise RuntimeError(
            "Previous or uncertain MERGE state: "
            + merge_state
            + ". Manual reconciliation required."
        )

    check_dbt_profile_cap()
    plan_for_request(request)

    client = bigquery_client()

    gate.verify_bigquery_load_metadata(
        client, month, receipt
    )

    quality = gate.verify_monthly_source_quality(
        client, month, receipt
    )

    if quality["month"] != month:
        raise RuntimeError("Readiness quality month mismatch")

    run_command([
        DBT_PYTHON,
        str(ROOT / "scripts/verify_2026_raw_months.py"),
        "--verify", "--month", month,
    ], timeout=900)

    prior = bounded_fact_count(month)

    if prior["rows"] != 0:
        raise RuntimeError(
            "Target already contains selected month; repeat MERGE blocked"
        )

    print("MONTHLY_BIGQUERY_READINESS_PASS:", month, flush=True)
    print("PRE_MERGE_TARGET_EMPTY_PASS:", month, flush=True)

    return {
        "month": month,
        "mode": "merge",
        "expected_rows": receipt["imported_rows"],
        "ingestion_job_id": receipt["job_id"],
    }


def execute_upstream(request):
    if request["mode"] == "fixture":
        print("UPSTREAM_DBT_SKIPPED_FOR_FIXTURE")
        return request

    plan = plan_for_request(request)
    run_command(plan["upstream"])

    print("MONTHLY_DBT_UPSTREAM_PASS:", request["month"])
    return request


def execute_merge(request):
    if request["mode"] == "fixture":
        print("MERGE_SKIPPED_FOR_FIXTURE")
        return request

    month = request["month"]

    if bounded_fact_count(month)["rows"] != 0:
        raise RuntimeError(
            "Selected month appeared in target before MERGE"
        )

    plan = plan_for_request(request)
    command = plan["merge"]

    guard = import_module(
        ROOT / "scripts/guard_2026_monthly_merge.py",
        "bts_2026_monthly_merge_guard",
    )

    guard.reserve_merge(month)
    print(
        "PERSISTENT_MERGE_ATTEMPT_RESERVED:",
        month,
        flush=True,
    )

    run_command(command)

    target_path = Path(
        command[command.index("--target-path") + 1]
    )

    results_path = target_path / "run_results.json"

    if not results_path.is_file():
        raise RuntimeError("dbt run_results.json missing")

    with results_path.open("r", encoding="utf-8") as handle:
        results = json.load(handle)

    invocation_id = results.get("metadata", {}).get(
        "invocation_id"
    )

    if not isinstance(invocation_id, str) or not invocation_id:
        raise RuntimeError("dbt invocation ID missing")

    matching = [
        row for row in results.get("results", [])
        if row.get("unique_id", "").endswith(
            ".fact_flights_2026_monthly_incremental"
        )
        and row.get("status") == "success"
    ]

    if len(matching) != 1:
        raise RuntimeError("Expected successful incremental model missing")

    print("MONTHLY_DBT_MERGE_PASS:", month, flush=True)

    return {
        **request,
        "dbt_invocation_id": invocation_id,
    }


def verify_and_complete(request):
    if request["mode"] == "fixture":
        print("FEBRUARY_DBT_DAG_FIXTURE_PASS")
        print("NO_COMPLETION_RECEIPT_WRITTEN")
        return request

    month = request["month"]
    expected = request["expected_rows"]

    result = bounded_fact_count(
        month, with_distinct=True
    )

    if (
        result["rows"] != expected
        or result["unique_keys"] != expected
        or result["invalid_keys"] != 0
    ):
        raise RuntimeError(
            "Post-MERGE fact rows or flight keys failed reconciliation"
        )

    completion = (
        ROOT / "evidence/bigquery/monthly_processing"
        / (month + ".json")
    )

    record = {
        "month": month,
        "status": "DONE",
        "dbt_invocation_id": request["dbt_invocation_id"],
        "verified_fact_rows": result["rows"],
        "ingestion_job_id": request["ingestion_job_id"],
        "verification_job_id": result["job_id"],
        "reporting_published": False,
    }

    with completion.open("x", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2)
        handle.write("\n")

    print("MONTHLY_COMPLETION_RECEIPT_SAVED:", completion)
    print("MONTHLY_DBT_FACT_RECONCILIATION_PASS:", month)
    print("REPORTING_PUBLICATION_NOT_EXECUTED")

    return record


@dag(
    dag_id="bts_2026_monthly_dbt",
    description="Guarded month-aware dbt incremental execution",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bts", "monthly", "dbt", "incremental", "guarded"],
)
def bts_2026_monthly_dbt():

    @task(retries=0)
    def authorize_request():
        context = get_current_context()
        dag_run = context.get("dag_run")

        configuration = (
            dag_run.conf if dag_run is not None else None
        )

        request = validate_configuration(configuration)
        print("DBT_EXECUTION_MODE:", request["mode"])
        print("SELECTED_MONTH:", request["month"])
        return request

    @task(retries=0)
    def verify_readiness(request):
        return validate_readiness(request)

    @task(retries=0)
    def run_upstream(request):
        return execute_upstream(request)

    @task(retries=0)
    def run_incremental_merge(request):
        return execute_merge(request)

    @task(retries=0)
    def verify_completion(request):
        return verify_and_complete(request)

    verify_completion(
        run_incremental_merge(
            run_upstream(
                verify_readiness(
                    authorize_request()
                )
            )
        )
    )


bts_2026_monthly_dbt()
