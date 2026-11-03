"""Reusable BTS monthly file preparation DAG.

Default: read-only February fixture.
Explicit prepare mode: acquire and prepare the next eligible month.
No BigQuery uploads, dbt MERGE or Power BI refresh.
"""

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, get_current_context, task


ROOT = Path("/opt/bts")
PYTHON = Path("/home/airflow/dbt-venv/bin/python")
COORDINATOR = ROOT / "scripts/run_2026_monthly_ingestion.py"


def validate_run_mode(configuration):
    if configuration is None:
        configuration = {}

    if not isinstance(configuration, dict):
        raise ValueError("DAG configuration must be an object")

    if set(configuration) - {"mode", "authorize_preparation"}:
        raise ValueError("Unexpected configuration field")

    mode = configuration.get("mode", "fixture")

    if mode == "fixture":
        if "authorize_preparation" in configuration:
            raise ValueError("Fixture cannot include authorization")
        return "fixture"

    if mode == "prepare":
        if configuration.get("authorize_preparation") is not True:
            raise ValueError("Preparation requires explicit authorization")
        return "prepare"

    raise ValueError("Unsupported execution mode")


def check_environment():
    files = [
        PYTHON,
        COORDINATOR,
        ROOT / "scripts/fetch_2026_bts_source.py",
        ROOT / "scripts/project_batch.py",
        ROOT / "scripts/build_2026_bq_monthly.py",
        ROOT / "scripts/load_2026_bq_monthly.py",
        ROOT / "config/bts_core_columns.txt",
        ROOT / "evidence/source/2026_monthly_source_manifest.csv",
    ]

    directories = [
        ROOT / "data/raw/2026",
        ROOT / "data/processed/2026",
        ROOT / "data/load/2026",
        ROOT / "evidence/source",
        ROOT / "evidence/bigquery/monthly_ingestion",
        ROOT / "evidence/bigquery/monthly_processing",
    ]

    for path in files:
        if not path.is_file():
            raise RuntimeError("Missing required file: " + str(path))

    for path in directories:
        if not path.is_dir():
            raise RuntimeError("Missing required directory: " + str(path))

    return True


def get_next_candidate():
    scripts_directory = str(ROOT / "scripts")

    if scripts_directory not in sys.path:
        sys.path.insert(0, scripts_directory)

    from run_2026_monthly_ingestion import select_next_month

    selection = select_next_month(ROOT)

    if selection["status"] not in (
        "WAITING_FOR_PREPARED_SOURCE",
        "READY_FOR_LOCAL_PREFLIGHT",
    ):
        raise RuntimeError(
            "Monthly preparation blocked: " + selection["status"]
        )

    month = selection["month"]

    if not isinstance(month, str):
        raise RuntimeError("Invalid candidate month")

    if not month.startswith("2026-") or month <= "2026-02":
        raise RuntimeError("Historical or unsupported month")

    return selection


def invoke_existing_coordinator(mode):
    if mode == "fixture":
        month = "2026-02"
        command = [
            str(PYTHON),
            str(COORDINATOR),
            "--fixture-february",
        ]

    elif mode == "prepare":
        selection = get_next_candidate()
        month = selection["month"]

        print("SELECTED_MONTH:", month, flush=True)
        print("PREPARATION_STATUS:", selection["status"], flush=True)

        command = [
            str(PYTHON),
            str(COORDINATOR),
            "--prepare",
        ]

    else:
        raise RuntimeError("Unexpected execution mode")

    completed = subprocess.run(
        command,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=2400,
        check=False,
    )

    output = completed.stdout or ""
    print(output[-16000:], flush=True)

    if completed.returncode != 0:
        raise RuntimeError(
            "Monthly coordinator failed: exit code "
            + str(completed.returncode)
        )

    if mode == "fixture":
        if "FEBRUARY_FIXTURE_VALIDATION_PASS" not in output:
            raise RuntimeError("Fixture success marker missing")

        status = "FEBRUARY_FIXTURE_VALIDATED"

    else:
        fresh = "MONTHLY_PREPARATION_PASS: " + month in output

        existing = (
            "VALIDATING_EXISTING_OUTPUT_ONLY" in output
            and "LOCAL_PREPARED_FILE_PASS" in output
        )

        if not (fresh or existing):
            raise RuntimeError("Preparation success marker missing")

        tag = month.replace("-", "_")

        required = [
            ROOT / "data/processed/2026"
            / f"bts_{tag}_core.csv.gz",
            ROOT / "evidence/source"
            / f"{tag}_processed_manifest.csv",
            ROOT / "data/load/2026"
            / f"bts_{tag}_bq.csv.gz",
            ROOT / "evidence/source"
            / f"{tag}_bq_load_manifest.csv",
        ]

        if not all(path.is_file() for path in required):
            raise RuntimeError("Monthly preparation artifacts incomplete")

        status = "MONTHLY_FILES_PREPARED"

    return {
        "month": month,
        "mode": mode,
        "status": status,
        "bigquery_upload": False,
        "dbt_merge": False,
    }


@dag(
    dag_id="bts_2026_monthly_prepare",
    description="Guarded BTS monthly preparation",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bts", "monthly", "preparation", "no-merge"],
)
def bts_2026_monthly_prepare():

    @task(retries=0)
    def validate_request():
        context = get_current_context()
        dag_run = context.get("dag_run")
        configuration = (
            dag_run.conf if dag_run is not None else None
        )

        mode = validate_run_mode(configuration)
        print("AUTHORIZED_MODE:", mode)
        print("NO_BIGQUERY_LOAD_AUTHORIZED")
        print("NO_DBT_MERGE_AUTHORIZED")
        return mode

    @task(retries=0)
    def validate_container_mounts(mode):
        check_environment()
        print("MONTHLY_CONTAINER_MOUNTS_PASS")
        return mode

    @task(retries=0)
    def execute_preparation(mode):
        return invoke_existing_coordinator(mode)

    @task(retries=0)
    def report_preparation(result):
        print("PROCESSING_MONTH:", result["month"])
        print("PREPARATION_STATUS:", result["status"])
        print("BIGQUERY_UPLOAD:", result["bigquery_upload"])
        print("DBT_MERGE:", result["dbt_merge"])
        print("MONTHLY_PREPARATION_DAG_PASS")
        return result

    report_preparation(
        execute_preparation(
            validate_container_mounts(
                validate_request()
            )
        )
    )


bts_2026_monthly_prepare()
