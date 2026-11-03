"""Guarded BTS 2026 ingestion and readiness handoff.

Default fixture: validate February evidence without BigQuery calls.
Explicit ingest: load one prepared month using the existing loader.

Actual BigQuery readiness and dbt MERGE are separate approvals.
"""

import importlib.util
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, get_current_context, task


ROOT = Path("/opt/bts")
PYTHON = Path("/home/airflow/dbt-venv/bin/python")
LOADER = ROOT / "scripts/load_2026_bq_monthly.py"
COORDINATOR = ROOT / "scripts/run_2026_monthly_ingestion.py"
GATE = Path("/opt/airflow/dags/bts_monthly_readiness_gate.py")


def validate_run_configuration(configuration):
    if configuration is None:
        configuration = {}

    if not isinstance(configuration, dict):
        raise ValueError("DAG configuration must be an object")

    mode = configuration.get("mode", "fixture")

    if mode == "fixture":
        if set(configuration) - {"mode"}:
            raise ValueError("Fixture cannot accept write authorization")
        return {"mode": "fixture", "month": "2026-02"}

    if mode != "ingest":
        raise ValueError("Unsupported ingestion mode")

    required = {
        "mode", "month", "authorize_bigquery_append"
    }

    if set(configuration) != required:
        raise ValueError("Exact ingestion authorization fields required")

    month = configuration["month"]

    if not isinstance(month, str):
        raise ValueError("Invalid reporting month")

    if not re.fullmatch(r"2026-(0[3-9]|1[0-2])", month):
        raise ValueError("Historical or unsupported reporting month")

    if configuration["authorize_bigquery_append"] is not True:
        raise ValueError("BigQuery append is not explicitly authorized")

    return {"mode": "ingest", "month": month}


def import_existing_readiness_gate():
    """Reuse receipt validators without executing the gate's DAG."""
    spec = importlib.util.spec_from_file_location(
        "bts_readiness_ingestion_handoff", GATE
    )

    if spec is None or spec.loader is None:
        raise RuntimeError("Existing readiness gate unavailable")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def select_prepared_candidate(request):
    """Block month skipping, repeat ingestion and partial outputs."""
    if request["mode"] == "fixture":
        return request

    directory = str(ROOT / "scripts")

    if directory not in sys.path:
        sys.path.insert(0, directory)

    from run_2026_monthly_ingestion import select_next_month

    selection = select_next_month(ROOT)

    if selection["month"] != request["month"]:
        raise RuntimeError("Requested month is not next in sequence")

    if selection["status"] != "READY_FOR_LOCAL_PREFLIGHT":
        raise RuntimeError("Selected month is not fully prepared")

    month = request["month"]
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
        raise RuntimeError("Monthly prepared files incomplete")

    receipt = (
        ROOT / "evidence/bigquery/monthly_ingestion"
        / f"{month}.json"
    )

    if receipt.exists():
        raise RuntimeError(
            "Monthly receipt already exists: repeat ingestion blocked"
        )

    print("PREPARED_MONTH_ELIGIBLE:", month)
    print("SEQUENTIAL_MONTH_GUARD_PASS")

    return request


def execute_existing_loader(request):
    """Only explicitly authorized ingestion can append BigQuery rows."""
    if request["mode"] == "fixture":
        print("FEBRUARY_FIXTURE_NO_UPLOAD")
        return request

    month = int(request["month"][-2:])

    command = [
        str(PYTHON),
        str(LOADER),
        "--year", "2026",
        "--month", str(month),
        "--execute",
    ]

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
            "Existing BigQuery loader failed with exit code "
            + str(completed.returncode)
        )

    if "INGESTION_RECEIPT_SAVED:" not in output:
        raise RuntimeError(
            "Loader did not confirm an ingestion receipt"
        )

    print("EXISTING_MONTHLY_LOADER_COMPLETED")
    return request


def validate_receipt_handoff(request):
    """Validate receipt and manifest; do not re-run BigQuery SQL."""
    month = request["month"]

    if request["mode"] == "fixture":
        receipt_path = (
            ROOT / "evidence/bigquery"
            / "B3_38_2026_02_load_receipt.json"
        )
    else:
        receipt_path = (
            ROOT / "evidence/bigquery/monthly_ingestion"
            / f"{month}.json"
        )

    manifest_path = (
        ROOT / "evidence/source"
        / f"{month.replace('-', '_')}_bq_load_manifest.csv"
    )

    gate = import_existing_readiness_gate()

    receipt = gate.validate_ingestion_receipt(
        receipt_path, month
    )

    gate.validate_ingestion_manifest(
        manifest_path, receipt, month
    )

    if request["mode"] == "fixture":
        if receipt["imported_rows"] != 515037:
            raise RuntimeError("February reference count mismatch")

        print("FEBRUARY_INGESTION_HANDOFF_FIXTURE_PASS")
    else:
        print("INGESTION_HANDOFF_READY_FOR_READINESS_GATE:", month)

    print("RECEIPT_AND_MANIFEST_VALIDATION_PASS")
    print("NO_DBT_MERGE_AUTHORIZED")

    return {
        "month": month,
        "mode": request["mode"],
        "validated_rows": receipt["imported_rows"],
        "readiness_sql_executed": False,
        "dbt_merge_executed": False,
    }


@dag(
    dag_id="bts_2026_monthly_ingest",
    description="Guarded BTS BigQuery ingestion and receipt handoff",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bts", "monthly", "ingestion", "guarded"],
)
def bts_2026_monthly_ingest():

    @task(retries=0)
    def authorize_request():
        context = get_current_context()
        dag_run = context.get("dag_run")
        configuration = (
            dag_run.conf if dag_run is not None else None
        )

        request = validate_run_configuration(configuration)

        print("INGESTION_MODE:", request["mode"])
        print("SELECTED_MONTH:", request["month"])
        print("DBT_MERGE_NOT_AUTHORIZED")

        return request

    @task(retries=0)
    def verify_candidate(request):
        return select_prepared_candidate(request)

    @task(retries=0)
    def run_ingestion_or_fixture(request):
        return execute_existing_loader(request)

    @task(retries=0)
    def validate_handoff(request):
        result = validate_receipt_handoff(request)
        print("HANDOFF_VALIDATION_PASS:", result["month"])
        return result

    validate_handoff(
        run_ingestion_or_fixture(
            verify_candidate(
                authorize_request()
            )
        )
    )


bts_2026_monthly_ingest()
