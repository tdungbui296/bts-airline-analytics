"""Unified BTS 2026 monthly Airflow orchestration.

Fixture: prepare -> ingest -> skip readiness -> dbt (no BigQuery).
Live: prepare -> ingest -> readiness -> dbt -> completion check.

No schedule, no automatic retries, no automatic recovery,
and no reporting publication.
"""

import importlib.util
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, get_current_context, task
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.utils.trigger_rule import TriggerRule


ROOT = Path("/opt/bts")
COMPLETIONS = ROOT / "evidence/bigquery/monthly_processing"
INGESTIONS = ROOT / "evidence/bigquery/monthly_ingestion"
JOURNALS = ROOT / "evidence/dbt/monthly_runs"


def validate_parent_configuration(configuration):
    if configuration is None:
        configuration = {}

    if not isinstance(configuration, dict):
        raise ValueError("Unified DAG configuration must be an object")

    mode = configuration.get("mode", "fixture")

    if mode == "fixture":
        if set(configuration) - {"mode"}:
            raise ValueError("Fixture must not contain write permissions")

        return {
            "mode": "fixture",
            "month": "2026-02",
            "prepare_conf": {"mode": "fixture"},
            "ingest_conf": {"mode": "fixture"},
            "dbt_conf": {"mode": "fixture"},
        }

    required = {
        "mode",
        "month",
        "authorize_preparation",
        "authorize_bigquery_append",
        "authorize_dbt_merge",
        "accept_per_query_cap_bytes",
    }

    if mode != "live" or set(configuration) != required:
        raise ValueError("Exact live authorization fields required")

    month = configuration["month"]

    if not isinstance(month, str) or not re.fullmatch(
        r"2026-(0[3-9]|1[0-2])", month
    ):
        raise ValueError("Invalid or historical reporting month")

    for field in (
        "authorize_preparation",
        "authorize_bigquery_append",
        "authorize_dbt_merge",
    ):
        if configuration[field] is not True:
            raise ValueError("Explicit authorization missing: " + field)

    cap = configuration["accept_per_query_cap_bytes"]

    if type(cap) is not int or cap != 1_000_000_000:
        raise ValueError("Required dbt per-query cap acknowledgement missing")

    return {
        "mode": "live",
        "month": month,
        "prepare_conf": {
            "mode": "prepare",
            "authorize_preparation": True,
        },
        "ingest_conf": {
            "mode": "ingest",
            "month": month,
            "authorize_bigquery_append": True,
        },
        "dbt_conf": {
            "mode": "merge",
            "month": month,
            "authorize_dbt_merge": True,
            "accept_per_query_cap_bytes": cap,
        },
    }


def import_file(path, name):
    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError("Required monthly module unavailable: " + name)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def next_month(month):
    year, number = map(int, month.split("-"))

    if year != 2026 or number >= 12:
        return None

    return f"2026-{number + 1:02d}"


def reserve_unified_attempt(request, run_id):
    if request["mode"] == "fixture":
        print("UNIFIED_FEBRUARY_FIXTURE_AUTHORIZED", flush=True)
        print("NO_LIVE_ATTEMPT_JOURNAL_CREATED", flush=True)
        return request

    month = request["month"]

    gate = import_file(
        Path("/opt/airflow/dags/bts_monthly_readiness_gate.py"),
        "bts_unified_readiness_contract",
    )

    guard = import_file(
        ROOT / "scripts/guard_2026_monthly_merge.py",
        "bts_unified_merge_guard",
    )

    expected = "2026-03"

    while expected is not None:
        completion = COMPLETIONS / (expected + ".json")

        if not completion.exists():
            break

        gate.validate_completion_receipt(completion, expected)
        expected = next_month(expected)

    if expected != month:
        raise RuntimeError(
            "Nonconsecutive or already completed month blocked; "
            "expected " + str(expected)
        )

    if (INGESTIONS / (month + ".json")).exists():
        raise RuntimeError(
            "Existing ingestion receipt requires manual reconciliation"
        )

    state = guard.inspect_month(month)["state"]

    if state != "NEVER_ATTEMPTED":
        raise RuntimeError(
            "Existing or uncertain MERGE state: " + state
        )

    workspace = JOURNALS / (
        "unified_" + month.replace("-", "_")
    )

    if workspace.exists():
        raise RuntimeError(
            "Existing unified orchestration attempt; "
            "manual reconciliation required"
        )

    workspace.mkdir(parents=True, exist_ok=False)

    journal = workspace / "attempt.json"

    record = {
        "month": month,
        "stage": "unified_monthly_pipeline",
        "state": "RESERVED_BEFORE_PREPARATION",
        "airflow_run_id": run_id,
        "created_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "automatic_retry_permitted": False,
    }

    with journal.open("x", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    print("UNIFIED_ATTEMPT_RESERVED:", month, flush=True)
    print("SEQUENTIAL_MONTH_GUARD_PASS:", month, flush=True)

    return request


def verify_pipeline_completion(request):
    month = request["month"]

    if request["mode"] == "fixture":
        print("UNIFIED_FEBRUARY_FIXTURE_PASS")
        print("READINESS_BIGQUERY_GATE_SKIPPED_FOR_FIXTURE")
        print("NO_REAL_MONTH_RECEIPT_CREATED_BY_PARENT")
        print("NO_REPORTING_PUBLICATION")
        return {"month": month, "status": "FIXTURE_PASS"}

    gate = import_file(
        Path("/opt/airflow/dags/bts_monthly_readiness_gate.py"),
        "bts_unified_completion_contract",
    )

    receipt = gate.validate_completion_receipt(
        COMPLETIONS / (month + ".json"),
        month,
    )

    guard = import_file(
        ROOT / "scripts/guard_2026_monthly_merge.py",
        "bts_unified_completion_guard",
    )

    state = guard.inspect_month(month)

    if (
        not state["journal_exists"]
        or not state["run_results_exists"]
        or not state["completion_exists"]
    ):
        raise RuntimeError(
            "Persistent MERGE execution or completion evidence missing"
        )

    workspace = JOURNALS / (
        "unified_" + month.replace("-", "_")
    )

    if not (workspace / "attempt.json").is_file():
        raise RuntimeError("Unified orchestration journal missing")

    if receipt.get("reporting_published") is not False:
        raise RuntimeError("Unexpected reporting publication state")

    print("UNIFIED_MONTHLY_COMPLETION_PASS:", month, flush=True)
    print("NO_REPORTING_PUBLICATION")

    return {"month": month, "status": "DONE"}


@dag(
    dag_id="bts_2026_monthly_unified",
    description="Guarded parent DAG coordinating BTS monthly workflows",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["bts", "monthly", "unified", "guarded"],
)
def bts_2026_monthly_unified():

    @task(retries=0)
    def authorize_request():
        context = get_current_context()
        current_run = context.get("dag_run")

        configuration = (
            current_run.conf if current_run is not None else None
        )

        request = validate_parent_configuration(configuration)

        print("UNIFIED_EXECUTION_MODE:", request["mode"])
        print("UNIFIED_SELECTED_MONTH:", request["month"])

        return request

    @task(retries=0)
    def reserve_month(request):
        context = get_current_context()
        current_run = context.get("dag_run")

        if current_run is None:
            raise RuntimeError("Airflow DAG run context missing")

        return reserve_unified_attempt(
            request,
            current_run.run_id,
        )

    @task.branch(retries=0)
    def choose_readiness_path(request):
        if request["mode"] == "fixture":
            return "skip_readiness_fixture"

        return "trigger_readiness"

    @task(retries=0)
    def skip_readiness_fixture(request):
        if request["mode"] != "fixture":
            raise RuntimeError("Live run cannot bypass readiness")

        print("READINESS_SKIPPED_ONLY_FOR_FEBRUARY_FIXTURE")
        return "FIXTURE_READINESS_SKIP_PASS"

    @task(retries=0)
    def verify_end_state(request):
        return verify_pipeline_completion(request)

    request = authorize_request()
    reserved = reserve_month(request)

    prepare = TriggerDagRunOperator(
        task_id="trigger_preparation",
        trigger_dag_id="bts_2026_monthly_prepare",
        trigger_run_id="unified_prepare__{{ run_id }}",
        conf="{{ ti.xcom_pull(task_ids='reserve_month')['prepare_conf'] }}",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        skip_when_already_exists=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=2),
    )

    ingest = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="bts_2026_monthly_ingest",
        trigger_run_id="unified_ingest__{{ run_id }}",
        conf="{{ ti.xcom_pull(task_ids='reserve_month')['ingest_conf'] }}",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        skip_when_already_exists=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=1),
    )

    readiness = TriggerDagRunOperator(
        task_id="trigger_readiness",
        trigger_dag_id="bts_monthly_readiness_gate",
        trigger_run_id="unified_readiness__{{ run_id }}",
        conf="{{ {'month': ti.xcom_pull(task_ids='reserve_month')['month']} }}",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        skip_when_already_exists=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=1),
    )

    dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="bts_2026_monthly_dbt",
        trigger_run_id="unified_dbt__{{ run_id }}",
        conf="{{ ti.xcom_pull(task_ids='reserve_month')['dbt_conf'] }}",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
        skip_when_already_exists=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(hours=2),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    selector = choose_readiness_path(reserved)
    bypass = skip_readiness_fixture(reserved)
    final = verify_end_state(reserved)

    reserved >> prepare >> ingest >> selector
    selector >> readiness
    selector >> bypass
    readiness >> dbt
    bypass >> dbt
    dbt >> final


bts_2026_monthly_unified()
