"""BTS 2026 reporting publication.

Manual, explicitly authorized BigQuery view deployment.
Does not ingest raw data or rerun incremental MERGE.
"""

import json
import os
import subprocess
from datetime import datetime, timezone

from airflow.sdk import dag, task, get_current_context
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


PROJECT = "tdb-dbt-bigquery-portfolio"
DATASET = "dbt_dev"
LOCATION = "asia-southeast1"
DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/bts/dbt"
DBT_PROFILES = "/opt/bts/dbt-profile"

MODELS = (
    "fact_flights_reporting",
    "fact_flight_delay_cause_2026",
    "mart_airport_monthly_performance_2026",
    "mart_carrier_monthly_performance_2026",
    "mart_route_monthly_performance_2026",
    "mart_carrier_monthly_delay_cause_2026",
)

VARS = {
    "enable_incremental_demo": True,
    "enable_2026_incremental_demo": True,
    "enable_2026_reporting": True,
    "incremental_2026_mode": "monthly",
    "reprocess_month": "2026-02",
}


def client():
    return bigquery.Client(project=PROJECT, location=LOCATION)


def run_dbt(operation, extra):
    target = "/tmp/bts-airflow-dbt/reporting-publish/" + operation

    os.makedirs(target + "/target", exist_ok=True)
    os.makedirs(target + "/logs", exist_ok=True)

    command = [
        DBT,
        operation,
        "--project-dir", DBT_PROJECT,
        "--profiles-dir", DBT_PROFILES,
        "--target", "dev",
        "--target-path", target + "/target",
        "--log-path", target + "/logs",
        "--vars", json.dumps(VARS),
        *extra,
    ]

    result = subprocess.run(
        command,
        cwd=DBT_PROJECT,
        capture_output=True,
        text=True,
        timeout=1200,
        check=False,
    )

    if result.stdout:
        print(result.stdout[-30000:])

    if result.stderr:
        print(result.stderr[-10000:])

    if result.returncode != 0:
        raise RuntimeError(
            f"dbt {operation} failed with exit code {result.returncode}"
        )

    return result.stdout



# Separate one-time unified reporting publication.
# The original reporting DAG is not modified.

UNIFIED_MODELS = (
    "dim_date_reporting",
    "mart_airport_monthly_performance_reporting",
    "mart_carrier_monthly_performance_reporting",
    "mart_route_monthly_performance_reporting",
    "mart_carrier_monthly_delay_cause_reporting",
)

HISTORICAL_MARTS = (
    "mart_airport_monthly_performance",
    "mart_carrier_monthly_performance",
    "mart_route_monthly_performance",
    "mart_carrier_monthly_delay_cause",
)


def require_authorization():
    conf = get_current_context()["dag_run"].conf or {}

    if not isinstance(conf, dict):
        raise RuntimeError("DAG conf must be a JSON object")

    if set(conf) != {
        "release_scope",
        "authorize_unified_reporting",
    }:
        raise RuntimeError("Unexpected authorization fields")

    if conf["release_scope"] != "unified_reporting_views":
        raise RuntimeError("Incorrect release scope")

    if conf["authorize_unified_reporting"] is not True:
        raise RuntimeError("Explicit authorization required")

    return True


def check_normal_profile():
    from pathlib import Path
    import yaml

    profile_path = Path("/opt/bts/dbt-profile/profiles.yml")

    profiles = yaml.safe_load(
        profile_path.read_text(encoding="utf-8")
    )

    caps = []

    for profile in profiles.values():
        if not isinstance(profile, dict):
            continue

        output = profile.get("outputs", {}).get("dev")

        if isinstance(output, dict):
            caps.append(output.get("maximum_bytes_billed"))

    if len(caps) != 1 or int(caps[0]) != 1_000_000_000:
        raise RuntimeError("Normal 1 GB query limit not verified")

    print("NORMAL_1GB_QUERY_CAP_PASS", flush=True)


def verify_dependencies(bq):
    baselines = {
        "raw_bts.flights": 14080680,
        "raw_bts.flights_2026": 1059040,
        "dbt_dev.fact_flights_2026_monthly_incremental": 1059040,
    }

    for relation, expected_rows in baselines.items():
        obj = bq.get_table(f"{PROJECT}.{relation}")

        if obj.table_type != "TABLE" or obj.num_rows != expected_rows:
            raise RuntimeError("Source baseline mismatch: " + relation)

    calendar = bq.get_table(f"{PROJECT}.{DATASET}.dim_date")

    if calendar.table_type != "TABLE" or calendar.num_rows != 731:
        raise RuntimeError("Historical calendar missing")

    historical_fact = bq.get_table(
        f"{PROJECT}.{DATASET}.fact_flights"
    )

    if historical_fact.table_type != "VIEW":
        raise RuntimeError("Historical fact view missing")

    for name in MODELS:
        obj = bq.get_table(f"{PROJECT}.{DATASET}.{name}")

        if obj.table_type != "VIEW" or not obj.schema:
            raise RuntimeError("2026 supporting view missing: " + name)

    for name in HISTORICAL_MARTS:
        old = bq.get_table(f"{PROJECT}.{DATASET}.{name}")
        new = bq.get_table(f"{PROJECT}.{DATASET}.{name}_2026")

        if old.table_type != "TABLE" or not old.num_rows:
            raise RuntimeError("Historical mart unavailable: " + name)

        if new.table_type != "VIEW":
            raise RuntimeError("2026 mart unavailable: " + name)

        old_schema = {
            f.name: f.field_type.upper()
            for f in old.schema
        }

        new_schema = {
            f.name: f.field_type.upper()
            for f in new.schema
        }

        if old_schema != new_schema:
            raise RuntimeError("Incompatible mart schemas: " + name)


def verify_storage(bq):
    total = 0

    for dataset in ("raw_bts", "dbt_dev"):
        for ref in bq.list_tables(f"{PROJECT}.{dataset}"):
            obj = bq.get_table(ref.reference)

            if obj.table_type == "TABLE":
                total += int(obj.num_bytes or 0)

    gib = total / (1024 ** 3)

    print("TOTAL_LOGICAL_STORAGE_GIB:", round(gib, 5), flush=True)

    if gib >= 9:
        raise RuntimeError("Storage safeguard reached")


@dag(
    dag_id="bts_unified_reporting_publish_once",
    description="One-time controlled publication of BTS unified views",
    start_date=datetime(2026, 10, 29, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["bts", "dbt", "bigquery", "unified-reporting"],
)
def bts_unified_reporting_publish_once():

    @task(retries=0)
    def authorize_publication():
        require_authorization()
        print("EXPLICIT_UNIFIED_PUBLICATION_AUTHORIZED", flush=True)

    @task(retries=0)
    def validate_dependencies():
        require_authorization()
        check_normal_profile()

        bq = client()

        verify_dependencies(bq)
        verify_storage(bq)

        for name in UNIFIED_MODELS:
            try:
                bq.get_table(f"{PROJECT}.{DATASET}.{name}")
            except NotFound:
                print("DESTINATION_AVAILABLE:", name, flush=True)
                continue

            raise RuntimeError(
                "Reporting destination already exists: " + name
            )

        print("UNIFIED_DEPENDENCIES_PASS", flush=True)

    @task(retries=0)
    def validate_dbt_selection():
        require_authorization()
        check_normal_profile()

        output = run_dbt(
            "ls",
            [
                "--resource-type", "model",
                "--output", "json",
                "--select", *UNIFIED_MODELS,
            ],
        )

        selected = {}

        for line in output.splitlines():
            line = line.strip()

            if not line.startswith("{"):
                continue

            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            if item.get("resource_type") != "model":
                continue

            name = item["name"]

            if name in selected:
                raise RuntimeError("Duplicate dbt selection")

            if item.get("config", {}).get("materialized") != "view":
                raise RuntimeError("Non-view model selected: " + name)

            if item.get("package_name") != "bts_airline_analytics":
                raise RuntimeError("Unexpected dbt package: " + name)

            if item.get("unique_id") != (
                "model.bts_airline_analytics." + name
            ):
                raise RuntimeError("Unexpected dbt model identity: " + name)

            selected[name] = item

        if set(selected) != set(UNIFIED_MODELS):
            raise RuntimeError(
                "Unexpected dbt model selection: "
                + repr(sorted(selected))
            )

        print("EXACT_FIVE_UNIFIED_VIEWS_PASS", flush=True)
        print("NO_INCREMENTAL_MERGE_SELECTED", flush=True)

    @task(retries=0)
    def publish_unified_views():
        require_authorization()
        check_normal_profile()

        bq = client()

        # Fail closed if the destination changed after preflight.
        for name in UNIFIED_MODELS:
            try:
                bq.get_table(f"{PROJECT}.{DATASET}.{name}")
            except NotFound:
                continue

            raise RuntimeError(
                "Destination appeared before publication: " + name
            )

        run_dbt(
            "run",
            ["--select", *UNIFIED_MODELS],
        )

        print("FIVE_UNIFIED_VIEW_PUBLICATION_SUBMITTED", flush=True)

    @task(retries=0)
    def verify_publication():
        require_authorization()

        bq = client()

        for name in UNIFIED_MODELS:
            obj = bq.get_table(f"{PROJECT}.{DATASET}.{name}")

            if obj.table_type != "VIEW":
                raise RuntimeError("Expected BigQuery VIEW: " + name)

            if not obj.view_query or not obj.schema:
                raise RuntimeError("Incomplete unified view: " + name)

            print(
                "UNIFIED_VIEW_VERIFIED:",
                name,
                "| columns:", len(obj.schema),
                flush=True,
            )

        verify_dependencies(bq)
        verify_storage(bq)

        print("UNIFIED_REPORTING_PUBLICATION_PASS", flush=True)
        print("NO_FEBRUARY_MERGE_RERUN", flush=True)

    a = authorize_publication()
    b = validate_dependencies()
    c = validate_dbt_selection()
    d = publish_unified_views()
    e = verify_publication()

    a >> b >> c >> d >> e


dag = bts_unified_reporting_publish_once()
