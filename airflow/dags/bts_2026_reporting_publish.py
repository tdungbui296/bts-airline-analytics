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


@dag(
    dag_id="bts_2026_reporting_publish",
    description="Controlled publication of BTS 2026 reporting views",
    start_date=datetime(2026, 10, 29, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bts", "dbt", "bigquery", "reporting"],
)
def bts_2026_reporting_publish():

    @task(retries=0)
    def authorize_publication():
        conf = get_current_context()["dag_run"].conf or {}

        expected = {
            "release_scope": "2026_views",
            "authorize_reporting_publication": True,
        }

        if conf != expected:
            raise RuntimeError(
                "Exact manual reporting authorization required"
            )

        print("MANUAL_REPORTING_AUTHORIZATION_PASS")

    @task(retries=0)
    def validate_upstream_data():
        bq = client()

        raw = bq.get_table(f"{PROJECT}.raw_bts.flights")
        raw_2026 = bq.get_table(f"{PROJECT}.raw_bts.flights_2026")
        historical = bq.get_table(f"{PROJECT}.{DATASET}.fact_flights")

        incremental = bq.get_table(
            f"{PROJECT}.{DATASET}.fact_flights_2026_monthly_incremental"
        )

        if raw.table_type != "TABLE" or raw.num_rows != 14080680:
            raise RuntimeError("Historical raw baseline changed")

        if raw_2026.table_type != "TABLE" or raw_2026.num_rows != 1059040:
            raise RuntimeError("2026 raw baseline changed")

        if historical.table_type != "VIEW":
            raise RuntimeError("Historical fact view missing")

        if (
            incremental.table_type != "TABLE"
            or incremental.num_rows != 1059040
        ):
            raise RuntimeError("2026 incremental baseline changed")

        for name in MODELS:
            try:
                bq.get_table(f"{PROJECT}.{DATASET}.{name}")
            except NotFound:
                continue

            raise RuntimeError(
                f"Reporting destination already exists: {name}"
            )

        print("RAW_AND_INCREMENTAL_BASELINES_PASS")
        print("SIX_REPORTING_DESTINATIONS_AVAILABLE")
        print("NO_DATA_MODIFICATIONS")

    @task(retries=0)
    def validate_dbt_selection():
        output = run_dbt(
            "ls",
            [
                "--resource-type", "model",
                "--output", "json",
                "--select", " ".join(MODELS),
            ],
        )

        selected = set()

        for line in output.splitlines():
            line = line.strip()

            if not line.startswith("{"):
                continue

            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            if item.get("resource_type") == "model":
                selected.add(item["name"])

                if item.get("config", {}).get("materialized") != "view":
                    raise RuntimeError(
                        "Selected model is not a view: " + item["name"]
                    )

        if selected != set(MODELS):
            raise RuntimeError(
                "dbt selected models differ from the authorized six: "
                + repr(sorted(selected))
            )

        print("EXACT_SIX_DBT_VIEW_MODELS_SELECTED")
        print("NO_INCREMENTAL_MERGE_SELECTED")

    @task(retries=0)
    def publish_reporting_views():
        run_dbt(
            "run",
            ["--select", " ".join(MODELS)],
        )

        print("SIX_REPORTING_VIEWS_DEPLOYED")
        print("FEBRUARY_MERGE_NOT_SELECTED")

    @task(retries=0)
    def verify_reporting_views():
        bq = client()

        for name in MODELS:
            view = bq.get_table(f"{PROJECT}.{DATASET}.{name}")

            if view.table_type != "VIEW":
                raise RuntimeError("Expected VIEW: " + name)

            if not view.view_query:
                raise RuntimeError("Missing view SQL: " + name)

            if len(view.schema) == 0:
                raise RuntimeError("Empty view schema: " + name)

            print(
                f"REPORTING_VIEW_PASS: {name}"
                f" | columns={len(view.schema)}"
            )

        fact = bq.get_table(
            f"{PROJECT}.{DATASET}.fact_flights_2026_monthly_incremental"
        )

        if fact.table_type != "TABLE" or fact.num_rows != 1059040:
            raise RuntimeError("Incremental fact changed")

        print("SIX_REPORTING_VIEWS_VERIFIED")
        print("INCREMENTAL_FACT_UNCHANGED")
        print("REPORTING_PUBLICATION_SUCCESS")

    a = authorize_publication()
    b = validate_upstream_data()
    c = validate_dbt_selection()
    d = publish_reporting_views()
    e = verify_reporting_views()

    a >> b >> c >> d >> e


dag = bts_2026_reporting_publish()