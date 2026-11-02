"""DAG: manually authorized, one-time BTS historical reporting recovery.

Six historical dbt relations only. No 2026 MERGE, no Power BI refresh.
This DAG must never be added to a recurring schedule.
"""

import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import yaml
from airflow.sdk import dag, task, get_current_context
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


PROJECT = "tdb-dbt-bigquery-portfolio"
DATASET = "dbt_dev"
LOCATION = "asia-southeast1"

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = Path("/opt/bts/dbt")
ORIGINAL_PROFILES = Path("/opt/bts/dbt-profile")

NORMAL_CAP = 1_000_000_000
RECOVERY_CAP = 3_000_000_000
STORAGE_STOP_BYTES = 9 * (1024 ** 3)

MODELS = (
    "fact_flight_delay_cause",
    "dim_date",
    "mart_airport_monthly_performance",
    "mart_carrier_monthly_performance",
    "mart_route_monthly_performance",
    "mart_carrier_monthly_delay_cause",
)

EXPECTED_MATERIALIZATIONS = {
    "fact_flight_delay_cause": "view",
    "dim_date": "table",
    "mart_airport_monthly_performance": "table",
    "mart_carrier_monthly_performance": "table",
    "mart_route_monthly_performance": "table",
    "mart_carrier_monthly_delay_cause": "table",
}

HISTORICAL_ESTIMATES = {
    "dim_date": 112645440,
    "mart_airport_monthly_performance": 1454347488,
    "mart_carrier_monthly_performance": 2345386424,
    "mart_route_monthly_performance": 2345386424,
    "mart_carrier_monthly_delay_cause": 568078248,
}

VARS = {
    "enable_incremental_demo": True,
    "enable_2026_incremental_demo": True,
    "enable_2026_reporting": True,
    "incremental_2026_mode": "monthly",
    "reprocess_month": "2026-02",
    "historical_delay_cause_materialization": "view",
}


def bq_client():
    return bigquery.Client(project=PROJECT, location=LOCATION)


def table_metadata(client, name):
    return client.get_table(f"{PROJECT}.{DATASET}.{name}")


def verify_baselines(client):
    expected = {
        "raw_bts.flights": 14080680,
        "raw_bts.flights_2026": 1059040,
        "dbt_dev.fact_flights_2026_monthly_incremental": 1059040,
    }

    for name, rows in expected.items():
        obj = client.get_table(f"{PROJECT}.{name}")

        if obj.table_type != "TABLE" or obj.num_rows != rows:
            raise RuntimeError("Source baseline changed: " + name)

    historical = table_metadata(client, "fact_flights")

    if historical.table_type != "VIEW":
        raise RuntimeError("Historical fact view missing")

    print("SOURCE_BASELINES_PASS", flush=True)


def logical_storage_bytes(client):
    total = 0

    for dataset in ("raw_bts", "dbt_dev"):
        for reference in client.list_tables(f"{PROJECT}.{dataset}"):
            obj = client.get_table(reference.reference)

            if obj.table_type == "TABLE":
                total += int(obj.num_bytes or 0)

    return total


def verify_storage(client):
    total = logical_storage_bytes(client)

    print(
        "TOTAL_LOGICAL_STORAGE_GIB:",
        round(total / 1024**3, 5),
        flush=True,
    )

    if total >= STORAGE_STOP_BYTES:
        raise RuntimeError(
            "Storage safety threshold reached. "
            "Stop historical recovery."
        )

    print("STORAGE_SAFEGUARD_PASS", flush=True)


def load_profile():
    project = yaml.safe_load(
        (DBT_PROJECT / "dbt_project.yml").read_text(encoding="utf-8")
    )

    profile_name = project.get("profile") or project["name"]

    profiles = yaml.safe_load(
        (ORIGINAL_PROFILES / "profiles.yml").read_text(
            encoding="utf-8"
        )
    )

    if profile_name not in profiles:
        raise RuntimeError("dbt profile unavailable")

    target = profiles[profile_name]["outputs"]["dev"]

    if target.get("type") != "bigquery":
        raise RuntimeError("Unexpected adapter")

    if target.get("project") != PROJECT:
        raise RuntimeError("Unexpected BigQuery project")

    if target.get("dataset") != DATASET:
        raise RuntimeError("Unexpected BigQuery dataset")

    if int(target.get("maximum_bytes_billed") or 0) != NORMAL_CAP:
        raise RuntimeError(
            "Original 1 GB cost safeguard has changed"
        )

    return profiles, profile_name


def invoke_dbt(operation, name, profile_dir, work):
    command = [
        DBT,
        operation,
        "--project-dir", str(DBT_PROJECT),
        "--profiles-dir", str(profile_dir),
        "--target", "dev",
        "--target-path", str(work / "target"),
        "--log-path", str(work / "logs"),
        "--vars", json.dumps(VARS),
        "--select", name,
    ]

    if operation == "compile":
        command.append("--no-partial-parse")

    result = subprocess.run(
        command,
        cwd=DBT_PROJECT,
        capture_output=True,
        text=True,
        timeout=1200,
        check=False,
    )

    if result.returncode != 0:
        print(result.stdout[-5000:], flush=True)
        print(result.stderr[-5000:], flush=True)
        raise RuntimeError(
            f"dbt {operation} failed for {name}; "
            "no automatic retry permitted"
        )

    print(
        f"DBT_{operation.upper()}_PASS: {name}",
        flush=True,
    )


def recover_model(name):
    if name not in EXPECTED_MATERIALIZATIONS:
        raise RuntimeError("Unauthorized dbt model")

    client = bq_client()

    # Fail closed if a prior attempt already created this object.
    try:
        table_metadata(client, name)
    except NotFound:
        pass
    else:
        raise RuntimeError(
            "Recovery destination already exists: " + name
        )

    verify_baselines(client)
    verify_storage(client)

    profiles, profile_name = load_profile()

    with tempfile.TemporaryDirectory(
        prefix="bts_historical_recovery_"
    ) as directory:
        work = Path(directory)
        os.chmod(work, 0o700)

        temporary_profile = work / "profiles"
        temporary_profile.mkdir(mode=0o700)

        # Change only the temporary profile in this one task.
        profiles[profile_name]["outputs"]["dev"][
            "maximum_bytes_billed"
        ] = RECOVERY_CAP

        temp_file = temporary_profile / "profiles.yml"

        temp_file.write_text(
            yaml.safe_dump(profiles, sort_keys=False),
            encoding="utf-8",
        )

        os.chmod(temp_file, 0o600)

        if (
            profiles[profile_name]["outputs"]["dev"][
                "maximum_bytes_billed"
            ] != RECOVERY_CAP
        ):
            raise RuntimeError("Temporary query cap not applied")

        invoke_dbt("compile", name, temporary_profile, work)

        manifest = json.loads(
            (work / "target" / "manifest.json").read_text(
                encoding="utf-8"
            )
        )

        matching = [
            node
            for node in manifest["nodes"].values()
            if node.get("resource_type") == "model"
            and node.get("name") == name
        ]

        if len(matching) != 1:
            raise RuntimeError("Compiled model not uniquely resolved")

        model = matching[0]

        if model["config"]["materialized"] != (
            EXPECTED_MATERIALIZATIONS[name]
        ):
            raise RuntimeError("Unexpected materialization: " + name)

        if (
            model.get("database") != PROJECT
            or model.get("schema") != DATASET
        ):
            raise RuntimeError("Unexpected compiled destination")

        # Re-estimate the exact compiled SQL immediately before
        # the table is created, after upstream dependencies exist.
        if name in HISTORICAL_ESTIMATES:
            sql = model.get("compiled_code")

            if not sql:
                raise RuntimeError("Compiled SQL missing: " + name)

            dry = client.query(
                sql,
                location=LOCATION,
                job_config=bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                ),
            )

            estimate = int(dry.total_bytes_processed or 0)
            earlier = HISTORICAL_ESTIMATES[name]

            print(
                "LIVE_PREFLIGHT_ESTIMATE:",
                name,
                estimate,
                flush=True,
            )

            if estimate <= 0 or estimate > RECOVERY_CAP:
                raise RuntimeError(
                    "Historical model exceeds query cap: " + name
                )

            if estimate > int(earlier * 1.25):
                raise RuntimeError(
                    "Historical scan estimate increased >25%: " + name
                )

        invoke_dbt("run", name, temporary_profile, work)

    # Temporary profile is deleted before verification.
    obj = table_metadata(client, name)

    expected_type = (
        "VIEW"
        if EXPECTED_MATERIALIZATIONS[name] == "view"
        else "TABLE"
    )

    if obj.table_type != expected_type:
        raise RuntimeError("Unexpected resulting object type: " + name)

    if not obj.schema:
        raise RuntimeError("Resulting object has no columns: " + name)

    if obj.table_type == "VIEW":
        if not obj.view_query:
            raise RuntimeError("View SQL missing: " + name)
    else:
        if not obj.num_rows or obj.num_rows <= 0:
            raise RuntimeError("Restored table is empty: " + name)

        if name == "dim_date" and obj.num_rows != 731:
            raise RuntimeError("Historical calendar count mismatch")

    print(
        "RECOVERY_OBJECT_VERIFIED:",
        name,
        "| type:", obj.table_type,
        "| rows:", obj.num_rows,
        flush=True,
    )

    verify_storage(client)
    verify_baselines(client)


@dag(
    dag_id="bts_historical_reporting_recovery_once",
    description="Guarded one-time recovery of six historical dbt relations",
    start_date=datetime(2026, 10, 29, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["bts", "recovery", "bigquery", "dbt"],
)
def bts_historical_reporting_recovery_once():

    @task(retries=0)
    def authorize_recovery():
        conf = get_current_context()["dag_run"].conf or {}

        expected = {
            "release_scope": "historical_reporting_once",
            "authorize_historical_recovery": True,
        }

        if conf != expected:
            raise RuntimeError(
                "Exact historical recovery authorization required"
            )

        print("HISTORICAL_RECOVERY_AUTHORIZED", flush=True)

    @task(retries=0)
    def validate_baseline():
        client = bq_client()

        verify_baselines(client)
        verify_storage(client)
        load_profile()

        for name in MODELS:
            try:
                table_metadata(client, name)
            except NotFound:
                print("RECOVERY_TARGET_AVAILABLE:", name, flush=True)
            else:
                raise RuntimeError(
                    "Existing object prevents recovery: " + name
                )

        print("SIX_RECOVERY_TARGETS_AVAILABLE", flush=True)

    @task(retries=0)
    def restore_delay_cause_view():
        recover_model("fact_flight_delay_cause")

    @task(retries=0)
    def restore_dim_date():
        recover_model("dim_date")

    @task(retries=0)
    def restore_airport_mart():
        recover_model("mart_airport_monthly_performance")

    @task(retries=0)
    def restore_carrier_mart():
        recover_model("mart_carrier_monthly_performance")

    @task(retries=0)
    def restore_route_mart():
        recover_model("mart_route_monthly_performance")

    @task(retries=0)
    def restore_delay_cause_mart():
        recover_model("mart_carrier_monthly_delay_cause")

    @task(retries=0)
    def verify_recovery():
        client = bq_client()

        for name, materialization in EXPECTED_MATERIALIZATIONS.items():
            obj = table_metadata(client, name)

            expected_type = (
                "VIEW" if materialization == "view" else "TABLE"
            )

            if obj.table_type != expected_type:
                raise RuntimeError("Final object type mismatch: " + name)

            if not obj.schema:
                raise RuntimeError("Missing final schema: " + name)

            print("FINAL_RECOVERY_PASS:", name, flush=True)

        verify_baselines(client)
        verify_storage(client)

        print("SIX_HISTORICAL_OBJECTS_RECOVERED", flush=True)
        print("FEBRUARY_MERGE_NOT_EXECUTED", flush=True)
        print("RECOVERY_DAG_SUCCESS", flush=True)

    a = authorize_recovery()
    b = validate_baseline()
    c = restore_delay_cause_view()
    d = restore_dim_date()
    e = restore_airport_mart()
    f = restore_carrier_mart()
    g = restore_route_mart()
    h = restore_delay_cause_mart()
    i = verify_recovery()

    a >> b >> c >> d >> e >> f >> g >> h >> i


dag = bts_historical_reporting_recovery_once()