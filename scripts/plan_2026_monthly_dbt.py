"""Build month-aware dbt commands without executing them.

This module only plans commands. It does not authorize BigQuery writes,
invoke dbt, or establish readiness for a monthly MERGE.
"""

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/bts/dbt"
DBT_PROFILES = "/opt/bts/dbt-profile"

MODEL = (
    ROOT / "dbt/models/incremental_demo/"
    "fact_flights_2026_monthly_incremental.sql"
)

SELECTORS = {
    "upstream": "+fact_flights_2026",
    "merge": "fact_flights_2026_monthly_incremental",
}


def validate_month(month, fixture=False):
    if not isinstance(month, str):
        raise ValueError("Reporting month must be a string")

    if not re.fullmatch(r"2026-(0[1-9]|1[0-2])", month):
        raise ValueError("Unsupported reporting period")

    if fixture:
        if month != "2026-02":
            raise ValueError("Fixture is restricted to February")
    elif month <= "2026-02":
        raise ValueError("Historical reprocessing is prohibited")

    return month


def validate_model_contract():
    if not MODEL.is_file():
        raise RuntimeError("Existing incremental dbt model missing")

    sql = MODEL.read_text(encoding="utf-8-sig")

    required = [
        "incremental_strategy='merge'",
        "unique_key='flight_key'",
        "on_schema_change='fail'",
        "incremental_predicates=target_predicates",
        "flags.FULL_REFRESH",
        "incremental_2026_mode",
        "reprocess_month",
    ]

    for item in required:
        if item not in sql:
            raise RuntimeError("Incremental model contract changed: " + item)

    return True


def variables_for_month(month):
    return {
        "enable_incremental_demo": True,
        "enable_2026_incremental_demo": True,
        "incremental_2026_mode": "monthly",
        "reprocess_month": month,
    }


def build_command(month, stage, fixture=False):
    validate_month(month, fixture=fixture)

    if stage not in SELECTORS:
        raise ValueError("Unrecognized dbt stage")

    label = stage + "_" + month.replace("-", "_")
    base = "/opt/bts/evidence/dbt/monthly_runs/" + label

    command = [
        DBT_EXECUTABLE,
        "run",
        "--project-dir", DBT_PROJECT,
        "--profiles-dir", DBT_PROFILES,
        "--target", "dev",
        "--target-path", base + "/target",
        "--log-path", base + "/logs",
        "--vars", json.dumps(
            variables_for_month(month),
            sort_keys=True,
        ),
        "--select", SELECTORS[stage],
    ]

    if "--full-refresh" in command:
        raise RuntimeError("Full refresh is prohibited")

    return command


def plan_month(month, fixture=False):
    validate_month(month, fixture=fixture)
    validate_model_contract()

    return {
        "month": month,
        "fixture": fixture,
        "upstream": build_command(
            month, "upstream", fixture=fixture
        ),
        "merge": build_command(
            month, "merge", fixture=fixture
        ),
        "commands_executed": False,
        "readiness_authorized": False,
    }


def self_test():
    assert validate_model_contract()
    print("EXISTING_DBT_MODEL_CONTRACT_PASS")

    for month in ("2026-03", "2026-04", "2026-12"):
        plan = plan_month(month)

        for stage in ("upstream", "merge"):
            command = plan[stage]

            assert command[0] == DBT_EXECUTABLE
            assert command[1] == "run"
            assert "--full-refresh" not in command
            assert command[command.index("--select") + 1] == (
                SELECTORS[stage]
            )

            variables = json.loads(
                command[command.index("--vars") + 1]
            )

            assert variables["reprocess_month"] == month
            assert variables["incremental_2026_mode"] == "monthly"
            assert variables["enable_2026_incremental_demo"] is True

        assert plan["commands_executed"] is False
        assert plan["readiness_authorized"] is False

    print("DYNAMIC_MARCH_APRIL_DECEMBER_COMMANDS_PASS")

    blocked = (
        "2026-01",
        "2026-02",
        "2026-13",
        "2027-01",
        "2026-3",
    )

    for month in blocked:
        try:
            plan_month(month)
        except ValueError:
            continue

        raise RuntimeError("Unsafe month accepted: " + month)

    print("HISTORICAL_AND_INVALID_MONTH_BLOCK_PASS")

    fixture = plan_month("2026-02", fixture=True)

    assert fixture["fixture"] is True
    assert fixture["commands_executed"] is False

    print("FEBRUARY_REFERENCE_FIXTURE_PASS")

    for stage in ("full_refresh", "reporting", "bootstrap"):
        try:
            build_command("2026-03", stage)
        except ValueError:
            continue

        raise RuntimeError("Unsupported dbt stage accepted")

    print("UNAPPROVED_DBT_STAGES_BLOCK_PASS")
    print("NO_SUBPROCESS_EXECUTION")
    print("NO_BIGQUERY_API_CALLS")
    print("NO_DBT_COMMAND_EXECUTED")
    print("B3_51CY_OFFLINE_TEST_PASS")


def main():
    parser = argparse.ArgumentParser(
        description="Read-only BTS monthly dbt command planner"
    )

    options = parser.add_mutually_exclusive_group(required=True)

    options.add_argument("--self-test", action="store_true")
    options.add_argument(
        "--fixture-february",
        action="store_true",
    )
    options.add_argument("--plan", metavar="YYYY-MM")

    args = parser.parse_args()

    if args.self_test:
        self_test()
        return

    if args.fixture_february:
        plan = plan_month("2026-02", fixture=True)
    else:
        plan = plan_month(args.plan)

    print("PLANNED_MONTH:", plan["month"])
    print("FIXTURE_MODE:", plan["fixture"])

    for stage in ("upstream", "merge"):
        command = plan[stage]
        print(
            "PLANNED_DBT_STAGE:",
            stage,
            command[command.index("--select") + 1],
        )

        variables = json.loads(
            command[command.index("--vars") + 1]
        )
        print("DBT_VARS:", json.dumps(variables, sort_keys=True))

    print("NO_DBT_EXECUTION")
    print("NO_BIGQUERY_WRITE")
    print("READINESS_AUTHORIZATION_REQUIRED")
    print("MONTHLY_DBT_PLAN_PASS")


if __name__ == "__main__":
    main()
