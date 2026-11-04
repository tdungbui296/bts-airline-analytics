import json
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task, get_current_context

RECEIPT = Path(
    "/opt/bts/evidence/bigquery/B3_38_2026_02_load_receipt.json"
)


@dag(
    dag_id="bts_2026_02_failure_drill",
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bts", "offline", "test-only"],
    description="Safe offline failure and recovery demonstration",
)
def bts_2026_02_failure_drill():

    @task(retries=0)
    def read_february_receipt():
        with RECEIPT.open("r", encoding="utf-8") as f:
            receipt = json.load(f)

        if receipt.get("month") != "2026-02":
            raise ValueError("Incorrect reporting month")

        if receipt.get("job_status") != "DONE":
            raise ValueError("Load receipt is not completed")

        if receipt.get("destination") != "raw_bts.flights_2026":
            raise ValueError("Incorrect raw destination")

        if int(receipt.get("bad_records", -1)) != 0:
            raise ValueError("Bad records detected")

        rows = int(receipt["imported_rows"])

        print(f"FEBRUARY_RECEIPT_ROWS={rows}")
        return rows

    @task(retries=0)
    def validate_expected_rows(actual_rows):
        context = get_current_context()
        config = context["dag_run"].conf or {}

        inject = config.get("inject_bad_expected_count", False)

        if type(inject) is not bool:
            raise ValueError("Failure drill parameter must be boolean")

        expected_rows = 515037 + (1 if inject else 0)

        if actual_rows != expected_rows:
            raise ValueError(
                "QUALITY_GATE_FAILED: "
                f"actual={actual_rows}, expected={expected_rows}"
            )

        print("QUALITY_GATE_PASS")
        return actual_rows

    @task(retries=0)
    def downstream_quality_release(rows):
        print(f"DOWNSTREAM_RELEASE_PASS: {rows}")
        print("No cloud queries or mutations were executed.")

    downstream_quality_release(
        validate_expected_rows(read_february_receipt())
    )


bts_2026_02_failure_drill()