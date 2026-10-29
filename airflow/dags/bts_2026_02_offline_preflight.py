import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task

EVIDENCE = Path("/opt/bts/evidence")


@dag(
    dag_id="bts_2026_02_offline_preflight",
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bts", "offline", "validation"],
    description="Read-only BTS February 2026 evidence preflight",
)
def bts_2026_02_offline_preflight():

    @task(multiple_outputs=False)
    def validate_monthly_manifests():
        validated = []

        for month in ("01", "02"):
            path = EVIDENCE / "source" / (
                f"2026_{month}_bq_load_manifest.csv"
            )

            with path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                if not reader.fieldnames or not rows:
                    raise ValueError(f"Empty manifest: {month}")

                if any(row.get(None) is not None for row in rows):
                    raise ValueError(f"Malformed CSV manifest: {month}")

            validated.append(f"2026-{month}")

        print("MANIFEST_VALIDATION_PASS")
        return validated

    @task(multiple_outputs=False)
    def validate_raw_load_receipts():
        expected = {
            "01": 544003,
            "02": 515037,
        }

        for month, row_count in expected.items():
            filename = (
                f"B3_37_2026_01_load_receipt.json"
                if month == "01"
                else f"B3_38_2026_02_load_receipt.json"
            )

            path = EVIDENCE / "bigquery" / filename

            with path.open("r", encoding="utf-8") as f:
                receipt = json.load(f)

            if receipt.get("job_status") != "DONE":
                raise ValueError(f"Load not DONE: {month}")

            if int(receipt.get("imported_rows", -1)) != row_count:
                raise ValueError(f"Imported row mismatch: {month}")

            if int(receipt.get("bad_records", -1)) != 0:
                raise ValueError(f"Bad records found: {month}")

            if receipt.get("destination") != "raw_bts.flights_2026":
                raise ValueError(f"Unexpected destination: {month}")

        february_path = (
            EVIDENCE / "bigquery" /
            "B3_38_2026_02_load_receipt.json"
        )

        with february_path.open("r", encoding="utf-8") as f:
            february = json.load(f)

        if february.get("month") != "2026-02":
            raise ValueError("February reporting period mismatch")

        if int(february.get("verified_total_rows", -1)) != 1059040:
            raise ValueError("2026 raw landing total mismatch")

        if int(february.get("historical_raw_rows_unchanged", -1)) != 14080680:
            raise ValueError("Historical baseline mismatch")

        expiry = int(february.get("table_expiration_ms") or 0)
        current_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        if expiry <= current_ms:
            raise ValueError("Recorded raw table expiration has passed")

        print("RAW_LOAD_RECEIPTS_PASS")

        return {
            "january_rows": 544003,
            "february_rows": 515037,
            "raw_total_rows": 1059040,
        }

    @task()
    def reconcile_offline_evidence(months, counts):
        if months != ["2026-01", "2026-02"]:
            raise ValueError("Unexpected monthly manifest coverage")

        calculated = (
            counts["january_rows"] + counts["february_rows"]
        )

        if calculated != counts["raw_total_rows"]:
            raise ValueError("Monthly flight totals do not reconcile")

        january_merge = (
            EVIDENCE / "bigquery" /
            "B3_47B_january_merge_receipt.json"
        )

        with january_merge.open("r", encoding="utf-8") as f:
            receipt = json.load(f)

        if not isinstance(receipt, dict) or not receipt:
            raise ValueError("Invalid January MERGE evidence document")

        print("OFFLINE_RECONCILIATION_PASS")
        print(f"2026_RAW_ROWS={calculated}")
        print("CLOUD_QUERIES_EXECUTED=0")
        print("DBT_MERGE_EXECUTED=0")
        print("This run validates stored evidence, not live BigQuery state.")

    manifests = validate_monthly_manifests()
    receipts = validate_raw_load_receipts()

    reconcile_offline_evidence(manifests, receipts)


bts_2026_02_offline_preflight()