"""Select the next consecutive 2026 month for BTS ingestion.

Runs on the Windows host. Does not download source data.
By default, invokes the existing loader in local preflight mode.
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROJECT = "tdb-dbt-bigquery-portfolio"
FIRST_NEW_MONTH = 3


def read_json(path):
    if not path.is_file():
        raise RuntimeError(f"Expected regular file: {path}")

    if path.stat().st_size > 32768:
        raise RuntimeError(f"Oversized receipt: {path}")

    result = json.loads(path.read_text(encoding="utf-8-sig"))

    if not isinstance(result, dict):
        raise RuntimeError("Receipt must contain a JSON object")

    return result


def select_next_month(root):
    ingestion_dir = (
        root / "evidence/bigquery/monthly_ingestion"
    )
    completion_dir = (
        root / "evidence/bigquery/monthly_processing"
    )
    manifest_dir = root / "evidence/source"
    load_dir = root / "data/load/2026"

    for number in range(FIRST_NEW_MONTH, 13):
        period = f"2026-{number:02d}"
        tag = f"2026_{number:02d}"

        receipt_path = ingestion_dir / f"{period}.json"
        completion_path = completion_dir / f"{period}.json"

        if completion_path.exists() and not receipt_path.exists():
            raise RuntimeError(
                f"Completion without ingestion receipt: {period}"
            )

        if receipt_path.exists():
            receipt = read_json(receipt_path)

            if (
                receipt.get("month") != period
                or receipt.get("project") != PROJECT
                or receipt.get("destination")
                != "raw_bts.flights_2026"
                or receipt.get("job_status") != "DONE"
                or type(receipt.get("bad_records")) is not int
                or receipt["bad_records"] != 0
                or type(receipt.get("imported_rows")) is not int
                or receipt["imported_rows"] <= 0
                or not isinstance(receipt.get("job_id"), str)
                or not receipt["job_id"].strip()
                or not isinstance(receipt.get("source_sha256"), str)
                or not re.fullmatch(
                    r"[0-9a-fA-F]{64}",
                    receipt["source_sha256"]
                )
            ):
                raise RuntimeError(
                    f"Invalid existing ingestion receipt: {period}"
                )

            if not completion_path.exists():
                return {
                    "status": "WAITING_FOR_PROCESSING",
                    "month": period,
                }

            completed = read_json(completion_path)

            if (
                completed.get("month") != period
                or completed.get("status") != "DONE"
                or not isinstance(
                    completed.get("dbt_invocation_id"), str
                )
                or not completed["dbt_invocation_id"].strip()
            ):
                raise RuntimeError(
                    f"Invalid completion receipt: {period}"
                )

            continue

        manifest = manifest_dir / f"{tag}_bq_load_manifest.csv"
        gzip_path = load_dir / f"bts_{tag}_bq.csv.gz"

        if manifest.is_file() and gzip_path.is_file():
            return {
                "status": "READY_FOR_LOCAL_PREFLIGHT",
                "month": period,
            }

        return {
            "status": "WAITING_FOR_PREPARED_SOURCE",
            "month": period,
            "manifest_present": manifest.is_file(),
            "gzip_present": gzip_path.is_file(),
        }

    return {
        "status": "ALL_2026_MONTHS_COMPLETED",
        "month": None,
    }


def run_self_tests():
    def write_json(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)

        def select():
            return select_next_month(root)

        if select()["status"] != "WAITING_FOR_PREPARED_SOURCE":
            raise RuntimeError("Missing March source was accepted")

        print("MISSING_MARCH_SOURCE_WAIT_PASS", flush=True)

        def prepare(month):
            tag = month.replace("-", "_")
            manifest = (
                root / "evidence/source"
                / f"{tag}_bq_load_manifest.csv"
            )
            gzip_path = (
                root / "data/load/2026"
                / f"bts_{tag}_bq.csv.gz"
            )

            manifest.parent.mkdir(parents=True, exist_ok=True)
            gzip_path.parent.mkdir(parents=True, exist_ok=True)

            # Placeholder files test selection only.
            # They are never passed to the BigQuery loader.
            manifest.write_text("synthetic-test")
            gzip_path.write_bytes(b"synthetic-test")

        prepare("2026-04")

        if select()["month"] != "2026-03":
            raise RuntimeError("Skipped missing March")

        print("NO_MONTH_SKIPPING_PASS", flush=True)

        prepare("2026-03")

        choice = select()

        if (
            choice["month"] != "2026-03"
            or choice["status"] != "READY_FOR_LOCAL_PREFLIGHT"
        ):
            raise RuntimeError("March not selected")

        print("AUTOMATIC_MARCH_SELECTION_PASS", flush=True)

        ingestion = (
            root / "evidence/bigquery/monthly_ingestion/2026-03.json"
        )

        valid_receipt = {
            "month": "2026-03",
            "project": PROJECT,
            "destination": "raw_bts.flights_2026",
            "job_status": "DONE",
            "bad_records": 0,
            "imported_rows": 100,
            "job_id": "SYNTHETIC-ONLY",
            "source_sha256": "a" * 64,
        }

        write_json(ingestion, valid_receipt)

        choice = select()

        if choice != {
            "status": "WAITING_FOR_PROCESSING",
            "month": "2026-03",
        }:
            raise RuntimeError("Advanced before March processing")

        print("PROCESSING_DEPENDENCY_BLOCK_PASS", flush=True)

        completion = (
            root / "evidence/bigquery/monthly_processing/2026-03.json"
        )

        write_json(completion, {
            "month": "2026-03",
            "status": "DONE",
            "dbt_invocation_id": "SYNTHETIC-ONLY",
        })

        choice = select()

        if (
            choice["month"] != "2026-04"
            or choice["status"] != "READY_FOR_LOCAL_PREFLIGHT"
        ):
            raise RuntimeError("Failed to advance to April")

        print("AUTOMATIC_APRIL_ADVANCEMENT_PASS", flush=True)

        invalid = dict(valid_receipt)
        invalid["job_status"] = "FAILED"
        write_json(ingestion, invalid)

        try:
            select()
        except RuntimeError:
            print("INVALID_RECEIPT_BLOCK_PASS", flush=True)
        else:
            raise RuntimeError("Invalid receipt accepted")

    print("SYNTHETIC_TEST_FILES_TEMPORARY_ONLY")
    print("NO_REAL_INGESTION_RECEIPTS_CREATED")
    print("NO_BIGQUERY_API_CALLS")
    print("NO_BIGQUERY_LOAD_JOB")
    print("B3_51CH_SELF_TEST_PASS")


def monthly_action_plan(root, period):
    """Build a reusable sequence without executing any commands."""
    if not isinstance(period, str):
        raise ValueError("Invalid monthly period")

    if not re.fullmatch(r"2026-(0[3-9]|1[0-2])", period):
        raise ValueError("Only new 2026 months may be prepared")

    month = int(period[-2:])
    scripts = root / "scripts"

    return [
        (
            "ACQUIRE_SOURCE",
            [
                sys.executable,
                str(scripts / "fetch_2026_bts_source.py"),
                "--fetch",
            ],
        ),
        (
            "PROCESS_52_COLUMNS",
            [
                sys.executable,
                str(scripts / "project_batch.py"),
                "--year", "2026",
                "--month", str(month),
            ],
        ),
        (
            "BUILD_51_COLUMNS",
            [
                sys.executable,
                str(scripts / "build_2026_bq_monthly.py"),
                "--year", "2026",
                "--month", str(month),
            ],
        ),
        (
            "VALIDATE_LOAD_FILE",
            [
                sys.executable,
                str(scripts / "load_2026_bq_monthly.py"),
                "--year", "2026",
                "--month", str(month),
            ],
        ),
    ]


def run_monthly_preparation(root, selection):
    """Prepare one new month; fail closed on partial outputs."""
    period = selection["month"]
    status = selection["status"]

    if status not in (
        "WAITING_FOR_PREPARED_SOURCE",
        "READY_FOR_LOCAL_PREFLIGHT",
    ):
        print("NO_PREPARATION_ALLOWED_FOR_STATUS:", status)
        return

    plan = monthly_action_plan(root, period)

    if status == "READY_FOR_LOCAL_PREFLIGHT":
        print("PREPARED_OUTPUT_ALREADY_PRESENT")
        print("VALIDATING_EXISTING_OUTPUT_ONLY", flush=True)
        subprocess.run(plan[-1][1], check=True)
        return

    tag = period.replace("-", "_")

    protected_outputs = [
        root / "data/processed/2026"
        / f"bts_{tag}_core.csv.gz",
        root / "evidence/source"
        / f"{tag}_processed_manifest.csv",
        root / "data/load/2026"
        / f"bts_{tag}_bq.csv.gz",
        root / "evidence/source"
        / f"{tag}_bq_load_manifest.csv",
    ]

    existing = [
        str(path.relative_to(root))
        for path in protected_outputs
        if path.exists()
    ]

    if existing:
        raise RuntimeError(
            "Partial monthly outputs require manual recovery: "
            + ", ".join(existing)
        )

    for label, command in plan:
        print("START_STAGE:", label, flush=True)

        script = Path(command[1])

        if not script.is_file():
            raise RuntimeError(
                "Required processing script missing: " + script.name
            )

        subprocess.run(command, check=True)

        if label == "ACQUIRE_SOURCE":
            source_manifest = (
                root / "evidence/source/"
                "2026_monthly_source_manifest.csv"
            )

            with source_manifest.open(
                "r", encoding="utf-8-sig", newline=""
            ) as handle:
                import csv
                records = list(csv.DictReader(handle))

            matching = [
                row for row in records
                if row.get("reporting_year") == "2026"
                and row.get("reporting_month") == str(
                    int(period[-2:])
                )
            ]

            if len(matching) != 1:
                raise RuntimeError(
                    "Official source not registered for " + period
                )

        print("STAGE_COMPLETED:", label, flush=True)

    print("MONTHLY_PREPARATION_PASS:", period)
    print("BIGQUERY_LOAD_NOT_EXECUTED")


def main():
    parser = argparse.ArgumentParser(
        description="Select and prepare next eligible BTS month"
    )

    options = parser.add_mutually_exclusive_group()

    options.add_argument(
        "--self-test",
        action="store_true",
        help="Synthetic, offline month-selection tests",
    )

    options.add_argument(
        "--fixture-february",
        action="store_true",
        help="Read-only validation of existing February artifacts",
    )

    options.add_argument(
        "--prepare",
        action="store_true",
        help="Fetch, process, build and locally validate next month",
    )

    options.add_argument(
        "--execute",
        action="store_true",
        help="Explicitly authorize existing BigQuery loader",
    )

    args = parser.parse_args()

    if args.self_test:
        run_self_tests()

        plan = monthly_action_plan(ROOT, "2026-03")

        if [label for label, _ in plan] != [
            "ACQUIRE_SOURCE",
            "PROCESS_52_COLUMNS",
            "BUILD_51_COLUMNS",
            "VALIDATE_LOAD_FILE",
        ]:
            raise RuntimeError("Unexpected monthly processing stages")

        if any("--execute" in command for _, command in plan):
            raise RuntimeError("Load authorization leaked into preparation")

        if not (
            plan[1][1][-4:] == [
                "--year", "2026", "--month", "3"
            ]
            and plan[2][1][-4:] == [
                "--year", "2026", "--month", "3"
            ]
        ):
            raise RuntimeError("Dynamic month forwarding failed")

        try:
            monthly_action_plan(ROOT, "2026-02")
        except ValueError:
            print("FEBRUARY_REPROCESSING_BLOCK_PASS", flush=True)
        else:
            raise RuntimeError("Historical month accepted")

        print("FOUR_STAGE_PREPARATION_PLAN_PASS")
        print("DYNAMIC_MONTH_FORWARDING_PASS")
        print("NO_BIGQUERY_LOAD_AUTHORIZATION_PASS")
        print("B3_51CL_OFFLINE_TEST_PASS")
        return

    if args.fixture_february:
        files = [
            ROOT / "data/processed/2026/bts_2026_02_core.csv.gz",
            ROOT / "evidence/source/2026_02_processed_manifest.csv",
            ROOT / "data/load/2026/bts_2026_02_bq.csv.gz",
            ROOT / "evidence/source/2026_02_bq_load_manifest.csv",
        ]

        if not all(path.is_file() for path in files):
            raise RuntimeError("February reference artifacts missing")

        print("FEBRUARY_REFERENCE_ARTIFACTS_PASS")
        print("RUNNING_EXISTING_LOADER_LOCAL_PREFLIGHT")

        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/load_2026_bq_monthly.py"),
                "--year", "2026", "--month", "2",
            ],
            check=True,
        )

        print("FEBRUARY_FIXTURE_VALIDATION_PASS")
        print("NO_TRANSFORMATION_EXECUTED")
        print("NO_BIGQUERY_UPLOAD")
        return

    choice = select_next_month(ROOT)

    print("SELECTION_STATUS:", choice["status"], flush=True)
    print("NEXT_MONTH:", choice["month"], flush=True)

    if args.prepare:
        run_monthly_preparation(ROOT, choice)
        return

    if choice["status"] != "READY_FOR_LOCAL_PREFLIGHT":
        print("NO_LOAD_ATTEMPTED")

        if args.execute:
            raise RuntimeError(
                "Execution blocked: prepared source not ready"
            )

        if choice["status"] == "WAITING_FOR_PREPARED_SOURCE":
            print("NEXT_ACTION: LOCAL_PREPARATION")
        return

    period = choice["month"]
    month = int(period[-2:])

    command = [
        sys.executable,
        str(ROOT / "scripts/load_2026_bq_monthly.py"),
        "--year", "2026",
        "--month", str(month),
    ]

    if args.execute:
        command.append("--execute")

    print(
        "LOADER_MODE:",
        "EXECUTE" if args.execute else "LOCAL_PREFLIGHT",
        flush=True,
    )

    subprocess.run(command, check=True)

    if not args.execute:
        print("B3_51CL_HOST_PREFLIGHT_PASS")


if __name__ == "__main__":
    main()
