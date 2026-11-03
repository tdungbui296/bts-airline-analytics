"""Validate the complete 2026 raw BigQuery monthly row distribution.

Offline self-tests require no Google Cloud access.
Real verification requires explicit --verify and a reporting month.
No data modification, uploads or dbt execution.
"""

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "tdb-dbt-bigquery-portfolio"
LOCATION = "asia-southeast1"
TABLE_ID = PROJECT + ".raw_bts.flights_2026"
MAX_BYTES_BILLED = 100_000_000

BASELINE = {
    "2026-01": 544003,
    "2026-02": 515037,
}


def validate_period(month):
    if not isinstance(month, str):
        raise ValueError("Reporting month must be a string")

    if not re.fullmatch(r"2026-(0[2-9]|1[0-2])", month):
        raise ValueError("Unsupported reporting month")

    return month


def read_ingestion_receipt(root, month):
    if month == "2026-02":
        path = (
            root / "evidence/bigquery"
            / "B3_38_2026_02_load_receipt.json"
        )
    else:
        path = (
            root / "evidence/bigquery/monthly_ingestion"
            / (month + ".json")
        )

    if not path.is_file() or path.stat().st_size > 65536:
        raise ValueError("Missing or oversized ingestion receipt")

    with path.open("r", encoding="utf-8-sig") as handle:
        receipt = json.load(handle)

    if not isinstance(receipt, dict):
        raise ValueError("Invalid ingestion receipt")

    required = {
        "month": month,
        "project": PROJECT,
        "region": LOCATION,
        "destination": "raw_bts.flights_2026",
        "job_status": "DONE",
        "bad_records": 0,
    }

    for field, expected in required.items():
        if receipt.get(field) != expected:
            raise ValueError("Ingestion receipt mismatch: " + field)

    for field in ("imported_rows", "verified_total_rows"):
        value = receipt.get(field)

        if type(value) is not int or value <= 0:
            raise ValueError("Invalid receipt row count: " + field)

    return receipt


def expected_month_counts(root, month):
    validate_period(month)
    expected = dict(BASELINE)

    if month == "2026-02":
        receipt = read_ingestion_receipt(root, month)

        if receipt["imported_rows"] != BASELINE[month]:
            raise ValueError("February baseline receipt mismatch")
    else:
        for number in range(3, int(month[-2:]) + 1):
            period = f"2026-{number:02d}"
            receipt = read_ingestion_receipt(root, period)
            expected[period] = receipt["imported_rows"]

    if sum(expected.values()) != receipt["verified_total_rows"]:
        raise ValueError("Cumulative receipt row count mismatch")

    return expected


def select_date_field(schema):
    accepted = {"flightdate", "flight_date", "fl_date"}

    matches = [
        field for field in schema
        if field.name.lower() in accepted
        and field.field_type.upper() in (
            "DATE", "STRING", "DATETIME", "TIMESTAMP"
        )
    ]

    if len(matches) != 1:
        raise ValueError(
            "Exactly one recognized raw flight-date column required"
        )

    return matches[0].name


def build_raw_month_sql(date_field):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", date_field):
        raise ValueError("Unsafe column identifier")

    if date_field.lower() not in (
        "flightdate", "flight_date", "fl_date"
    ):
        raise ValueError("Unrecognized flight-date column")

    return f"""
SELECT
    FORMAT_DATE(
        '%Y-%m',
        SAFE_CAST(`{date_field}` AS DATE)
    ) AS reporting_month,
    COUNT(*) AS flight_rows
FROM `{TABLE_ID}`
GROUP BY reporting_month
ORDER BY reporting_month
"""


def reconcile_counts(expected, actual, total_rows):
    if expected != actual:
        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))

        raise ValueError(
            "Raw month distribution mismatch; "
            f"missing={missing}; unexpected={unexpected}; "
            f"expected={expected}; actual={actual}"
        )

    if sum(actual.values()) != total_rows:
        raise ValueError("Raw cumulative row count mismatch")

    return True


def verify_raw_months(month):
    from google.cloud import bigquery

    expected = expected_month_counts(ROOT, month)

    client = bigquery.Client(
        project=PROJECT,
        location=LOCATION,
    )

    table = client.get_table(TABLE_ID)

    if table.table_type != "TABLE":
        raise ValueError("Expected an ordinary raw BigQuery table")

    date_field = select_date_field(table.schema)
    sql = build_raw_month_sql(date_field)

    dry_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        use_legacy_sql=False,
        maximum_bytes_billed=MAX_BYTES_BILLED,
    )

    dry_job = client.query(
        sql,
        job_config=dry_config,
        location=LOCATION,
    )

    estimated = dry_job.total_bytes_processed

    if (
        type(estimated) is not int
        or estimated <= 0
        or estimated > MAX_BYTES_BILLED
    ):
        raise RuntimeError(
            "Raw reconciliation exceeds the approved scan limit"
        )

    print("RAW_MONTH_DRY_RUN_BYTES:", estimated, flush=True)
    print("RAW_MONTH_DRY_RUN_PASS", flush=True)

    query_config = bigquery.QueryJobConfig(
        use_query_cache=False,
        use_legacy_sql=False,
        maximum_bytes_billed=MAX_BYTES_BILLED,
    )

    job = client.query(
        sql,
        job_config=query_config,
        location=LOCATION,
    )

    rows = list(job.result(timeout=300))

    actual = {}

    for row in rows:
        month_key = row.reporting_month
        count = int(row.flight_rows)

        if month_key is None or month_key in actual or count <= 0:
            raise ValueError(
                "Invalid or duplicated raw reporting-month group"
            )

        actual[month_key] = count

    reconcile_counts(expected, actual, table.num_rows)

    print("RAW_MONTH_COUNTS:", json.dumps(actual, sort_keys=True))
    print("RAW_TABLE_TOTAL_ROWS:", table.num_rows)
    print("RAW_RECONCILIATION_JOB_ID:", job.job_id)
    print("RAW_MONTH_RECONCILIATION_PASS:", month)

    return actual


def self_test():
    assert validate_period("2026-02") == "2026-02"
    assert validate_period("2026-03") == "2026-03"
    assert validate_period("2026-12") == "2026-12"

    for invalid in ("2026-01", "2027-01", "2026-13", "2026-3"):
        try:
            validate_period(invalid)
        except ValueError:
            continue

        raise RuntimeError("Unsupported period accepted")

    print("MONTH_BOUNDARY_GUARDS_PASS")

    expected = {
        "2026-01": 544003,
        "2026-02": 515037,
        "2026-03": 612102,
    }

    assert reconcile_counts(
        expected, dict(expected), 1671142
    )

    print("THREE_MONTH_RECONCILIATION_PASS")

    invalid_distributions = [
        (
            {"2026-01": 544003, "2026-02": 515037},
            1059040,
        ),
        (
            {
                "2026-01": 544003,
                "2026-02": 515038,
                "2026-03": 612102,
            },
            1671143,
        ),
        (
            {
                **expected,
                "2026-04": 1,
            },
            1671143,
        ),
        (
            dict(expected),
            1671143,
        ),
    ]

    for actual, total in invalid_distributions:
        try:
            reconcile_counts(expected, actual, total)
        except ValueError:
            continue

        raise RuntimeError("Invalid raw distribution accepted")

    print("DUPLICATE_MISSING_EXTRA_ROWS_BLOCK_PASS")

    class Field:
        def __init__(self, name, field_type):
            self.name = name
            self.field_type = field_type

    assert select_date_field([
        Field("FlightDate", "DATE")
    ]) == "FlightDate"

    assert "SAFE_CAST(`FlightDate` AS DATE)" in (
        build_raw_month_sql("FlightDate")
    )

    for invalid in ("FlightDate;DROP", "OtherColumn"):
        try:
            build_raw_month_sql(invalid)
        except ValueError:
            continue

        raise RuntimeError("Unsafe SQL field accepted")

    print("SCHEMA_AND_SQL_IDENTIFIER_GUARDS_PASS")
    print("NO_BIGQUERY_CLIENT_CREATED")
    print("NO_BIGQUERY_API_CALLS")
    print("B3_51CU_OFFLINE_TEST_PASS")


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument("--self-test", action="store_true")
    group.add_argument("--verify", action="store_true")

    parser.add_argument("--month")

    args = parser.parse_args()

    if args.self_test:
        if args.month is not None:
            parser.error("--month is not used for offline tests")

        self_test()
        return

    if args.month is None:
        parser.error("--verify requires --month")

    verify_raw_months(validate_period(args.month))


if __name__ == "__main__":
    main()
