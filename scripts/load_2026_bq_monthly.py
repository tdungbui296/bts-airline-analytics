"""Safely append one prepared BTS 2026 month to BigQuery.

Default mode performs local validation only.
--execute is a separate, explicitly authorized operation.
"""

import argparse
import csv
import gzip
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROJECT = "tdb-dbt-bigquery-portfolio"
LOCATION = "asia-southeast1"
DESTINATION = f"{PROJECT}.raw_bts.flights_2026"

MANIFEST_FIELDS = [
    "reporting_year",
    "reporting_month",
    "input_file",
    "input_sha256",
    "expected_row_count",
    "output_file",
    "output_row_count",
    "row_count_reconciled",
    "output_column_count",
    "output_size_bytes",
    "output_size_mb",
    "output_sha256",
]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def positive_int(value, field):
    text = str(value).strip()

    require(
        bool(re.fullmatch(r"[0-9]+", text)),
        f"Invalid integer: {field}",
    )

    number = int(text)
    require(number > 0, f"Nonpositive value: {field}")
    return number


def validate_prepared_month(year, month):
    require(year == 2026, "Only the 2026 source contract is supported")
    require(1 <= month <= 12, "Invalid month")

    period = f"{year}-{month:02d}"
    tag = f"{year}_{month:02d}"

    manifest_path = (
        ROOT / "evidence" / "source"
        / f"{tag}_bq_load_manifest.csv"
    )
    source_path = (
        ROOT / "data" / "load" / "2026"
        / f"bts_{tag}_bq.csv.gz"
    )

    require(manifest_path.is_file(), "Monthly manifest missing")
    require(source_path.is_file(), "Prepared gzip file missing")

    with manifest_path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.DictReader(handle)
        require(
            reader.fieldnames == MANIFEST_FIELDS,
            "Unexpected manifest schema",
        )
        rows = list(reader)

    require(len(rows) == 1, "Expected one manifest record")
    record = rows[0]

    require(
        positive_int(record["reporting_year"], "year") == year,
        "Manifest reporting year mismatch",
    )
    require(
        positive_int(record["reporting_month"], "month") == month,
        "Manifest reporting month mismatch",
    )

    expected = positive_int(
        record["expected_row_count"], "expected_row_count"
    )
    output_rows = positive_int(
        record["output_row_count"], "output_row_count"
    )

    require(expected == output_rows, "Manifest row mismatch")
    require(
        record["row_count_reconciled"].strip().lower() == "true",
        "Manifest reconciliation not passed",
    )
    require(
        positive_int(record["output_column_count"], "columns") == 51,
        "Expected 51 source columns",
    )
    require(
        positive_int(record["output_size_bytes"], "file size")
        == source_path.stat().st_size,
        "Prepared gzip size mismatch",
    )

    expected_name = f"bts_{tag}_bq.csv.gz"
    manifest_name = (
        record["output_file"]
        .replace("\\", "/")
        .split("/")[-1]
    )
    require(
        manifest_name == expected_name,
        "Prepared output filename mismatch",
    )

    for field in ("input_sha256", "output_sha256"):
        require(
            bool(re.fullmatch(
                r"[0-9a-fA-F]{64}", record[field]
            )),
            f"Invalid {field}",
        )

    actual_hash = sha256_file(source_path)
    require(
        actual_hash.lower() == record["output_sha256"].lower(),
        "Prepared gzip SHA256 mismatch",
    )

    print("MANIFEST_SHA256_PASS", flush=True)

    # Full streaming validation: no large in-memory dataframe.
    observed = 0
    start = date(year, month, 1)
    end = date(
        year + (month == 12),
        month % 12 + 1,
        1,
    )

    with gzip.open(
        source_path, "rt", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.reader(handle)
        header = next(reader, None)

        require(header is not None, "Empty gzip CSV")
        require(len(header) == 51, "CSV header is not 51 columns")
        require(
            len({name.lower() for name in header}) == 51,
            "Duplicate CSV header columns",
        )

        normalized = [name.strip().lower() for name in header]

        require(
            "flightdate" in normalized,
            "FlightDate header missing",
        )

        date_index = normalized.index("flightdate")

        for row in reader:
            observed += 1

            require(
                len(row) == 51,
                f"Invalid CSV column count at record {observed}",
            )

            try:
                flight_date = date.fromisoformat(row[date_index])
            except (ValueError, TypeError) as exc:
                raise RuntimeError(
                    f"Invalid FlightDate at record {observed}"
                ) from exc

            require(
                start <= flight_date < end,
                f"Out-of-month flight at record {observed}",
            )

    require(
        observed == expected,
        "Actual CSV data rows differ from manifest",
    )

    print("CSV_51_COLUMN_CONTRACT_PASS", flush=True)
    print("ALL_CSV_REPORTING_DATES_PASS", flush=True)
    print("CSV_ROW_COUNT_RECONCILED_PASS", flush=True)
    print("VALIDATED_ROWS:", observed, flush=True)

    return {
        "period": period,
        "path": source_path,
        "header": header,
        "rows": observed,
        "sha256": actual_hash,
        "size": source_path.stat().st_size,
        "start": start,
        "end": end,
    }


def execute_load(prepared):
    """Opt-in BigQuery append; never reprocess historical months."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    period = prepared["period"]

    require(
        period > "2026-02",
        "January-February are preserved historical loads",
    )

    receipt_dir = (
        ROOT / "evidence" / "bigquery" / "monthly_ingestion"
    )
    receipt_path = receipt_dir / f"{period}.json"

    require(
        not receipt_path.exists(),
        "Ingestion receipt already exists; refusing repeat",
    )

    client = bigquery.Client(project=PROJECT, location=LOCATION)
    table = client.get_table(DESTINATION)

    require(table.table_type == "TABLE", "Unexpected destination type")
    require(len(table.schema) == 51, "Raw schema is not 51 columns")

    schema_names = [field.name.lower() for field in table.schema]
    header_names = [name.strip().lower() for name in prepared["header"]]

    require(
        schema_names == header_names,
        "CSV column order differs from BigQuery schema",
    )

    job_id = (
        "bts_" + period.replace("-", "_") + "_raw_ingest_v1"
    )

    # Fixed month-level ID. Changing the source hash must
    # not silently create a second append job.
    try:
        previous = client.get_job(
            job_id, project=PROJECT, location=LOCATION
        )
    except NotFound:
        previous = None

    require(
        previous is None,
        "Monthly load job ID already exists; manual recovery required",
    )

    # Also reject any existing rows for the candidate month.
    # This is a read-only, maximum-billed-bytes-capped query.
    sql = """
SELECT COUNT(*) AS month_rows
FROM `tdb-dbt-bigquery-portfolio.raw_bts.flights_2026`
WHERE FlightDate >= @month_start
  AND FlightDate < @next_month_start
"""

    parameters = [
        bigquery.ScalarQueryParameter(
            "month_start", "DATE", prepared["start"]
        ),
        bigquery.ScalarQueryParameter(
            "next_month_start", "DATE", prepared["end"]
        ),
    ]

    config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        use_query_cache=False,
        maximum_bytes_billed=100_000_000,
        query_parameters=parameters,
    )

    dry = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            dry_run=True,
            use_legacy_sql=False,
            use_query_cache=False,
            maximum_bytes_billed=100_000_000,
            query_parameters=parameters,
        ),
        location=LOCATION,
    )

    require(
        0 < int(dry.total_bytes_processed or 0) <= 100_000_000,
        "Month-existence query cost preflight failed",
    )

    counts = list(client.query(
        sql, job_config=config, location=LOCATION
    ).result(timeout=120))

    require(len(counts) == 1, "Unexpected month-count result")
    require(
        int(counts[0]["month_rows"]) == 0,
        "Candidate month already contains raw flights",
    )

    before_rows = table.num_rows

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=table.schema,
        autodetect=False,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        max_bad_records=0,
        allow_jagged_rows=False,
        ignore_unknown_values=False,
        allow_quoted_newlines=False,
        encoding="UTF-8",
    )

    print("AUTHORIZED_NEW_MONTH:", period, flush=True)
    print("SUBMITTING_APPEND_LOAD_JOB", flush=True)

    with prepared["path"].open("rb") as handle:
        job = client.load_table_from_file(
            handle,
            DESTINATION,
            job_config=load_config,
            job_id=job_id,
            location=LOCATION,
        )

    job.result(timeout=600)

    require(
        job.state == "DONE" and job.error_result is None,
        "BigQuery load did not complete successfully",
    )
    require(
        job.output_rows == prepared["rows"],
        "BigQuery loaded row count mismatch",
    )
    require(
        job.input_files == 1,
        "Unexpected BigQuery input-file count",
    )

    after = client.get_table(DESTINATION)

    require(
        after.num_rows == before_rows + prepared["rows"],
        "Cumulative BigQuery row count mismatch",
    )

    receipt = {
        "checkpoint": "BTS_RECURRING_MONTHLY_LOAD",
        "month": period,
        "project": PROJECT,
        "region": LOCATION,
        "destination": "raw_bts.flights_2026",
        "job_id": job.job_id,
        "job_status": "DONE",
        "imported_rows": prepared["rows"],
        "bad_records": 0,
        "input_files": 1,
        "source_sha256": prepared["sha256"],
        "verified_total_rows": after.num_rows,
        "write_disposition": "WRITE_APPEND",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    # Do not overwrite or replace historical evidence.
    receipt_dir.mkdir(parents=True, exist_ok=True)

    with receipt_path.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2)
        handle.write("\n")

    print("BIGQUERY_APPEND_VERIFIED_PASS", flush=True)
    print("INGESTION_RECEIPT_SAVED:", receipt_path, flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Validate or load one prepared BTS 2026 month"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Explicitly authorize a BigQuery append",
    )

    args = parser.parse_args()

    print("PROCESSING_MONTH:", f"{args.year}-{args.month:02d}")
    print("MODE:", "EXECUTE" if args.execute else "LOCAL_PREFLIGHT")

    prepared = validate_prepared_month(args.year, args.month)

    if args.execute:
        execute_load(prepared)
    else:
        print("LOCAL_PREPARED_FILE_PASS")
        print("NO_BIGQUERY_API_CALLS")
        print("NO_BIGQUERY_LOAD_JOB")
        print("NO_RECEIPT_WRITTEN")
        print("B3_51CF_LOCAL_PREFLIGHT_PASS")


if __name__ == "__main__":
    main()
