from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2024"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2024"
)

COLUMN_MANIFEST = (
    PROJECT_ROOT
    / "config"
    / "bts_core_columns.txt"
)

SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "evidence"
    / "source"
    / "2024_source_manifest.csv"
)

VALIDATION_SUMMARY = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2024_monthly_validation_summary.csv"
)

PROJECTION_MANIFEST = (
    PROJECT_ROOT
    / "evidence"
    / "source"
    / "2024_processed_manifest.csv"
)

PROJECTION_SUMMARY = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2024_projection_summary.json"
)


def calculate_sha256(file_path: Path) -> str:
    """Calculate a file's SHA-256 checksum."""
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            sha256.update(chunk)

    return sha256.hexdigest().upper()


def extract_month(file_path: Path) -> int:
    """Extract the reporting month from a BTS ZIP filename."""
    match = re.search(
        r"_2024_(\d+)\.zip$",
        file_path.name,
    )

    if not match:
        raise ValueError(
            f"Could not extract month from: {file_path.name}"
        )

    month = int(match.group(1))

    if month not in range(1, 13):
        raise ValueError(
            f"Invalid reporting month in: {file_path.name}"
        )

    return month


def load_core_columns() -> list[str]:
    """Load and validate the 51-column analytical contract."""
    columns = [
        line.strip()
        for line in COLUMN_MANIFEST.read_text(
            encoding="ascii"
        ).splitlines()
        if line.strip()
    ]

    if len(columns) != 51:
        raise ValueError(
            f"Expected 51 core columns; found {len(columns)}."
        )

    if len(columns) != len(set(columns)):
        raise ValueError(
            "The core-column manifest contains duplicates."
        )

    return columns


def load_source_manifest() -> dict[int, dict[str, str]]:
    """Load source checksums and metadata by month."""
    with SOURCE_MANIFEST.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        rows = list(csv.DictReader(file))

    manifest = {
        int(row["ReportingMonth"]): row
        for row in rows
    }

    if sorted(manifest) != list(range(1, 13)):
        raise ValueError(
            "The source manifest does not contain months 1â€“12."
        )

    return manifest


def load_validation_results() -> dict[int, dict[str, str]]:
    """Load expected row counts from batch validation."""
    with VALIDATION_SUMMARY.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        rows = list(csv.DictReader(file))

    results = {
        int(row["reporting_month"]): row
        for row in rows
    }

    if sorted(results) != list(range(1, 13)):
        raise ValueError(
            "The validation results do not contain months 1â€“12."
        )

    approved_review_conditions = {
        7: {
            "delayed_missing_cause_rows": "2",
            "candidate_key_null_rows": "0",
        },
        8: {
            "delayed_missing_cause_rows": "0",
            "candidate_key_null_rows": "1",
        },
    }

    for month, row in results.items():
        review_required = (
            row["review_required"]
            .strip()
            .lower()
            == "true"
        )

        if review_required:
            expected_conditions = (
                approved_review_conditions.get(month)
            )

            if expected_conditions is None:
                raise ValueError(
                    f"Month {month} is marked for manual review "
                    "and has not been approved."
                )

            mismatches = []

            for field, expected_value in (
                expected_conditions.items()
            ):
                actual_value = row[field].strip()

                if actual_value != expected_value:
                    mismatches.append(
                        f"{field}: expected {expected_value}, "
                        f"found {actual_value}"
                    )

            if mismatches:
                raise ValueError(
                    f"Month {month} no longer matches its "
                    "approved review conditions: "
                    + "; ".join(mismatches)
                )

            print(
                f"2024-{month:02d}: manually reviewed "
                "conditions approved."
            )

        if row["error_message"].strip():
            raise ValueError(
                f"Month {month} has a validation error: "
                f"{row['error_message']}"
            )

    return results


def project_month(
    source_zip: Path,
    month: int,
    core_columns: list[str],
    expected_rows: int,
    expected_source_hash: str,
) -> dict[str, object]:
    """Project one validated monthly ZIP into CSV.GZ."""

    actual_source_hash = calculate_sha256(source_zip)

    if actual_source_hash != expected_source_hash.upper():
        raise ValueError(
            f"Source checksum changed for month {month:02d}."
        )

    output_path = (
        OUTPUT_DIR
        / f"bts_2024_{month:02d}_core.csv.gz"
    )

    temporary_path = Path(
        str(output_path) + ".tmp"
    )

    if temporary_path.exists():
        temporary_path.unlink()

    row_count = 0
    output_columns = (
        core_columns
        + ["_source_file_name"]
    )

    try:
        with zipfile.ZipFile(source_zip, "r") as archive:
            csv_members = [
                member
                for member in archive.namelist()
                if member.lower().endswith(".csv")
                and not member.endswith("/")
            ]

            if len(csv_members) != 1:
                raise ValueError(
                    f"Expected one CSV in {source_zip.name}; "
                    f"found {len(csv_members)}."
                )

            csv_member = csv_members[0]

            with archive.open(
                csv_member,
                "r",
            ) as raw_file:
                with TextIOWrapper(
                    raw_file,
                    encoding="utf-8-sig",
                    newline="",
                ) as text_file:
                    reader = csv.DictReader(text_file)

                    if reader.fieldnames is None:
                        raise ValueError(
                            f"No header found in {source_zip.name}."
                        )

                    source_headers = [
                        header
                        if header is not None
                        else ""
                        for header in reader.fieldnames
                    ]

                    missing_columns = [
                        column
                        for column in core_columns
                        if column not in source_headers
                    ]

                    if missing_columns:
                        raise ValueError(
                            f"Month {month:02d} is missing columns: "
                            f"{missing_columns}"
                        )

                    with gzip.open(
                        temporary_path,
                        mode="wt",
                        encoding="utf-8",
                        newline="",
                        compresslevel=6,
                    ) as output_file:
                        writer = csv.DictWriter(
                            output_file,
                            fieldnames=output_columns,
                            extrasaction="ignore",
                        )

                        writer.writeheader()

                        for source_row_number, row in enumerate(
                            reader,
                            start=2,
                        ):
                            if None in row:
                                raise ValueError(
                                    "More row values than headers in "
                                    f"{source_zip.name}, CSV row "
                                    f"{source_row_number}."
                                )

                            projected_row = {
                                column: row.get(column, "")
                                for column in core_columns
                            }

                            projected_row[
                                "_source_file_name"
                            ] = source_zip.name

                            writer.writerow(projected_row)
                            row_count += 1

        if row_count != expected_rows:
            raise ValueError(
                f"Row mismatch for month {month:02d}: "
                f"expected {expected_rows:,}, "
                f"projected {row_count:,}."
            )

        temporary_path.replace(output_path)

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()

        raise

    return {
        "reporting_year": 2024,
        "reporting_month": month,
        "source_file": source_zip.name,
        "source_sha256": actual_source_hash,
        "source_row_count": expected_rows,
        "output_file": str(
            output_path.relative_to(PROJECT_ROOT)
        ),
        "output_row_count": row_count,
        "row_count_reconciled": (
            row_count == expected_rows
        ),
        "core_source_column_count": len(
            core_columns
        ),
        "technical_column_count": 1,
        "output_column_count": len(
            output_columns
        ),
        "output_size_bytes": output_path.stat().st_size,
        "output_size_mb": round(
            output_path.stat().st_size
            / (1024 * 1024),
            2,
        ),
        "output_sha256": calculate_sha256(
            output_path
        ),
    }


def main() -> None:
    required_paths = [
        RAW_DIR,
        COLUMN_MANIFEST,
        SOURCE_MANIFEST,
        VALIDATION_SUMMARY,
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(
                f"Required path not found: {path}"
            )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    PROJECTION_MANIFEST.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    PROJECTION_SUMMARY.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    core_columns = load_core_columns()
    source_manifest = load_source_manifest()
    validation_results = load_validation_results()

    zip_files = sorted(
        RAW_DIR.glob(
            "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_*.zip"
        ),
        key=extract_month,
    )

    if len(zip_files) != 12:
        raise ValueError(
            f"Expected 12 ZIP files; found {len(zip_files)}."
        )

    projection_rows = []

    for source_zip in zip_files:
        month = extract_month(source_zip)

        expected_rows = int(
            validation_results[month]["row_count"]
        )

        expected_source_hash = (
            source_manifest[month]["SHA256"]
        )

        print(
            f"Projecting 2024-{month:02d} "
            f"({expected_rows:,} rows)...",
            flush=True,
        )

        result = project_month(
            source_zip=source_zip,
            month=month,
            core_columns=core_columns,
            expected_rows=expected_rows,
            expected_source_hash=expected_source_hash,
        )

        projection_rows.append(result)

        print(
            f"2024-{month:02d}: completed, "
            f"{result['output_size_mb']} MB",
            flush=True,
        )

    manifest_fields = [
        "reporting_year",
        "reporting_month",
        "source_file",
        "source_sha256",
        "source_row_count",
        "output_file",
        "output_row_count",
        "row_count_reconciled",
        "core_source_column_count",
        "technical_column_count",
        "output_column_count",
        "output_size_bytes",
        "output_size_mb",
        "output_sha256",
    ]

    with PROJECTION_MANIFEST.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=manifest_fields,
        )

        writer.writeheader()
        writer.writerows(projection_rows)

    total_source_rows = sum(
        int(row["source_row_count"])
        for row in projection_rows
    )

    total_output_rows = sum(
        int(row["output_row_count"])
        for row in projection_rows
    )

    total_output_bytes = sum(
        int(row["output_size_bytes"])
        for row in projection_rows
    )

    summary = {
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "year": 2024,
        "monthly_output_count": len(
            projection_rows
        ),
        "total_source_rows": total_source_rows,
        "total_output_rows": total_output_rows,
        "all_row_counts_reconciled": all(
            bool(row["row_count_reconciled"])
            for row in projection_rows
        ),
        "core_source_column_count": len(
            core_columns
        ),
        "technical_column_count": 1,
        "output_column_count": (
            len(core_columns) + 1
        ),
        "total_output_size_bytes": (
            total_output_bytes
        ),
        "total_output_size_mb": round(
            total_output_bytes
            / (1024 * 1024),
            2,
        ),
        "processed_manifest": str(
            PROJECTION_MANIFEST.relative_to(
                PROJECT_ROOT
            )
        ),
    }

    PROJECTION_SUMMARY.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print("2024 batch projection completed.")
    print(
        f"Monthly outputs: "
        f"{summary['monthly_output_count']}"
    )
    print(
        f"Total rows: "
        f"{summary['total_output_rows']:,}"
    )
    print(
        "All row counts reconciled: "
        f"{summary['all_row_counts_reconciled']}"
    )
    print(
        f"Compressed output size: "
        f"{summary['total_output_size_mb']} MB"
    )
    print(
        f"Manifest: {PROJECTION_MANIFEST}"
    )
    print(
        f"Summary: {PROJECTION_SUMMARY}"
    )


if __name__ == "__main__":
    main()
