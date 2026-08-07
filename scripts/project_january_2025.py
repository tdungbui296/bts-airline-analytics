from __future__ import annotations

import csv
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_ZIP = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "schema_samples"
    / "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2025_1.zip"
)

COLUMN_MANIFEST = (
    PROJECT_ROOT
    / "config"
    / "bts_core_columns.txt"
)

OUTPUT_CSV = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "pilot"
    / "bts_2025_01_core.csv"
)

SUMMARY_JSON = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "january_2025_projection_summary.json"
)


def calculate_sha256(file_path: Path) -> str:
    """Calculate the SHA-256 checksum of a file."""
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest().upper()


def load_required_columns(file_path: Path) -> list[str]:
    """Read and validate the core-column manifest."""
    columns = [
        line.strip()
        for line in file_path.read_text(
            encoding="ascii"
        ).splitlines()
        if line.strip()
    ]

    if len(columns) != 51:
        raise ValueError(
            f"Expected 51 required columns, "
            f"but found {len(columns)}."
        )

    duplicate_columns = sorted(
        {
            column
            for column in columns
            if columns.count(column) > 1
        }
    )

    if duplicate_columns:
        raise ValueError(
            "Duplicate manifest columns found: "
            f"{duplicate_columns}"
        )

    return columns


def main() -> None:
    if not SOURCE_ZIP.exists():
        raise FileNotFoundError(
            f"Source ZIP not found: {SOURCE_ZIP}"
        )

    if not COLUMN_MANIFEST.exists():
        raise FileNotFoundError(
            f"Column manifest not found: {COLUMN_MANIFEST}"
        )

    required_columns = load_required_columns(
        COLUMN_MANIFEST
    )

    OUTPUT_CSV.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    SUMMARY_JSON.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with zipfile.ZipFile(SOURCE_ZIP, "r") as archive:
        csv_members = [
            member
            for member in archive.namelist()
            if member.lower().endswith(".csv")
            and not member.endswith("/")
        ]

        if len(csv_members) != 1:
            raise ValueError(
                "Expected exactly one CSV inside the ZIP, "
                f"but found {len(csv_members)}: {csv_members}"
            )

        csv_member = csv_members[0]

        with archive.open(csv_member, "r") as raw_file:
            with TextIOWrapper(
                raw_file,
                encoding="utf-8-sig",
                newline=""
            ) as text_file:
                reader = csv.DictReader(text_file)

                if reader.fieldnames is None:
                    raise ValueError(
                        "The source CSV has no header row."
                    )

                source_headers = [
                    header if header is not None else ""
                    for header in reader.fieldnames
                ]

                non_blank_headers = [
                    header
                    for header in source_headers
                    if header.strip()
                ]

                missing_columns = [
                    column
                    for column in required_columns
                    if column not in non_blank_headers
                ]

                if missing_columns:
                    raise ValueError(
                        "Required source columns are missing: "
                        f"{missing_columns}"
                    )

                duplicate_headers = sorted(
                    {
                        header
                        for header in non_blank_headers
                        if non_blank_headers.count(header) > 1
                    }
                )

                if duplicate_headers:
                    raise ValueError(
                        "Duplicate source headers found: "
                        f"{duplicate_headers}"
                    )

                omitted_columns = [
                    header
                    for header in non_blank_headers
                    if header not in required_columns
                ]

                output_columns = (
                    required_columns
                    + ["_source_file_name"]
                )

                row_count = 0

                with OUTPUT_CSV.open(
                    "w",
                    encoding="utf-8",
                    newline=""
                ) as output_file:
                    writer = csv.DictWriter(
                        output_file,
                        fieldnames=output_columns,
                        extrasaction="ignore"
                    )

                    writer.writeheader()

                    for row_number, row in enumerate(
                        reader,
                        start=2
                    ):
                        if None in row:
                            raise ValueError(
                                "A row contains more values than "
                                "the header defines. "
                                f"CSV row: {row_number}"
                            )

                        projected_row = {
                            column: row.get(column, "")
                            for column in required_columns
                        }

                        projected_row[
                            "_source_file_name"
                        ] = SOURCE_ZIP.name

                        writer.writerow(projected_row)
                        row_count += 1

    summary = {
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_zip": SOURCE_ZIP.name,
        "source_zip_sha256": calculate_sha256(
            SOURCE_ZIP
        ),
        "source_csv_member": csv_member,
        "source_header_count_including_blank": len(
            source_headers
        ),
        "source_official_column_count": len(
            non_blank_headers
        ),
        "blank_header_count": (
            len(source_headers)
            - len(non_blank_headers)
        ),
        "required_source_column_count": len(
            required_columns
        ),
        "technical_metadata_column_count": 1,
        "output_column_count": len(output_columns),
        "omitted_source_column_count": len(
            omitted_columns
        ),
        "omitted_source_columns": omitted_columns,
        "rows_read": row_count,
        "rows_written": row_count,
        "row_count_reconciled": True,
        "output_csv": str(
            OUTPUT_CSV.relative_to(PROJECT_ROOT)
        ),
        "output_size_bytes": OUTPUT_CSV.stat().st_size,
        "output_size_mb": round(
            OUTPUT_CSV.stat().st_size
            / (1024 * 1024),
            2
        ),
        "output_sha256": calculate_sha256(
            OUTPUT_CSV
        )
    }

    SUMMARY_JSON.write_text(
        json.dumps(
            summary,
            indent=2
        ),
        encoding="utf-8"
    )

    print("Projection completed successfully.")
    print(f"Rows read:     {row_count:,}")
    print(f"Rows written:  {row_count:,}")
    print(f"Core fields:   {len(required_columns)}")
    print(f"Output fields: {len(output_columns)}")
    print(f"Output file:   {OUTPUT_CSV}")
    print(f"Summary file:  {SUMMARY_JSON}")


if __name__ == "__main__":
    main()
