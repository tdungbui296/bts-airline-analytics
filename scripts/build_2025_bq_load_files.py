from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2025"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "load"
    / "2025"
)

SCHEMA_PATH = (
    PROJECT_ROOT
    / "config"
    / "bq_flights_schema.json"
)

PROCESSED_MANIFEST_PATH = (
    PROJECT_ROOT
    / "evidence"
    / "source"
    / "2025_processed_manifest.csv"
)

LOAD_MANIFEST_PATH = (
    PROJECT_ROOT
    / "evidence"
    / "source"
    / "2025_bq_load_manifest.csv"
)

SUMMARY_PATH = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_bq_load_files_summary.json"
)

CHUNK_SIZE = 100_000


def calculate_sha256(file_path: Path) -> str:
    """Calculate the SHA-256 checksum of a file."""
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            sha256.update(chunk)

    return sha256.hexdigest().upper()


def extract_month(file_path: Path) -> int:
    """Extract month from bts_2025_MM_core.csv.gz."""
    match = re.search(
        r"bts_2025_(\d{2})_core\.csv\.gz$",
        file_path.name,
    )

    if not match:
        raise ValueError(
            f"Cannot extract month from: {file_path.name}"
        )

    month = int(match.group(1))

    if month not in range(1, 13):
        raise ValueError(
            f"Invalid month in filename: {file_path.name}"
        )

    return month


def load_schema() -> list[dict[str, str]]:
    """Load and validate the explicit 51-column schema."""
    schema = json.loads(
        SCHEMA_PATH.read_text(
            encoding="ascii"
        )
    )

    if len(schema) != 51:
        raise ValueError(
            f"Expected 51 schema fields; found {len(schema)}."
        )

    valid_types = {
        "INT64",
        "DATE",
        "STRING",
    }

    valid_modes = {
        "REQUIRED",
        "NULLABLE",
    }

    field_names = []

    for field in schema:
        if set(field) != {
            "name",
            "type",
            "mode",
        }:
            raise ValueError(
                "Each schema field must contain exactly "
                "name, type and mode."
            )

        if field["type"] not in valid_types:
            raise ValueError(
                f"Unsupported type: {field['type']}"
            )

        if field["mode"] not in valid_modes:
            raise ValueError(
                f"Unsupported mode: {field['mode']}"
            )

        field_names.append(field["name"])

    if len(field_names) != len(set(field_names)):
        raise ValueError(
            "Duplicate schema field names found."
        )

    return schema


def load_processed_manifest() -> dict[int, dict[str, str]]:
    """Load expected rows and checksums for projected files."""
    manifest = pd.read_csv(
        PROCESSED_MANIFEST_PATH,
        dtype="string",
    )

    if len(manifest) != 12:
        raise ValueError(
            "The processed manifest must contain 12 rows."
        )

    rows_by_month = {
        int(row["reporting_month"]): {
            key: str(value)
            for key, value in row.items()
        }
        for row in manifest.to_dict(
            orient="records"
        )
    }

    if sorted(rows_by_month) != list(range(1, 13)):
        raise ValueError(
            "The processed manifest must contain months 1–12."
        )

    return rows_by_month


def normalize_integer_column(
    series: pd.Series,
    column_name: str,
) -> pd.Series:
    """
    Convert integer-compatible source text such as
    0.00, 84.00 and -60.00 to canonical integer text.
    """
    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    parse_failure_mask = (
        series.notna()
        & numeric.isna()
    )

    if parse_failure_mask.any():
        samples = (
            series.loc[parse_failure_mask]
            .drop_duplicates()
            .head(5)
            .tolist()
        )

        raise ValueError(
            f"{column_name} contains values that cannot "
            f"be parsed numerically: {samples}"
        )

    fractional_mask = (
        numeric.notna()
        & ~np.isclose(
            numeric % 1,
            0,
            equal_nan=False,
        )
    )

    if fractional_mask.any():
        samples = (
            series.loc[fractional_mask]
            .drop_duplicates()
            .head(5)
            .tolist()
        )

        raise ValueError(
            f"{column_name} contains fractional values: "
            f"{samples}"
        )

    return (
        numeric
        .round()
        .astype("Int64")
        .astype("string")
    )


def normalize_date_column(
    series: pd.Series,
    column_name: str,
) -> pd.Series:
    """Convert dates to canonical YYYY-MM-DD text."""
    parsed = pd.to_datetime(
        series,
        errors="coerce",
        format="%Y-%m-%d",
    )

    parse_failure_mask = (
        series.notna()
        & parsed.isna()
    )

    if parse_failure_mask.any():
        samples = (
            series.loc[parse_failure_mask]
            .drop_duplicates()
            .head(5)
            .tolist()
        )

        raise ValueError(
            f"{column_name} contains invalid dates: "
            f"{samples}"
        )

    return (
        parsed
        .dt.strftime("%Y-%m-%d")
        .astype("string")
    )


def normalize_month(
    input_path: Path,
    output_path: Path,
    schema: list[dict[str, str]],
    expected_rows: int,
    expected_input_hash: str,
) -> dict[str, object]:
    """Normalize one monthly file for BigQuery loading."""

    actual_input_hash = calculate_sha256(
        input_path
    )

    if actual_input_hash != expected_input_hash.upper():
        raise ValueError(
            f"Input checksum changed: {input_path.name}"
        )

    column_names = [
        field["name"]
        for field in schema
    ]

    int_columns = [
        field["name"]
        for field in schema
        if field["type"] == "INT64"
    ]

    date_columns = [
        field["name"]
        for field in schema
        if field["type"] == "DATE"
    ]

    required_columns = [
        field["name"]
        for field in schema
        if field["mode"] == "REQUIRED"
    ]

    temporary_path = Path(
        str(output_path) + ".tmp"
    )

    if temporary_path.exists():
        temporary_path.unlink()

    row_count = 0
    write_header = True

    try:
        with gzip.open(
            temporary_path,
            mode="wt",
            encoding="utf-8",
            newline="",
            compresslevel=6,
        ) as output_file:

            reader = pd.read_csv(
                input_path,
                compression="gzip",
                usecols=column_names,
                dtype="string",
                keep_default_na=True,
                chunksize=CHUNK_SIZE,
            )

            for chunk in reader:
                chunk = chunk[column_names].copy()

                for column in required_columns:
                    null_count = int(
                        chunk[column].isna().sum()
                    )

                    if null_count > 0:
                        raise ValueError(
                            f"Required column {column} contains "
                            f"{null_count} null values in "
                            f"{input_path.name}."
                        )

                for column in int_columns:
                    chunk[column] = (
                        normalize_integer_column(
                            chunk[column],
                            column,
                        )
                    )

                for column in date_columns:
                    chunk[column] = (
                        normalize_date_column(
                            chunk[column],
                            column,
                        )
                    )

                chunk.to_csv(
                    output_file,
                    index=False,
                    header=write_header,
                    na_rep="",
                    lineterminator="\n",
                    quoting=csv.QUOTE_MINIMAL,
                )

                row_count += len(chunk)
                write_header = False

        if row_count != expected_rows:
            raise ValueError(
                f"Row mismatch for {input_path.name}: "
                f"expected {expected_rows:,}, "
                f"wrote {row_count:,}."
            )

        temporary_path.replace(
            output_path
        )

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()

        raise

    with gzip.open(
        output_path,
        mode="rt",
        encoding="utf-8",
        newline="",
    ) as file:
        output_header = next(
            csv.reader(file)
        )

    if output_header != column_names:
        raise ValueError(
            f"Output header mismatch: {output_path.name}"
        )

    return {
        "reporting_year": 2025,
        "reporting_month": extract_month(
            input_path
        ),
        "input_file": str(
            input_path.relative_to(
                PROJECT_ROOT
            )
        ),
        "input_sha256": actual_input_hash,
        "expected_row_count": expected_rows,
        "output_file": str(
            output_path.relative_to(
                PROJECT_ROOT
            )
        ),
        "output_row_count": row_count,
        "row_count_reconciled": (
            row_count == expected_rows
        ),
        "output_column_count": len(
            column_names
        ),
        "output_size_bytes": (
            output_path.stat().st_size
        ),
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
        INPUT_DIR,
        SCHEMA_PATH,
        PROCESSED_MANIFEST_PATH,
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(
                f"Required path not found: {path}"
            )

    schema = load_schema()
    manifest = load_processed_manifest()

    input_files = sorted(
        INPUT_DIR.glob(
            "bts_2025_*_core.csv.gz"
        ),
        key=extract_month,
    )

    if len(input_files) != 12:
        raise ValueError(
            f"Expected 12 input files; found "
            f"{len(input_files)}."
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    LOAD_MANIFEST_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    SUMMARY_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_rows = []

    for input_path in input_files:
        month = extract_month(
            input_path
        )

        expected_rows = int(
            manifest[month][
                "output_row_count"
            ]
        )

        expected_hash = (
            manifest[month][
                "output_sha256"
            ]
        )

        output_path = (
            OUTPUT_DIR
            / f"bts_2025_{month:02d}_bq.csv.gz"
        )

        print(
            f"Normalizing 2025-{month:02d} "
            f"({expected_rows:,} rows)...",
            flush=True,
        )

        result = normalize_month(
            input_path=input_path,
            output_path=output_path,
            schema=schema,
            expected_rows=expected_rows,
            expected_input_hash=expected_hash,
        )

        output_rows.append(result)

        print(
            f"2025-{month:02d}: completed, "
            f"{result['output_size_mb']} MB",
            flush=True,
        )

    manifest_fields = [
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

    with LOAD_MANIFEST_PATH.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=manifest_fields,
        )

        writer.writeheader()
        writer.writerows(output_rows)

    total_expected_rows = sum(
        int(row["expected_row_count"])
        for row in output_rows
    )

    total_output_rows = sum(
        int(row["output_row_count"])
        for row in output_rows
    )

    total_output_bytes = sum(
        int(row["output_size_bytes"])
        for row in output_rows
    )

    summary = {
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "year": 2025,
        "monthly_output_count": len(
            output_rows
        ),
        "output_column_count": len(
            schema
        ),
        "total_expected_rows": (
            total_expected_rows
        ),
        "total_output_rows": (
            total_output_rows
        ),
        "all_row_counts_reconciled": all(
            bool(row["row_count_reconciled"])
            for row in output_rows
        ),
        "total_output_size_bytes": (
            total_output_bytes
        ),
        "total_output_size_mb": round(
            total_output_bytes
            / (1024 * 1024),
            2,
        ),
        "load_manifest": str(
            LOAD_MANIFEST_PATH.relative_to(
                PROJECT_ROOT
            )
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(
                PROJECT_ROOT
            )
        ),
    }

    SUMMARY_PATH.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(
        "2025 BigQuery load-file generation completed."
    )
    print(
        f"Monthly outputs: "
        f"{summary['monthly_output_count']}"
    )
    print(
        f"Output columns: "
        f"{summary['output_column_count']}"
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
        f"Manifest: {LOAD_MANIFEST_PATH}"
    )
    print(
        f"Summary: {SUMMARY_PATH}"
    )


if __name__ == "__main__":
    main()
