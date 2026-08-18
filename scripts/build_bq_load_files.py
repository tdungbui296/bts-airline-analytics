from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "config"
    / "bq_flights_schema.json"
)

CHUNK_SIZE = 100_000


def parse_year(value: str) -> int:
    """Validate a four-digit reporting year."""
    if len(value) != 4 or not value.isdigit():
        raise argparse.ArgumentTypeError(
            "year must be a four-digit value such as 2024"
        )

    return int(value)


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


def extract_month(
    file_path: Path,
    year: int,
) -> int:
    """Extract month from bts_YEAR_MM_core.csv.gz."""
    match = re.search(
        rf"bts_{year}_(\d{{2}})_core\.csv\.gz$",
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


def load_processed_manifest(
    processed_manifest_path: Path,
) -> dict[int, dict[str, str]]:
    """Load expected rows and checksums for projected files."""
    manifest = pd.read_csv(
        processed_manifest_path,
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
    year: int,
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
        with (
            temporary_path.open("wb") as raw_output_file,
            gzip.GzipFile(
                filename=temporary_path.name,
                mode="wb",
                compresslevel=6,
                fileobj=raw_output_file,
                mtime=0,
            ) as gzip_output_file,
            io.TextIOWrapper(
                gzip_output_file,
                encoding="utf-8",
                newline="",
            ) as output_file,
        ):
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
        "reporting_year": year,
        "reporting_month": extract_month(
            input_path,
            year,
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
    parser = argparse.ArgumentParser(
        description=(
            "Generate BigQuery-ready BTS load files for one "
            "reporting year."
        )
    )

    parser.add_argument(
        "--year",
        required=True,
        type=parse_year,
        help="Four-digit source year, for example 2024 or 2025.",
    )

    args = parser.parse_args()
    year = args.year

    input_dir = (
        PROJECT_ROOT
        / "data"
        / "processed"
        / str(year)
    )

    output_dir = (
        PROJECT_ROOT
        / "data"
        / "load"
        / str(year)
    )

    processed_manifest_path = (
        PROJECT_ROOT
        / "evidence"
        / "source"
        / f"{year}_processed_manifest.csv"
    )

    load_manifest_path = (
        PROJECT_ROOT
        / "evidence"
        / "source"
        / f"{year}_bq_load_manifest.csv"
    )

    summary_path = (
        PROJECT_ROOT
        / "analysis"
        / "profiling"
        / f"{year}_bq_load_files_summary.json"
    )

    required_paths = [
        input_dir,
        SCHEMA_PATH,
        processed_manifest_path,
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(
                f"Required path not found: {path}"
            )

    schema = load_schema()
    manifest = load_processed_manifest(
        processed_manifest_path
    )

    input_files = sorted(
        input_dir.glob(
            f"bts_{year}_*_core.csv.gz"
        ),
        key=lambda path: extract_month(
            path,
            year,
        ),
    )

    if len(input_files) != 12:
        raise ValueError(
            f"Expected 12 input files; found "
            f"{len(input_files)}."
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    load_manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_rows = []

    for input_path in input_files:
        month = extract_month(
            input_path,
            year,
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
            output_dir
            / f"bts_{year}_{month:02d}_bq.csv.gz"
        )

        print(
            f"Normalizing {year}-{month:02d} "
            f"({expected_rows:,} rows)...",
            flush=True,
        )

        result = normalize_month(
            input_path=input_path,
            output_path=output_path,
            schema=schema,
            expected_rows=expected_rows,
            expected_input_hash=expected_hash,
            year=year,
        )

        output_rows.append(result)

        print(
            f"{year}-{month:02d}: completed, "
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

    with load_manifest_path.open(
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
        "year": year,
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
            load_manifest_path.relative_to(
                PROJECT_ROOT
            )
        ),
        "output_directory": str(
            output_dir.relative_to(
                PROJECT_ROOT
            )
        ),
    }

    summary_path.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(
        f"{year} BigQuery load-file generation completed."
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
        f"Manifest: {load_manifest_path}"
    )
    print(
        f"Summary: {summary_path}"
    )


if __name__ == "__main__":
    main()
