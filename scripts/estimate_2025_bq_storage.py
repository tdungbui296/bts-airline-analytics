from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2025"
)

SCHEMA_PATH = (
    PROJECT_ROOT
    / "config"
    / "bq_flights_schema.json"
)

PROJECTION_SUMMARY_PATH = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_projection_summary.json"
)

COLUMN_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_logical_storage_by_column.csv"
)

SUMMARY_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_logical_storage_estimate.json"
)

CHUNK_SIZE = 100_000

# A conservative comparison threshold.
# Decimal 10 GB is slightly stricter than 10 GiB.
CONSERVATIVE_LIMIT_BYTES = 10_000_000_000


def load_schema() -> list[dict[str, str]]:
    """Load and validate the explicit BigQuery schema."""
    schema = json.loads(
        SCHEMA_PATH.read_text(
            encoding="ascii"
        )
    )

    if len(schema) != 51:
        raise ValueError(
            f"Expected 51 schema fields; found {len(schema)}."
        )

    supported_types = {
        "INT64",
        "DATE",
        "STRING",
    }

    field_names = []

    for field in schema:
        required_keys = {
            "name",
            "type",
            "mode",
        }

        if set(field) != required_keys:
            raise ValueError(
                "Each schema field must contain exactly "
                "name, type and mode."
            )

        if field["type"] not in supported_types:
            raise ValueError(
                f"Unsupported type for estimator: "
                f"{field['name']}={field['type']}"
            )

        field_names.append(field["name"])

    if len(field_names) != len(set(field_names)):
        raise ValueError(
            "Duplicate field names found in the schema."
        )

    return schema


def main() -> None:
    required_paths = [
        INPUT_DIR,
        SCHEMA_PATH,
        PROJECTION_SUMMARY_PATH,
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(
                f"Required path not found: {path}"
            )

    schema = load_schema()

    column_names = [
        field["name"]
        for field in schema
    ]

    type_by_column = {
        field["name"]: field["type"]
        for field in schema
    }

    mode_by_column = {
        field["name"]: field["mode"]
        for field in schema
    }

    input_files = sorted(
        INPUT_DIR.glob(
            "bts_2025_*_core.csv.gz"
        )
    )

    if len(input_files) != 12:
        raise ValueError(
            f"Expected 12 input files; found {len(input_files)}."
        )

    statistics = {
        column: {
            "column_name": column,
            "bq_type": type_by_column[column],
            "mode": mode_by_column[column],
            "row_count": 0,
            "non_null_count": 0,
            "null_count": 0,
            "value_bytes": 0,
            "overhead_bytes": 0,
            "estimated_logical_bytes": 0,
        }
        for column in column_names
    }

    total_rows = 0
    monthly_row_counts = {}

    for file_path in input_files:
        print(
            f"Estimating {file_path.name}...",
            flush=True,
        )

        monthly_rows = 0

        reader = pd.read_csv(
            file_path,
            compression="gzip",
            usecols=column_names,
            dtype="string",
            keep_default_na=False,
            na_filter=False,
            chunksize=CHUNK_SIZE,
        )

        for chunk in reader:
            chunk_row_count = len(chunk)

            total_rows += chunk_row_count
            monthly_rows += chunk_row_count

            for column in column_names:
                series = chunk[column]

                # Empty CSV fields will be loaded as NULL.
                non_null_mask = series.ne("")

                non_null_count = int(
                    non_null_mask.sum()
                )

                null_count = (
                    chunk_row_count
                    - non_null_count
                )

                result = statistics[column]

                result["row_count"] += (
                    chunk_row_count
                )

                result["non_null_count"] += (
                    non_null_count
                )

                result["null_count"] += (
                    null_count
                )

                bq_type = type_by_column[column]

                if bq_type in {
                    "INT64",
                    "DATE",
                }:
                    logical_bytes = (
                        non_null_count * 8
                    )

                    result[
                        "value_bytes"
                    ] += logical_bytes

                    result[
                        "estimated_logical_bytes"
                    ] += logical_bytes

                elif bq_type == "STRING":
                    non_null_values = (
                        series.loc[
                            non_null_mask
                        ]
                        .astype(str)
                    )

                    utf8_value_bytes = int(
                        non_null_values
                        .str.encode("utf-8")
                        .str.len()
                        .sum()
                    )

                    string_overhead_bytes = (
                        non_null_count * 2
                    )

                    result[
                        "value_bytes"
                    ] += utf8_value_bytes

                    result[
                        "overhead_bytes"
                    ] += string_overhead_bytes

                    result[
                        "estimated_logical_bytes"
                    ] += (
                        utf8_value_bytes
                        + string_overhead_bytes
                    )

                else:
                    raise ValueError(
                        f"Unsupported BigQuery type: "
                        f"{bq_type}"
                    )

        monthly_row_counts[
            file_path.name
        ] = monthly_rows

        print(
            f"{file_path.name}: "
            f"{monthly_rows:,} rows",
            flush=True,
        )

    output_rows = []

    for column in column_names:
        result = statistics[column]

        estimated_bytes = int(
            result["estimated_logical_bytes"]
        )

        output_rows.append(
            {
                "column_name": column,
                "bq_type": result["bq_type"],
                "mode": result["mode"],
                "row_count": result["row_count"],
                "non_null_count": (
                    result["non_null_count"]
                ),
                "null_count": result["null_count"],
                "null_percentage": round(
                    result["null_count"]
                    / result["row_count"]
                    * 100,
                    4,
                ),
                "value_bytes": result["value_bytes"],
                "overhead_bytes": (
                    result["overhead_bytes"]
                ),
                "estimated_logical_bytes": (
                    estimated_bytes
                ),
                "estimated_logical_mb": round(
                    estimated_bytes
                    / 1_000_000,
                    2,
                ),
                "estimated_logical_mib": round(
                    estimated_bytes
                    / (1024 ** 2),
                    2,
                ),
            }
        )

    output_df = pd.DataFrame(
        output_rows
    )

    total_estimated_bytes = int(
        output_df[
            "estimated_logical_bytes"
        ].sum()
    )

    projection_summary = json.loads(
        PROJECTION_SUMMARY_PATH.read_text(
            encoding="utf-8"
        )
    )

    expected_total_rows = int(
        projection_summary[
            "total_output_rows"
        ]
    )

    utilization_percentage = round(
        total_estimated_bytes
        / CONSERVATIVE_LIMIT_BYTES
        * 100,
        2,
    )

    remaining_bytes = (
        CONSERVATIVE_LIMIT_BYTES
        - total_estimated_bytes
    )

    summary = {
        "scope": (
            "Estimated active logical storage for "
            "the 2025 landing table only"
        ),
        "input_file_count": len(input_files),
        "expected_total_rows": (
            expected_total_rows
        ),
        "estimated_total_rows": total_rows,
        "row_count_reconciled": (
            total_rows == expected_total_rows
        ),
        "schema_field_count": len(schema),
        "estimated_logical_bytes": (
            total_estimated_bytes
        ),
        "estimated_logical_gb_decimal": round(
            total_estimated_bytes
            / 1_000_000_000,
            4,
        ),
        "estimated_logical_gib": round(
            total_estimated_bytes
            / (1024 ** 3),
            4,
        ),
        "conservative_limit_bytes": (
            CONSERVATIVE_LIMIT_BYTES
        ),
        "utilization_of_conservative_10_gb_percent": (
            utilization_percentage
        ),
        "remaining_bytes_under_conservative_limit": (
            remaining_bytes
        ),
        "remaining_gb_under_conservative_limit": round(
            remaining_bytes
            / 1_000_000_000,
            4,
        ),
        "within_conservative_10_gb_limit": (
            total_estimated_bytes
            < CONSERVATIVE_LIMIT_BYTES
        ),
        "monthly_row_counts": monthly_row_counts,
        "column_estimate_output": str(
            COLUMN_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
        "important_note": (
            "This is a local estimate of logical "
            "storage for the landing table. Actual "
            "BigQuery storage must be measured after "
            "loading, and additional dbt tables would "
            "consume separate storage."
        ),
    }

    COLUMN_OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_df.to_csv(
        COLUMN_OUTPUT,
        index=False,
        encoding="utf-8",
    )

    SUMMARY_OUTPUT.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(
        "2025 logical-storage estimate completed."
    )
    print(
        f"Rows estimated: {total_rows:,}"
    )
    print(
        "Row count reconciled: "
        f"{summary['row_count_reconciled']}"
    )
    print(
        "Estimated logical size: "
        f"{summary['estimated_logical_gb_decimal']} GB"
    )
    print(
        "Estimated logical size: "
        f"{summary['estimated_logical_gib']} GiB"
    )
    print(
        "Utilization of conservative 10 GB limit: "
        f"{utilization_percentage}%"
    )
    print(
        "Within conservative limit: "
        f"{summary['within_conservative_10_gb_limit']}"
    )
    print(
        f"Column output: {COLUMN_OUTPUT}"
    )
    print(
        f"Summary: {SUMMARY_OUTPUT}"
    )


if __name__ == "__main__":
    main()
