from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_CSV = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "pilot"
    / "bts_2025_01_core.csv"
)

COLUMN_MANIFEST = (
    PROJECT_ROOT
    / "config"
    / "bts_core_columns.txt"
)

PROJECTION_SUMMARY = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "january_2025_projection_summary.json"
)

COLUMN_PROFILE_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "january_2025_column_profile.csv"
)

CHECK_SUMMARY_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "january_2025_basic_profile_summary.json"
)

STATUS_COUNTS_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "january_2025_status_value_counts.csv"
)

CHUNK_SIZE = 100_000

TECHNICAL_COLUMNS = [
    "_source_file_name"
]

CANDIDATE_KEY = [
    "FlightDate",
    "DOT_ID_Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "DestAirportID",
    "CRSDepTime",
]

STATUS_COLUMNS = [
    "Cancelled",
    "Diverted",
    "DepDel15",
    "ArrDel15",
    "CancellationCode",
    "DivAirportLandings",
]


def load_manifest() -> list[str]:
    """Load the approved 51-column source manifest."""
    columns = [
        line.strip()
        for line in COLUMN_MANIFEST.read_text(
            encoding="ascii"
        ).splitlines()
        if line.strip()
    ]

    if len(columns) != 51:
        raise ValueError(
            f"Expected 51 manifest columns; found {len(columns)}."
        )

    if len(columns) != len(set(columns)):
        raise ValueError(
            "The column manifest contains duplicate names."
        )

    return columns


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Pilot CSV not found: {INPUT_CSV}"
        )

    if not COLUMN_MANIFEST.exists():
        raise FileNotFoundError(
            f"Column manifest not found: {COLUMN_MANIFEST}"
        )

    if not PROJECTION_SUMMARY.exists():
        raise FileNotFoundError(
            f"Projection summary not found: {PROJECTION_SUMMARY}"
        )

    core_columns = load_manifest()
    expected_columns = core_columns + TECHNICAL_COLUMNS

    actual_columns = pd.read_csv(
        INPUT_CSV,
        nrows=0
    ).columns.tolist()

    missing_columns = [
        column
        for column in expected_columns
        if column not in actual_columns
    ]

    unexpected_columns = [
        column
        for column in actual_columns
        if column not in expected_columns
    ]

    schema_matches_exactly = (
        actual_columns == expected_columns
    )

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )

    if unexpected_columns:
        raise ValueError(
            f"Unexpected columns found: {unexpected_columns}"
        )

    null_counts = {
        column: 0
        for column in expected_columns
    }

    distinct_values = {
        column: set()
        for column in expected_columns
    }

    sample_values = {
        column: []
        for column in expected_columns
    }

    status_value_counts = {
        column: Counter()
        for column in STATUS_COLUMNS
    }

    row_count = 0
    flight_date_parse_failures = 0
    year_parse_failures = 0
    month_parse_failures = 0
    year_date_mismatches = 0
    month_date_mismatches = 0

    reader = pd.read_csv(
        INPUT_CSV,
        dtype="string",
        chunksize=CHUNK_SIZE,
        keep_default_na=True
    )

    for chunk_number, chunk in enumerate(
        reader,
        start=1
    ):
        row_count += len(chunk)

        for column in expected_columns:
            series = chunk[column]

            null_counts[column] += int(
                series.isna().sum()
            )

            unique_non_null = (
                series
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            distinct_values[column].update(
                unique_non_null
            )

            for value in unique_non_null:
                if (
                    len(sample_values[column]) < 5
                    and value not in sample_values[column]
                ):
                    sample_values[column].append(value)

        for column in STATUS_COLUMNS:
            status_value_counts[column].update(
                chunk[column]
                .dropna()
                .astype(str)
                .tolist()
            )

        flight_dates = pd.to_datetime(
            chunk["FlightDate"],
            errors="coerce",
            format="mixed"
        )

        years = pd.to_numeric(
            chunk["Year"],
            errors="coerce"
        )

        months = pd.to_numeric(
            chunk["Month"],
            errors="coerce"
        )

        flight_date_parse_failures += int(
            flight_dates.isna().sum()
        )

        year_parse_failures += int(
            years.isna().sum()
        )

        month_parse_failures += int(
            months.isna().sum()
        )

        valid_year_comparison = (
            flight_dates.notna()
            & years.notna()
        )

        valid_month_comparison = (
            flight_dates.notna()
            & months.notna()
        )

        year_date_mismatches += int(
            (
                years[valid_year_comparison]
                != flight_dates[
                    valid_year_comparison
                ].dt.year
            ).sum()
        )

        month_date_mismatches += int(
            (
                months[valid_month_comparison]
                != flight_dates[
                    valid_month_comparison
                ].dt.month
            ).sum()
        )

        print(
            f"Chunk {chunk_number}: "
            f"processed {row_count:,} rows",
            flush=True
        )

    column_profile_rows = []

    for column in expected_columns:
        null_count = null_counts[column]
        non_null_count = row_count - null_count

        column_profile_rows.append(
            {
                "column_name": column,
                "null_count": null_count,
                "null_percentage": round(
                    null_count / row_count * 100,
                    4
                ),
                "non_null_count": non_null_count,
                "distinct_non_null_count": len(
                    distinct_values[column]
                ),
                "sample_values": " | ".join(
                    sample_values[column]
                ),
            }
        )

    column_profile = pd.DataFrame(
        column_profile_rows
    )

    column_profile.to_csv(
        COLUMN_PROFILE_OUTPUT,
        index=False,
        encoding="utf-8"
    )

    print(
        "Checking the provisional candidate key...",
        flush=True
    )

    key_data = pd.read_csv(
        INPUT_CSV,
        usecols=CANDIDATE_KEY,
        dtype="string",
        keep_default_na=True
    )

    candidate_key_null_mask = (
        key_data[CANDIDATE_KEY]
        .isna()
        .any(axis=1)
    )

    candidate_key_null_rows = int(
        candidate_key_null_mask.sum()
    )

    complete_keys = key_data.loc[
        ~candidate_key_null_mask,
        CANDIDATE_KEY
    ]

    duplicate_key_mask = complete_keys.duplicated(
        subset=CANDIDATE_KEY,
        keep=False
    )

    duplicate_candidate_key_rows = int(
        duplicate_key_mask.sum()
    )

    duplicate_candidate_key_groups = int(
        complete_keys.loc[
            duplicate_key_mask,
            CANDIDATE_KEY
        ]
        .drop_duplicates()
        .shape[0]
    )

    projection_summary = json.loads(
        PROJECTION_SUMMARY.read_text(
            encoding="utf-8"
        )
    )

    expected_row_count = int(
        projection_summary["rows_written"]
    )

    status_rows = []

    for column, counter in status_value_counts.items():
        for value, count in sorted(
            counter.items(),
            key=lambda item: (
                -item[1],
                item[0]
            )
        ):
            status_rows.append(
                {
                    "column_name": column,
                    "observed_value": value,
                    "row_count": count,
                    "percentage_of_all_rows": round(
                        count / row_count * 100,
                        4
                    ),
                }
            )

    pd.DataFrame(status_rows).to_csv(
        STATUS_COUNTS_OUTPUT,
        index=False,
        encoding="utf-8"
    )

    summary = {
        "input_file": str(
            INPUT_CSV.relative_to(PROJECT_ROOT)
        ),
        "expected_row_count_from_projection": (
            expected_row_count
        ),
        "profiled_row_count": row_count,
        "row_count_reconciled": (
            row_count == expected_row_count
        ),
        "expected_column_count": len(
            expected_columns
        ),
        "actual_column_count": len(
            actual_columns
        ),
        "schema_matches_exactly": (
            schema_matches_exactly
        ),
        "missing_columns": missing_columns,
        "unexpected_columns": unexpected_columns,
        "candidate_key_columns": CANDIDATE_KEY,
        "candidate_key_null_rows": (
            candidate_key_null_rows
        ),
        "duplicate_candidate_key_rows": (
            duplicate_candidate_key_rows
        ),
        "duplicate_candidate_key_groups": (
            duplicate_candidate_key_groups
        ),
        "flight_date_parse_failures": (
            flight_date_parse_failures
        ),
        "year_parse_failures": (
            year_parse_failures
        ),
        "month_parse_failures": (
            month_parse_failures
        ),
        "year_date_mismatches": (
            year_date_mismatches
        ),
        "month_date_mismatches": (
            month_date_mismatches
        ),
        "column_profile_output": str(
            COLUMN_PROFILE_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
        "status_counts_output": str(
            STATUS_COUNTS_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
    }

    CHECK_SUMMARY_OUTPUT.write_text(
        json.dumps(
            summary,
            indent=2
        ),
        encoding="utf-8"
    )

    print()
    print("Basic profiling completed successfully.")
    print(f"Rows profiled: {row_count:,}")
    print(
        "Row count reconciled: "
        f"{row_count == expected_row_count}"
    )
    print(
        "Schema matches exactly: "
        f"{schema_matches_exactly}"
    )
    print(
        "Candidate-key null rows: "
        f"{candidate_key_null_rows:,}"
    )
    print(
        "Duplicate candidate-key rows: "
        f"{duplicate_candidate_key_rows:,}"
    )
    print(
        "Duplicate candidate-key groups: "
        f"{duplicate_candidate_key_groups:,}"
    )
    print(
        "FlightDate parse failures: "
        f"{flight_date_parse_failures:,}"
    )
    print(
        "Year/date mismatches: "
        f"{year_date_mismatches:,}"
    )
    print(
        "Month/date mismatches: "
        f"{month_date_mismatches:,}"
    )
    print(f"Summary: {CHECK_SUMMARY_OUTPUT}")


if __name__ == "__main__":
    main()
