from __future__ import annotations

import csv
import json
import re
import zipfile
from io import TextIOWrapper
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2025"
)

COLUMN_MANIFEST = (
    PROJECT_ROOT
    / "config"
    / "bts_core_columns.txt"
)

OUTPUT_CSV = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_monthly_validation_summary.csv"
)

OUTPUT_JSON = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_batch_validation_summary.json"
)

CHUNK_SIZE = 100_000

CANDIDATE_KEY = [
    "FlightDate",
    "DOT_ID_Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "DestAirportID",
    "CRSDepTime",
]

DELAY_CAUSE_COLUMNS = [
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

VALIDATION_COLUMNS = list(
    dict.fromkeys(
        [
            "Year",
            "Month",
            "FlightDate",
            "Cancelled",
            "CancellationCode",
            "Diverted",
            "DivAirportLandings",
            "ArrDel15",
            *CANDIDATE_KEY,
            *DELAY_CAUSE_COLUMNS,
        ]
    )
)


def load_manifest() -> list[str]:
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


def extract_month(file_path: Path) -> int:
    match = re.search(
        r"_2025_(\d+)\.zip$",
        file_path.name
    )

    if not match:
        raise ValueError(
            f"Could not extract month from {file_path.name}"
        )

    return int(match.group(1))


def validate_file(
    file_path: Path,
    required_columns: list[str],
) -> dict[str, object]:

    expected_month = extract_month(file_path)

    result: dict[str, object] = {
        "reporting_year": 2025,
        "reporting_month": expected_month,
        "source_file": file_path.name,
        "zip_ok": False,
        "csv_member_count": 0,
        "source_official_column_count": 0,
        "blank_header_count": 0,
        "missing_required_columns": "",
        "duplicate_source_headers": "",
        "row_count": 0,
        "flight_date_parse_failures": 0,
        "year_mismatch_rows": 0,
        "month_mismatch_rows": 0,
        "candidate_key_null_rows": 0,
        "duplicate_key_occurrences": 0,
        "cancelled_and_diverted_rows": 0,
        "cancelled_without_code_rows": 0,
        "non_cancelled_with_code_rows": 0,
        "delayed_missing_cause_rows": 0,
        "ineligible_with_cause_rows": 0,
        "div_airport_landing_values": "",
        "review_required": True,
        "error_message": "",
    }

    try:
        with zipfile.ZipFile(file_path, "r") as archive:
            result["zip_ok"] = True

            csv_members = [
                member
                for member in archive.namelist()
                if member.lower().endswith(".csv")
                and not member.endswith("/")
            ]

            result["csv_member_count"] = len(csv_members)

            if len(csv_members) != 1:
                raise ValueError(
                    "Expected exactly one CSV in the ZIP; "
                    f"found {len(csv_members)}."
                )

            csv_member = csv_members[0]

            with archive.open(csv_member, "r") as raw_file:
                with TextIOWrapper(
                    raw_file,
                    encoding="utf-8-sig",
                    newline=""
                ) as text_file:
                    header_reader = csv.reader(text_file)
                    source_headers = next(header_reader)

            non_blank_headers = [
                header
                for header in source_headers
                if header.strip()
            ]

            blank_header_count = (
                len(source_headers)
                - len(non_blank_headers)
            )

            duplicate_headers = sorted(
                {
                    header
                    for header in non_blank_headers
                    if non_blank_headers.count(header) > 1
                }
            )

            missing_columns = [
                column
                for column in required_columns
                if column not in non_blank_headers
            ]

            result["source_official_column_count"] = len(
                non_blank_headers
            )

            result["blank_header_count"] = (
                blank_header_count
            )

            result["missing_required_columns"] = (
                " | ".join(missing_columns)
            )

            result["duplicate_source_headers"] = (
                " | ".join(duplicate_headers)
            )

            if missing_columns:
                raise ValueError(
                    "Required columns are missing."
                )

            if duplicate_headers:
                raise ValueError(
                    "Duplicate source headers were found."
                )

            seen_key_hashes: set[int] = set()
            duplicate_key_hashes: set[int] = set()
            diversion_values: set[str] = set()

            with archive.open(csv_member, "r") as raw_file:
                with TextIOWrapper(
                    raw_file,
                    encoding="utf-8-sig",
                    newline=""
                ) as text_file:

                    reader = pd.read_csv(
                        text_file,
                        usecols=VALIDATION_COLUMNS,
                        dtype="string",
                        keep_default_na=True,
                        chunksize=CHUNK_SIZE,
                    )

                    for chunk in reader:
                        result["row_count"] += len(chunk)

                        flight_dates = pd.to_datetime(
                            chunk["FlightDate"],
                            errors="coerce",
                            format="mixed",
                        )

                        years = pd.to_numeric(
                            chunk["Year"],
                            errors="coerce",
                        )

                        months = pd.to_numeric(
                            chunk["Month"],
                            errors="coerce",
                        )

                        result[
                            "flight_date_parse_failures"
                        ] += int(
                            flight_dates.isna().sum()
                        )

                        valid_year_rows = (
                            flight_dates.notna()
                            & years.notna()
                        )

                        valid_month_rows = (
                            flight_dates.notna()
                            & months.notna()
                        )

                        result["year_mismatch_rows"] += int(
                            (
                                years[valid_year_rows]
                                != 2025
                            ).sum()
                        )

                        result["year_mismatch_rows"] += int(
                            (
                                years[valid_year_rows]
                                != flight_dates[
                                    valid_year_rows
                                ].dt.year
                            ).sum()
                        )

                        result["month_mismatch_rows"] += int(
                            (
                                months[valid_month_rows]
                                != expected_month
                            ).sum()
                        )

                        result["month_mismatch_rows"] += int(
                            (
                                months[valid_month_rows]
                                != flight_dates[
                                    valid_month_rows
                                ].dt.month
                            ).sum()
                        )

                        key_null_mask = (
                            chunk[CANDIDATE_KEY]
                            .isna()
                            .any(axis=1)
                        )

                        result[
                            "candidate_key_null_rows"
                        ] += int(
                            key_null_mask.sum()
                        )

                        complete_keys = chunk.loc[
                            ~key_null_mask,
                            CANDIDATE_KEY
                        ]

                        key_hashes = (
                            pd.util.hash_pandas_object(
                                complete_keys,
                                index=False,
                            )
                            .astype("uint64")
                            .tolist()
                        )

                        for key_hash in key_hashes:
                            key_hash_int = int(key_hash)

                            if key_hash_int in seen_key_hashes:
                                result[
                                    "duplicate_key_occurrences"
                                ] += 1

                                duplicate_key_hashes.add(
                                    key_hash_int
                                )
                            else:
                                seen_key_hashes.add(
                                    key_hash_int
                                )

                        cancelled = pd.to_numeric(
                            chunk["Cancelled"],
                            errors="coerce",
                        )

                        diverted = pd.to_numeric(
                            chunk["Diverted"],
                            errors="coerce",
                        )

                        arr_del_15 = pd.to_numeric(
                            chunk["ArrDel15"],
                            errors="coerce",
                        )

                        cancellation_code_present = (
                            chunk["CancellationCode"]
                            .notna()
                        )

                        result[
                            "cancelled_and_diverted_rows"
                        ] += int(
                            (
                                cancelled.eq(1)
                                & diverted.eq(1)
                            ).sum()
                        )

                        result[
                            "cancelled_without_code_rows"
                        ] += int(
                            (
                                cancelled.eq(1)
                                & ~cancellation_code_present
                            ).sum()
                        )

                        result[
                            "non_cancelled_with_code_rows"
                        ] += int(
                            (
                                cancelled.eq(0)
                                & cancellation_code_present
                            ).sum()
                        )

                        cause_data = chunk[
                            DELAY_CAUSE_COLUMNS
                        ]

                        eligible_for_causes = (
                            arr_del_15.eq(1)
                        )

                        any_cause_missing = (
                            cause_data
                            .isna()
                            .any(axis=1)
                        )

                        any_cause_present = (
                            cause_data
                            .notna()
                            .any(axis=1)
                        )

                        result[
                            "delayed_missing_cause_rows"
                        ] += int(
                            (
                                eligible_for_causes
                                & any_cause_missing
                            ).sum()
                        )

                        result[
                            "ineligible_with_cause_rows"
                        ] += int(
                            (
                                ~eligible_for_causes
                                & any_cause_present
                            ).sum()
                        )

                        diversion_values.update(
                            chunk[
                                "DivAirportLandings"
                            ]
                            .dropna()
                            .astype(str)
                            .unique()
                            .tolist()
                        )

            result["div_airport_landing_values"] = (
                " | ".join(
                    sorted(diversion_values)
                )
            )

            failure_fields = [
                "flight_date_parse_failures",
                "year_mismatch_rows",
                "month_mismatch_rows",
                "candidate_key_null_rows",
                "duplicate_key_occurrences",
                "cancelled_and_diverted_rows",
                "cancelled_without_code_rows",
                "non_cancelled_with_code_rows",
                "delayed_missing_cause_rows",
                "ineligible_with_cause_rows",
            ]

            result["review_required"] = any(
                int(result[field]) > 0
                for field in failure_fields
            )

    except Exception as error:
        result["error_message"] = str(error)
        result["review_required"] = True

    return result


def main() -> None:
    if not RAW_DIR.exists():
        raise FileNotFoundError(
            f"Raw directory not found: {RAW_DIR}"
        )

    required_columns = load_manifest()

    zip_files = sorted(
        RAW_DIR.glob(
            "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2025_*.zip"
        ),
        key=extract_month,
    )

    observed_months = [
        extract_month(file_path)
        for file_path in zip_files
    ]

    missing_months = [
        month
        for month in range(1, 13)
        if month not in observed_months
    ]

    if len(zip_files) != 12 or missing_months:
        raise ValueError(
            "The 2025 source batch is incomplete. "
            f"Files: {len(zip_files)}, "
            f"missing months: {missing_months}"
        )

    results = []

    for file_path in zip_files:
        month = extract_month(file_path)

        print(
            f"Validating 2025-{month:02d}...",
            flush=True
        )

        result = validate_file(
            file_path,
            required_columns,
        )

        results.append(result)

        status = (
            "REVIEW"
            if result["review_required"]
            else "PASS"
        )

        print(
            f"2025-{month:02d}: {status}, "
            f"rows={int(result['row_count']):,}",
            flush=True,
        )

    result_df = pd.DataFrame(results)

    OUTPUT_CSV.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_df.to_csv(
        OUTPUT_CSV,
        index=False,
        encoding="utf-8",
    )

    review_months = (
        result_df.loc[
            result_df["review_required"].eq(True),
            "reporting_month",
        ]
        .astype(int)
        .tolist()
    )

    summary = {
        "year": 2025,
        "monthly_file_count": len(zip_files),
        "missing_months": missing_months,
        "total_rows": int(
            result_df["row_count"].sum()
        ),
        "passed_month_count": int(
            result_df[
                "review_required"
            ].eq(False).sum()
        ),
        "review_month_count": int(
            result_df[
                "review_required"
            ].eq(True).sum()
        ),
        "review_months": review_months,
        "monthly_validation_output": str(
            OUTPUT_CSV.relative_to(PROJECT_ROOT)
        ),
    }

    OUTPUT_JSON.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print("2025 batch validation completed.")
    print(
        f"Total rows: {summary['total_rows']:,}"
    )
    print(
        "Passed months: "
        f"{summary['passed_month_count']}"
    )
    print(
        "Review months: "
        f"{summary['review_months']}"
    )
    print(f"CSV output: {OUTPUT_CSV}")
    print(f"JSON output: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
