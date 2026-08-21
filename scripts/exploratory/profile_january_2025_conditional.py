from __future__ import annotations

import json
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

OUTPUT_DIR = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
)

SUMMARY_OUTPUT = (
    OUTPUT_DIR
    / "january_2025_conditional_profile_summary.json"
)

DIVERSION_OUTPUT = (
    OUTPUT_DIR
    / "january_2025_diversion_landing_crosstab.csv"
)

OPERATIONAL_NULL_OUTPUT = (
    OUTPUT_DIR
    / "january_2025_operational_nulls_by_status.csv"
)

DELAY_CAUSE_OUTPUT = (
    OUTPUT_DIR
    / "january_2025_delay_cause_population.csv"
)


DELAY_CAUSE_COLUMNS = [
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

OPERATIONAL_COLUMNS = [
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "DepartureDelayGroups",
    "TaxiOut",
    "WheelsOff",
    "WheelsOn",
    "TaxiIn",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "ArrivalDelayGroups",
    "ActualElapsedTime",
    "AirTime",
]

REQUIRED_COLUMNS = [
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "DivAirportLandings",
    *DELAY_CAUSE_COLUMNS,
    *OPERATIONAL_COLUMNS,
]


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Input CSV not found: {INPUT_CSV}"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df = pd.read_csv(
        INPUT_CSV,
        usecols=REQUIRED_COLUMNS,
        dtype="string",
        keep_default_na=True,
    )

    cancelled = pd.to_numeric(
        df["Cancelled"],
        errors="coerce"
    )

    diverted = pd.to_numeric(
        df["Diverted"],
        errors="coerce"
    )

    arr_del_15 = pd.to_numeric(
        df["ArrDel15"],
        errors="coerce"
    )

    both_flags = (
        cancelled.eq(1)
        & diverted.eq(1)
    )

    cancelled_only = (
        cancelled.eq(1)
        & diverted.eq(0)
    )

    diverted_only = (
        cancelled.eq(0)
        & diverted.eq(1)
    )

    completed = (
        cancelled.eq(0)
        & diverted.eq(0)
    )

    df["flight_status"] = "unknown"
    df.loc[completed, "flight_status"] = "completed"
    df.loc[cancelled_only, "flight_status"] = "cancelled"
    df.loc[diverted_only, "flight_status"] = "diverted"
    df.loc[
        both_flags,
        "flight_status"
    ] = "cancelled_and_diverted"

    cancellation_code_present = (
        df["CancellationCode"].notna()
    )

    cancellation_relationship = {
        "cancelled_rows": int(
            cancelled.eq(1).sum()
        ),
        "cancelled_with_code": int(
            (
                cancelled.eq(1)
                & cancellation_code_present
            ).sum()
        ),
        "cancelled_without_code": int(
            (
                cancelled.eq(1)
                & ~cancellation_code_present
            ).sum()
        ),
        "non_cancelled_with_code": int(
            (
                cancelled.eq(0)
                & cancellation_code_present
            ).sum()
        ),
        "non_cancelled_without_code": int(
            (
                cancelled.eq(0)
                & ~cancellation_code_present
            ).sum()
        ),
    }

    diversion_crosstab = pd.crosstab(
        index=df["Diverted"].fillna("<NULL>"),
        columns=df[
            "DivAirportLandings"
        ].fillna("<NULL>"),
        dropna=False,
    )

    diversion_crosstab.index.name = "Diverted"
    diversion_crosstab.columns.name = (
        "DivAirportLandings"
    )

    diversion_crosstab.reset_index().to_csv(
        DIVERSION_OUTPUT,
        index=False,
        encoding="utf-8",
    )

    operational_rows = []

    status_order = [
        "completed",
        "cancelled",
        "diverted",
        "cancelled_and_diverted",
        "unknown",
    ]

    for status in status_order:
        status_mask = df["flight_status"].eq(
            status
        )

        status_count = int(
            status_mask.sum()
        )

        if status_count == 0:
            continue

        for column in OPERATIONAL_COLUMNS:
            null_count = int(
                df.loc[
                    status_mask,
                    column
                ].isna().sum()
            )

            operational_rows.append(
                {
                    "flight_status": status,
                    "column_name": column,
                    "status_row_count": status_count,
                    "null_count": null_count,
                    "non_null_count": (
                        status_count - null_count
                    ),
                    "null_percentage": round(
                        null_count
                        / status_count
                        * 100,
                        4,
                    ),
                }
            )

    pd.DataFrame(
        operational_rows
    ).to_csv(
        OPERATIONAL_NULL_OUTPUT,
        index=False,
        encoding="utf-8",
    )

    delay_cause_rows = []

    arr_del_15_label = (
        df["ArrDel15"]
        .fillna("<NULL>")
    )

    for cause_column in DELAY_CAUSE_COLUMNS:
        for delay_status in [
            "0.00",
            "1.00",
            "<NULL>",
        ]:
            status_mask = arr_del_15_label.eq(
                delay_status
            )

            status_count = int(
                status_mask.sum()
            )

            cause_non_null_count = int(
                df.loc[
                    status_mask,
                    cause_column
                ].notna().sum()
            )

            delay_cause_rows.append(
                {
                    "arr_del_15_value": delay_status,
                    "delay_cause_column": cause_column,
                    "status_row_count": status_count,
                    "cause_non_null_count": (
                        cause_non_null_count
                    ),
                    "cause_null_count": (
                        status_count
                        - cause_non_null_count
                    ),
                }
            )

    pd.DataFrame(
        delay_cause_rows
    ).to_csv(
        DELAY_CAUSE_OUTPUT,
        index=False,
        encoding="utf-8",
    )

    summary = {
        "row_count": len(df),
        "flight_status_counts": {
            str(key): int(value)
            for key, value in (
                df["flight_status"]
                .value_counts(
                    dropna=False
                )
                .items()
            )
        },
        "cancelled_and_diverted_rows": int(
            both_flags.sum()
        ),
        "cancellation_code_relationship": (
            cancellation_relationship
        ),
        "diversion_crosstab_output": str(
            DIVERSION_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
        "operational_null_output": str(
            OPERATIONAL_NULL_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
        "delay_cause_output": str(
            DELAY_CAUSE_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
    }

    SUMMARY_OUTPUT.write_text(
        json.dumps(
            summary,
            indent=2
        ),
        encoding="utf-8",
    )

    print(
        "Conditional profiling completed successfully."
    )
    print()
    print("Flight-status counts:")
    print(
        df["flight_status"].value_counts()
    )
    print()
    print("Cancellation-code relationship:")
    for key, value in (
        cancellation_relationship.items()
    ):
        print(f"{key}: {value:,}")
    print()
    print(
        f"Summary: {SUMMARY_OUTPUT}"
    )


if __name__ == "__main__":
    main()
