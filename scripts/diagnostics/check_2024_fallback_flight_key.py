from pathlib import Path
from zipfile import ZipFile

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "2024"

COLUMNS = [
    "FlightDate",
    "DOT_ID_Reporting_Airline",
    "Tail_Number",
    "OriginAirportID",
    "DestAirportID",
    "CRSDepTime",
    "CRSArrTime",
    "Flight_Number_Reporting_Airline",
]


frames = []

for zip_path in sorted(RAW_DIR.glob("*.zip")):
    with ZipFile(zip_path) as archive:
        csv_members = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv")
        ]

        if len(csv_members) != 1:
            raise ValueError(
                f"{zip_path.name}: expected one CSV, "
                f"found {len(csv_members)}."
            )

        with archive.open(csv_members[0]) as csv_file:
            frame = pd.read_csv(
                csv_file,
                usecols=COLUMNS,
                dtype="string",
                encoding="utf-8-sig",
                low_memory=False,
            )

        frame["_source_file"] = zip_path.name
        frame["_source_row_number"] = frame.index + 2
        frames.append(frame)

flights = pd.concat(
    frames,
    ignore_index=True,
)

missing_flight_number = flights[
    flights["Flight_Number_Reporting_Airline"].isna()
    | flights["Flight_Number_Reporting_Airline"]
        .str.strip()
        .eq("")
].copy()

fallback_columns = [
    "FlightDate",
    "DOT_ID_Reporting_Airline",
    "Tail_Number",
    "OriginAirportID",
    "DestAirportID",
    "CRSDepTime",
    "CRSArrTime",
]

missing_fallback_components = (
    missing_flight_number[fallback_columns]
    .isna()
    .any(axis=1)
)

duplicate_fallback_rows = (
    flights
    .duplicated(
        subset=fallback_columns,
        keep=False,
    )
)

matching_duplicate_rows = flights[
    duplicate_fallback_rows
].merge(
    missing_flight_number[fallback_columns]
        .drop_duplicates(),
    on=fallback_columns,
    how="inner",
)

print(
    "Rows missing reporting flight number:",
    len(missing_flight_number),
)

print(
    "Missing fallback-component rows:",
    int(missing_fallback_components.sum()),
)

print(
    "Rows sharing the exceptional fallback identity:",
    len(matching_duplicate_rows),
)

print()
print("Exceptional source row:")
print(
    missing_flight_number[
        [
            "_source_file",
            "_source_row_number",
            *fallback_columns,
        ]
    ].to_string(index=False)
)

if len(matching_duplicate_rows) > 1:
    print()
    print("WARNING: fallback identity is not unique.")
    print(
        matching_duplicate_rows[
            [
                "_source_file",
                "_source_row_number",
                *fallback_columns,
            ]
        ].to_string(index=False)
    )