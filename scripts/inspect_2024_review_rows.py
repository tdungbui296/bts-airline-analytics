from pathlib import Path
from zipfile import ZipFile

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "2024"
OUTPUT_DIR = PROJECT_ROOT / "analysis" / "profiling"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

KEY_COLUMNS = [
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

CONTEXT_COLUMNS = [
    "FlightDate",
    "Reporting_Airline",
    "DOT_ID_Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "Origin",
    "DestAirportID",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDel15",
    "Cancelled",
    "Diverted",
    *DELAY_CAUSE_COLUMNS,
]

USE_COLUMNS = list(dict.fromkeys(CONTEXT_COLUMNS + KEY_COLUMNS))


def blank_mask(frame: pd.DataFrame) -> pd.DataFrame:
    """Return True where a string value is null or blank."""
    string_frame = frame.astype("string")

    return string_frame.apply(
        lambda column: (
            column.isna()
            | column.str.strip().eq("")
        )
    )


def read_month(month: int) -> tuple[pd.DataFrame, Path]:
    pattern = (
        "On_Time_Reporting_Carrier_On_Time_Performance_"
        f"1987_present_2024_{month}.zip"
    )

    zip_path = RAW_DIR / pattern

    if not zip_path.exists():
        raise FileNotFoundError(
            f"Source ZIP not found: {zip_path}"
        )

    with ZipFile(zip_path) as archive:
        csv_members = [
            member
            for member in archive.namelist()
            if member.lower().endswith(".csv")
        ]

        if len(csv_members) != 1:
            raise ValueError(
                f"Expected one CSV in {zip_path.name}; "
                f"found {len(csv_members)}."
            )

        with archive.open(csv_members[0]) as csv_file:
            frame = pd.read_csv(
                csv_file,
                usecols=USE_COLUMNS,
                dtype="string",
                low_memory=False,
            )

    frame["_source_file"] = zip_path.name
    frame["_source_row_number"] = frame.index + 2

    return frame, zip_path


# July: arrival-delay flag is 1, but every cause field is blank.
july, july_path = read_month(7)

july_cause_blank = blank_mask(
    july[DELAY_CAUSE_COLUMNS]
).all(axis=1)

july_arr_del_15 = pd.to_numeric(
    july["ArrDel15"],
    errors="coerce",
).eq(1)

july_review = july.loc[
    july_arr_del_15 & july_cause_blank
].copy()

july_review.insert(
    0,
    "review_reason",
    "arrival_delayed_15_but_all_delay_causes_missing",
)

# August: one or more candidate-key fields are blank.
august, august_path = read_month(8)

august_key_blank = blank_mask(
    august[KEY_COLUMNS]
)

august_review_mask = august_key_blank.any(axis=1)

august_review = august.loc[
    august_review_mask
].copy()

august_review.insert(
    0,
    "missing_key_fields",
    august_key_blank.loc[
        august_review_mask
    ].apply(
        lambda row: " | ".join(
            row.index[row].tolist()
        ),
        axis=1,
    ),
)

august_review.insert(
    0,
    "review_reason",
    "candidate_key_component_missing",
)

july_output = (
    OUTPUT_DIR
    / "2024_07_missing_delay_cause_rows.csv"
)

august_output = (
    OUTPUT_DIR
    / "2024_08_candidate_key_null_rows.csv"
)

combined_output = (
    OUTPUT_DIR
    / "2024_review_anomaly_rows.csv"
)

july_review.to_csv(
    july_output,
    index=False,
)

august_review.to_csv(
    august_output,
    index=False,
)

pd.concat(
    [july_review, august_review],
    ignore_index=True,
    sort=False,
).to_csv(
    combined_output,
    index=False,
)

print(f"July source: {july_path.name}")
print(f"July review rows: {len(july_review):,}")
print()
print(f"August source: {august_path.name}")
print(f"August review rows: {len(august_review):,}")

if not august_review.empty:
    print()
    print("Missing August key fields:")
    print(
        august_review[
            [
                "_source_row_number",
                "missing_key_fields",
            ]
        ].to_string(index=False)
    )

print()
print(f"Combined output: {combined_output}")