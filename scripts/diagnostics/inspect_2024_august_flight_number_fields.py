from pathlib import Path
from zipfile import ZipFile

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ZIP_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "2024"
    / (
        "On_Time_Reporting_Carrier_On_Time_Performance_"
        "1987_present_2024_8.zip"
    )
)

SOURCE_ROW_NUMBER = 234965
DATAFRAME_INDEX = SOURCE_ROW_NUMBER - 2


with ZipFile(ZIP_PATH) as archive:
    csv_members = [
        name
        for name in archive.namelist()
        if name.lower().endswith(".csv")
    ]

    if len(csv_members) != 1:
        raise ValueError(
            f"Expected one CSV; found {len(csv_members)}."
        )

    csv_member = csv_members[0]

    with archive.open(csv_member) as csv_file:
        header = pd.read_csv(
            csv_file,
            nrows=0,
            encoding="utf-8-sig",
        )

    relevant_columns = [
        column
        for column in header.columns
        if (
            "Flight_Number" in column
            or column
            in {
                "Reporting_Airline",
                "DOT_ID_Reporting_Airline",
                "Marketing_Airline_Network",
                "Operating_Airline",
                "Tail_Number",
                "FlightDate",
                "Origin",
                "Dest",
                "CRSDepTime",
            }
        )
    ]

    with archive.open(csv_member) as csv_file:
        frame = pd.read_csv(
            csv_file,
            usecols=relevant_columns,
            dtype="string",
            encoding="utf-8-sig",
            low_memory=False,
        )

if DATAFRAME_INDEX >= len(frame):
    raise IndexError(
        f"Source row {SOURCE_ROW_NUMBER} is outside the file."
    )

record = frame.iloc[DATAFRAME_INDEX]

print(f"Source row number: {SOURCE_ROW_NUMBER}")
print()
print(record.to_string())