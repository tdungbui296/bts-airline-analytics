from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2025"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2025_type_anomaly_rows.csv"
)

COLUMNS = [
    "Year",
    "Month",
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "CRSArrTime",
    "Cancelled",
    "Diverted",
    "DivAirportLandings",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
]


def main() -> None:
    files = sorted(
        INPUT_DIR.glob("bts_2025_*_core.csv.gz")
    )

    if len(files) != 12:
        raise ValueError(
            f"Expected 12 files; found {len(files)}."
        )

    anomaly_frames = []

    for file_path in files:
        df = pd.read_csv(
            file_path,
            compression="gzip",
            usecols=COLUMNS,
            dtype="string",
            keep_default_na=True,
        )

        crs_elapsed_numeric = pd.to_numeric(
            df["CRSElapsedTime"],
            errors="coerce",
        )

        anomaly_mask = (
            df["DivAirportLandings"].isna()
            | crs_elapsed_numeric.lt(0)
        )

        anomalies = df.loc[
            anomaly_mask,
            COLUMNS,
        ].copy()

        if not anomalies.empty:
            anomalies.insert(
                0,
                "processed_file",
                file_path.name,
            )

            anomaly_frames.append(anomalies)

    if not anomaly_frames:
        print("No anomaly rows found.")
        return

    result = pd.concat(
        anomaly_frames,
        ignore_index=True,
    )

    result.to_csv(
        OUTPUT_PATH,
        index=False,
        encoding="utf-8",
    )

    print(result.to_string(index=False))
    print()
    print(f"Anomaly rows found: {len(result)}")
    print(f"Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
