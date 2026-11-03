"""Download and register the next eligible official BTS 2026 ZIP.

No BigQuery, dbt or Airflow operations.
"""
import argparse
import csv
import hashlib
import io
import os
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
FIELDS = [
    "reporting_year", "reporting_month", "source_file",
    "source_sha256", "source_row_count"
]
MAX_DOWNLOAD = 120_000_000


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def zip_members(archive):
    """Select exactly one flight CSV; allow optional BTS readme."""
    members = [
        item for item in archive.infolist()
        if not item.is_dir()
    ]

    csv_files = [
        item for item in members
        if item.filename.lower().endswith(".csv")
    ]

    readmes = [
        item for item in members
        if item.filename.replace("\\", "/").split("/")[-1].lower()
        == "readme.html"
    ]

    require(
        len(csv_files) == 1,
        "Expected exactly one flight-data CSV in the ZIP"
    )

    require(
        len(readmes) <= 1,
        "Multiple BTS readme files found"
    )

    require(
        len(members) == len(csv_files) + len(readmes),
        "Unexpected additional file in BTS ZIP"
    )

    return csv_files[0]


def baseline_header(path):
    with zipfile.ZipFile(path) as archive:
        item = zip_members(archive)
        with archive.open(item) as binary:
            with io.TextIOWrapper(
                binary, encoding="utf-8-sig", newline=""
            ) as stream:
                return next(csv.reader(stream))


def validate_zip(path, year, month, expected_header):
    require(
        0 < path.stat().st_size <= MAX_DOWNLOAD,
        "Unexpected ZIP size"
    )

    with zipfile.ZipFile(path) as archive:
        item = zip_members(archive)

        require(
            0 < item.file_size < 1_500_000_000,
            "Unexpected decompressed CSV size"
        )

        count = 0

        with archive.open(item) as binary:
            with io.TextIOWrapper(
                binary, encoding="utf-8-sig", newline=""
            ) as stream:
                reader = csv.reader(stream)
                header = next(reader, None)

                require(
                    header == expected_header,
                    "CSV header differs from February baseline"
                )

                names = [
                    field.strip().lower() for field in header
                ]

                require(
                    all(field in names for field in (
                        "year", "month", "flightdate"
                    )),
                    "Required BTS period fields missing"
                )

                year_index = names.index("year")
                month_index = names.index("month")

                for row in reader:
                    count += 1

                    require(
                        len(row) == len(header),
                        f"Invalid CSV width at record {count}"
                    )

                    require(
                        row[year_index].strip() == str(year)
                        and row[month_index].strip() == str(month),
                        f"Incorrect reporting period at record {count}"
                    )

        require(count > 0, "Empty monthly source CSV")

    return count


def read_manifest(path):
    require(path.is_file(), "Historical source manifest missing")
    original = path.read_bytes()

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.DictReader(stream)
        require(
            reader.fieldnames == FIELDS,
            "Unexpected source manifest schema"
        )
        rows = list(reader)

    periods = [
        (row["reporting_year"], row["reporting_month"])
        for row in rows
    ]

    require(
        len(periods) == len(set(periods)),
        "Duplicate source manifest periods"
    )
    require(
        ("2026", "1") in periods and ("2026", "2") in periods,
        "Historical January-February provenance missing"
    )

    return original, rows


def save_manifest(path, original, rows):
    newline = "\r\n" if b"\r\n" in original else "\n"
    encoding = (
        "utf-8-sig"
        if original.startswith(b"\xef\xbb\xbf")
        else "utf-8"
    )

    with tempfile.NamedTemporaryFile(
        "w",
        encoding=encoding,
        newline="",
        dir=path.parent,
        prefix=".bts_manifest_",
        suffix=".tmp",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)

        try:
            writer = csv.DictWriter(
                stream,
                fieldnames=FIELDS,
                lineterminator=newline,
            )
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    try:
        require(
            path.read_bytes() == original,
            "Source manifest changed during acquisition"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Download and register the next official month",
    )
    args = parser.parse_args()

    sys.path.insert(0, str(ROOT / "scripts"))

    from run_2026_monthly_ingestion import select_next_month

    selection = select_next_month(ROOT)

    print("MONTH_SELECTION_STATUS:", selection["status"], flush=True)
    print("NEXT_MONTH:", selection["month"], flush=True)

    if selection["status"] != "WAITING_FOR_PREPARED_SOURCE":
        print("NO_SOURCE_FETCH_NEEDED")
        return

    year, month = map(int, selection["month"].split("-"))

    require(
        year == 2026 and 3 <= month <= 12,
        "Historical source month blocked"
    )

    filename = f"{PREFIX}_{year}_{month}.zip"
    url = f"https://transtats.bts.gov/PREZIP/{filename}"

    print("OFFICIAL_SOURCE_URL:", url, flush=True)

    if not args.fetch:
        print("PREVIEW_ONLY_NO_DOWNLOAD")
        return

    manifest = (
        ROOT / "evidence/source/2026_monthly_source_manifest.csv"
    )

    original, rows = read_manifest(manifest)

    previous = next(
        row for row in rows
        if row["reporting_year"] == "2026"
        and row["reporting_month"] == "2"
    )

    previous_zip = (
        ROOT / "data/raw/2026" / previous["source_file"]
    )

    require(previous_zip.is_file(), "February baseline ZIP missing")
    require(
        sha256_file(previous_zip)
        == previous["source_sha256"].upper(),
        "February baseline SHA256 mismatch"
    )

    expected_header = baseline_header(previous_zip)

    destination = ROOT / "data/raw/2026" / filename

    registered = [
        row for row in rows
        if row["reporting_year"] == str(year)
        and row["reporting_month"] == str(month)
    ]

    require(
        len(registered) <= 1,
        "Duplicate source month records"
    )

    if registered:
        require(
            destination.is_file(),
            "Previously registered source ZIP is missing"
        )
        require(
            sha256_file(destination)
            == registered[0]["source_sha256"].upper(),
            "Registered source checksum mismatch"
        )
        print("SOURCE_ALREADY_REGISTERED_PASS")
        print("NO_DOWNLOAD_OR_MANIFEST_CHANGE")
        return

    require(
        not destination.exists(),
        "Unregistered source ZIP exists; manual review required"
    )

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None

    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=destination.parent,
            prefix=".bts_fetch_",
            suffix=".part",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)

            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (BTS portfolio research)"
                },
            )

            try:
                with urllib.request.urlopen(
                    request, timeout=90
                ) as response:
                    final_url = urlparse(response.geturl())

                    require(
                        final_url.scheme == "https"
                        and final_url.hostname in (
                            "transtats.bts.gov",
                            "www.transtats.bts.gov",
                        ),
                        "Unexpected source redirect"
                    )

                    require(
                        response.status == 200,
                        "Unexpected HTTP response"
                    )

                    size = 0

                    while True:
                        chunk = response.read(1024 * 1024)

                        if not chunk:
                            break

                        size += len(chunk)

                        require(
                            size <= MAX_DOWNLOAD,
                            "Download exceeds 120 MB safety limit"
                        )

                        stream.write(chunk)

            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    print("OFFICIAL_SOURCE_NOT_YET_RELEASED")
                    return

                raise RuntimeError(
                    f"Official BTS request failed: HTTP {exc.code}"
                ) from exc

        print("DOWNLOADED_BYTES:", temporary.stat().st_size, flush=True)

        count = validate_zip(
            temporary, year, month, expected_header
        )

        checksum = sha256_file(temporary)

        print("CSV_HEADER_AND_REPORTING_PERIOD_PASS")
        print("SOURCE_RECORDS:", count)
        print("SOURCE_SHA256:", checksum)

        os.rename(temporary, destination)
        temporary = None

        rows.append({
            "reporting_year": str(year),
            "reporting_month": str(month),
            "source_file": filename,
            "source_sha256": checksum,
            "source_row_count": str(count),
        })

        save_manifest(manifest, original, rows)

        print("OFFICIAL_SOURCE_ZIP_SAVED:", destination)
        print("SOURCE_MANIFEST_UPDATED:", manifest)
        print("NO_BIGQUERY_API_CALLS")
        print("NO_BIGQUERY_LOAD_JOB")
        print("NO_DBT_EXECUTION")
        print("NO_AIRFLOW_TRIGGER")
        print("B3_51CJ_SOURCE_ACQUISITION_PASS")

    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
