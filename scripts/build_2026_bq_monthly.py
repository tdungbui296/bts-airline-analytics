"""Create a verified, deterministic BigQuery load file for one 2026 BTS month.

Reuses the historical schema and normalization logic from build_bq_load_files.py.
Does not modify the historical 2024/2025 annual loader or its receipts.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import re
import shutil
from pathlib import Path

from build_bq_load_files import (
    PROJECT_ROOT,
    calculate_sha256,
    load_schema,
    normalize_month,
)


def read_single_manifest(path: Path, required: set[str]) -> dict[str, str]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError(f"Missing/duplicate manifest headers: {path}")
        if not required.issubset(reader.fieldnames):
            raise ValueError(f"Missing columns in {path.name}: {sorted(required - set(reader.fieldnames))}")
        rows = list(reader)
    if len(rows) != 1 or None in rows[0] or any(value is None for value in rows[0].values()):
        raise ValueError(f"Expected precisely one complete manifest row: {path}")
    return rows[0]


def expected_input(month: int) -> tuple[Path, int, str, dict[str, str]]:
    tag = f"2026_{month:02d}"
    manifest_path = PROJECT_ROOT / "evidence" / "source" / f"{tag}_processed_manifest.csv"
    manifest = read_single_manifest(manifest_path, {
        "reporting_year", "reporting_month", "source_file", "source_sha256",
        "source_row_count", "output_file", "output_row_count", "output_sha256",
        "output_column_count", "row_count_reconciled",
    })
    if (manifest["reporting_year"], manifest["reporting_month"]) != ("2026", str(month)):
        raise ValueError(f"Incorrect reporting period in {manifest_path.name}")
    if manifest["row_count_reconciled"].lower() != "true":
        raise ValueError(f"Unreconciled processed manifest: {manifest_path.name}")
    if int(manifest["output_column_count"]) != 52:
        raise ValueError("Processed output must have 52 columns")
    expected_rows = int(manifest["output_row_count"])
    if expected_rows <= 0 or expected_rows != int(manifest["source_row_count"]):
        raise ValueError("Processed/source row counts disagree")
    expected_name = f"bts_2026_{month:02d}_core.csv.gz"
    expected_file = PROJECT_ROOT / "data" / "processed" / "2026" / expected_name
    if (manifest["output_file"].replace("\\", "/") !=
            f"data/processed/2026/{expected_name}"):
        raise ValueError("Unexpected processed output path in manifest")
    if not expected_file.is_file():
        raise FileNotFoundError(expected_file)
    expected_hash = manifest["output_sha256"].upper()
    if not re.fullmatch(r"[0-9A-F]{64}", expected_hash):
        raise ValueError("Invalid processed SHA-256")
    if calculate_sha256(expected_file) != expected_hash:
        raise ValueError("Processed file does not match SHA-256 in manifest")

    # Reconcile with independently recorded original ZIP provenance.
    source_manifest = PROJECT_ROOT / "evidence" / "source" / "2026_monthly_source_manifest.csv"
    with source_manifest.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))
    matches = [r for r in rows if r.get("reporting_year") == "2026"
               and r.get("reporting_month") == str(month)]
    if len(matches) != 1:
        raise ValueError("Missing or repeated source receipt for requested month")
    source_row = matches[0]
    for field in ("source_file", "source_sha256", "source_row_count"):
        if source_row.get(field) != manifest[field]:
            raise ValueError(f"Source receipt mismatch: {field}")
    source_path = PROJECT_ROOT / "data" / "raw" / "2026" / manifest["source_file"]
    if not source_path.is_file() or calculate_sha256(source_path) != manifest["source_sha256"].upper():
        raise ValueError("Original 2026 ZIP missing or checksum mismatch")
    return expected_file, expected_rows, expected_hash, manifest


def make_stable_gzip(source: Path, destination: Path) -> None:
    """Recompress normalized CSV bytes with an empty filename and mtime=0."""
    with gzip.open(source, "rb") as raw:
        with destination.open("wb") as file:
            with gzip.GzipFile(filename="", mode="wb", compresslevel=6,
                               mtime=0, fileobj=file) as compressed:
                shutil.copyfileobj(raw, compressed, length=1024 * 1024)
    with destination.open("rb") as file:
        header = file.read(10)
    if len(header) != 10 or header[3] != 0 or header[4:8] != b"\x00\x00\x00\x00":
        raise ValueError("Output gzip contains filename or timestamp metadata")


def process_month(month: int) -> dict[str, object]:
    input_path, expected_rows, input_hash, processed = expected_input(month)
    schema = load_schema()
    field_names = [field["name"] for field in schema]
    if len(field_names) != 51:
        raise ValueError("BigQuery schema must contain exactly 51 fields")

    output_dir = PROJECT_ROOT / "data" / "load" / "2026"
    manifest_dir = PROJECT_ROOT / "evidence" / "source"
    summary_dir = PROJECT_ROOT / "analysis" / "profiling"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)
    tag = f"2026_{month:02d}"
    target = output_dir / f"bts_2026_{month:02d}_bq.csv.gz"
    stage = output_dir / f".bts_2026_{month:02d}_normalize.csv.gz"
    stable = output_dir / f".bts_2026_{month:02d}_stable.csv.gz"
    manifest_path = manifest_dir / f"{tag}_bq_load_manifest.csv"
    summary_path = summary_dir / f"{tag}_bq_load_summary.json"

    for temp in (stage, Path(str(stage) + ".tmp"), stable):
        if temp.exists():
            raise FileExistsError(f"Stale temporary file; review before retrying: {temp}")

    print(f"Normalizing 2026-{month:02d}: {expected_rows:,} rows...", flush=True)
    try:
        historic_result = normalize_month(
            input_path=input_path,
            output_path=stage,
            schema=schema,
            expected_rows=expected_rows,
            expected_input_hash=input_hash,
            year=2026,
        )
        if not historic_result["row_count_reconciled"] or historic_result["output_column_count"] != 51:
            raise ValueError("Historical normalization did not reconcile")
        make_stable_gzip(stage, stable)
        with gzip.open(stable, "rt", encoding="utf-8", newline="") as file:
            header = next(csv.reader(file))
        if header != field_names:
            raise ValueError("Final BigQuery file header differs from explicit schema")
        final_hash = calculate_sha256(stable)
        if target.exists():
            if calculate_sha256(target) != final_hash:
                raise ValueError("Existing BigQuery load file differs; refusing overwrite")
            print(f"2026-{month:02d}: deterministic replay PASS; existing output preserved")
        else:
            stable.replace(target)
            print(f"2026-{month:02d}: new BigQuery-ready file created")

        result = dict(historic_result)
        result["output_file"] = str(target.relative_to(PROJECT_ROOT))
        result["output_sha256"] = final_hash
        result["output_size_bytes"] = target.stat().st_size
        result["output_size_mb"] = round(target.stat().st_size / (1024 * 1024), 2)
        result["input_file"] = str(input_path.relative_to(PROJECT_ROOT))

        manifest_fields = list(result)
        if manifest_path.exists():
            old = read_single_manifest(manifest_path, set(manifest_fields))
            if {k: old.get(k) for k in manifest_fields} != {k: str(v) for k, v in result.items()}:
                raise ValueError("Existing BigQuery load manifest differs; refusing overwrite")
        else:
            with manifest_path.open("x", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=manifest_fields)
                writer.writeheader()
                writer.writerow(result)

        summary = {
            "reporting_year": 2026,
            "reporting_month": month,
            "input_file": result["input_file"],
            "input_sha256": input_hash,
            "source_zip_sha256": processed["source_sha256"],
            "expected_row_count": expected_rows,
            "output_file": result["output_file"],
            "output_row_count": result["output_row_count"],
            "output_column_count": result["output_column_count"],
            "output_sha256": result["output_sha256"],
            "output_size_bytes": result["output_size_bytes"],
            "row_count_reconciled": result["row_count_reconciled"],
            "deterministic_gzip": True,
        }
        summary_text = json.dumps(summary, indent=2) + "\n"
        if summary_path.exists():
            if summary_path.read_text(encoding="utf-8") != summary_text:
                raise ValueError("Existing BigQuery load summary differs; refusing overwrite")
        else:
            summary_path.write_text(summary_text, encoding="utf-8")

        print(f"2026-{month:02d}: BigQuery normalization PASS")
        print(f"Rows: {result['output_row_count']:,}; columns: {result['output_column_count']}")
        print(f"SHA256: {result['output_sha256']}")
        print(f"Manifest: {manifest_path}")
        print(f"Summary: {summary_path}")
        return result
    finally:
        for temp in (stage, stable):
            if temp.exists():
                temp.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a single 2026 BigQuery-ready monthly BTS load file")
    parser.add_argument("--year", required=True, type=int, choices=(2026,))
    parser.add_argument("--month", required=True, type=int, choices=range(1, 13), metavar="1-12")
    args = parser.parse_args()
    process_month(args.month)


if __name__ == "__main__":
    main()
