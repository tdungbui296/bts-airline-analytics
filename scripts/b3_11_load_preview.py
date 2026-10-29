"""B3.11: verify BTS load artifacts and write a NON-EXECUTABLE command review.

This tool does NOT invoke bq, gcloud, dbt, network APIs, or any cloud service.
It refuses to overwrite an existing preview. It does not create cloud resources.

Run from the BTS repository root:
    python scripts/b3_11_load_preview.py
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
from pathlib import Path

PROJECT = "tdb-dbt-bigquery-portfolio"
LOCATION = "asia-southeast1"
EXPECTED_TOTALS = {2024: 7_079_061, 2025: 7_001_619, 2026: 1_059_040}
EXPECTED_2026 = {1: 544_003, 2: 515_037}
OUTPUT = Path("docs/B3_11_bigquery_load_command_PREVIEW.md")


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest().upper()


def verified_month(root: Path, schema_fields: list[str], year: int, month: int,
                   row: dict[str, str]) -> dict[str, object]:
    tag = f"{year}-{month:02d}"
    expected_name = f"bts_{year}_{month:02d}_bq.csv.gz"
    path = root / "data" / "load" / str(year) / expected_name
    if not path.is_file():
        raise RuntimeError(f"FAIL {tag}: missing {path}")
    expected_row_count = int(row["output_row_count"])
    if expected_row_count <= 0:
        raise RuntimeError(f"FAIL {tag}: zero or invalid row count")
    if row["row_count_reconciled"].strip().lower() != "true":
        raise RuntimeError(f"FAIL {tag}: manifest reconciliation not true")
    if int(row["output_column_count"]) != 51:
        raise RuntimeError(f"FAIL {tag}: manifest column count not 51")
    if not re.fullmatch(r"[0-9A-Fa-f]{64}", row["output_sha256"].strip()):
        raise RuntimeError(f"FAIL {tag}: malformed manifest hash")
    actual_hash = file_hash(path)
    if actual_hash != row["output_sha256"].strip().upper():
        raise RuntimeError(f"FAIL {tag}: SHA-256 mismatch")
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle))
    if header != schema_fields:
        raise RuntimeError(f"FAIL {tag}: header differs from ordered JSON schema")
    if year == 2026 and expected_row_count != EXPECTED_2026[month]:
        raise RuntimeError(f"FAIL {tag}: wrong known 2026 source row count")
    expected_output = path.relative_to(root).as_posix()
    if row.get("output_file") and Path(row["output_file"].replace("\\", "/")).as_posix() != expected_output:
        raise RuntimeError(f"FAIL {tag}: manifest output_file path mismatch")
    return {"year": year, "month": month, "rows": expected_row_count,
            "relative_file": path.relative_to(root), "hash": actual_hash}


def plan_commands(entries: list[dict[str, object]]) -> str:
    lines = [
        "# B3.11 — BTS BigQuery load command PREVIEW (NOT AN EXECUTION SCRIPT)",
        "",
        "**Status: REVIEW ONLY. DO NOT RUN THESE COMMANDS.**",
        "",
        f"- Target project: `{PROJECT}`; region: `{LOCATION}`.",
        "- Source: 24 historical monthly gzip CSV files plus January/February 2026.",
        "- The commands below use append behavior (`--noreplace`); append is NOT idempotent.",
        "- **BLOCKED** until the explicitly authorized billed phase and all remote resource/",
        "  already-loaded-month checks, permission checks, cost/quota safeguards, and",
        "  partition-retention fixes are complete.",
        "- Do not paste individual load commands from this document into a terminal.",
        "- Each completed load needs a job ID, month-specific warehouse count,",
        "  and durable receipt before another month is authorized.",
        "- If a load errors or its receipt is missing, stop; verify the remote table and",
        "  job status before considering a retry. Never blindly append again.",
        "- These are NEW proposed loading commands; they are not historical load logs.",
        "",
        "## Proposed command pattern (review only)",
        "",
        "```powershell",
    ]
    for entry in entries:
        y, m = int(entry["year"]), int(entry["month"])
        target = "raw_bts.flights" if y < 2026 else "raw_bts.flights_2026"
        relpath = entry["relative_file"].as_posix()
        lines += [
            f"# {y}-{m:02d}: {int(entry['rows']):,} rows; SHA256 {entry['hash']}",
            f"# DO NOT RUN until remote month absence is independently established.",
            (f"bq --location={LOCATION} load --source_format=CSV "
             f"--skip_leading_rows=1 --max_bad_records=0 --noreplace "
             f'"{PROJECT}:{target}" "{relpath}" "config/bq_flights_schema.json"'),
            "",
        ]
    lines += [
        "```", "", "## Required deployment gates (not satisfied in this preview)", "",
        "1. Explicit authorization to attach billing/trial; verify project, quotas, budgets, and permissions.",
        "2. Remove `dbt_dev` default partition expiration, keep controlled table TTL, and verify metadata.",
        "3. Confirm no prior month is loaded before any append; record job IDs and monthly counts.",
        "4. Restore historical landing data; build historical views and bootstrap the physical incremental table.",
        "5. Confirm partition and clustering metadata; load January then February individually.",
        "6. Run month-scoped MERGE and replay/correction tests with separate evidence and cost checks.",
        "7. Maintain separate raw-load idempotency checks: a dbt MERGE does not prevent CSV append duplicates.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    root = Path.cwd()
    if not (root / "dbt/dbt_project.yml").is_file():
        raise RuntimeError("Run from the BTS repository root containing dbt/dbt_project.yml")
    dest = root / OUTPUT
    if dest.exists():
        raise FileExistsError(f"Existing preview preserved (refusing overwrite): {dest}")
    schema = json.loads((root / "config/bq_flights_schema.json").read_text(encoding="utf-8-sig"))
    names = [field["name"] for field in schema]
    if len(names) != 51 or len(set(names)) != 51:
        raise RuntimeError("Expected 51 distinct schema column names")
    all_entries: list[dict[str, object]] = []
    for year in (2024, 2025, 2026):
        rows: list[dict[str, str]] = []
        if year in (2024, 2025):
            manifest = root / f"evidence/source/{year}_bq_load_manifest.csv"
            with manifest.open("r", encoding="utf-8-sig", newline="") as file:
                rows = list(csv.DictReader(file))
            months = [int(r["reporting_month"]) for r in rows]
            if len(rows) != 12 or sorted(months) != list(range(1, 13)):
                raise RuntimeError(f"FAIL {year}: annual manifest must contain each month once")
        else:
            for month in (1, 2):
                manifest = root / f"evidence/source/2026_{month:02d}_bq_load_manifest.csv"
                with manifest.open("r", encoding="utf-8-sig", newline="") as file:
                    monthly = list(csv.DictReader(file))
                if len(monthly) != 1 or int(monthly[0]["reporting_month"]) != month:
                    raise RuntimeError(f"FAIL 2026-{month:02d}: monthly manifest invalid")
                rows.extend(monthly)
        year_entries = []
        for row in rows:
            if int(row["reporting_year"]) != year:
                raise RuntimeError(f"FAIL {year}: manifest reporting_year mismatch")
            month = int(row["reporting_month"])
            year_entries.append(verified_month(root, names, year, month, row))
        year_entries.sort(key=lambda entry: int(entry["month"]))
        total = sum(int(entry["rows"]) for entry in year_entries)
        if total != EXPECTED_TOTALS[year]:
            raise RuntimeError(f"FAIL {year}: expected {EXPECTED_TOTALS[year]:,}, got {total:,}")
        all_entries.extend(year_entries)
        print(f"{year}: PASS | {len(year_entries)} files | {total:,} rows | SHA-256/header verified")
    if len(all_entries) != 26:
        raise RuntimeError("FAIL: expected exactly 26 monthly files")
    text = plan_commands(all_entries)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("x", encoding="utf-8", newline="\n") as output:
        output.write(text)
    print(f"\nB3.11 LOCAL LOAD PLAN PASSED: {len(all_entries)} proposed commands; NO CLOUD CALLS")
    print(f"Preview written: {OUTPUT.as_posix()}")
    print("BLOCKED: preview commands must not be executed before billed-deployment authorization")


if __name__ == "__main__":
    main()
