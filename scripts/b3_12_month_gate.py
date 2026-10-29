"""B3.12 — BTS append-load safety gate (offline only).

This program cannot submit Google Cloud requests. It validates the local monthly
artifact and exercises the future remote month-admission policy against mock state.

Usage from repository root:
    python scripts/b3_12_month_gate.py --self-test
    python scripts/b3_12_month_gate.py --year 2024 --month 1
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path

PROJECT = "tdb-dbt-bigquery-portfolio"
REGION = "asia-southeast1"
EXPECTED = {
    2024: (7079061, None),
    2025: (7001619, None),
    2026: (1059040, {1: 544003, 2: 515037}),
}


def months_in_target(year: int) -> list[tuple[int, int]]:
    if year in (2024, 2025):
        return [(y, m) for y in (2024, 2025) for m in range(1, 13)]
    if year == 2026:
        return [(2026, 1), (2026, 2)]
    raise ValueError("Year outside approved scope")


def target_for(year: int) -> str:
    return "raw_bts.flights_2026" if year == 2026 else "raw_bts.flights"


def manifest_for(root: Path, year: int, month: int) -> dict[str, str]:
    if year == 2026:
        manifest = root / f"evidence/source/2026_{month:02d}_bq_load_manifest.csv"
    else:
        manifest = root / f"evidence/source/{year}_bq_load_manifest.csv"
    with manifest.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    matched = [row for row in rows
               if int(row["reporting_year"]) == year
               and int(row["reporting_month"]) == month]
    if len(matched) != 1:
        raise ValueError(f"Exactly one manifest record required for {year}-{month:02d}")
    return matched[0]


def local_artifact(root: Path, year: int, month: int) -> dict[str, object]:
    scope = months_in_target(year)
    if (year, month) not in scope:
        raise ValueError("Month not in approved 26-month scope")
    path = root / f"data/load/{year}/bts_{year}_{month:02d}_bq.csv.gz"
    record = manifest_for(root, year, month)
    schema = json.loads((root / "config/bq_flights_schema.json").read_text(encoding="utf-8-sig"))
    column_names = [field["name"] for field in schema]
    if len(column_names) != 51 or len(set(column_names)) != 51:
        raise ValueError("Invalid BigQuery schema")
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest().upper()
    if digest != record["output_sha256"].upper():
        raise ValueError("Source SHA256 does not match manifest")
    with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as handle:
        if next(csv.reader(handle)) != column_names:
            raise ValueError("Header order does not match schema")
    if record["row_count_reconciled"].strip().lower() != "true":
        raise ValueError("Manifest row reconciliation failed")
    if int(record["output_column_count"]) != 51:
        raise ValueError("Manifest column count mismatch")
    if year == 2026 and int(record["output_row_count"]) != EXPECTED[2026][1][month]:
        raise ValueError("Known 2026 month row count mismatch")
    return {
        "month": f"{year}-{month:02d}",
        "target": target_for(year),
        "rows": int(record["output_row_count"]),
        "sha256": digest,
        "file": path.relative_to(root).as_posix(),
        "proposed_job_id": f"bts_{year}_{month:02d}_{digest[:16].lower()}",
    }


def gate_month(
    *,
    requested: tuple[int, int],
    expected_counts: dict[tuple[int, int], int],
    target_exists: bool,
    observed_counts: dict[tuple[int, int], int] | None,
    observed_total: int | None,
    region: str,
    schema_matches: bool,
    ambiguous_previous_job: bool = False,
) -> str:
    """Pure admission decision; never submits a load or query."""
    scope = months_in_target(requested[0])
    if requested not in scope:
        raise ValueError("Unapproved month")
    if region.lower() != REGION or not schema_matches:
        return "BLOCK: region or schema mismatch"
    if ambiguous_previous_job:
        return "BLOCK: prior job result ambiguous; inspect job before retry"
    if not target_exists:
        if requested != scope[0]:
            return "BLOCK: target missing after expected initial load"
        return "CANDIDATE: initial load; requires separate billed-deployment authorization"
    if observed_counts is None or observed_total is None:
        return "BLOCK: remote counts and total not independently verified"
    if set(observed_counts) != set(scope) or observed_total < 0:
        return "BLOCK: incomplete remote month inventory"
    if any(value < 0 for value in observed_counts.values()):
        return "BLOCK: invalid remote count"
    if sum(observed_counts.values()) != observed_total:
        return "BLOCK: remote total inconsistent with monthly inventory"
    idx = scope.index(requested)
    for earlier in scope[:idx]:
        if observed_counts[earlier] != expected_counts[earlier]:
            return "BLOCK: earlier month absent or differs from expected"
    if observed_counts[requested] != 0:
        return "BLOCK: requested month already loaded or partially present"
    if any(observed_counts[later] != 0 for later in scope[idx+1:]):
        return "BLOCK: later month already present; sequence not controlled"
    return "CANDIDATE: requested month absent; requires separate billed-deployment authorization"


def self_test() -> int:
    scope = months_in_target(2024)
    counts = {month: 100 for month in scope}
    clean = {month: (100 if i < 1 else 0) for i, month in enumerate(scope)}
    base = dict(
        requested=(2024, 2),
        expected_counts=counts,
        target_exists=True,
        observed_counts=clean,
        observed_total=100,
        region=REGION,
        schema_matches=True,
    )
    cases = [
        ("clean_next_month", base, "CANDIDATE:"),
        ("replay_month", {**base, "requested": (2024, 1)}, "BLOCK:"),
        ("partial_month", {**base, "observed_counts": {**clean, (2024, 2): 7},
                            "observed_total": 107}, "BLOCK:"),
        ("bad_prior", {**base, "observed_counts": {**clean, (2024, 1): 99},
                       "observed_total": 99}, "BLOCK:"),
        ("bad_total", {**base, "observed_total": 101}, "BLOCK:"),
        ("missing_remote_counts", {**base, "observed_counts": None}, "BLOCK:"),
        ("schema_mismatch", {**base, "schema_matches": False}, "BLOCK:"),
        ("region_mismatch", {**base, "region": "US"}, "BLOCK:"),
        ("ambiguous_job", {**base, "ambiguous_previous_job": True}, "BLOCK:"),
        ("missing_later_target", {**base, "target_exists": False}, "BLOCK:"),
        ("initial_target_absent", {**base, "requested": (2024, 1), "target_exists": False},
         "CANDIDATE:"),
        ("future_month_present", {**base,
                                  "observed_counts": {**clean, (2024, 3): 4},
                                  "observed_total": 104}, "BLOCK:"),
    ]
    for name, kwargs, prefix in cases:
        outcome = gate_month(**kwargs)
        if not outcome.startswith(prefix):
            raise AssertionError(f"{name}: unexpected decision {outcome}")
        print(f"PASS: {name}: {outcome}")
    print(f"\nB3.12 OFFLINE SAFETY TESTS PASSED: {len(cases)} scenarios; NO CLOUD CALLS")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, choices=[2024, 2025, 2026])
    parser.add_argument("--month", type=int)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        if args.year is not None or args.month is not None:
            parser.error("--self-test cannot be combined with a month")
        self_test()
        return
    if args.year is None or args.month is None:
        parser.error("Use --self-test or provide both --year and --month")
    info = local_artifact(Path.cwd(), args.year, args.month)
    print(json.dumps(info, indent=2))
    print("\nLOCAL ARTIFACT VERIFIED. No remote month query performed.")
    print("BLOCKED: no load is authorized; this program cannot execute BigQuery jobs.")


if __name__ == "__main__":
    main()
