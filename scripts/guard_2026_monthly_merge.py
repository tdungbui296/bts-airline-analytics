"""Fail-closed journal for 2026 monthly dbt MERGE attempts.

A recorded attempt is not proof of a successful BigQuery MERGE.
Never automatically retry an uncertain attempt.
"""

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_EVIDENCE_ROOT = Path(
    "/opt/bts/evidence/dbt/monthly_runs"
)
DEFAULT_COMPLETION_ROOT = Path(
    "/opt/bts/evidence/bigquery/monthly_processing"
)


def validate_month(month):
    if not isinstance(month, str) or not re.fullmatch(
        r"2026-(0[3-9]|1[0-2])", month
    ):
        raise ValueError(
            "Only new canonical 2026 months are supported"
        )
    return month


def inspect_month(
    month,
    evidence_root=DEFAULT_EVIDENCE_ROOT,
    completion_root=DEFAULT_COMPLETION_ROOT,
):
    month = validate_month(month)

    workspace = (
        Path(evidence_root)
        / ("merge_" + month.replace("-", "_"))
    )

    journal = workspace / "merge_attempt.json"
    results = workspace / "target" / "run_results.json"
    completion = Path(completion_root) / (month + ".json")

    if completion.exists():
        state = "COMPLETION_RECEIPT_PRESENT"
    elif journal.exists():
        state = (
            "ATTEMPT_WITH_DBT_RESULTS"
            if results.exists()
            else "ATTEMPT_RECORDED_NO_RESULTS"
        )
    elif workspace.is_dir() and any(
        path.is_file() for path in workspace.rglob("*")
    ):
        state = "ORPHANED_DBT_ARTIFACTS"
    else:
        state = "NEVER_ATTEMPTED"

    return {
        "month": month,
        "state": state,
        "journal_exists": journal.exists(),
        "run_results_exists": results.exists(),
        "completion_exists": completion.exists(),
    }


def reserve_merge(
    month,
    evidence_root=DEFAULT_EVIDENCE_ROOT,
    completion_root=DEFAULT_COMPLETION_ROOT,
):
    month = validate_month(month)

    state = inspect_month(
        month,
        evidence_root=evidence_root,
        completion_root=completion_root,
    )

    if state["state"] != "NEVER_ATTEMPTED":
        raise RuntimeError(
            "MERGE blocked: "
            + state["state"]
            + ". Manual BigQuery/dbt reconciliation required."
        )

    workspace = (
        Path(evidence_root)
        / ("merge_" + month.replace("-", "_"))
    )

    workspace.mkdir(parents=True, exist_ok=True)
    journal = workspace / "merge_attempt.json"

    payload = {
        "month": month,
        "stage": "dbt_incremental_merge",
        "state": "RESERVED_BEFORE_DBT",
        "created_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "automatic_retry_permitted": False,
    }

    try:
        with journal.open("x", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                sort_keys=True,
                indent=2,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise RuntimeError(
            "Concurrent or previous MERGE attempt detected"
        ) from error

    return journal


def self_test():
    with tempfile.TemporaryDirectory(
        prefix="bts_merge_guard_test_"
    ) as temporary:
        base = Path(temporary)
        evidence = base / "evidence"
        completions = base / "completions"
        completions.mkdir()

        def inspect(month):
            return inspect_month(
                month, evidence, completions
            )

        def reserve(month):
            return reserve_merge(
                month, evidence, completions
            )

        assert inspect("2026-03")["state"] == (
            "NEVER_ATTEMPTED"
        )
        print("NEW_MONTH_UNATTEMPTED_PASS")

        journal = reserve("2026-03")
        payload = json.loads(
            journal.read_text(encoding="utf-8")
        )

        assert payload["month"] == "2026-03"
        assert payload["state"] == "RESERVED_BEFORE_DBT"
        assert payload["automatic_retry_permitted"] is False

        assert inspect("2026-03")["state"] == (
            "ATTEMPT_RECORDED_NO_RESULTS"
        )

        try:
            reserve("2026-03")
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "Duplicate MERGE was accepted"
            )

        print("PERSISTENT_ATTEMPT_AND_DUPLICATE_BLOCK_PASS")

        results = journal.parent / "target" / "run_results.json"
        results.parent.mkdir()
        results.write_text(
            '{"results": []}\n',
            encoding="utf-8",
        )

        assert inspect("2026-03")["state"] == (
            "ATTEMPT_WITH_DBT_RESULTS"
        )

        try:
            reserve("2026-03")
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "Uncertain MERGE retry was accepted"
            )

        print("UNCERTAIN_MERGE_RECOVERY_BLOCK_PASS")

        completion = completions / "2026-04.json"
        completion.write_text(
            '{"status": "DONE"}\n',
            encoding="utf-8",
        )

        assert inspect("2026-04")["state"] == (
            "COMPLETION_RECEIPT_PRESENT"
        )

        try:
            reserve("2026-04")
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "Completed month was accepted"
            )

        print("COMPLETED_MONTH_BLOCK_PASS")

        orphan = (
            evidence / "merge_2026_05"
            / "target" / "run_results.json"
        )
        orphan.parent.mkdir(parents=True)
        orphan.write_text(
            '{"results": []}\n',
            encoding="utf-8",
        )

        assert inspect("2026-05")["state"] == (
            "ORPHANED_DBT_ARTIFACTS"
        )

        try:
            reserve("2026-05")
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "Orphaned execution was accepted"
            )

        print("ORPHANED_EXECUTION_ARTIFACT_BLOCK_PASS")

        for invalid in (
            "2026-01",
            "2026-02",
            "2026-13",
            "2027-01",
            "../03",
            "2026-3",
        ):
            try:
                reserve(invalid)
            except ValueError:
                continue
            raise AssertionError(
                "Unsafe month accepted: " + invalid
            )

        print("HISTORICAL_AND_INVALID_MONTH_BLOCK_PASS")

    print("SYNTHETIC_TEST_ARTIFACTS_REMOVED")
    print("NO_REAL_MONTH_JOURNAL_CREATED")
    print("NO_BIGQUERY_API_CALLS")
    print("NO_DBT_EXECUTION")
    print("B3_51DE_R1_GUARD_SELF_TEST_PASS")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--self-test",
        action="store_true",
        required=True,
    )
    parser.parse_args()
    self_test()
