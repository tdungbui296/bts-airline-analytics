from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "2024"
)

TYPE_MAP_PATH = (
    PROJECT_ROOT
    / "config"
    / "bq_proposed_types.csv"
)

COLUMN_MANIFEST_PATH = (
    PROJECT_ROOT
    / "config"
    / "bts_core_columns.txt"
)

PROJECTION_SUMMARY_PATH = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2024_projection_summary.json"
)

COLUMN_AUDIT_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2024_type_audit_by_column.csv"
)

SUMMARY_OUTPUT = (
    PROJECT_ROOT
    / "analysis"
    / "profiling"
    / "2024_type_audit_summary.json"
)

CHUNK_SIZE = 100_000
MAX_INVALID_SAMPLES = 5

NULLABLE_RULES = {
    "string_nullable",
    "integer_nullable",
    "non_negative_integer_nullable",
    "flag_0_or_1_nullable",
    "hhmm_nullable",
}

INTEGER_RULES = {
    "integer",
    "integer_nullable",
    "non_negative_integer",
    "non_negative_integer_nullable",
    "flag_0_or_1",
    "flag_0_or_1_nullable",
    "hhmm",
    "hhmm_nullable",
    "diversion_landing_code",
}

NON_NEGATIVE_RULES = {
    "non_negative_integer",
    "non_negative_integer_nullable",
}

FLAG_RULES = {
    "flag_0_or_1",
    "flag_0_or_1_nullable",
}

HHMM_RULES = {
    "hhmm",
    "hhmm_nullable",
}

VALID_DIVERSION_CODES = {
    0, 1, 2, 3, 4, 5, 9
}


def load_contract() -> pd.DataFrame:
    """Load and validate the proposed 51-column type contract."""
    contract = pd.read_csv(
        TYPE_MAP_PATH,
        dtype="string",
    )

    required_contract_fields = {
        "column_name",
        "proposed_bq_type",
        "validation_rule",
    }

    if set(contract.columns) != required_contract_fields:
        raise ValueError(
            "The type-map columns must be exactly: "
            "column_name, proposed_bq_type, validation_rule."
        )

    if len(contract) != 51:
        raise ValueError(
            f"Expected 51 type-map rows; found {len(contract)}."
        )

    if contract["column_name"].duplicated().any():
        duplicates = (
            contract.loc[
                contract["column_name"].duplicated(
                    keep=False
                ),
                "column_name",
            ]
            .drop_duplicates()
            .tolist()
        )

        raise ValueError(
            f"Duplicate type-map columns: {duplicates}"
        )

    manifest_columns = [
        line.strip()
        for line in COLUMN_MANIFEST_PATH.read_text(
            encoding="ascii"
        ).splitlines()
        if line.strip()
    ]

    contract_columns = (
        contract["column_name"]
        .astype(str)
        .tolist()
    )

    if manifest_columns != contract_columns:
        raise ValueError(
            "The type-map column order does not match "
            "bts_core_columns.txt."
        )

    return contract


def add_invalid_samples(
    existing_samples: list[str],
    values: pd.Series,
) -> None:
    """Keep up to five distinct examples of invalid source values."""
    for value in (
        values
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    ):
        if (
            value not in existing_samples
            and len(existing_samples) < MAX_INVALID_SAMPLES
        ):
            existing_samples.append(value)


def valid_hhmm_mask(values: pd.Series) -> pd.Series:
    """
    Validate integer HHMM values.

    Valid examples:
    0, 5, 30, 930, 2359, 2400

    2400 is allowed, but values such as 2460 are not.
    """
    integer_values = values.round().astype("Int64")

    hours = integer_values // 100
    minutes = integer_values % 100

    normal_clock_value = (
        integer_values.between(0, 2359)
        & hours.between(0, 23)
        & minutes.between(0, 59)
    )

    midnight_end_value = integer_values.eq(2400)

    return (
        normal_clock_value
        | midnight_end_value
    ).fillna(False)


def main() -> None:
    required_paths = [
        INPUT_DIR,
        TYPE_MAP_PATH,
        COLUMN_MANIFEST_PATH,
        PROJECTION_SUMMARY_PATH,
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(
                f"Required path not found: {path}"
            )

    contract = load_contract()

    column_names = (
        contract["column_name"]
        .astype(str)
        .tolist()
    )

    rule_by_column = dict(
        zip(
            contract["column_name"].astype(str),
            contract["validation_rule"].astype(str),
        )
    )

    type_by_column = dict(
        zip(
            contract["column_name"].astype(str),
            contract["proposed_bq_type"].astype(str),
        )
    )

    input_files = sorted(
        INPUT_DIR.glob(
            "bts_2024_*_core.csv.gz"
        )
    )

    if len(input_files) != 12:
        raise ValueError(
            f"Expected 12 processed files; found {len(input_files)}."
        )

    audit = {
        column: {
            "column_name": column,
            "proposed_bq_type": type_by_column[column],
            "validation_rule": rule_by_column[column],
            "row_count": 0,
            "null_count": 0,
            "non_null_count": 0,
            "nullability_violation_count": 0,
            "parse_failure_count": 0,
            "fractional_value_count": 0,
            "domain_violation_count": 0,
            "minimum_observed": None,
            "maximum_observed": None,
            "maximum_string_length": 0,
            "invalid_samples": [],
        }
        for column in column_names
    }

    total_rows = 0
    monthly_row_counts: dict[str, int] = {}

    for file_path in input_files:
        print(
            f"Auditing {file_path.name}...",
            flush=True,
        )

        monthly_rows = 0

        reader = pd.read_csv(
            file_path,
            compression="gzip",
            usecols=column_names,
            dtype="string",
            keep_default_na=True,
            chunksize=CHUNK_SIZE,
        )

        for chunk in reader:
            chunk_rows = len(chunk)
            total_rows += chunk_rows
            monthly_rows += chunk_rows

            for column in column_names:
                rule = rule_by_column[column]
                source_values = chunk[column]

                null_mask = source_values.isna()
                non_null_mask = ~null_mask

                null_count = int(null_mask.sum())
                non_null_count = int(non_null_mask.sum())

                result = audit[column]

                result["row_count"] += chunk_rows
                result["null_count"] += null_count
                result["non_null_count"] += non_null_count

                if (
                    rule not in NULLABLE_RULES
                    and null_count > 0
                ):
                    result[
                        "nullability_violation_count"
                    ] += null_count

                    if (
                        "<NULL>"
                        not in result["invalid_samples"]
                        and len(
                            result["invalid_samples"]
                        ) < MAX_INVALID_SAMPLES
                    ):
                        result["invalid_samples"].append(
                            "<NULL>"
                        )

                if rule in {
                    "string",
                    "string_nullable",
                }:
                    if non_null_count:
                        lengths = (
                            source_values.loc[non_null_mask]
                            .astype(str)
                            .str.encode("utf-8")
                            .str.len()
                        )

                        result[
                            "maximum_string_length"
                        ] = max(
                            result[
                                "maximum_string_length"
                            ],
                            int(lengths.max()),
                        )

                    continue

                if rule == "valid_date":
                    parsed_dates = pd.to_datetime(
                        source_values,
                        errors="coerce",
                        format="mixed",
                    )

                    parse_failure_mask = (
                        non_null_mask
                        & parsed_dates.isna()
                    )

                    parse_failures = int(
                        parse_failure_mask.sum()
                    )

                    result[
                        "parse_failure_count"
                    ] += parse_failures

                    add_invalid_samples(
                        result["invalid_samples"],
                        source_values.loc[
                            parse_failure_mask
                        ],
                    )

                    valid_dates = parsed_dates.dropna()

                    if not valid_dates.empty:
                        minimum = valid_dates.min().date()
                        maximum = valid_dates.max().date()

                        if (
                            result["minimum_observed"] is None
                            or str(minimum)
                            < result["minimum_observed"]
                        ):
                            result[
                                "minimum_observed"
                            ] = str(minimum)

                        if (
                            result["maximum_observed"] is None
                            or str(maximum)
                            > result["maximum_observed"]
                        ):
                            result[
                                "maximum_observed"
                            ] = str(maximum)

                    continue

                if rule in INTEGER_RULES:
                    numeric_values = pd.to_numeric(
                        source_values,
                        errors="coerce",
                    )

                    parse_failure_mask = (
                        non_null_mask
                        & numeric_values.isna()
                    )

                    parse_failures = int(
                        parse_failure_mask.sum()
                    )

                    result[
                        "parse_failure_count"
                    ] += parse_failures

                    add_invalid_samples(
                        result["invalid_samples"],
                        source_values.loc[
                            parse_failure_mask
                        ],
                    )

                    parsed_non_null_mask = (
                        numeric_values.notna()
                    )

                    fractional_mask = (
                        parsed_non_null_mask
                        & ~np.isclose(
                            numeric_values % 1,
                            0,
                            equal_nan=False,
                        )
                    )

                    fractional_count = int(
                        fractional_mask.sum()
                    )

                    result[
                        "fractional_value_count"
                    ] += fractional_count

                    add_invalid_samples(
                        result["invalid_samples"],
                        source_values.loc[
                            fractional_mask
                        ],
                    )

                    valid_integer_mask = (
                        parsed_non_null_mask
                        & ~fractional_mask
                    )

                    integer_values = numeric_values.loc[
                        valid_integer_mask
                    ]

                    domain_violation_mask = pd.Series(
                        False,
                        index=source_values.index,
                    )

                    if rule in NON_NEGATIVE_RULES:
                        domain_violation_mask = (
                            valid_integer_mask
                            & numeric_values.lt(0)
                        )

                    elif rule in FLAG_RULES:
                        domain_violation_mask = (
                            valid_integer_mask
                            & ~numeric_values.isin([0, 1])
                        )

                    elif rule in HHMM_RULES:
                        valid_clock_mask = valid_hhmm_mask(
                            numeric_values
                        )

                        domain_violation_mask = (
                            valid_integer_mask
                            & ~valid_clock_mask
                        )

                    elif rule == "diversion_landing_code":
                        domain_violation_mask = (
                            valid_integer_mask
                            & ~numeric_values.isin(
                                VALID_DIVERSION_CODES
                            )
                        )

                    domain_violation_count = int(
                        domain_violation_mask.sum()
                    )

                    result[
                        "domain_violation_count"
                    ] += domain_violation_count

                    add_invalid_samples(
                        result["invalid_samples"],
                        source_values.loc[
                            domain_violation_mask
                        ],
                    )

                    valid_domain_values = numeric_values.loc[
                        valid_integer_mask
                        & ~domain_violation_mask
                    ]

                    if not valid_domain_values.empty:
                        minimum = int(
                            valid_domain_values.min()
                        )

                        maximum = int(
                            valid_domain_values.max()
                        )

                        if (
                            result["minimum_observed"] is None
                            or minimum
                            < result["minimum_observed"]
                        ):
                            result[
                                "minimum_observed"
                            ] = minimum

                        if (
                            result["maximum_observed"] is None
                            or maximum
                            > result["maximum_observed"]
                        ):
                            result[
                                "maximum_observed"
                            ] = maximum

                    continue

                raise ValueError(
                    f"Unsupported validation rule "
                    f"'{rule}' for column '{column}'."
                )

        monthly_row_counts[file_path.name] = (
            monthly_rows
        )

        print(
            f"{file_path.name}: "
            f"{monthly_rows:,} rows audited",
            flush=True,
        )

    output_rows = []

    for column in column_names:
        result = audit[column]

        total_violation_count = (
            result["nullability_violation_count"]
            + result["parse_failure_count"]
            + result["fractional_value_count"]
            + result["domain_violation_count"]
        )

        output_rows.append(
            {
                "column_name": result["column_name"],
                "proposed_bq_type": (
                    result["proposed_bq_type"]
                ),
                "validation_rule": (
                    result["validation_rule"]
                ),
                "row_count": result["row_count"],
                "null_count": result["null_count"],
                "null_percentage": round(
                    result["null_count"]
                    / result["row_count"]
                    * 100,
                    4,
                ),
                "non_null_count": (
                    result["non_null_count"]
                ),
                "nullability_violation_count": (
                    result[
                        "nullability_violation_count"
                    ]
                ),
                "parse_failure_count": (
                    result["parse_failure_count"]
                ),
                "fractional_value_count": (
                    result["fractional_value_count"]
                ),
                "domain_violation_count": (
                    result["domain_violation_count"]
                ),
                "total_violation_count": (
                    total_violation_count
                ),
                "minimum_observed": (
                    result["minimum_observed"]
                ),
                "maximum_observed": (
                    result["maximum_observed"]
                ),
                "maximum_string_length": (
                    result["maximum_string_length"]
                ),
                "invalid_samples": " | ".join(
                    result["invalid_samples"]
                ),
                "type_audit_passed": (
                    total_violation_count == 0
                ),
            }
        )

    audit_df = pd.DataFrame(output_rows)

    COLUMN_AUDIT_OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    audit_df.to_csv(
        COLUMN_AUDIT_OUTPUT,
        index=False,
        encoding="utf-8",
    )

    projection_summary = json.loads(
        PROJECTION_SUMMARY_PATH.read_text(
            encoding="utf-8"
        )
    )

    expected_total_rows = int(
        projection_summary["total_output_rows"]
    )

    review_columns = (
        audit_df.loc[
            ~audit_df["type_audit_passed"],
            "column_name",
        ]
        .astype(str)
        .tolist()
    )

    summary = {
        "input_file_count": len(input_files),
        "expected_total_rows": expected_total_rows,
        "audited_total_rows": total_rows,
        "row_count_reconciled": (
            total_rows == expected_total_rows
        ),
        "audited_column_count": len(column_names),
        "passed_column_count": int(
            audit_df["type_audit_passed"].sum()
        ),
        "review_column_count": int(
            (~audit_df["type_audit_passed"]).sum()
        ),
        "review_columns": review_columns,
        "all_proposed_types_safe": (
            len(review_columns) == 0
            and total_rows == expected_total_rows
        ),
        "monthly_row_counts": monthly_row_counts,
        "column_audit_output": str(
            COLUMN_AUDIT_OUTPUT.relative_to(
                PROJECT_ROOT
            )
        ),
    }

    SUMMARY_OUTPUT.write_text(
        json.dumps(
            summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print("2024 type audit completed.")
    print(
        f"Rows audited: {total_rows:,}"
    )
    print(
        "Row count reconciled: "
        f"{summary['row_count_reconciled']}"
    )
    print(
        "Columns passed: "
        f"{summary['passed_column_count']} / "
        f"{summary['audited_column_count']}"
    )
    print(
        f"Review columns: {review_columns}"
    )
    print(
        "All proposed types safe: "
        f"{summary['all_proposed_types_safe']}"
    )
    print(f"Column output: {COLUMN_AUDIT_OUTPUT}")
    print(f"Summary: {SUMMARY_OUTPUT}")


if __name__ == "__main__":
    main()
