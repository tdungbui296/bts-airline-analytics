"""Locally audit the BTS incremental SQL branches without any warehouse calls.

Reads existing dbt SQL and uses mock dbt/Jinja helpers to verify branch
selection and protective guards. This is NOT `dbt compile`, a BigQuery query,
or proof of successful MERGE execution.

From BTS repository root:
    python scripts/b3_5_offline_incremental_audit.py --write-evidence
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, StrictUndefined


MODEL_RELATIVE = Path('dbt/models/incremental_demo/fact_flights_incremental_demo.sql')
EXPECTED_SQL_SHA256 = 'A43BF19208C19FAAEAA90F9D1D25BCB1572917E224768BAB6D8D85D3508700AF'
EVIDENCE_RELATIVE = Path('evidence/incremental/b3_5_offline_incremental_audit.json')


class LocalCompilerError(Exception):
    pass


class FakeExceptions:
    @staticmethod
    def raise_compiler_error(message: str) -> None:
        raise LocalCompilerError(message)


def render(sql: str, *, mode: str | None, month: str | int | None = None,
           target_exists: bool = False, full_refresh: bool = False) -> str:
    """Render with mocks; never creates a dbt adapter or makes a connection."""
    variables: dict[str, object] = {'enable_incremental_demo': True}
    if mode is not None:
        variables['incremental_load_mode'] = mode
    if month is not None:
        variables['reprocess_month'] = month

    env = Environment(undefined=StrictUndefined, keep_trailing_newline=True)
    template = env.from_string(sql)
    return template.render(
        config=lambda **kwargs: '',
        ref=lambda name: f'`PORTFOLIO_LOCAL`.`dbt_dev`.`{name}`',
        var=lambda name, default=None: variables.get(name, default),
        execute=True,
        flags=SimpleNamespace(WHICH='run', FULL_REFRESH=full_refresh),
        is_incremental=lambda: target_exists,
        exceptions=FakeExceptions(),
    )


def contains_actual_source(sql: str, name: str) -> bool:
    return re.search(
        rf'(?im)^\s*from\s+`PORTFOLIO_LOCAL`\.`dbt_dev`\.`{re.escape(name)}`\s*$',
        sql,
    ) is not None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write-evidence', action='store_true')
    args = parser.parse_args()

    root = Path.cwd()
    model_path = root / MODEL_RELATIVE
    if not model_path.is_file():
        raise SystemExit(f'FAIL: dbt model missing: {model_path}')

    source = model_path.read_bytes()
    digest = hashlib.sha256(source).hexdigest().upper()
    if digest != EXPECTED_SQL_SHA256:
        raise SystemExit(
            'FAIL: incremental SQL differs from the reviewed B3.4 version.\n'
            f'Actual SHA256: {digest}\n'
            'No evidence written. Review changes before continuing.'
        )

    text = source.decode('utf-8')
    checks: list[str] = []

    bootstrap = render(text, mode='bootstrap', target_exists=False)
    if not contains_actual_source(bootstrap, 'fact_flights'):
        raise SystemExit('FAIL: bootstrap does not select the historical fact')
    if contains_actual_source(bootstrap, 'fact_flights_2026'):
        raise SystemExit('FAIL: bootstrap unexpectedly selects the 2026 fact')
    checks.append('bootstrap_uses_historical_fact_only')
    print('PASS: bootstrap selects historical fact only')

    for month in ('2026-01', '2026-02'):
        monthly = render(text, mode='monthly', month=month, target_exists=True)
        if not contains_actual_source(monthly, 'fact_flights_2026'):
            raise SystemExit(f'FAIL: {month} does not select the 2026 fact')
        if contains_actual_source(monthly, 'fact_flights'):
            raise SystemExit(f'FAIL: {month} unexpectedly selects the historical fact')
        if f"date('{month}-01')" not in monthly:
            raise SystemExit(f'FAIL: {month} lacks the exact lower date bound')
        if 'interval 1 month' not in monthly.lower():
            raise SystemExit(f'FAIL: {month} lacks the upper month bound')
        if not re.search(r'\bflight_date\s*>=', monthly):
            raise SystemExit(f'FAIL: {month} lacks the lower window predicate')
        if not re.search(r'\bflight_date\s*<', monthly):
            raise SystemExit(f'FAIL: {month} lacks the exclusive upper bound')
        checks.append(f'{month}_source_and_exact_month_filter')
        print(f'PASS: {month} uses 2026 fact and exact monthly window')

    rejected_cases = [
        ('missing_mode', dict(mode=None), 'Set incremental_load_mode'),
        ('monthly_without_target', dict(mode='monthly', month='2026-01', target_exists=False), 'target incremental table does not exist'),
        ('repeat_bootstrap', dict(mode='bootstrap', target_exists=True), 'Bootstrap blocked'),
        ('bootstrap_with_month', dict(mode='bootstrap', month='2026-01'), 'Bootstrap must not include'),
        ('bootstrap_full_refresh', dict(mode='bootstrap', full_refresh=True), 'full-refresh is prohibited'),
        ('monthly_full_refresh', dict(mode='monthly', month='2026-01', target_exists=True, full_refresh=True), 'full-refresh is prohibited'),
        ('bad_month_no_zero_pad', dict(mode='monthly', month='2026-1', target_exists=True), 'reprocess_month must'),
        ('bad_month_not_2026', dict(mode='monthly', month='2025-12', target_exists=True), 'reprocess_month must'),
        ('bad_month_13', dict(mode='monthly', month='2026-13', target_exists=True), 'reprocess_month must'),
        ('numeric_month', dict(mode='monthly', month=202601, target_exists=True), 'reprocess_month must'),
    ]
    for label, kwargs, expected_error in rejected_cases:
        try:
            render(text, **kwargs)
        except LocalCompilerError as exc:
            if expected_error.lower() not in str(exc).lower():
                raise SystemExit(f'FAIL: {label} raised an unexpected error: {exc}')
        else:
            raise SystemExit(f'FAIL: {label} was not rejected')
        checks.append(label + '_rejected')
        print(f'PASS: unsafe operation rejected ({label})')

    print(f'\nB3.5 OFFLINE AUDIT PASS: {len(checks)} checks; no BigQuery calls')
    print('LIMITATION: Jinja mocks do not replace real dbt compilation or MERGE tests.')

    if args.write_evidence:
        destination = root / EVIDENCE_RELATIVE
        if destination.exists():
            raise SystemExit(f'FAIL: refusing to overwrite existing evidence: {destination}')
        destination.parent.mkdir(parents=True, exist_ok=True)
        evidence = {
            'checkpoint': 'B3.5',
            'audit_type': 'offline_jinja_branch_and_guard_checks',
            'model_path': MODEL_RELATIVE.as_posix(),
            'model_sha256': digest,
            'checks': checks,
            'number_of_checks': len(checks),
            'warehouse_connected': False,
            'dbt_compilation_performed': False,
            'merge_executed': False,
        }
        with destination.open('x', encoding='utf-8', newline='\n') as file:
            json.dump(evidence, file, indent=2)
            file.write('\n')
        print(f'Evidence written: {EVIDENCE_RELATIVE.as_posix()}')


if __name__ == '__main__':
    main()
