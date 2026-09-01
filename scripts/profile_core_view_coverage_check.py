"""
Validate that every column referenced by profile_database_views.sql exists on
the table the view alias points to. Run against any of the four profile
datasets (default: profile_data_candidate, since that's where rebuild candidate
tables live before promotion).

Catches the failure mode that has bitten the v6.4 first-build:
    populate_*.sql CTAS silently sheds a column that views.sql references,
    rebuild populate steps complete fine, then `views` step fails late with
    a confusing 'Name X not found inside pc' error.

This check is cheap (only reads INFORMATION_SCHEMA) and is wired into
scripts/profile_release_check.py so it gates every refactor.

Usage:
    python scripts/profile_core_view_coverage_check.py
    python scripts/profile_core_view_coverage_check.py --dataset profile_data_candidate
    python scripts/profile_core_view_coverage_check.py --dataset profile_data
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.cloud import bigquery

PROJECT_ID = 'bi-data-391216'
REPO_ROOT = Path(__file__).resolve().parents[1]
VIEWS_SQL = REPO_ROOT / "sql" / "profile_database_views.sql"

# Map alias -> table name. Aliases come from FROM/JOIN clauses in views.sql.
# CTE/view aliases (signal-rollup intermediates, profile_signals view itself)
# are listed as None so they're skipped.
ALIAS_TO_TABLE: dict[str, str | None] = {
    'pc': 'profile_core',
    'pe': 'profile_engagement',
    'pi': 'profile_identifiers',
    'pst': 'profile_segment_tags',
    'paa': 'profile_ad_attribution',
    'pca': 'profile_content_affinity',
    't': 'profile_segment_tags',
    'se': 'site_events',
    # View-on-view aliases (resolved at view-build time, not column-check time)
    'ps': None,    # profile_signals
    'pcnt': None,  # profile_contactability
    'ir': None,    # CTE: identifier_rollup
    'tc': None,    # CTE: top_content
}


def extract_refs(views_sql_text: str, alias: str) -> set[str]:
    """Find every alias.column in the views file (case-sensitive)."""
    return set(re.findall(rf'\b{alias}\.([a-z_][a-z0-9_]*)\b', views_sql_text))


def get_table_columns(client: bigquery.Client, dataset: str, table: str) -> set[str]:
    """Query INFORMATION_SCHEMA.COLUMNS for the given table."""
    sql = f"""
        SELECT column_name
        FROM `{client.project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table_name
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('table_name', 'STRING', table),
        ]
    )
    return {r.column_name for r in client.query(sql, job_config=cfg).result()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--dataset',
        default='profile_data_candidate',
        help='Dataset to validate against (default: profile_data_candidate)',
    )
    parser.add_argument(
        '--project',
        default=PROJECT_ID,
        help=f'GCP project (default: {PROJECT_ID})',
    )
    parser.add_argument(
        '--show-extra',
        action='store_true',
        help='Also print columns present in the table but unused by views',
    )
    args = parser.parse_args()

    if not VIEWS_SQL.exists():
        print(f'[ERROR] views file not found: {VIEWS_SQL}', file=sys.stderr)
        return 2
    views_text = VIEWS_SQL.read_text(encoding='utf-8')

    client = bigquery.Client(project=args.project)
    total_missing = 0
    print(f'Validating profile_database_views.sql column coverage against '
          f'{args.project}.{args.dataset}')
    print('=' * 78)

    for alias, table in ALIAS_TO_TABLE.items():
        refs = extract_refs(views_text, alias)
        if not refs:
            continue
        if table is None:
            # CTE / view-on-view alias -- not a base table
            continue
        try:
            actual = get_table_columns(client, args.dataset, table)
        except Exception as e:
            print(f'[ERROR] could not query {args.dataset}.{table}: {str(e)[:140]}',
                  file=sys.stderr)
            total_missing += 1
            continue
        if not actual:
            print(f'[FAIL] {args.dataset}.{table} does not exist '
                  f'(but views.sql references {len(refs)} columns on alias {alias})')
            total_missing += len(refs)
            continue
        missing = sorted(refs - actual)
        extra = sorted(actual - refs)
        status = 'OK  ' if not missing else 'FAIL'
        print(f'[{status}] {alias:5s} -> {args.dataset}.{table:30s} '
              f'refs={len(refs):3d}  missing={len(missing):2d}  extra={len(extra)}')
        for m in missing:
            print(f'         MISSING: {m}')
        if args.show_extra:
            for e in extra:
                print(f'         extra:   {e}')
        total_missing += len(missing)

    print('=' * 78)
    if total_missing == 0:
        print('PASS: all view column references resolve.')
        return 0
    print(f'FAIL: {total_missing} column reference(s) cannot be resolved.')
    print('Likely fix: add the missing column to sql/populate_identity_core.sql')
    print('(or the relevant populate_*.sql) so the CTAS emits it. Use')
    print('CAST(NULL AS <type>) AS <col> if no concrete value is available yet;')
    print('downstream enrich/personas/restore steps populate values later.')
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
