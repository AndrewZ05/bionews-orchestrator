#!/usr/bin/env python3
"""
Read-only inventory of the live profile datasets vs the manifest.

Inspects what physically exists in BigQuery for the four profile datasets
(profile_data, profile_data_candidate, profile_ops, profile_staging) and diffs
each against the relations the manifest (shared/profile_database_manifest.py)
declares it should contain. Nothing is written or dropped -- this is the
pre-rebuild "look before you wipe" companion to scripts/profile_hard_reset.py.

For each dataset it prints:
  - every live table/view with its type and row count
  - MISSING:   declared in the manifest but absent in BigQuery
  - UNEXPECTED: present in BigQuery but not declared in the manifest

Exit code is always 0 on a successful inspection (drift is reported, not
failed -- a candidate/staging dataset is legitimately empty between rebuilds).
A nonzero exit means the inventory itself could not run (e.g. no credentials,
project unreachable).

Usage:
  python scripts/profile_dataset_inventory.py
  python scripts/profile_dataset_inventory.py --project bi-data-391216
  python scripts/profile_dataset_inventory.py --dataset profile_data
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from shared.bigquery_client import close_bigquery_client, get_bigquery_client
from shared.profile_database_manifest import (
    OPS_DATASET,
    OPS_TABLE_GROUPS,
    PHYSICAL_TABLE_GROUPS,
    PRODUCTION_DATASET,
    REBUILD_CANDIDATE_DATASET,
    STAGING_DATASET,
    STAGING_TABLE_GROUPS,
    VIEW_GROUPS,
)


DEFAULT_PROJECT = os.getenv("GCP_PROJECT_ID", "bi-data-391216")


def _relations_from_groups(*group_tuples) -> set[str]:
    """Flatten one or more ((label, (relation, ...)), ...) structures to a set."""
    names: set[str] = set()
    for groups in group_tuples:
        for _label, relations in groups:
            names.update(relations)
    return names


# Manifest-declared relations per dataset. The consumer dataset
# (profile_data) holds the physical tables plus the canonical views; the
# candidate dataset mirrors the consumer shape during a rebuild; ops and
# staging hold their own table groups.
EXPECTED_RELATIONS: dict[str, set[str]] = {
    PRODUCTION_DATASET: _relations_from_groups(PHYSICAL_TABLE_GROUPS, VIEW_GROUPS),
    REBUILD_CANDIDATE_DATASET: _relations_from_groups(
        PHYSICAL_TABLE_GROUPS, VIEW_GROUPS
    ),
    OPS_DATASET: _relations_from_groups(OPS_TABLE_GROUPS),
    STAGING_DATASET: _relations_from_groups(STAGING_TABLE_GROUPS),
}

ALL_DATASETS = [
    PRODUCTION_DATASET,
    REBUILD_CANDIDATE_DATASET,
    OPS_DATASET,
    STAGING_DATASET,
]


def dataset_exists(client: bigquery.Client, project_id: str, dataset_name: str) -> bool:
    try:
        client.get_dataset(f"{project_id}.{dataset_name}")
        return True
    except NotFound:
        return False


def list_dataset_objects(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
) -> list[tuple[str, str, int]]:
    """Return (table_name, table_type, row_count) for a dataset.

    Uses the dataset-scoped INFORMATION_SCHEMA.TABLES view (same as
    profile_hard_reset.py, region-agnostic). Row count comes from the cached
    table metadata (num_rows) per base table; views report -1 (no stored rows).
    """
    sql = f"""
    SELECT table_name, table_type
    FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
    ORDER BY
      CASE WHEN table_type LIKE '%VIEW%' THEN 0 ELSE 1 END,
      table_name
    """
    rows = list(client.query(sql).result())

    objects: list[tuple[str, str, int]] = []
    for row in rows:
        is_view = "VIEW" in (row.table_type or "")
        row_count = -1
        if not is_view:
            try:
                tbl = client.get_table(f"{project_id}.{dataset_name}.{row.table_name}")
                row_count = int(tbl.num_rows) if tbl.num_rows is not None else -1
            except Exception:
                row_count = -1
        objects.append((row.table_name, row.table_type, row_count))
    return objects


def inventory_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
) -> dict:
    expected = EXPECTED_RELATIONS.get(dataset_name, set())

    if not dataset_exists(client, project_id, dataset_name):
        print(f"[--] {dataset_name}: dataset does not exist")
        if expected:
            print(f"     MISSING ({len(expected)}): " + ", ".join(sorted(expected)))
        print("")
        return {
            "dataset": dataset_name,
            "exists": False,
            "live": 0,
            "missing": sorted(expected),
            "unexpected": [],
        }

    objects = list_dataset_objects(client, project_id, dataset_name)
    live_names = {name for name, _type, _rows in objects}

    print(f"[OK] {dataset_name}: {len(objects)} live object(s)")
    for name, table_type, row_count in objects:
        count_str = "view" if row_count < 0 else f"{row_count:,} rows"
        print(f"  - {table_type:<18} {name:<40} {count_str}")

    missing = sorted(expected - live_names)
    unexpected = sorted(live_names - expected)
    if missing:
        print(f"  MISSING ({len(missing)}): " + ", ".join(missing))
    if unexpected:
        print(f"  UNEXPECTED ({len(unexpected)}): " + ", ".join(unexpected))
    if not missing and not unexpected and expected:
        print("  (matches manifest)")
    print("")

    return {
        "dataset": dataset_name,
        "exists": True,
        "live": len(objects),
        "missing": missing,
        "unexpected": unexpected,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=ALL_DATASETS,
        help="Inventory only this dataset (repeatable). Default: all four.",
    )
    args = parser.parse_args()

    datasets = args.dataset or ALL_DATASETS

    print("Profile dataset inventory (read-only)")
    print(f"Project : {args.project}")
    print("Datasets: " + ", ".join(datasets))
    print("")

    client = get_bigquery_client(project=args.project)
    try:
        results = [
            inventory_dataset(client, args.project, dataset_name)
            for dataset_name in datasets
        ]
    finally:
        close_bigquery_client()

    total_missing = sum(len(r["missing"]) for r in results)
    total_unexpected = sum(len(r["unexpected"]) for r in results)
    print(
        f"Summary: {len(results)} dataset(s) inspected; "
        f"{total_missing} missing, {total_unexpected} unexpected relation(s)."
    )
    # Drift is informational -- the inventory ran successfully either way.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
