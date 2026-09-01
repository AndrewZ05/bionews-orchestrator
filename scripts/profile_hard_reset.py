#!/usr/bin/env python3
"""
Hard-reset the profile datasets for a clean-slate rebuild.

Default behavior clears:
  - profile_data
  - profile_data_candidate
  - profile_staging

Optional:
  --include-ops  also clears profile_ops build history / snapshots / lineage
  --dry-run      show what would be dropped without deleting anything

This is intended for one-time pre-release resets where preserving the previous
profile release is not valuable. Clearing profile_data before rebuild also
ensures the snapshot bridge does not carry forward legacy app-written fields.

Before wiping, optionally inspect live objects (read-only):
  python scripts/profile_dataset_inventory.py
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
    PRODUCTION_DATASET,
    REBUILD_CANDIDATE_DATASET,
    STAGING_DATASET,
)


DEFAULT_PROJECT = os.getenv("GCP_PROJECT_ID", "bi-data-391216")


def get_bq_client(project_id: str) -> bigquery.Client:
    return get_bigquery_client(project=project_id)


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
) -> list[tuple[str, str]]:
    sql = f"""
    SELECT table_name, table_type
    FROM `{project_id}.{dataset_name}.INFORMATION_SCHEMA.TABLES`
    ORDER BY
      CASE
        WHEN table_type LIKE '%VIEW%' THEN 0
        ELSE 1
      END,
      table_name
    """
    rows = list(client.query(sql).result())
    return [(row.table_name, row.table_type) for row in rows]


def drop_relation(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    relation_name: str,
    relation_type: str,
) -> None:
    fq = f"`{project_id}.{dataset_name}.{relation_name}`"
    if relation_type == "VIEW":
        sql = f"DROP VIEW IF EXISTS {fq}"
    elif relation_type == "MATERIALIZED VIEW":
        sql = f"DROP MATERIALIZED VIEW IF EXISTS {fq}"
    else:
        sql = f"DROP TABLE IF EXISTS {fq}"
    client.query(sql).result()


def summarize_objects(dataset_name: str, objects: list[tuple[str, str]]) -> None:
    if not objects:
        print(f"[OK] {dataset_name}: already empty")
        return

    print(f"[PLAN] {dataset_name}: {len(objects)} object(s)")
    for relation_name, relation_type in objects:
        print(f"  - {relation_type:<18} {relation_name}")


def clear_dataset(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    dry_run: bool,
) -> dict:
    if not dataset_exists(client, project_id, dataset_name):
        return {
            "dataset": dataset_name,
            "exists": False,
            "objects_found": 0,
            "objects_dropped": 0,
            "objects_remaining": 0,
            "dropped": [],
        }

    objects = list_dataset_objects(client, project_id, dataset_name)
    summarize_objects(dataset_name, objects)

    if dry_run:
        return {
            "dataset": dataset_name,
            "exists": True,
            "objects_found": len(objects),
            "objects_dropped": 0,
            "objects_remaining": len(objects),
            "dropped": [],
        }

    dropped = []
    for relation_name, relation_type in objects:
        drop_relation(client, project_id, dataset_name, relation_name, relation_type)
        dropped.append((relation_name, relation_type))

    remaining = list_dataset_objects(client, project_id, dataset_name)
    return {
        "dataset": dataset_name,
        "exists": True,
        "objects_found": len(objects),
        "objects_dropped": len(dropped),
        "objects_remaining": len(remaining),
        "dropped": dropped,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--include-ops", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    datasets = [
        PRODUCTION_DATASET,
        REBUILD_CANDIDATE_DATASET,
        STAGING_DATASET,
    ]
    if args.include_ops:
        datasets.append(OPS_DATASET)

    print("Profile hard reset starting")
    print(f"Project : {args.project}")
    print("Datasets: " + ", ".join(datasets))
    print(f"Mode    : {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    print("")

    client = get_bq_client(args.project)
    try:
        results = []
        for dataset_name in datasets:
            result = clear_dataset(
                client,
                args.project,
                dataset_name,
                dry_run=args.dry_run,
            )
            results.append(result)
            if not result["exists"]:
                print(f"[OK] {dataset_name}: dataset does not exist yet")
            elif args.dry_run:
                print(
                    f"[OK] {dataset_name}: would clear {result['objects_found']} object(s)"
                )
            else:
                print(
                    f"[OK] {dataset_name}: dropped {result['objects_dropped']} object(s); "
                    f"{result['objects_remaining']} remaining"
                )
            print("")
    finally:
        close_bigquery_client()

    if args.dry_run:
        print("Dry run complete.")
        print(
            "Re-run without --dry-run to clear the datasets before the first beta rebuild."
        )
        return 0

    print("Hard reset complete.")
    print("")
    print("Recommended next steps:")
    print("  1. python orchestrate.py --source profile_database --env prod --build-mode rebuild")
    print("  2. python scripts/persona_snapshot_check.py")
    print("  3. python scripts/profile_release_check.py")
    if not args.include_ops:
        print("")
        print(
            f"Note: {OPS_DATASET} was preserved. Add --include-ops if you also want "
            "a completely fresh build-history / snapshot / lineage layer for beta."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
