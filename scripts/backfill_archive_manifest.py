#!/usr/bin/env python3
"""
One-time maintenance: index legacy per-job archive external tables in the
staging archive manifest, then set a drain expiration on them.

Background: shared/gcs_pipeline.py STEP 5 created external tables named
{table}_{execution_uuid}_{job8} in {source}_archive with "TTL: 30 days" in
the description but no actual expiration -- hundreds accumulated. This script:

  1. Parses each legacy external table name into table/execution/job ids.
  2. Writes a manifest row (orchestrator_monitoring.staging_archive_manifest)
     with the table's GCS parquet URIs so the data stays discoverable from
     BigQuery after the table definition expires.
  3. Sets expiration = now + --drain-days (default 90) on tables that have
     none. External table expiry drops only the BQ pointer; the parquet in
     GCS is untouched and can be re-externalized from the manifest URIs.

Usage:
  python scripts/backfill_archive_manifest.py --source mailchimp [--dry-run]
  python scripts/backfill_archive_manifest.py --source mailchimp --drain-days 90
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

os.environ.setdefault("GCP_PROJECT_ID", "bi-data-391216")

from shared.bigquery_client import get_bigquery_client  # noqa: E402

# {table}_{execution-uuid}_{8-hex-job-suffix}
LEGACY_NAME_RE = re.compile(
    r"^(?P<table>.+?)_"
    r"(?P<exec_uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})_"
    r"(?P<job8>[0-9a-f]{8})$"
)

# Older shape: {table}_{actual_table_name}_{8-hex-job-suffix}, e.g.
# members_archived_mailchimp_members_archived_009afecc (no execution uuid).
LEGACY_NAME_FALLBACK_RE = re.compile(r"^(?P<table>.+)_(?P<job8>[0-9a-f]{8})$")

MANIFEST = "orchestrator_monitoring.staging_archive_manifest"


def parse_legacy_name(table_id: str, source: str):
    """Return (logical_table, execution_id, job_id) or None if not legacy."""
    m = LEGACY_NAME_RE.match(table_id)
    if m:
        return (
            m.group("table"),
            m.group("exec_uuid"),
            f"{m.group('exec_uuid')}_{m.group('job8')}",
        )
    if "__" in table_id:
        return None  # staging_lifecycle snapshots ({table}__{ts}_{exec8})
    m = LEGACY_NAME_FALLBACK_RE.match(table_id)
    if m:
        table = m.group("table")
        # Collapse {logical}_{source}_{logical} to {logical}
        dup = re.match(rf"^(?P<a>.+)_{re.escape(source)}_(?P<b>.+)$", table)
        if dup and dup.group("a") == dup.group("b"):
            table = dup.group("a")
        return (table, None, m.group("job8"))
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Source name (e.g. mailchimp)")
    parser.add_argument(
        "--drain-days",
        type=int,
        default=90,
        help="Days until legacy archive tables expire (default 90)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report what would happen; no writes"
    )
    args = parser.parse_args()

    client = get_bigquery_client()
    archive_dataset = f"{args.source}_archive"
    expires_at = datetime.now(timezone.utc) + timedelta(days=args.drain_days)

    manifest_rows = []
    to_expire = []
    skipped = []

    for t in client.list_tables(f"{client.project}.{archive_dataset}"):
        parsed = parse_legacy_name(t.table_id, args.source)
        if not parsed:
            skipped.append(t.table_id)  # snapshots ({table}__ts_exec) and other shapes
            continue
        logical_table, exec_uuid, job_id = parsed
        tbl = client.get_table(f"{client.project}.{archive_dataset}.{t.table_id}")
        uris = []
        if tbl.external_data_configuration:
            uris = list(tbl.external_data_configuration.source_uris or [])
        manifest_rows.append(
            {
                "execution_id": exec_uuid,
                "job_id": job_id,
                "source": args.source,
                "group_name": None,
                "table_name": logical_table,
                "staging_table": f"{client.project}.{args.source}_staging.{logical_table}",
                "archive_table": f"{client.project}.{archive_dataset}.{t.table_id}",
                "gcs_parquet_uris": uris,
                "row_count": None,
                "archived_at": (tbl.created or datetime.now(timezone.utc)).isoformat(),
                "expires_at": expires_at.isoformat(),
                "archive_method": "EXTERNAL",
            }
        )
        if tbl.expires is None:
            to_expire.append(tbl)

    print(f"Legacy externals matched: {len(manifest_rows)}")
    print(f"Need expiration set:      {len(to_expire)}")
    print(f"Skipped (non-legacy):     {len(skipped)}")

    if args.dry_run:
        for r in manifest_rows[:3]:
            print(
                f"  sample: {r['archive_table']} uris={len(r['gcs_parquet_uris'])} "
                f"archived_at={r['archived_at'][:10]}"
            )
        print("Dry run: no writes performed.")
        return

    # Skip rows already backfilled (idempotent re-runs)
    existing = {
        row.archive_table
        for row in client.query(
            f"SELECT DISTINCT archive_table FROM `{MANIFEST}` WHERE source = '{args.source}'"
        ).result()
    }
    new_rows = [r for r in manifest_rows if r["archive_table"] not in existing]
    print(f"Manifest rows to insert (after dedup): {len(new_rows)}")

    inserted = 0
    for i in range(0, len(new_rows), 200):
        chunk = new_rows[i : i + 200]
        errors = client.insert_rows_json(f"{client.project}.{MANIFEST}", chunk)
        if errors:
            print(f"  INSERT ERRORS in chunk {i // 200}: {errors[:2]}")
        else:
            inserted += len(chunk)
    print(f"Manifest rows inserted: {inserted}")

    expired = 0
    for tbl in to_expire:
        tbl.expires = expires_at
        client.update_table(tbl, ["expires"])
        expired += 1
        if expired % 100 == 0:
            print(f"  expirations set: {expired}/{len(to_expire)}")
    print(f"Expirations set: {expired} (drain on {expires_at:%Y-%m-%d})")


if __name__ == "__main__":
    main()
