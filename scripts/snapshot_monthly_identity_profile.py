#!/usr/bin/env python3
"""
Monthly forensic snapshots of the identity hub and profile database.

WHY THIS EXISTS
---------------
Two incidents were diagnosed late, after the evidence had already expired:

  2026-05-24  bn_id_xref fell 19,212,694 -> 17,062,095 (-11.2%) and never
              recovered. Not investigated until June, by which point the
              pre-incident data was gone.
  2026-08-07  a full hub rebuild aged out 2,085,967 anonymous identities under
              a 90-day effective lifetime nobody had approved. Recoverable only
              because an operator happened to take an ad-hoc backup the day
              before (identity_hub_backup_20260806).

Relying on someone taking a manual backup before the thing goes wrong is not a
recovery plan. BigQuery time travel is 7 days, and profile_core_snapshot holds
9 non-consecutive days ending 2026-06-21, so a question asked one month late
currently cannot be answered at all.

WHAT IT DOES
------------
Takes a BigQuery table snapshot of each critical table on a monthly cadence.
Snapshots are metadata-only at creation and bill solely for data that later
diverges from the base table, so the true cost is a small fraction of the
27.2 GB these tables occupy (~$0.54/month if they diverged completely).

Retention is 13 months, matching the identity retention policy set 2026-08-07,
so any month-over-month comparison the graph itself supports can also be
audited against real data.

Idempotent: re-running in the same month is a no-op unless --force is given.

USAGE
-----
  python scripts/snapshot_monthly_identity_profile.py --env prod --dry-run
  python scripts/snapshot_monthly_identity_profile.py --env prod
  python scripts/snapshot_monthly_identity_profile.py --env prod --list

CRON
----
Runs 13:30 server time (EDT) on the 1st of each month, twelve times a year.
That is roughly five hours after the daily pipeline kicks off at 08:01 EDT
(12:01 UTC) and well clear of its ~32 minute run, so the snapshot captures a
settled state rather than a half-written one.

Must be ONE line. cron's command field runs to end of line and does not honour
backslash continuation, so a wrapped entry silently becomes a broken one. The
PATH prefix and absolute interpreter path follow the working convention in
docs/PROFILE_HEALTH_MONITORING.md; cron's default PATH is too bare to find the
venv. Logs go under /home/orchestrator/logs/cron/ because the cron user cannot
write to /var/log/ and the redirect fails before Python starts.

  30 13 1 * * cd /home/orchestrator && PATH=/snap/bin:/usr/bin:/bin /home/orchestrator/venv/bin/python scripts/snapshot_monthly_identity_profile.py --env prod >> /home/orchestrator/logs/cron/monthly_snapshot.log 2>&1
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.bigquery_client import setup_gcp_credentials  # noqa: E402

# Resolve service-account credentials BEFORE the client is created. Without
# this, ADC falls through to the VM metadata token, whose OAuth scopes do not
# include BigQuery -- the cron and any bare invocation then fail with
# ACCESS_TOKEN_SCOPE_INSUFFICIENT (seen 2026-08-20).
setup_gcp_credentials()

from google.cloud import bigquery  # noqa: E402
from google.cloud.exceptions import Conflict, NotFound  # noqa: E402

ENV_TO_PROJECT = {
    "prod": "bi-data-391216",
    "dev": "bi-dev-391216",
    "test": "bi-dev-391216",
}

SNAPSHOT_DATASET = "platform_monthly_snapshots"
RETENTION_MONTHS = 13

# The tables worth being able to look back at. bn_id_xref is first because it
# is the input contract for profile_database and the table both incidents hit.
SNAPSHOT_TABLES = [
    ("identity_hub_data", "bn_id_xref"),
    ("identity_hub_data", "bn_id_hub"),
    ("identity_hub_data", "bn_id_node_index"),
    ("identity_hub_data", "bn_id_manifest"),
    ("identity_hub_data", "bn_id_metrics"),
    ("profile_data", "profile_core"),
]

logger = logging.getLogger("monthly_snapshot")


def _snapshot_name(table: str, stamp: str) -> str:
    return f"{table}_{stamp}"


def _ensure_dataset(client: bigquery.Client, project: str) -> None:
    ref = f"{project}.{SNAPSHOT_DATASET}"
    try:
        client.get_dataset(ref)
    except NotFound:
        ds = bigquery.Dataset(ref)
        ds.location = "US"
        ds.description = (
            "Monthly forensic snapshots of identity hub and profile tables. "
            "Created by scripts/snapshot_monthly_identity_profile.py. "
            f"Retention {RETENTION_MONTHS} months."
        )
        client.create_dataset(ds)
        logger.info(f"Created dataset {ref}")


def _list_snapshots(client: bigquery.Client, project: str) -> int:
    try:
        tables = sorted(
            client.list_tables(f"{project}.{SNAPSHOT_DATASET}"),
            key=lambda t: t.table_id,
        )
    except NotFound:
        logger.info(f"No snapshot dataset yet ({SNAPSHOT_DATASET})")
        return 0
    if not tables:
        logger.info("No snapshots recorded")
        return 0
    logger.info(f"{'snapshot':<44}{'rows':>14}  expires")
    for t in tables:
        full = client.get_table(f"{project}.{SNAPSHOT_DATASET}.{t.table_id}")
        expires = str(full.expires)[:10] if full.expires else "never"
        logger.info(f"{t.table_id:<44}{full.num_rows:>14,}  {expires}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, choices=tuple(ENV_TO_PROJECT))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be created; make no changes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace this month's snapshot if it already exists",
    )
    parser.add_argument(
        "--list",
        dest="do_list",
        action="store_true",
        help="List existing snapshots and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    project = ENV_TO_PROJECT[args.env]
    client = bigquery.Client(project=project)

    if args.do_list:
        return _list_snapshots(client, project)

    stamp = date.today().strftime("%Y%m")
    expiry = datetime.now(timezone.utc) + timedelta(days=RETENTION_MONTHS * 31)

    if args.dry_run:
        logger.info(f"DRY RUN -- no changes will be made (stamp {stamp})")
    else:
        _ensure_dataset(client, project)

    created, skipped, failed = 0, 0, 0

    for source_dataset, table in SNAPSHOT_TABLES:
        target = _snapshot_name(table, stamp)
        source_ref = f"{project}.{source_dataset}.{table}"
        target_ref = f"{project}.{SNAPSHOT_DATASET}.{target}"

        try:
            src = client.get_table(source_ref)
        except NotFound:
            logger.error(f"SKIP {table}: source {source_ref} not found")
            failed += 1
            continue

        if args.dry_run:
            logger.info(
                f"would snapshot {source_dataset}.{table} "
                f"({src.num_rows:,} rows) -> {target}"
            )
            created += 1
            continue

        # BigQuery does not accept OR REPLACE on CREATE SNAPSHOT TABLE at all
        # ("OR REPLACE is not allowed with CREATE SNAPSHOT TABLE"), so --force
        # has to drop first. DROP SNAPSHOT TABLE is the matching verb; plain
        # DROP TABLE will not remove a snapshot.
        if args.force:
            try:
                client.query(f"DROP SNAPSHOT TABLE IF EXISTS `{target_ref}`").result()
            except Exception as e:
                logger.error(
                    f"FAILED {table}: could not drop existing -- {str(e)[:150]}"
                )
                failed += 1
                continue

        sql = (
            f"CREATE SNAPSHOT TABLE `{target_ref}` "
            f"CLONE `{source_ref}` "
            f"OPTIONS(expiration_timestamp = TIMESTAMP '{expiry:%Y-%m-%d %H:%M:%S} UTC')"
        )
        try:
            client.query(sql).result()
            # Verify the snapshot actually exists and carries the same row
            # count as the source. A snapshot that silently failed to
            # materialise is exactly the failure mode this cron exists to
            # prevent, so treat any mismatch as a hard failure.
            snap = client.get_table(target_ref)
            if snap.num_rows != src.num_rows:
                logger.error(
                    f"FAILED {table}: snapshot row count {snap.num_rows:,} "
                    f"!= source {src.num_rows:,}"
                )
                failed += 1
                continue
            logger.info(f"Snapshot {target}: {src.num_rows:,} rows (verified)")
            created += 1
        except Conflict:
            logger.info(f"Snapshot {target} already exists -- skipping")
            skipped += 1
        except Exception as e:
            msg = str(e)
            if "Already Exists" in msg or "already exists" in msg:
                logger.info(f"Snapshot {target} already exists -- skipping")
                skipped += 1
            else:
                logger.error(f"FAILED {table}: {msg[:200]}")
                failed += 1

    verb = "would create" if args.dry_run else "created"
    logger.info(f"Done: {verb} {created}, skipped {skipped}, failed {failed}")
    if failed and not args.dry_run:
        _send_failure_email(failed, created, skipped)
    return 1 if failed else 0


def _send_failure_email(failed: int, created: int, skipped: int) -> None:
    # Reuse the orchestrator's alerting path so a broken monthly snapshot
    # surfaces in the same inbox as a broken pipeline instead of dying in a
    # log nobody reads until the next incident. Email failure must never mask
    # the non-zero exit code, hence the broad catch.
    try:
        import yaml as _yaml

        from shared.monitoring import send_job_failure_alert

        cfg_path = Path(__file__).resolve().parents[1] / "configs" / "defaults.yaml"
        cfg = _yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        send_job_failure_alert(
            job_id=f"monthly_snapshot_{datetime.now(timezone.utc):%Y%m%d}",
            source="monthly_snapshot",
            error_message=(
                f"Monthly identity/profile snapshot FAILED: {failed} table(s) "
                f"failed, {created} created, {skipped} skipped. See "
                "/home/orchestrator/logs/cron/monthly_snapshot.log. Without "
                "this snapshot the next incident may be unrecoverable."
            ),
            config=cfg,
        )
    except Exception as e:  # noqa: BLE001
        logger.error(f"Could not send failure alert email: {e}")


if __name__ == "__main__":
    sys.exit(main())
