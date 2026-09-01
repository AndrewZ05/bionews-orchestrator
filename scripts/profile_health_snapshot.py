#!/usr/bin/env python3
"""Append one day's profile-database health metrics and report any alerts.

Run daily from cron AFTER the profile build finishes. The metrics are point-in-
time COUNT(*) measurements, so they must be taken against a settled database.

WHY THIS EXISTS
---------------
Nothing in the warehouse records how big the profile database was on a given
day, and every attempt to reconstruct that history after the fact has failed:
created_at is re-stamped by backfills, source_created_at is survivorship-biased
through the live source tables, profile_core_snapshot retained only 9 days, and
bn_id_metrics stores per-run work volume rather than cumulative totals.

A trustworthy history cannot be derived retroactively -- it has to be recorded.
Each run appends and never rewrites, so tomorrow's history is correct even
though yesterday's is unrecoverable. Every day this does not run is a day
permanently missing from the record.

It also watches for a regression that already happened once: the date fill used
to be a one-off script, so profiles created afterwards had no date. Coverage
slid from 99.6% to 99.4% over two days with 66,780 profiles undated, and nothing
surfaced it -- it was found by manual inspection. date_coverage.* and
integrity.* now make that visible the next morning.

EXIT CODES
----------
    0  metrics written and clean -- OR skipped because a build was running
    1  failed to write metrics (cron should surface this)
    2  metrics written but one or more alerts are firing

Exit 2 is deliberately distinct: the ledger succeeded, so the run is not broken,
but a human needs to look. Alert monitors can treat 1 and 2 differently.

A skip also exits 0 because it is not a failure -- nothing was wrong, the
timing was just unlucky. The skip is stated in the log, and the missing day is
visible in the ledger itself, so it does not hide.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load .env exactly as orchestrate.py:30 does. Under cron the environment is
# stripped, so GOOGLE_APPLICATION_CREDENTIALS is unset and the client falls back
# to the VM's default service account -- which carries a read-only OAuth scope
# and fails the very first CREATE TABLE with:
#   403 Access Denied: Missing required OAuth scope
# Loading .env here makes the cron environment match the interactive one.
try:
    import dotenv

    dotenv.load_dotenv(REPO_ROOT / ".env")
except ImportError:  # dotenv absent -- rely on an externally-set env var
    pass

SQL_PATH = REPO_ROOT / "sql" / "profile_database_health_ledger.sql"

logger = logging.getLogger("profile_health_snapshot")


def _statements(sql_text: str) -> list[str]:
    """Split the ledger file into executable statements.

    Comment-only lines are stripped first so that a `;` inside a comment cannot
    split a statement in the middle.
    """
    body = "\n".join(
        line for line in sql_text.splitlines() if not line.strip().startswith("--")
    )
    return [s.strip() for s in body.split(";") if s.strip()]


# Minutes after which a `running` row is treated as a crashed build rather than
# a live one. Matches the pipeline's own definition
# (plugins/profile_database_extractor.py:1775), so a row left behind by a crash
# cannot block monitoring indefinitely.
STALE_BUILD_MINUTES = 90

ACTIVE_BUILD_SQL = """
SELECT build_id, started_at,
       TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), started_at, MINUTE) AS age_min
FROM profile_ops.profile_build_runs
WHERE status = 'running'
  AND started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {stale} MINUTE)
ORDER BY started_at DESC
LIMIT 1
""".format(stale=STALE_BUILD_MINUTES)


def pick_blocker(rows) -> dict | None:
    """Decide whether a build row should stop this snapshot.

    Split from the query so the decision can be tested against fixtures.
    Writing fake `running` rows into profile_ops.profile_build_runs to test this
    is NOT safe: the pipeline's own preflight reads that table, and on
    2026-08-18 a test row existing for ten seconds blocked the scheduled 08:01
    build. The table is shared state, not a test surface.

    Rows are expected to be pre-filtered by ACTIVE_BUILD_SQL (status=running,
    within the staleness window); this returns the first, or None.
    """
    for row in rows:
        return {
            "build_id": row["build_id"],
            "started_at": str(row["started_at"])[:19],
            "age_min": row["age_min"],
        }
    return None


def active_build(client) -> dict | None:
    """Return the in-flight build blocking this run, or None."""
    rows = [dict(r) for r in client.query(ACTIVE_BUILD_SQL).result()]
    return pick_blocker(rows)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    try:
        from google.cloud import bigquery
    except ImportError:
        logger.error("google-cloud-bigquery not installed -- use the venv interpreter")
        return 1

    if not SQL_PATH.exists():
        logger.error("Ledger SQL not found: %s", SQL_PATH)
        return 1

    # Fail with a clear reason rather than a 403 midway through. Without
    # credentials the client silently falls back to the VM default service
    # account, whose OAuth scope is read-only.
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds:
        logger.error(
            "GOOGLE_APPLICATION_CREDENTIALS is not set and no .env supplied it. "
            "Under cron the environment is stripped; the VM default service "
            "account has a read-only scope and cannot write the ledger."
        )
        return 1
    if not Path(creds).exists():
        logger.error("Credentials file does not exist: %s", creds)
        return 1
    logger.info("Using credentials: %s", creds)

    client = bigquery.Client()

    blocker = active_build(client)
    if blocker is not None:
        logger.info(
            "Profile build %s is running (started %s, %s min ago) -- skipping "
            "snapshot so metrics are not taken mid-build. Exit 0; the next "
            "scheduled run will record settled state.",
            blocker["build_id"][:8],
            blocker["started_at"],
            blocker["age_min"],
        )
        return 0

    # The INSERT carries its own `WHERE NOT EXISTS (... measured_on = CURRENT_DATE())`
    # guard, so a second run on the same day writes nothing rather than
    # duplicating the day. The CREATE statements are IF NOT EXISTS / OR REPLACE.
    for stmt in _statements(SQL_PATH.read_text(encoding="utf-8")):
        label = " ".join(stmt.split())[:70]
        try:
            job = client.query(stmt)
            job.result()
            if job.num_dml_affected_rows is not None:
                logger.info("OK (%s rows): %s", job.num_dml_affected_rows, label)
            else:
                logger.info("OK: %s", label)
        except Exception as exc:  # noqa: BLE001 -- cron needs the reason in the log
            logger.error("FAILED: %s -- %s", label, str(exc)[:400])
            return 1

    alerts = list(
        client.query(
            "SELECT metric_name, dimension, metric_value, alert "
            "FROM profile_ops.profile_health_alerts"
        ).result()
    )

    if not alerts:
        logger.info("Health check clean -- no alerts firing")
        return 0

    logger.warning("%d alert(s) firing:", len(alerts))
    for row in alerts:
        logger.warning(
            "  [%s/%s] value=%s -- %s",
            row.metric_name,
            row.dimension,
            row.metric_value,
            row.alert,
        )
    return 2


if __name__ == "__main__":
    sys.exit(main())
