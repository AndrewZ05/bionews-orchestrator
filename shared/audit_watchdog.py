#!/usr/bin/env python3
# Mutual audit watchdog: each audit checks WHEN ITS SIBLING LAST RAN via the
# sibling dataset's audit_runs table, so a silently-dead audit is surfaced by
# the one still running (no extra scheduler or infrastructure required).
"""Audit watchdog.

``check_sibling_run`` reads MAX(started_at) from the sibling audit's audit_runs
table and returns an ASCII warning string when the last run is older than the
threshold (or when no run has ever been recorded). Best-effort: any query
failure (e.g. the sibling's audit_runs table does not exist yet because that
audit has never run on the new code) returns None -- the watchdog never fails
or noisily pollutes the host audit.
"""

import logging
from datetime import datetime, timezone

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

# Hours after which a sibling audit's silence is flagged. Daily-scheduled audits
# plus generous slack: 36h means one fully-missed day triggers the warning.
DEFAULT_MAX_AGE_HOURS = 36


def check_sibling_run(
    client,
    sibling_dataset: str,
    sibling_label: str,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    runs_table: str = "audit_runs",
):
    """Return a warning string when the sibling audit has not run recently.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        sibling_dataset: the sibling audit's results dataset (validated).
        sibling_label: human label for the warning ('freshness audit' /
            'anomaly audit').
        max_age_hours: silence threshold in hours (default 36).
        runs_table: the sibling's runs table name.

    Returns:
        An ASCII warning string, or None when the sibling ran recently OR the
        check could not be performed (best-effort -- absence of evidence is not
        flagged, only positive evidence of staleness or an empty runs table).
    """
    try:
        safe_dataset = _validate_identifier(sibling_dataset, "dataset")
        safe_table = _validate_identifier(runs_table, "table")
        sql = (
            "-- Watchdog: when did the sibling audit last run?\n"
            "SELECT MAX(started_at) AS last_run FROM `{0}`.`{1}`".format(
                safe_dataset, safe_table
            )
        )
        rows = list(client.query(sql).result())
        last_run = rows[0]["last_run"] if rows else None
        if last_run is None:
            return "WATCHDOG: no {0} run recorded in {1}.{2} yet".format(
                sibling_label, safe_dataset, safe_table
            )
        now = datetime.now(timezone.utc)
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
        age_hours = (now - last_run).total_seconds() / 3600.0
        if age_hours > max_age_hours:
            return (
                "WATCHDOG: {0} has not run for {1:.0f}h "
                "(last {2}, threshold {3}h)".format(
                    sibling_label,
                    age_hours,
                    last_run.strftime("%Y-%m-%d %H:%M UTC"),
                    max_age_hours,
                )
            )
        logger.debug("Watchdog OK: %s last ran %.1fh ago", sibling_label, age_hours)
        return None
    except Exception as exc:  # noqa: BLE001 - watchdog is best-effort
        logger.debug("Watchdog check skipped (%s): %s", sibling_label, exc)
        return None
