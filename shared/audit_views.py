#!/usr/bin/env python3
# Convenience BigQuery views over the audit results tables, so table health is
# queryable in ONE place without re-deriving "latest" logic ad hoc:
#   v_latest_freshness           -- the latest freshness row per table
#   v_latest_anomaly_run         -- every row of the most recent anomaly run
#                                   (renamed from v_latest_anomaly; honest about
#                                   what it actually contains)
#   v_latest_anomaly_per_detector -- one row per (table, detector) from the most
#                                    recent finding for that combination, via
#                                    QUALIFY ROW_NUMBER(). The "per-table latest"
#                                    semantic dashboards usually want.
#   v_table_health               -- one row per table: latest freshness status
#                                   joined with the latest run's anomaly rollup
#                                   (active count, max sev)
"""Audit views: idempotent CREATE OR REPLACE VIEW DDL for the audit datasets.

Called best-effort by the audit runners after their results tables are ensured:
the freshness runner creates v_latest_freshness and the cross-audit
v_table_health (it runs daily and owns the "health" framing); the anomaly runner
creates v_latest_anomaly_run and v_latest_anomaly_per_detector. A view-creation
failure is logged and swallowed -- views are conveniences, never a reason to
fail an audit run.

All identifiers pass the shared allowlist before interpolation; the SQL uses
only double-dash line comments. ASCII-only log strings.
"""

import logging

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

# v_latest_freshness: the most recent freshness row per audited table.
_LATEST_FRESHNESS_VIEW = """-- v_latest_freshness: latest freshness row per table.
CREATE OR REPLACE VIEW `{project}`.`{fds}`.`v_latest_freshness` AS
SELECT *
FROM `{project}`.`{fds}`.`{frt}`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dataset_name, table_name ORDER BY audited_at DESC
) = 1"""

# v_latest_anomaly_run: every row of the most recent anomaly run.
# Renamed from v_latest_anomaly to be semantically honest: this is the
# latest-RUN snapshot, not the "most recent finding per (table, detector)"
# the prior name implied. The cross-audit v_table_health joins against this
# one because the email's framing is "what's the platform's status as of the
# most recent run."
_LATEST_ANOMALY_RUN_VIEW = """-- v_latest_anomaly_run: all rows of the most recent anomaly run.
CREATE OR REPLACE VIEW `{project}`.`{ads}`.`v_latest_anomaly_run` AS
SELECT *
FROM `{project}`.`{ads}`.`{art}`
WHERE audit_run_id = (
  SELECT audit_run_id FROM `{project}`.`{ads}`.`{art}`
  ORDER BY audited_at DESC LIMIT 1
)"""

# v_latest_anomaly_per_detector: the most recent finding for each (dataset,
# table, detector) combination -- across all history, not just the latest run.
# Dashboards usually want this semantic: "show the current state of every
# detector per table, even if a HIGH-severity finding came from an older run
# that the latest run did not re-fire." Uses QUALIFY ROW_NUMBER so a table
# whose detector saw 5 historical findings shows up exactly once.
_LATEST_ANOMALY_PER_DETECTOR_VIEW = """-- v_latest_anomaly_per_detector: latest finding per (table, detector).
CREATE OR REPLACE VIEW `{project}`.`{ads}`.`v_latest_anomaly_per_detector` AS
SELECT *
FROM `{project}`.`{ads}`.`{art}`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dataset_name, table_name, detector
  ORDER BY audited_at DESC
) = 1"""

# v_table_health: one row per table joining the two latest views.
_TABLE_HEALTH_VIEW = """-- v_table_health: latest freshness status per table joined with the latest
-- anomaly run's ACTIVE rollup. One row per table; NULL anomaly columns mean the
-- table had no row in the latest anomaly run (and vice versa for freshness).
CREATE OR REPLACE VIEW `{project}`.`{fds}`.`v_table_health` AS
WITH f AS (
  SELECT dataset_name, table_name, audit_status AS freshness_status,
         days_behind, most_recent_date, date_column_used,
         audited_at AS freshness_audited_at
  FROM `{project}`.`{fds}`.`v_latest_freshness`
),
a AS (
  SELECT dataset_name, table_name,
         COUNTIF(audit_status = 'ANOMALY' AND scope = 'ACTIVE') AS active_anomalies,
         MIN(CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2
             WHEN 'LOW' THEN 3 END) AS sev_rank
  FROM `{project}`.`{ads}`.`v_latest_anomaly_run`
  GROUP BY dataset_name, table_name
)
SELECT
  COALESCE(f.dataset_name, a.dataset_name) AS dataset_name,
  COALESCE(f.table_name, a.table_name) AS table_name,
  f.freshness_status,
  f.days_behind,
  f.most_recent_date,
  f.date_column_used,
  f.freshness_audited_at,
  IFNULL(a.active_anomalies, 0) AS active_anomalies,
  CASE a.sev_rank WHEN 1 THEN 'HIGH' WHEN 2 THEN 'MEDIUM' WHEN 3 THEN 'LOW'
  END AS max_anomaly_severity
FROM f
FULL OUTER JOIN a
  ON f.dataset_name = a.dataset_name AND f.table_name = a.table_name"""


def _run_ddl(client, sql, view_label):
    """Execute one view DDL, best-effort. Returns True on success."""
    try:
        client.query(sql).result()
        logger.info("Ensured view %s", view_label)
        return True
    except Exception as exc:  # noqa: BLE001 - views are conveniences
        logger.warning("Could not ensure view %s: %s", view_label, exc)
        return False


def ensure_freshness_views(
    client,
    freshness_dataset,
    anomaly_dataset,
    freshness_table="table_freshness_results",
):
    """Create/refresh v_latest_freshness and the cross-audit v_table_health.

    Called by the freshness runner. v_table_health references v_latest_anomaly_run
    in the anomaly dataset; if that view does not exist yet (anomaly audit has
    never run), its DDL fails and is swallowed -- it will succeed on the next
    freshness run after the anomaly audit has created its view.
    """
    try:
        project = _validate_identifier(client.project, "project")
        fds = _validate_identifier(freshness_dataset, "dataset")
        ads = _validate_identifier(anomaly_dataset, "dataset")
        frt = _validate_identifier(freshness_table, "table")
    except Exception as exc:  # noqa: BLE001 - views are conveniences
        logger.warning("Skipping freshness views (bad identifier): %s", exc)
        return

    _run_ddl(
        client,
        _LATEST_FRESHNESS_VIEW.format(project=project, fds=fds, frt=frt),
        "{0}.v_latest_freshness".format(fds),
    )
    _run_ddl(
        client,
        _TABLE_HEALTH_VIEW.format(project=project, fds=fds, ads=ads),
        "{0}.v_table_health".format(fds),
    )


def ensure_anomaly_views(client, anomaly_dataset, anomaly_table="anomaly_results"):
    """Create/refresh v_latest_anomaly_run and v_latest_anomaly_per_detector.

    Called by the anomaly runner. The two views are semantically distinct:
    v_latest_anomaly_run answers "what did the most recent run say" (consumed
    by v_table_health); v_latest_anomaly_per_detector answers "what is each
    detector's current state per table, across all history" (for dashboards
    and ad-hoc queries). Both are best-effort.
    """
    try:
        project = _validate_identifier(client.project, "project")
        ads = _validate_identifier(anomaly_dataset, "dataset")
        art = _validate_identifier(anomaly_table, "table")
    except Exception as exc:  # noqa: BLE001 - views are conveniences
        logger.warning("Skipping anomaly views (bad identifier): %s", exc)
        return

    _run_ddl(
        client,
        _LATEST_ANOMALY_RUN_VIEW.format(project=project, ads=ads, art=art),
        "{0}.v_latest_anomaly_run".format(ads),
    )
    _run_ddl(
        client,
        _LATEST_ANOMALY_PER_DETECTOR_VIEW.format(project=project, ads=ads, art=art),
        "{0}.v_latest_anomaly_per_detector".format(ads),
    )
    # One-shot cleanup of the legacy view name. Older projects have a
    # v_latest_anomaly view pointing at the old definition; CREATE OR REPLACE
    # does not delete it, so it lingers orphaned. DROP IF EXISTS is idempotent
    # and best-effort; once dropped on every project this can be removed.
    _run_ddl(
        client,
        "DROP VIEW IF EXISTS `{0}`.`{1}`.`v_latest_anomaly`".format(project, ads),
        "{0}.v_latest_anomaly (legacy drop)".format(ads),
    )
