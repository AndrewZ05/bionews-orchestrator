#!/usr/bin/env python3
# Preventive detectors: DUPLICATE KEYS + NULL-RATE, sharing ONE probe query per
# table (single columnar scan) so the checks stay cheap on 90M-row tables:
#   * duplicate_keys -- COUNT vs COUNT(DISTINCT pk) over the recent window. A
#     merge/dedup bug shows up here days before anyone notices downstream.
#   * null_rate -- the date column's NULL share, recent window vs all-history
#     baseline. A column drifting from 2% to 60% NULL signals an upstream change
#     before volume or freshness move (an all-NULL date column once broke the
#     freshness probe itself).
"""Data-quality detector (duplicate primary keys + NULL-rate drift).

``build_quality_sql`` emits one aggregate query computing, in a single scan:
total rows, all-history NULL count for the date column, recent-window row count,
recent NULL count, and the recent COUNT(DISTINCT <pk concat>). The
classification functions are PURE so they unit-test without BigQuery.

Identifiers (date column, every PK column) are validated before interpolation;
@days is bound as a scalar parameter; -- comments only.
"""

import logging
from typing import Any, Dict, List

from shared.anomaly_audit import (
    DETECTOR_DUPLICATE_KEYS,
    DETECTOR_NULL_RATE,
    SCOPE_ACTIVE,
    SEVERITY_HIGH,
    SEVERITY_MEDIUM,
    STATUS_ANOMALY,
    _validate_identifier,
)

logger = logging.getLogger(__name__)

# Defaults (overridable via the audit config / per-table anomaly_overrides):
DEFAULT_RECENT_DAYS = 7  # window for the dup check + recent null rate
DEFAULT_DUP_TOLERANCE = 0.001  # >0.1% duplicate keys in the window -> HIGH
DEFAULT_NULL_JUMP = 0.20  # recent null-rate exceeding baseline by >20pts
DEFAULT_NULL_MIN_ROWS = 100  # need this many recent rows to judge null rate


def build_quality_sql(project_id, dataset_name, table_name, date_column, pk_columns):
    """Build the single-scan quality probe for one table.

    pk_columns may be empty: the DISTINCT-keys expression is then omitted and
    only the null-rate aggregates are computed.
    """
    safe_project = _validate_identifier(project_id, "project")
    safe_dataset = _validate_identifier(dataset_name, "dataset")
    safe_table = _validate_identifier(table_name, "table")
    safe_date = _validate_identifier(date_column, "date_column")
    safe_pks = [_validate_identifier(c, "date_column") for c in (pk_columns or [])]

    recent = "DATE(`{0}`) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)".format(
        safe_date
    )
    lines = [
        "-- Quality probe: dup keys + null rates in ONE columnar scan.",
        "SELECT",
        "  COUNT(*) AS total_rows,",
        "  COUNTIF(`{0}` IS NULL) AS null_date_rows,".format(safe_date),
        "  COUNTIF({0}) AS recent_rows,".format(recent),
        "  COUNTIF({0} AND `{1}` IS NULL) AS recent_null_rows,".format(
            recent, safe_date
        ),
    ]
    if safe_pks:
        concat = ", '|', ".join(
            "IFNULL(CAST(`{0}` AS STRING), '')".format(c) for c in safe_pks
        )
        lines.append(
            "  COUNT(DISTINCT IF({0}, CONCAT({1}), NULL)) AS recent_distinct_keys".format(
                recent, concat
            )
        )
    else:
        lines.append("  NULL AS recent_distinct_keys")
    lines.append(
        "FROM `{0}`.`{1}`.`{2}`".format(safe_project, safe_dataset, safe_table)
    )
    return "\n".join(lines)


def classify_duplicates(
    recent_rows,
    recent_distinct_keys,
    today,
    tolerance=DEFAULT_DUP_TOLERANCE,
) -> List[Dict[str, Any]]:
    """PURE: flag when the recent window holds duplicate primary keys.

    Returns ONE HIGH/ACTIVE finding when dup_ratio > tolerance, else [].
    Not comparable (no PK / empty window) -> [].
    """
    if not recent_rows or recent_distinct_keys is None:
        return []
    dups = int(recent_rows) - int(recent_distinct_keys)
    if dups <= 0:
        return []
    ratio = dups / float(recent_rows)
    if ratio <= tolerance:
        return []
    message = (
        "duplicate primary keys in last window: {0} dup row(s) of {1} "
        "({2:.2%})".format(dups, recent_rows, ratio)
    )
    return [
        {
            "detector": DETECTOR_DUPLICATE_KEYS,
            "anomaly_date": today,
            "observed_count": dups,
            "expected_low": None,
            "median_count": int(recent_rows),
            "date_lag_days": None,
            "severity": SEVERITY_HIGH,
            "scope": SCOPE_ACTIVE,
            "audit_status": STATUS_ANOMALY,
            "error_message": message,
        }
    ]


def classify_null_rate(
    total_rows,
    null_date_rows,
    recent_rows,
    recent_null_rows,
    today,
    jump=DEFAULT_NULL_JUMP,
    min_rows=DEFAULT_NULL_MIN_ROWS,
) -> List[Dict[str, Any]]:
    """PURE: flag when the date column's recent NULL share JUMPED vs baseline.

    Drift, not an absolute bar: recent_rate must exceed the all-history baseline
    by more than ``jump`` (20 points default), with at least ``min_rows`` recent
    rows so tiny windows cannot trip it. A table whose column was ALWAYS mostly
    NULL is not news; one that suddenly degraded is.
    """
    if not total_rows or not recent_rows or recent_rows < min_rows:
        return []
    baseline = (null_date_rows or 0) / float(total_rows)
    recent_rate = (recent_null_rows or 0) / float(recent_rows)
    if recent_rate - baseline <= jump:
        return []
    message = "date-column NULL rate jumped: recent {0:.1%} vs baseline {1:.1%}".format(
        recent_rate, baseline
    )
    return [
        {
            "detector": DETECTOR_NULL_RATE,
            "anomaly_date": today,
            "observed_count": int(recent_null_rows or 0),
            "expected_low": None,
            "median_count": int(recent_rows),
            "date_lag_days": None,
            "severity": SEVERITY_MEDIUM,
            "scope": SCOPE_ACTIVE,
            "audit_status": STATUS_ANOMALY,
            "error_message": message,
        }
    ]


def run_quality_checks(client, target: Dict[str, Any], today, days=DEFAULT_RECENT_DAYS):
    """Run the probe for one target and classify. Returns a findings list.

    Raises on query failure -- the anomaly runner's per-table boundary handles
    retry/skip exactly as for the other probes.
    """
    from google.cloud import bigquery

    sql = build_quality_sql(
        target["project_id"],
        target["dataset_name"],
        target["table_name"],
        target["date_column"],
        target.get("primary_key") or [],
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", int(days))]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    row = rows[0]
    findings: List[Dict[str, Any]] = []
    findings.extend(
        classify_duplicates(row["recent_rows"], row["recent_distinct_keys"], today)
    )
    findings.extend(
        classify_null_rate(
            row["total_rows"],
            row["null_date_rows"],
            row["recent_rows"],
            row["recent_null_rows"],
            today,
        )
    )
    return findings
