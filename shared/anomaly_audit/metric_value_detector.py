#!/usr/bin/env python3
# Detector: METRIC-VALUE ZERO (BI-486). The volume/structural detectors count
# ROWS (COUNT(*)) per day -- they are blind to a metric COLUMN going to zero while
# rows stay present. BI-486 is exactly that: post_reach (sourced from a deprecated
# Meta metric) went to 0 on 2026-06-11 while each day still had ~70 post rows, so
# every row-count detector saw a healthy table. This detector watches the daily
# SUM of named metric columns and flags when a column that was historically
# non-zero goes all-zero across the recent SETTLED window.
"""Metric-value detector for the anomaly audit.

A PURE function over an ordered ``(date, metric_sum)`` series per watched column,
plus a SQL builder that produces those per-column daily sums. No I/O lives in the
pure function (mirrors structural/volume).

Signal: the watched column's SUM is zero (or null) on every SETTLED day in the
recent window, while its history (the body before the recent window) had a
positive median. That is the "a real metric quietly went to 0" pattern -- the one
the row-count detectors cannot see.

All log strings are ASCII-only.
"""

import logging
import statistics
from datetime import date
from typing import Dict, List, Optional, Tuple

from shared.anomaly_audit import (
    DETECTOR_METRIC_ZERO,
    SCOPE_ACTIVE,
    SEVERITY_HIGH,
    STATUS_ANOMALY,
    _validate_identifier,
)

logger = logging.getLogger(__name__)


# -- SQL --------------------------------------------------------------------

# Per-date SUM of one metric column over the trailing history window. The metric
# column and date column are interpolated by the CALLER only after passing
# _validate_identifier (BigQuery cannot bind identifiers as @parameters); @days is
# bound as a scalar. SAFE_CAST tolerates STRING-typed metric columns (these raw
# tables store numbers as STRING) and NULLIF drops the literal 'nan'. Only --
# comments are used.
_METRIC_SUM_SQL = """-- METRIC_SUM: per-date SUM of one metric column over the trailing window.
SELECT
  DATE(`{date_column}`) AS anomaly_date,
  SUM(IFNULL(SAFE_CAST(NULLIF(LOWER(CAST(`{metric_column}` AS STRING)), 'nan')
             AS FLOAT64), 0.0)) AS metric_sum
FROM `{project_id}`.`{dataset_name}`.`{table_name}`
WHERE DATE(`{date_column}`) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
  AND `{date_column}` IS NOT NULL
GROUP BY anomaly_date
ORDER BY anomaly_date"""


def build_metric_sum_sql(
    project_id: str,
    dataset_name: str,
    table_name: str,
    date_column: str,
    metric_column: str,
) -> str:
    """Build the parameterized METRIC_SUM SQL for one (table, metric column).

    Every interpolated identifier is validated against the shared allowlist BEFORE
    it is placed into the SQL; ``@days`` is left as a bind parameter for the caller.
    """
    safe_project = _validate_identifier(project_id, "project")
    safe_dataset = _validate_identifier(dataset_name, "dataset")
    safe_table = _validate_identifier(table_name, "table")
    safe_date = _validate_identifier(date_column, "date_column")
    safe_metric = _validate_identifier(metric_column, "metric_column")
    return _METRIC_SUM_SQL.format(
        project_id=safe_project,
        dataset_name=safe_dataset,
        table_name=safe_table,
        date_column=safe_date,
        metric_column=safe_metric,
    )


def query_metric_sum(
    client,
    project_id: str,
    dataset_name: str,
    table_name: str,
    date_column: str,
    metric_column: str,
    days: int,
    retry_attempts: int = 1,
    backoff_seconds: Optional[list] = None,
) -> List[Tuple[date, float]]:
    """Run METRIC_SUM for one metric column; return ordered (date, sum) tuples.

    Mirrors volume_detector.query_daily_counts: identifiers are validated +
    interpolated by build_metric_sum_sql; ``@days`` is bound as a scalar. The
    caller (anomaly_runner) owns the retry budget, so this defaults to a single
    attempt and lets exceptions propagate to be handled per-table.
    """
    sql = build_metric_sum_sql(
        project_id, dataset_name, table_name, date_column, metric_column
    )

    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", int(days))]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    series = [(r["anomaly_date"], float(r["metric_sum"] or 0.0)) for r in rows]
    series.sort(key=lambda pair: pair[0])
    return series


# -- pure detector ----------------------------------------------------------


def _median(values) -> float:
    vals = list(values)
    if not vals:
        return 0.0
    return float(statistics.median(vals))


# How many trailing days the metric-zero detector treats as possibly-partial and
# excludes from judgement. Deliberately SMALL and INDEPENDENT of the row-count
# settle_lag (which auto-inflates from the data pattern -- and a metric going to
# zero ITSELF inflates it, delaying the very alert we want). A metric SUM going to
# 0 is unambiguous on the day it lands; we only skip today (which may be partially
# loaded), so a fresh zero alerts within `recent_window` days, not weeks.
DEFAULT_METRIC_SETTLE_LAG = 1


def detect_metric_zero(
    metric_column: str,
    series: List[Tuple[date, float]],
    today: date,
    recent_window: int,
    min_history_settled: int,
    metric_settle_lag: int = DEFAULT_METRIC_SETTLE_LAG,
) -> List[Dict]:
    """Flag a watched metric column that went all-zero across the recent window.

    Uses its OWN small settle window (metric_settle_lag, default 1 -- just exclude a
    possibly-partial today) rather than the row-count settle_lag, so a fresh metric
    zeroing alerts within `recent_window` days instead of waiting for the
    auto-inflated row-count lag (which the zeros themselves inflate).

    Args:
        metric_column: the watched column name (for the finding detail).
        series: ordered (date, metric_sum) over the history window.
        today: audit "today" (frontier reference).
        recent_window: how many recent days must ALL be zero to fire.
        min_history_settled: need at least this many history days BEFORE the recent
            window to trust the "was historically non-zero" baseline.
        metric_settle_lag: trailing days excluded as possibly-partial (default 1).

    Returns:
        A one-element finding list if the column went zero, else [].
    """
    if not series:
        return []

    # Exclude only the possibly-partial trailing day(s) -- a small, fixed window,
    # NOT the row-count settle_lag.
    lag = metric_settle_lag if metric_settle_lag and metric_settle_lag > 0 else 0
    settled = series[: len(series) - lag] if lag > 0 else list(series)
    if len(settled) < recent_window + min_history_settled:
        # Not enough history to distinguish "newly zero" from "always near zero /
        # cold start" -- stay quiet (recency/structural cover the rest).
        return []

    recent = settled[-recent_window:]
    body = settled[: len(settled) - recent_window]

    body_median = _median(v for _, v in body)
    recent_max = max((v for _, v in recent), default=0.0)

    # Fire only when the column was clearly non-zero historically AND every recent
    # settled day is zero. recent_max == 0 means the whole recent window is zero.
    if body_median > 0.0 and recent_max == 0.0:
        first_zero = recent[0][0]
        last_seen = first_zero
        # Walk back through the body to report the last day it was non-zero.
        for d, v in reversed(body):
            if v > 0.0:
                last_seen = d
                break
        detail = (
            "metric '%s' SUM is zero on all %d recent settled day(s) "
            "(history median %.1f); last non-zero %s, first zero %s"
            % (
                metric_column,
                len(recent),
                body_median,
                last_seen.isoformat(),
                first_zero.isoformat(),
            )
        )
        logger.info("Metric-zero detector flagged column %s", metric_column)
        # Canonical finding shape (matches _row_from_finding): observed_count is
        # the zero metric sum, expected_low/median_count carry the historical
        # baseline, error_message carries the human-readable detail.
        return [
            {
                "detector": DETECTOR_METRIC_ZERO,
                "anomaly_date": first_zero,
                "observed_count": 0.0,
                "expected_low": body_median,
                "median_count": body_median,
                "date_lag_days": None,
                "severity": SEVERITY_HIGH,
                "scope": SCOPE_ACTIVE,
                "audit_status": STATUS_ANOMALY,
                "error_message": detail,
            }
        ]

    return []
