#!/usr/bin/env python3
# Detector 1 (PRIMARY): recency-lag. Judges the DATE FRONTIER on date PRESENCE
# only -- never row volume -- so a partially-filled recent day neither trips nor
# suppresses an alert. Pulls the DISTINCT present dates once, then a pure analysis
# function derives the LAG (frontier) and interior GAP findings.
"""Recency-lag detector for the Anomaly Audit.

This is the PRIMARY, "as soon as possible" detector. For each audited table it
reads the set of DATES PRESENT on the freshness-resolved date column over the
trailing history window (DISTINCT DATE(col) -- no row counts), then:

  * computes ``date_lag = (today - MAX(present)).days`` and flags a LAG finding
    (severity HIGH, scope ACTIVE) when ``date_lag > cadence + grace``;
  * walks the expected present dates between the first and last present date
    (restricted to the table's active weekday set so weekday-only tables do not
    false-gap on weekends) and flags a GAP finding (severity MEDIUM, scope ACTIVE)
    for every expected-but-absent interior date.

The detector ALERTS IMMEDIATELY -- it is never gated by any recent window, so the
frontier-lag signal is honoured on day one. It uses DATE PRESENCE ONLY; row volume
is irrelevant here (the volume detector owns that). Empty history yields no
findings (the runner emits the cold-start / OK handling).

ASCII-ONLY RULE: every log string is pure ASCII (use -- or :, never an em-dash or
other unicode). The SQL uses only double-dash comments and binds the lookback as a
scalar parameter; identifiers are interpolated only after _validate_identifier.
"""

import logging
import time
from datetime import date
from typing import Dict, List

from shared.anomaly_audit import (
    DETECTOR_RECENCY_LAG,
    SCOPE_ACTIVE,
    SCOPE_HISTORICAL,
    SEVERITY_HIGH,
    SEVERITY_MEDIUM,
    STATUS_ANOMALY,
    _validate_identifier,
)
from shared.anomaly_audit.cadence import abnormal_gaps

logger = logging.getLogger(__name__)

# Default retry policy if the caller passes nothing usable. Mirrors the freshness
# calculator's policy so behaviour is consistent across the two audits.
_DEFAULT_RETRY_ATTEMPTS = 3
_DEFAULT_BACKOFF_SECONDS = [5, 15, 45]

# DISTINCT_DATES: the set of DATES PRESENT on the resolved date column over the
# trailing window, used by the recency-lag detector to learn cadence + the active
# weekday set and to compute date_lag and interior gaps. DATE PRESENCE ONLY -- no
# row volume is selected, so a partially-filled recent day neither trips nor
# suppresses the detector. @days is bound as a scalar parameter; identifiers are
# interpolated by the caller after _validate_identifier. Only -- comments.
DISTINCT_DATES_SQL = """-- DISTINCT_DATES: the set of DATES PRESENT on the resolved date column over the
-- trailing window, used by the recency-lag detector to learn cadence + the active
-- weekday set and to compute date_lag and interior gaps. DATE PRESENCE ONLY -- no
-- row volume is selected, so a partially-filled recent day neither trips nor
-- suppresses the detector. @days is bound as a scalar parameter; identifiers are
-- interpolated by the caller after _validate_identifier. Only -- comments.
SELECT DISTINCT DATE(`{date_column}`) AS present_date
FROM `{project_id}`.`{dataset_name}`.`{table_name}`
WHERE DATE(`{date_column}`) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
  AND `{date_column}` IS NOT NULL
ORDER BY present_date"""

# Substrings that indicate a transient BigQuery error worth retrying. Anything
# else (invalid query, not found) is non-transient and fails fast. Mirrors the
# freshness calculator's marker list so retry behaviour does not drift.
_TRANSIENT_MARKERS = (
    "ratelimitexceeded",
    "rate limit",
    "backenderror",
    "internalerror",
    "internal error",
    "timeout",
    "timed out",
    "service unavailable",
    "serviceunavailable",
    "try again",
    "deadline",
    "connection reset",
    "connection aborted",
)


class RecencyQueryError(Exception):
    """Raised when the DISTINCT_DATES query fails after the retry budget.

    The runner maps this to an ERROR result row (it never aborts the run).
    """


def _is_transient(exc) -> bool:
    """Return True if an exception looks like a retryable transient BQ error."""
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_MARKERS)


def build_distinct_dates_sql(
    project_id: str, dataset_name: str, table_name: str, date_column: str
) -> str:
    """Build the DISTINCT_DATES SQL for one table.

    Every interpolated identifier is validated against the shared allowlist BEFORE
    it is placed into the SQL (BigQuery cannot bind identifiers as parameters);
    the lookback day count is bound separately by the caller as the @days scalar
    parameter. Only double-dash comments are used.

    Args:
        project_id: GCP project id (hyphen-permitting allowlist).
        dataset_name: dataset name (underscore-strict allowlist).
        table_name: table name (underscore-strict allowlist).
        date_column: resolved freshness date column (underscore-strict allowlist).

    Returns:
        The parameterized DISTINCT_DATES query string.

    Raises:
        ValueError: if any identifier fails the allowlist.
    """
    safe_project = _validate_identifier(project_id, "project")
    safe_dataset = _validate_identifier(dataset_name, "dataset")
    safe_table = _validate_identifier(table_name, "table")
    safe_column = _validate_identifier(date_column, "date_column")
    return DISTINCT_DATES_SQL.format(
        project_id=safe_project,
        dataset_name=safe_dataset,
        table_name=safe_table,
        date_column=safe_column,
    )


def query_distinct_dates(
    client,
    project_id: str,
    dataset_name: str,
    table_name: str,
    date_column: str,
    days: int,
    retry_attempts: int = _DEFAULT_RETRY_ATTEMPTS,
    backoff_seconds: List[int] = None,
) -> List[date]:
    """Run the DISTINCT_DATES query and return the sorted list of present dates.

    DATE PRESENCE ONLY -- no row volume is selected, so a partially-filled recent
    day appears as a present date and is treated identically to a fully-filled one.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        project_id: GCP project id.
        dataset_name: dataset name.
        table_name: table name.
        date_column: resolved freshness date column.
        days: trailing lookback window in calendar days (bound as @days).
        retry_attempts: total query attempts (transient errors only).
        backoff_seconds: per-retry sleep durations.

    Returns:
        A sorted list of datetime.date present on the column in the window (may be
        empty when the table has no rows in the window).

    Raises:
        RecencyQueryError: on query failure after the retry budget is exhausted.
    """
    if backoff_seconds is None:
        backoff_seconds = list(_DEFAULT_BACKOFF_SECONDS)
    if not retry_attempts or retry_attempts < 1:
        retry_attempts = _DEFAULT_RETRY_ATTEMPTS

    sql = build_distinct_dates_sql(project_id, dataset_name, table_name, date_column)
    target = "{0}.{1}.{2}".format(project_id, dataset_name, table_name)

    # The lookback is bound as a scalar parameter -- never interpolated.
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", int(days))]
    )

    last_exc = None
    for attempt in range(1, retry_attempts + 1):
        try:
            rows = list(client.query(sql, job_config=job_config).result())
            # present_date comes back as a datetime.date; filter any stray NULLs.
            dates = [r["present_date"] for r in rows if r["present_date"] is not None]
            dates.sort()
            return dates
        except Exception as exc:  # noqa: BLE001 - classified into transient/fatal
            last_exc = exc
            if not _is_transient(exc):
                logger.warning(
                    "Distinct-dates query failed (non-transient) for %s: %s",
                    target,
                    exc,
                )
                raise RecencyQueryError(
                    "distinct-dates query failed for {0}: {1}".format(target, exc)
                ) from exc
            if attempt < retry_attempts:
                idx = (
                    min(attempt - 1, len(backoff_seconds) - 1) if backoff_seconds else 0
                )
                delay = backoff_seconds[idx] if backoff_seconds else 0
                logger.warning(
                    "Distinct-dates transient error for %s (attempt %d/%d), "
                    "retrying in %ds: %s",
                    target,
                    attempt,
                    retry_attempts,
                    delay,
                    exc,
                )
                if delay:
                    time.sleep(delay)
            else:
                logger.warning(
                    "Distinct-dates query exhausted retries for %s: %s", target, exc
                )
    raise RecencyQueryError(
        "distinct-dates query failed for {0} after {1} attempts: {2}".format(
            target, retry_attempts, last_exc
        )
    ) from last_exc


def _finding(
    *,
    anomaly_date: date,
    severity: str,
    date_lag_days,
    scope: str = SCOPE_ACTIVE,
    observed_count=None,
) -> Dict:
    """Build one canonical recency finding dict the runner maps into a result row.

    The recency detector populates only the keys it owns; the volume-specific
    numeric columns are explicitly None so the runner's row builder is uniform
    across detectors. For an aggregated interior-GAP finding, observed_count
    carries the number of missing dates (the column is otherwise unused by this
    detector) and scope is ACTIVE only when a gap falls in the recent window.
    """
    return {
        "detector": DETECTOR_RECENCY_LAG,
        "anomaly_date": anomaly_date,
        "severity": severity,
        "scope": scope,
        "observed_count": observed_count,
        "expected_low": None,
        "median_count": None,
        "date_lag_days": date_lag_days,
        "audit_status": STATUS_ANOMALY,
    }


def detect_recency(
    present_dates: List[date],
    today: date,
    cadence: int,
    weekdays: set,
    grace: int,
    recent_window: int = 14,
) -> List[Dict]:
    """Derive recency LAG + interior GAP findings from the present-date set.

    PURE function -- no I/O, never raises for empty/short input. Uses DATE PRESENCE
    ONLY; row volume plays no part. This detector is NOT window-gated: it alerts on
    a frontier lag immediately (day one).

    Logic:
      * ``date_lag = (today - max(present_dates)).days``. Emit a LAG finding
        (severity HIGH, scope ACTIVE, anomaly_date = today, date_lag_days =
        date_lag) when ``date_lag > cadence + grace``.
      * Walk ``expected_present_dates(min_present, max_present, weekdays)`` and for
        every expected date that is NOT present, emit a GAP finding (severity
        MEDIUM, scope ACTIVE, anomaly_date = the missing date). Interior gaps only
        -- the frontier (anything after max_present) is the LAG detector's job, and
        never-produced weekdays are excluded by the active-weekday set.

    Args:
        present_dates: sorted (or unsorted) list of present dates; may be empty.
        today: the run's reference date (the runner supplies datetime-derived).
        cadence: learned expected calendar-day cadence (>=1; daily == 1).
        weekdays: the active weekday int set (date.weekday(); Mon=0..Sun=6).
        grace: lag tolerance in days added to the cadence before flagging.

    Returns:
        A list of finding dicts (possibly empty). Empty history -> [] (the runner
        owns the cold-start note / OK row).
    """
    if not present_dates:
        # No present dates in the window -- nothing to lag-compare. The runner
        # decides how to represent an empty table (cold-start / OK / structural).
        return []

    # Defensive: tolerate an unsorted input and a non-positive cadence/grace.
    present_set = set(present_dates)
    max_present = max(present_set)
    min_present = min(present_set)
    safe_cadence = cadence if isinstance(cadence, int) and cadence >= 1 else 1
    safe_grace = grace if isinstance(grace, int) and grace >= 0 else 0

    findings: List[Dict] = []

    # 1. Frontier LAG (HIGH). date_lag is presence-based: today minus the newest
    #    present date. A partially-filled current day is still "present" and so
    #    yields date_lag 0 -- it does not trip this, exactly as required.
    date_lag = (today - max_present).days
    if date_lag > safe_cadence + safe_grace:
        logger.warning(
            "Recency LAG: frontier %s is %d day(s) behind today %s "
            "(cadence=%d, grace=%d)",
            max_present,
            date_lag,
            today,
            safe_cadence,
            safe_grace,
        )
        findings.append(
            _finding(
                anomaly_date=today,
                severity=SEVERITY_HIGH,
                date_lag_days=date_lag,
            )
        )

    # 2. Interior GAPs (MEDIUM) -- ABNORMAL-gap model, not calendar enumeration.
    #    A gap between consecutive present dates is flagged ONLY when it is much
    #    larger than the table's OWN typical spacing (abnormal_gaps: gap > 3x the
    #    median gap AND > a small floor). This is what lets an IRREGULARLY-loaded
    #    table -- e.g. a LimeSurvey reference table snapshotted every ~2 weeks --
    #    avoid flagging every in-between calendar day; only a genuinely unusual
    #    pause is reported. One finding per abnormal gap; anomaly_date is the gap's
    #    END (when loading resumed), observed_count carries the gap length in days,
    #    scope is ACTIVE only when the gap ended within recent_window days.
    for gap_start, gap_end, gap_days in abnormal_gaps(present_dates):
        within_recent = (today - gap_end).days <= recent_window
        scope = SCOPE_ACTIVE if within_recent else SCOPE_HISTORICAL
        logger.warning(
            "Recency GAP: abnormal %d-day gap %s..%s (scope=%s)",
            gap_days,
            gap_start,
            gap_end,
            scope,
        )
        findings.append(
            _finding(
                anomaly_date=gap_end,
                severity=SEVERITY_MEDIUM,
                date_lag_days=gap_days,
                scope=scope,
                observed_count=gap_days,
            )
        )

    if not findings:
        logger.debug(
            "Recency OK: frontier %s within cadence+grace and no interior gaps",
            max_present,
        )
    return findings
