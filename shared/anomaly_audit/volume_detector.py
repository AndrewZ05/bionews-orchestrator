#!/usr/bin/env python3
# Detector 2: VOLUME-OUTLIER (secondary, settled data).
# Pulls per-date row counts over the trailing history window ONCE (the runner reuses
# the same series for the structural detector), then applies a guarded low-side
# robust band on SETTLED days only. The guard (MAD band + relative floor + minimum
# median) is what stops small-count tables (FB/IG insights, medians 44-278) from
# false-flagging a 10% wiggle while still catching genuine drops on high-volume
# tables. LOW side only -- high spikes (a big send day) are normal and never flagged.
"""Volume-outlier detector for the anomaly audit.

The SQL builder + fetch function are the only I/O here; the statistics
(``compute_settle_lag``, ``_median``, ``_mad``, ``detect_volume``) are PURE
functions over an ordered ``(date, count)`` series so they are trivially unit
testable with no BigQuery.

Settle-lag: trailing days are frequently still filling (campaign backfill), so the
last ``settle_lag`` days are excluded from judgement. The lag is the larger of the
configured ``backfill_days`` (default 5) and an auto-detected count of trailing days
whose count is materially below the body median.

Guard band (a SETTLED day is flagged LOW only when ALL THREE hold):
  (a) below MAD band:   n < median - K*1.4826*MAD   (K default 4.0)
  (b) materially low:   n < REL_FLOOR * median        (REL_FLOOR default 0.5)
  (c) table big enough: median >= MIN_COUNT           (MIN_COUNT default 100)

All log strings are ASCII-only; all SQL uses double-dash comments exclusively.
"""

import logging
import statistics
import time
from collections import namedtuple
from datetime import date
from typing import List, Tuple

from shared.anomaly_audit import (
    DETECTOR_VOLUME_OUTLIER,
    SCOPE_ACTIVE,
    SCOPE_HISTORICAL,
    SEVERITY_LOW,
    SEVERITY_MEDIUM,
    STATUS_ANOMALY,
    _validate_identifier,
)

logger = logging.getLogger(__name__)

# Default retry policy if the audit.retry block is absent from config (mirrors the
# freshness calculator's defaults so behavior is consistent across the two audits).
_DEFAULT_RETRY_ATTEMPTS = 3
_DEFAULT_BACKOFF_SECONDS = [5, 15, 45]

# Scale factor that makes the MAD a consistent estimator of the standard deviation
# for normally-distributed data: 1/Phi^-1(3/4) ~= 1.4826.
_MAD_SCALE = 1.4826

# A trailing day is considered "still filling" by the auto-detector when its count
# is below this fraction of the body median.
_SETTLE_BODY_FRACTION = 0.8

# Substrings that indicate a transient BigQuery error worth retrying. Anything else
# (invalid query, not found, syntax error) fails fast so we do not waste the budget.
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

# DAILY_COUNTS: per-date row counts over the trailing history window on the resolved
# date column. Feeds the volume-outlier and structural detectors. The lookback
# (@days) is bound as a scalar parameter; project/dataset/table and the date column
# are interpolated by the CALLER only after passing _validate_identifier (BigQuery
# cannot bind identifiers as parameters). Only -- comments are used.
_DAILY_COUNTS_SQL = """-- DAILY_COUNTS: per-date row counts over the trailing history window on the
-- resolved date column. Feeds the volume-outlier and structural detectors. The
-- lookback (@days) is bound as a scalar parameter; project/dataset/table and the
-- date column are interpolated by the CALLER only after passing _validate_identifier
-- (BigQuery cannot bind identifiers as parameters). Only -- comments are used.
SELECT
  DATE(`{date_column}`) AS anomaly_date,
  COUNT(*) AS observed_count
FROM `{project_id}`.`{dataset_name}`.`{table_name}`
WHERE DATE(`{date_column}`) >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
  AND `{date_column}` IS NOT NULL
GROUP BY anomaly_date
ORDER BY anomaly_date"""


# VolumeResult exposes the findings AND the settled-day count so the runner can
# apply the same cold-start gating consistently across all three detectors. When
# cold_start is True the band was skipped (insufficient settled history) and
# findings is empty.
VolumeResult = namedtuple(
    "VolumeResult", ["findings", "settled_day_count", "settle_lag", "cold_start"]
)


def build_daily_counts_sql(project_id, dataset_name, table_name, date_column):
    """Build the parameterized DAILY_COUNTS SQL for one table.

    Every interpolated token is validated against the shared allowlist BEFORE it is
    placed into the SQL (BigQuery cannot bind identifiers as @parameters); the
    lookback is bound separately by the caller as the scalar parameter ``@days``.

    Args:
        project_id: GCP project id (hyphens allowed).
        dataset_name: BigQuery dataset (underscore-strict).
        table_name: BigQuery table (underscore-strict).
        date_column: the resolved date column (underscore-strict).

    Returns:
        The DAILY_COUNTS SQL string with identifiers interpolated and ``@days``
        left as a bind parameter.
    """
    safe_project = _validate_identifier(project_id, "project")
    safe_dataset = _validate_identifier(dataset_name, "dataset")
    safe_table = _validate_identifier(table_name, "table")
    safe_column = _validate_identifier(date_column, "date_column")
    return _DAILY_COUNTS_SQL.format(
        project_id=safe_project,
        dataset_name=safe_dataset,
        table_name=safe_table,
        date_column=safe_column,
    )


def _is_transient(exc):
    """Return True if an exception looks like a retryable transient BQ error."""
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_MARKERS)


def query_daily_counts(
    client,
    project_id,
    dataset_name,
    table_name,
    date_column,
    days,
    retry_attempts=3,
    backoff_seconds=None,
) -> List[Tuple[date, int]]:
    """Run DAILY_COUNTS and return an ordered list of (date, count) tuples.

    The lookback ``days`` is bound as a scalar query parameter; identifiers are
    validated and interpolated by :func:`build_daily_counts_sql`. Transient BigQuery
    errors are retried with the configured backoff; non-transient errors fail fast.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        project_id, dataset_name, table_name, date_column: the resolved target.
        days: trailing-window size in days (e.g. 90), bound as ``@days``.
        retry_attempts: total attempts (default 3).
        backoff_seconds: sleep durations between retries (default [5,15,45]).

    Returns:
        A list of (datetime.date, int) tuples ordered ascending by date.

    Raises:
        Exception: re-raises the last BigQuery error after the retry budget for
            non-transient failures or exhausted transient retries.
    """
    if backoff_seconds is None:
        backoff_seconds = list(_DEFAULT_BACKOFF_SECONDS)
    if not retry_attempts or retry_attempts < 1:
        retry_attempts = _DEFAULT_RETRY_ATTEMPTS

    sql = build_daily_counts_sql(project_id, dataset_name, table_name, date_column)
    target = "{0}.{1}.{2}".format(project_id, dataset_name, table_name)

    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", int(days))]
    )

    last_exc = None
    for attempt in range(1, retry_attempts + 1):
        try:
            rows = list(client.query(sql, job_config=job_config).result())
            series = [(r["anomaly_date"], int(r["observed_count"] or 0)) for r in rows]
            # Defensive: ensure ascending date order regardless of result ordering.
            series.sort(key=lambda pair: pair[0])
            return series
        except Exception as exc:
            last_exc = exc
            if not _is_transient(exc):
                logger.warning(
                    "Daily-counts query failed (non-transient) for %s: %s",
                    target,
                    exc,
                )
                raise
            if attempt < retry_attempts:
                idx = (
                    min(attempt - 1, len(backoff_seconds) - 1) if backoff_seconds else 0
                )
                delay = backoff_seconds[idx] if backoff_seconds else 0
                logger.warning(
                    "Daily-counts transient error for %s (attempt %d/%d), "
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
                logger.warning("Daily-counts exhausted retries for %s: %s", target, exc)
    raise last_exc


def _median(values):
    """Return the median of a non-empty numeric iterable, else 0.0.

    Thin wrapper over statistics.median that tolerates empty input deterministically
    so the callers never need to special-case it.
    """
    vals = list(values)
    if not vals:
        return 0.0
    return float(statistics.median(vals))


def _mad(values, center=None):
    """Return the Median Absolute Deviation of a numeric iterable, else 0.0.

    MAD = median(|x_i - center|), where center defaults to median(values). Returns
    0.0 for empty input. The raw (unscaled) MAD is returned; the 1.4826 consistency
    scale is applied at the band-comparison site so the meaning is explicit there.
    """
    vals = list(values)
    if not vals:
        return 0.0
    c = _median(vals) if center is None else float(center)
    return float(statistics.median([abs(v - c) for v in vals]))


def compute_settle_lag(series: List[Tuple[date, int]], backfill_days: int) -> int:
    """Compute how many trailing days to exclude from judgement as still-filling.

    The settle-lag is the LARGER of:
      * the configured ``backfill_days`` (default 5 when None/<=0), and
      * an auto-detected count of trailing days whose count is materially below the
        body median (< 0.8 * median of the days before the trailing region).

    The auto-detection walks backward from the frontier, growing the trailing region
    while each next-most-recent day is below the body fraction of the body median;
    it stops at the first day that looks settled. This catches a longer-than-default
    backfill tail without ever shrinking below the configured floor.

    Args:
        series: ordered (date, count) tuples (ascending by date).
        backfill_days: per-table configured floor for the lag.

    Returns:
        A non-negative int, never exceeding ``len(series)``.
    """
    floor = backfill_days if (backfill_days and backfill_days > 0) else 5
    n = len(series)
    if n == 0:
        return 0
    floor = min(floor, n)

    counts = [c for _, c in series]
    # Body median is computed from a STABLE INTERIOR window that excludes the whole
    # candidate trailing region (everything from the configured floor onward). This
    # keeps an isolated low day at the settle boundary OUT of the body so it cannot
    # inflate the body downward and make itself look "still-filling" (the bug: a
    # genuine settled dip adjacent to the floor would otherwise be swallowed and
    # never judged). Anchoring the body to the interior is the fix.
    interior = counts[: n - floor] if n > floor else counts
    if len(interior) < 3:
        # Not enough interior history to auto-extend reliably -- use the floor.
        return floor
    body_median = _median(interior)
    if body_median <= 0:
        return floor

    threshold = _SETTLE_BODY_FRACTION * body_median
    # Grow the tail past the floor ONLY across a CONTIGUOUS run of still-filling days
    # measured from the frontier inward. Require the day being absorbed AND its
    # more-recent neighbor to both be below threshold, so a single isolated dip
    # bordering the floor is NOT absorbed (that dip is a real settled anomaly).
    auto = floor
    while auto < n:
        next_idx = n - auto - 1  # the next older day we would absorb into the tail
        prev_idx = next_idx + 1  # its more-recent neighbor (already in the tail)
        cand_low = counts[next_idx] < threshold
        neighbor_low = prev_idx < n and counts[prev_idx] < threshold
        if cand_low and neighbor_low:
            auto += 1
        else:
            break

    return min(max(floor, auto), n)


def detect_volume(
    series: List[Tuple[date, int]],
    today: date,
    K=4.0,
    rel_floor=0.5,
    min_count=100,
    backfill_days=5,
    recent_window=None,
    min_history=30,
) -> VolumeResult:
    """Detect low-side volume outliers on SETTLED days using the guarded robust band.

    The last ``settle_lag`` days (see :func:`compute_settle_lag`) are dropped from
    judgement as still-filling. If fewer than ``min_history`` (30) SETTLED days
    remain, the band is skipped entirely and a cold-start result is returned (the
    runner falls back to recency-lag + structural-zero only).

    A SETTLED day is flagged LOW only when ALL THREE guards hold:
      (a) ``n < median - K*1.4826*MAD``
      (b) ``n < rel_floor * median``
      (c) ``median >= min_count``

    Each flagged day is classified ACTIVE when it is within ``recent_window``
    settled days of the frontier (the most recent settled day), else HISTORICAL.
    ACTIVE volume dips are MEDIUM severity (emailed); HISTORICAL outliers are LOW
    severity (results table only). High days are NEVER flagged.

    Args:
        series: ordered (date, count) tuples (ascending by date).
        today: the current date (carried for symmetry; not used for gating here --
            the recency detector owns immediate alerting; volume is settled-only).
        K: MAD multiplier (default 4.0).
        rel_floor: relative-floor fraction of the median (default 0.5).
        min_count: minimum median for a table to be judged on volume (default 100).
        backfill_days: per-table still-filling tail floor (default 5).
        recent_window: ACTIVE classification window in settled days. When None it is
            computed as max(7, 2*backfill_days).
        min_history: minimum settled days required to run the band (default 30).

    Returns:
        VolumeResult(findings, settled_day_count, settle_lag, cold_start). ``findings``
        is a list of canonical finding dicts; empty when cold_start is True or no day
        trips the guard.
    """
    effective_backfill = backfill_days if (backfill_days and backfill_days > 0) else 5
    if recent_window is None or recent_window <= 0:
        recent_window = max(7, 2 * effective_backfill)

    settle_lag = compute_settle_lag(series, effective_backfill)
    settled = series[: len(series) - settle_lag] if settle_lag else list(series)
    settled_day_count = len(settled)

    # Cold start: not enough settled history for a stable baseline -- skip the band.
    if settled_day_count < min_history:
        logger.info(
            "Volume band skipped -- only %d settled day(s) (< %d); baseline warming up",
            settled_day_count,
            min_history,
        )
        return VolumeResult(
            findings=[],
            settled_day_count=settled_day_count,
            settle_lag=settle_lag,
            cold_start=True,
        )

    counts = [c for _, c in settled]
    median = _median(counts)
    mad = _mad(counts, center=median)
    mad_lower = median - K * _MAD_SCALE * mad
    rel_lower = rel_floor * median
    expected_low = int(round(mad_lower))
    median_int = int(round(median))

    # Guard (c): a table whose typical volume is below MIN_COUNT is too small/noisy
    # to judge on this band -- skip it entirely (stops FB/IG small-count false flags).
    if median < min_count:
        logger.info(
            "Volume band suppressed -- median %d < min_count %d (small-count table)",
            median_int,
            int(min_count),
        )
        return VolumeResult(
            findings=[],
            settled_day_count=settled_day_count,
            settle_lag=settle_lag,
            cold_start=False,
        )

    findings = []
    frontier_idx = settled_day_count - 1
    for idx, (d, n) in enumerate(settled):
        # Guards (a) AND (b): below the MAD band AND materially low vs the median.
        if not (n < mad_lower and n < rel_lower):
            continue
        # Classify ACTIVE when within recent_window settled days of the frontier.
        within_recent = (frontier_idx - idx) < recent_window
        scope = SCOPE_ACTIVE if within_recent else SCOPE_HISTORICAL
        severity = SEVERITY_MEDIUM if within_recent else SEVERITY_LOW
        findings.append(
            {
                "detector": DETECTOR_VOLUME_OUTLIER,
                "anomaly_date": d,
                "observed_count": int(n),
                "expected_low": expected_low,
                "median_count": median_int,
                "date_lag_days": None,
                "severity": severity,
                "scope": scope,
                "audit_status": STATUS_ANOMALY,
            }
        )

    if findings:
        logger.info(
            "Volume detector flagged %d low day(s) -- median %d, expected_low %d",
            len(findings),
            median_int,
            expected_low,
        )

    return VolumeResult(
        findings=findings,
        settled_day_count=settled_day_count,
        settle_lag=settle_lag,
        cold_start=False,
    )
