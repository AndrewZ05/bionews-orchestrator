#!/usr/bin/env python3
# Detector 3: STRUCTURAL (always-on, any age, volume-immune).
# Consumes the SAME ordered (date, count) series and the SAME settle_lag the volume
# detector produced (the runner computes both ONCE and shares them) -- there is no
# SQL here. Two volume-immune signals: a SETTLED day with 0 rows on a table that is
# normally non-zero, and a table-wide collapse where the recent settled total is
# near-zero vs the prior comparable window. These fired 0 times in tuning -- low
# noise, high confidence -- and need little history so they run even at cold start.
"""Structural detector for the anomaly audit.

PURE functions over an ordered ``(date, count)`` series; no I/O, no BigQuery. Two
checks:

* Zero settled day: any SETTLED day (older than ``settle_lag`` from the frontier)
  whose count is 0 while the table is normally non-zero (body median > 0). Flagged
  HIGH; ACTIVE when within ``recent_window`` settled days of the frontier, else
  HISTORICAL.
* Table-wide collapse: the total over the last ``recent_window`` SETTLED days is
  below 5% of the total over the prior comparable ``recent_window`` SETTLED-day
  window (and the prior window total is positive). A single HIGH/ACTIVE flag.

All log strings are ASCII-only.
"""

import logging
import statistics
from datetime import date
from typing import Dict, List, Tuple

from shared.anomaly_audit import (
    DETECTOR_COLLAPSE,
    DETECTOR_STRUCTURAL_ZERO,
    SCOPE_ACTIVE,
    SCOPE_HISTORICAL,
    SEVERITY_HIGH,
    STATUS_ANOMALY,
)

logger = logging.getLogger(__name__)

# A table-wide collapse is flagged when the recent settled window total is below
# this fraction of the prior comparable window total.
_COLLAPSE_FRACTION = 0.05


def _median(values):
    """Return the median of a non-empty numeric iterable, else 0.0."""
    vals = list(values)
    if not vals:
        return 0.0
    return float(statistics.median(vals))


def detect_structural(
    series: List[Tuple[date, int]], today: date, settle_lag: int, recent_window: int
) -> List[Dict]:
    """Detect structural anomalies (zero settled days, table-wide collapse).

    Operates on the settled portion of the series (the last ``settle_lag`` days are
    excluded as still-filling, matching the volume detector). Needs little history,
    so it runs alongside the recency-lag detector even at cold start.

    Args:
        series: ordered (date, count) tuples (ascending by date).
        today: the current date (carried for symmetry; classification uses the
            settled frontier, not today, so a still-filling recent day cannot skew
            the structural verdict).
        settle_lag: trailing days to exclude as still-filling (from the volume
            detector's :func:`compute_settle_lag`).
        recent_window: window size in settled days for ACTIVE classification and the
            collapse comparison (the runner's max(7, 2*backfill_days)).

    Returns:
        A list of canonical finding dicts (possibly empty). Zero-day findings carry
        detector 'structural_zero'; the collapse finding carries detector 'collapse'.
    """
    findings: List[Dict] = []

    lag = settle_lag if (settle_lag and settle_lag > 0) else 0
    settled = series[: len(series) - lag] if lag else list(series)
    settled_day_count = len(settled)
    if settled_day_count == 0:
        return findings

    if recent_window is None or recent_window <= 0:
        recent_window = 7

    counts = [c for _, c in settled]
    body_median = _median(counts)
    frontier_idx = settled_day_count - 1

    # 1. Zero settled day on a normally-non-zero table (body median > 0). A table
    #    that legitimately has many zero days (body median 0) is NOT flagged.
    if body_median > 0:
        for idx, (d, n) in enumerate(settled):
            if n != 0:
                continue
            within_recent = (frontier_idx - idx) < recent_window
            scope = SCOPE_ACTIVE if within_recent else SCOPE_HISTORICAL
            findings.append(
                {
                    "detector": DETECTOR_STRUCTURAL_ZERO,
                    "anomaly_date": d,
                    "observed_count": 0,
                    "expected_low": None,
                    "median_count": int(round(body_median)),
                    "date_lag_days": None,
                    "severity": SEVERITY_HIGH,
                    "scope": scope,
                    "audit_status": STATUS_ANOMALY,
                }
            )
        if any(f["detector"] == DETECTOR_STRUCTURAL_ZERO for f in findings):
            logger.info(
                "Structural detector flagged %d zero settled day(s)",
                sum(1 for f in findings if f["detector"] == DETECTOR_STRUCTURAL_ZERO),
            )

    # 2. Table-wide collapse: recent settled-window total << prior comparable window.
    #    Only meaningful when there are two full comparable windows of settled days.
    if settled_day_count >= 2 * recent_window:
        recent_slice = settled[settled_day_count - recent_window :]
        prior_slice = settled[
            settled_day_count - 2 * recent_window : settled_day_count - recent_window
        ]
        recent_total = sum(c for _, c in recent_slice)
        prior_total = sum(c for _, c in prior_slice)
        if prior_total > 0 and recent_total < _COLLAPSE_FRACTION * prior_total:
            frontier_date = settled[frontier_idx][0]
            findings.append(
                {
                    "detector": DETECTOR_COLLAPSE,
                    "anomaly_date": frontier_date,
                    "observed_count": int(recent_total),
                    "expected_low": None,
                    "median_count": int(prior_total),
                    "date_lag_days": None,
                    "severity": SEVERITY_HIGH,
                    "scope": SCOPE_ACTIVE,
                    "audit_status": STATUS_ANOMALY,
                }
            )
            logger.info(
                "Structural detector flagged table-wide collapse -- recent total %d "
                "vs prior total %d over %d settled days",
                int(recent_total),
                int(prior_total),
                recent_window,
            )

    return findings
