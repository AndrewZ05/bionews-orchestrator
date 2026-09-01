#!/usr/bin/env python3
# Anomaly Audit subpackage.
# Houses the leaf detector modules (cadence learning, recency-lag, volume-outlier,
# structural zero/collapse), the result writer, the email summary builder and the
# orchestration runner used by the anomaly_audit plugin.
#
# This is a SECOND data-quality check beside the working table freshness audit
# (shared/freshness_audit/). Where the freshness audit answers "is the newest data
# recent enough?", the anomaly audit answers "is the date frontier still advancing,
# and do settled days have a normal row count?".
#
# This module re-exports the SINGLE shared identifier-validation helper from the
# freshness audit so that every SQL builder in this package uses identical
# allowlist logic and the project/dataset/table/column rules never drift. BigQuery
# cannot bind dataset/table/column names as @parameters, so any token interpolated
# into SQL MUST first pass through _validate_identifier.
"""Anomaly Audit package.

Provides the building blocks for the anomaly audit: cadence learning, the three
detectors (recency-lag, volume-outlier, structural zero/collapse), the results
sink, the ACTIVE-only email summary, and the orchestration runner. All log strings
in this package are ASCII-only (use ``--`` or ``:``, never em-dashes or other
unicode) and all SQL uses double-dash line comments exclusively (never the
slash-star block form).
"""

# Re-export the freshness audit's identifier validator UNCHANGED so the allowlist
# logic (project allows hyphens; dataset/table/date_column underscore-strict) is
# shared and cannot drift between the two audits.
from shared.freshness_audit import _validate_identifier  # noqa: F401

# Detector identifiers as written to the anomaly_results.detector column. The spec
# schema lists only 'recency_lag'|'volume_outlier'|'structural_zero'|'collapse';
# recency LAG and interior GAP findings are BOTH recorded under 'recency_lag'.
DETECTOR_RECENCY_LAG = "recency_lag"
DETECTOR_RECENCY_GAP = "recency_lag"
DETECTOR_VOLUME_OUTLIER = "volume_outlier"
DETECTOR_STRUCTURAL_ZERO = "structural_zero"
DETECTOR_COLLAPSE = "collapse"
# Metric-value detector: a watched metric COLUMN (e.g. post_reach) whose daily
# SUM goes to zero on settled days while it was historically non-zero. Row-count
# detectors above are blind to this -- rows stay present, only the value zeros out
# (the BI-486 reach deprecation: 70 rows/day, reach silently 0). See BI-486.
DETECTOR_METRIC_ZERO = "metric_zero"
# Preventive detectors (locate issues before they cross an SLA):
DETECTOR_SCHEMA_DRIFT = "schema_drift"
DETECTOR_DUPLICATE_KEYS = "duplicate_keys"
DETECTOR_NULL_RATE = "null_rate"
DETECTOR_FRESHNESS_TREND = "freshness_trend"

# Severity levels (drive email grouping/sorting HIGH -> MEDIUM -> LOW).
SEVERITY_HIGH = "HIGH"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_LOW = "LOW"

# Scope: ACTIVE findings are emailed; HISTORICAL findings go only to the results
# table (surfaced in email solely via the "N new historical outlier(s)" note).
SCOPE_ACTIVE = "ACTIVE"
SCOPE_HISTORICAL = "HISTORICAL"

# Per-row audit status written to anomaly_results.audit_status.
STATUS_OK = "OK"
STATUS_ANOMALY = "ANOMALY"
STATUS_ERROR = "ERROR"

__all__ = [
    "_validate_identifier",
    "DETECTOR_RECENCY_LAG",
    "DETECTOR_RECENCY_GAP",
    "DETECTOR_VOLUME_OUTLIER",
    "DETECTOR_STRUCTURAL_ZERO",
    "DETECTOR_COLLAPSE",
    "DETECTOR_METRIC_ZERO",
    "DETECTOR_SCHEMA_DRIFT",
    "DETECTOR_DUPLICATE_KEYS",
    "DETECTOR_NULL_RATE",
    "DETECTOR_FRESHNESS_TREND",
    "SEVERITY_HIGH",
    "SEVERITY_MEDIUM",
    "SEVERITY_LOW",
    "SCOPE_ACTIVE",
    "SCOPE_HISTORICAL",
    "STATUS_OK",
    "STATUS_ANOMALY",
    "STATUS_ERROR",
]
