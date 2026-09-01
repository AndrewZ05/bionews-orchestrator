#!/usr/bin/env python3
"""Orchestration heart of the Anomaly Audit feature.

``run_anomaly_audit`` ties the anomaly_audit subpackage together, MIRRORING the
freshness ``audit_runner`` in structure, retry policy and resilience contract:

  1. ``_resolve_audit_settings`` pulls the ``audit.*`` block (K, rel_floor,
     min_count, backfill_days, grace, history_days, min_history_settled,
     recent_window_floor, retry, results_dataset/table, location, sources,
     recipients) plus the optional per-table ``overrides`` block keyed by
     ``dataset.table``;
  2. ``build_all_targets`` (REUSED from the freshness yaml_target_loader,
     UNCHANGED) produces one target per audited table -- the SAME tables and
     resolved date columns the freshness audit uses;
  3. ensure the results dataset + table exist;
  4. snapshot the prior HISTORICAL ``(dataset, table, anomaly_date)`` keys from
     ``anomaly_results`` BEFORE writing this run, for the new-outlier diff;
  5. for EACH target -- inside a per-table try/except that NEVER aborts the run
     (transient retry with backoff, mirrors freshness ``_audit_one_table``) --
     resolve per-table overrides, fetch the present-date set (recency) and the
     daily-count series (volume + structural) ONCE, learn cadence + the active
     weekday set, compute settle_lag once and share it, gate on cold start, run
     the three detectors, and collect findings into result rows. A table with NO
     findings still yields ONE OK row; validation/probe errors yield ONE ERROR
     row with an ASCII error_message;
  6. strip the helper keys and write every row;
  7. compute N = the count of genuinely new HISTORICAL combos vs the pre-write
     snapshot;
  8. email ACTIVE-only findings plus the N-new-historical note;
  9. return a summary dict.

Resilience contract: only a config-unreadable condition is run-fatal; every
per-table problem becomes an ERROR row so one bad table cannot abort the run.

ASCII-ONLY RULE: every log string in this module is pure ASCII (use -- or :,
never an em-dash or other unicode).
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.anomaly_audit import (
    SCOPE_ACTIVE,
    SCOPE_HISTORICAL,
    STATUS_ANOMALY,
    STATUS_ERROR,
    STATUS_OK,
    _validate_identifier,
)
from shared.anomaly_audit.cadence import active_weekdays, learn_cadence
from shared.anomaly_audit.recency_detector import (
    RecencyQueryError,
    detect_recency,
    query_distinct_dates,
)
from shared.anomaly_audit.volume_detector import (
    compute_settle_lag,
    detect_volume,
    query_daily_counts,
)
from shared.anomaly_audit.structural_detector import detect_structural
from shared.anomaly_audit.metric_value_detector import (
    detect_metric_zero,
    query_metric_sum,
)
from shared.anomaly_audit.result_writer import (
    ensure_results_dataset,
    ensure_results_table,
    write_results,
    write_run_row,
)
from shared.anomaly_audit import email_sender
from shared.anomaly_audit.schema_detector import detect_schema_drift, fetch_live_columns
from shared.anomaly_audit.quality_detector import run_quality_checks
from shared.audit_job_context import fetch_job_context
from shared.audit_trends import detect_at_risk, fetch_days_behind_history
from shared.audit_views import ensure_anomaly_views
from shared.audit_watchdog import check_sibling_run
from shared.freshness_audit.yaml_target_loader import build_all_targets

logger = logging.getLogger(__name__)

# Default retry policy if the config omits audit.retry. Matches the spec.
_DEFAULT_RETRY_ATTEMPTS = 3
_DEFAULT_BACKOFF_SECONDS = [5, 15, 45]

# Validated default parameters (design "Validated default parameters" table).
_DEFAULT_K = 4.0
_DEFAULT_REL_FLOOR = 0.5
_DEFAULT_MIN_COUNT = 100
_DEFAULT_BACKFILL_DAYS = 5
_DEFAULT_GRACE = 1
_DEFAULT_HISTORY_DAYS = 90
_DEFAULT_MIN_HISTORY_SETTLED = 30
_DEFAULT_RECENT_WINDOW_FLOOR = 7

# The exact columns of the anomaly_results schema, in order. Helper keys
# (email_recipients / send_email_notification) carried on the result dict for the
# email step are dropped before writing. audit_cadence/audit_reason are step 3
# additions surfacing the per-resource audit policy on each row.
_RESULTS_SCHEMA_COLUMNS = (
    "audit_id",
    "audit_run_id",
    "dataset_name",
    "table_name",
    "date_column_used",
    "detector",
    "anomaly_date",
    "observed_count",
    "expected_low",
    "median_count",
    "date_lag_days",
    "severity",
    "scope",
    "audit_status",
    "error_message",
    "audited_at",
    "audit_cadence",
    "audit_reason",
)


def _resolve_audit_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """Pull the audit.* block out of config, applying defaults where omitted.

    Mirrors the freshness ``_resolve_audit_settings``: defensively coerces the
    retry policy, derives the sources list and the recipient string, and reads the
    detector tuning defaults plus the optional per-table overrides block.
    """
    audit = config.get("audit", {}) or {}
    retry = audit.get("retry", {}) or {}

    attempts = retry.get("attempts", _DEFAULT_RETRY_ATTEMPTS)
    backoff = retry.get("backoff_seconds", _DEFAULT_BACKOFF_SECONDS)

    # Defensive coercion: attempts must be a positive int; backoff a list of ints.
    try:
        attempts = int(attempts)
    except (TypeError, ValueError):
        attempts = _DEFAULT_RETRY_ATTEMPTS
    if attempts < 1:
        attempts = _DEFAULT_RETRY_ATTEMPTS
    if not isinstance(backoff, (list, tuple)) or not backoff:
        backoff = _DEFAULT_BACKOFF_SECONDS

    # Sources to audit. Single source of truth is configs/audit_sources.yaml
    # (shared with the freshness audit). The legacy per-audit ``audit.sources:``
    # list still wins when present so an emergency override does not require
    # editing the registry; otherwise we fall back to the registry.
    legacy_sources = audit.get("sources") or []
    if not isinstance(legacy_sources, (list, tuple)):
        legacy_sources = [legacy_sources]
    if legacy_sources:
        sources = list(legacy_sources)
        logger.info(
            "Using legacy audit.sources from config (%d source(s)); "
            "consider removing it to defer to configs/audit_sources.yaml",
            len(sources),
        )
    else:
        from shared.freshness_audit.yaml_target_loader import (
            load_active_audit_sources,
        )

        sources = load_active_audit_sources()

    # Recipients: prefer audit.email_recipients, else the email channel's list.
    email_channel = (
        (config.get("notifications", {}) or {}).get("channels", {}) or {}
    ).get("email", {}) or {}
    recipients = audit.get("email_recipients")
    if not recipients:
        chan_recipients = email_channel.get("recipients") or []
        if isinstance(chan_recipients, (list, tuple)):
            recipients = ",".join(chan_recipients) if chan_recipients else None
        else:
            recipients = chan_recipients or None

    # Per-table overrides keyed by 'dataset.table'. Each value may carry any of
    # K / rel_floor / min_count / backfill_days / grace / cadence_days.
    overrides = audit.get("overrides") or {}
    if not isinstance(overrides, dict):
        overrides = {}

    return {
        "sources": list(sources),
        "recipients": recipients,
        "results_dataset": audit.get("results_dataset", "anomaly_audit"),
        "results_table": audit.get("results_table", "anomaly_results"),
        # Sibling (freshness) audit locations: read by the at-risk trend detector
        # and the mutual watchdog.
        "freshness_dataset": audit.get("freshness_dataset", "freshness_audit"),
        "freshness_table": audit.get("freshness_table", "table_freshness_results"),
        # Preventive-check toggles (all default ON; disable per environment).
        "schema_drift_enabled": bool(audit.get("schema_drift", True)),
        "quality_checks_enabled": bool(audit.get("quality_checks", True)),
        "freshness_trend_enabled": bool(audit.get("freshness_trend", True)),
        "retry_attempts": attempts,
        "backoff_seconds": list(backoff),
        "location": (config.get("bigquery", {}) or {}).get("location", "US"),
        # Detector tuning defaults (per-table overrides applied later).
        "K": float(audit.get("K", _DEFAULT_K)),
        "rel_floor": float(audit.get("rel_floor", _DEFAULT_REL_FLOOR)),
        "min_count": int(audit.get("min_count", _DEFAULT_MIN_COUNT)),
        "backfill_days": int(audit.get("backfill_days", _DEFAULT_BACKFILL_DAYS)),
        "grace": int(audit.get("grace", _DEFAULT_GRACE)),
        "history_days": int(audit.get("history_days", _DEFAULT_HISTORY_DAYS)),
        "min_history_settled": int(
            audit.get("min_history_settled", _DEFAULT_MIN_HISTORY_SETTLED)
        ),
        "recent_window_floor": int(
            audit.get("recent_window_floor", _DEFAULT_RECENT_WINDOW_FLOOR)
        ),
        "overrides": overrides,
    }


def _resolve_table_params(
    settings: Dict[str, Any],
    dataset_name: str,
    table_name: str,
    *,
    resource_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Resolve the effective detector parameters for one table.

    Precedence (highest wins):
      1. ``resource_overrides`` -- the per-resource ``audit.anomaly:`` block from
         the SOURCE YAML (e.g. limesurvey.yaml). Lives next to the resource so
         tuning travels with the resource definition; this is the operator's
         primary surface for per-table cadence/sensitivity.
      2. ``audit.overrides[dataset.table]`` in the ANOMALY AUDIT YAML -- the
         global back-compat tuning surface. Still honored so existing
         configurations keep working.
      3. Audit-level defaults from ``audit.*`` (K, rel_floor, min_count, etc.).

    Recognized knobs (any source): ``K``, ``rel_floor``, ``min_count``,
    ``backfill_days``, ``grace``, ``cadence_days``. Per-resource overrides may
    additionally set ``enabled: false`` to suppress detector findings entirely
    (handled by the caller, not here).

    ``recent_window`` is DERIVED from the effective backfill_days as
    ``max(recent_window_floor, 2 * backfill_days)`` so a table is never judged on
    volume until its backfill has settled. The recency-lag detector is NEVER
    window-gated -- recent_window applies only to the volume/structural
    ACTIVE-vs-HISTORICAL classification.
    """
    key = "{0}.{1}".format(dataset_name, table_name)
    global_override = settings["overrides"].get(key) or {}
    if not isinstance(global_override, dict):
        global_override = {}
    resource_override = resource_overrides or {}
    if not isinstance(resource_override, dict):
        resource_override = {}

    def _pick(name, default, cast):
        # Resource override wins over global override wins over default.
        if name in resource_override and resource_override[name] is not None:
            return cast(resource_override[name])
        if name in global_override and global_override[name] is not None:
            return cast(global_override[name])
        return cast(default)

    K = _pick("K", settings["K"], float)
    rel_floor = _pick("rel_floor", settings["rel_floor"], float)
    min_count = _pick("min_count", settings["min_count"], int)
    backfill_days = _pick("backfill_days", settings["backfill_days"], int)
    grace = _pick("grace", settings["grace"], int)

    # Optional explicit cadence override; None means auto-learn from history.
    cadence_raw = resource_override.get("cadence_days")
    if cadence_raw is None:
        cadence_raw = global_override.get("cadence_days")
    cadence_override: Optional[int] = None
    if cadence_raw is not None:
        try:
            cadence_override = int(cadence_raw)
        except (TypeError, ValueError):
            cadence_override = None

    recent_window = max(settings["recent_window_floor"], 2 * backfill_days)

    # watch_metrics: list of metric COLUMNS whose daily SUM should be watched for
    # the BI-486 "value went to zero while rows stayed" pattern. Resource override
    # wins over global override; empty/absent means the metric-zero detector is a
    # no-op for this table (row-count detectors still run).
    watch_metrics = resource_override.get("watch_metrics")
    if watch_metrics is None:
        watch_metrics = global_override.get("watch_metrics")
    if not isinstance(watch_metrics, (list, tuple)):
        watch_metrics = []
    watch_metrics = [str(m) for m in watch_metrics if m]

    # Metric-zero detector knobs, INDEPENDENT of the row-count settle/window so a
    # metric going to zero alerts fast (the row-count settle_lag auto-inflates --
    # and the zeros themselves inflate it). metric_recent_window: how many recent
    # days must ALL be zero to fire (default 3 = confident + fast). metric_settle_lag:
    # trailing days excluded as possibly-partial (default 1 = just today).
    metric_recent_window = _pick(
        "metric_recent_window", settings.get("metric_recent_window", 3), int
    )
    metric_settle_lag = _pick(
        "metric_settle_lag", settings.get("metric_settle_lag", 1), int
    )

    return {
        "K": K,
        "rel_floor": rel_floor,
        "min_count": min_count,
        "backfill_days": backfill_days,
        "grace": grace,
        "cadence_override": cadence_override,
        "recent_window": recent_window,
        "watch_metrics": watch_metrics,
        "metric_recent_window": metric_recent_window,
        "metric_settle_lag": metric_settle_lag,
    }


def _build_result_row(
    config_row: Dict[str, Any],
    audit_run_id: str,
    audited_at,
    date_column_used,
    *,
    detector=None,
    anomaly_date=None,
    observed_count=None,
    expected_low=None,
    median_count=None,
    date_lag_days=None,
    severity=None,
    scope=None,
    audit_status,
    error_message=None,
) -> Dict[str, Any]:
    """Build a single anomaly_results row dict for one audited table/finding.

    Mirrors the freshness ``_build_result_row``: returns the 16 results-schema
    columns PLUS two transient helper keys (``email_recipients`` and
    ``send_email_notification``) copied from the config row for the email step;
    the helper keys are stripped by ``_strip_helper_keys`` before the row is
    written to BigQuery.

    ``anomaly_date`` is serialized to a 'YYYY-MM-DD' string (or None) and
    ``audited_at`` to an ISO-8601 string here so the dict handed to
    ``write_results`` -> ``client.insert_rows_json`` is JSON-serializable (the
    stdlib JSON encoder cannot handle datetime.date / datetime.datetime). The
    numeric columns stay int|None.
    """
    anomaly_date_iso = (
        anomaly_date.isoformat() if hasattr(anomaly_date, "isoformat") else anomaly_date
    )
    audited_at_iso = (
        audited_at.isoformat() if hasattr(audited_at, "isoformat") else audited_at
    )

    return {
        # --- results schema columns ---
        "audit_id": uuid.uuid4().hex,
        "audit_run_id": audit_run_id,
        "dataset_name": config_row.get("dataset_name"),
        "table_name": config_row.get("table_name"),
        "date_column_used": date_column_used,
        "detector": detector,
        "anomaly_date": anomaly_date_iso,
        "observed_count": observed_count,
        "expected_low": expected_low,
        "median_count": median_count,
        "date_lag_days": date_lag_days,
        "severity": severity,
        "scope": scope,
        "audit_status": audit_status,
        "error_message": error_message,
        "audited_at": audited_at_iso,
        # Per-resource audit policy stamped by yaml_target_loader. NULL when the
        # resource has no audit: override and source-level defaults apply.
        "audit_cadence": config_row.get("audit_cadence"),
        "audit_reason": config_row.get("audit_reason"),
        # --- transient helper keys (stripped before write) ---
        "email_recipients": config_row.get("email_recipients"),
        "send_email_notification": config_row.get("send_email_notification"),
    }


def _strip_helper_keys(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``result`` containing ONLY the 16 results-schema columns.

    Drops the transient ``email_recipients`` / ``send_email_notification`` helper
    keys so the dict matches the anomaly_results schema exactly for streaming
    inserts.
    """
    return {col: result.get(col) for col in _RESULTS_SCHEMA_COLUMNS}


def _row_from_finding(
    config_row: Dict[str, Any],
    audit_run_id: str,
    audited_at,
    date_column_used,
    finding: Dict[str, Any],
) -> Dict[str, Any]:
    """Map one canonical detector finding dict into a result row.

    Detectors return finding dicts with the canonical key set (detector,
    anomaly_date, severity, scope, observed_count, expected_low, median_count,
    date_lag_days, audit_status). This adapter passes them straight through to
    ``_build_result_row`` so all three detectors share one row-building path.
    """
    return _build_result_row(
        config_row,
        audit_run_id,
        audited_at,
        date_column_used,
        detector=finding.get("detector"),
        anomaly_date=finding.get("anomaly_date"),
        observed_count=finding.get("observed_count"),
        expected_low=finding.get("expected_low"),
        median_count=finding.get("median_count"),
        date_lag_days=finding.get("date_lag_days"),
        severity=finding.get("severity"),
        scope=finding.get("scope"),
        audit_status=finding.get("audit_status", STATUS_ANOMALY),
        # Preventive detectors (schema drift, dup keys, null rate, trend) carry a
        # human-readable description on the finding; legacy detectors leave None.
        error_message=finding.get("error_message"),
    )


def _audit_one_table(
    client,
    config_row: Dict[str, Any],
    audit_run_id: str,
    audited_at,
    today,
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Evaluate a single audited table with retry, returning its result rows.

    Never raises for the expected failure modes: a query failure (transient or
    permanent) becomes a single ERROR row with an ASCII error_message. A table
    with no findings yields a single OK row so every audited table is represented.
    Transient failures are retried with exponential backoff by THIS loop; the
    detector query functions are invoked with retry_attempts=1 so the budget is
    not multiplied across two layers.

    Cold-start gating lives here: settled_day_count = len(series) - settle_lag; if
    it is below min_history_settled, only the recency-lag and structural-zero
    detectors run (they need little history); the volume band is skipped and a
    'baseline warming up' note is logged.
    """
    dataset_name = config_row.get("dataset_name")
    table_name = config_row.get("table_name")
    date_column = config_row.get("date_column")
    table_label = "{0}.{1}".format(dataset_name, table_name)

    # Per-resource anomaly overrides stamped on the target by the yaml_target_loader
    # (audit.anomaly: block in the source YAML). Layered above the audit-level
    # overrides keyed by 'dataset.table' inside _resolve_table_params.
    resource_overrides = config_row.get("anomaly_overrides") or {}
    if not isinstance(resource_overrides, dict):
        resource_overrides = {}
    # 'enabled: false' on the resource's audit.anomaly block suppresses the
    # recency-lag detector specifically -- a known-irregular table whose cadence
    # is intentional and would false-fire under recency. Volume + structural
    # still run because a row count crashing to zero is signal regardless of
    # cadence (operator's earlier decision).
    recency_enabled = bool(resource_overrides.get("enabled", True))

    params = _resolve_table_params(
        settings,
        dataset_name,
        table_name,
        resource_overrides=resource_overrides,
    )
    retry_attempts = settings["retry_attempts"]
    backoff_seconds = settings["backoff_seconds"]
    history_days = settings["history_days"]

    last_error: Optional[str] = None

    for attempt in range(retry_attempts):
        try:
            # 1. Fetch the present-date set (recency) and the daily-count series
            #    (volume + structural) ONCE. Both query functions run with
            #    retry_attempts=1 -- this outer loop owns the retry budget.
            present_dates = query_distinct_dates(
                client,
                config_row.get("project_id"),
                dataset_name,
                table_name,
                date_column,
                history_days,
                retry_attempts=1,
                backoff_seconds=backoff_seconds,
            )
            series: List[Tuple] = query_daily_counts(
                client,
                config_row.get("project_id"),
                dataset_name,
                table_name,
                date_column,
                history_days,
                retry_attempts=1,
                backoff_seconds=backoff_seconds,
            )

            # 2. Learn cadence + the active weekday set from the present dates. An
            #    explicit per-table cadence override wins over the auto-learned one.
            cadence = params["cadence_override"]
            if cadence is None or cadence < 1:
                cadence = learn_cadence(present_dates)
            weekdays = active_weekdays(present_dates)

            # 3. Compute settle_lag ONCE and share it with both volume and
            #    structural so the still-filling trailing region is consistent.
            settle_lag = compute_settle_lag(series, params["backfill_days"])
            recent_window = params["recent_window"]
            settled_day_count = max(0, len(series) - settle_lag)
            cold_start = settled_day_count < settings["min_history_settled"]

            findings: List[Dict[str, Any]] = []

            # 4a. Recency-lag (PRIMARY): always runs, never window-gated, presence
            #     only. Tolerates a partially-filled current day.
            #     Suppressed for tables whose per-resource audit.anomaly.enabled
            #     is false -- known-irregular cadence tables would false-fire here
            #     even with the auto-learned cadence (e.g. snapshot tables loaded
            #     on demand). Volume + structural still run below.
            if recency_enabled:
                findings.extend(
                    detect_recency(
                        present_dates,
                        today,
                        cadence,
                        weekdays,
                        params["grace"],
                        recent_window,
                    )
                )
            else:
                logger.debug(
                    "Recency detector suppressed for %s by audit.anomaly.enabled:false",
                    table_label,
                )

            # 4b. Structural (always-on, volume-immune): zero settled day +
            #     table-wide collapse. Needs little history so it runs at cold
            #     start alongside recency.
            findings.extend(detect_structural(series, today, settle_lag, recent_window))

            # 4b-bis. Metric-value zero (BI-486): for each watched metric COLUMN,
            #     query its daily SUM and flag if it went all-zero on the recent
            #     settled window while historically non-zero. Row-count detectors
            #     above are blind to this. A per-metric query failure (e.g. column
            #     dropped) is logged and skipped -- it must not abort the table.
            for metric_col in params.get("watch_metrics", []):
                try:
                    metric_series = query_metric_sum(
                        client,
                        config_row.get("project_id"),
                        dataset_name,
                        table_name,
                        metric_col,
                        date_column,
                        history_days,
                        retry_attempts=1,
                        backoff_seconds=backoff_seconds,
                    )
                    findings.extend(
                        detect_metric_zero(
                            metric_col,
                            metric_series,
                            today,
                            params["metric_recent_window"],
                            settings["min_history_settled"],
                            metric_settle_lag=params["metric_settle_lag"],
                        )
                    )
                except Exception as metric_exc:  # noqa: BLE001
                    logger.warning(
                        "Metric-zero check skipped for %s column %s: %s",
                        table_label,
                        metric_col,
                        metric_exc,
                    )

            # 4c. Volume band (SECONDARY): only when enough settled history exists.
            #     At cold start the band is SKIPPED and a baseline-warming note is
            #     logged (recency + structural-zero still cover the urgent signals).
            if cold_start:
                logger.info(
                    "Cold start for %s: %d settled day(s) < %d -- baseline warming "
                    "up, skipping volume band (recency + structural only)",
                    table_label,
                    settled_day_count,
                    settings["min_history_settled"],
                )
            else:
                volume_result = detect_volume(
                    series,
                    today,
                    params["K"],
                    params["rel_floor"],
                    params["min_count"],
                    params["backfill_days"],
                    recent_window,
                    settings["min_history_settled"],
                )
                # VolumeResult exposes findings + settled_day_count; the detector
                # also self-gates on min_history, so its findings are empty if it
                # decided history was insufficient.
                findings.extend(getattr(volume_result, "findings", []) or [])

            # 5. Build result rows. Every audited table gets >=1 row: each finding
            #    is its own ANOMALY row, and a table with no findings gets one OK
            #    row so it is represented (like freshness).
            if findings:
                rows = [
                    _row_from_finding(
                        config_row, audit_run_id, audited_at, date_column, f
                    )
                    for f in findings
                ]
                # Tag rows with the cold-start state (transient helper key, stripped
                # before write) so the summary can report which tables are still
                # warming up even when they ALSO produced recency/structural findings.
                if cold_start:
                    for r in rows:
                        r["_cold_start"] = True
                anomaly_count = sum(
                    1 for f in findings if f.get("audit_status") == STATUS_ANOMALY
                )
                logger.info(
                    "%s: %d finding(s) (%d anomaly), cadence=%d, settle_lag=%d, "
                    "recent_window=%d%s",
                    table_label,
                    len(findings),
                    anomaly_count,
                    cadence,
                    settle_lag,
                    recent_window,
                    " [cold start]" if cold_start else "",
                )
                return rows

            logger.debug(
                "OK %s: no anomalies (cadence=%d, settle_lag=%d, recent_window=%d)",
                table_label,
                cadence,
                settle_lag,
                recent_window,
            )
            # Surface the cold-start state in a machine-readable field (not just
            # logs) per design decision 7: the OK row carries 'baseline warming up'
            # so the warming-up status is observable in anomaly_results and the
            # summary, while audit_status stays OK (no anomaly).
            return [
                _build_result_row(
                    config_row,
                    audit_run_id,
                    audited_at,
                    date_column,
                    audit_status=STATUS_OK,
                    error_message="baseline warming up" if cold_start else None,
                )
            ]

        except (
            RecencyQueryError,
            Exception,
        ) as exc:  # noqa: BLE001 - per-table boundary
            # Distinguish a transient query failure (retry) from anything else.
            # The query helpers raise typed errors only after their own (single)
            # attempt; here we classify by the same transient markers via message.
            last_error = "anomaly probe error: {0}".format(exc)
            message = str(exc).lower()
            # Strict-by-default: a missing table is captured as an ERROR row,
            # not silently skipped. The reason string carried on the row says
            # "table not found", which is self-documenting. To suppress a
            # known-absent table set active: false in the source YAML so the
            # suppression is operator intent rather than a silent coverage gap.
            # Falls through to the ERROR-row return at the end of the function.
            transient = any(
                marker in message
                for marker in (
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
            )
            if transient and attempt < retry_attempts - 1:
                idx = min(attempt, len(backoff_seconds) - 1)
                delay = backoff_seconds[idx]
                logger.warning(
                    "Probe failed for %s (attempt %d/%d): %s -- retrying in %ss",
                    table_label,
                    attempt + 1,
                    retry_attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)
                continue
            logger.warning("Failed auditing %s: %s", table_label, exc)
            break

    # All retries exhausted (or a non-retried error) -- one ERROR row.
    return [
        _build_result_row(
            config_row,
            audit_run_id,
            audited_at,
            config_row.get("date_column"),
            audit_status=STATUS_ERROR,
            error_message=last_error or "table could not be evaluated",
        )
    ]


def _snapshot_prior_historical(client, results_dataset: str, results_table: str) -> set:
    """Snapshot existing HISTORICAL (dataset, table, anomaly_date) keys.

    Runs a simple SELECT DISTINCT over the results table for scope='HISTORICAL'
    BEFORE this run writes. The returned set is diffed against this run's
    HISTORICAL findings (in Python) to count genuinely new combos for the email's
    one-line note -- so the note never repeats an already-catalogued item. A query
    failure (e.g. the table was just created and is empty) yields an empty set so
    the diff degrades to "all of this run's historical combos are new".
    """
    safe_dataset = _validate_identifier(results_dataset, "dataset")
    safe_table = _validate_identifier(results_table, "table")
    sql = (
        "-- Snapshot prior HISTORICAL keys for the new-outlier diff. Scope is bound\n"
        "-- as a scalar parameter; identifiers are interpolated after validation.\n"
        "SELECT DISTINCT dataset_name, table_name, anomaly_date\n"
        "FROM `{0}`.`{1}`\n"
        "WHERE scope = @scope AND anomaly_date IS NOT NULL".format(
            safe_dataset, safe_table
        )
    )
    try:
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("scope", "STRING", SCOPE_HISTORICAL)
            ]
        )
        rows = list(client.query(sql, job_config=job_config).result())
        snapshot = set()
        for r in rows:
            ad = r["anomaly_date"]
            ad_iso = ad.isoformat() if hasattr(ad, "isoformat") else ad
            snapshot.add((r["dataset_name"], r["table_name"], ad_iso))
        logger.info(
            "Snapshotted %d prior HISTORICAL outlier key(s) for the new-outlier diff",
            len(snapshot),
        )
        return snapshot
    except Exception as exc:  # noqa: BLE001 - snapshot is best-effort
        logger.warning(
            "Could not snapshot prior HISTORICAL outliers (treating as empty): %s",
            exc,
        )
        return set()


def _count_new_historical(results: List[Dict[str, Any]], prior_snapshot: set) -> int:
    """Count genuinely new HISTORICAL (dataset, table, anomaly_date) combos.

    Diffs this run's HISTORICAL ANOMALY rows against the pre-write snapshot. An
    already-recorded historical date contributes 0; only combos absent from the
    snapshot are counted, deduped within this run.
    """
    this_run = set()
    for row in results:
        if row.get("scope") != SCOPE_HISTORICAL:
            continue
        if row.get("audit_status") != STATUS_ANOMALY:
            continue
        key = (
            row.get("dataset_name"),
            row.get("table_name"),
            row.get("anomaly_date"),
        )
        if key[2] is None:
            continue
        this_run.add(key)
    new_combos = this_run - prior_snapshot
    return len(new_combos)


def _snapshot_prior_active(client, results_dataset: str, results_table: str) -> set:
    """Snapshot the PRIOR run's ACTIVE (dataset, table, detector) anomaly keys.

    Runs BEFORE this run writes, so the latest audit_run_id in the results table
    is the previous run. The returned set is diffed against this run's ACTIVE
    findings to produce the new/resolved lists for the email. Best-effort: any
    failure returns an empty set (delta degrades to "no prior baseline").
    """
    safe_dataset = _validate_identifier(results_dataset, "dataset")
    safe_table = _validate_identifier(results_table, "table")
    sql = (
        "-- Snapshot the prior run's ACTIVE anomaly keys for the run-over-run\n"
        "-- delta. Scope/status are bound; identifiers validated+interpolated.\n"
        "SELECT DISTINCT dataset_name, table_name, detector\n"
        "FROM `{0}`.`{1}`\n"
        "WHERE scope = @scope AND audit_status = @status\n"
        "  AND audit_run_id = (\n"
        "    SELECT audit_run_id FROM `{0}`.`{1}` ORDER BY audited_at DESC LIMIT 1\n"
        "  )".format(safe_dataset, safe_table)
    )
    try:
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("scope", "STRING", SCOPE_ACTIVE),
                bigquery.ScalarQueryParameter("status", "STRING", STATUS_ANOMALY),
            ]
        )
        rows = list(client.query(sql, job_config=job_config).result())
        snapshot = {(r["dataset_name"], r["table_name"], r["detector"]) for r in rows}
        logger.info(
            "Snapshotted %d prior ACTIVE anomaly key(s) for the run-over-run delta",
            len(snapshot),
        )
        return snapshot
    except Exception as exc:  # noqa: BLE001 - snapshot is best-effort
        logger.warning(
            "Could not snapshot prior ACTIVE anomalies (delta disabled): %s", exc
        )
        return set()


def _compute_active_delta(results: List[Dict[str, Any]], prior_active: set) -> dict:
    """Diff this run's ACTIVE anomaly keys against the prior run's.

    Returns {"new_active": [labels], "resolved_active": [labels]} where a label
    is "dataset.table (detector)". With no prior baseline (empty snapshot) the
    resolved list is empty and every current ACTIVE key would read as new -- so
    callers should treat an empty snapshot as "no delta available" if desired;
    here we report it plainly (first run simply lists all actives as new).
    """
    current = set()
    for row in results:
        if row.get("audit_status") != STATUS_ANOMALY:
            continue
        if row.get("scope") != SCOPE_ACTIVE:
            continue
        current.add(
            (row.get("dataset_name"), row.get("table_name"), row.get("detector"))
        )

    def _label(key):
        return "{0}.{1} ({2})".format(key[0], key[1], key[2])

    return {
        "new_active": sorted(_label(k) for k in (current - prior_active)),
        "resolved_active": sorted(_label(k) for k in (prior_active - current)),
    }


def run_anomaly_audit(
    client,
    config: Dict[str, Any],
    audit_run_id: str,
    additional_recipients: Optional[List[str]] = None,
    today=None,
) -> Dict[str, Any]:
    """Run a full anomaly audit and return a summary dict.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        config: the loaded anomaly_audit config (see configs/anomaly_audit.yaml).
        audit_run_id: caller-supplied id tying all result rows of this run together.
        additional_recipients: extra email addresses to always include on the
            summary email (additive to per-target recipients).
        today: reference date for the recency/volume windows. None (production)
            means the run timestamp's date; tests pass a fixed date so fixtures
            stay deterministic.

    Returns:
        {
          audit_run_id, total_tables, anomalies, active, historical, errored,
          written, new_historical, email_sent, results: [<schema dicts>...]
        }

    Resilience: only a config-unreadable / target-build fatal propagates; every
    per-table problem is captured as an ERROR row so one bad table cannot abort.
    """
    settings = _resolve_audit_settings(config)

    # Single run-level timestamp + reference date so every row of this run shares
    # an audited_at, and recency uses one consistent "today". The today override
    # exists for deterministic tests (fixtures anchored to a fixed date would
    # otherwise rot as real days pass); production callers leave it None.
    audited_at = datetime.now(timezone.utc)
    if today is None:
        today = audited_at.date()

    logger.info(
        "Starting anomaly audit run %s (sources=%s, results=%s.%s)",
        audit_run_id,
        ",".join(settings["sources"]) or "(none)",
        settings["results_dataset"],
        settings["results_table"],
    )

    # 1. Build the per-table audit list by REUSING the freshness yaml_target_loader
    #    UNCHANGED -- the anomaly audit audits the SAME production tables on the
    #    SAME resolved date columns. A source that fails to load is skipped inside
    #    build_all_targets and never aborts the run.
    targets = build_all_targets(
        settings["sources"], client.project, recipients=settings["recipients"]
    )
    logger.info("Built %d anomaly audit target(s)", len(targets))

    # 2. Ensure the results sink exists before we start writing, then refresh
    #    the latest-run convenience view (best-effort inside ensure_anomaly_views).
    ensure_results_dataset(client, settings["results_dataset"], settings["location"])
    ensure_results_table(client, settings["results_dataset"], settings["results_table"])
    ensure_anomaly_views(client, settings["results_dataset"], settings["results_table"])

    # 3. Snapshot prior HISTORICAL keys and the prior run's ACTIVE keys BEFORE
    #    writing, so both the new-outlier note and the run-over-run delta can be
    #    computed as Python set-diffs against this run's findings.
    prior_snapshot = _snapshot_prior_historical(
        client, settings["results_dataset"], settings["results_table"]
    )
    prior_active = _snapshot_prior_active(
        client, settings["results_dataset"], settings["results_table"]
    )

    # 3b. Prefetch LIVE COLUMNS once per dataset for the schema-drift check (one
    #     INFORMATION_SCHEMA query per dataset, not per table). A dataset whose
    #     fetch fails maps to None -> schema checks silently skip its tables.
    live_columns_by_dataset: Dict[str, Any] = {}
    if settings["schema_drift_enabled"]:
        for ds in sorted({t["dataset_name"] for t in targets}):
            try:
                live_columns_by_dataset[ds] = fetch_live_columns(
                    client, client.project, ds
                )
            except Exception as exc:  # noqa: BLE001 - drift check is preventive
                logger.warning("Schema-drift fetch failed for %s: %s", ds, exc)
                live_columns_by_dataset[ds] = None

    # 4. Audit each table. The per-table helper never raises for expected errors.
    results: List[Dict[str, Any]] = []
    for config_row in targets:
        rows = _audit_one_table(
            client, config_row, audit_run_id, audited_at, today, settings
        )
        results.extend(rows)

        # Preventive checks run only for tables that were actually audited (an
        # empty rows list means the table was skipped as missing).
        if not rows:
            continue
        date_column = config_row.get("date_column")

        # 4d. SCHEMA DRIFT (pure compare vs the per-dataset prefetch).
        if settings["schema_drift_enabled"]:
            dataset_columns = live_columns_by_dataset.get(config_row["dataset_name"])
            if dataset_columns is not None:
                for finding in detect_schema_drift(
                    config_row,
                    dataset_columns.get(config_row["table_name"]),
                    today,
                ):
                    results.append(
                        _row_from_finding(
                            config_row, audit_run_id, audited_at, date_column, finding
                        )
                    )

        # 4e. QUALITY (duplicate PKs + null-rate) -- one probe query per table.
        #     A probe failure is logged and skipped: these checks are preventive
        #     and must never turn a healthy table into an ERROR row.
        if settings["quality_checks_enabled"]:
            try:
                for finding in run_quality_checks(client, config_row, today):
                    results.append(
                        _row_from_finding(
                            config_row, audit_run_id, audited_at, date_column, finding
                        )
                    )
            except Exception as exc:  # noqa: BLE001 - preventive check
                logger.warning(
                    "Quality probe failed for %s.%s: %s",
                    config_row.get("dataset_name"),
                    config_row.get("table_name"),
                    exc,
                )

    # 4f. AT-RISK FRESHNESS TREND: tables whose days_behind is climbing toward
    #     the SLA, read from the sibling freshness audit's results history.
    if settings["freshness_trend_enabled"]:
        history = fetch_days_behind_history(
            client, settings["freshness_dataset"], settings["freshness_table"]
        )
        target_by_key = {(t["dataset_name"], t["table_name"]): t for t in targets}
        for key, finding in detect_at_risk(history, today).items():
            base = target_by_key.get(
                key,
                {
                    "dataset_name": key[0],
                    "table_name": key[1],
                    "email_recipients": settings["recipients"],
                    "send_email_notification": True,
                },
            )
            results.append(
                _row_from_finding(
                    base, audit_run_id, audited_at, base.get("date_column"), finding
                )
            )

    # 5. Aggregate run-level counts. A "table" count is the number of audited
    #    targets; anomaly/active/historical/errored count individual result rows.
    total_tables = len(targets)
    anomalies = 0
    active = 0
    historical = 0
    errored = 0
    for row in results:
        status = row.get("audit_status")
        if status == STATUS_ERROR:
            errored += 1
        elif status == STATUS_ANOMALY:
            anomalies += 1
            if row.get("scope") == SCOPE_ACTIVE:
                active += 1
            elif row.get("scope") == SCOPE_HISTORICAL:
                historical += 1

    # 6. Compute the new-historical diff and the ACTIVE run-over-run delta vs
    #    the pre-write snapshots.
    new_historical = _count_new_historical(results, prior_snapshot)
    active_delta = _compute_active_delta(results, prior_active)
    if active_delta["new_active"] or active_delta["resolved_active"]:
        logger.info(
            "Delta vs prior run: %d new active, %d resolved",
            len(active_delta["new_active"]),
            len(active_delta["resolved_active"]),
        )

    logger.info(
        "Anomaly audit run %s complete: %d table(s), %d anomaly row(s) "
        "(%d active, %d historical), %d error(s), %d new historical",
        audit_run_id,
        total_tables,
        anomalies,
        active,
        historical,
        errored,
        new_historical,
    )

    # 7. Write the schema-only subset of every result row.
    written = 0
    if results:
        schema_rows = [_strip_helper_keys(r) for r in results]
        written = write_results(
            client,
            settings["results_dataset"],
            settings["results_table"],
            schema_rows,
        )
        logger.info(
            "Wrote %d result row(s) to %s.%s",
            written,
            settings["results_dataset"],
            settings["results_table"],
        )

    # 8. Send the ACTIVE-only summary email + the N-new-historical note when the
    #    email channel is enabled. The email_sender owns the "should we send"
    #    decision (an ACTIVE finding, or new_historical > 0, or always_send).
    email_sent = False
    notifications = config.get("notifications", {}) or {}
    email_config = (notifications.get("channels", {}) or {}).get("email", {}) or {}
    notifications_enabled = notifications.get("enabled", True)
    email_enabled = email_config.get("enabled", True)

    # ALL anomaly findings (ACTIVE + HISTORICAL) for the email body -- the email
    # now renders the full table plus a console link. ERROR/OK rows are not
    # findings. The send-decision and per-row recipients still key off ACTIVE
    # inside send_anomaly_summary.
    anomaly_findings = [r for r in results if r.get("audit_status") == STATUS_ANOMALY]

    # Mutual watchdog (computed regardless of email): warn when the SIBLING
    # (freshness) audit went silent.
    watchdog_note = check_sibling_run(
        client, settings["freshness_dataset"], "freshness audit"
    )
    if watchdog_note:
        logger.warning("%s", watchdog_note)

    # Pipeline-job context for the sources that have ACTIVE findings (best-effort;
    # {} when the orchestrator monitoring tables do not exist in this project).
    dataset_to_source = {t["dataset_name"]: t.get("source_system") for t in targets}
    affected_sources = sorted(
        {
            dataset_to_source.get(r.get("dataset_name"))
            for r in anomaly_findings
            if r.get("scope") == SCOPE_ACTIVE
            and dataset_to_source.get(r.get("dataset_name"))
        }
    )
    job_context = fetch_job_context(client, affected_sources)

    if notifications_enabled and email_enabled:
        run_meta = {
            "audit_run_id": audit_run_id,
            "audited_at": audited_at,
            "always_send": bool(notifications.get("on_success", False)),
            # For the console link in the email.
            "results_dataset": settings["results_dataset"],
            "results_table": settings["results_table"],
            # Run-over-run ACTIVE delta (new/resolved) rendered above the table.
            "delta": active_delta,
            "watchdog_note": watchdog_note,
            "job_context": job_context,
        }
        email_sent = email_sender.send_anomaly_summary(
            anomaly_findings,
            run_meta,
            email_config,
            new_historical,
            additional_recipients=additional_recipients,
        )
    else:
        logger.info("Email notifications disabled -- skipping anomaly summary email")

    # 9. Return the summary. Expose the schema-only view of the result rows.
    # Tables still warming up (insufficient settled history for the volume band),
    # surfaced in the summary per design decision 7 so the state is observable even
    # when the table also produced recency/structural findings.
    cold_start_tables = sorted(
        {
            "{0}.{1}".format(r.get("dataset_name"), r.get("table_name"))
            for r in results
            if r.get("_cold_start") or r.get("error_message") == "baseline warming up"
        }
    )

    cold_start_note = (
        "{0} table(s) baseline warming up".format(len(cold_start_tables))
        if cold_start_tables
        else ""
    )

    # Record one audit_runs metadata row (audit-the-auditor). Best-effort.
    finished_at = datetime.now(timezone.utc)
    write_run_row(
        client,
        settings["results_dataset"],
        {
            "run_id": audit_run_id,
            "source": "anomaly",
            "started_at": audited_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round((finished_at - audited_at).total_seconds(), 3),
            "tables_audited": total_tables,
            "tables_skipped": None,
            "passed": None,
            "warning": None,
            "failed": None,
            "errored": errored,
            "anomalies": anomalies,
            "active": active,
            "historical": historical,
            "new_failures": len(active_delta["new_active"]),
            "recovered": len(active_delta["resolved_active"]),
            "new_historical": new_historical,
            "cold_start_count": len(cold_start_tables),
            "skipped_tables": None,
            "cold_start_tables": json.dumps(cold_start_tables),
            "email_sent": email_sent,
            "error_message": None,
        },
    )

    return {
        "audit_run_id": audit_run_id,
        "total_tables": total_tables,
        "anomalies": anomalies,
        "active": active,
        "historical": historical,
        "errored": errored,
        "written": written,
        "new_historical": new_historical,
        "email_sent": email_sent,
        "cold_start_tables": cold_start_tables,
        "cold_start_note": cold_start_note,
        "new_active": active_delta["new_active"],
        "resolved_active": active_delta["resolved_active"],
        "watchdog_note": watchdog_note,
        "job_context": job_context,
        "results": [_strip_helper_keys(r) for r in results],
    }
