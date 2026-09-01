#!/usr/bin/env python3
"""
Anomaly Audit Extractor - Plugin entrypoint for the anomaly_audit source
========================================================================

This plugin is discovered by orchestrate.py get_extractor_function('anomaly_audit')
which does importlib.import_module('plugins.anomaly_audit_extractor') and calls the
exposed run_pipeline(...). Like the table_freshness plugin, it does NOT pull rows
from an external source; instead it runs a second data-quality check across the
SAME configured BigQuery tables, looking for data ANOMALIES (not freshness):

  1. Resolves audit targets via the freshness YAML target loader (reused).
  2. For each target, runs three detectors -- recency-lag, volume-outlier, and
     structural (zero/collapse) -- over the resolved date column.
  3. Classifies each finding by severity and scope (ACTIVE vs HISTORICAL).
  4. Writes one result row per finding (or one OK row) to anomaly_results.
  5. Optionally emails an ACTIVE-only summary plus a new-historical-outlier note.

All orchestration lives in shared.anomaly_audit.anomaly_runner; this module is a
thin adapter that obtains a BigQuery client, derives an audit_run_id, invokes the
runner, and maps its summary dict into the stats shape the orchestrator expects.

ASCII-only logging is mandatory (no em-dashes, no unicode) -- prod logs/emails
mojibake on non-ASCII characters.

Author: Data Pipeline Team
Created: 2026-06-09
"""

import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

# The anomaly runner is the orchestration heart; it owns target resolution,
# the three detectors, cold-start gating, result writing, and email summary.
from shared.anomaly_audit import anomaly_runner
from shared.audit_log_filter import suppress_config_deprecations

logger = logging.getLogger(__name__)


def _generate_audit_run_id(execution_id: Optional[str]) -> str:
    """
    Derive the audit_run_id that ties all result rows of a single run together.

    If the orchestrator passed an execution_id, reuse it so anomaly results join
    cleanly against the orchestrator's job records. Otherwise synthesize a
    human-sortable id of the form run_YYYYMMDD_HHMMSS_<8 hex> using UTC.
    """
    if execution_id:
        return execution_id
    # datetime.utcnow() keeps the id timezone-stable (UTC) and human-sortable.
    stamp = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    return f"{stamp}_{suffix}"


def run_pipeline(
    config: Dict[str, Any],
    sites: Optional[List[str]] = None,
    tables: Optional[List[str]] = None,
    group: Optional[str] = None,
    refresh_mode: Optional[str] = None,
    lookback_days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    test_mode: bool = False,
    batch_size: Optional[int] = None,
    max_retries: int = 3,
    skip_validation: bool = False,
    export_format: Optional[str] = None,
    export_dir: Optional[str] = None,
    bq_client: Any = None,
    execution_id: Optional[str] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Run the anomaly audit across every resolved target table.

    Args:
        config: Loaded anomaly_audit config (audit.* block + notifications block).
        sites: Unused by this plugin (kept for the standard plugin signature).
        tables: Unused -- the target list is resolved from the source YAMLs.
        group: Unused.
        refresh_mode/lookback_days/start_date/end_date: Unused (audit is point-in-time).
        test_mode: When True, email sending is forced off so test runs do not spam.
        batch_size/max_retries/skip_validation/export_*: Unused (kept for signature).
        bq_client: Optional pre-built BigQuery client; one is created if not supplied.
        execution_id: If provided, used directly as the audit_run_id.
        schema_prefix/schema_suffix: Unused.
        **kwargs: Absorbed for forward-compatibility with the orchestrator.

    Returns:
        Stats dict with keys: status, audit_run_id, tables_audited, anomaly_count,
        active_count, historical_count, error_count, new_historical_count,
        anomalies_active, anomalies_historical_new, lag_count, records_extracted,
        records_loaded, email_sent, duration_seconds. On a top-level failure,
        returns status='failure' plus an 'error' message rather than raising,
        matching the plugin convention so a single bad source does not crash the
        orchestrator.
    """
    start_perf = time.monotonic()
    audit_run_id = _generate_audit_run_id(execution_id)

    logger.info("Starting anomaly audit -- audit_run_id: %s", audit_run_id)
    if test_mode:
        logger.info("test_mode enabled: email notifications will be suppressed")

    try:
        # Obtain a BigQuery client: reuse the orchestrator-supplied one if present,
        # else fall back to the thread-safe singleton (project from GCP_PROJECT_ID).
        client = bq_client
        if client is None:
            from shared.bigquery_client import get_bigquery_client

            client = get_bigquery_client()

        # test_mode must not send email. The runner reads the email channel's
        # "enabled" flag; suppress it on a shallow copy so we do not mutate the
        # caller's config dict. Only the email channel is overridden.
        run_config = config
        if test_mode:
            run_config = dict(config)
            notifications = dict(run_config.get("notifications", {}) or {})
            channels = dict(notifications.get("channels", {}) or {})
            email_channel = dict(channels.get("email", {}) or {})
            email_channel["enabled"] = False
            channels["email"] = email_channel
            notifications["channels"] = channels
            run_config["notifications"] = notifications

        # Delegate the full audit to the runner. Per-target failures are captured
        # inside the runner as ERROR result rows; only a config-unreadable
        # condition is run-fatal (surfaced as an exception and handled below).
        #
        # suppress_config_deprecations quiets the v1->v2 YAML deprecation
        # WARNINGs emitted while the runner walks every active source. See the
        # equivalent block in plugins/table_freshness_extractor.py for rationale.
        with suppress_config_deprecations():
            summary = anomaly_runner.run_anomaly_audit(
                client, run_config, audit_run_id, additional_recipients=None
            )

        duration_seconds = round(time.monotonic() - start_perf, 3)

        # Map the runner summary onto the plugin stats shape. The runner returns
        # total_tables, anomalies, active, historical, errored, written,
        # new_historical, email_sent, and the result rows.
        active_count = summary.get("active", 0)
        historical_count = summary.get("historical", 0)
        new_historical_count = summary.get("new_historical", 0)

        # lag_count: number of recency_lag findings (LAG + interior GAP) recorded
        # this run, derived from the runner's result rows so the orchestrator can
        # surface the primary-detector signal without re-querying.
        lag_count = 0
        for row in summary.get("results", []) or []:
            if row.get("detector") == "recency_lag":
                lag_count += 1

        stats: Dict[str, Any] = {
            "status": "success",
            "audit_run_id": summary.get("audit_run_id", audit_run_id),
            "tables_audited": summary.get("total_tables", 0),
            "anomaly_count": summary.get("anomalies", 0),
            "active_count": active_count,
            "historical_count": historical_count,
            "error_count": summary.get("errored", 0),
            "new_historical_count": new_historical_count,
            # Unit-requested aliases for downstream consumers.
            "anomalies_active": active_count,
            "anomalies_historical_new": new_historical_count,
            "lag_count": lag_count,
            "duration_seconds": duration_seconds,
            # Extra context useful to the orchestrator / monitoring tables.
            "records_extracted": summary.get("total_tables", 0),
            "records_loaded": summary.get("written", 0),
            "email_sent": summary.get("email_sent", False),
        }

        logger.info(
            "Anomaly audit complete -- audited: %s, anomalies: %s, active: %s, "
            "historical: %s, error: %s, new_historical: %s, written: %s, "
            "duration_s: %s",
            stats["tables_audited"],
            stats["anomaly_count"],
            stats["active_count"],
            stats["historical_count"],
            stats["error_count"],
            stats["new_historical_count"],
            stats["records_loaded"],
            duration_seconds,
        )
        return stats

    except (
        Exception
    ) as exc:  # noqa: BLE001 - top-level guard, must not crash orchestrator
        duration_seconds = round(time.monotonic() - start_perf, 3)
        logger.error("Anomaly audit failed with unexpected error: %s", exc)
        return {
            "status": "failure",
            "audit_run_id": audit_run_id,
            "tables_audited": 0,
            "anomaly_count": 0,
            "active_count": 0,
            "historical_count": 0,
            "error_count": 0,
            "new_historical_count": 0,
            "anomalies_active": 0,
            "anomalies_historical_new": 0,
            "lag_count": 0,
            "duration_seconds": duration_seconds,
            "error": str(exc),
        }
