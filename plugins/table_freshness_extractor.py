#!/usr/bin/env python3
"""
Table Freshness Extractor - Plugin entrypoint for the freshness audit source
============================================================================

This plugin is discovered by orchestrate.py get_extractor_function('table_freshness')
which does importlib.import_module('plugins.table_freshness_extractor') and calls the
exposed run_pipeline(...). Unlike a normal ETL extractor, it does NOT pull rows from an
external source; instead it audits the freshness of a configured set of BigQuery tables:

  1. Derives the per-table audit list from configs/audit_sources.yaml plus each
     source's configs/<source>.yaml via shared.freshness_audit.yaml_target_loader.
     There is no BigQuery config table.
  2. For each derived target, validates the table then runs a single freshness probe
     (MAX(DATE(date_column)), days_behind, total/recent row counts).
  3. Classifies each table PASS / WARNING / FAIL / ERROR.
  4. Writes one result row per table to table_freshness_results.
  5. Optionally emails a plain-text summary.

All orchestration lives in shared.freshness_audit.audit_runner; this module is a thin
adapter that obtains a BigQuery client, derives an audit_run_id, invokes the runner, and
maps its summary dict into the stats shape the orchestrator expects.

ASCII-only logging is mandatory (no em-dashes, no unicode) -- prod logs/emails mojibake
on non-ASCII characters.

Author: Data Pipeline Team
Created: 2026-06-01
"""

import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

# The audit runner is the orchestration heart; it owns config load, validation,
# probing, status classification, result writing, and email summary.
from shared.freshness_audit import audit_runner
from shared.freshness_audit.config_loader import FreshnessConfigError
from shared.audit_log_filter import suppress_config_deprecations

logger = logging.getLogger(__name__)


def _generate_audit_run_id(execution_id: Optional[str]) -> str:
    """
    Derive the audit_run_id that ties all result rows of a single run together.

    If the orchestrator passed an execution_id, reuse it so freshness results join
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
    Audit the freshness of every active table across every active source.

    The audit target list is derived at runtime from configs/audit_sources.yaml
    plus each source's configs/<source>.yaml via
    shared.freshness_audit.yaml_target_loader.build_all_targets.

    Args:
        config: Loaded table_freshness config (audit.* block + notifications block).
        sites: Unused by this plugin (kept for the standard plugin signature).
        tables: Unused -- the table list is built from YAML, not the CLI.
        group: Unused.
        refresh_mode/lookback_days/start_date/end_date: Unused (audit is point-in-time).
        test_mode: When True, email sending is forced off so test runs do not spam.
        batch_size/max_retries/skip_validation/export_*: Unused (kept for signature).
        bq_client: Optional pre-built BigQuery client; one is created if not supplied.
        execution_id: If provided, used directly as the audit_run_id.
        schema_prefix/schema_suffix: Unused.
        **kwargs: Absorbed for forward-compatibility with the orchestrator.

    Returns:
        Stats dict with keys: status, audit_run_id, tables_audited, pass_count,
        warning_count, fail_count, error_count, duration_seconds. On a top-level
        failure (unreadable config table or unexpected error), returns
        status='failure' plus an 'error' message rather than raising, matching the
        plugin convention so a single bad source does not crash the orchestrator.
    """
    start_perf = time.monotonic()
    audit_run_id = _generate_audit_run_id(execution_id)

    logger.info("Starting table freshness audit -- audit_run_id: %s", audit_run_id)
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

        # Delegate the full audit to the runner. FreshnessConfigError (the config
        # table itself is unreadable) is a run-level fatal handled below; per-table
        # failures are captured inside the runner as ERROR results.
        #
        # suppress_config_deprecations quiets the v1->v2 YAML deprecation
        # WARNINGs emitted by shared.config_loader / shared.config_schema while
        # the runner walks every active source. The audit itself doesn't act on
        # those warnings, and they drown the real audit output. Source YAMLs
        # should migrate to v2 keys separately; this is a scoped, non-load-
        # bearing convenience for the operator running the audit.
        with suppress_config_deprecations():
            summary = audit_runner.run_audit(client, run_config, audit_run_id)

        duration_seconds = round(time.monotonic() - start_perf, 3)

        # Map the runner summary onto the plugin stats shape. The runner returns
        # passed/warning/failed/errored counts plus total/written/email_sent.
        stats: Dict[str, Any] = {
            "status": "success",
            "audit_run_id": summary.get("audit_run_id", audit_run_id),
            "tables_audited": summary.get("total", 0),
            "pass_count": summary.get("passed", 0),
            "warning_count": summary.get("warning", 0),
            "fail_count": summary.get("failed", 0),
            "error_count": summary.get("errored", 0),
            "duration_seconds": duration_seconds,
            # Extra context useful to the orchestrator / monitoring tables.
            "records_extracted": summary.get("total", 0),
            "records_loaded": summary.get("written", 0),
            "email_sent": summary.get("email_sent", False),
        }

        logger.info(
            "Freshness audit complete -- audited: %s, pass: %s, warning: %s, "
            "fail: %s, error: %s, written: %s, duration_s: %s",
            stats["tables_audited"],
            stats["pass_count"],
            stats["warning_count"],
            stats["fail_count"],
            stats["error_count"],
            stats["records_loaded"],
            duration_seconds,
        )
        return stats

    except FreshnessConfigError as exc:
        # The config table could not be read -- a run-level fatal. Do not raise;
        # return failure stats so the orchestrator records it cleanly.
        duration_seconds = round(time.monotonic() - start_perf, 3)
        logger.error("Freshness audit aborted -- config table unreadable: %s", exc)
        return {
            "status": "failure",
            "audit_run_id": audit_run_id,
            "tables_audited": 0,
            "pass_count": 0,
            "warning_count": 0,
            "fail_count": 0,
            "error_count": 0,
            "duration_seconds": duration_seconds,
            "error": str(exc),
        }

    except (
        Exception
    ) as exc:  # noqa: BLE001 - top-level guard, must not crash orchestrator
        duration_seconds = round(time.monotonic() - start_perf, 3)
        logger.error("Freshness audit failed with unexpected error: %s", exc)
        return {
            "status": "failure",
            "audit_run_id": audit_run_id,
            "tables_audited": 0,
            "pass_count": 0,
            "warning_count": 0,
            "fail_count": 0,
            "error_count": 0,
            "duration_seconds": duration_seconds,
            "error": str(exc),
        }
