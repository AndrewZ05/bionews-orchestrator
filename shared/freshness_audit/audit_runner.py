#!/usr/bin/env python3
"""Orchestration heart of the Table Freshness Auditing feature.

``run_audit`` ties the freshness_audit subpackage together:

  1. resolve the ``audit.*`` settings from the loaded config;
  2. build the per-table audit target list via ``yaml_target_loader``
     (registry: configs/audit_sources.yaml; per-source YAML supplies thresholds
     and per-resource overrides);
  3. ensure the results dataset + table exist;
  4. for EACH target -- inside a per-table try/except that NEVER aborts the
     run -- validate the target, probe its freshness, determine status, and build
     a result dict (validation failure / missing table / empty table / probe
     error all become an ERROR result with an ASCII error_message);
  5. write the schema-only subset of every result to ``table_freshness_results``;
  6. send a summary email when notifications are enabled and there is something
     to say;
  7. return a summary dict with run-level counts and the result rows.

Resilience contract: only an unreadable registry / AuditConfigError (a malformed
per-resource audit: block, e.g. missing reason or unknown cadence) is a run-level
fatal that propagates. Every per-table problem -- INCLUDING a missing table in
BigQuery -- is captured as an ERROR result so one bad table cannot abort the
run AND coverage gaps are visible in the result table.

Retry contract: transient per-table BQ query failures are retried with
exponential backoff (default attempts=3, backoff_seconds=[5, 15, 45]) using
``time.sleep``. The retry loop lives in ONE place -- ``_audit_one_table`` here --
which guards the validate+probe sequence; ``calculate_freshness`` is invoked with
``retry_attempts=1`` so the budget is not multiplied across two layers.

ASCII-ONLY RULE: every log string in this module is pure ASCII. INFO for
run-level milestones, WARNING for FAIL/ERROR tables, DEBUG for per-table PASS.
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.audit_views import ensure_freshness_views
from shared.audit_watchdog import check_sibling_run
from shared.freshness_audit import _validate_identifier
from shared.freshness_audit.yaml_target_loader import build_all_targets
from shared.freshness_audit.config_loader import FreshnessConfigError
from shared.freshness_audit.table_validator import validate_target
from shared.freshness_audit.freshness_calculator import (
    calculate_freshness,
    FreshnessProbeError,
)
from shared.freshness_audit.status_rules import (
    determine_status,
    STATUS_PASS,
    STATUS_WARNING,
    STATUS_FAIL,
    STATUS_ERROR,
    STATUS_STATIC,
)
from shared.freshness_audit.result_writer import (
    ensure_results_dataset,
    ensure_results_table,
    write_results,
    write_run_row,
)
from shared.freshness_audit.email_sender import send_summary_email

logger = logging.getLogger(__name__)

# Default retry policy if the config omits audit.retry. Matches the spec.
_DEFAULT_RETRY_ATTEMPTS = 3
_DEFAULT_BACKOFF_SECONDS = [5, 15, 45]

# The exact 11 columns of the table_freshness_results schema. Helper keys
# (email_recipients / send_email_notification) carried on the result dict for
# the email step are dropped before writing.
_RESULTS_SCHEMA_COLUMNS = (
    "audit_id",
    "audit_run_id",
    "dataset_name",
    "table_name",
    "most_recent_date",
    "days_behind",
    "total_row_count",
    "recent_date_row_count",
    "audit_status",
    "error_message",
    "audited_at",
    "date_column_used",
    "freshness_threshold_days",
    "warning_threshold_days",
    "audit_cadence",
    "audit_reason",
)


def _resolve_audit_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    """Pull the audit.* block out of config, applying defaults where omitted."""
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
    # (shared with the anomaly audit). The legacy per-audit ``audit.sources:``
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

    return {
        "sources": list(sources),
        "recipients": recipients,
        "results_dataset": audit.get("results_dataset"),
        "results_table": audit.get("results_table", "table_freshness_results"),
        # Where the anomaly audit writes its results -- referenced by the
        # cross-audit v_table_health view this runner maintains.
        "anomaly_dataset": audit.get("anomaly_dataset", "anomaly_audit"),
        "retry_attempts": attempts,
        "backoff_seconds": list(backoff),
        "location": (config.get("bigquery", {}) or {}).get("location", "US"),
    }


def _build_result_row(
    config_row: Dict[str, Any],
    audit_run_id: str,
    audited_at,
    *,
    most_recent_date=None,
    days_behind=None,
    total_row_count=None,
    recent_date_row_count=None,
    audit_status,
    error_message=None,
    date_column_used=None,
) -> Dict[str, Any]:
    """Build a single result dict for one audited table.

    The returned dict carries the results-schema columns PLUS two transient
    helper keys (``email_recipients`` and ``send_email_notification``) copied
    from the config row for the email step. The helper keys are stripped by
    ``_strip_helper_keys`` before the row is written to BigQuery.

    ``date_column_used`` is the column the probe actually measured (it may have
    fallen back past the configured column); when no probe ran (validation
    failure / skip path) it defaults to the CONFIGURED column so every row still
    says what would have been measured. The thresholds are stamped from the
    config row so each row records the SLA it was judged against.
    """
    # Serialize the BQ DATE / TIMESTAMP values to ISO strings here so the dict
    # handed to result_writer.write_results -> client.insert_rows_json is
    # JSON-serializable (the stdlib JSON encoder cannot handle datetime.date /
    # datetime.datetime). This matches write_results' documented contract:
    # most_recent_date as 'YYYY-MM-DD' or None, audited_at as an ISO-8601 string.
    # days_behind and the row counts stay int|None (already JSON-safe).
    most_recent_date_iso = (
        most_recent_date.isoformat() if most_recent_date is not None else None
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
        "most_recent_date": most_recent_date_iso,
        "days_behind": days_behind,
        "total_row_count": total_row_count,
        "recent_date_row_count": recent_date_row_count,
        "audit_status": audit_status,
        "error_message": error_message,
        "audited_at": audited_at_iso,
        "date_column_used": date_column_used or config_row.get("date_column"),
        "freshness_threshold_days": config_row.get("freshness_threshold_days"),
        "warning_threshold_days": config_row.get("warning_threshold_days"),
        # Per-resource audit policy stamped by yaml_target_loader. Both NULL on
        # rows where the resource has no audit: override (the common case --
        # source-level defaults apply), so the column is self-explanatory in BQ.
        "audit_cadence": config_row.get("audit_cadence"),
        "audit_reason": config_row.get("audit_reason"),
        # --- transient helper keys (stripped before write) ---
        "email_recipients": config_row.get("email_recipients"),
        "send_email_notification": config_row.get("send_email_notification"),
    }


def _strip_helper_keys(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``result`` containing ONLY the 11 results-schema columns.

    Drops the transient ``email_recipients`` / ``send_email_notification`` helper
    keys so the dict matches the table_freshness_results schema exactly for
    streaming inserts.
    """
    return {col: result.get(col) for col in _RESULTS_SCHEMA_COLUMNS}


def _audit_one_table(
    client,
    config_row: Dict[str, Any],
    audit_run_id: str,
    audited_at,
    retry_attempts: int,
    backoff_seconds: List[int],
) -> Dict[str, Any]:
    """Evaluate a single configured table with retry, returning a result dict.

    Never raises for the expected failure modes (missing table, missing
    column, empty table, transient or permanent query error): all become an
    ERROR result with an ASCII error_message. Transient failures are retried
    with exponential backoff; the final failure is recorded as ERROR.

    Missing tables ARE recorded as ERROR rows (not silently skipped). The
    review on 2026-06-16 caught the prior SKIP behavior as a coverage-gap
    risk: an operator reading "audit run complete" had no way to tell that a
    resource declared active in YAML was silently absent from BigQuery. To
    suppress a known-absent table, set active: false in the source YAML.
    """
    table_label = f"{config_row.get('dataset_name')}.{config_row.get('table_name')}"
    last_error: Optional[str] = None

    # One "attempt" covers the full validate + probe sequence so a transient
    # failure anywhere in evaluating this table gets the configured retries.
    for attempt in range(retry_attempts):
        try:
            # 1. Validate the target exists and the date_column is usable. This
            #    converts not-found / missing-column into (False, reason) rather
            #    than raising, so those are terminal (no point retrying).
            ok, reason = validate_target(client, config_row)
            if not ok:
                # Strict-by-default: every validation failure -- including
                # missing table / missing dataset (config drift) -- becomes an
                # ERROR row with the validator's message. The reason string is
                # already self-documenting ("table not found in BigQuery: ...").
                # To suppress a known-absent table set active: false in the
                # source YAML; that way the suppression is operator intent.
                logger.warning("Validation failed for %s: %s", table_label, reason)
                return _build_result_row(
                    config_row,
                    audit_run_id,
                    audited_at,
                    audit_status=STATUS_ERROR,
                    error_message=reason or "validation failed",
                )

            # 2. Probe freshness. This OUTER loop owns the retry budget, so the
            #    probe runs exactly once per attempt (retry_attempts=1) to avoid
            #    multiplicative amplification (outer x inner). FreshnessProbeError
            #    surfaces here and is retried with backoff by this loop.
            probe = calculate_freshness(
                client,
                config_row,
                retry_attempts=1,
                backoff_seconds=backoff_seconds,
            )

            # 3. No usable freshness signal -> ERROR. Distinguish a truly EMPTY
            #    table (0 rows) from a POPULATED table whose every candidate date
            #    column (incl. extracted_at) was NULL -- the latter is not "empty",
            #    it just has no date to evaluate, which is a different problem.
            if probe.most_recent_date is None or probe.days_behind is None:
                if probe.total_row_count == 0:
                    msg = "table is empty -- no rows to evaluate"
                else:
                    msg = (
                        "populated ({0} rows) but no non-NULL date in any candidate "
                        "column -- no freshness signal".format(probe.total_row_count)
                    )
                return _build_result_row(
                    config_row,
                    audit_run_id,
                    audited_at,
                    total_row_count=probe.total_row_count,
                    recent_date_row_count=probe.recent_date_row_count,
                    audit_status=STATUS_ERROR,
                    error_message=msg,
                )

            # 4. Normal path: PASS / WARNING / FAIL / STATIC from the thresholds.
            #    is_static makes a within-threshold result report as STATUS_STATIC
            #    (intentionally-unchanging reference data) while a static table past
            #    even its large threshold still FAILs.
            warning_threshold = int(config_row.get("warning_threshold_days") or 0)
            freshness_threshold = int(config_row.get("freshness_threshold_days") or 0)
            is_static = bool(config_row.get("is_static"))
            status = determine_status(
                probe.days_behind,
                warning_threshold,
                freshness_threshold,
                is_static=is_static,
            )

            # Tables without a genuine data-date cursor are audited on extracted_at
            # (the ETL load time), which is a weaker freshness signal. Cap their
            # worst status at WARNING so a stale LOAD is visible but does not read
            # as a hard data-staleness FAIL.
            #
            # EXCEPTION: tables explicitly tagged audit.cadence: manual or
            # audit.cadence: snapshot are FAIL-eligible even without a real
            # date cursor. The operator declared a long-cadence intent (365-day
            # backstop) and the whole point of the backstop is "if it has been
            # silent for a year, FAIL." Otherwise a hand-curated table that
            # quietly stopped getting touched would forever read WARNING.
            cadence = config_row.get("audit_cadence")
            cap_allows_fail = cadence in ("manual", "snapshot", "static")
            if (
                not config_row.get("has_date_cursor", True)
                and status == STATUS_FAIL
                and not cap_allows_fail
            ):
                logger.debug(
                    "Capping %s at WARNING (no data-date cursor; audited on %s)",
                    table_label,
                    config_row.get("date_column"),
                )
                status = STATUS_WARNING

            # Log severity tracks actual status so an operator can grep
            # for the real signal: FAIL is logger.error (urgent), WARNING is
            # logger.warning (degraded but tolerable), PASS stays debug.
            if status == STATUS_PASS:
                logger.debug(
                    "PASS %s: most_recent_date=%s days_behind=%s",
                    table_label,
                    probe.most_recent_date,
                    probe.days_behind,
                )
            elif status == STATUS_FAIL:
                logger.error(
                    "FAIL %s: most_recent_date=%s days_behind=%s",
                    table_label,
                    probe.most_recent_date,
                    probe.days_behind,
                )
            else:
                logger.warning(
                    "%s %s: most_recent_date=%s days_behind=%s",
                    status,
                    table_label,
                    probe.most_recent_date,
                    probe.days_behind,
                )

            return _build_result_row(
                config_row,
                audit_run_id,
                audited_at,
                most_recent_date=probe.most_recent_date,
                days_behind=probe.days_behind,
                total_row_count=probe.total_row_count,
                recent_date_row_count=probe.recent_date_row_count,
                audit_status=status,
                error_message=None,
                date_column_used=getattr(probe, "date_column_used", None),
            )

        except FreshnessProbeError as exc:
            # Probe query failed (possibly transient). Retry with backoff.
            last_error = f"freshness probe error: {exc}"
            if attempt < retry_attempts - 1:
                # backoff_seconds may be shorter than the attempt count; clamp.
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
            logger.error(
                "Probe failed for %s after %d attempts: %s",
                table_label,
                retry_attempts,
                exc,
            )
        except Exception as exc:  # noqa: BLE001 - per-table resilience boundary
            # Any other unexpected per-table error is captured as ERROR; we do
            # not retry unknown errors (they are unlikely to be transient).
            last_error = f"unexpected error: {exc}"
            logger.error("Unexpected error auditing %s: %s", table_label, exc)
            break

    # All retries exhausted (or a non-retried error) -- record an ERROR result.
    return _build_result_row(
        config_row,
        audit_run_id,
        audited_at,
        audit_status=STATUS_ERROR,
        error_message=last_error or "table could not be evaluated",
    )


def _snapshot_prior_statuses(client, results_dataset: str, results_table: str) -> dict:
    """Snapshot each table's status from its most recent PRIOR result row.

    Runs BEFORE this run writes, so "latest" in the results table is the previous
    run. Returns {(dataset_name, table_name): audit_status}. Modeled on the
    anomaly audit's prior-run snapshot: best-effort -- any failure (e.g. the
    table was just created and is empty) returns {} so the delta degrades to
    "no prior baseline" rather than failing the run.
    """
    safe_dataset = _validate_identifier(results_dataset, "dataset")
    safe_table = _validate_identifier(results_table, "table")
    sql = (
        "-- Snapshot the latest PRIOR status per table for the run-over-run delta.\n"
        "-- Identifiers are interpolated after validation; no bound values needed.\n"
        "SELECT dataset_name, table_name, audit_status\n"
        "FROM `{0}`.`{1}`\n"
        "QUALIFY ROW_NUMBER() OVER (\n"
        "  PARTITION BY dataset_name, table_name ORDER BY audited_at DESC\n"
        ") = 1".format(safe_dataset, safe_table)
    )
    try:
        rows = list(client.query(sql).result())
        snapshot = {
            (r["dataset_name"], r["table_name"]): r["audit_status"] for r in rows
        }
        logger.info(
            "Snapshotted prior status for %d table(s) for the run-over-run delta",
            len(snapshot),
        )
        return snapshot
    except Exception as exc:  # noqa: BLE001 - snapshot is best-effort
        logger.warning(
            "Could not snapshot prior statuses (delta disabled this run): %s", exc
        )
        return {}


# Status groupings for the delta classification. WARNING counts as "good" for
# newly_failed purposes (a WARNING->FAIL transition IS news) and as "bad" for
# recovered purposes only when it was FAIL/ERROR before (PASS is the recovery bar).
_BAD_STATUSES = (STATUS_FAIL, STATUS_ERROR)
# STATIC is a healthy outcome (intentionally-unchanging data within threshold),
# so it joins PASS/WARNING as "good" for delta/recovery: a STATIC table is not a
# new failure, and FAIL/ERROR -> STATIC counts as recovered.
_GOOD_STATUSES = (STATUS_PASS, STATUS_WARNING, STATUS_STATIC)


def _compute_delta(results: List[Dict[str, Any]], prior: dict) -> dict:
    """Classify run-over-run changes per table.

    Returns {"newly_failed": [labels], "recovered": [labels]} where a label is
    "dataset.table". newly_failed = now FAIL/ERROR but previously PASS/WARNING;
    recovered = now PASS but previously FAIL/ERROR. Tables with no prior row
    (first audit) are neither -- there is no baseline to have changed from.
    """
    newly_failed: List[str] = []
    recovered: List[str] = []
    for row in results:
        key = (row.get("dataset_name"), row.get("table_name"))
        prior_status = prior.get(key)
        if prior_status is None:
            continue
        now = row.get("audit_status")
        label = "{0}.{1}".format(key[0], key[1])
        if now in _BAD_STATUSES and prior_status in _GOOD_STATUSES:
            newly_failed.append(label)
        elif now == STATUS_PASS and prior_status in _BAD_STATUSES:
            recovered.append(label)
    return {"newly_failed": sorted(newly_failed), "recovered": sorted(recovered)}


def run_audit(
    client,
    config: Dict[str, Any],
    audit_run_id: str,
    additional_recipients: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run a full freshness audit and return a summary dict.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        config: the loaded table_freshness config (see configs/table_freshness.yaml).
        audit_run_id: caller-supplied id tying all result rows of this run together.
        additional_recipients: extra email addresses to always include on the
            summary email (additive to per-config recipients).

    Returns:
        {
          audit_run_id, total, passed, warning, failed, errored, written,
          email_sent, results: [<schema dicts>...]
        }

    Raises:
        FreshnessConfigError: if the config table cannot be read (run-level fatal).
    """
    settings = _resolve_audit_settings(config)

    # Single run-level timestamp so every row of this run shares an audited_at.
    audited_at = datetime.now(timezone.utc)

    logger.info(
        "Starting freshness audit run %s (sources=%s, results=%s.%s)",
        audit_run_id,
        ",".join(settings["sources"]) or "(none)",
        settings["results_dataset"],
        settings["results_table"],
    )

    # 1. Build the per-table audit list by reading each source's YAML config
    #    (production "_data" tables + their incremental date columns). This is the
    #    source of truth -- there is no BigQuery config table. A source that fails
    #    to load is skipped inside build_all_targets and never aborts the run.
    config_rows = build_all_targets(
        settings["sources"], client.project, recipients=settings["recipients"]
    )
    logger.info("Built %d freshness audit target(s)", len(config_rows))

    # 2. Ensure the results sink exists before we start writing, then refresh
    #    the convenience views (latest-per-table + cross-audit table health).
    #    View creation is best-effort inside ensure_freshness_views.
    ensure_results_dataset(client, settings["results_dataset"], settings["location"])
    ensure_results_table(client, settings["results_dataset"], settings["results_table"])
    ensure_freshness_views(
        client,
        settings["results_dataset"],
        settings["anomaly_dataset"],
        settings["results_table"],
    )

    # 3. Audit each table. The per-table helper never raises for expected errors.
    #    None means SKIPPED (table absent in BigQuery -- config drift): no row is
    #    written; the skip is surfaced in the summary and as an email note.
    results: List[Dict[str, Any]] = []
    skipped_tables: List[str] = []
    for config_row in config_rows:
        result = _audit_one_table(
            client,
            config_row,
            audit_run_id,
            audited_at,
            settings["retry_attempts"],
            settings["backoff_seconds"],
        )
        if result is None:
            skipped_tables.append(
                "{0}.{1}".format(
                    config_row.get("dataset_name"), config_row.get("table_name")
                )
            )
            continue
        results.append(result)

    if skipped_tables:
        logger.info(
            "Skipped %d known-missing table(s) (config drift) -- see "
            "docs/ANOMALY_AUDIT_MISSING_TABLES.md",
            len(skipped_tables),
        )

    # 4. Aggregate run-level counts.
    counts = {
        STATUS_PASS: 0,
        STATUS_WARNING: 0,
        STATUS_FAIL: 0,
        STATUS_ERROR: 0,
        STATUS_STATIC: 0,
    }
    for row in results:
        status = row.get("audit_status")
        if status in counts:
            counts[status] += 1

    # Run-summary log severity mirrors the worst outcome so the line surfaces
    # under `grep WARNING` / `grep ERROR` alongside the per-row findings -- the
    # full summary is in the email but the CLI operator gets one greppable line
    # without having to read 200 lines of per-table output to find it.
    summary_msg = (
        "Audit run %s complete: %d PASS, %d WARNING, %d FAIL, %d ERROR, "
        "%d STATIC (total %d)"
    )
    summary_args = (
        audit_run_id,
        counts[STATUS_PASS],
        counts[STATUS_WARNING],
        counts[STATUS_FAIL],
        counts[STATUS_ERROR],
        counts[STATUS_STATIC],
        len(results),
    )
    if counts[STATUS_FAIL] or counts[STATUS_ERROR]:
        logger.error(summary_msg, *summary_args)
    elif counts[STATUS_WARNING]:
        logger.warning(summary_msg, *summary_args)
    else:
        logger.info(summary_msg, *summary_args)

    # 5a. Snapshot the PRIOR per-table statuses BEFORE writing this run, then
    #     classify what changed (newly failed / recovered) for the email + summary.
    prior_statuses = _snapshot_prior_statuses(
        client, settings["results_dataset"], settings["results_table"]
    )
    delta = _compute_delta(results, prior_statuses)
    if delta["newly_failed"] or delta["recovered"]:
        logger.info(
            "Delta vs prior run: %d newly failed, %d recovered",
            len(delta["newly_failed"]),
            len(delta["recovered"]),
        )

    # 5b. Write the schema-only subset of every result row.
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

    # 6a. Mutual watchdog: warn when the SIBLING (anomaly) audit went silent.
    #     Computed regardless of email so the summary always carries it.
    watchdog_note = check_sibling_run(
        client, settings["anomaly_dataset"], "anomaly audit"
    )
    if watchdog_note:
        logger.warning("%s", watchdog_note)

    # 6b. Send the summary email when notifications are enabled and there is a
    #    configured, enabled email channel. The email_sender owns the actual
    #    "should we send" decision (actionable rows or always_send).
    email_sent = False
    notifications = config.get("notifications", {}) or {}
    email_config = (notifications.get("channels", {}) or {}).get("email", {}) or {}
    notifications_enabled = notifications.get("enabled", True)
    email_enabled = email_config.get("enabled", True)

    if notifications_enabled and email_enabled:
        run_meta = {
            "audit_run_id": audit_run_id,
            "audited_at": audited_at,
            # always_send is False by default; the extractor/test_mode can pass a
            # config-level override, but actionable rows still trigger a send.
            "always_send": bool(notifications.get("on_success", False)),
            # Surfaced as a one-line note in the email bodies.
            "skipped_count": len(skipped_tables),
            # Run-over-run change lists rendered as a "Changes since last run"
            # section and reflected in the subject counts.
            "delta": delta,
            # For the console link in the email.
            "results_dataset": settings["results_dataset"],
            "results_table": settings["results_table"],
            "watchdog_note": watchdog_note,
        }
        email_sent = send_summary_email(
            results,
            run_meta,
            email_config,
            additional_recipients=additional_recipients,
        )
    else:
        logger.info("Email notifications disabled -- skipping audit summary email")

    # 7. Record one audit_runs metadata row (audit-the-auditor: duration, counts,
    #    skips, email status). write_run_row is best-effort and never raises.
    finished_at = datetime.now(timezone.utc)
    write_run_row(
        client,
        settings["results_dataset"],
        {
            "run_id": audit_run_id,
            "source": "freshness",
            "started_at": audited_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round((finished_at - audited_at).total_seconds(), 3),
            "tables_audited": len(results),
            "tables_skipped": len(skipped_tables),
            "passed": counts[STATUS_PASS],
            "warning": counts[STATUS_WARNING],
            "failed": counts[STATUS_FAIL],
            "errored": counts[STATUS_ERROR],
            "anomalies": None,
            "active": None,
            "historical": None,
            "new_failures": len(delta["newly_failed"]),
            "recovered": len(delta["recovered"]),
            "new_historical": None,
            "cold_start_count": None,
            "skipped_tables": json.dumps(skipped_tables),
            "cold_start_tables": None,
            "email_sent": email_sent,
            "error_message": None,
        },
    )

    # 8. Return the summary. results carry only the schema columns plus helper
    #    keys; expose the schema-only view to keep the contract clean.
    return {
        "audit_run_id": audit_run_id,
        "total": len(results),
        "passed": counts[STATUS_PASS],
        "warning": counts[STATUS_WARNING],
        "failed": counts[STATUS_FAIL],
        "errored": counts[STATUS_ERROR],
        "static": counts[STATUS_STATIC],
        "written": written,
        "email_sent": email_sent,
        "skipped": len(skipped_tables),
        "skipped_tables": skipped_tables,
        "newly_failed": delta["newly_failed"],
        "recovered": delta["recovered"],
        "watchdog_note": watchdog_note,
        "results": [_strip_helper_keys(r) for r in results],
    }
