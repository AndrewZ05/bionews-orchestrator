#!/usr/bin/env python3
# Runs the single-pass FRESHNESS_PROBE query for one validated config row.
# Honors the audit.retry block (attempts + backoff_seconds) around the probe,
# retrying only transient BigQuery errors; non-transient failures fail fast.
"""Freshness calculator.

Given an authenticated client and a validated config row, runs one query that
computes most_recent_date, days_behind, total_row_count and recent_date_row_count.
Identifiers are interpolated only after passing the shared allowlist; there are no
user-bindable values in the probe. An empty table yields a None most_recent_date /
days_behind (the runner maps that to ERROR). Query failures raise
FreshnessProbeError after the retry budget is exhausted.
"""

import logging
import time
from dataclasses import dataclass

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

# Default retry policy if the audit.retry block is absent from config.
_DEFAULT_RETRY_ATTEMPTS = 3
_DEFAULT_BACKOFF_SECONDS = [5, 15, 45]

# Column whose value (when present) is the load-time fallback used last, after all
# data-bearing date columns have been tried.
_EXTRACTED_AT = "extracted_at"

# COLUMNS_QUERY: discover the DATE/TIMESTAMP/DATETIME columns actually present on a
# table, so we can try every candidate date field (not just the configured one)
# before falling back to extracted_at. table_name is bound as a scalar parameter;
# the dataset path is interpolated from validated identifiers.
COLUMNS_QUERY = """-- COLUMNS_QUERY: list DATE/TIMESTAMP/DATETIME columns on a table so the probe
-- can try every candidate date field before falling back to extracted_at.
SELECT column_name, data_type
FROM `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = @table_name
  AND data_type IN ('DATE', 'TIMESTAMP', 'DATETIME')"""

# PROBE_HEADER / per-column / footer build a SINGLE query that returns COUNT(*) plus
# MAX(DATE(col)) for EVERY candidate column. The caller then picks the first
# candidate (in priority order) whose max is non-NULL. This distinguishes a truly
# empty table (COUNT(*)=0) from a populated table whose chosen date column is
# entirely NULL (COUNT(*)>0 but that column's max IS NULL) -- the latter must NOT
# read as "empty"; we move on to the next candidate column.
# Identifiers are validated+interpolated by the caller (BQ cannot bind names).


def build_multi_probe_sql(project_id, dataset_name, table_name, columns):
    """Build a single probe query returning COUNT(*) and MAX(DATE(col)) per column.

    ``columns`` is the ordered, already-validated candidate column list. Each
    column gets two output expressions: ``max_<i>`` (MAX(DATE(col))) so the caller
    can pick the first non-NULL in priority order. Only -- comments are used.
    """
    target = "`{0}`.`{1}`.`{2}`".format(project_id, dataset_name, table_name)
    lines = ["-- Multi-column freshness probe: COUNT(*) plus MAX(DATE) per candidate."]
    lines.append("SELECT")
    lines.append("  COUNT(*) AS total_row_count,")
    select_parts = []
    for i, col in enumerate(columns):
        # DATE(col) tolerates DATE/DATETIME/TIMESTAMP; alias is positional so we can
        # map results back to the column by index.
        select_parts.append("  MAX(DATE(`{0}`)) AS max_{1}".format(col, i))
    lines.append(",\n".join(select_parts))
    lines.append("FROM {0}".format(target))
    return "\n".join(lines)


def build_recent_count_sql(project_id, dataset_name, table_name, column):
    """Build the recent-date row-count query for the chosen column."""
    target = "`{0}`.`{1}`.`{2}`".format(project_id, dataset_name, table_name)
    return (
        "-- Recent-date row count for the chosen freshness column.\n"
        "SELECT COUNTIF(DATE(`{col}`) = (SELECT MAX(DATE(`{col}`)) FROM {t})) "
        "AS recent_date_row_count\nFROM {t}".format(col=column, t=target)
    )


# Substrings that indicate a transient BigQuery error worth retrying. Anything else
# (e.g. invalid query, syntax error, not found) is treated as non-transient and
# fails fast so we do not waste the retry budget on deterministic failures.
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


@dataclass
class FreshnessProbeResult:
    """Result of a single freshness probe.

    most_recent_date / days_behind are None ONLY when the table is genuinely empty
    (total_row_count == 0). A populated table whose configured column is all-NULL
    does NOT yield None here -- the probe tries every candidate date column and
    falls back to extracted_at, recording which column won in date_column_used.
    """

    most_recent_date: object  # datetime.date | None
    days_behind: object  # int | None
    total_row_count: int
    recent_date_row_count: int
    # The column the freshness actually came from (may differ from the configured
    # date_column if it was all-NULL and a fallback candidate was used). None when
    # the table is empty or no candidate column had any non-NULL value.
    date_column_used: object = None


class FreshnessProbeError(Exception):
    """Raised when the freshness probe query fails (runner maps to ERROR)."""


def _is_transient(exc):
    """Return True if an exception looks like a retryable transient BQ error."""
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_MARKERS)


def _run_with_retry(
    client, sql, target, retry_attempts, backoff_seconds, job_config=None
):
    """Run a query with transient-error retry/backoff; return the row list.

    Non-transient errors fail fast as FreshnessProbeError; transient errors are
    retried up to retry_attempts with the configured backoff.
    """
    last_exc = None
    for attempt in range(1, retry_attempts + 1):
        try:
            return list(client.query(sql, job_config=job_config).result())
        except Exception as exc:
            last_exc = exc
            if not _is_transient(exc):
                logger.warning(
                    "Freshness probe failed (non-transient) for %s: %s", target, exc
                )
                raise FreshnessProbeError(
                    "probe failed for {0}: {1}".format(target, exc)
                ) from exc
            if attempt < retry_attempts:
                idx = (
                    min(attempt - 1, len(backoff_seconds) - 1) if backoff_seconds else 0
                )
                delay = backoff_seconds[idx] if backoff_seconds else 0
                logger.warning(
                    "Freshness probe transient error for %s (attempt %d/%d), "
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
                    "Freshness probe exhausted retries for %s: %s", target, exc
                )
    raise FreshnessProbeError(
        "probe failed for {0} after {1} attempts: {2}".format(
            target, retry_attempts, last_exc
        )
    ) from last_exc


def _candidate_columns(config, discovered):
    """Build the ordered, de-duplicated, validated candidate date-column list.

    Priority (per the operator rule "check all possible date fields then use
    extracted_at"):
      1. the configured date_column (if set and a real date column),
      2. any other columns the source declared (config['date_column_candidates']),
      3. every other DATE/TIMESTAMP/DATETIME column discovered on the table,
      4. extracted_at LAST as the load-time fallback -- BUT ONLY if the table
         actually has it. Computed/output tables (e.g. identity_hub.bn_id_hub)
         do not have extracted_at; referencing it in the probe SQL produces a
         BigQuery 400 "Unrecognized name" error and burns the retry budget.
    Each name is validated against the identifier allowlist before use.
    """
    ordered = []
    seen = set()

    def add(col):
        if not col:
            return
        if col in seen:
            return
        # Only keep columns that actually exist on the table. extracted_at used
        # to be unconditionally appended as a "guaranteed final fallback" but
        # that broke tables that genuinely lack it (bn_id_hub, bn_id_neighbors).
        if col not in discovered:
            return
        try:
            _validate_identifier(col, "date_column")
        except ValueError:
            return
        seen.add(col)
        ordered.append(col)

    add(config.get("date_column"))
    for col in config.get("date_column_candidates") or []:
        add(col)
    # All remaining discovered date columns, deterministic order, EXCEPT
    # extracted_at -- we want that one last as the load-time fallback.
    for col in sorted(discovered):
        if col != _EXTRACTED_AT:
            add(col)
    # extracted_at last, but only if the table actually has it. Computed
    # tables without an ETL load column (e.g. identity_hub.bn_id_hub) skip
    # this step.
    if _EXTRACTED_AT in discovered:
        add(_EXTRACTED_AT)
    return ordered


def calculate_freshness(client, config, retry_attempts=3, backoff_seconds=None):
    """Probe a table's freshness, trying every candidate date column.

    Distinguishes a truly empty table (COUNT(*) == 0 -> most_recent_date None)
    from a populated table whose configured column is all-NULL (tries the next
    candidate, finally extracted_at). Records which column won in
    ``date_column_used``.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        config: dict with project_id, dataset_name, table_name, date_column, and
            optionally date_column_candidates (extra columns the source declared).
        retry_attempts: total attempts per query (default 3).
        backoff_seconds: sleep durations between retries (default [5,15,45]).

    Returns:
        FreshnessProbeResult. most_recent_date/days_behind are None ONLY when the
        table is empty OR no candidate column (incl. extracted_at) had any value.

    Raises:
        FreshnessProbeError: on query failure after the retry budget.
    """
    if backoff_seconds is None:
        backoff_seconds = list(_DEFAULT_BACKOFF_SECONDS)
    if not retry_attempts or retry_attempts < 1:
        retry_attempts = _DEFAULT_RETRY_ATTEMPTS

    safe_project = _validate_identifier(config.get("project_id"), "project")
    safe_dataset = _validate_identifier(config.get("dataset_name"), "dataset")
    safe_table = _validate_identifier(config.get("table_name"), "table")
    # Validate the configured column up front so a malicious value is rejected
    # before any SQL is built (preserves the injection guard + its test).
    _validate_identifier(config.get("date_column"), "date_column")

    target = "{0}.{1}.{2}".format(safe_project, safe_dataset, safe_table)

    # 1. Discover the table's actual date/timestamp columns.
    cols_sql = COLUMNS_QUERY.format(project_id=safe_project, dataset_name=safe_dataset)
    from google.cloud import bigquery

    cols_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", safe_table)
        ]
    )
    col_rows = _run_with_retry(
        client, cols_sql, target, retry_attempts, backoff_seconds, cols_job_config
    )
    discovered = {r["column_name"] for r in col_rows}

    # 2. Build the ordered candidate list (configured -> others -> extracted_at).
    candidates = _candidate_columns(config, discovered)
    if not candidates:
        # No usable date column at all (not even extracted_at present). Probe just
        # the row count so we can still tell empty from populated.
        candidates = []

    # 3. Single multi-column probe: COUNT(*) + MAX(DATE(col)) per candidate.
    if candidates:
        probe_sql = build_multi_probe_sql(
            safe_project, safe_dataset, safe_table, candidates
        )
    else:
        probe_sql = "SELECT COUNT(*) AS total_row_count FROM `{0}`.`{1}`.`{2}`".format(
            safe_project, safe_dataset, safe_table
        )
    rows = _run_with_retry(client, probe_sql, target, retry_attempts, backoff_seconds)
    row = rows[0]
    total_row_count = int(row["total_row_count"] or 0)

    # 4. Truly empty table -> None (runner maps to ERROR "empty").
    if total_row_count == 0:
        return FreshnessProbeResult(
            most_recent_date=None,
            days_behind=None,
            total_row_count=0,
            recent_date_row_count=0,
            date_column_used=None,
        )

    # 5. Pick the FIRST candidate whose MAX(DATE) is non-NULL (priority order).
    chosen_col = None
    most_recent_date = None
    for i, col in enumerate(candidates):
        val = row["max_{0}".format(i)]
        if val is not None:
            chosen_col = col
            most_recent_date = val
            break

    # 6. Populated but every candidate date column is NULL -> no freshness signal.
    if most_recent_date is None:
        logger.warning(
            "%s has %d rows but no non-NULL date in any candidate column %s",
            target,
            total_row_count,
            candidates,
        )
        return FreshnessProbeResult(
            most_recent_date=None,
            days_behind=None,
            total_row_count=total_row_count,
            recent_date_row_count=0,
            date_column_used=None,
        )

    # 7. Compute days_behind and the recent-date row count on the CHOSEN column.
    from datetime import date as _date

    days_behind = (_date.today() - most_recent_date).days
    recent_sql = build_recent_count_sql(
        safe_project, safe_dataset, safe_table, chosen_col
    )
    recent_rows = _run_with_retry(
        client, recent_sql, target, retry_attempts, backoff_seconds
    )
    recent_date_row_count = int(recent_rows[0]["recent_date_row_count"] or 0)

    if chosen_col != config.get("date_column"):
        logger.info(
            "%s: configured column %r had no value; used fallback column %r",
            target,
            config.get("date_column"),
            chosen_col,
        )

    return FreshnessProbeResult(
        most_recent_date=most_recent_date,
        days_behind=days_behind,
        total_row_count=total_row_count,
        recent_date_row_count=recent_date_row_count,
        date_column_used=chosen_col,
    )
