#!/usr/bin/env python3
# Owns the table_freshness_results sink: ensures the dataset and table exist with
# the exact spec schema, then writes result rows via streaming insert.
"""Freshness result writer.

Creates the results dataset/table on demand (idempotently) and writes batches of
result dicts. The primary write path is client.insert_rows_json (streaming insert,
no DML, no SQL); the parameterized RESULT_INSERT DML is provided as the documented
fallback for environments where streaming inserts are disabled. Dicts passed to
write_results MUST contain exactly the 11 results-schema columns.
"""

import logging

from google.cloud import bigquery

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

# RESULTS_SCHEMA: schema for table_freshness_results. The last three columns
# (date_column_used + the two thresholds) make every row self-explanatory: WHICH
# column the freshness was measured on (the probe may fall back past the
# configured column) and WHAT SLA it was judged against at the time. Older rows
# predate these columns and read NULL.
RESULTS_SCHEMA = [
    bigquery.SchemaField("audit_id", "STRING"),
    bigquery.SchemaField("audit_run_id", "STRING"),
    bigquery.SchemaField("dataset_name", "STRING"),
    bigquery.SchemaField("table_name", "STRING"),
    bigquery.SchemaField("most_recent_date", "DATE"),
    bigquery.SchemaField("days_behind", "INT64"),
    bigquery.SchemaField("total_row_count", "INT64"),
    bigquery.SchemaField("recent_date_row_count", "INT64"),
    bigquery.SchemaField("audit_status", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("audited_at", "TIMESTAMP"),
    bigquery.SchemaField("date_column_used", "STRING"),
    bigquery.SchemaField("freshness_threshold_days", "INT64"),
    bigquery.SchemaField("warning_threshold_days", "INT64"),
    # Per-resource audit policy: NULL means "default" (no cadence preset, no
    # threshold override). Otherwise carries the cadence preset name (e.g. 'rare')
    # and the operator-supplied reason -- step 1 of the per-table tolerance work.
    # Older rows predate these columns and read NULL.
    bigquery.SchemaField("audit_cadence", "STRING"),
    bigquery.SchemaField("audit_reason", "STRING"),
]

# RESULT_INSERT: parameterized single-row INSERT into table_freshness_results.
# This is the documented fallback path (result_writer normally uses
# client.insert_rows_json for batch streaming). audit_run_id is supplied by the
# CALLER as a bound parameter; it is NEVER generated inside SQL. All values are
# bound as scalar query parameters to avoid injection. Dataset/table identifiers
# are validated+interpolated by the caller.
RESULT_INSERT = """-- RESULT_INSERT: parameterized single-row INSERT into table_freshness_results.
-- This is the documented fallback path (result_writer normally uses
-- client.insert_rows_json for batch streaming). audit_run_id is supplied by the
-- CALLER as a bound parameter; it is NEVER generated inside SQL. All values are
-- bound as scalar query parameters to avoid injection. Dataset/table identifiers
-- are validated+interpolated by the caller.
INSERT INTO `{results_dataset}`.`{results_table}` (
  audit_id,
  audit_run_id,
  dataset_name,
  table_name,
  most_recent_date,
  days_behind,
  total_row_count,
  recent_date_row_count,
  audit_status,
  error_message,
  audited_at,
  date_column_used,
  freshness_threshold_days,
  warning_threshold_days,
  audit_cadence,
  audit_reason
)
VALUES (
  @audit_id,
  @audit_run_id,
  @dataset_name,
  @table_name,
  @most_recent_date,
  @days_behind,
  @total_row_count,
  @recent_date_row_count,
  @audit_status,
  @error_message,
  @audited_at,
  @date_column_used,
  @freshness_threshold_days,
  @warning_threshold_days,
  @audit_cadence,
  @audit_reason
)"""


class FreshnessResultWriteError(Exception):
    """Raised when the results streaming insert returns row errors."""


def ensure_results_dataset(client, dataset, location="US"):
    """Create the results dataset if it does not already exist (idempotent).

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        dataset: dataset name (validated identifier).
        location: BigQuery location for the dataset if it must be created.
    """
    safe_dataset = _validate_identifier(dataset, "dataset")
    dataset_id = "{0}.{1}".format(client.project, safe_dataset)
    dataset_obj = bigquery.Dataset(dataset_id)
    dataset_obj.location = location
    # exists_ok=True makes this a no-op when the dataset is already present.
    client.create_dataset(dataset_obj, exists_ok=True)
    logger.info(
        "Ensured results dataset exists: %s (location=%s)", dataset_id, location
    )


# RUNS_SCHEMA: one row per audit RUN (freshness or anomaly -- the 'source' column
# distinguishes them), so the audits themselves are auditable: duration, what was
# skipped, what the counts were, whether the email went out. Enables
# "did the audit run today?" alerting via SELECT MAX(started_at). Shared by both
# audits (the anomaly result_writer re-exports these helpers); each audit writes
# into its OWN dataset's audit_runs table. List-valued facts (skipped tables,
# cold-start tables) are stored as JSON-encoded STRINGs to keep the row flat.
RUNS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("finished_at", "TIMESTAMP"),
    bigquery.SchemaField("duration_seconds", "FLOAT64"),
    bigquery.SchemaField("tables_audited", "INT64"),
    bigquery.SchemaField("tables_skipped", "INT64"),
    bigquery.SchemaField("passed", "INT64"),
    bigquery.SchemaField("warning", "INT64"),
    bigquery.SchemaField("failed", "INT64"),
    bigquery.SchemaField("errored", "INT64"),
    bigquery.SchemaField("anomalies", "INT64"),
    bigquery.SchemaField("active", "INT64"),
    bigquery.SchemaField("historical", "INT64"),
    bigquery.SchemaField("new_failures", "INT64"),
    bigquery.SchemaField("recovered", "INT64"),
    bigquery.SchemaField("new_historical", "INT64"),
    bigquery.SchemaField("cold_start_count", "INT64"),
    bigquery.SchemaField("skipped_tables", "STRING"),
    bigquery.SchemaField("cold_start_tables", "STRING"),
    bigquery.SchemaField("email_sent", "BOOL"),
    bigquery.SchemaField("error_message", "STRING"),
]


def ensure_runs_table(client, dataset, table="audit_runs"):
    """Create the audit_runs table if absent (idempotent, additive migration).

    Same pattern as ensure_results_table: create-if-absent with RUNS_SCHEMA, then
    append any missing columns as NULLABLE so schema growth never needs a manual
    prod step.
    """
    safe_dataset = _validate_identifier(dataset, "dataset")
    safe_table = _validate_identifier(table, "table")
    table_id = "{0}.{1}.{2}".format(client.project, safe_dataset, safe_table)
    table_obj = bigquery.Table(table_id, schema=RUNS_SCHEMA)
    created = client.create_table(table_obj, exists_ok=True)

    existing_names = {field.name for field in (created.schema or [])}
    missing = [f for f in RUNS_SCHEMA if f.name not in existing_names]
    if missing:
        created.schema = list(created.schema) + [
            bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE") for f in missing
        ]
        created = client.update_table(created, ["schema"])
        logger.info(
            "Migrated runs table %s: added column(s) %s",
            table_id,
            ", ".join(f.name for f in missing),
        )
    return created


def write_run_row(client, dataset, row, table="audit_runs"):
    """Insert one audit-run metadata row, best-effort.

    NEVER raises: a metadata write failure must not fail the audit itself. The
    row dict should be JSON-safe (timestamps as ISO strings, lists pre-encoded
    as JSON strings). Returns True on success, False otherwise.
    """
    try:
        table_obj = ensure_runs_table(client, dataset, table)
        errors = client.insert_rows_json(table_obj, [row])
        if errors:
            logger.warning("Audit-run row insert reported errors: %s", errors)
            return False
        logger.info(
            "Recorded audit run %s in %s.%s",
            row.get("run_id"),
            dataset,
            table,
        )
        return True
    except Exception as exc:  # noqa: BLE001 - metadata write is best-effort
        logger.warning("Could not record audit-run metadata: %s", exc)
        return False


def ensure_results_table(client, dataset, table="table_freshness_results"):
    """Create the results table if absent and MIGRATE missing columns (idempotent).

    Additive schema migration: when the live table predates columns in
    RESULTS_SCHEMA (e.g. date_column_used / the threshold columns), the missing
    fields are appended as NULLABLE via client.update_table -- the API equivalent
    of ALTER TABLE ADD COLUMN. Existing rows read NULL for the new columns; no
    data is rewritten and the operation is safe to repeat on every run.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        dataset: dataset name (validated identifier).
        table: results table name (validated identifier).

    Returns:
        The existing (possibly migrated) or newly created bigquery.Table.
    """
    safe_dataset = _validate_identifier(dataset, "dataset")
    safe_table = _validate_identifier(table, "table")
    table_id = "{0}.{1}.{2}".format(client.project, safe_dataset, safe_table)
    table_obj = bigquery.Table(table_id, schema=RESULTS_SCHEMA)
    # exists_ok=True returns the existing table without altering its schema.
    created = client.create_table(table_obj, exists_ok=True)
    logger.info("Ensured results table exists: %s", table_id)

    # Additive migration: append any RESULTS_SCHEMA columns the live table lacks.
    existing_names = {field.name for field in (created.schema or [])}
    missing = [f for f in RESULTS_SCHEMA if f.name not in existing_names]
    if missing:
        created.schema = list(created.schema) + [
            bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE") for f in missing
        ]
        created = client.update_table(created, ["schema"])
        logger.info(
            "Migrated results table %s: added column(s) %s",
            table_id,
            ", ".join(f.name for f in missing),
        )
    return created


def write_results(client, dataset, table, rows):
    """Insert a batch of result dicts into the results table via streaming insert.

    Each dict MUST contain exactly the 11 results-schema columns. most_recent_date
    should be an ISO date string ('YYYY-MM-DD') or None; audited_at an ISO-8601 UTC
    string; days_behind and the row counts int or None.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        dataset: results dataset name (validated identifier).
        table: results table name (validated identifier).
        rows: list of schema-matching dicts to insert.

    Returns:
        The number of rows written.

    Raises:
        FreshnessResultWriteError: if BigQuery returns per-row insert errors.
    """
    if not rows:
        logger.info("No freshness result rows to write")
        return 0

    safe_dataset = _validate_identifier(dataset, "dataset")
    safe_table = _validate_identifier(table, "table")

    # Resolve a Table object with the known schema so insert_rows_json can serialize
    # correctly (idempotent ensure also guarantees the table is present).
    table_obj = ensure_results_table(client, safe_dataset, safe_table)

    errors = client.insert_rows_json(table_obj, rows)
    if errors:
        # insert_rows_json returns a non-empty list of error dicts on failure.
        logger.error(
            "Freshness result insert returned %d row errors: %s", len(errors), errors
        )
        raise FreshnessResultWriteError(
            "insert_rows_json returned errors for {0}.{1}: {2}".format(
                safe_dataset, safe_table, errors
            )
        )

    logger.info(
        "Wrote %d freshness result rows to %s.%s", len(rows), safe_dataset, safe_table
    )
    return len(rows)
