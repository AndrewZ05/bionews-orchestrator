#!/usr/bin/env python3
# Owns the anomaly_results sink: ensures the dataset and table exist with the
# exact spec schema, then writes result rows via streaming insert.
"""Anomaly result writer.

Creates the results dataset/table on demand (idempotently) and writes batches of
result dicts. The primary write path is client.insert_rows_json (streaming insert,
no DML, no SQL); the parameterized RESULT_INSERT DML is provided as the documented
fallback for environments where streaming inserts are disabled. Dicts passed to
write_results MUST contain exactly the 16 results-schema columns.

This module MIRRORS shared.freshness_audit.result_writer: it reuses the single
shared _validate_identifier helper so dataset/table allowlist logic never drifts.
All log strings are ASCII-only (no em-dash, no unicode); the SQL uses only double-
dash line comments (never slash-star block comments).
"""

import logging

from google.cloud import bigquery

from shared.freshness_audit import _validate_identifier

# The audit_runs metadata table (one row per run) has ONE shared implementation
# in the freshness result_writer; both audits write into their OWN dataset's
# audit_runs table through these re-exported helpers.
from shared.freshness_audit.result_writer import (  # noqa: F401 - re-export
    RUNS_SCHEMA,
    ensure_runs_table,
    write_run_row,
)

logger = logging.getLogger(__name__)

# RESULTS_SCHEMA: exact spec schema for anomaly_results. Field order and types
# MUST match the design's enumerated 16 columns; the runner's strip/build logic
# relies on this identical set.
RESULTS_SCHEMA = [
    bigquery.SchemaField("audit_id", "STRING"),
    bigquery.SchemaField("audit_run_id", "STRING"),
    bigquery.SchemaField("dataset_name", "STRING"),
    bigquery.SchemaField("table_name", "STRING"),
    bigquery.SchemaField("date_column_used", "STRING"),
    bigquery.SchemaField("detector", "STRING"),
    bigquery.SchemaField("anomaly_date", "DATE"),
    bigquery.SchemaField("observed_count", "INT64"),
    bigquery.SchemaField("expected_low", "INT64"),
    bigquery.SchemaField("median_count", "INT64"),
    bigquery.SchemaField("date_lag_days", "INT64"),
    bigquery.SchemaField("severity", "STRING"),
    bigquery.SchemaField("scope", "STRING"),
    bigquery.SchemaField("audit_status", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
    bigquery.SchemaField("audited_at", "TIMESTAMP"),
    # Per-resource audit policy: NULL means "default" (no cadence preset, no
    # threshold override). Otherwise carries the cadence preset name (e.g.
    # 'rare') and the operator-supplied reason -- step 1 of the per-table
    # tolerance work. Older rows predate these columns and read NULL. Migrated
    # additively by ensure_results_table.
    bigquery.SchemaField("audit_cadence", "STRING"),
    bigquery.SchemaField("audit_reason", "STRING"),
]

# RESULT_INSERT: parameterized single-row INSERT into anomaly_results.
# This is the documented fallback path (result_writer normally uses
# client.insert_rows_json for batch streaming). audit_run_id is supplied by the
# CALLER as a bound parameter; it is NEVER generated inside SQL. All values are
# bound as scalar query parameters to avoid injection. Dataset/table identifiers
# are validated+interpolated by the caller. Only -- comments are used.
RESULT_INSERT = """-- RESULT_INSERT: parameterized single-row INSERT into anomaly_results.
-- This is the documented fallback path (result_writer normally uses
-- client.insert_rows_json for batch streaming). audit_run_id is supplied by the
-- CALLER as a bound parameter; it is NEVER generated inside SQL. All values are
-- bound as scalar query parameters to avoid injection. Dataset/table identifiers
-- are validated+interpolated by the caller. Only -- comments are used.
INSERT INTO `{results_dataset}`.`{results_table}` (
  audit_id,
  audit_run_id,
  dataset_name,
  table_name,
  date_column_used,
  detector,
  anomaly_date,
  observed_count,
  expected_low,
  median_count,
  date_lag_days,
  severity,
  scope,
  audit_status,
  error_message,
  audited_at,
  audit_cadence,
  audit_reason
)
VALUES (
  @audit_id,
  @audit_run_id,
  @dataset_name,
  @table_name,
  @date_column_used,
  @detector,
  @anomaly_date,
  @observed_count,
  @expected_low,
  @median_count,
  @date_lag_days,
  @severity,
  @scope,
  @audit_status,
  @error_message,
  @audited_at,
  @audit_cadence,
  @audit_reason
)"""


class AnomalyResultWriteError(Exception):
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


def ensure_results_table(client, dataset, table="anomaly_results"):
    """Create the results table if absent and MIGRATE missing columns (idempotent).

    Additive schema migration: when the live table predates columns in
    RESULTS_SCHEMA (e.g. audit_cadence / audit_reason added in the per-table
    cadence work), the missing fields are appended as NULLABLE via
    client.update_table -- the API equivalent of ALTER TABLE ADD COLUMN. Existing
    rows read NULL for the new columns; no data is rewritten and the operation
    is safe to repeat on every run.

    Mirrors the freshness ensure_results_table migration block exactly so the
    two writers stay in sync. Without this, the next anomaly run on a project
    whose anomaly_results predates audit_cadence / audit_reason would crash on
    insert_rows_json with "no such field".

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

    Each dict MUST contain exactly the 16 results-schema columns. anomaly_date
    should be an ISO date string ('YYYY-MM-DD') or None; audited_at an ISO-8601 UTC
    string; observed_count/expected_low/median_count/date_lag_days int or None.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        dataset: results dataset name (validated identifier).
        table: results table name (validated identifier).
        rows: list of schema-matching dicts to insert.

    Returns:
        The number of rows written.

    Raises:
        AnomalyResultWriteError: if BigQuery returns per-row insert errors.
    """
    if not rows:
        logger.info("No anomaly result rows to write")
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
            "Anomaly result insert returned %d row errors: %s", len(errors), errors
        )
        raise AnomalyResultWriteError(
            "insert_rows_json returned errors for {0}.{1}: {2}".format(
                safe_dataset, safe_table, errors
            )
        )

    logger.info(
        "Wrote %d anomaly result rows to %s.%s", len(rows), safe_dataset, safe_table
    )
    return len(rows)
