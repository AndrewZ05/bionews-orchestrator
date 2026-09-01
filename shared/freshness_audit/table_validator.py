#!/usr/bin/env python3
# Validates that a freshness config row points at a real, queryable target.
# Converts not-found / missing-column cases into (False, ascii_message) so the
# runner records audit_status='ERROR'; only unexpected errors surface as messages.
"""Freshness target validator.

Before the calculator runs its probe, confirm that the configured target is real:
  1. the dataset exists,
  2. the table exists,
  3. the date_column exists on that table.

Existence is checked via client.get_dataset / client.get_table (which raise
google.cloud.exceptions.NotFound when absent); the column is confirmed via
INFORMATION_SCHEMA.COLUMNS. All failure modes are returned as
(False, ascii_reason) so a single bad config never aborts the run.
"""

import logging

from google.cloud.exceptions import NotFound

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

# VALIDATE_COLUMN_QUERY: confirm the date_column exists on the target table.
# The dataset is a validated identifier interpolated into the
# INFORMATION_SCHEMA.COLUMNS path; the table_name IS bindable as a scalar @param.
VALIDATE_COLUMN_QUERY = """-- VALIDATE_COLUMN_QUERY: confirm the configured date_column exists on the target
-- table. Dataset is a validated identifier interpolated into the
-- INFORMATION_SCHEMA path; table_name and column_name are bound as scalar params.
SELECT column_name
FROM `{project_id}`.`{dataset_name}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = @table_name
  AND column_name = @date_column"""


def validate_target(client, config):
    """Validate that a config row points at a real, queryable target.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        config: a config dict with at least project_id, dataset_name, table_name,
            date_column.

    Returns:
        (True, None) if the dataset, table, and date_column all exist; otherwise
        (False, ascii_reason). Never raises for the not-found / missing-column
        cases -- those are converted to messages. Only an unexpected exception is
        surfaced (also as (False, message)) so the runner records ERROR rather
        than aborting the whole run.
    """
    # Local import to avoid importing the SDK at module import time where possible.
    from google.cloud import bigquery

    project_id = config.get("project_id")
    dataset_name = config.get("dataset_name")
    table_name = config.get("table_name")
    date_column = config.get("date_column")

    # Validate identifiers first. A bad identifier is itself an ERROR condition,
    # not a crash -- convert the ValueError into a (False, message).
    try:
        safe_project = _validate_identifier(project_id, "project")
        safe_dataset = _validate_identifier(dataset_name, "dataset")
        safe_table = _validate_identifier(table_name, "table")
        safe_column = _validate_identifier(date_column, "date_column")
    except ValueError as exc:
        return (False, str(exc))

    # 1. Dataset existence.
    dataset_ref = "{0}.{1}".format(safe_project, safe_dataset)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        return (False, "dataset not found: {0}".format(dataset_ref))
    except Exception as exc:
        return (False, "error checking dataset {0}: {1}".format(dataset_ref, exc))

    # 2. Table existence.
    table_ref = "{0}.{1}.{2}".format(safe_project, safe_dataset, safe_table)
    try:
        client.get_table(table_ref)
    except NotFound:
        # An active resource whose table does not exist in BigQuery is config
        # drift: the source YAML marks it active but the pipeline never delivered
        # it. Flag it explicitly so the YAML gets corrected (mark inactive or fix
        # the name) -- there should be very few of these.
        return (
            False,
            "config drift -- active in YAML but table not found in BigQuery: "
            "{0}".format(table_ref),
        )
    except Exception as exc:
        return (False, "error checking table {0}: {1}".format(table_ref, exc))

    # 3. date_column existence via INFORMATION_SCHEMA.COLUMNS. table_name and
    # column_name are bound as scalar parameters; the dataset path is interpolated
    # from validated identifiers.
    sql = VALIDATE_COLUMN_QUERY.format(
        project_id=safe_project, dataset_name=safe_dataset
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", safe_table),
            bigquery.ScalarQueryParameter("date_column", "STRING", safe_column),
        ]
    )
    try:
        result = client.query(sql, job_config=job_config).result()
        found = any(True for _ in result)
    except Exception as exc:
        # An unexpected query failure during validation is reported as the reason.
        return (
            False,
            "error validating column {0} on {1}.{2}: {3}".format(
                safe_column, safe_dataset, safe_table, exc
            ),
        )

    if not found:
        return (
            False,
            "date_column not found: {0} on {1}.{2}".format(
                safe_column, safe_dataset, safe_table
            ),
        )

    return (True, None)
