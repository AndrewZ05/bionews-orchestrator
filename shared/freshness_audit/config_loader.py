#!/usr/bin/env python3
# Legacy config loader. The active source of truth is now configs/audit_sources.yaml
# plus each source's own YAML; see shared.freshness_audit.yaml_target_loader.
# This module survives for ONE reason: it owns FreshnessConfigError, which the
# runner imports as a run-level-fatal signal distinct from a single table's
# ERROR result. load_active_configs() is no longer wired into the runner.
"""Freshness config loader -- legacy.

Historically loaded per-table audit configurations from a BigQuery
``table_freshness_config`` table. That model was retired; the runner now derives
the audit target list from configs/audit_sources.yaml plus each source's
configs/<source>.yaml via ``yaml_target_loader.build_all_targets``. The function
``load_active_configs`` is preserved for backward compatibility and tests; it
is NOT called by the live pipeline.

The class ``FreshnessConfigError`` is still part of the active surface -- the
runner imports it so any future loader (registry-unreadable, source-YAML
malformed) can raise it as a run-level fatal that is distinct from a per-table
ERROR result.
"""

import logging

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

# CONFIG_READ_QUERY: load active freshness configs. Dataset/table are validated
# identifiers interpolated by config_loader (BigQuery cannot bind table names).
# Only active rows are returned; column list is explicit to lock the dict shape.
CONFIG_READ_QUERY = """-- CONFIG_READ_QUERY: load active freshness configs. Dataset/table are validated
-- identifiers interpolated by config_loader (BigQuery cannot bind table names).
-- Only active rows are returned; column list is explicit to lock the dict shape.
SELECT
  config_id,
  project_id,
  dataset_name,
  table_name,
  date_column,
  partition_column,
  freshness_threshold_days,
  warning_threshold_days,
  source_system,
  table_owner,
  email_recipients,
  send_email_notification,
  is_active
FROM `{config_dataset}`.`{config_table}`
WHERE is_active = TRUE"""

# Columns returned, in schema order. Used to lock the dict shape on each row.
_CONFIG_COLUMNS = (
    "config_id",
    "project_id",
    "dataset_name",
    "table_name",
    "date_column",
    "partition_column",
    "freshness_threshold_days",
    "warning_threshold_days",
    "source_system",
    "table_owner",
    "email_recipients",
    "send_email_notification",
    "is_active",
)

# INT64 columns that must be coerced to Python int (BQ may yield None for NULL).
_INT_COLUMNS = ("freshness_threshold_days", "warning_threshold_days")


class FreshnessConfigError(Exception):
    """Raised when the config table itself cannot be read (run-level fatal)."""


def load_active_configs(client, config_dataset, config_table="table_freshness_config"):
    """Load active rows from the table_freshness_config table.

    Args:
        client: an authenticated google.cloud.bigquery.Client.
        config_dataset: dataset holding the config table (validated identifier).
        config_table: name of the config table (validated identifier).

    Returns:
        A list of plain dicts, one per active config row, with exactly the config
        columns as keys. NULL email_recipients is coerced to None; threshold INT64
        values are coerced to int. Returns an empty list if no rows are active.

    Raises:
        FreshnessConfigError: if the query fails (an unreadable config table is a
            run-level fatal and is intentionally NOT swallowed here).
    """
    # Validate identifiers before interpolation -- BQ cannot bind table names.
    safe_dataset = _validate_identifier(config_dataset, "dataset")
    safe_table = _validate_identifier(config_table, "table")

    sql = CONFIG_READ_QUERY.format(config_dataset=safe_dataset, config_table=safe_table)

    try:
        # No bound parameters: the WHERE clause is a literal and identifiers are
        # validated+interpolated above.
        rows = client.query(sql).result()
        configs = []
        for row in rows:
            config = {col: row[col] for col in _CONFIG_COLUMNS}
            # Coerce NULL email_recipients to None (BQ already yields None, but be
            # explicit so downstream split logic can rely on the contract).
            if config.get("email_recipients") in (None, ""):
                config["email_recipients"] = None
            # Coerce INT64 thresholds to int where present.
            for int_col in _INT_COLUMNS:
                value = config.get(int_col)
                if value is not None:
                    config[int_col] = int(value)
            configs.append(config)
    except Exception as exc:
        # Distinct from a single table ERROR: the config table being unreadable
        # aborts the whole run. Do not swallow.
        logger.error(
            "Failed to read freshness config table %s.%s: %s",
            safe_dataset,
            safe_table,
            exc,
        )
        raise FreshnessConfigError(
            "could not read config table {0}.{1}: {2}".format(
                safe_dataset, safe_table, exc
            )
        ) from exc

    logger.info(
        "Loaded %d active freshness config rows from %s.%s",
        len(configs),
        safe_dataset,
        safe_table,
    )
    return configs
