#!/usr/bin/env python3
# Preventive detector: SCHEMA DRIFT. Compares each resource's declared YAML
# schema (carried on the target as schema_columns) against the table's LIVE
# BigQuery columns, so an upstream API/extractor change (column added, removed,
# or retyped) surfaces the day it lands -- before it breaks consumers or
# silently NULLs the audit's date column.
"""Schema-drift detector for the anomaly audit.

One INFORMATION_SCHEMA.COLUMNS query PER DATASET (not per table) fetches every
live column; ``detect_schema_drift`` is then a PURE comparison per table. One
AGGREGATED finding per drifted table (never one row per column -- the
one-row-per-issue spam lesson is baked in): the error_message names the missing
/ added / retyped columns.

Severity: MEDIUM when columns are MISSING or RETYPED (consumers can break);
LOW when columns were only ADDED (usually benign autodiscovery lag).
All SQL uses -- comments only; identifiers are validated before interpolation.
"""

import logging
from typing import Any, Dict, List, Optional

from shared.anomaly_audit import (
    DETECTOR_SCHEMA_DRIFT,
    SCOPE_ACTIVE,
    SEVERITY_LOW,
    SEVERITY_MEDIUM,
    STATUS_ANOMALY,
    _validate_identifier,
)

logger = logging.getLogger(__name__)

# Type aliases: YAML schema blocks and INFORMATION_SCHEMA spell some types
# differently; normalize both sides before comparing so BOOL vs BOOLEAN or
# FLOAT vs FLOAT64 never reads as a retype.
_TYPE_ALIASES = {
    "BOOLEAN": "BOOL",
    "FLOAT": "FLOAT64",
    "INTEGER": "INT64",
    "BYTES": "BYTES",
}

# Universal bookkeeping columns the load/merge machinery adds to EVERY table;
# excluded from the "added" comparison (they are expected infrastructure).
_ETL_BOOKKEEPING_COLUMNS = frozenset(
    {
        "row_hash",
        "loaded_at",
        "updated_at",
        "extracted_at",
        "execution_id",
        "source",
        "system_create_timestamp",
        "system_create_time",
    }
)

_COLUMNS_SQL = """-- Live columns for every table in one dataset (one query per
-- dataset, shared by every per-table schema comparison).
SELECT table_name, column_name, data_type
FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS"""


def _norm_type(bq_type: Any) -> str:
    """Normalize a BigQuery type spelling for comparison (parametrized types like
    NUMERIC(10,2) compare on their base name)."""
    t = str(bq_type or "").upper().split("(")[0].strip()
    return _TYPE_ALIASES.get(t, t)


def fetch_live_columns(
    client, project_id: str, dataset_name: str
) -> Dict[str, Dict[str, str]]:
    """Fetch {table_name: {column_name: data_type}} for one dataset.

    One INFORMATION_SCHEMA query per dataset. Raises on failure -- the caller
    (anomaly runner) treats a dataset-level fetch failure as "skip schema checks
    for this dataset" rather than failing the run.
    """
    safe_project = _validate_identifier(project_id, "project")
    safe_dataset = _validate_identifier(dataset_name, "dataset")
    sql = _COLUMNS_SQL.format(project=safe_project, dataset=safe_dataset)
    out: Dict[str, Dict[str, str]] = {}
    for row in client.query(sql).result():
        out.setdefault(row["table_name"], {})[row["column_name"]] = row["data_type"]
    return out


def detect_schema_drift(
    target: Dict[str, Any],
    live_columns: Optional[Dict[str, str]],
    today,
) -> List[Dict[str, Any]]:
    """Compare the target's YAML schema to the live columns. PURE.

    Args:
        target: a loader target dict (uses schema_columns / table metadata).
        live_columns: {column: type} for THIS table from fetch_live_columns, or
            None when the table was absent from the dataset listing (the runner's
            skip-missing policy already covers absent tables -- return []).
        today: the run's reference date (stamped as the finding's anomaly_date).

    Returns:
        [] (no drift / not comparable) or ONE aggregated finding dict.
    """
    declared = target.get("schema_columns") or {}
    if not declared or live_columns is None:
        # Nothing declared to compare against, or table absent (skip policy).
        return []

    declared_norm = {str(c): _norm_type(t) for c, t in declared.items()}
    live_norm = {str(c): _norm_type(t) for c, t in live_columns.items()}

    missing = sorted(set(declared_norm) - set(live_norm))
    # "Added" excludes the pipeline's universal bookkeeping columns: every table
    # gains these from the load/merge machinery without the YAML declaring them,
    # so they are expected infrastructure, not drift (confirmed by the first live
    # preflight run, where they accounted for nearly every added-column note).
    added = sorted(set(live_norm) - set(declared_norm) - _ETL_BOOKKEEPING_COLUMNS)
    retyped = sorted(
        c
        for c in set(declared_norm) & set(live_norm)
        if declared_norm[c] != live_norm[c]
    )

    if not (missing or added or retyped):
        return []

    parts = []
    if missing:
        parts.append("missing: {0}".format(", ".join(missing)))
    if retyped:
        parts.append(
            "retyped: {0}".format(
                ", ".join(
                    "{0} {1}->{2}".format(c, declared_norm[c], live_norm[c])
                    for c in retyped
                )
            )
        )
    if added:
        parts.append("added: {0}".format(", ".join(added)))
    message = "schema drift vs YAML -- " + "; ".join(parts)

    severity = SEVERITY_MEDIUM if (missing or retyped) else SEVERITY_LOW
    logger.warning(
        "Schema drift %s.%s: %s",
        target.get("dataset_name"),
        target.get("table_name"),
        message,
    )
    return [
        {
            "detector": DETECTOR_SCHEMA_DRIFT,
            "anomaly_date": today,
            "observed_count": len(missing) + len(added) + len(retyped),
            "expected_low": None,
            "median_count": None,
            "date_lag_days": None,
            "severity": severity,
            "scope": SCOPE_ACTIVE,
            "audit_status": STATUS_ANOMALY,
            "error_message": message,
        }
    ]
