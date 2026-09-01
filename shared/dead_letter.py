"""
Dead-letter recording for per-table ETL failures.

The transform loop already isolates failures (one bad table does not abort the
run). What was missing: a durable, queryable record of WHICH tables failed and
WHY, so they can be found and retried instead of being lost in logs. This
records failures to orchestrator_monitoring.dead_letter_queue, honoring the
existing `dead_letter_queue` config block (enabled, max_retries, purge_after_days).

It is intentionally lightweight: recording is best-effort and never raises into
the pipeline (a DLQ write failure must not turn a recoverable per-table failure
into a crashed run). Reading back for retry is a future Phase; this establishes
the durable record and the structured run summary.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

TABLE_NAME = "dead_letter_queue"


def _monitoring_dataset() -> str:
    from shared.job_manager import MONITORING_DATASET

    return os.environ.get("MONITORING_DATASET", MONITORING_DATASET)


def dlq_enabled(config: Dict[str, Any]) -> bool:
    return bool((config.get("dead_letter_queue", {}) or {}).get("enabled", False))


def _ensure_table(client: bigquery.Client) -> str:
    table_id = f"{client.project}.{_monitoring_dataset()}.{TABLE_NAME}"
    try:
        client.get_table(table_id)
        return table_id
    except Exception:  # noqa: BLE001 - NotFound or transient; try create
        pass

    schema = [
        bigquery.SchemaField("dlq_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("execution_id", "STRING"),
        bigquery.SchemaField("job_id", "STRING"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("stage", "STRING", description="extract|transform|merge"),
        bigquery.SchemaField("error_type", "STRING"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("refresh_mode", "STRING"),
        bigquery.SchemaField("failed_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField(
            "retry_count",
            "INTEGER",
            description="Attempts already made (0 = first failure)",
        ),
        bigquery.SchemaField(
            "resolved",
            "BOOL",
            description="True once a later run succeeds for this table",
        ),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="failed_at"
    )
    table.clustering_fields = ["source", "table_name"]
    table.description = (
        "Per-table ETL failures for retry/triage. One row per failed table per "
        "run. `resolved` flips to TRUE when a subsequent run succeeds for the "
        "same (source, table)."
    )
    try:
        client.create_table(table)
        logger.info("Created dead-letter table: %s", table_id)
    except Exception as e:  # noqa: BLE001 - race or perms; recording is best-effort
        logger.debug("Could not create dead-letter table: %s", e)
    return table_id


def record_failures(
    client: bigquery.Client,
    config: Dict[str, Any],
    source: str,
    failures: List[Dict[str, Any]],
    *,
    execution_id: Optional[str] = None,
    job_id: Optional[str] = None,
    refresh_mode: Optional[str] = None,
) -> int:
    """
    Record failed tables to the dead-letter queue. `failures` is a list of
    {table, error, stage[, error_type]} dicts (the shape the transform loop
    already builds). Returns rows written. Never raises.
    """
    if not failures or not dlq_enabled(config):
        return 0
    try:
        table_id = _ensure_table(client)
    except Exception as e:  # noqa: BLE001
        logger.debug("DLQ ensure-table failed: %s", e)
        return 0

    # Deterministic dlq_id (no Date.now/random available); execution + table is
    # unique per run, and a retry of the same table in a new run gets a new
    # execution_id.
    rows = []
    for f in failures:
        tbl = f.get("table") or f.get("table_name")
        if not tbl:
            continue
        rows.append(
            {
                "dlq_id": f"{execution_id or 'manual'}:{tbl}",
                "execution_id": execution_id,
                "job_id": job_id,
                "source": source,
                "table_name": tbl,
                "stage": f.get("stage"),
                "error_type": f.get("error_type"),
                "error_message": (f.get("error") or "")[:1000],
                "refresh_mode": refresh_mode,
                "retry_count": int(f.get("retry_count", 0)),
                "resolved": False,
            }
        )
    if not rows:
        return 0

    # Parameterized INSERT (immediately consistent; table may be new this run).
    values, params = [], []
    for i, r in enumerate(rows):
        values.append(
            f"(@dlq_id_{i}, @exec_{i}, @job_{i}, @src_{i}, @tbl_{i}, @stage_{i}, "
            f"@etype_{i}, @emsg_{i}, @rmode_{i}, CURRENT_TIMESTAMP(), @rc_{i}, FALSE)"
        )
        params += [
            bigquery.ScalarQueryParameter(f"dlq_id_{i}", "STRING", r["dlq_id"]),
            bigquery.ScalarQueryParameter(f"exec_{i}", "STRING", r["execution_id"]),
            bigquery.ScalarQueryParameter(f"job_{i}", "STRING", r["job_id"]),
            bigquery.ScalarQueryParameter(f"src_{i}", "STRING", r["source"]),
            bigquery.ScalarQueryParameter(f"tbl_{i}", "STRING", r["table_name"]),
            bigquery.ScalarQueryParameter(f"stage_{i}", "STRING", r["stage"]),
            bigquery.ScalarQueryParameter(f"etype_{i}", "STRING", r["error_type"]),
            bigquery.ScalarQueryParameter(f"emsg_{i}", "STRING", r["error_message"]),
            bigquery.ScalarQueryParameter(f"rmode_{i}", "STRING", r["refresh_mode"]),
            bigquery.ScalarQueryParameter(f"rc_{i}", "INT64", r["retry_count"]),
        ]
    query = f"""
    INSERT INTO `{table_id}`
      (dlq_id, execution_id, job_id, source, table_name, stage, error_type,
       error_message, refresh_mode, failed_at, retry_count, resolved)
    VALUES {", ".join(values)}
    """
    try:
        client.query(
            query, job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()
        logger.info("Recorded %d table failure(s) to dead-letter queue", len(rows))
        return len(rows)
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not write to dead-letter queue: %s", e)
        return 0


def mark_resolved(
    client: bigquery.Client,
    config: Dict[str, Any],
    source: str,
    succeeded_tables: List[str],
) -> int:
    """
    Mark prior unresolved DLQ rows resolved when a later run succeeds for those
    tables. Best-effort; returns rows updated.
    """
    if not succeeded_tables or not dlq_enabled(config):
        return 0
    table_id = f"{client.project}.{_monitoring_dataset()}.{TABLE_NAME}"
    try:
        client.get_table(table_id)
    except Exception:  # noqa: BLE001 - nothing to resolve if table absent
        return 0
    query = f"""
    UPDATE `{table_id}`
    SET resolved = TRUE
    WHERE source = @source
      AND resolved = FALSE
      AND table_name IN UNNEST(@tables)
    """
    try:
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                    bigquery.ArrayQueryParameter("tables", "STRING", succeeded_tables),
                ]
            ),
        )
        job.result()
        return int(job.num_dml_affected_rows or 0)
    except Exception as e:  # noqa: BLE001
        logger.debug("Could not mark DLQ rows resolved: %s", e)
        return 0


def find_retryable_tables(
    client: bigquery.Client,
    config: Dict[str, Any],
    source: str,
) -> List[Dict[str, Any]]:
    """
    Return the still-failing tables for `source` that are eligible for retry --
    the read-back half of the DLQ (record_failures writes; this reads).

    A (source, table) is retryable when it has unresolved failures AND the highest
    retry_count recorded so far is < the configured max_retries (default 3), so a
    permanently-broken table (e.g. a deprecated metric) stops retrying. The next
    retry should record retry_count = max_seen + 1.

    Returns a list of {table_name, retry_count, error_message, stage, last_failed}
    dicts, ordered by table_name. Never raises (returns [] on any error / when the
    DLQ is disabled or absent).
    """
    if not dlq_enabled(config):
        return []
    max_retries = int((config.get("dead_letter_queue", {}) or {}).get("max_retries", 3))
    table_id = f"{client.project}.{_monitoring_dataset()}.{TABLE_NAME}"
    try:
        client.get_table(table_id)
    except Exception:  # noqa: BLE001 - nothing to retry if table absent
        return []

    # Per (source, table): max retry_count seen and the latest error. Only tables
    # with NO resolved row AND max(retry_count) < max_retries are retryable.
    query = f"""
    WITH agg AS (
      SELECT
        table_name,
        MAX(retry_count) AS max_retry,
        LOGICAL_OR(resolved) AS any_resolved,
        ARRAY_AGG(STRUCT(error_message, stage, failed_at)
                  ORDER BY failed_at DESC LIMIT 1)[OFFSET(0)] AS latest
      FROM `{table_id}`
      WHERE source = @source
      GROUP BY table_name
    )
    SELECT table_name, max_retry, latest.error_message AS error_message,
           latest.stage AS stage, latest.failed_at AS last_failed
    FROM agg
    WHERE any_resolved = FALSE AND max_retry < @max_retries
    ORDER BY table_name
    """
    try:
        rows = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                    bigquery.ScalarQueryParameter("max_retries", "INT64", max_retries),
                ]
            ),
        ).result()
        return [
            {
                "table_name": r["table_name"],
                "retry_count": int(r["max_retry"] or 0),
                "error_message": r["error_message"],
                "stage": r["stage"],
                "last_failed": r["last_failed"],
            }
            for r in rows
        ]
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not read dead-letter queue for retry: %s", e)
        return []
