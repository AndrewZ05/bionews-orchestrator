#!/usr/bin/env python3

from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
import os
from email.mime.text import MIMEText
import smtplib
import uuid
import logging
import threading

# Import notification functions from shared module
from shared.notifications import (
    send_email_notification,
    send_notification as send_notification_channels,
)
from shared.bigquery_client import get_bigquery_client

# Configure logging
logger = logging.getLogger(__name__)


def _fetch_log_content(log_path: str) -> Optional[str]:
    """Retrieve full log content from GCS or local path."""
    if not log_path:
        return None

    try:
        if log_path.startswith("gs://"):
            path = log_path[5:]
            if "/" not in path:
                return None
            bucket_name, blob_name = path.split("/", 1)
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            if not blob.exists():
                return None
            return blob.download_as_text()
        else:
            if os.path.exists(log_path):
                with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
                    return fh.read()
    except Exception as exc:
        logger.debug(f"Failed to fetch log content from {log_path}: {exc}")
    return None


# Thread-safe monitoring infrastructure initialization
_monitoring_initialized = False
_monitoring_lock = threading.Lock()


def ensure_monitoring_infrastructure(client: bigquery.Client) -> None:
    """
    Thread-safe initialization of monitoring infrastructure.

    Creates orchestrator_monitoring dataset and all monitoring tables.
    Uses double-check locking pattern for thread safety and performance.

    This is the single source of truth for monitoring infrastructure setup.
    All other code should call this function instead of creating the dataset directly.

    Tables created:
    - jobs: Centralized job tracking
    - schema_changes: Schema evolution tracking
    - schema_versions: Schema version history
    - merge_locks: Merge operation locks
    - data_changes: Track insert/update/remove counts per merge
    - _pipeline_executions: Pipeline execution tracking (legacy)
    - _pipeline_checkpoints: Pipeline checkpoint tracking (legacy)
    """
    global _monitoring_initialized

    # Fast path: already initialized
    if _monitoring_initialized:
        return

    # Slow path: need to initialize
    with _monitoring_lock:
        # Double-check: another thread might have initialized while we waited
        if _monitoring_initialized:
            return

        # Create dataset if it doesn't exist
        dataset_id = f"{client.project}.orchestrator_monitoring"
        try:
            client.get_dataset(dataset_id)
            logger.debug(f"Monitoring dataset exists: {dataset_id}")
        except Exception:
            dataset_obj = bigquery.Dataset(dataset_id)
            dataset_obj.location = "US"
            dataset_obj.description = "Orchestrator monitoring: jobs, data health, lineage, schema changes, etc."
            client.create_dataset(dataset_obj)
            logger.info(
                f"Created centralized monitoring dataset: orchestrator_monitoring"
            )

        # Create jobs table
        _ensure_jobs_table(client)
        _ensure_jobs_machine_columns(client)
        _ensure_jobs_bq_usage_columns(client)

        # Create table processing tracking table
        _ensure_table_processing_table(client)

        # Create schema tracking tables
        _ensure_schema_tracking_tables(client)

        # Create merge locks table
        _ensure_merge_locks_table(client)

        # Create data changes tracking table
        _ensure_data_changes_table(client)

        # Create legacy pipeline tables
        _ensure_legacy_pipeline_tables(client)

        # Mark as initialized
        _monitoring_initialized = True
        logger.debug("Monitoring infrastructure initialization complete")


def _ensure_jobs_table(client: bigquery.Client) -> None:
    # Create centralized jobs table if it doesn't exist.
    table_id = f"{client.project}.orchestrator_monitoring.jobs"

    try:
        client.get_table(table_id)
        logger.debug(f"Jobs table exists: {table_id}")
        return
    except Exception:
        pass

    # Define schema for centralized jobs table
    schema = [
        bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("job_name", "STRING"),
        bigquery.SchemaField("job_type", "STRING"),
        bigquery.SchemaField("command_line", "STRING"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status_updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("queued_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("completed_at", "TIMESTAMP"),
        bigquery.SchemaField("duration_seconds", "INT64"),
        bigquery.SchemaField("tables", "STRING", mode="REPEATED"),
        bigquery.SchemaField("sites", "STRING", mode="REPEATED"),
        bigquery.SchemaField("groups", "STRING", mode="REPEATED"),
        bigquery.SchemaField("refresh_mode", "STRING"),
        bigquery.SchemaField("total_rows", "INT64"),
        bigquery.SchemaField("sites_processed", "INT64"),
        bigquery.SchemaField("tables_processed", "INT64"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("error_type", "STRING"),
        bigquery.SchemaField("retry_count", "INT64"),
        bigquery.SchemaField("log_file_path", "STRING"),
        bigquery.SchemaField("log_preview", "STRING"),
        bigquery.SchemaField("environment", "STRING"),
        bigquery.SchemaField("triggered_by", "STRING"),
        bigquery.SchemaField("parent_job_id", "STRING"),
        bigquery.SchemaField("hostname", "STRING"),
        bigquery.SchemaField("ip_address", "STRING"),
        bigquery.SchemaField("os_type", "STRING"),
        bigquery.SchemaField("os_version", "STRING"),
        bigquery.SchemaField("python_version", "STRING"),
        bigquery.SchemaField("process_id", "INT64"),
        bigquery.SchemaField("parent_process_id", "INT64"),
        bigquery.SchemaField("user", "STRING"),
        bigquery.SchemaField("working_directory", "STRING"),
        bigquery.SchemaField("is_remote_killable", "BOOL"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("bq_total_bytes_billed", "INT64"),
        bigquery.SchemaField("bq_total_bytes_processed", "INT64"),
        bigquery.SchemaField("bq_total_slot_ms", "INT64"),
        bigquery.SchemaField("bq_labeled_job_count", "INT64"),
        bigquery.SchemaField("bq_estimated_cost_usd", "FLOAT64"),
        bigquery.SchemaField("bq_usage_detail", "JSON"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="queued_at"
    )
    table.clustering_fields = ["source", "status", "job_type"]

    client.create_table(table)
    logger.info("Created centralized jobs table")


def _ensure_jobs_machine_columns(client: bigquery.Client) -> None:
    """Add host / process columns expected by create_centralized_job (idempotent)."""
    table_id = f"{client.project}.orchestrator_monitoring.jobs"
    clauses = (
        "ADD COLUMN IF NOT EXISTS hostname STRING",
        "ADD COLUMN IF NOT EXISTS ip_address STRING",
        "ADD COLUMN IF NOT EXISTS os_type STRING",
        "ADD COLUMN IF NOT EXISTS os_version STRING",
        "ADD COLUMN IF NOT EXISTS python_version STRING",
        "ADD COLUMN IF NOT EXISTS process_id INT64",
        "ADD COLUMN IF NOT EXISTS parent_process_id INT64",
        "ADD COLUMN IF NOT EXISTS user STRING",
        "ADD COLUMN IF NOT EXISTS working_directory STRING",
        "ADD COLUMN IF NOT EXISTS is_remote_killable BOOL",
    )
    for clause in clauses:
        try:
            client.query(f"ALTER TABLE `{table_id}` {clause}").result()
        except Exception as exc:
            logger.debug("jobs table ALTER (%s): %s", clause, exc)


def _ensure_jobs_bq_usage_columns(client: bigquery.Client) -> None:
    """Add BigQuery cost rollup columns to orchestrator_monitoring.jobs (idempotent)."""
    table_id = f"{client.project}.orchestrator_monitoring.jobs"
    clauses = (
        "ADD COLUMN IF NOT EXISTS bq_total_bytes_billed INT64",
        "ADD COLUMN IF NOT EXISTS bq_total_bytes_processed INT64",
        "ADD COLUMN IF NOT EXISTS bq_total_slot_ms INT64",
        "ADD COLUMN IF NOT EXISTS bq_labeled_job_count INT64",
        "ADD COLUMN IF NOT EXISTS bq_estimated_cost_usd FLOAT64",
        "ADD COLUMN IF NOT EXISTS bq_usage_detail JSON",
    )
    for clause in clauses:
        try:
            client.query(f"ALTER TABLE `{table_id}` {clause}").result()
        except Exception as exc:
            logger.debug("jobs table ALTER (%s): %s", clause, exc)


def _to_naive_utc(dt: datetime) -> datetime:
    """
    Return dt as a naive UTC datetime (tzinfo stripped). A naive input is assumed to
    be host LOCAL time (that is what datetime.now() returns) and is converted to UTC;
    a tz-aware input is converted to UTC. BigQuery reads a naive TIMESTAMP parameter
    as UTC, so the returned value matches INFORMATION_SCHEMA.creation_time (UTC).
    """
    from datetime import timezone as _tz

    # .astimezone() treats a naive datetime as host-local and a tz-aware one by its
    # own zone, so both cases convert correctly to UTC.
    return dt.astimezone(_tz.utc).replace(tzinfo=None)


def attach_bigquery_usage_to_stats(
    stats: Optional[Dict[str, Any]],
    job_id: str,
    window_start: datetime,
    window_end: datetime,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Merge labeled BigQuery child-job usage into stats for update_job_status.

    Child jobs must set label ``orchestrator_job_id`` to this ``job_id`` UUID.
    Pricing uses ``monitoring.bigquery_estimated_cost_usd_per_tebibyte`` from ``config`` when provided.
    """
    from shared.bq_orchestrator_usage import fetch_labeled_bigquery_usage

    merged: Dict[str, Any] = dict(stats or {})
    try:
        # INFORMATION_SCHEMA.JOBS_BY_PROJECT.creation_time is UTC. Callers pass the
        # run window from datetime.now() (naive LOCAL time); on a non-UTC host that
        # shifts the window by the tz offset and matches ZERO child jobs -> cost was
        # always $0. Normalize the window to UTC so the label match works regardless
        # of host timezone. Naive datetimes are assumed to be LOCAL and converted;
        # tz-aware datetimes are converted to UTC and made naive (BigQuery reads a
        # naive TIMESTAMP param as UTC).
        window_start = _to_naive_utc(window_start)
        window_end = _to_naive_utc(window_end)
        client = get_bigquery_client()
        _ensure_jobs_bq_usage_columns(client)
        usage = fetch_labeled_bigquery_usage(
            client, client.project, job_id, window_start, window_end, config=config
        )
        merged.update(usage)
        if usage.get("bq_labeled_job_count", 0) == 0:
            logger.info(
                "BigQuery usage rollup: no finished jobs (end_time set) with label orchestrator_job_id=%s in window "
                "(SQL post-process + profile_database set this label; extend other extractors as needed).",
                job_id,
            )
        else:
            logger.info(
                "BigQuery usage rollup: labeled_jobs=%s bytes_billed=%s est_usd=%s bytes_processed=%s slot_ms=%s",
                usage["bq_labeled_job_count"],
                usage["bq_total_bytes_billed"],
                usage.get("bq_estimated_cost_usd"),
                usage["bq_total_bytes_processed"],
                usage["bq_total_slot_ms"],
            )
    except Exception as exc:
        logger.warning("BigQuery usage rollup failed (non-fatal): %s", exc)
    return merged


def _ensure_table_processing_table(client: bigquery.Client) -> None:
    # Create table processing tracking table if it doesn't exist.
    # Delegate to table_tracking module for actual creation
    from shared.table_tracking import ensure_table_processing_table

    ensure_table_processing_table(client)


def _ensure_schema_tracking_tables(client: bigquery.Client) -> None:
    # Create schema tracking tables if they don't exist.
    tables = {
        "schema_changes": [
            bigquery.SchemaField("change_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("change_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("field_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("field_type_before", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("field_type_after", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("field_mode_before", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("field_mode_after", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("applied", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("metadata", "JSON", mode="NULLABLE"),
        ],
        "schema_versions": [
            bigquery.SchemaField("version_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("schema_json", "JSON", mode="REQUIRED"),
            bigquery.SchemaField("execution_id", "STRING", mode="NULLABLE"),
        ],
    }

    for table_name, schema in tables.items():
        table_id = f"{client.project}.orchestrator_monitoring.{table_name}"
        try:
            client.get_table(table_id)
            logger.debug(f"Schema tracking table exists: {table_name}")
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="timestamp"
            )
            table.clustering_fields = ["source", "table_name"]
            client.create_table(table)
            logger.info(f"Created schema tracking table: {table_name}")


def _ensure_merge_locks_table(client: bigquery.Client) -> None:
    # Create merge locks table if it doesn't exist.
    table_id = f"{client.project}.orchestrator_monitoring.merge_locks"

    try:
        client.get_table(table_id)
        logger.debug(f"Merge locks table exists: {table_id}")
        return
    except Exception:
        pass

    # Create using SQL for simplicity
    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        lock_key STRING NOT NULL,
        job_id STRING,
        execution_id STRING,
        acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        expires_at TIMESTAMP
    )
    """

    try:
        client.query(create_query).result()
        logger.info("Created merge locks table")
    except Exception as e:
        logger.warning(f"Could not create merge locks table: {e}")


def _ensure_data_changes_table(client: bigquery.Client) -> None:
    """Create data_changes table for tracking merge statistics (inserts/updates/removes)."""
    table_id = f"{client.project}.orchestrator_monitoring.data_changes"

    try:
        client.get_table(table_id)
        logger.debug(f"Data changes table exists: {table_id}")
        return
    except Exception:
        pass

    # Create using SQL with partitioning and clustering
    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        change_timestamp TIMESTAMP NOT NULL,
        dataset STRING NOT NULL,
        table_name STRING NOT NULL,
        rows_inserted INT64,
        rows_updated INT64,
        rows_removed INT64,
        rows_total INT64,
        execution_id STRING
    )
    PARTITION BY DATE(change_timestamp)
    CLUSTER BY dataset, table_name
    """

    try:
        client.query(create_query).result()
        logger.info("Created data_changes monitoring table")
    except Exception as e:
        logger.warning(f"Could not create data_changes table: {e}")


def _ensure_legacy_pipeline_tables(client: bigquery.Client) -> None:
    # Create legacy pipeline tracking tables if they don't exist.
    tables = {
        "_pipeline_executions": [
            bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("completed_at", "TIMESTAMP"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("total_rows", "INT64"),
            bigquery.SchemaField("sites_processed", "INT64"),
        ],
        "_pipeline_checkpoints": [
            bigquery.SchemaField("checkpoint_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("execution_id", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("site", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("checkpoint_time", "TIMESTAMP"),
            bigquery.SchemaField("rows_processed", "INT64"),
        ],
    }

    for table_name, schema in tables.items():
        table_id = f"{client.project}.orchestrator_monitoring.{table_name}"
        try:
            client.get_table(table_id)
            logger.debug(f"Legacy pipeline table exists: {table_name}")
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            logger.info(f"Created legacy pipeline table: {table_name}")


def ensure_centralized_monitoring_dataset(client: bigquery.Client) -> None:
    """
    DEPRECATED: Use ensure_monitoring_infrastructure() instead.

    This function is kept for backward compatibility.
    """
    ensure_monitoring_infrastructure(client)


def ensure_centralized_jobs_table(client: bigquery.Client) -> None:
    """
    DEPRECATED: Use ensure_monitoring_infrastructure() instead.

    This function is kept for backward compatibility.
    """
    ensure_monitoring_infrastructure(client)


def create_centralized_job(
    source: str, job_name: str, command_line: str, config: Dict[str, Any]
) -> str:
    # Create new centralized job record with machine tracking
    client = get_bigquery_client()
    ensure_centralized_jobs_table(client)

    job_id = str(uuid.uuid4())
    execution_id = str(uuid.uuid4())

    # Extract job configuration
    tables = config.get("tables", [])
    sites = config.get("sites", [])
    groups = config.get("groups", [])
    refresh_mode = config.get("refresh_mode", "full")
    environment = config.get("environment", "production")
    triggered_by = config.get("triggered_by", "manual")
    parent_job_id = config.get("parent_job_id", None)  # For job chaining/lineage

    # Get machine information
    from shared.job_manager import get_machine_info

    machine_info = get_machine_info()

    table_id = f"{client.project}.orchestrator_monitoring.jobs"

    # Use parameterized query to avoid SQL injection and escaping issues
    query = f"""
    INSERT INTO `{table_id}` (
        job_id, execution_id, source, job_name, job_type, command_line,
        status, status_updated_at, queued_at, tables, sites, `groups`,
        refresh_mode, environment, triggered_by, parent_job_id,
        hostname, ip_address, os_type, os_version, python_version,
        process_id, parent_process_id, user, working_directory, is_remote_killable,
        created_at, updated_at
    ) VALUES (
        @job_id, @execution_id, @source, @job_name, @job_type, @command_line,
        @status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
        @tables, @sites, @groups,
        @refresh_mode, @environment, @triggered_by, @parent_job_id,
        @hostname, @ip_address, @os_type, @os_version, @python_version,
        @process_id, @parent_process_id, @user, @working_directory, @is_remote_killable,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id),
            bigquery.ScalarQueryParameter("source", "STRING", source),
            bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
            bigquery.ScalarQueryParameter("job_type", "STRING", "extraction"),
            bigquery.ScalarQueryParameter("command_line", "STRING", command_line),
            bigquery.ScalarQueryParameter("status", "STRING", "QUEUED"),
            bigquery.ArrayQueryParameter("tables", "STRING", tables),
            bigquery.ArrayQueryParameter("sites", "STRING", sites),
            bigquery.ArrayQueryParameter("groups", "STRING", groups),
            bigquery.ScalarQueryParameter("refresh_mode", "STRING", refresh_mode),
            bigquery.ScalarQueryParameter("environment", "STRING", environment),
            bigquery.ScalarQueryParameter("triggered_by", "STRING", triggered_by),
            bigquery.ScalarQueryParameter("parent_job_id", "STRING", parent_job_id),
            bigquery.ScalarQueryParameter(
                "hostname", "STRING", machine_info["hostname"]
            ),
            bigquery.ScalarQueryParameter(
                "ip_address", "STRING", machine_info["ip_address"]
            ),
            bigquery.ScalarQueryParameter("os_type", "STRING", machine_info["os_type"]),
            bigquery.ScalarQueryParameter(
                "os_version", "STRING", machine_info["os_version"]
            ),
            bigquery.ScalarQueryParameter(
                "python_version", "STRING", machine_info["python_version"]
            ),
            bigquery.ScalarQueryParameter(
                "process_id", "INT64", machine_info["process_id"]
            ),
            bigquery.ScalarQueryParameter(
                "parent_process_id", "INT64", machine_info["parent_process_id"]
            ),
            bigquery.ScalarQueryParameter("user", "STRING", machine_info["user"]),
            bigquery.ScalarQueryParameter(
                "working_directory", "STRING", machine_info["working_directory"]
            ),
            bigquery.ScalarQueryParameter(
                "is_remote_killable", "BOOL", machine_info["is_remote_killable"]
            ),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        logger.info(
            f"Created job {job_id} on {machine_info['hostname']} (PID: {machine_info['process_id']})"
        )
        return job_id
    except Exception as e:
        logger.error(f"Failed to create centralized job: {e}")
        return job_id  # Return job_id even if DB write fails


def update_job_status(
    job_id: str,
    status: str,
    error_message: str = None,
    stats: Dict[str, Any] = None,
    duration_seconds: int = None,
) -> bool:
    # Update job status in centralized table
    client = get_bigquery_client()
    table_id = f"{client.project}.orchestrator_monitoring.jobs"

    # Build update query based on status
    if status == "RUNNING":
        query = f"""
        UPDATE `{table_id}`
        SET status = @status,
            status_updated_at = CURRENT_TIMESTAMP(),
            started_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_id = @job_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            ]
        )
    elif status in ["COMPLETED", "FAILED", "CANCELLED", "WARNING"]:
        # Extract stats for completed jobs
        total_rows = stats.get("total_rows", 0) if stats else 0
        sites_processed = stats.get("sites", 0) if stats else 0
        tables_processed = stats.get("tables", 0) if stats else 0

        # Build SET clause dynamically
        set_parts = [
            "status = @status",
            "status_updated_at = CURRENT_TIMESTAMP()",
            "completed_at = CURRENT_TIMESTAMP()",
            "total_rows = @total_rows",
            "sites_processed = @sites_processed",
            "tables_processed = @tables_processed",
        ]
        params = [
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("total_rows", "INT64", total_rows),
            bigquery.ScalarQueryParameter("sites_processed", "INT64", sites_processed),
            bigquery.ScalarQueryParameter(
                "tables_processed", "INT64", tables_processed
            ),
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
        ]

        if error_message:
            set_parts.append("error_message = @error_message")
            params.append(
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message)
            )

        if duration_seconds is not None:
            set_parts.append("duration_seconds = @duration_seconds")
            params.append(
                bigquery.ScalarQueryParameter(
                    "duration_seconds", "INT64", int(duration_seconds)
                )
            )

        if stats:
            for col in (
                "bq_total_bytes_billed",
                "bq_total_bytes_processed",
                "bq_total_slot_ms",
                "bq_labeled_job_count",
            ):
                if stats.get(col) is not None:
                    set_parts.append(f"{col} = @{col}")
                    params.append(
                        bigquery.ScalarQueryParameter(col, "INT64", int(stats[col]))
                    )
            if stats.get("bq_estimated_cost_usd") is not None:
                set_parts.append("bq_estimated_cost_usd = @bq_estimated_cost_usd")
                params.append(
                    bigquery.ScalarQueryParameter(
                        "bq_estimated_cost_usd",
                        "FLOAT64",
                        float(stats["bq_estimated_cost_usd"]),
                    )
                )
            if stats.get("bq_usage_detail") is not None:
                set_parts.append("bq_usage_detail = SAFE.PARSE_JSON(@bq_usage_detail)")
                params.append(
                    bigquery.ScalarQueryParameter(
                        "bq_usage_detail", "STRING", str(stats["bq_usage_detail"])
                    )
                )

        set_parts.append("updated_at = CURRENT_TIMESTAMP()")

        query = f"""
        UPDATE `{table_id}`
        SET {", ".join(set_parts)}
        WHERE job_id = @job_id
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
    else:
        query = f"""
        UPDATE `{table_id}`
        SET status = @status,
            status_updated_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_id = @job_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            ]
        )

    try:
        if "job_config" in locals():
            client.query(query, job_config=job_config).result()
        else:
            client.query(query).result()
        return True
    except Exception as e:
        logger.error(f"Failed to update job status: {e}")
        return False


def get_local_log_path(job_id: str) -> str:
    # Get local file path for job log
    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    return os.path.join(logs_dir, f"{job_id}.log")


def append_job_log_to_file(job_id: str, log_message: str) -> bool:
    # Append log message to local log file
    try:
        log_path = get_local_log_path(job_id)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(log_message)
            if not log_message.endswith("\n"):
                f.write("\n")
        return True
    except Exception as e:
        logger.error(f"Failed to write to log file: {e}")
        return False


def update_job_log_path(job_id: str, log_path: str) -> bool:
    """Update orchestrator_monitoring.jobs with the latest log file path."""
    try:
        client = get_bigquery_client()
        table_id = f"{client.project}.orchestrator_monitoring.jobs"
        query = f"""
        UPDATE `{table_id}`
        SET log_file_path = @log_path,
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_id = @job_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("log_path", "STRING", log_path),
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            ]
        )
        client.query(query, job_config=job_config).result()
        return True
    except Exception as update_err:
        logger.warning(f"Failed to update log_file_path for job {job_id}: {update_err}")
        return False


def upload_job_log_to_gcs(job_id: str, bucket_name: str = None) -> str:
    """
    Upload job log file to GCS and return GCS path.

    Args:
        job_id: Job identifier
        bucket_name: GCS bucket name (if None, reads from env STAGING_BUCKET)

    Returns:
        GCS path (gs://bucket/logs/job_id.log) or empty string on failure
    """
    try:
        from google.cloud import storage

        # Get bucket name from env if not provided
        if not bucket_name:
            bucket_name = os.getenv("STAGING_BUCKET")
            if not bucket_name:
                logger.error("No GCS bucket specified for log upload")
                return ""

        # Get local log file path
        local_path = get_local_log_path(job_id)
        if not os.path.exists(local_path):
            logger.warning(f"Log file not found: {local_path}")
            return ""

        # Upload to GCS: gs://bucket/logs/{job_id}.log
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"logs/{job_id}.log")

        blob.upload_from_filename(local_path)

        gcs_path = f"gs://{bucket_name}/logs/{job_id}.log"
        logger.info(f"Uploaded job log to {gcs_path}")
        return gcs_path

    except Exception as e:
        logger.error(f"Failed to upload log to GCS: {e}")
        return ""


# Full logs are uploaded to GCS via capture_extractor_logs() which calls
# upload_job_log_to_gcs() and updates log_file_path with the GCS URI.


def get_job_status(job_id: str) -> Dict[str, Any]:
    # Get current job status and details
    client = get_bigquery_client()
    table_id = f"{client.project}.orchestrator_monitoring.jobs"

    query = f"""
    SELECT * FROM `{table_id}` WHERE job_id = @job_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    )

    try:
        result = client.query(query, job_config=job_config).result()
        for row in result:
            return dict(row)
        return {}
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        return {}


def capture_extractor_logs(
    job_id: str, extractor_function, config: Dict[str, Any] = None, *args, **kwargs
) -> Dict[str, Any]:
    """
    Capture all output from extractor function and save to local log file.

    Logs are written to: logs/{job_id}.log
    After completion/failure, logs are uploaded to GCS and BigQuery is updated.

    Args:
        job_id: Job identifier
        extractor_function: Function to execute and capture logs from
        config: Pipeline configuration (optional, for GCS bucket name)
        *args, **kwargs: Arguments to pass to extractor function

    Returns:
        Result from extractor function
    """
    import sys
    import io
    import traceback
    from contextlib import redirect_stdout, redirect_stderr

    local_log_path = get_local_log_path(job_id)
    try:
        Path(local_log_path).touch(exist_ok=True)
    except Exception as touch_err:
        logger.warning(f"Unable to initialize log file for job {job_id}: {touch_err}")
    else:
        update_job_log_path(job_id, local_log_path)

    log_buffer = []

    # Custom log handler to capture logging and write to file
    class LogCaptureHandler(logging.Handler):
        def __init__(self, buffer, job_id):
            super().__init__()
            self.buffer = buffer
            self.job_id = job_id

        def emit(self, record):
            log_line = self.format(record)
            self.buffer.append(log_line)
            # Write to file immediately
            append_job_log_to_file(self.job_id, log_line)

    # Setup log capture
    log_handler = LogCaptureHandler(log_buffer, job_id)
    log_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    # Add to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)

    try:
        # Tee stream: captures output AND writes to the real terminal in real-time
        class TeeStream:
            def __init__(self, original, buffer):
                self.original = original
                self.buffer = buffer

            def write(self, data):
                self.buffer.write(data)
                self.original.write(data)
                self.original.flush()

            def flush(self):
                self.buffer.flush()
                self.original.flush()

        # Capture stdout and stderr while still printing to terminal
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        real_stdout = sys.stdout
        real_stderr = sys.stderr

        with (
            redirect_stdout(TeeStream(real_stdout, stdout_capture)),
            redirect_stderr(TeeStream(real_stderr, stderr_capture)),
        ):
            # Call extractor function
            result = extractor_function(*args, **kwargs)

        # Collect all captured output
        stdout_output = stdout_capture.getvalue()
        stderr_output = stderr_capture.getvalue()

        # Write stdout/stderr to log file
        if stdout_output:
            append_job_log_to_file(job_id, f"STDOUT: {stdout_output}")
        if stderr_output:
            append_job_log_to_file(job_id, f"STDERR: {stderr_output}")

        # Upload logs to GCS and update BigQuery
        # Get bucket name from config or environment
        bucket_name = None
        if config:
            # Try multiple config paths: top-level gcs_bucket, pipeline.gcs_bucket, gcs.bucket
            bucket_name = (
                config.get("gcs_bucket")
                or config.get("pipeline", {}).get("gcs_bucket")
                or config.get("gcs", {}).get("bucket")
            )
        if not bucket_name:
            bucket_name = os.getenv("STAGING_BUCKET") or os.getenv("GCS_BUCKET")

        gcs_path = upload_job_log_to_gcs(job_id, bucket_name)

        # Update job record with GCS log path
        if gcs_path:
            if update_job_log_path(job_id, gcs_path):
                logger.info(f"Updated job {job_id} with log path: {gcs_path}")

        return result

    except Exception as e:
        # Capture exception in logs
        error_log = f"EXCEPTION: {str(e)}\n{traceback.format_exc()}"
        append_job_log_to_file(job_id, error_log)

        # Upload logs to GCS even on failure (especially important for debugging!)
        # Get bucket name from config or environment
        bucket_name = None
        if config:
            # Try multiple config paths: top-level gcs_bucket, pipeline.gcs_bucket, gcs.bucket
            bucket_name = (
                config.get("gcs_bucket")
                or config.get("pipeline", {}).get("gcs_bucket")
                or config.get("gcs", {}).get("bucket")
            )
        if not bucket_name:
            bucket_name = os.getenv("STAGING_BUCKET") or os.getenv("GCS_BUCKET")

        gcs_path = upload_job_log_to_gcs(job_id, bucket_name)

        # Update job record with GCS log path (even on failure - important for debugging!)
        if gcs_path:
            if update_job_log_path(job_id, gcs_path):
                logger.info(f"Updated failed job {job_id} with log path: {gcs_path}")

        raise
    finally:
        # Cleanup log handler
        root_logger.removeHandler(log_handler)


def _generate_recovery_commands(
    source: str,
    group: Optional[str],
    table: Optional[str],
    lookback: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    error_message: str,
    failed_tables: List[str],
) -> str:
    """
    Generate smart recovery commands based on failure context.

    Args:
        source: Data source (e.g., 'mailchimp', 'wordpress')
        group: Table group if specified
        table: Specific table if specified
        lookback: Lookback days if specified
        start_date: Start date if specified
        end_date: End date if specified
        error_message: The error message
        failed_tables: List of failed table names from BigQuery monitoring

    Returns:
        Formatted recovery commands section for email
    """
    commands = []

    # Build base command
    base_cmd = f"python orchestrate.py --source {source} --env prod"

    # Detect failure patterns for smart recommendations
    _msg_lower = error_message.lower()
    # Schema DRIFT is benign: an upstream column was auto-added and the run merely
    # WARNED (see schema_evolution.py "SCHEMA DRIFT:" marker). It must NOT be treated
    # as a schema ERROR -- suggesting a full rebuild there is wrong and destructive.
    is_schema_drift = "schema drift" in _msg_lower
    is_schema_error = (not is_schema_drift) and any(
        keyword in _msg_lower
        for keyword in ["schema", "column", "field", "type mismatch", "data type"]
    )
    is_rate_limit = any(
        keyword in error_message.lower()
        for keyword in ["rate limit", "429", "too many requests", "503"]
    )
    is_memory_error = any(
        keyword in error_message.lower()
        for keyword in ["memory", "memoryerror", "allocation failed"]
    )
    is_api_error = any(
        keyword in error_message.lower()
        for keyword in ["api", "connection", "timeout", "network"]
    )

    # Check for failure manifest availability
    from pathlib import Path

    manifest_dir = Path("failures")
    has_manifests = False
    if manifest_dir.exists():
        manifests = list(manifest_dir.glob(f"{source}_*.json"))
        has_manifests = len(manifests) > 0

    recovery_text = "\nRECOVERY COMMANDS:\n"
    recovery_text += "=" * 80 + "\n\n"

    # Provide context-specific recommendations
    if is_schema_drift:
        recovery_text += "SCHEMA DRIFT (informational -- no action required)\n"
        recovery_text += (
            "An upstream source added a new column and it was auto-added to the "
            "production table. The run succeeded. Verify the new column is an intended "
            "upstream change. Do NOT rebuild.\n\n"
        )
    elif is_schema_error:
        recovery_text += "⚠️  SCHEMA ERROR DETECTED\n"
        recovery_text += (
            "Schema mismatches require a full table rebuild to apply new schema.\n\n"
        )
    elif is_rate_limit:
        recovery_text += "⚠️  API RATE LIMIT DETECTED\n"
        recovery_text += (
            "Wait 5-10 minutes before retrying, or use a longer lookback period.\n\n"
        )
    elif is_memory_error:
        recovery_text += "⚠️  MEMORY ERROR DETECTED\n"
        recovery_text += "Chunked processing should handle this automatically. If persists, contact support.\n\n"
    elif is_api_error:
        recovery_text += "⚠️  API/NETWORK ERROR DETECTED\n"
        recovery_text += "Transient connection issues. Retry should resolve this.\n\n"

    # Option 1: Retry with exact same parameters
    retry_cmd = base_cmd
    if group:
        retry_cmd += f" --group {group}"
    if table:
        retry_cmd += f" --table {table}"
    if lookback:
        retry_cmd += f" --lookback {lookback}"
    if start_date and end_date:
        retry_cmd += f" --start-date {start_date} --end-date {end_date}"

    commands.append(
        {
            "title": "1. Retry with Same Parameters",
            "description": "Re-run the job with identical settings (for transient errors)",
            "command": retry_cmd,
        }
    )

    # Option 2: Retry specific failed tables (if multiple tables and some failed)
    if failed_tables and len(failed_tables) > 0:
        for idx, failed_table in enumerate(
            failed_tables[:3], start=2
        ):  # Show max 3 tables
            table_cmd = f"{base_cmd} --table {failed_table}"
            if lookback:
                table_cmd += f" --lookback {lookback}"
            if start_date and end_date:
                table_cmd += f" --start-date {start_date} --end-date {end_date}"

            commands.append(
                {
                    "title": f"{idx}. Retry Single Table: {failed_table}",
                    "description": "Process only this specific table",
                    "command": table_cmd,
                }
            )

        if len(failed_tables) > 3:
            commands.append(
                {
                    "title": f"{len(failed_tables) - 3} more tables failed",
                    "description": f"See BigQuery table_processing for full list",
                    "command": None,
                }
            )

    # Option 3: Smart retry using failure manifest (if available)
    if has_manifests:
        retry_manifest_cmd = f"{base_cmd} --retry"
        commands.append(
            {
                "title": f"{len(commands) + 1}. Smart Retry (Failure Manifest)",
                "description": "Retry only the specific items that failed (recommended for partial failures)",
                "command": retry_manifest_cmd,
            }
        )

    # Option 4: Full rebuild (for schema errors)
    if is_schema_error and (table or (failed_tables and len(failed_tables) == 1)):
        target_table = table or failed_tables[0]
        rebuild_cmd = f"{base_cmd} --table {target_table} --rebuild"
        if lookback:
            rebuild_cmd += f" --lookback {lookback}"

        commands.append(
            {
                "title": f"{len(commands) + 1}. Full Rebuild (DANGEROUS - Schema Fix)",
                "description": "⚠️  Deletes and rebuilds the table. Use for schema changes only.",
                "command": rebuild_cmd,
            }
        )

    # Format commands for email
    for cmd_info in commands:
        recovery_text += f"{cmd_info['title']}\n"
        recovery_text += f"   {cmd_info['description']}\n"
        if cmd_info["command"]:
            recovery_text += f"   $ {cmd_info['command']}\n"
        recovery_text += "\n"

    recovery_text += (
        "\nNOTE: Run commands from the orchestrator directory: cd /home/orchestrator\n"
    )
    recovery_text += "=" * 80 + "\n"

    return recovery_text


# Map each severity to (subject tag, subject summary, email body header). INFO
# and WARNING are not failures -- the data loaded fine, the operator is just
# being told something changed (e.g. schema drift auto-absorbed). Keeping this
# explicit means every alert subject states the severity up front.
# Job Monitor dashboard, linked from every alert email so the reader can jump
# straight to live job status instead of querying BigQuery by hand.
# Env-overridable for dev/staging deployments of the dashboard.
JOB_MONITOR_URL = os.getenv("JOB_MONITOR_URL", "https://job-monitor.bionews.com/")

_SEVERITY_LABELS = {
    "FAILURE": ("FAILED", "Pipeline Failed", "Pipeline Job Failure Alert"),
    "PARTIAL": (
        "PARTIAL FAILURE",
        "Pipeline Partially Failed",
        "Pipeline Job Partial-Failure Alert",
    ),
    "WARNING": (
        "WARNING",
        "Pipeline Completed with Warnings",
        "Pipeline Job Warning",
    ),
    "INFO": (
        "INFO",
        "Pipeline Completed (informational notice)",
        "Pipeline Job Notification",
    ),
}


def send_job_failure_alert(
    job_id: str,
    source: str,
    error_message: str,
    config: Dict[str, Any],
    group: Optional[str] = None,
    table: Optional[str] = None,
    lookback: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    severity: str = "FAILURE",
) -> bool:
    # Send email alert for a job. `severity` is one of FAILURE / PARTIAL /
    # WARNING / INFO and drives the subject prefix + body header so the reader
    # can tell a total failure from an informational notice at a glance.
    try:
        # Get alerting configuration
        alert_config = config.get("alerting", {})
        if not alert_config.get("enabled", True):
            logger.info("Alerting is disabled, skipping email alert")
            return False

        # Get email configuration
        email_config = alert_config.get("email", {})
        smtp_server = email_config.get("smtp_server", "smtp.gmail.com")
        smtp_port = email_config.get("smtp_port", 587)
        sender_email = email_config.get("sender_email", os.getenv("ALERT_SENDER_EMAIL"))
        sender_password = email_config.get(
            "sender_password", os.getenv("ALERT_SENDER_PASSWORD")
        )
        recipient_email = email_config.get(
            "recipient_email", os.getenv("ALERT_RECIPIENT_EMAIL")
        )

        if not all([sender_email, sender_password, recipient_email]):
            logger.error("Missing email configuration for alerts")
            return False

        # Try to get table tracking information and log content for this job
        table_tracking_info = ""
        log_section = ""
        client = None
        failed_tables = []
        try:
            from shared.bigquery_client import get_bigquery_client

            client = get_bigquery_client()

            # Query table_processing for tables related to this job
            query = f"""
            SELECT
                table_name,
                status,
                phase,
                rows_inserted,
                rows_updated,
                staging_rows,
                error_message
            FROM `{client.project}.orchestrator_monitoring.table_processing`
            WHERE job_id = @job_id
            ORDER BY created_at DESC
            """

            tp_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
                ]
            )
            result = client.query(query, job_config=tp_config).result()
            rows = list(result)

            if rows:
                table_tracking_info = "\n\nTable Processing Details:\n"
                table_tracking_info += "-" * 40 + "\n"
                for row in rows:
                    table_tracking_info += f"Table: {row.table_name}\n"
                    table_tracking_info += f"  Status: {row.status}\n"
                    table_tracking_info += f"  Phase: {row.phase or 'unknown'}\n"
                    if row.staging_rows:
                        table_tracking_info += f"  Staging rows: {row.staging_rows:,}\n"
                    if row.rows_inserted or row.rows_updated:
                        table_tracking_info += f"  Inserted: {row.rows_inserted or 0:,}, Updated: {row.rows_updated or 0:,}\n"
                    if row.error_message:
                        table_tracking_info += f"  Error: {row.error_message[:200]}\n"
                    table_tracking_info += "\n"

                    # Track failed tables for recovery commands
                    if row.status in ("FAILED", "WARNING"):
                        failed_tables.append(row.table_name)

                table_tracking_info += (
                    "\nNote: Table tracking data may have 30-90 min visibility lag.\n"
                )
                table_tracking_info += (
                    "Check application logs for most recent table details.\n"
                )
        except Exception as e:
            logger.debug(f"Could not fetch table tracking info: {e}")
            # Not critical if we can't get this data

        # Retrieve job log path and embed full log content
        try:
            log_path = None
            if client:
                log_query = f"""
                SELECT log_file_path
                FROM `{client.project}.orchestrator_monitoring.jobs`
                WHERE job_id = @job_id
                ORDER BY status_updated_at DESC
                LIMIT 1
                """

                log_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
                    ]
                )

                log_result = client.query(log_query, job_config=log_config).result()
                for row in log_result:
                    log_path = row.log_file_path
                    break

            if log_path:
                log_content = _fetch_log_content(log_path)
                if log_content is not None:
                    log_section = f"\nFull Job Log ({log_path}):\n{'-' * 80}\n{log_content}\n{'-' * 80}\n"
                else:
                    log_section = f"\nFull Job Log:\nUnable to retrieve content. Log path: {log_path}\n"
            else:
                log_section = "\nFull Job Log:\nLog path unavailable for this job.\n"
        except Exception as log_exc:
            logger.debug(f"Failed to attach log content: {log_exc}")
            if log_section == "":
                log_section = (
                    "\nFull Job Log:\nUnable to retrieve log content due to an error.\n"
                )

        # Generate smart recovery commands
        recovery_section = _generate_recovery_commands(
            source,
            group,
            table,
            lookback,
            start_date,
            end_date,
            error_message,
            failed_tables,
        )

        # Subject + body header are driven by severity so the reader can tell a
        # total failure from an informational notice without opening the email.
        subject_tag, subject_summary, body_header = _SEVERITY_LABELS.get(
            severity, _SEVERITY_LABELS["FAILURE"]
        )
        # INFO/WARNING are not failures, so don't say "Failed at"; the rest did fail.
        timestamp_label = (
            "Reported at" if severity in ("INFO", "WARNING") else "Failed at"
        )
        detail_label = "Details" if severity in ("INFO", "WARNING") else "Error Message"
        subject = f"[{source.upper()}] [{subject_tag}] {subject_summary}"
        body = f"""
{body_header}
========================

Severity: {subject_tag}

Job Details:
- Job ID: {job_id}
- Source: {source}
- {timestamp_label}: {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

Job Monitor:
{JOB_MONITOR_URL}

{detail_label}:
{error_message}
{table_tracking_info}
{log_section}

{recovery_section}

Job Logs Location:
bi-data-391216.orchestrator_monitoring.jobs
WHERE job_id = '{job_id}'

Table Tracking:
bi-data-391216.orchestrator_monitoring.table_processing
WHERE job_id = '{job_id}'

This is an automated alert from the Data Pipeline Orchestrator.
"""

        # Create text-based email message
        msg = MIMEText(body, "plain")
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = subject

        # Send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()

        logger.info(f"Sent failure alert email for job {job_id} to {recipient_email}")
        return True

    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        return False


def send_job_success_alert(
    job_id: str, source: str, stats: Dict[str, Any], config: Dict[str, Any]
) -> bool:
    # Send email alert for successful job (optional)
    try:
        # Get alerting configuration
        alert_config = config.get("alerting", {})
        if not alert_config.get("enabled", True):
            return False

        # Check if success alerts are enabled
        if not alert_config.get("on_success", False):
            return False

        # Get email configuration
        email_config = alert_config.get("email", {})
        smtp_server = email_config.get("smtp_server", "smtp.gmail.com")
        smtp_port = email_config.get("smtp_port", 587)
        sender_email = email_config.get("sender_email", os.getenv("ALERT_SENDER_EMAIL"))
        sender_password = email_config.get(
            "sender_password", os.getenv("ALERT_SENDER_PASSWORD")
        )
        recipient_email = email_config.get(
            "recipient_email", os.getenv("ALERT_RECIPIENT_EMAIL")
        )

        if not all([sender_email, sender_password, recipient_email]):
            return False

        # Create email content with consistent subject line format
        subject = f"[{source.upper()}] Pipeline Success"
        body = f"""
Pipeline Job Success Alert
========================

Job Details:
- Job ID: {job_id}
- Source: {source}
- Completed at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

Job Monitor:
{JOB_MONITOR_URL}

Results:
- Total rows processed: {stats.get("total_rows", 0):,}
- Sites processed: {stats.get("sites", 0)}
- Tables processed: {stats.get("tables", 0)}
- Duration: {stats.get("duration_seconds", 0)} seconds

Job Details Location:
bi-data-391216.orchestrator_monitoring.jobs
WHERE job_id = '{job_id}'

This is an automated alert from the Data Pipeline Orchestrator.
"""

        # Create text-based email message
        msg = MIMEText(body, "plain")
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = subject

        # Send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()

        logger.info(f"Sent success alert email for job {job_id} to {recipient_email}")
        return True

    except Exception as e:
        logger.error(f"Failed to send success alert email: {e}")
        return False


# Note: get_bigquery_client is now imported from shared.bigquery_client
# Import at top of file instead of wrapper function here


def ensure_monitoring_tables(client: bigquery.Client, dataset: str) -> None:
    """
    DEPRECATED: Use ensure_monitoring_infrastructure() instead.

    This function is kept for backward compatibility.
    Note: The dataset parameter is ignored - all monitoring tables
    are created in orchestrator_monitoring dataset.
    """
    ensure_monitoring_infrastructure(client)


def _ensure_pipeline_tracking_tables(client: bigquery.Client, dataset: str) -> None:
    tables = {
        "_pipeline_executions": [
            bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("completed_at", "TIMESTAMP"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("total_rows", "INT64"),
            bigquery.SchemaField("sites_processed", "INT64"),
        ],
        "_pipeline_checkpoints": [
            bigquery.SchemaField("checkpoint_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("execution_id", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("site", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("checkpoint_time", "TIMESTAMP"),
            bigquery.SchemaField("rows_processed", "INT64"),
        ],
    }

    for table_name, schema in tables.items():
        table_id = f"{client.project}.{dataset}.{table_name}"
        try:
            client.get_table(table_id)
        except Exception:
            try:
                client.create_table(bigquery.Table(table_id, schema=schema))
                logger.info(f"Created pipeline tracking table: {table_id}")
            except Exception as exc:
                logger.warning(
                    f"Could not create pipeline tracking table {table_id}: {exc}"
                )


def start_execution(
    client: bigquery.Client,
    source: str,
    tables: List[str],
    mode: str,
    config: Dict[str, Any],
) -> str:
    execution_id = str(uuid.uuid4())
    # Legacy pipeline tables are always in orchestrator_monitoring dataset
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )

    ensure_monitoring_tables(client, dataset)

    table_id = f"{client.project}.{dataset}._pipeline_executions"

    # Use regular INSERT query instead of streaming insert
    query = f"""
    INSERT INTO `{table_id}` (execution_id, source, status, started_at)
    VALUES ('{execution_id}', '{source}', 'running', CURRENT_TIMESTAMP())
    """

    try:
        client.query(query).result()
    except Exception as e:
        logger.error(f"Error starting execution tracking: {e}")

    return execution_id


def complete_execution(
    client: bigquery.Client,
    execution_id: str,
    stats: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    # Legacy pipeline tables are always in orchestrator_monitoring dataset
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )
    table_id = f"{client.project}.{dataset}._pipeline_executions"

    # Use MERGE instead of UPDATE to avoid streaming buffer issues
    query = f"""
    MERGE `{table_id}` T
    USING (
        SELECT '{execution_id}' as execution_id,
               'completed' as status,
               CURRENT_TIMESTAMP() as completed_at,
               {stats.get("total_rows", 0)} as total_rows,
               {stats.get("sites", 0)} as sites_processed
    ) S
    ON T.execution_id = S.execution_id
    WHEN MATCHED THEN
        UPDATE SET 
            status = S.status,
            completed_at = S.completed_at,
            total_rows = S.total_rows,
            sites_processed = S.sites_processed
    WHEN NOT MATCHED THEN
        INSERT (execution_id, source, status, started_at, completed_at, total_rows, sites_processed)
        VALUES (S.execution_id, '{config.get("source", {}).get("type", "unknown")}', S.status, CURRENT_TIMESTAMP(), S.completed_at, S.total_rows, S.sites_processed)
    """

    try:
        client.query(query).result()
    except Exception as e:
        logger.error(f"Error completing execution: {e}")


def fail_execution(
    client: bigquery.Client,
    execution_id: str,
    error_message: str,
    config: Dict[str, Any],
) -> None:
    # Legacy pipeline tables are always in orchestrator_monitoring dataset
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )
    table_id = f"{client.project}.{dataset}._pipeline_executions"

    # Collapse newlines and cap length; parameterization handles quotes/backslashes
    # safely (the old manual escaping could break the UPDATE and lose the failure
    # record on an error message containing an odd quote or backslash).
    error_message = error_message.replace("\n", " ")[:1000]

    query = f"""
    UPDATE `{table_id}`
    SET status = 'failed',
        completed_at = CURRENT_TIMESTAMP(),
        error_message = @error_message
    WHERE execution_id = @execution_id
    """
    fail_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id),
        ]
    )

    try:
        client.query(query, job_config=fail_config).result()
    except Exception as e:
        logger.error(f"Error recording failure: {e}")


def save_checkpoint_original(
    client: bigquery.Client,
    execution_id: str,
    source: str,
    site: str,
    table: str,
    rows: int,
    config: Dict[str, Any],
) -> None:
    # Legacy pipeline tables are always in orchestrator_monitoring dataset
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )
    table_id = f"{client.project}.{dataset}._pipeline_checkpoints"

    row = {
        "checkpoint_id": str(uuid.uuid4()),
        "execution_id": execution_id,
        "source": source,
        "site": site,
        "table_name": table,
        "checkpoint_time": datetime.utcnow().isoformat(),
        "rows_processed": rows,
    }

    try:
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            logger.error(f"Error saving checkpoint: {errors}")
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")


def get_last_checkpoint(
    client: bigquery.Client, checkpoint_id: str, config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    # Legacy pipeline tables are always in orchestrator_monitoring dataset
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )
    table_id = f"{client.project}.{dataset}._pipeline_checkpoints"

    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE checkpoint_id = '{checkpoint_id}'
    """

    try:
        results = client.query(query).result()
        for row in results:
            return dict(row)
    except Exception as e:
        logger.debug(f"Could not get execution status: {e}")

    return None


def get_execution_history(
    client: bigquery.Client, source: str, config: Dict[str, Any], days: int = 7
) -> List[Dict[str, Any]]:
    # Legacy pipeline tables are always in orchestrator_monitoring dataset
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )
    table_id = f"{client.project}.{dataset}._pipeline_executions"

    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE source = '{source}'
      AND started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    ORDER BY started_at DESC
    """

    results = []
    try:
        for row in client.query(query).result():
            results.append(dict(row))
    except Exception as e:
        logger.debug(f"Could not get execution history: {e}")

    return results


# Add the missing init_monitoring function
def init_monitoring(config: Dict[str, Any]) -> str:
    # Initialize monitoring and return an execution ID
    try:
        client = get_bigquery_client()
        source = config.get("source", {}).get("name", "unknown")
        tables = list(config.get("resources", {}).keys())
        mode = config.get("pipeline", {}).get("mode", "incremental")

        return start_execution(client, source, tables, mode, config)
    except Exception as e:
        print(f"Error initializing monitoring: {e}")
        return str(uuid.uuid4())  # Return a dummy execution ID if monitoring fails


#
# Smart Alerting Functions
#
def should_send_alert(
    execution_id: str,
    error_type: str,
    source: str,
    client: bigquery.Client,
    dataset: str,
) -> bool:
    # Determine if alert should be sent based on smart alerting rules
    try:
        # Check if this is a retry (same error within last hour)
        query = f"""
        SELECT COUNT(*) as retry_count
        FROM `{client.project}.{dataset}._pipeline_executions`
        WHERE source = @source
          AND error_message LIKE @error_pattern
          AND started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
          AND execution_id != @execution_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter(
                    "error_pattern", "STRING", f"%{error_type}%"
                ),
                bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id),
            ]
        )

        result = client.query(query, job_config=job_config).result()
        retry_count = list(result)[0].retry_count

        # Get source-specific threshold (configurable)
        thresholds = {
            "facebook": 2,  # Alert after 2 failures
            "wordpress": 3,  # Alert after 3 failures
            "generic": 2,  # Default threshold
            "default": 3,  # Fallback threshold
        }

        threshold = thresholds.get(source, thresholds["default"])

        # Only send alert if this is the threshold failure
        return retry_count >= threshold - 1

    except Exception as e:
        logger.error(f"Error checking alert conditions: {e}")
        return True  # Default to sending alert if check fails


def get_alert_severity(source: str, error_type: str, retry_count: int) -> str:
    # Determine alert severity based on source and error type
    # Critical errors that should always be high severity
    critical_errors = ["connection", "authentication", "quota", "permission"]

    if any(critical in error_type.lower() for critical in critical_errors):
        return "high"

    # Source-specific severity rules (configurable)
    # Default thresholds can be overridden in config
    default_thresholds = {"facebook": 2, "wordpress": 3, "generic": 3}

    threshold = default_thresholds.get(source, 3)
    if retry_count >= threshold:
        return "high"

    return "medium"


def create_alert_deduplication_key(source: str, error_type: str) -> str:
    # Create a key for alert deduplication
    import hashlib

    key = f"{source}:{error_type}"
    return hashlib.md5(key.encode()).hexdigest()[:8]


def log_alert_sent(
    client: bigquery.Client,
    dataset: str,
    alert_key: str,
    execution_id: str,
    source: str,
    severity: str,
) -> None:
    # Log that an alert was sent to prevent duplicates
    try:
        # Ensure alert log table exists
        table_id = f"{client.project}.{dataset}._alert_log"

        # Create table if it doesn't exist
        try:
            client.get_table(table_id)
        except Exception as e:
            logger.debug(f"Alert log table not found, creating: {e}")
            schema = [
                bigquery.SchemaField("alert_key", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("severity", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("sent_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("error_type", "STRING"),
            ]
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)

        # Insert alert log
        query = f"""
        INSERT INTO `{client.project}.{dataset}._alert_log`
        (alert_key, execution_id, source, severity, sent_at, error_type)
        VALUES (@alert_key, @execution_id, @source, @severity, CURRENT_TIMESTAMP(), @error_type)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("alert_key", "STRING", alert_key),
                bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id),
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("severity", "STRING", severity),
                bigquery.ScalarQueryParameter(
                    "error_type", "STRING", "pipeline_failure"
                ),
            ]
        )

        client.query(query, job_config=job_config).result()

    except Exception as e:
        logger.error(f"Error logging alert: {e}")


#
# Notification Functions
#


# Email and Slack notification functions removed - now using shared/notifications.py
# Legacy wrapper for backward compatibility
def send_notification(
    event_type: str, message: str, config: Dict[str, Any], execution_id: str = None
) -> Dict[str, bool]:
    # Send notifications for pipeline events - delegates to shared.notifications
    # Add execution context to message
    if execution_id:
        message = f"[Execution: {execution_id}] {message}"

    # Use the shared notification system
    success = send_notification_channels(event_type, message, config)

    return {"email_sent": success, "slack_sent": success}


def notify_pipeline_success(
    execution_id: str, stats: Dict[str, Any], config: Dict[str, Any]
) -> None:
    # Send success notification
    message = (
        f"Pipeline completed successfully. Rows processed: {stats.get('total_rows', 0)}"
    )
    if execution_id:
        message = f"[Execution: {execution_id}] {message}"
    send_notification_channels("success", message, config)


def notify_pipeline_failure(
    execution_id: str, error_message: str, config: Dict[str, Any], source: str = None
) -> None:
    # Send failure notification with smart alerting
    client = get_bigquery_client()
    dataset = config.get("pipeline", {}).get(
        "monitoring_dataset", "orchestrator_monitoring"
    )

    # Extract error type from error message
    error_type = "unknown"
    if "connection" in error_message.lower():
        error_type = "connection"
    elif "authentication" in error_message.lower():
        error_type = "authentication"
    elif "quota" in error_message.lower():
        error_type = "quota"
    elif "permission" in error_message.lower():
        error_type = "permission"
    elif "timeout" in error_message.lower():
        error_type = "timeout"

    # Check if we should send alert
    if not should_send_alert(
        execution_id, error_type, source or "unknown", client, dataset
    ):
        logger.info(f"Alert suppressed for {source} - retry detected")
        return

    # Get alert severity
    retry_count = 0  # This would be calculated from the query above
    severity = get_alert_severity(source or "unknown", error_type, retry_count)

    # Create deduplication key
    alert_key = create_alert_deduplication_key(source or "unknown", error_type)

    # Send notification
    message = f"Pipeline failed with error: {error_message} (Severity: {severity})"
    send_notification("failure", message, config, execution_id)

    # Log that alert was sent
    log_alert_sent(
        client, dataset, alert_key, execution_id, source or "unknown", severity
    )


def notify_data_quality_issues(
    table_id: str, quality_results: Dict[str, Any], config: Dict[str, Any]
) -> None:
    # Send data quality issue notification
    failed_checks = quality_results.get("checks_failed", 0)
    total_checks = quality_results.get("total_checks", 0)

    if failed_checks > 0:
        message = f"Data quality issues detected in {table_id}: {failed_checks}/{total_checks} checks failed"
        send_notification("warning", message, config)


def clear_incremental_state(
    source: str, tables: List[str], bq_client: Optional[bigquery.Client] = None
) -> int:
    # remove incremental state rows for the specified source/table list

    if not source:
        return 0

    unique_tables = sorted({table for table in tables if table})
    if not unique_tables:
        return 0

    client = bq_client or get_bigquery_client()

    ensure_monitoring_infrastructure(client)

    dataset_id = f"{client.project}.orchestrator_monitoring"
    state_table = f"{dataset_id}.extraction_state"

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{state_table}` (
        source STRING,
        table_name STRING,
        last_extracted_at TIMESTAMP,
        execution_id STRING,
        rows_loaded INT64,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    delete_sql = f"""
    DELETE FROM `{state_table}`
    WHERE source = @source
      AND table_name IN UNNEST(@tables)
    """

    try:
        client.query(create_sql).result()

        from google.cloud.bigquery import (
            QueryJobConfig,
            ScalarQueryParameter,
            ArrayQueryParameter,
        )

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("source", "STRING", source),
                ArrayQueryParameter("tables", "STRING", unique_tables),
            ]
        )

        job = client.query(delete_sql, job_config=job_config)
        result = job.result()
        deleted_rows = (
            result.total_rows
            if hasattr(result, "total_rows")
            else job.num_dml_affected_rows or 0
        )

        # Format table list - show count if many tables, otherwise list them
        if len(unique_tables) > 10:
            table_summary = f"{len(unique_tables)} tables"
        else:
            table_summary = ", ".join(unique_tables)

        if deleted_rows:
            logger.info(
                "Cleared %d incremental state rows for %s (%s)",
                deleted_rows,
                source,
                table_summary,
            )
        else:
            logger.info(
                "No incremental state rows to clear for %s (%s)",
                source,
                table_summary,
            )
        return deleted_rows

    except Exception as exc:
        logger.warning(
            "Failed to clear incremental state for %s (%s): %s",
            source,
            unique_tables,
            exc,
        )
        return 0
