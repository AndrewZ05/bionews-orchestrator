"""
Job management utilities for pipeline orchestration.

Provides comprehensive job lifecycle management including:
- Job creation and tracking
- Machine/process information recording
- Cross-platform job cancellation (Windows/Linux)
- Job status querying and reporting
- Log file management

Author: Data Pipeline Team
Created: 2025-10-26
"""

import logging
import os
import socket

# NOTE: 'import platform' removed - it was causing hangs on Windows
# Use os.name and sys.version_info instead
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Constants
MONITORING_DATASET = "orchestrator_monitoring"
JOBS_TABLE = "jobs"

# Startup grace window (seconds) for lock-based orphan cleanup. A job inserts its
# RUNNING row and then acquires its lock a moment later; without a grace window the
# lock-based sweep could kill a job during that gap. Overridable via env for tests.
_LOCK_ORPHAN_GRACE_SECONDS = 120


def _resolve_stale_running_hours(explicit: Optional[int]) -> int:
    """
    Hours after which RUNNING rows in jobs are marked KILLED (all hosts).

    Returns 0 to disable the time-based sweep (process-based cleanup still runs).
    """
    if explicit is not None:
        try:
            h = int(explicit)
            if h <= 0:
                return 0
            return max(1, min(h, 336))  # 1 hour .. 14 days
        except (TypeError, ValueError):
            pass
    raw = (os.environ.get("ORCHESTRATOR_STALE_RUNNING_HOURS") or "").strip()
    if raw:
        try:
            h = int(raw)
            if h <= 0:
                return 0
            return max(1, min(h, 336))
        except ValueError:
            pass
    return 24


def get_machine_info() -> Dict[str, Any]:
    """
    Collect comprehensive machine and process information.

    Returns:
        Dictionary with machine details including hostname, IP, OS, PID, etc.
    """
    try:
        hostname = socket.gethostname()
        try:
            # Set a short timeout for DNS lookup to avoid hanging
            socket.setdefaulttimeout(2.0)
            ip_address = socket.gethostbyname(hostname)
            socket.setdefaulttimeout(None)  # Reset to default
        except Exception:
            socket.setdefaulttimeout(None)  # Reset to default
            ip_address = "unknown"

        # Determine OS type from os.name (simpler than platform.system())
        os_type = (
            "Windows"
            if os.name == "nt"
            else "Linux" if os.name == "posix" else "Unknown"
        )

        # Get OS version from environment
        os_version = os.environ.get("OS", "Unknown")

        # Get Python version from sys.version_info
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        process_id = os.getpid()
        parent_process_id = os.getppid() if hasattr(os, "getppid") else None

        # Get username
        try:
            if os_type == "Windows":
                user = os.environ.get("USERNAME", "unknown")
            else:
                user = os.environ.get("USER", "unknown")
        except Exception:
            user = "unknown"

        working_directory = os.getcwd()

        # Determine if remote killing is feasible
        # Remote killing requires SSH (Linux) or WinRM (Windows) to be configured
        # We'll mark it as potentially killable if it's a standard OS
        is_remote_killable = os_type in ("Windows", "Linux")

        return {
            "hostname": hostname,
            "ip_address": ip_address,
            "os_type": os_type,
            "os_version": os_version,
            "python_version": python_version,
            "process_id": process_id,
            "parent_process_id": parent_process_id,
            "user": user,
            "working_directory": working_directory,
            "is_remote_killable": is_remote_killable,
        }
    except Exception as e:
        logger.warning(f"Error collecting machine info: {e}")
        return {
            "hostname": "unknown",
            "ip_address": "unknown",
            "os_type": "unknown",
            "os_version": "unknown",
            "python_version": "unknown",
            "process_id": None,
            "parent_process_id": None,
            "user": "unknown",
            "working_directory": "unknown",
            "is_remote_killable": False,
        }


def generate_job_id() -> str:
    """Generate unique job ID."""
    return f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def create_job_log_table(
    bq_client: bigquery.Client, dataset: str = MONITORING_DATASET
) -> None:
    """
    Create job log table if it doesn't exist.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name (default: orchestrator_monitoring)
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"

    try:
        bq_client.get_table(table_id)
        logger.debug(f"Jobs table already exists: {table_id}")
        return
    except Exception:
        pass  # Table doesn't exist, create it

    # Define comprehensive schema with machine tracking
    schema = [
        # Job identification
        bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("job_name", "STRING"),
        bigquery.SchemaField("job_type", "STRING"),
        bigquery.SchemaField("command_line", "STRING"),
        # Job status
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status_updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("queued_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("completed_at", "TIMESTAMP"),
        bigquery.SchemaField("duration_seconds", "INT64"),
        # Job parameters
        bigquery.SchemaField("tables", "STRING", mode="REPEATED"),
        bigquery.SchemaField("sites", "STRING", mode="REPEATED"),
        bigquery.SchemaField("groups", "STRING", mode="REPEATED"),
        bigquery.SchemaField("refresh_mode", "STRING"),
        # Job results
        bigquery.SchemaField("total_rows", "INT64"),
        bigquery.SchemaField("sites_processed", "INT64"),
        bigquery.SchemaField("tables_processed", "INT64"),
        # Error tracking
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("error_type", "STRING"),
        bigquery.SchemaField("retry_count", "INT64"),
        # Logging
        bigquery.SchemaField("log_file_path", "STRING"),
        bigquery.SchemaField("log_preview", "STRING"),
        # Environment
        bigquery.SchemaField("environment", "STRING"),
        bigquery.SchemaField("triggered_by", "STRING"),
        bigquery.SchemaField("parent_job_id", "STRING"),
        # Machine tracking fields (NEW)
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
        # Timestamps
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    # Partition by queued_at for efficient querying
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="queued_at"
    )

    # Cluster by commonly filtered fields
    table.clustering_fields = ["source", "status", "job_type", "hostname"]

    bq_client.create_table(table)
    logger.info(f"Created jobs table: {table_id}")


def log_job_start(
    bq_client: bigquery.Client,
    dataset: str,
    job_id: str,
    source: str,
    table_name: Optional[str] = None,
    **kwargs,
) -> None:
    """
    Log job start with machine information.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        job_id: Unique job identifier
        source: Data source (e.g., 'facebook', 'wordpress')
        table_name: Table being processed (optional)
        **kwargs: Additional job parameters (tables, sites, groups, etc.)
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"

    # Collect machine information
    machine_info = get_machine_info()

    # Automatically cleanup any orphaned jobs/executions before logging a new one
    try:
        cleanup_orphaned_jobs(bq_client, dataset, machine_info.get("hostname"))
    except Exception as cleanup_err:
        logger.warning(f"Pre-start cleanup failed (continuing): {cleanup_err}")

    # Prepare job record
    now = datetime.now(timezone.utc)

    row = {
        "job_id": job_id,
        "execution_id": kwargs.get("execution_id", job_id),
        "source": source,
        "job_name": table_name or kwargs.get("job_name", "unknown"),
        "job_type": kwargs.get("job_type", "extraction"),
        "command_line": " ".join(sys.argv),
        "status": "RUNNING",
        "status_updated_at": now.isoformat(),
        "queued_at": now.isoformat(),
        "started_at": now.isoformat(),
        "tables": kwargs.get("tables", []),
        "sites": kwargs.get("sites", []),
        "groups": kwargs.get("groups", []),
        "refresh_mode": kwargs.get("refresh_mode", "incremental"),
        "environment": kwargs.get("environment", "prod"),
        "triggered_by": kwargs.get("triggered_by", "manual"),
        "retry_count": 0,
        # Machine information
        "hostname": machine_info["hostname"],
        "ip_address": machine_info["ip_address"],
        "os_type": machine_info["os_type"],
        "os_version": machine_info["os_version"],
        "python_version": machine_info["python_version"],
        "process_id": machine_info["process_id"],
        "parent_process_id": machine_info["parent_process_id"],
        "user": machine_info["user"],
        "working_directory": machine_info["working_directory"],
        "is_remote_killable": machine_info["is_remote_killable"],
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
    }

    try:
        errors = bq_client.insert_rows_json(table_id, [row])
        if errors:
            logger.error(f"Error logging job start: {errors}")
        else:
            logger.info(f"Logged job start: {job_id} on {machine_info['hostname']}")
    except Exception as e:
        logger.error(f"Failed to log job start: {e}")


def log_job_complete(
    bq_client: bigquery.Client, dataset: str, job_id: str, total_rows: int = 0, **kwargs
) -> None:
    """
    Log job completion.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        job_id: Unique job identifier
        total_rows: Total rows processed
        **kwargs: Additional completion info (tables_processed, sites_processed, etc.)
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"
    now = datetime.now(timezone.utc)

    # First, get the job start time to calculate duration
    query = f"""
    SELECT started_at
    FROM `{table_id}`
    WHERE job_id = @job_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    )

    try:
        result = bq_client.query(query, job_config=job_config).result()
        started_at = None
        for row in result:
            started_at = row.started_at
            break

        duration_seconds = None
        if started_at:
            duration = now - started_at
            duration_seconds = int(duration.total_seconds())

        # Update job record
        update_query = f"""
        UPDATE `{table_id}`
        SET
            status = 'COMPLETED',
            status_updated_at = @now,
            completed_at = @now,
            duration_seconds = @duration,
            total_rows = @total_rows,
            tables_processed = @tables_processed,
            sites_processed = @sites_processed,
            updated_at = @now
        WHERE job_id = @job_id
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("duration", "INT64", duration_seconds),
                bigquery.ScalarQueryParameter("total_rows", "INT64", total_rows),
                bigquery.ScalarQueryParameter(
                    "tables_processed", "INT64", kwargs.get("tables_processed", 0)
                ),
                bigquery.ScalarQueryParameter(
                    "sites_processed", "INT64", kwargs.get("sites_processed", 0)
                ),
            ]
        )

        bq_client.query(update_query, job_config=update_config).result()
        logger.info(
            f"Logged job completion: {job_id} ({duration_seconds}s, {total_rows} rows)"
        )

    except Exception as e:
        logger.error(f"Failed to log job completion: {e}")


def log_job_failure(
    bq_client: bigquery.Client, dataset: str, job_id: str, error: Exception
) -> None:
    """
    Log job failure with error details.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        job_id: Unique job identifier
        error: Exception that caused the failure
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"
    now = datetime.now(timezone.utc)

    error_message = str(error)
    error_type = type(error).__name__

    # Get duration if possible
    query = f"""
    SELECT started_at
    FROM `{table_id}`
    WHERE job_id = @job_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    )

    try:
        result = bq_client.query(query, job_config=job_config).result()
        started_at = None
        for row in result:
            started_at = row.started_at
            break

        duration_seconds = None
        if started_at:
            duration = now - started_at
            duration_seconds = int(duration.total_seconds())

        # Update job record
        update_query = f"""
        UPDATE `{table_id}`
        SET
            status = 'FAILED',
            status_updated_at = @now,
            completed_at = @now,
            duration_seconds = @duration,
            error_message = @error_message,
            error_type = @error_type,
            updated_at = @now
        WHERE job_id = @job_id
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("duration", "INT64", duration_seconds),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
                bigquery.ScalarQueryParameter("error_type", "STRING", error_type),
            ]
        )

        bq_client.query(update_query, job_config=update_config).result()
        logger.error(f"Logged job failure: {job_id} - {error_type}: {error_message}")

    except Exception as e:
        logger.error(f"Failed to log job failure: {e}")


def cancel_job(
    bq_client: bigquery.Client, dataset: str, job_id: str, reason: Optional[str] = None
) -> bool:
    """
    Cancel a job and optionally kill its process.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        job_id: Unique job identifier
        reason: Cancellation reason (optional)

    Returns:
        True if cancellation was successful, False otherwise
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"
    now = datetime.now(timezone.utc)

    try:
        # Get job details to find process information
        query = f"""
        SELECT
            hostname,
            os_type,
            process_id,
            user,
            status,
            is_remote_killable
        FROM `{table_id}`
        WHERE job_id = @job_id
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        )

        result = bq_client.query(query, job_config=job_config).result()
        job_info = None
        for row in result:
            job_info = {
                "hostname": row.hostname,
                "os_type": row.os_type,
                "process_id": row.process_id,
                "user": row.user,
                "status": row.status,
                "is_remote_killable": row.is_remote_killable,
            }
            break

        if not job_info:
            logger.warning(f"Job not found: {job_id}")
            return False

        # Update job status to CANCELLED (uppercase — consistent with orchestrate / dashboard)
        update_query = f"""
        UPDATE `{table_id}`
        SET
            status = 'CANCELLED',
            status_updated_at = @now,
            completed_at = @now,
            error_message = @reason,
            updated_at = @now
        WHERE job_id = @job_id
        """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter(
                    "reason", "STRING", reason or "User cancelled"
                ),
            ]
        )

        bq_client.query(update_query, job_config=update_config).result()
        logger.info(f"Cancelled job: {job_id}")

        # Attempt to kill the process if it's running
        if (
            str(job_info.get("status") or "").upper() == "RUNNING"
            and job_info["process_id"]
        ):
            try:
                kill_success = kill_job_process(
                    hostname=job_info["hostname"],
                    os_type=job_info["os_type"],
                    process_id=job_info["process_id"],
                    user=job_info["user"],
                )
                if kill_success:
                    logger.info(
                        f"Killed process {job_info['process_id']} for job {job_id}"
                    )
                else:
                    logger.warning(f"Could not kill process for job {job_id}")
            except Exception as e:
                logger.warning(f"Error killing process for job {job_id}: {e}")

        return True

    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        return False


def check_job_status(bq_client: bigquery.Client, dataset: str, job_id: str) -> str:
    """
    Check job status.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        job_id: Unique job identifier

    Returns:
        Job status string as stored (e.g. RUNNING, COMPLETED, FAILED, CANCELLED) or NOT_FOUND
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"

    try:
        query = f"""
        SELECT status
        FROM `{table_id}`
        WHERE job_id = @job_id
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        )

        result = bq_client.query(query, job_config=job_config).result()
        for row in result:
            return row.status

        return "NOT_FOUND"

    except Exception as e:
        logger.error(f"Failed to check job status: {e}")
        return "ERROR"


def list_recent_jobs(
    bq_client: bigquery.Client,
    dataset: str,
    source: Optional[str] = None,
    table: Optional[str] = None,
    hostname: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 10,
    check_running: bool = True,
) -> List[Dict]:
    """
    List recent jobs with optional filtering.
    Automatically checks if RUNNING jobs are actually still running and marks dead ones as KILLED.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        source: Filter by source (optional)
        table: Filter by table name (optional)
        hostname: Filter by hostname (optional)
        status: Filter by status (optional)
        limit: Maximum number of jobs to return (default: 10)
        check_running: Check if RUNNING jobs are actually alive (default: True)

    Returns:
        List of job dictionaries
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"

    # First, if check_running is enabled, check all RUNNING jobs on this machine
    if check_running:
        current_hostname = socket.gethostname()
        cleanup_orphaned_jobs(bq_client, dataset, current_hostname)

    where_clauses = []
    query_params = []

    if source:
        where_clauses.append("source = @source")
        query_params.append(bigquery.ScalarQueryParameter("source", "STRING", source))

    if table:
        where_clauses.append("job_name = @table_name")
        query_params.append(
            bigquery.ScalarQueryParameter("table_name", "STRING", table)
        )

    if hostname:
        where_clauses.append("hostname = @hostname")
        query_params.append(
            bigquery.ScalarQueryParameter("hostname", "STRING", hostname)
        )

    if status:
        where_clauses.append("UPPER(status) = UPPER(@status)")
        query_params.append(bigquery.ScalarQueryParameter("status", "STRING", status))

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
    SELECT
        job_id,
        source,
        job_name,
        status,
        hostname,
        user,
        os_type,
        process_id,
        queued_at,
        started_at,
        completed_at,
        duration_seconds,
        total_rows,
        error_message
    FROM `{table_id}`
    WHERE {where_sql}
    ORDER BY queued_at DESC
    LIMIT @limit
    """

    query_params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)

    try:
        result = bq_client.query(query, job_config=job_config).result()

        jobs = []
        for row in result:
            jobs.append(
                {
                    "job_id": row.job_id,
                    "source": row.source,
                    "job_name": row.job_name,
                    "status": row.status,
                    "hostname": row.hostname,
                    "user": row.user,
                    "os_type": row.os_type,
                    "process_id": row.process_id,
                    "queued_at": row.queued_at,
                    "started_at": row.started_at,
                    "completed_at": row.completed_at,
                    "duration_seconds": row.duration_seconds,
                    "total_rows": row.total_rows,
                    "error_message": row.error_message,
                }
            )

        return jobs

    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        return []


def cleanup_orphaned_jobs(
    bq_client: bigquery.Client,
    dataset: str,
    hostname: Optional[str] = None,
    stale_running_hours: Optional[int] = None,
) -> int:
    """
    Mark stuck RUNNING jobs as KILLED.

    1) **Lock-based (all hosts, OS-independent):** a RUNNING row whose ``source`` has no
       active (unexpired) row in ``job_locks`` is KILLED. The BigQuery lock is the
       authoritative "am I really running" signal -- it is heartbeat-refreshed while the
       process lives and auto-expires within minutes once it dies, so an absent lock means
       the job is gone regardless of which host/OS it ran on. A short startup grace window
       (``COALESCE(started_at, queued_at)`` younger than a couple of minutes) is exempt so a
       job that has inserted its RUNNING row but not yet acquired its lock is not killed
       mid-startup.

    2) **Process-based (this host only):** RUNNING rows for ``hostname`` (default: current
       machine) or ``hostname IS NULL`` are KILLED if ``process_id`` is NULL or the PID is
       not running locally.

    3) **Time-based (all hosts):** RUNNING rows whose ``COALESCE(started_at, queued_at)`` is
       older than ``stale_running_hours`` (from this argument, env ``ORCHESTRATOR_STALE_RUNNING_HOURS``,
       or default 24; use ``0`` to disable) are KILLED. This clears rows left behind when a
       job ran on another machine or the process check cannot see the PID.

    4) Legacy ``_pipeline_executions`` rows stuck in running for 120+ minutes → KILLED.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        hostname: Hostname for process check (default: current machine)
        stale_running_hours: Override stale threshold hours; ``None`` uses env then default ``24``. ``0`` disables.

    Returns:
        Number of records marked as KILLED across jobs and legacy executions
    """
    if hostname is None:
        hostname = socket.gethostname()

    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"

    # Get all RUNNING jobs on this hostname with process IDs
    # Also get jobs with NULL process_id (old jobs before machine tracking)
    query = f"""
    SELECT job_id, process_id, os_type, started_at, hostname
    FROM `{table_id}`
    WHERE UPPER(TRIM(status)) = 'RUNNING'
      AND (hostname = @hostname OR hostname IS NULL)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("hostname", "STRING", hostname)]
    )

    killed_count = 0

    try:
        result = bq_client.query(query, job_config=job_config).result()

        jobs_to_kill = []
        for row in result:
            # If no process_id, it's an old job before machine tracking - mark as KILLED
            if row.process_id is None:
                jobs_to_kill.append(row.job_id)
            # Check if process is actually running
            elif not is_process_running(row.process_id, row.os_type):
                jobs_to_kill.append(row.job_id)

        # Mark dead jobs as KILLED — single bulk UPDATE.
        # Earlier this was a per-row loop; on a busy day BigQuery's per-table
        # DML rate limit serialised it down to one statement every 2-10 seconds
        # and an entire orchestrator launch could spend 15+ minutes inside this
        # function. One bulk UPDATE behaves the same semantically regardless of
        # the number of rows.
        if jobs_to_kill:
            now = datetime.now(timezone.utc)
            bulk_update_query = f"""
            UPDATE `{table_id}`
            SET
                status = 'KILLED',
                status_updated_at = @now,
                completed_at = @now,
                error_message = 'Process no longer running (auto-detected)',
                updated_at = @now
            WHERE job_id IN UNNEST(@job_ids)
            """
            bulk_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("job_ids", "STRING", jobs_to_kill),
                    bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
                ]
            )
            bulk_job = bq_client.query(bulk_update_query, job_config=bulk_config)
            bulk_job.result()
            killed_count = int(bulk_job.num_dml_affected_rows or len(jobs_to_kill))
            logger.info(f"Marked {killed_count} orphaned jobs as KILLED on {hostname}")

        # Lock-based (all hosts, OS-independent): a RUNNING row whose source has no
        # active (unexpired) lock is orphaned. The lock is heartbeat-refreshed while
        # the process lives and auto-expires within minutes once it dies, so an absent
        # lock is authoritative regardless of host/OS -- this catches jobs the local
        # PID check cannot see (e.g. a Linux VM run viewed from a Windows dashboard)
        # without waiting for the multi-hour time-based sweep. A short startup grace
        # window exempts rows that inserted RUNNING but have not yet taken their lock.
        lock_killed = _cleanup_locked_orphans(bq_client, dataset, table_id)

        # Time-based: RUNNING on any host older than threshold (handles other machines / missed PIDs)
        stale_hours = _resolve_stale_running_hours(stale_running_hours)
        stale_killed = 0
        if stale_hours > 0:
            try:
                now_ts = datetime.now(timezone.utc)
                err_msg = (
                    f"Marked KILLED: RUNNING for more than {stale_hours} hours "
                    "(stale job cleanup; applies to all hosts)"
                )
                stale_cfg = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("now", "TIMESTAMP", now_ts),
                        bigquery.ScalarQueryParameter(
                            "hours", "INT64", int(stale_hours)
                        ),
                        bigquery.ScalarQueryParameter("err_msg", "STRING", err_msg),
                    ]
                )
                count_sql = f"""
                SELECT COUNT(*) AS c
                FROM `{table_id}`
                WHERE UPPER(TRIM(status)) = 'RUNNING'
                  AND COALESCE(started_at, queued_at) < TIMESTAMP_SUB(@now, INTERVAL @hours HOUR)
                """
                cnt_row = list(
                    bq_client.query(count_sql, job_config=stale_cfg).result()
                )[0]
                stale_killed = int(cnt_row.c or 0)
                if stale_killed:
                    stale_update = f"""
                    UPDATE `{table_id}`
                    SET
                        status = 'KILLED',
                        status_updated_at = @now,
                        completed_at = @now,
                        error_message = @err_msg,
                        duration_seconds = CAST(
                            TIMESTAMP_DIFF(@now, COALESCE(started_at, queued_at), SECOND) AS INT64
                        ),
                        updated_at = @now
                    WHERE UPPER(TRIM(status)) = 'RUNNING'
                      AND COALESCE(started_at, queued_at) < TIMESTAMP_SUB(@now, INTERVAL @hours HOUR)
                    """
                    bq_client.query(stale_update, job_config=stale_cfg).result()
                    logger.info(
                        "Marked %s RUNNING job(s) as KILLED (stale > %s h, any host)",
                        stale_killed,
                        stale_hours,
                    )
            except Exception as stale_exc:
                logger.warning("Stale RUNNING job cleanup failed: %s", stale_exc)

        # Also clean up legacy _pipeline_executions entries stuck in 'running'
        pipeline_table_id = f"{bq_client.project}.{dataset}._pipeline_executions"
        pipeline_timeout_minutes = 120
        pipeline_killed = 0

        # Single bulk UPDATE on legacy _pipeline_executions. Earlier this was
        # a SELECT-then-per-row-UPDATE loop; on a busy day each per-row UPDATE
        # took 2-10 seconds (and one observed at 190s) due to BigQuery's per-
        # table DML rate limit. One bulk UPDATE handles any number of rows in
        # a single statement and is identical semantically.
        try:
            now_pl = datetime.now(timezone.utc)
            bulk_pl_query = f"""
            UPDATE `{pipeline_table_id}`
            SET
                status = 'KILLED',
                completed_at = @now,
                error_message = 'Auto-marked as killed after exceeding execution timeout'
            WHERE UPPER(TRIM(status)) = 'RUNNING'
              AND started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @timeout_minute MINUTE)
            """
            bulk_pl_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "timeout_minute", "INT64", pipeline_timeout_minutes
                    ),
                    bigquery.ScalarQueryParameter("now", "TIMESTAMP", now_pl),
                ]
            )
            bulk_pl_job = bq_client.query(bulk_pl_query, job_config=bulk_pl_config)
            bulk_pl_job.result()
            pipeline_killed = int(bulk_pl_job.num_dml_affected_rows or 0)
            if pipeline_killed:
                logger.info(
                    "Marked %d legacy pipeline execution(s) as KILLED after %d minutes",
                    pipeline_killed,
                    pipeline_timeout_minutes,
                )

        except Exception as pipeline_err:
            logger.error(
                f"Failed to cleanup legacy pipeline executions: {pipeline_err}"
            )

        return killed_count + lock_killed + stale_killed + pipeline_killed

    except Exception as e:
        logger.error(f"Failed to cleanup orphaned jobs: {e}")
        return 0


def _cleanup_locked_orphans(
    bq_client: bigquery.Client,
    dataset: str,
    table_id: str,
) -> int:
    """Mark RUNNING jobs whose source holds no active lock as KILLED.

    A single UPDATE kills every RUNNING row that (a) is older than the startup
    grace window and (b) has no matching source with an unexpired row in the
    ``job_locks`` table. Host- and OS-independent: the lock, not the local PID,
    is the source of truth for whether a job is still alive.

    Returns the number of rows marked KILLED (0 on any error -- best effort, so a
    missing/empty lock table never blocks the rest of orphan cleanup).
    """
    lock_table_id = f"{bq_client.project}.{dataset}.job_locks"
    try:
        try:
            grace_seconds = int(
                os.environ.get(
                    "ORCHESTRATOR_LOCK_ORPHAN_GRACE_SECONDS",
                    _LOCK_ORPHAN_GRACE_SECONDS,
                )
            )
        except (TypeError, ValueError):
            grace_seconds = _LOCK_ORPHAN_GRACE_SECONDS
        grace_seconds = max(0, grace_seconds)

        now_ts = datetime.now(timezone.utc)
        err_msg = (
            "Marked KILLED: RUNNING with no active lock for its source "
            "(lock-based orphan cleanup; applies to all hosts)"
        )
        cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now_ts),
                bigquery.ScalarQueryParameter("grace", "INT64", grace_seconds),
                bigquery.ScalarQueryParameter("err_msg", "STRING", err_msg),
            ]
        )
        # NOT IN against the active-lock source set. Guard the subquery against a
        # NULL source (would make NOT IN return no rows) with a WHERE filter.
        update_sql = f"""
        UPDATE `{table_id}`
        SET
            status = 'KILLED',
            status_updated_at = @now,
            completed_at = @now,
            error_message = @err_msg,
            duration_seconds = CAST(
                TIMESTAMP_DIFF(@now, COALESCE(started_at, queued_at), SECOND) AS INT64
            ),
            updated_at = @now
        WHERE UPPER(TRIM(status)) = 'RUNNING'
          AND source IS NOT NULL
          AND COALESCE(started_at, queued_at) < TIMESTAMP_SUB(@now, INTERVAL @grace SECOND)
          AND source NOT IN (
              SELECT source
              FROM `{lock_table_id}`
              WHERE source IS NOT NULL
                AND expires_at > @now
          )
        """
        job = bq_client.query(update_sql, job_config=cfg)
        job.result()
        killed = int(job.num_dml_affected_rows or 0)
        if killed:
            logger.info(
                "Marked %s RUNNING job(s) as KILLED (no active lock, any host)",
                killed,
            )
        return killed
    except Exception as exc:  # noqa: BLE001 -- best effort; never block cleanup
        logger.warning("Lock-based orphan cleanup failed: %s", exc)
        return 0


def cleanup_old_jobs(bq_client: bigquery.Client, dataset: str, days: int = 30) -> None:
    """
    Clean up old job records.

    Args:
        bq_client: BigQuery client instance
        dataset: Dataset name
        days: Delete jobs older than this many days (default: 30)
    """
    table_id = f"{bq_client.project}.{dataset}.{JOBS_TABLE}"
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        query = f"""
        DELETE FROM `{table_id}`
        WHERE queued_at < @cutoff_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cutoff_date", "TIMESTAMP", cutoff_date)
            ]
        )

        query_job = bq_client.query(query, job_config=job_config)
        result = query_job.result()

        logger.info(f"Cleaned up jobs older than {days} days from {table_id}")

    except Exception as e:
        logger.error(f"Failed to cleanup old jobs: {e}")


def is_process_running(process_id: int, os_type: str = None) -> bool:
    """
    Check if a process is currently running on the local machine.

    Args:
        process_id: Process ID to check
        os_type: Operating system type (auto-detected if None)

    Returns:
        True if process is running, False otherwise
    """
    if not process_id:
        return False

    if os_type is None:
        os_type = (
            "Windows"
            if os.name == "nt"
            else "Linux" if os.name == "posix" else "Unknown"
        )

    try:
        if os_type == "Windows":
            # Use tasklist to check if process exists
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {process_id}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            # If process exists, PID will appear in output
            return str(process_id) in result.stdout
        else:
            # Use kill -0 on Linux/Unix (doesn't actually kill, just checks)
            result = subprocess.run(
                ["kill", "-0", str(process_id)], capture_output=True, timeout=5
            )
            return result.returncode == 0
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, Exception) as e:
        logger.debug(f"Error checking process {process_id}: {e}")
        return False


def kill_job_process(hostname: str, os_type: str, process_id: int, user: str) -> bool:
    """
    Kill a job process on local or remote machine.

    Args:
        hostname: Machine hostname where process is running
        os_type: Operating system type ('Windows', 'Linux', etc.)
        process_id: Process ID to kill
        user: User who owns the process

    Returns:
        True if process was killed successfully, False otherwise
    """
    current_hostname = socket.gethostname()
    is_local = hostname == current_hostname

    try:
        if is_local:
            # Kill local process
            if os_type == "Windows":
                # Use taskkill on Windows
                subprocess.run(
                    ["taskkill", "/F", "/PID", str(process_id)],
                    check=True,
                    capture_output=True,
                )
                logger.info(f"Killed local Windows process {process_id}")
                return True
            else:
                # Use kill on Linux/Unix
                subprocess.run(
                    ["kill", "-9", str(process_id)], check=True, capture_output=True
                )
                logger.info(f"Killed local Linux process {process_id}")
                return True
        else:
            # Kill remote process
            logger.warning(f"Remote process killing not yet implemented for {hostname}")
            logger.info(
                f"To manually kill: ssh {user}@{hostname} 'kill -9 {process_id}'"
            )
            return False

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to kill process {process_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Error killing process {process_id}: {e}")
        return False
