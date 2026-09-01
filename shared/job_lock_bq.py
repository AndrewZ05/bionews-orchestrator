"""
BigQuery-based distributed job locking for multi-machine deployments.
Prevents concurrent execution of the same ETL source across all machines.
Locks are automatically released on process exit or via timeout.
"""

import logging
import os
import sys
import atexit
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict
from google.cloud import bigquery
from google.api_core.exceptions import (
    GoogleAPIError as GoogleCloudError,
    NotFound,
    Conflict,
)

logger = logging.getLogger(__name__)

# Module-level tracking for acquired locks
_acquired_locks = {}  # source -> (job_id, lock_id)
_lock_heartbeats = {}  # source -> stop event
_lock_lost = {}  # source -> True once the heartbeat finds the lock no longer owned
_bq_client = None

# Consecutive heartbeat query failures tolerated before the lock is treated as
# lost (uncertain ownership). The heartbeat fires every timeout/3; 2 failures
# means ~2/3 of the TTL has elapsed without a confirmed refresh, so the lock is
# near expiry and a takeover is possible -- fail closed rather than race.
_MAX_HEARTBEAT_FAILURES = 2


def is_lock_lost(source: str) -> bool:
    """
    True if this process's lock for `source` was lost mid-run (heartbeat found
    no owned, unexpired row -- expiry overrun, takeover, or manual release).

    The orchestrator must check this before each destructive step (e.g. each
    production MERGE): once the lock is lost, a concurrent run may hold it, and
    proceeding risks two MERGEs racing on the same table.
    """
    return bool(_lock_lost.get(source))


def _process_name() -> str:
    """Best-effort command text for lock diagnostics."""
    if not sys.argv:
        return "unknown"
    return " ".join(str(arg) for arg in sys.argv)


def _heartbeat_interval_seconds(timeout_minutes: int) -> int:
    """Heartbeat often enough to keep the lease fresh without noisy BQ DML."""
    try:
        configured = int(os.environ.get("ORCHESTRATOR_LOCK_HEARTBEAT_SECONDS", "120"))
    except ValueError:
        configured = 120

    max_interval = max(30, int(timeout_minutes * 60 / 3))
    return max(30, min(configured, max_interval))


def _get_bq_client():
    """Get or create BigQuery client."""
    global _bq_client
    if _bq_client is None:
        project_id = os.environ.get("GCP_PROJECT_ID")
        if not project_id:
            raise RuntimeError("GCP_PROJECT_ID environment variable not set")
        _bq_client = bigquery.Client(project=project_id)
    return _bq_client


def _ensure_lock_table():
    """Create lock table if it doesn't exist."""
    try:
        client = _get_bq_client()
        project_id = os.environ.get("GCP_PROJECT_ID")

        # Check if table exists
        table_id = f"{project_id}.{_get_monitoring_dataset()}.job_locks"
        try:
            client.get_table(table_id)
            return  # Table exists
        except NotFound:
            pass

        # Create table with proper schema
        from google.cloud.bigquery import SchemaField

        schema = [
            SchemaField(
                "source", "STRING", mode="REQUIRED", description="Data source name"
            ),
            SchemaField(
                "lock_id",
                "STRING",
                mode="REQUIRED",
                description="Unique lock identifier",
            ),
            SchemaField(
                "job_id", "STRING", mode="REQUIRED", description="Centralized job ID"
            ),
            SchemaField(
                "process_pid", "INTEGER", mode="REQUIRED", description="Process ID"
            ),
            SchemaField(
                "process_name", "STRING", mode="NULLABLE", description="Process command"
            ),
            SchemaField(
                "hostname", "STRING", mode="REQUIRED", description="Machine hostname"
            ),
            SchemaField(
                "acquired_at",
                "TIMESTAMP",
                mode="REQUIRED",
                description="When lock was acquired",
            ),
            SchemaField(
                "expires_at",
                "TIMESTAMP",
                mode="REQUIRED",
                description="When lock expires",
            ),
        ]

        table = bigquery.Table(table_id, schema=schema)
        table.clustering_fields = ["source"]
        client.create_table(table)
        logger.info(f"Created job_locks table in {_get_monitoring_dataset()}")

    except GoogleCloudError as e:
        logger.warning(f"Could not create lock table: {e}")
        raise


def _get_monitoring_dataset():
    """Get monitoring dataset name from job_manager to stay consistent with the rest of the pipeline."""
    from shared.job_manager import MONITORING_DATASET

    return os.environ.get("MONITORING_DATASET", MONITORING_DATASET)


def _cleanup_locks_on_exit():
    """Release all acquired locks on process exit."""
    for source in list(_acquired_locks.keys()):
        release_job_lock(source)
    _acquired_locks.clear()


# Register cleanup on exit
atexit.register(_cleanup_locks_on_exit)


def _heartbeat_job_lock(lock_source: str, lock_id: str, timeout_minutes: int) -> int:
    """Extend this process's lock lease. Returns affected row count."""
    client = _get_bq_client()
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = _get_monitoring_dataset()
    table_id = f"{project_id}.{dataset_id}.job_locks"

    heartbeat_query = f"""
    UPDATE `{table_id}`
    SET expires_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @timeout_minutes MINUTE)
    WHERE source = @source
    AND lock_id = @lock_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", lock_source),
            bigquery.ScalarQueryParameter("lock_id", "STRING", lock_id),
            bigquery.ScalarQueryParameter("timeout_minutes", "INT64", timeout_minutes),
        ]
    )
    heartbeat_job = client.query(heartbeat_query, job_config=job_config)
    heartbeat_job.result()
    return int(heartbeat_job.num_dml_affected_rows or 0)


def _delete_lock_row(lock_source: str, lock_id: str) -> int:
    """Delete a specific lock row by scope and UUID. Returns affected row count."""
    client = _get_bq_client()
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = _get_monitoring_dataset()
    table_id = f"{project_id}.{dataset_id}.job_locks"

    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE source = @source
    AND lock_id = @lock_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", lock_source),
            bigquery.ScalarQueryParameter("lock_id", "STRING", lock_id),
        ]
    )

    delete_job = client.query(delete_query, job_config=job_config)
    delete_job.result()
    return int(delete_job.num_dml_affected_rows or 0)


def _start_lock_heartbeat(source: str, lock_id: str, timeout_minutes: int) -> None:
    """Start a daemon thread that keeps the lock alive while this process runs."""
    if source in _lock_heartbeats:
        return

    stop_event = threading.Event()
    interval = _heartbeat_interval_seconds(timeout_minutes)

    def _loop() -> None:
        consecutive_failures = 0
        while not stop_event.wait(interval):
            try:
                affected = _heartbeat_job_lock(source, lock_id, timeout_minutes)
                consecutive_failures = 0
                if affected == 0:
                    # Our lock row is gone or owned by someone else (expiry
                    # overrun, takeover, or manual release). Flag it loudly so
                    # the main run aborts before its next destructive MERGE,
                    # rather than racing a concurrent run holding the lock now.
                    _lock_lost[source] = True
                    logger.error(
                        "ETL lock LOST for source '%s' (lock_id: %s): heartbeat "
                        "found no owned row. A concurrent run may hold the lock; "
                        "the pipeline will abort before the next destructive step.",
                        source,
                        lock_id,
                    )
                    break
            except Exception as e:
                # A transient BigQuery/query failure means we can no longer
                # PROVE we still hold the lock. If failures persist long enough
                # to approach the lock TTL, the lock may expire and another run
                # could acquire it while we still believe we own it. After N
                # consecutive failures, treat the lock as lost (uncertain) so
                # the main run aborts before its next destructive MERGE rather
                # than risk a concurrent-MERGE race.
                consecutive_failures += 1
                if consecutive_failures >= _MAX_HEARTBEAT_FAILURES:
                    _lock_lost[source] = True
                    logger.error(
                        "ETL lock UNCERTAIN for source '%s' (lock_id: %s): "
                        "%d consecutive heartbeat failures; cannot confirm "
                        "ownership. Treating lock as lost; the pipeline will "
                        "abort before the next destructive step. Last error: %s",
                        source,
                        lock_id,
                        consecutive_failures,
                        e,
                    )
                    break
                logger.warning(
                    "ETL lock heartbeat failed for source '%s' (lock_id: %s) "
                    "[%d/%d before treating lock as lost]: %s",
                    source,
                    lock_id,
                    consecutive_failures,
                    _MAX_HEARTBEAT_FAILURES,
                    e,
                )

    thread = threading.Thread(
        target=_loop,
        name=f"etl-lock-heartbeat-{lock_id[:8]}",
        daemon=True,
    )
    _lock_heartbeats[source] = stop_event
    thread.start()


def _stop_lock_heartbeat(source: str) -> None:
    stop_event = _lock_heartbeats.pop(source, None)
    if stop_event:
        stop_event.set()


def acquire_job_lock(source: str, job_id: str, timeout_minutes: int = 30) -> bool:
    """
    Acquire an ETL lock in BigQuery for the given source.

    Locks are scoped per source: different sources can run concurrently,
    two runs against the same source are mutually exclusive. The lock is
    held in a BigQuery table and includes an automatic expiration.
    Expired rows are reused atomically by the MERGE below.

    Args:
        source: Data source name (e.g., 'facebook', 'mailchimp')
        job_id: Centralized job ID from monitoring system
        timeout_minutes: How long the lock is valid (default 30 minutes)

    Returns:
        True if lock acquired, False if another job holds an active lock
        for this source

    Raises:
        RuntimeError: If there's a BigQuery error
    """
    try:
        client = _get_bq_client()
        project_id = os.environ.get("GCP_PROJECT_ID")
        dataset_id = _get_monitoring_dataset()
        table_id = f"{project_id}.{dataset_id}.job_locks"

        # Ensure table exists
        _ensure_lock_table()

        active_lock_query = f"""
        SELECT source, lock_id, job_id, process_pid, process_name, hostname, acquired_at, expires_at
        FROM `{table_id}`
        WHERE source = @source
        AND expires_at > CURRENT_TIMESTAMP()
        ORDER BY acquired_at ASC
        LIMIT 1
        """
        active_lock_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
            ]
        )
        active_rows = list(
            client.query(active_lock_query, job_config=active_lock_config).result()
        )
        if active_rows:
            existing = active_rows[0]
            logger.error(
                f"Cannot acquire ETL lock for source '{source}'. "
                f"Held by job {existing.job_id} "
                f"(PID: {existing.process_pid} on {existing.hostname}, "
                f"expires: {existing.expires_at}, "
                f"process: {existing.process_name or 'unknown'})"
            )
            return False

        # Generate unique lock ID
        import uuid

        lock_id = str(uuid.uuid4())

        # Get process info
        pid = os.getpid()
        hostname = os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown"))
        process_name = _process_name()

        # Atomic acquire using MERGE. BigQuery executes MERGE atomically against
        # the target table, so concurrent acquires from multiple machines race at
        # the storage layer rather than between our SELECT and INSERT. Semantics:
        #   * MATCHED on active (non-expired) lock -> no-op (we lose the race)
        #   * MATCHED on expired lock -> overwrite via UPDATE (reuses the row)
        #   * NOT MATCHED -> INSERT a fresh row.
        # num_dml_affected_rows distinguishes acquired (1) from no-op (0).
        merge_query = f"""
        MERGE `{table_id}` T
        USING (
            SELECT
                @source AS source,
                @lock_id AS lock_id,
                @job_id AS job_id,
                @pid AS process_pid,
                @process_name AS process_name,
                @hostname AS hostname,
                CURRENT_TIMESTAMP() AS acquired_at,
                TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @timeout_minutes MINUTE) AS expires_at
        ) S
        ON T.source = S.source
        WHEN MATCHED AND T.expires_at <= CURRENT_TIMESTAMP() THEN
            UPDATE SET
                lock_id = S.lock_id,
                job_id = S.job_id,
                process_pid = S.process_pid,
                process_name = S.process_name,
                hostname = S.hostname,
                acquired_at = S.acquired_at,
                expires_at = S.expires_at
        WHEN NOT MATCHED THEN
            INSERT (source, lock_id, job_id, process_pid, process_name, hostname, acquired_at, expires_at)
            VALUES (S.source, S.lock_id, S.job_id, S.process_pid, S.process_name, S.hostname, S.acquired_at, S.expires_at)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("lock_id", "STRING", lock_id),
                bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                bigquery.ScalarQueryParameter("pid", "INTEGER", pid),
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("hostname", "STRING", hostname),
                bigquery.ScalarQueryParameter(
                    "timeout_minutes", "INT64", timeout_minutes
                ),
            ]
        )

        merge_job = client.query(merge_query, job_config=job_config)
        merge_job.result()  # Wait for completion

        # num_dml_affected_rows is 1 if we inserted OR updated an expired row.
        # It's 0 if an active lock already existed (MERGE no-op).
        affected = merge_job.num_dml_affected_rows or 0

        if affected == 0:
            # Active lock exists for this source - fetch details for the error message.
            detail_query = f"""
            SELECT source, lock_id, job_id, process_pid, process_name, hostname, acquired_at, expires_at
            FROM `{table_id}`
            WHERE source = @source
            AND expires_at > CURRENT_TIMESTAMP()
            ORDER BY acquired_at ASC
            LIMIT 1
            """
            detail_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                ]
            )
            rows = list(client.query(detail_query, job_config=detail_config).result())
            if rows:
                existing = rows[0]
                logger.error(
                    f"Cannot acquire ETL lock for source '{source}'. "
                    f"Held by job {existing.job_id} "
                    f"(PID: {existing.process_pid} on {existing.hostname}, "
                    f"expires: {existing.expires_at}, "
                    f"process: {existing.process_name or 'unknown'})"
                )
            else:
                # Lock vanished between MERGE and detail query (released by holder).
                # Don't loop here - caller can retry. This is a rare timing window.
                logger.error(
                    f"Cannot acquire ETL lock for source '{source}'. "
                    f"MERGE was a no-op but no active lock found - likely just released. "
                    f"Retry the run."
                )
            return False

        # Verify the row we wrote is actually ours. Protects against the edge case
        # where two machines submit MERGE at the same instant: BigQuery serializes
        # them, but only one's lock_id ends up in the row. The loser sees affected=1
        # for its own UPDATE/INSERT but the holder is now the other machine.
        verify_query = f"""
        SELECT lock_id, job_id, process_pid, hostname, expires_at
        FROM `{table_id}`
        WHERE source = @source
        LIMIT 1
        """
        verify_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
            ]
        )
        verify_rows = list(
            client.query(verify_query, job_config=verify_config).result()
        )

        if not verify_rows or verify_rows[0].lock_id != lock_id:
            actual_holder = verify_rows[0] if verify_rows else None
            if actual_holder:
                logger.error(
                    f"Lost race acquiring ETL lock for source '{source}'. "
                    f"Lock now held by job {actual_holder.job_id} "
                    f"(PID: {actual_holder.process_pid} on {actual_holder.hostname})"
                )
            else:
                logger.error(
                    f"Lost race acquiring ETL lock for source '{source}'. "
                    f"Lock row no longer exists."
                )
            cleaned = _delete_lock_row(source, lock_id)
            if cleaned:
                logger.info(
                    "Cleaned up %s non-winning lock row(s) for source '%s' (lock_id: %s)",
                    cleaned,
                    source,
                    lock_id,
                )
            return False

        expires_at = verify_rows[0].expires_at
        # Track lock locally for release on exit
        _acquired_locks[source] = (job_id, lock_id)
        _lock_lost.pop(source, None)  # fresh acquire clears any prior lost-flag
        _start_lock_heartbeat(source, lock_id, timeout_minutes)

        logger.info(
            f"Acquired ETL lock for source '{source}' "
            f"(lock_id: {lock_id}, expires: {expires_at})"
        )
        return True

    except GoogleCloudError as e:
        logger.error(f"BigQuery error acquiring lock: {e}")
        raise RuntimeError(f"Job lock acquisition failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error acquiring lock: {e}")
        raise


def release_job_lock(source: str):
    """
    Release this process's ETL lock for the given source.

    Removes the lock record owned by this process.
    Safe to call even if lock wasn't acquired or was already released.

    Args:
        source: Data source name
    """
    try:
        if source not in _acquired_locks:
            return
        _stop_lock_heartbeat(source)

        _job_id, lock_id = _acquired_locks[source]

        # Match by lock_id only. PID is not unique across machines and the row
        # we own is identified by its UUID. The lock_id guard prevents deleting
        # a successor's lock if ours already expired and was taken over.
        deleted_rows = _delete_lock_row(source, lock_id)

        del _acquired_locks[source]
        if deleted_rows == 0:
            logger.warning(
                f"Released ETL lock for source '{source}', but row was already gone "
                f"(lock_id: {lock_id}) - it likely expired and was taken over."
            )
        else:
            logger.info(f"Released ETL lock for source '{source}'")

    except GoogleCloudError as e:
        logger.warning(f"BigQuery error releasing lock: {e}")
    except Exception as e:
        logger.warning(f"Error releasing lock for source '{source}': {e}")


def cleanup_expired_locks():
    """
    Clean up all expired locks in BigQuery.

    Removes lock records that have passed their expiration time.
    This is called periodically and also on startup for safety.
    """
    try:
        client = _get_bq_client()
        project_id = os.environ.get("GCP_PROJECT_ID")
        dataset_id = _get_monitoring_dataset()
        table_id = f"{project_id}.{dataset_id}.job_locks"

        cleanup_query = f"""
        DELETE FROM `{table_id}`
        WHERE expires_at <= CURRENT_TIMESTAMP()
        """

        result = client.query(cleanup_query).result()

        logger.info("Cleaned up expired job locks in BigQuery")

    except GoogleCloudError as e:
        logger.warning(f"BigQuery error cleaning up locks: {e}")
    except Exception as e:
        logger.warning(f"Error cleaning up expired locks: {e}")


def get_active_locks() -> Dict:
    """
    Get information about all active job locks.

    Returns:
        Dictionary mapping source to lock info (job_id, pid, hostname, expires_at)
    """
    try:
        client = _get_bq_client()
        project_id = os.environ.get("GCP_PROJECT_ID")
        dataset_id = _get_monitoring_dataset()
        table_id = f"{project_id}.{dataset_id}.job_locks"

        query = f"""
        SELECT source, lock_id, job_id, process_pid, process_name, hostname, acquired_at, expires_at
        FROM `{table_id}`
        WHERE expires_at > CURRENT_TIMESTAMP()
        ORDER BY source, acquired_at DESC
        """

        results = client.query(query).result()

        locks = {}
        for row in results:
            if row.source not in locks:
                locks[row.source] = []
            locks[row.source].append(
                {
                    "lock_id": row.lock_id,
                    "job_id": row.job_id,
                    "pid": row.process_pid,
                    "process_name": row.process_name,
                    "hostname": row.hostname,
                    "acquired_at": (
                        row.acquired_at.isoformat() if row.acquired_at else None
                    ),
                    "expires_at": (
                        row.expires_at.isoformat() if row.expires_at else None
                    ),
                }
            )

        return locks

    except GoogleCloudError as e:
        logger.warning(f"BigQuery error retrieving locks: {e}")
        return {}
    except Exception as e:
        logger.warning(f"Error retrieving active locks: {e}")
        return {}


def force_release_lock(source: str, lock_id: str):
    """
    Force release a specific lock (admin only).

    Used when a job is stuck and needs manual intervention. Operators
    targeting a stale legacy row can pass its source string literally.

    Args:
        source: Data source name
        lock_id: Lock ID to release
    """
    try:
        client = _get_bq_client()
        project_id = os.environ.get("GCP_PROJECT_ID")
        dataset_id = _get_monitoring_dataset()
        table_id = f"{project_id}.{dataset_id}.job_locks"

        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE source = @source
        AND lock_id = @lock_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("lock_id", "STRING", lock_id),
            ]
        )

        delete_job = client.query(delete_query, job_config=job_config)
        delete_job.result()
        affected = int(delete_job.num_dml_affected_rows or 0)
        if affected:
            logger.info(
                f"Force released lock {lock_id} for source '{source}' ({affected} row(s))"
            )
        else:
            logger.warning(f"No lock row matched lock {lock_id} for source '{source}'")

    except GoogleCloudError as e:
        logger.error(f"BigQuery error force releasing lock: {e}")
    except Exception as e:
        logger.error(f"Error force releasing lock: {e}")
