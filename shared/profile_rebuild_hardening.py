"""
Profile rebuild hardening: dataset lease, heartbeat, BQ job labels, pre-promote backup.

Env (all optional):
  PROFILE_DATASET_LEASE_TTL_MINUTES   — lease + heartbeat extension TTL (default 240)
  PROFILE_LEASE_HEARTBEAT_SECONDS     — background heartbeat interval (default 120)
  PROFILE_BQ_MAX_BYTES_BILLED         — cap bytes billed per BigQuery child job (optional)
  PROFILE_BACKUP_BEFORE_PROMOTE     — if 1/true, CLONE production physical tables into
                                        profile_ops before candidate→prod promotion
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Any, Callable, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def sanitize_bq_label_value(value: str, max_len: int = 63) -> str:
    """BigQuery job labels: lowercase letters, numbers, underscores, dashes; max 63 chars."""
    v = str(value).lower()
    v = re.sub(r"[^a-z0-9_-]+", "_", v).strip("_") or "na"
    return v[:max_len]


def build_profile_sql_job_labels(
    execution_id: str,
    mode: str,
    step_name: str,
    consumer_dataset: str,
    orchestrator_job_id: Optional[str] = None,
) -> dict[str, str]:
    """Default labels for profile_database BigQuery jobs (cost / lineage filters)."""
    short_id = str(execution_id).replace("-", "")[:32]
    ds = consumer_dataset.split(".")[-1] if consumer_dataset else "na"
    out: dict[str, str] = {
        "orchestrator_component": "profile_database",
        "profile_mode": sanitize_bq_label_value(mode),
        "profile_step": sanitize_bq_label_value(step_name),
        "profile_build_id": sanitize_bq_label_value(short_id),
        "profile_consumer": sanitize_bq_label_value(ds),
    }
    if orchestrator_job_id:
        out["orchestrator_job_id"] = str(orchestrator_job_id).lower()[:63]
    return out


def profile_maximum_bytes_billed() -> Optional[int]:
    raw = os.environ.get("PROFILE_BQ_MAX_BYTES_BILLED") or os.environ.get(
        "BQ_MAX_BYTES_BILLED_PROFILE"
    )
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid PROFILE_BQ_MAX_BYTES_BILLED ignored: %s", raw)
        return None


def lease_ttl_minutes() -> int:
    try:
        return max(30, int(os.environ.get("PROFILE_DATASET_LEASE_TTL_MINUTES", "240")))
    except ValueError:
        return 240


def heartbeat_interval_seconds() -> int:
    try:
        return max(30, int(os.environ.get("PROFILE_LEASE_HEARTBEAT_SECONDS", "120")))
    except ValueError:
        return 120


def backup_before_promote_enabled() -> bool:
    return os.environ.get("PROFILE_BACKUP_BEFORE_PROMOTE", "").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def should_acquire_candidate_lease(
    mode: str, build_consumer_dataset: str, candidate_dataset: str
) -> bool:
    """Exclusive lease on the candidate consumer dataset during blue/green work."""
    if os.environ.get("PROFILE_SKIP_DATASET_LEASE", "").lower() in ("1", "true", "yes", "on"):
        return False
    if build_consumer_dataset != candidate_dataset:
        return False
    return mode in {"rebuild", "resume_rebuild", "resume_publish"}


def acquire_dataset_lease(
    bq_client: bigquery.Client,
    dataset_id: str,
    holder_build_id: str,
    mode: str,
    ttl_minutes: int,
) -> None:
    """
    Take or refresh an exclusive lease on dataset_id for holder_build_id.

    Raises RuntimeError if another active holder holds a non-expired lease.
    """
    project = bq_client.project
    fq = f"`{project}.profile_ops.profile_dataset_leases`"
    merge_sql = f"""
    MERGE {fq} T
    USING (
      SELECT
        @dataset_id AS dataset_id,
        @holder AS holder_build_id,
        @ttl AS ttl_m,
        @mode AS mode
    ) S
    ON T.dataset_id = S.dataset_id
    WHEN MATCHED AND (
      T.lease_until < CURRENT_TIMESTAMP()
      OR T.holder_build_id = S.holder_build_id
    ) THEN
      UPDATE SET
        holder_build_id = S.holder_build_id,
        acquired_at = CURRENT_TIMESTAMP(),
        lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL S.ttl_m MINUTE),
        last_heartbeat_at = CURRENT_TIMESTAMP(),
        mode = S.mode
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        dataset_id,
        holder_build_id,
        acquired_at,
        lease_until,
        last_heartbeat_at,
        mode
      )
      VALUES (
        S.dataset_id,
        S.holder_build_id,
        CURRENT_TIMESTAMP(),
        TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL S.ttl_m MINUTE),
        CURRENT_TIMESTAMP(),
        S.mode
      )
    """
    cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
            bigquery.ScalarQueryParameter("holder", "STRING", holder_build_id),
            bigquery.ScalarQueryParameter("ttl", "INT64", ttl_minutes),
            bigquery.ScalarQueryParameter("mode", "STRING", mode),
        ],
    )
    bq_client.query(merge_sql, job_config=cfg).result()

    chk = """
    SELECT holder_build_id, lease_until
    FROM profile_ops.profile_dataset_leases
    WHERE dataset_id = @dataset_id
    LIMIT 1
    """
    chk_cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
        ],
    )
    rows = list(bq_client.query(chk, job_config=chk_cfg).result())
    if not rows:
        raise RuntimeError(f"Lease row missing after MERGE for dataset_id={dataset_id}")
    row = rows[0]
    if row.holder_build_id != holder_build_id:
        raise RuntimeError(
            f"Candidate dataset '{dataset_id}' is locked by build_id={row.holder_build_id} "
            f"until {row.lease_until} (UTC). Wait or clear that lease row after confirming "
            "the other job is dead."
        )


def release_dataset_lease(
    bq_client: bigquery.Client, dataset_id: str, holder_build_id: str
) -> None:
    sql = """
    DELETE FROM profile_ops.profile_dataset_leases
    WHERE dataset_id = @dataset_id AND holder_build_id = @holder
    """
    cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
            bigquery.ScalarQueryParameter("holder", "STRING", holder_build_id),
        ],
    )
    try:
        bq_client.query(sql, job_config=cfg).result()
    except Exception as e:
        logger.warning("Failed to release dataset lease for %s: %s", dataset_id, e)


def heartbeat_dataset_lease(
    bq_client: bigquery.Client,
    dataset_id: str,
    holder_build_id: str,
    ttl_minutes: int,
) -> None:
    sql = """
    UPDATE profile_ops.profile_dataset_leases
    SET
      last_heartbeat_at = CURRENT_TIMESTAMP(),
      lease_until = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MINUTE)
    WHERE dataset_id = @dataset_id AND holder_build_id = @holder
    """
    cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("dataset_id", "STRING", dataset_id),
            bigquery.ScalarQueryParameter("holder", "STRING", holder_build_id),
            bigquery.ScalarQueryParameter("ttl", "INT64", ttl_minutes),
        ],
    )
    bq_client.query(sql, job_config=cfg).result()


def heartbeat_build_run_row(bq_client: bigquery.Client, build_id: str) -> None:
    """Stamp last_heartbeat_at on profile_ops.profile_build_runs (ops logging builds only)."""
    sql = """
    UPDATE profile_ops.profile_build_runs
    SET last_heartbeat_at = CURRENT_TIMESTAMP()
    WHERE build_id = @build_id AND status = 'running'
    """
    cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("build_id", "STRING", build_id),
        ],
    )
    try:
        bq_client.query(sql, job_config=cfg).result()
    except Exception as e:
        logger.warning("Build run heartbeat failed: %s", e)


def start_lease_heartbeat_thread(
    bq_client: bigquery.Client,
    dataset_id: str,
    holder_build_id: str,
    ttl_minutes: int,
    persist_ops_log: bool,
) -> tuple[threading.Event, threading.Thread]:
    """
    Background heartbeat: extends lease TTL and (if enabled) stamps profile_build_runs.
    """
    stop = threading.Event()
    interval = heartbeat_interval_seconds()

    def _loop() -> None:
        while not stop.wait(interval):
            try:
                heartbeat_dataset_lease(
                    bq_client, dataset_id, holder_build_id, ttl_minutes
                )
                if persist_ops_log:
                    heartbeat_build_run_row(bq_client, holder_build_id)
            except Exception as e:
                logger.warning(
                    "Heartbeat tick failed (dataset=%s build=%s): %s",
                    dataset_id,
                    holder_build_id,
                    e,
                )

    th = threading.Thread(
        target=_loop,
        name=f"profile-lease-heartbeat-{holder_build_id[:8]}",
        daemon=True,
    )
    th.start()
    return stop, th


def clone_production_tables_before_promote(
    bq_client: bigquery.Client,
    production_dataset: str,
    table_names: tuple[str, ...] | list[str],
    build_id: str,
) -> list[str]:
    """
    CLONE each existing production physical table into profile_ops with a backup suffix.

    Returns list of backup table FQNs created (skipped tables omitted).
    """
    project = bq_client.project
    suffix = sanitize_bq_label_value(
        f"{int(time.time())}_{str(build_id).replace('-', '')[:12]}"
    )
    created: list[str] = []
    for table_name in table_names:
        src = f"`{project}.{production_dataset}.{table_name}`"
        dst = f"`{project}.profile_ops.{table_name}_promote_bak_{suffix}`"
        exists_sql = f"SELECT 1 AS x FROM {src} LIMIT 1"
        try:
            list(bq_client.query(exists_sql).result())
        except Exception:
            logger.info("Backup skip (source missing): %s.%s", production_dataset, table_name)
            continue
        clone_sql = f"CREATE OR REPLACE TABLE {dst} CLONE {src}"
        try:
            bq_client.query(clone_sql).result()
            created.append(f"{project}.profile_ops.{table_name}_promote_bak_{suffix}")
            logger.info("Pre-promote backup: %s", created[-1])
        except Exception as e:
            logger.error("Pre-promote CLONE failed for %s: %s", table_name, e)
            raise
    return created
