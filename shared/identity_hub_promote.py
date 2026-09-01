"""Atomic shadow→production promote helpers for Identity Hub."""
from __future__ import annotations

import logging
from typing import List, Sequence, Tuple

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def ensure_manifest_table(client: bigquery.Client, manifest_table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{manifest_table}` (
      active_run_id STRING, promoted_at TIMESTAMP, status STRING
    )
    """
    client.query(sql).result()


def write_manifest(
    client: bigquery.Client,
    manifest_table: str,
    run_id: str,
    status: str,
) -> None:
    ensure_manifest_table(client, manifest_table)
    sql = f"""
    INSERT INTO `{manifest_table}` (active_run_id, promoted_at, status)
    VALUES ('{run_id}', CURRENT_TIMESTAMP(), '{status}')
    """
    client.query(sql).result()


def promote_tables_atomic(
    client: bigquery.Client,
    promotion_order: Sequence[Tuple[str, str]],
    *,
    project: str,
    ops_dataset: str = "identity_hub_staging",
    run_id: str,
) -> List[str]:
    """
    Promote shadow→canon with CLONE backups and auto-rollback on failure.

    promotion_order: list of (shadow_fqn, canon_fqn)
    Returns list of canon table labels successfully promoted (before failure).
    Raises RuntimeError after attempting rollback if a copy fails mid-way.
    """
    backups: List[Tuple[str, str]] = []  # (canon, backup)
    promoted: List[str] = []
    suffix = run_id.replace("-", "")[:12]

    # 1) Backup current production tables
    for shadow, canon in promotion_order:
        label = canon.split(".")[-1]
        backup = f"{project}.{ops_dataset}.{label}_promote_bak_{suffix}"
        try:
            client.query(f"SELECT 1 FROM `{canon}` LIMIT 1").result()
            client.query(f"CREATE OR REPLACE TABLE `{backup}` CLONE `{canon}`").result()
            backups.append((canon, backup))
            print(f"    Backup {label} → {backup.split('.')[-1]}", flush=True)
        except Exception as e:
            # First promote / missing table: skip backup
            logger.info("Promote backup skip for %s: %s", canon, e)

    # 2) Copy shadow → canon
    try:
        for shadow, canon in promotion_order:
            copy_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.copy_table(shadow, canon, job_config=copy_config)
            job.result()
            label = canon.split(".")[-1]
            promoted.append(label)
            print(f"    Promoted {label}", flush=True)
        return promoted
    except Exception as e:
        print(
            f"    ERROR during promotion after {len(promoted)} tables: {e}",
            flush=True,
        )
        print(f"    Rolling back: {promoted}", flush=True)
        # 3) Rollback already-promoted tables from backups
        for canon, backup in backups:
            label = canon.split(".")[-1]
            if label not in promoted:
                continue
            try:
                copy_config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
                job = client.copy_table(backup, canon, job_config=copy_config)
                job.result()
                print(f"    Rolled back {label}", flush=True)
            except Exception as rb_err:
                print(f"    ROLLBACK FAILED for {label}: {rb_err}", flush=True)
        raise RuntimeError(
            f"Shadow table promotion failed after promoting {promoted}; "
            f"rollback attempted. Original error: {e}"
        ) from e
