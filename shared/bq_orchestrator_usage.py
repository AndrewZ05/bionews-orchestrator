"""
Aggregate BigQuery job usage for orchestrator runs.

Child jobs must carry label ``orchestrator_job_id`` (the centralized monitoring
``orchestrator_monitoring.jobs.job_id`` UUID) to be included. Jobs without the
label are ignored so concurrent runs do not mix costs.

Region defaults to ``region-us``; override with env ``ORCHESTRATOR_BQ_JOBS_REGION``.

Estimated USD (``bq_estimated_cost_usd``) uses ``(bytes_billed / 1024**4) * rate`` where ``rate`` comes from
``monitoring.bigquery_estimated_cost_usd_per_tebibyte`` in merged YAML (see ``configs/defaults.yaml``), then env
``ORCHESTRATOR_BQ_USD_PER_TIB``, then **6.25** (verify against your invoice / region).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def _jobs_region() -> str:
    raw = (os.environ.get("ORCHESTRATOR_BQ_JOBS_REGION") or "region-us").strip()
    return raw if raw.startswith("region-") else f"region-{raw}"


_DEFAULT_USD_PER_TEBIBYTE = 6.25


def resolve_usd_per_tebibyte(config: Optional[Dict[str, Any]] = None) -> float:
    """
    Dollars per tebibyte (1024^4 bytes) for on-demand-style estimates from bytes billed.

    Order: ``config['monitoring']['bigquery_estimated_cost_usd_per_tebibyte']`` if set and valid,
    else env ``ORCHESTRATOR_BQ_USD_PER_TIB``, else ``_DEFAULT_USD_PER_TEBIBYTE``.
    """
    if config:
        mon = config.get("monitoring")
        if isinstance(mon, dict):
            raw = mon.get("bigquery_estimated_cost_usd_per_tebibyte")
            if raw is not None and raw != "":
                try:
                    v = float(raw)
                    if v >= 0:
                        return v
                except (TypeError, ValueError):
                    pass
    env_raw = (os.environ.get("ORCHESTRATOR_BQ_USD_PER_TIB") or "").strip()
    if env_raw:
        try:
            v = float(env_raw)
            return v if v >= 0 else _DEFAULT_USD_PER_TEBIBYTE
        except ValueError:
            pass
    return _DEFAULT_USD_PER_TEBIBYTE


def estimate_bq_cost_usd_from_bytes_billed(
    bytes_billed: int, usd_per_tebibyte: float
) -> float:
    """Rough on-demand USD from summed ``total_bytes_billed`` (TiB basis, same as GCP list quotes)."""
    if not bytes_billed:
        return 0.0
    tib = bytes_billed / (1024**4)
    return round(tib * usd_per_tebibyte, 6)


def fetch_labeled_bigquery_usage(
    client: bigquery.Client,
    project_id: str,
    orchestrator_job_id: str,
    window_start: datetime,
    window_end: datetime,
    config: Optional[Dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Sum bytes/slots for DONE jobs in INFORMATION_SCHEMA that carry
    ``labels.orchestrator_job_id = orchestrator_job_id``.

    Returns keys aligned with ``orchestrator_monitoring.jobs`` BQ usage columns.
    """
    region = _jobs_region()
    # INFORMATION_SCHEMA can lag; widen the window slightly.
    t0 = window_start - timedelta(minutes=15)
    t1 = window_end + timedelta(minutes=15)

    summary_sql = f"""
    SELECT
      IFNULL(SUM(IFNULL(total_bytes_billed, 0)), 0) AS bq_total_bytes_billed,
      IFNULL(SUM(IFNULL(total_bytes_processed, 0)), 0) AS bq_total_bytes_processed,
      IFNULL(SUM(IFNULL(total_slot_ms, 0)), 0) AS bq_total_slot_ms,
      COUNT(1) AS bq_labeled_job_count
    FROM `{project_id}.{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE project_id = @project_id
      AND creation_time >= @t0
      AND creation_time <= @t1
      AND end_time IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM UNNEST(labels) AS l
        WHERE l.key = 'orchestrator_job_id'
          AND l.value = @orch_job_id
      )
    """

    cfg = bigquery.QueryJobConfig(
        use_query_cache=False,
        query_parameters=[
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("t0", "TIMESTAMP", t0),
            bigquery.ScalarQueryParameter("t1", "TIMESTAMP", t1),
            bigquery.ScalarQueryParameter("orch_job_id", "STRING", orchestrator_job_id),
        ],
    )
    row = list(client.query(summary_sql, job_config=cfg).result())[0]

    detail_sql = f"""
    SELECT
      job_type,
      IFNULL(statement_type, 'UNKNOWN') AS statement_type,
      COUNT(*) AS job_count,
      IFNULL(SUM(IFNULL(total_bytes_billed, 0)), 0) AS bytes_billed,
      IFNULL(SUM(IFNULL(total_bytes_processed, 0)), 0) AS bytes_processed,
      IFNULL(SUM(IFNULL(total_slot_ms, 0)), 0) AS slot_ms
    FROM `{project_id}.{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE project_id = @project_id
      AND creation_time >= @t0
      AND creation_time <= @t1
      AND end_time IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM UNNEST(labels) AS l
        WHERE l.key = 'orchestrator_job_id'
          AND l.value = @orch_job_id
      )
    GROUP BY 1, 2
    ORDER BY bytes_billed DESC
    LIMIT 40
    """
    detail_rows = list(client.query(detail_sql, job_config=cfg).result())
    breakdown = [
        {
            "job_type": r.job_type,
            "statement_type": r.statement_type,
            "job_count": r.job_count,
            "bytes_billed": int(r.bytes_billed or 0),
            "bytes_processed": int(r.bytes_processed or 0),
            "slot_ms": int(r.slot_ms or 0),
        }
        for r in detail_rows
    ]

    bytes_billed = int(row.bq_total_bytes_billed or 0)
    rate = resolve_usd_per_tebibyte(config)
    bq_estimated_cost_usd = estimate_bq_cost_usd_from_bytes_billed(bytes_billed, rate)

    return {
        "bq_total_bytes_billed": bytes_billed,
        "bq_total_bytes_processed": int(row.bq_total_bytes_processed or 0),
        "bq_total_slot_ms": int(row.bq_total_slot_ms or 0),
        "bq_labeled_job_count": int(row.bq_labeled_job_count or 0),
        "bq_estimated_cost_usd": bq_estimated_cost_usd,
        "bq_usage_detail": json.dumps(
            {
                "jobs_region": region,
                "window_start": t0.isoformat(),
                "window_end": t1.isoformat(),
                "orchestrator_job_id": orchestrator_job_id,
                "usd_per_tebibyte": rate,
                "cost_note": "On-demand-style estimate from total_bytes_billed; not slot/commitment pricing.",
                "by_job_type": breakdown,
            }
        ),
    }
