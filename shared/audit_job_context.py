#!/usr/bin/env python3
# Root-cause context: PIPELINE JOB CORRELATION. When a table has audit findings,
# the first triage question is "did the source's pipeline job even run?" -- a
# stale table behind a FAILED job is a different fix than one behind a SUCCESS
# job that wrote nothing. This helper fetches each source's most recent job from
# orchestrator_monitoring.jobs so the email can answer that question inline.
"""Pipeline-job context for the audit emails.

``fetch_job_context`` returns {source: 'STATUS at YYYY-MM-DD HH:MM UTC'} for the
requested sources, from the latest row per source in the orchestrator's central
jobs table. ENTIRELY best-effort: in projects where the monitoring tables do not
exist (currently the case here), any failure returns {} and the email simply
omits the context block.
"""

import logging
from typing import Dict, List

from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

_JOBS_SQL = """-- Latest pipeline job per source, for audit-email triage context.
SELECT source, status, status_updated_at
FROM `{ds}`.`{tbl}`
WHERE source IN UNNEST(@sources)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY source ORDER BY status_updated_at DESC
) = 1"""


def fetch_job_context(
    client,
    sources: List[str],
    monitoring_dataset: str = "orchestrator_monitoring",
    jobs_table: str = "jobs",
) -> Dict[str, str]:
    """Return {source: 'STATUS at <timestamp> UTC'} for the latest job per source.

    Best-effort: returns {} when the jobs table is absent/unreadable or sources
    is empty -- the caller renders nothing in that case.
    """
    if not sources:
        return {}
    try:
        safe_ds = _validate_identifier(monitoring_dataset, "dataset")
        safe_tbl = _validate_identifier(jobs_table, "table")
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("sources", "STRING", sorted(set(sources)))
            ]
        )
        sql = _JOBS_SQL.format(ds=safe_ds, tbl=safe_tbl)
        out: Dict[str, str] = {}
        for r in client.query(sql, job_config=job_config).result():
            ts = r["status_updated_at"]
            ts_str = ts.strftime("%Y-%m-%d %H:%M UTC") if ts is not None else "?"
            out[r["source"]] = "{0} at {1}".format(r["status"], ts_str)
        logger.info("Fetched pipeline-job context for %d source(s)", len(out))
        return out
    except Exception as exc:  # noqa: BLE001 - context is best-effort
        logger.debug("Job context unavailable: %s", exc)
        return {}
