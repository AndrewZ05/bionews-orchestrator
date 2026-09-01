#!/usr/bin/env python3
"""
Adaptive Watermarks
===================

Per-table high-water mark of the last successful extraction, so incremental pulls
size their window to "what's actually new since we last succeeded" instead of a
blanket fixed lookback. Two failure modes the fixed window can't handle:

  - over-fetch: after a clean nightly you only need ~1 day, but lookback_days re-pulls
    the whole window every run (API/BQ cost).
  - under-fetch: if a source was down longer than lookback_days, the missing tail is
    silently never re-pulled. A watermark stretches the window to cover the gap.

State lives in orchestrator_monitoring-style `<dataset>.extraction_state` (already
in use by the WordPress extractor; this generalizes that exact pattern):
    source, table_name, last_extracted_at, execution_id, rows_loaded, updated_at

This module is the single shared implementation. The pure window math
(resolve_watermark_start) is unit-tested without BigQuery; get/save do the I/O.

SAFETY: the watermark only ENGAGES when a table opts in (incremental.use_watermark:
true). The default path is unchanged fixed-lookback. When engaged, a configurable
`safety_days` cushion re-pulls a margin before the mark to absorb late-arriving rows,
and the resolved window is clamped so it never produces a start AFTER `end`/now.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

STATE_TABLE = "extraction_state"

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS `{tid}` (
    source STRING,
    table_name STRING,
    last_extracted_at TIMESTAMP,
    execution_id STRING,
    rows_loaded INT64,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
"""


def _state_table_id(client: Any, dataset: str) -> str:
    return f"{client.project}.{dataset}.{STATE_TABLE}"


def get_watermark(client: Any, dataset: str, source: str, table: str) -> Optional[str]:
    """
    Latest last_extracted_at for (source, table) as an ISO string, or None when no
    state exists yet (first run). Best-effort: returns None on any error -- callers
    fall back to fixed lookback, never crash.
    """
    from google.cloud import bigquery

    tid = _state_table_id(client, dataset)
    try:
        client.query(_CREATE_SQL.format(tid=tid)).result()
        rows = client.query(
            f"""
            SELECT last_extracted_at
            FROM `{tid}`
            WHERE source = @source AND table_name = @table_name
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                    bigquery.ScalarQueryParameter("table_name", "STRING", table),
                ]
            ),
        ).result()
        for row in rows:
            if row.last_extracted_at:
                return row.last_extracted_at.isoformat()
    except Exception as e:  # noqa: BLE001
        logger.warning("Watermark read failed for %s.%s: %s", source, table, e)
    return None


def save_watermark(
    client: Any,
    dataset: str,
    source: str,
    table: str,
    execution_id: Optional[str],
    rows_loaded: int,
) -> None:
    """
    Record a successful extraction (last_extracted_at = now). Best-effort; never
    raises into the pipeline. Append-only -- get_watermark reads the most recent.
    """
    from google.cloud import bigquery

    tid = _state_table_id(client, dataset)
    try:
        client.query(_CREATE_SQL.format(tid=tid)).result()
        client.query(
            f"""
            INSERT INTO `{tid}`
              (source, table_name, last_extracted_at, execution_id, rows_loaded)
            VALUES (@source, @table_name, CURRENT_TIMESTAMP(), @execution_id, @rows_loaded)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                    bigquery.ScalarQueryParameter("table_name", "STRING", table),
                    bigquery.ScalarQueryParameter(
                        "execution_id", "STRING", execution_id
                    ),
                    bigquery.ScalarQueryParameter(
                        "rows_loaded", "INT64", int(rows_loaded)
                    ),
                ]
            ),
        ).result()
    except Exception as e:  # noqa: BLE001
        logger.debug("Watermark save skipped for %s.%s: %s", source, table, e)


def resolve_watermark_start(
    watermark_iso: Optional[str],
    *,
    safety_days: int = 2,
    lookback_days: int = 7,
    initial_value: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Optional[str]:
    """
    PURE window math (no BigQuery). Given the stored watermark, return the effective
    start date (YYYY-MM-DD) for an adaptive incremental pull.

    Rules:
      - No watermark (first run): fall back to initial_value, else now - lookback_days.
      - With a watermark: start = watermark_date - safety_days (the late-arrival
        cushion). This naturally STRETCHES the window when behind (long outage ->
        older watermark -> wider pull) and SHRINKS it after a clean run.
      - Never returns a start AFTER `now` (clamped to today).
      - safety_days is also floored at 0; a negative config is treated as 0.

    `now` is injectable for deterministic tests.
    """
    now = now or datetime.now(timezone.utc)
    safety = max(int(safety_days), 0)

    if not watermark_iso:
        if initial_value:
            return initial_value
        return (now - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")

    try:
        wm = datetime.fromisoformat(watermark_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.warning(
            "Unparseable watermark %r; falling back to lookback", watermark_iso
        )
        return (now - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")

    # Compare on dates; strip tz for the arithmetic.
    wm_naive = wm.replace(tzinfo=None)
    now_naive = now.replace(tzinfo=None)
    start = wm_naive - timedelta(days=safety)
    if start > now_naive:
        start = now_naive  # never start in the future
    return start.strftime("%Y-%m-%d")


def watermark_enabled(table_config: dict) -> bool:
    """True if a table opts into adaptive watermarks (default False)."""
    inc = (table_config or {}).get("incremental", {}) or {}
    return bool(inc.get("use_watermark", False))


def watermark_safety_days(table_config: dict, default: int = 2) -> int:
    inc = (table_config or {}).get("incremental", {}) or {}
    try:
        return max(int(inc.get("watermark_safety_days", default)), 0)
    except (TypeError, ValueError):
        return default
