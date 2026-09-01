#!/usr/bin/env python3
# Preventive detector: AT-RISK FRESHNESS TREND. A table whose days_behind grows
# monotonically across consecutive freshness runs (1 -> 2 -> 3) will breach the
# SLA tomorrow -- flag it TODAY, while it is still PASS/WARNING. Runs inside the
# anomaly audit (the "find issues early" audit) reading the freshness audit's
# results history cross-dataset.
"""Freshness-trend (at-risk) detection.

``fetch_days_behind_history`` pulls each table's last N freshness runs;
``detect_at_risk`` is PURE: it flags tables whose days_behind strictly
increased across at least ``min_runs`` consecutive runs AND whose latest status
is not already FAIL/ERROR (those are alarmed by the freshness audit itself --
this detector exists for the ones still quietly decaying).

Best-effort at the fetch layer: a missing/empty freshness results table returns
{} and the detector simply produces no findings.
"""

import logging
from typing import Any, Dict, List, Tuple

from shared.anomaly_audit import (
    DETECTOR_FRESHNESS_TREND,
    SCOPE_ACTIVE,
    SEVERITY_MEDIUM,
    STATUS_ANOMALY,
)
from shared.freshness_audit import _validate_identifier

logger = logging.getLogger(__name__)

DEFAULT_RUNS = 5  # how many recent runs to fetch per table
DEFAULT_MIN_RUNS = 3  # monotonic growth across at least this many runs flags

_HISTORY_SQL = """-- Last N freshness runs per table (newest first) for trend analysis.
SELECT dataset_name, table_name, audited_at, days_behind, audit_status
FROM `{fds}`.`{frt}`
WHERE days_behind IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dataset_name, table_name ORDER BY audited_at DESC
) <= @n"""


def fetch_days_behind_history(
    client, freshness_dataset: str, freshness_table: str, n_runs: int = DEFAULT_RUNS
) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """Fetch per-table freshness history: {(dataset, table): rows newest-first}.

    Best-effort: any failure returns {} (trend detection silently disabled for
    the run -- e.g. the freshness audit has never written results here).
    """
    try:
        safe_fds = _validate_identifier(freshness_dataset, "dataset")
        safe_frt = _validate_identifier(freshness_table, "table")
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("n", "INT64", int(n_runs))]
        )
        sql = _HISTORY_SQL.format(fds=safe_fds, frt=safe_frt)
        history: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for r in client.query(sql, job_config=job_config).result():
            key = (r["dataset_name"], r["table_name"])
            history.setdefault(key, []).append(
                {
                    "audited_at": r["audited_at"],
                    "days_behind": int(r["days_behind"]),
                    "audit_status": r["audit_status"],
                }
            )
        # Ensure newest-first per table regardless of result ordering.
        for rows in history.values():
            rows.sort(key=lambda x: x["audited_at"], reverse=True)
        logger.info("Fetched freshness trend history for %d table(s)", len(history))
        return history
    except Exception as exc:  # noqa: BLE001 - trend input is best-effort
        logger.warning("Could not fetch freshness history (trend disabled): %s", exc)
        return {}


def detect_at_risk(
    history: Dict[Tuple[str, str], List[Dict[str, Any]]],
    today,
    min_runs: int = DEFAULT_MIN_RUNS,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """PURE: flag tables whose days_behind rose strictly across recent runs.

    Conditions per table (rows newest-first):
      * at least ``min_runs`` history rows;
      * days_behind STRICTLY increased oldest -> newest across the most recent
        ``min_runs`` rows (1 -> 2 -> 3; flat or noisy series do not flag);
      * the LATEST status is not already FAIL/ERROR (already alarmed elsewhere).

    Returns {(dataset, table): finding dict} so the caller can build result rows
    with its own table context.
    """
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for key, rows in history.items():
        if len(rows) < min_runs:
            continue
        latest = rows[0]
        if latest["audit_status"] in ("FAIL", "ERROR"):
            continue
        window = rows[:min_runs]  # newest-first
        series = [r["days_behind"] for r in reversed(window)]  # oldest-first
        if not all(series[i] < series[i + 1] for i in range(len(series) - 1)):
            continue
        message = (
            "days_behind rising over last {0} runs: {1} -- "
            "on track to breach the freshness SLA".format(
                min_runs, " -> ".join(str(v) for v in series)
            )
        )
        logger.warning("AT RISK %s.%s: %s", key[0], key[1], message)
        out[key] = {
            "detector": DETECTOR_FRESHNESS_TREND,
            "anomaly_date": today,
            "observed_count": latest["days_behind"],
            "expected_low": None,
            "median_count": None,
            "date_lag_days": latest["days_behind"],
            "severity": SEVERITY_MEDIUM,
            "scope": SCOPE_ACTIVE,
            "audit_status": STATUS_ANOMALY,
            "error_message": message,
        }
    return out
