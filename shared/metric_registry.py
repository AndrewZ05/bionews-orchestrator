#!/usr/bin/env python3
"""
Semantic Metric Versioning
==========================

Tracks WHEN the meaning of a metric changes -- e.g. Meta deprecating `reach` in
favor of `views` (BI-486), or IG replacing `impressions` with `views`. Structural
drift (a column's type) is auto-detected by schema_changes; a *definitional* change
is a human decision you encode in config at the moment you make it. This registry
makes those decisions effective-dated and queryable, so an analyst who sees
"facebook reach dropped 40% on 2026-06-11" can ask "did the definition change on
that date?" instead of guessing business movement.

Sources declare changes in their YAML:

  metric_definitions:
    - metric: post_reach
      effective_date: "2026-06-11"
      definition: "Unique users who saw the post (Meta v25.0)."
      change_reason: "Meta deprecated reach; superseded by views."
      replaces: post_impressions_unique   # optional

Layers:
  - resolve_definition() / changes_near(): PURE effective-date logic (unit-tested,
    no BigQuery).
  - sync_definitions_to_bq(): mirror the config entries into
    orchestrator_monitoring.metric_definitions (effective-dated rows) for SQL access.
  - explain_metric(): the analyst lookup ("what did X mean on date D / when last
    changed").
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MONITORING_DATASET = "orchestrator_monitoring"
TABLE_NAME = "metric_definitions"


def _as_date(value: Any) -> Optional[date]:
    """Coerce a YYYY-MM-DD string / date / datetime to a date. None on failure."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:  # noqa: BLE001
        return None


def normalize_entries(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate + normalize raw config metric_definitions into clean records, dropping
    malformed ones (missing metric or unparseable effective_date). Sorted by
    (metric, effective_date).
    """
    out = []
    for e in raw or []:
        metric = (e.get("metric") or "").strip()
        eff = _as_date(e.get("effective_date"))
        if not metric or eff is None:
            logger.debug("Skipping malformed metric_definition: %s", e)
            continue
        out.append(
            {
                "metric": metric,
                "effective_date": eff,
                "definition": (e.get("definition") or "").strip(),
                "change_reason": (e.get("change_reason") or "").strip(),
                "replaces": (e.get("replaces") or "").strip() or None,
            }
        )
    out.sort(key=lambda r: (r["metric"], r["effective_date"]))
    return out


def resolve_definition(
    entries: List[Dict[str, Any]], metric: str, as_of: date
) -> Optional[Dict[str, Any]]:
    """
    PURE: the definition of `metric` in effect on `as_of` -- the entry with the
    latest effective_date <= as_of. None if the metric has no entry effective by
    then. `entries` is normalize_entries() output.
    """
    candidates = [
        e for e in entries if e["metric"] == metric and e["effective_date"] <= as_of
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda e: e["effective_date"])


def changes_near(
    entries: List[Dict[str, Any]],
    target: date,
    window_days: int = 7,
    metric: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    PURE: definition changes whose effective_date is within +/- window_days of
    `target` (optionally filtered to one metric). This is what lets an anomaly on a
    given date be cross-referenced against "did a definition change right then".
    Sorted by absolute distance from target (closest first).
    """
    hits = []
    for e in entries:
        if metric and e["metric"] != metric:
            continue
        delta = abs((e["effective_date"] - target).days)
        if delta <= window_days:
            hits.append({**e, "days_from_target": (e["effective_date"] - target).days})
    hits.sort(key=lambda e: abs(e["days_from_target"]))
    return hits


def load_entries_for_source(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalized metric_definitions from a loaded source config ([] if none)."""
    return normalize_entries((config or {}).get("metric_definitions", []))


# --- BigQuery sync + lookup --------------------------------------------------


def _table_id(client) -> str:
    return f"{client.project}.{MONITORING_DATASET}.{TABLE_NAME}"


def sync_definitions_to_bq(client, source: str, entries: List[Dict[str, Any]]) -> int:
    """
    Mirror this source's effective-dated definitions into the registry table
    (idempotent: delete this source's rows, re-insert). Best-effort; returns rows
    written. `entries` is normalize_entries() output.
    """
    table_id = _table_id(client)
    from google.cloud import bigquery

    schema = [
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("metric", "STRING"),
        bigquery.SchemaField("effective_date", "DATE"),
        bigquery.SchemaField("definition", "STRING"),
        bigquery.SchemaField("change_reason", "STRING"),
        bigquery.SchemaField("replaces", "STRING"),
        bigquery.SchemaField("synced_at", "TIMESTAMP"),
    ]
    try:
        client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
    except Exception as e:  # noqa: BLE001
        logger.warning("metric_definitions ensure-table failed: %s", e)
        return 0

    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE source = @s",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("s", "STRING", source)]
            ),
        ).result()
    except Exception as e:  # noqa: BLE001
        logger.debug("metric_definitions delete-existing skipped: %s", e)

    if not entries:
        return 0
    rows = [
        {
            "source": source,
            "metric": e["metric"],
            "effective_date": e["effective_date"].isoformat(),
            "definition": e["definition"],
            "change_reason": e["change_reason"],
            "replaces": e["replaces"],
            "synced_at": datetime.now(timezone.utc).isoformat(),
        }
        for e in entries
    ]
    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            logger.warning("metric_definitions insert errors: %s", errors)
            return 0
        return len(rows)
    except Exception as e:  # noqa: BLE001
        logger.warning("metric_definitions insert failed: %s", e)
        return 0


def explain_metric(
    config: Dict[str, Any], metric: str, as_of: Optional[date] = None
) -> str:
    """
    Human-readable answer: what did `metric` mean on `as_of` (default today), and
    when did it last change. Pure (reads config, no BigQuery).
    """
    entries = load_entries_for_source(config)
    metric_entries = [e for e in entries if e["metric"] == metric]
    if not metric_entries:
        return f"No recorded definition for '{metric}'."
    if as_of is None:
        # date.today() is unavailable in some sandboxed contexts; fall back to the
        # latest effective_date if needed.
        try:
            as_of = datetime.now().date()
        except Exception:  # noqa: BLE001
            as_of = max(e["effective_date"] for e in metric_entries)

    current = resolve_definition(entries, metric, as_of)
    lines = [f"Metric '{metric}' as of {as_of.isoformat()}:"]
    if current:
        lines.append(f"  definition: {current['definition'] or '(none recorded)'}")
        lines.append(
            f"  effective since: {current['effective_date'].isoformat()}"
            + (
                f" (reason: {current['change_reason']})"
                if current["change_reason"]
                else ""
            )
        )
        if current["replaces"]:
            lines.append(f"  replaces: {current['replaces']}")
    else:
        first = min(metric_entries, key=lambda e: e["effective_date"])
        lines.append(
            f"  no definition in effect yet (first recorded "
            f"{first['effective_date'].isoformat()})."
        )
    if len(metric_entries) > 1:
        history = ", ".join(
            f"{e['effective_date'].isoformat()}" for e in metric_entries
        )
        lines.append(f"  definition changes on: {history}")
    return "\n".join(lines)
