"""
Pre-flight run-size estimate.

Projects how long a run will take from historical per-table durations in
orchestrator_monitoring.table_processing, so an operator gets a loud warning
(or a hard stop without --force) before kicking off a run that would plausibly
run for hours. The lock heartbeat keeps a long run safe, but a multi-hour run
is usually a sign the work should be split by group -- this surfaces that
BEFORE the lock is taken and extraction begins.

Estimate model:
  per_table_seconds = median(duration_seconds) over recent successful runs of
                      that (source, table); fall back to the source median, then
                      to a conservative default.
  projected_seconds = sum(per_table_seconds) * site_factor
where site_factor accounts for per-site iteration sources (WordPress): a run
over N sites repeats each table's work ~per-site. Non-per-site sources use
site_factor = 1.
"""

import logging
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

DEFAULT_PER_TABLE_SECONDS = 60
# Sources whose extraction iterates per-site, so projected time scales with the
# number of sites in the run.
PER_SITE_SOURCES = {"wordpress"}
DEFAULT_CEILING_MINUTES = 90
# Conservative per-site source size assumed when a run targets "all" sites and
# no explicit count is configured. Keeps the gate from green-lighting a
# 100+ site run as if it were one site. WordPress runs ~131 sites.
DEFAULT_ASSUMED_SITE_COUNT = 131


def _resolve_site_factor(config, source, sites) -> int:
    """
    How many times the per-table cost repeats. For per-site sources, this is the
    site count. A run targeting "all" sites (or the default no-site path) must
    NOT collapse to 1 -- that would let the gate green-light a full multi-site
    run as if it were a single site. Resolve "all"/empty to a configured or
    conservative-default count so the projection stays honest.
    """
    if source not in PER_SITE_SOURCES:
        return 1

    explicit = [s for s in (sites or []) if str(s).lower() != "all"]
    targets_all = (not sites) or any(str(s).lower() == "all" for s in sites)

    if explicit and not targets_all:
        return len(explicit) if len(explicit) > 1 else 1

    # "all" or default: use a configured estimate, else a conservative default.
    pf = (config.get("pipeline", {}) or {}).get("preflight", {}) or {}
    assumed = pf.get("assumed_site_count")
    if assumed is None:
        # Fall back to a configured site list length if present, else the default.
        sites_cfg = (config.get("source", {}) or {}).get("sites")
        if isinstance(sites_cfg, list) and sites_cfg:
            assumed = len(sites_cfg)
        else:
            assumed = DEFAULT_ASSUMED_SITE_COUNT
    try:
        assumed = int(assumed)
    except (TypeError, ValueError):
        assumed = DEFAULT_ASSUMED_SITE_COUNT
    # If the user named explicit sites alongside "all" (unusual), take the max.
    return max(assumed, len(explicit)) if explicit else assumed


def _ceiling_minutes(config: Dict[str, Any]) -> int:
    """Configurable run-time ceiling: pipeline.preflight.max_run_minutes."""
    pf = (config.get("pipeline", {}) or {}).get("preflight", {}) or {}
    try:
        return int(pf.get("max_run_minutes", DEFAULT_CEILING_MINUTES))
    except (TypeError, ValueError):
        return DEFAULT_CEILING_MINUTES


def _median_durations(
    client: bigquery.Client, source: str, lookback_days: int = 30
) -> Dict[str, float]:
    """
    Median duration_seconds per table for a source over recent successful runs.
    Returns {} on any error (estimate degrades to the default, never blocks).
    """
    query = """
    SELECT table_name,
           APPROX_QUANTILES(duration_seconds, 2)[OFFSET(1)] AS median_seconds
    FROM `{project}.orchestrator_monitoring.table_processing`
    WHERE source = @source
      AND duration_seconds IS NOT NULL
      AND duration_seconds > 0
      AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    GROUP BY table_name
    """.format(
        project=client.project
    )
    try:
        job = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("source", "STRING", source),
                    bigquery.ScalarQueryParameter("days", "INT64", lookback_days),
                ]
            ),
        )
        return {r.table_name: float(r.median_seconds) for r in job.result()}
    except Exception as e:  # noqa: BLE001 - estimate is best-effort
        logger.debug("preflight: could not read historical durations: %s", e)
        return {}


def estimate_run_seconds(
    client: bigquery.Client,
    source: str,
    tables: List[str],
    sites: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Project total run seconds. Returns a dict with projected_seconds, the
    site_factor used, and how many tables fell back to a default duration.

    History-backed per-table medians are already whole-run (all sites), so they
    are summed as-is. site_factor scales ONLY the flat per-table default used when
    a table has no history at all (see the loop below and _resolve_site_factor).

    `config` supplies the per-site count estimate used when a run targets "all"
    sites (see _resolve_site_factor); omitting it uses the conservative default.
    """
    medians = _median_durations(client, source)
    source_median = sorted(medians.values())[len(medians) // 2] if medians else None

    site_factor = _resolve_site_factor(config or {}, source, sites)

    # IMPORTANT: a table_processing row records the WHOLE-run, all-sites duration
    # for a table (one row per (run, table); duration covers every site already).
    # So a history-backed median must NOT be multiplied by site_factor -- doing so
    # double-counts the per-site fan-out and over-projects ~site_factor-fold (this
    # is what hard-blocked every WordPress group: core projected ~500 min vs ~22
    # min actual). site_factor exists only to scale the per-table DEFAULT, which is
    # a flat single-table guess with no site awareness.
    total = 0.0
    fell_back = 0
    for t in tables:
        secs = medians.get(t)
        if secs is not None:
            # Already whole-run (all sites) -- use as-is.
            total += secs
        else:
            # No history for this table: fall back. Prefer the source median (also
            # whole-run, so as-is); only the flat DEFAULT is single-table and gets
            # scaled by site_factor.
            if source_median:
                total += source_median
            else:
                total += DEFAULT_PER_TABLE_SECONDS * site_factor
            fell_back += 1

    projected = total

    return {
        "projected_seconds": projected,
        "projected_minutes": round(projected / 60, 1),
        "site_factor": site_factor,
        "tables": len(tables),
        "tables_using_default": fell_back,
        "had_history": bool(medians),
    }


def preflight_check(
    client: bigquery.Client,
    config: Dict[str, Any],
    source: str,
    tables: List[str],
    sites: Optional[List[str]] = None,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Run the size estimate and decide whether to proceed.

    Returns {"ok": bool, "estimate": <estimate dict>, "ceiling_minutes": int,
             "message": str}. ok=False means the projection exceeds the ceiling
    and --force was not supplied; the caller should abort. Never raises -- a
    failed estimate is logged and treated as ok=True (fail-open, since the lock
    heartbeat is the real safety net).
    """
    ceiling = _ceiling_minutes(config)
    try:
        est = estimate_run_seconds(client, source, tables, sites, config=config)
    except Exception as e:  # noqa: BLE001
        logger.debug("preflight: estimate failed, proceeding: %s", e)
        return {
            "ok": True,
            "estimate": None,
            "ceiling_minutes": ceiling,
            "message": "preflight estimate unavailable; proceeding",
        }

    over = est["projected_minutes"] > ceiling
    if not over:
        return {
            "ok": True,
            "estimate": est,
            "ceiling_minutes": ceiling,
            "message": (
                f"preflight: ~{est['projected_minutes']} min projected "
                f"({est['tables']} tables, site_factor {est['site_factor']}); "
                f"under {ceiling} min ceiling"
            ),
        }

    detail = (
        f"~{est['projected_minutes']} min projected for {est['tables']} tables "
        f"(site_factor {est['site_factor']}), over the {ceiling} min ceiling. "
        f"Consider splitting the run by group."
    )
    if force:
        return {
            "ok": True,
            "estimate": est,
            "ceiling_minutes": ceiling,
            "message": f"preflight: {detail} Proceeding anyway (--force).",
        }
    return {
        "ok": False,
        "estimate": est,
        "ceiling_minutes": ceiling,
        "message": (
            f"preflight BLOCKED: {detail} Re-run with --force to override, or "
            f"split into separate --groups invocations."
        ),
    }
