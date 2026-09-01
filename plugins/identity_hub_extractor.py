#!/usr/bin/env python3
"""
Identity Hub Extractor
======================

Orchestrates the Identity Hub pipeline which builds a cross-platform identity
graph by consuming data from all other pipelines. Creates a central bn_id
linking individual-level identifiers across data stores.

Unlike other extractors that export to Parquet → GCS → BigQuery, this pipeline
runs BigQuery-to-BigQuery transformations directly via IdentityHubBuilder.
Runs in-process — uses the orchestrator's BigQuery client and environment
routing so --env dev/prod/test controls which GCP project is targeted.

Usage:
    # Full rebuild — prod
    python orchestrate.py --source identity_hub --env prod --refresh full

    # Incremental refresh — dev, last 7 days (--lookback implies incremental)
    python orchestrate.py --source identity_hub --env dev --lookback 7

    # Dry-run (validate SQL, estimate bytes, no writes)
    python orchestrate.py --source identity_hub --env prod --refresh full --test

    # Run specific connectors only
    python orchestrate.py --source identity_hub --env prod --tables email_bridge,utm_crosswalk

    # Direct CLI (bypasses orchestrator — uses config project or GCP_PROJECT_ID)
    python shared/identity_hub.py --rebuild
    python shared/identity_hub.py --lookback 7

Author: Data Pipeline Team
Created: 2026-03-04
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Valid connector names — derived from YAML keys that have connect_* methods.
# See shared/identity_hub_registry.py. Kept as a module-level list for CLI help
# and unit tests; refreshed from config when extract() runs.
from shared.identity_hub_registry import (
    UNIMPLEMENTED_CONNECTORS,
    assert_no_enabled_unwired,
    resolve_connector_filter,
    wired_connectors_from_config,
)

# Fallback allowlist (used before config load / in unit tests without YAML).
VALID_CONNECTORS = [
    "localstorage",
    "bio_acceptor",
    "form_session_bridge",
    "wp_user_session_bridge",
    "mc_click_bridge",
    "ga4_mc_euid_bridge",
    "mc_euid_bridge",
    "email_bridge",
    "ga4_identifiers",
    "npi_email_bridge",
    "utm_crosswalk",
    "ip_device_time",
    "limesurvey_session",
    "aim_payload",
    "device_stat_id",
    "compound_device_ip",
    "aim_clickstream",
    "dmd_hcp_bridge",
    "npi_phone_bridge",
    "mailchimp_phone_bridge",
    "wp_npi_bridge",
    "wp_mc4wp_email_bridge",
]


def evaluate_upstream_preflight(checks, freshness, force=False):
    """
    Pure decision logic for the identity-hub upstream gate (no BigQuery).

    Args:
        checks: list of {name, table, max_age_days, critical} dicts (from config).
        freshness: {check_name -> age_days or None}. None means empty/missing/error
            upstream (treated as stale).
        force: when True, critical staleness is downgraded to a warning (no block).

    Returns a dict: {status: 'ok'|'failed', blocking: [str], warnings: [str],
    checks: [{name, stale, critical, age_days, threshold}]}.
    """
    result = {"status": "ok", "blocking": [], "warnings": [], "checks": []}
    for chk in checks or []:
        name = chk.get("name") or chk.get("table") or "?"
        thresh = int(chk.get("max_age_days", 7))
        critical = bool(chk.get("critical", False))
        age = freshness.get(name)
        stale = age is None or age > thresh
        result["checks"].append(
            {
                "name": name,
                "stale": stale,
                "critical": critical,
                "age_days": age,
                "threshold": thresh,
            }
        )
        if not stale:
            continue
        age_str = "no data" if age is None else f"{age}d old (> {thresh}d)"
        detail = f"{name}: {age_str}"
        if critical and not force:
            result["blocking"].append(detail)
            result["status"] = "failed"
        else:
            # non-critical, or critical bypassed by --force
            result["warnings"].append(
                detail + (" [force-bypassed critical]" if critical and force else "")
            )
    return result


def run_upstream_preflight(bq_client, config, force=False, dry_run=False):
    """
    Upstream freshness gate for the identity hub. Reads each configured source's
    latest ETL load timestamp and evaluates staleness (evaluate_upstream_preflight).
    Best-effort I/O: a query error makes that source look stale (None age), which
    only blocks if the source is critical. Skipped on dry-run and when disabled.
    """
    pf_cfg = (config or {}).get("upstream_preflight", {}) or {}
    checks = pf_cfg.get("checks", []) or []
    if dry_run or not pf_cfg.get("enabled", False) or not checks:
        return {
            "status": "ok",
            "blocking": [],
            "warnings": [],
            "checks": [],
            "skipped": True,
        }

    freshness = {}
    for chk in checks:
        name = chk.get("name") or chk.get("table")
        table = chk.get("table")
        date_col = chk.get("date_column", "extracted_at")
        if not table:
            freshness[name] = None
            continue
        try:
            # Identifiers are config-controlled (not user input); interpolated.
            sql = (
                f"SELECT DATE_DIFF(CURRENT_DATE(), DATE(MAX(`{date_col}`)), DAY) "
                f"AS age_days FROM `{table}`"
            )
            row = list(bq_client.query(sql).result())[0]
            freshness[name] = None if row.age_days is None else int(row.age_days)
        except Exception as e:  # noqa: BLE001 - treat unreadable source as stale
            logger.warning("Upstream preflight: could not read %s: %s", table, e)
            freshness[name] = None

    pf = evaluate_upstream_preflight(checks, freshness, force=force)
    for w in pf["warnings"]:
        logger.warning("Upstream preflight WARN: %s", w)
    if pf["status"] == "ok" and not pf["warnings"]:
        logger.info("Upstream preflight: all %d source(s) fresh.", len(checks))
    return pf


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: Optional[str],
    refresh_mode: str,
    lookback_days: Optional[int],
    start_date: Optional[str],
    end_date: Optional[str],
    test_mode: bool,
    batch_size: Optional[int],
    max_retries: int,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    rebuild: bool = False,
    bq_client: Any = None,
    execution_id: str = None,
    parallel_workers: Optional[int] = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Identity Hub pipeline entry point — runs in-process using the
    orchestrator's BigQuery client for proper environment routing.

    Args:
        config: Pipeline configuration from identity_hub.yaml
        sites: Not used (single source)
        tables: Optional list of connector names to run
        group: Not used
        refresh_mode: 'full' or 'incremental'
        lookback_days: Days to look back for incremental
        start_date: Start date for processing (YYYY-MM-DD)
        end_date: End date for processing (YYYY-MM-DD)
        test_mode: If True, runs in dry-run mode (no writes)
        rebuild: Whether to rebuild from scratch
        bq_client: Pre-initialized BigQuery client from orchestrator (env-routed)
        execution_id: Orchestrator execution ID for tracking
        **kwargs: Additional parameters (ignored)

    Returns:
        Dict with pipeline execution statistics
    """
    from shared.identity_hub import IdentityHubBuilder, load_config

    logger.info("")
    logger.info("=" * 80)
    logger.info("IDENTITY HUB PIPELINE (in-process)")
    logger.info("=" * 80)

    # ── Resolve BigQuery client ──
    # Prefer orchestrator's bq_client (env-routed); fall back to default
    if bq_client is None:
        from google.cloud import bigquery

        hub_config = _get_hub_config(config)
        project = hub_config.get("project", "bi-data-391216")
        bq_client = bigquery.Client(project=project)
        logger.info(
            f"No orchestrator client -- created standalone client for {project}"
        )

    project_id = bq_client.project
    logger.info(f"Target GCP project: {project_id}")

    # ── Load hub-specific config ──
    hub_config = _get_hub_config(config)

    # ── Determine mode and dates ──
    # Both 'full' and 'rebuild' mean "fetch maximum data" from sources;
    # the merge-side DELETE-not-matched is gated separately on refresh_mode='rebuild'.
    is_rebuild = rebuild or refresh_mode in ("full", "rebuild")
    incremental = not is_rebuild

    # ── Reject isolation flags that do NOT isolate this pipeline ──
    # schema_prefix/schema_suffix affix STAGING TABLE NAMES in the
    # Parquet -> GCS -> BigQuery flow (see shared/staging_lifecycle.py
    # apply_table_affix). Identity Hub is BigQuery-to-BigQuery and never touches
    # that path, so these were accepted and silently ignored -- an operator who
    # passed them expecting a sandboxed run would have rebuilt PRODUCTION
    # instead. Fail loudly and name the flag that actually works.
    ignored_affixes = [
        name
        for name, value in (
            ("--schema-prefix", schema_prefix),
            ("--schema-suffix", schema_suffix),
        )
        if value
    ]
    if ignored_affixes:
        msg = (
            f"{', '.join(ignored_affixes)} does not isolate identity_hub. Those "
            f"flags rename staging tables in the Parquet/GCS pipeline; identity_hub "
            f"writes BigQuery-to-BigQuery and would have rebuilt PRODUCTION. "
            f"For an isolated run use the dataset override instead:\n"
            f"  python shared/identity_hub.py --rebuild "
            f"--test-dataset identity_hub_data_test --sample-days 30\n"
            f"or pass test_dataset=<name> through the orchestrator."
        )
        logger.error(msg)
        return {
            "success": False,
            "status": "unsupported_isolation_flag",
            "errors": [msg],
            "table_files": {},
        }

    # ── Test-dataset isolation (the mechanism that DOES work) ──
    # Redirects every hub write -- hub, xref, neighbors, persistence, manifest --
    # plus staging, to a separate dataset. Production is untouched.
    test_dataset = kwargs.get("test_dataset")
    if test_dataset:
        logger.warning("=" * 80)
        logger.warning(f"TEST MODE -- writing to {test_dataset}, NOT production")
        logger.warning("=" * 80)

    if lookback_days and not start_date:
        start_date = (
            datetime.now(timezone.utc) - timedelta(days=lookback_days)
        ).strftime("%Y-%m-%d")

    # ── Resolve connector filter ──
    try:
        connector_filter = _resolve_connectors(tables, config)
    except ValueError as e:
        logger.error(str(e))
        return {
            "success": False,
            "status": "invalid_connectors",
            "errors": [str(e)],
            "table_files": {},
        }

    logger.info(f"Mode: {'FULL REBUILD' if is_rebuild else 'INCREMENTAL'}")
    logger.info(f"Lookback days: {lookback_days}")
    logger.info(f"Date range: {start_date} -> {end_date or 'now'}")
    logger.info(f"Dry run: {test_mode}")
    if connector_filter:
        logger.info(f"Connectors: {connector_filter}")
    if execution_id:
        logger.info(f"Execution ID: {execution_id}")
    logger.info("")

    pipeline_start = datetime.now()

    # Force-overwrite flag (bypass the shrink safeguard). Accepted via kwargs
    # so callers can opt-in without requiring a formal param. Default False.
    force_overwrite = bool(kwargs.get("force_overwrite", False))

    # Upstream quality gate: do not build the identity graph on stale/empty
    # upstream. CRITICAL sources hard-block (unless force/force_overwrite/dry-run);
    # non-critical sources warn. See configs/identity_hub.yaml: upstream_preflight.
    pf = run_upstream_preflight(
        bq_client,
        config,
        force=force_overwrite or bool(kwargs.get("force", False)),
        dry_run=test_mode,
    )
    if pf["status"] == "failed":
        msg = (
            "Identity Hub upstream preflight FAILED -- not building on stale/empty "
            "upstream: " + "; ".join(pf["blocking"])
        )
        logger.error(msg)
        return {
            "success": False,
            "status": "preflight_failed",
            "errors": [msg],
            "table_files": {},
            "preflight": pf,
        }

    try:
        builder = IdentityHubBuilder(
            client=bq_client,
            config=hub_config,
            start_date=start_date,
            end_date=end_date,
            incremental=incremental,
            connector_filter=connector_filter,
            project_override=project_id,
            force_overwrite=force_overwrite,
            # None in normal runs -> builder falls back to config's
            # output_dataset (identity_hub_data). Set only for sandboxed runs.
            output_dataset_override=test_dataset,
        )

        result = builder.run(dry_run=test_mode)

        elapsed = (datetime.now() - pipeline_start).total_seconds()

        # Extract stats for orchestrator
        uf_stats = result.get("union_find", {})
        hub_stats = result.get("hub_table", {})
        total_edges = hub_stats.get("rows", 0)
        total_components = uf_stats.get("components", 0)

        logger.info("")
        logger.info(f"Identity Hub completed in {elapsed:.1f}s")
        logger.info(f"  Hub rows: {total_edges:,}")
        logger.info(f"  Components: {total_components:,}")
        logger.info(
            f"  Tier1: {uf_stats.get('tier1_components', 0):,}  Tier2: {uf_stats.get('tier2_components', 0):,}"
        )

        # Build table_rows from connector stats for orchestrator reporting
        table_rows = {}
        for section, stats in result.items():
            if isinstance(stats, dict) and "edges" in stats:
                table_rows[section] = stats["edges"]

        return {
            "success": True,
            "total_rows": total_edges,
            "table_files": {},
            "table_rows": table_rows,
            "errors": [],
            "identity_hub_stats": result,
        }

    except Exception as e:
        elapsed = (datetime.now() - pipeline_start).total_seconds()
        logger.error(
            f"Identity Hub pipeline failed after {elapsed:.1f}s: {e}", exc_info=True
        )
        return {
            "success": False,
            "total_rows": 0,
            "table_files": {},
            "table_rows": {},
            "errors": [str(e)],
        }


def _get_hub_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the identity_hub section from the full pipeline config."""
    hub_config = config.get("identity_hub", {})
    if not hub_config:
        # Config was already the hub section (direct CLI usage)
        return config
    # Ensure project is set — fall back to source.connection.project
    if "project" not in hub_config:
        hub_config["project"] = (
            config.get("source", {})
            .get("connection", {})
            .get("project", "bi-data-391216")
        )
    return hub_config


def _resolve_connectors(
    tables: List[str], config: Dict[str, Any]
) -> Optional[List[str]]:
    """
    Resolve --tables argument to connector filter list.

    If tables match the default config resource list, return None (run all).
    Otherwise validate each name against the YAML/wired allowlist.
    """
    global VALID_CONNECTORS
    hub_config = (config or {}).get("identity_hub") or config or {}
    try:
        assert_no_enabled_unwired(hub_config)
        refreshed = wired_connectors_from_config(hub_config)
        if refreshed:
            VALID_CONNECTORS = refreshed
    except Exception as e:
        logger.warning("Connector registry refresh failed, using fallback list: %s", e)

    if not tables:
        return None

    config_resource_tables = set()
    if config and "resources" in config:
        config_resource_tables = set(config["resources"].keys())
    if set(tables) == config_resource_tables:
        return None

    return resolve_connector_filter(tables, hub_config, valid=VALID_CONNECTORS)


def get_available_tables(config: Dict[str, Any], group: str = None) -> List[str]:
    """Return available connector names for orchestrator table discovery."""
    return list(VALID_CONNECTORS)


def main():
    """CLI entry point for testing."""
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="Identity Hub Pipeline")
    parser.add_argument(
        "--env",
        choices=["prod", "dev", "test"],
        default="prod",
        help="Target environment",
    )
    parser.add_argument(
        "--refresh", choices=["full", "incremental"], default="incremental"
    )
    parser.add_argument("--rebuild", action="store_true", help="Full rebuild")
    parser.add_argument("--lookback", type=int, help="Days to look back")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument(
        "--connectors", nargs="+", choices=VALID_CONNECTORS, help="Connectors to run"
    )
    parser.add_argument(
        "--list-connectors", action="store_true", help="List available connectors"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if args.list_connectors:
        print("\nAvailable connectors:")
        for c in VALID_CONNECTORS:
            print(f"  {c}")
        return

    # Set env for project routing
    ENV_TO_PROJECT = {
        "prod": "bi-data-391216",
        "dev": "bi-dev-391216",
        "test": "bi-dev-391216",
    }
    project_id = ENV_TO_PROJECT[args.env]
    os.environ["GCP_PROJECT_ID"] = project_id

    # Load config
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "configs",
        "identity_hub.yaml",
    )

    import yaml

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Create env-routed client
    from google.cloud import bigquery

    bq_client = bigquery.Client(project=project_id)

    result = run_pipeline(
        config=config,
        sites=[],
        tables=args.connectors or [],
        group=None,
        refresh_mode=args.refresh,
        lookback_days=args.lookback,
        start_date=args.start_date,
        end_date=args.end_date,
        test_mode=args.dry_run,
        batch_size=None,
        max_retries=3,
        rebuild=args.rebuild,
        bq_client=bq_client,
    )

    if not result.get("success"):
        sys.exit(1)


if __name__ == "__main__":
    main()
