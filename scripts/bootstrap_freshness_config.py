#!/usr/bin/env python3
"""
One-shot, idempotent provisioning for the Table Freshness Auditing feature.

As of the YAML-driven redesign, the audit's list of tables to check is DERIVED at
runtime from each source's configs/<source>.yaml (see
shared.freshness_audit.yaml_target_loader) -- there is NO BigQuery config table to
seed. The only BigQuery object the audit needs is its RESULTS sink, and the runner
creates that automatically on every run. This script just lets an operator
pre-create the results dataset + table (e.g. before wiring up a dashboard).

Usage:
    python scripts/bootstrap_freshness_config.py
    python scripts/bootstrap_freshness_config.py --project bi-data-391216
    python scripts/bootstrap_freshness_config.py --dry-run

ASCII-only logging.
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Add project root to path so we can import shared.* when run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load .env (GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS, etc.) so this script
# is runnable standalone like orchestrate.py. Safe no-op if python-dotenv or the
# file is absent.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from shared import bigquery_client as bq_client_module
from shared.config_loader import load_config
from shared.freshness_audit import result_writer

logger = logging.getLogger(__name__)


def _resolve_project(cli_project, config):
    """Resolve the GCP project. Priority: --project, GCP_PROJECT_ID, config."""
    if cli_project:
        return cli_project
    env_project = os.environ.get("GCP_PROJECT_ID")
    if env_project:
        return env_project
    return (config.get("bigquery") or {}).get("project")


def main(argv=None):
    """Ensure the freshness results dataset + table exist. Returns an exit code."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Provision the Table Freshness Auditing results dataset + table."
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project (default: GCP_PROJECT_ID env var).",
    )
    parser.add_argument(
        "--env",
        default=os.environ.get("ORCHESTRATOR_ENV", "prod"),
        help="Environment label for logging only (default: prod).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log the actions that would be taken without touching BigQuery.",
    )
    args = parser.parse_args(argv)

    logger.info(
        "Bootstrap freshness results starting (env=%s, dry_run=%s)",
        args.env,
        args.dry_run,
    )

    resolved_creds = bq_client_module.setup_gcp_credentials()
    if resolved_creds:
        logger.info("Using GCP credentials: %s", resolved_creds)
    else:
        logger.warning("No GCP credentials file resolved -- relying on ambient ADC.")

    try:
        config = load_config("table_freshness")
    except FileNotFoundError as exc:
        logger.error("Could not load configs/table_freshness.yaml: %s", exc)
        return 1

    audit_cfg = config.get("audit") or {}
    results_dataset = audit_cfg.get("results_dataset", "freshness_audit")
    results_table = audit_cfg.get("results_table", "table_freshness_results")
    location = (config.get("bigquery") or {}).get("location", "US")

    project = _resolve_project(args.project, config)
    if not project:
        logger.error("No GCP project resolved. Pass --project or set GCP_PROJECT_ID.")
        return 1
    logger.info("Target project: %s", project)
    logger.info("Results sink: %s.%s.%s", project, results_dataset, results_table)

    if args.dry_run:
        logger.info(
            "[dry-run] would ensure results dataset + table exist: %s.%s.%s",
            project,
            results_dataset,
            results_table,
        )
        logger.info("Bootstrap freshness results completed (dry-run).")
        return 0

    client = bq_client_module.get_bigquery_client(project=project)
    try:
        result_writer.ensure_results_dataset(client, results_dataset, location)
        result_writer.ensure_results_table(client, results_dataset, results_table)
    except Exception as exc:  # noqa: BLE001 -- top-level guard for a CLI script.
        logger.error("Bootstrap failed: %s", exc)
        return 1

    logger.info("Bootstrap freshness results completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
