#!/usr/bin/env python3
"""
Universal Pipeline Orchestrator
Clean, YAML-driven data extraction orchestrator using plugin architecture.
"""

import os

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
os.environ.setdefault("GRPC_GO_LOG_VERBOSITY_LEVEL", "0")
os.environ.setdefault("GRPC_GO_LOG_SEVERITY_LEVEL", "off")
import sys
import atexit
import logging

logging.getLogger("grpc").setLevel(logging.ERROR)
logging.getLogger("google.auth").setLevel(logging.ERROR)
import uuid
import subprocess
import argparse
import traceback
import dotenv
import importlib
import yaml
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# Load environment variables from .env file
dotenv.load_dotenv()

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent))

# Import shared modules
from shared.config_loader import load_config
from shared.cli_utils import setup_pipeline_clients, get_bigquery_client
from shared.extractor_utils import apply_table_affix
from shared.monitoring import (
    start_execution,
    complete_execution,
    fail_execution,
    create_centralized_job,
    update_job_status,
    capture_extractor_logs,
    send_job_failure_alert,
    send_job_success_alert,
    clear_incremental_state,
    attach_bigquery_usage_to_stats,
)
from shared.notifications import initialize_notifications, send_notification
from shared.gcs_storage import (
    upload_file_to_gcs,
    verify_gcs_object_access,
    get_gcs_bucket_name,
)
from shared.external_tables import create_external_table_from_staging
from shared.bigquery_utils import process_hash_merge, process_snapshot_replace
from shared.pipeline_validator import validate_pipeline, generate_validation_report
from shared.recovery_cleanup import cleanup_recovery_files
from shared.log_config import configure_logging, get_log_level_description
from shared.table_tracking import (
    start_table_processing,
    update_table_processing,
    complete_table_processing,
)
from shared.job_manager import cleanup_orphaned_jobs, MONITORING_DATASET
from shared.job_lock_bq import (
    acquire_job_lock,
    release_job_lock,
    cleanup_expired_locks,
    get_active_locks,
    force_release_lock,
    is_lock_lost,
)

# NOTE: Extractor imports are now LAZY (loaded on demand via get_extractor_function)
# This avoids hanging on facebook_business SDK import when running other extractors
# Only import generic_extractor as fallback (it doesn't have heavy dependencies)
from plugins.generic_extractor import run_pipeline as generic_run_pipeline

# Configure basic logging (will be reconfigured based on --log-level argument)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress Google API discovery cache warning (harmless info message)
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.WARNING)


def execute_chained_command(
    command: str,
    job_id: str = None,
    execution_id: str = None,
    status: str = None,
    pass_job_id: bool = False,
    pass_execution_id: bool = False,
) -> int:
    """
    Execute a chained command with optional parameter passing.

    Args:
        command: Command to execute
        job_id: Current job ID
        execution_id: Current execution ID
        status: Current job status (success/failure)
        pass_job_id: Whether to append --job-id to command
        pass_execution_id: Whether to append --execution-id to command

    Returns:
        Return code from command execution
    """
    if not command:
        return 0

    # Build full command with optional parameters
    full_command = command

    if pass_job_id and job_id:
        full_command += f" --parent-job-id {job_id}"

    if pass_execution_id and execution_id:
        full_command += f" --execution-id {execution_id}"

    if status:
        full_command += f" --parent-status {status}"

    logger.info(f"")
    logger.info(f"{'=' * 80}")
    logger.info(f"Executing chained command: {full_command}")
    logger.info(f"{'=' * 80}")
    logger.info(f"")

    try:
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=False,  # Let output stream to terminal
            text=True,
        )

        if result.returncode == 0:
            logger.info(f"Chained command completed successfully")
        else:
            logger.warning(
                f"Chained command failed with return code {result.returncode}"
            )

        return result.returncode

    except Exception as e:
        logger.error(f"Failed to execute chained command: {e}")
        return 1


def _etl_suites_config_path() -> Path:
    return Path(__file__).resolve().parent / "configs" / "etl_suites.yaml"


def load_etl_suites_yaml() -> dict:
    path = _etl_suites_config_path()
    if not path.is_file():
        raise FileNotFoundError(f"ETL suite config not found: {path}")
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("etl_suites.yaml must parse to a mapping at the top level")
    return data


def list_etl_suite_names() -> None:
    """Print suite names and descriptions from configs/etl_suites.yaml."""
    data = load_etl_suites_yaml()
    suites = data.get("suites") or {}
    if not suites:
        print("No suites defined in etl_suites.yaml")
        return
    print("Configured ETL suites (--etl-suite NAME):")
    for name in sorted(suites.keys()):
        entry = suites[name] or {}
        desc = (entry.get("description") or "").strip()
        if desc:
            print(f"  {name}: {desc}")
        else:
            print(f"  {name}")


def build_etl_suite_passthrough_args(args) -> List[str]:
    """Flags from the parent invocation to forward to each suite subprocess."""
    out: List[str] = []
    if getattr(args, "log_level", None) and args.log_level != "normal":
        out.extend(["--log-level", args.log_level])
    if getattr(args, "no_alerts", False) or getattr(args, "no_alert", False):
        out.append("--no-alerts")
    if getattr(args, "notify_emails", None):
        out.extend(["--notify", args.notify_emails])
    if getattr(args, "vars", None):
        for v in args.vars:
            out.extend(["--vars", v])
    if getattr(args, "skip_validation", False):
        out.append("--skip-validation")
    # Always forward the parent's resolved trigger so suite child steps inherit
    # it instead of re-detecting (a child subprocess has no TTY and would other-
    # wise flip a manually-run suite's steps to "scheduled").
    if getattr(args, "triggered_by", None):
        out.extend(["--triggered-by", args.triggered_by])
    if getattr(args, "dry_run", False):
        out.append("--dry-run")
    return out


def run_etl_suite(args) -> int:
    """
    Run a named multi-step ETL sequence (configs/etl_suites.yaml).
    Each step invokes this orchestrator again with --source ... and shared --env.
    """
    suite_name = args.etl_suite
    data = load_etl_suites_yaml()
    suites = data.get("suites") or {}
    if suite_name not in suites:
        known = ", ".join(sorted(suites.keys())) or "(none)"
        logger.error(f"Unknown ETL suite {suite_name!r}. Known suites: {known}")
        return 1

    suite = suites[suite_name] or {}
    raw_steps = suite.get("steps")
    if not raw_steps or not isinstance(raw_steps, list):
        logger.error(f"Suite {suite_name!r} has no valid 'steps' list")
        return 1

    orchestrate_py = Path(__file__).resolve()
    repo_root = orchestrate_py.parent
    passthrough = build_etl_suite_passthrough_args(args)
    base = [sys.executable, str(orchestrate_py), "--env", args.env]
    suite_lock_source = f"etl_suite:{suite_name}"
    suite_job_id = f"etl_suite_{suite_name}_{uuid.uuid4()}"
    suite_lock_acquired = False

    if not getattr(args, "dry_run", False) and not acquire_job_lock(
        suite_lock_source, suite_job_id
    ):
        logger.error(
            "Cannot acquire ETL suite lock for suite %r. Another suite run is already in progress.",
            suite_name,
        )
        logger.info(
            "To view active locks, run: python orchestrate.py --env %s --job-locks",
            args.env,
        )
        return 1
    suite_lock_acquired = not getattr(args, "dry_run", False)

    try:
        child_env = os.environ.copy()

        for i, step in enumerate(raw_steps, start=1):
            if not isinstance(step, dict):
                logger.error(
                    f"Suite {suite_name!r} step {i} must be a mapping with 'orchestrate_args'"
                )
                return 1
            orch_args = step.get("orchestrate_args")
            if not isinstance(orch_args, list) or not all(
                isinstance(x, str) for x in orch_args
            ):
                logger.error(
                    f"Suite {suite_name!r} step {i}: 'orchestrate_args' must be a list of strings"
                )
                return 1
            if "--source" not in orch_args:
                logger.error(
                    f"Suite {suite_name!r} step {i}: orchestrate_args must include --source"
                )
                return 1
            if "--env" in orch_args:
                logger.error(
                    f"Suite {suite_name!r} step {i}: do not pass --env in orchestrate_args (inherited from suite run)"
                )
                return 1

            cmd = base + passthrough + orch_args
            logger.info("")
            logger.info("=" * 80)
            logger.info(f"ETL suite {suite_name!r}: step {i}/{len(raw_steps)}")
            logger.info(f"Command: {' '.join(cmd)}")
            logger.info("=" * 80)

            if getattr(args, "dry_run", False):
                logger.info("Dry-run: skipping subprocess")
                continue

            result = subprocess.run(cmd, cwd=str(repo_root), env=child_env)
            if result.returncode != 0:
                logger.error(
                    f"Suite {suite_name!r} stopped: step {i} exited with {result.returncode}"
                )
                return result.returncode
    finally:
        if suite_lock_acquired:
            release_job_lock(suite_lock_source)

    logger.info(
        f"ETL suite {suite_name!r} completed successfully ({len(raw_steps)} step(s))"
    )
    return 0


def validate_facebook_entity_types(sites, tables):
    """Validate that Facebook entity types match table requirements

    Note: Accepts plain numeric IDs. The extractor will:
    - Add 'act_' prefix internally for ad account operations
    - Auto-discover pages when extracting page-related data
    """

    # Skip validation if using 'all' (discovery mode)
    if sites == ["all"] or sites == "all":
        return

    # Validate that all sites are numeric IDs (accept both formats for backwards compat)
    for site in sites:
        # Accept both formats: 123456 or act_123456
        if not (site.isdigit() or (site.startswith("act_") and site[4:].isdigit())):
            raise ValueError(
                f"Invalid Facebook ID format: {site}"
                f"\n  Use numeric ID: --sites 123456789"
                f"\n  The extractor handles both ad accounts and pages automatically"
            )

    # Log provided IDs
    logger.info(f"Facebook sites: {sites}")


def get_valid_groups(source: str) -> List[str]:
    """Get list of valid groups for a source from its YAML configuration"""
    config = load_config(source)

    groups = set()
    resources = config.get("resources", {})

    for table_name, table_config in resources.items():
        if isinstance(table_config, dict) and "group" in table_config:
            groups.add(table_config["group"])

    return sorted(list(groups))


def validate_groups(source: str, requested_groups: List[str]) -> None:
    """Validate that requested groups exist in the configuration"""
    valid_groups = get_valid_groups(source)

    if not valid_groups:
        raise ValueError(
            f"Source '{source}' does not support groups."
            f"\n  Use --tables instead to specify individual tables"
            f"\n  Run with --list-tables to see available tables"
        )

    invalid_groups = [g for g in requested_groups if g not in valid_groups]

    if invalid_groups:
        raise ValueError(
            f"Invalid group(s) for source '{source}': {invalid_groups}"
            f"\n  Valid groups are: {valid_groups}"
            f"\n  Run with --list-groups to see available groups"
        )

    logger.info(f"Validated groups: {requested_groups}")


def validate_tables(source: str, requested_tables: List[str]) -> None:
    """Validate that requested tables exist in the configuration"""
    available_tables = get_available_tables(source)

    invalid_tables = [t for t in requested_tables if t not in available_tables]

    if invalid_tables:
        error_msg = f"Invalid table(s) for source '{source}': {invalid_tables}\n"
        error_msg += f"  Valid tables are: {available_tables}\n"
        error_msg += f"  Run with --list-tables to see all available tables"
        raise ValueError(error_msg)

    logger.info(f"Validated tables: {requested_tables}")


def initialize_features(config: Dict[str, Any]) -> None:
    """
    Initialize monitoring and notifications
    """
    initialize_notifications(config)
    logger.info("Initialized features")


def get_available_tables(source: str) -> List[str]:
    """
    Get list of available tables for a source from its YAML configuration.
    This function filters out tables with active: false or active: child.
    """
    config = load_config(source)

    # Use source-specific extractor's filtering logic
    try:
        if source == "facebook":
            from plugins.facebook_extractor import get_available_tables as fb_get_tables

            return fb_get_tables(config)
        elif source == "wordpress":
            from plugins.wordpress_extractor import (
                get_available_tables as wp_get_tables,
            )

            return wp_get_tables(config)
        else:
            # Default path: use the shared active-filter helper. The generic
            # extractor's own get_available_tables does NOT filter active:false/
            # child (it returns all resource keys), which would extract inactive
            # tables on the default path for ~12 sources. The shared helper
            # excludes false/"false"/"child"/"no"/"n"/"0".
            from shared.extractor_utils import (
                get_available_tables as shared_get_tables,
            )

            return shared_get_tables(config)
    except ImportError:
        logger.warning(
            f"Could not import extractor for {source}, falling back to basic filtering"
        )

    # Fallback: Manual filtering if extractor import fails
    resources = config.get("resources", {})
    return [k for k, v in resources.items() if v.get("active") not in (False, "child")]


def get_available_sites(source: str) -> List[str]:
    """
    Get list of available sites for a source from its YAML configuration
    """
    config = load_config(source)

    # Try to get sites from configuration
    if "sites" in config.get("source", {}):
        return config["source"]["sites"]
    elif "accounts" in config:
        return config.get("accounts", {}).get("default", [])

    # Fallback: use generic extractor to discover sites
    try:
        from plugins.generic_extractor import get_available_sites as generic_get_sites

        return generic_get_sites(config)
    except ImportError:
        logger.warning(f"Could not import generic extractor for {source}")

    return config.get("sites", {}).get("default", [])


def get_dataset_name(config: Dict[str, Any]) -> str:
    """
    Get the dataset name from configuration
    """
    pipeline_config = config.get("pipeline", {})
    return pipeline_config.get(
        "staging_dataset", f"{config.get('source', {}).get('name', 'data')}_staging"
    )


def list_available_tables(source: str):
    """
    List available tables for a source
    """
    try:
        config = load_config(source)
    except FileNotFoundError:
        logger.error(f"Invalid source: {source}")
        logger.error(f"Available sources: facebook, wordpress, mailchimp")
        sys.exit(1)
    source_name = config.get("source", {}).get("name", source)

    # Get available tables (excludes child tables)
    tables = get_available_tables(source)

    # Get child tables separately
    all_resources = config.get("resources", {})
    child_tables = {
        k: v for k, v in all_resources.items() if v.get("active") == "child"
    }

    if tables or child_tables:
        print(f"\nAvailable {source_name.title()} Resources:")
        print("=" * (len(source_name) + 20) + "\n")

        # Show standard tables
        for table in sorted(tables):
            table_config = config.get("resources", {}).get(table, {})
            description = table_config.get("description", "No description available")
            print(f"  {table:<20} - {description}")

        # Show child tables in separate section
        if child_tables:
            print(f"\nChild Tables (extracted via parent):")
            for table, table_config in sorted(child_tables.items()):
                parent = table_config.get("parent_table", "unknown")
                reason = table_config.get("extraction_note", "No description")
                print(f"  {table:<20} - via {parent}")
                print(f"  {'':<20}   {reason[:60]}...")

        print(
            f"\nTotal: {len(tables)} independent resources, {len(child_tables)} child resources"
        )
    else:
        print(f"\nNo resources found for {source_name}")
        print("Check your configuration file for 'resources' section")


def list_available_sites(source: str):
    """
    List available sites for a source
    """
    sites = get_available_sites(source)

    source_name = source.capitalize()
    print(f"\nAvailable {source_name} Sites:")
    print("=" * (len(source_name) + 20) + "\n")

    for site in sites:
        print(f"- {site}")

    print()


def list_available_groups(source: str):
    """
    List available table groups for a source
    """
    try:
        config = load_config(source)
    except FileNotFoundError:
        logger.error(f"Invalid source: {source}")
        logger.error(f"Available sources: facebook, wordpress, mailchimp")
        sys.exit(1)

    source_name = config.get("source", {}).get("name", source)
    table_groups = config.get("groups", {})

    if not table_groups:
        print(f"\nNo table groups defined for {source_name}")
        return

    print(f"\nAvailable {source_name.title()} Table Groups:")
    print("=" * 60)
    print()

    for group_name, group_config in sorted(table_groups.items()):
        description = group_config.get("description", "No description")
        tables = group_config.get("tables", [])
        workers = group_config.get("parallel_workers", "default")
        pattern = group_config.get("recommended_pattern", "standard")

        print(f"Group: {group_name}")
        print(f"  Description: {description}")
        print(f"  Load Pattern: {pattern}")
        print(f"  Workers: {workers}")
        print(f"  Tables ({len(tables)}):")
        for table in tables:
            print(f"    - {table}")
        print()

    print(f"\nUsage:")
    print(f"  python orchestrate.py --source {source} --group <group_name> --env prod")
    print(
        f"  python orchestrate.py --source {source} --load-pattern multi --group <group_name> --env prod"
    )
    print()


def run_next_pipeline(
    next_command: str,
    success: bool,
    execution_id: str,
    pass_execution_id: bool,
    stats: Dict[str, Any],
    source: str = None,
    tables: list = None,
    sites: list = None,
    groups: list = None,
    env: str = None,
) -> None:
    # Execute next pipeline in sequence
    if not next_command:
        return

    # Build command with parameters
    if pass_execution_id and "--execution-id" not in next_command:
        next_command += f" --execution-id {execution_id}"

    if "--parent-status" not in next_command:
        status = "success" if success else "failure"
        next_command += f" --parent-status {status}"

    # Pass through environment (MANDATORY for all pipelines)
    if env and "--env" not in next_command and "--environment" not in next_command:
        next_command += f" --env {env}"

    # Pass through source and table specifications
    if source and "--source" not in next_command:
        next_command += f" --source {source}"

    if tables and "--table" not in next_command and "--tables" not in next_command:
        # Pass tables using --tables (preferred) or --table (alias)
        next_command += f" --tables {' '.join(tables)}"

    if groups and "--group" not in next_command and "--groups" not in next_command:
        # Pass groups using --groups (preferred) or --group (alias)
        next_command += f" --groups {' '.join(groups)}"

    # Only pass sites if the next command is not shared/transform.py (which doesn't accept sites)
    if (
        sites
        and "--site" not in next_command
        and "--sites" not in next_command
        and "shared/transform.py" not in next_command
    ):
        # Pass sites using --sites (preferred) or --site (alias)
        next_command += f" --sites {' '.join(sites)}"

    # Add stats as environment variables
    env = os.environ.copy()
    env["PARENT_ROWS"] = str(stats.get("total_rows", 0))
    env["PARENT_DURATION"] = str(stats.get("duration_seconds", 0))

    try:
        print(f"\nExecuting next pipeline: {next_command}")
        result = subprocess.run(next_command, shell=True, env=env)

        if result.returncode != 0:
            print(f"Next pipeline failed with code {result.returncode}")

    except Exception as e:
        print(f"Failed to execute next pipeline: {e}")


def normalize_schema_affix(prefix=None, suffix=None):
    # Normalize prefix and suffix to ensure proper underscore handling
    # Prefix: Add trailing underscore if not present
    # Suffix: Add leading underscore if not present
    normalized_prefix = None
    normalized_suffix = None

    if prefix:
        prefix = prefix.strip()
        if prefix and not prefix.endswith("_"):
            normalized_prefix = f"{prefix}_"
        else:
            normalized_prefix = prefix

    if suffix:
        suffix = suffix.strip()
        if suffix and not suffix.startswith("_"):
            normalized_suffix = f"_{suffix}"
        else:
            normalized_suffix = suffix

    return normalized_prefix, normalized_suffix


def apply_schema_affix(table_name, prefix=None, suffix=None):
    # Apply prefix and suffix to table name
    # Example: apply_schema_affix('campaigns', 'test', 'v2') -> 'test_campaigns_v2'
    result = table_name

    if prefix:
        result = f"{prefix}{result}"

    if suffix:
        result = f"{result}{suffix}"

    return result


def auto_convert_value(value_str):
    # Automatically convert string to appropriate Python type
    # Examples: 'true' → True, '123' → 123, '["a","b"]' → ['a', 'b']
    import json

    if not isinstance(value_str, str):
        return value_str

    # Boolean
    if value_str.lower() == "true":
        return True
    if value_str.lower() == "false":
        return False

    # None/null
    if value_str.lower() in ("none", "null"):
        return None

    # Try JSON (for lists, dicts)
    if value_str.startswith(("[", "{")):
        try:
            return json.loads(value_str)
        except json.JSONDecodeError:
            pass

    # Try integer
    try:
        return int(value_str)
    except ValueError:
        pass

    # Try float
    try:
        return float(value_str)
    except ValueError:
        pass

    # Default: string
    return value_str


def set_nested_value(dictionary, key_path, value):
    # Set value in nested dictionary using dot notation
    # Example: set_nested_value({}, 'optimization.workers', 10)
    #          → {'optimization': {'workers': 10}}
    keys = key_path.split(".")
    current = dictionary

    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        elif not isinstance(current[key], dict):
            # Overwrite non-dict with dict
            current[key] = {}
        current = current[key]

    current[keys[-1]] = value


def parse_vars(vars_list):
    # Parse --vars arguments into nested dictionary
    # Examples:
    #   ['max_rows=1000', 'debug=true']
    #   → {'max_rows': 1000, 'debug': True}
    #
    #   ['optimization.workers=10', 'pipeline.batch_size=500']
    #   → {'optimization': {'workers': 10}, 'pipeline': {'batch_size': 500}}
    if not vars_list:
        return {}

    overrides = {}

    for var in vars_list:
        if "=" not in var:
            logger.warning(f"Invalid --vars format (missing '='): {var}")
            continue

        key, value = var.split("=", 1)
        key = key.strip()
        value = value.strip()

        # Auto-detect and convert value type
        value = auto_convert_value(value)

        # Handle nested keys (dot notation)
        if "." in key:
            set_nested_value(overrides, key, value)
        else:
            overrides[key] = value

    return overrides


def deep_merge(base, overrides):
    # Deep merge overrides into base config
    # Overrides take precedence
    import copy

    result = copy.deepcopy(base)

    for key, value in overrides.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value

    return result


def get_extractor_function(source: str):
    # Get the appropriate extractor function for the source
    # Try to import source-specific extractor
    try:
        module_name = f"plugins.{source}_extractor"
        module = importlib.import_module(module_name)
        return getattr(module, "run_pipeline")
    except (ImportError, AttributeError):
        logger.warning(f"Could not import {source}_extractor, falling back to generic")
        return generic_run_pipeline


def _filter_post_process_steps(config, pp_steps):
    """Return a copy of config whose pipeline.post_process keeps only the steps
    matching --pp-step (by exact config entry or by filename). Order is the
    original config order. Returns (filtered_config, kept, requested_not_found).
    """
    pipeline_cfg = config.get("pipeline", {})
    configured = pipeline_cfg.get("post_process", [])
    if isinstance(configured, dict):
        configured = [configured]

    def entry_paths(entry):
        # A step entry is a str (path or command) or a dict with sql_file/
        # script_path/command. Yield the candidate path string(s) to match on.
        if isinstance(entry, str):
            return [entry]
        return [
            p
            for p in (
                entry.get("sql_file"),
                entry.get("script_path"),
                entry.get("command"),
            )
            if p
        ]

    wanted = list(pp_steps)
    matched_wanted = set()
    kept = []
    for entry in configured:
        paths = entry_paths(entry)
        hit = None
        for w in wanted:
            for p in paths:
                if p == w or os.path.basename(p) == os.path.basename(w):
                    hit = w
                    break
            if hit:
                break
        if hit:
            kept.append(entry)
            matched_wanted.add(hit)

    not_found = [w for w in wanted if w not in matched_wanted]

    filtered = dict(config)
    filtered_pipeline = dict(pipeline_cfg)
    filtered_pipeline["post_process"] = kept
    filtered["pipeline"] = filtered_pipeline
    return filtered, kept, not_found


def run_post_process_only(config, args):
    """Run ONLY the pipeline post_process steps and exit (no extract/merge).

    Mirrors the post-processing block in main(): same run_post_processing and
    evaluate_post_process_outcome, same WARNING/alert-on-failure semantics. With
    --dry-run, lists the resolved steps (and SQL vs Python) without executing.
    Returns nothing; raises SystemExit(1) on post-process failure so callers /
    cron see a nonzero exit.
    """
    from shared.post_processor import (
        run_post_processing,
        evaluate_post_process_outcome,
    )

    source = args.source
    pp_steps = getattr(args, "pp_step", None)

    if pp_steps:
        config, kept, not_found = _filter_post_process_steps(config, pp_steps)
        if not_found:
            logger.error(
                "--pp-step: no post_process step matches: %s", ", ".join(not_found)
            )
            sys.exit(1)
        if not kept:
            logger.error("--pp-step matched no configured post_process steps.")
            sys.exit(1)

    configured = config.get("pipeline", {}).get("post_process", [])
    if isinstance(configured, dict):
        configured = [configured]

    logger.info("=" * 80)
    logger.info("POST-PROCESS-ONLY for source '%s'", source)
    logger.info("=" * 80)
    if not configured:
        logger.info(
            "No post_process steps configured for '%s' -- nothing to do.", source
        )
        return
    logger.info("Resolved %d post_process step(s) (config order):", len(configured))
    for i, entry in enumerate(configured, 1):
        if isinstance(entry, str):
            path = entry
        else:
            path = (
                entry.get("sql_file")
                or entry.get("script_path")
                or entry.get("command")
                or str(entry)
            )
        kind = "python" if (".py" in str(path)) else "sql"
        logger.info("  %d. [%s] %s", i, kind, path)

    if args.dry_run:
        logger.info("")
        logger.info("--dry-run: not executing. Re-run without --dry-run to apply.")
        return

    bq_client = get_bigquery_client()
    post_results = run_post_processing(
        bq_client=bq_client,
        config=config,
        source=source,
        execution_id="post-process-only",
        level="pipeline",
    )

    outcome = evaluate_post_process_outcome(post_results)
    if outcome["failed"]:
        logger.warning(outcome["summary"])
        if not (getattr(args, "no_alert", False) or getattr(args, "no_alerts", False)):
            # Reuse the standard failure alert so recovery failures still page.
            try:
                send_job_failure_alert(
                    "post-process-only", source, outcome["summary"], config
                )
            except Exception as alert_err:
                logger.warning("Could not send failure alert: %s", alert_err)
        sys.exit(1)

    succeeded = post_results.get("success", 0)
    total = post_results.get("total", 0)
    logger.info(
        "[OK] Post-process-only complete: %d/%d step(s) succeeded.", succeeded, total
    )


def _detect_trigger_source() -> str:
    """Best-effort guess of how this run was triggered, for the default of
    --triggered-by.

    Automated schedulers (cron, systemd timers) run with no controlling
    terminal, whereas an interactive shell run has a TTY. systemd additionally
    exports INVOCATION_ID / JOURNAL_STREAM. We use those signals so scheduled
    runs are labeled correctly without every crontab/systemd line having to pass
    an explicit flag. An explicit --triggered-by always overrides this default.
    """
    # systemd unit -> definitely scheduled.
    if os.environ.get("INVOCATION_ID") or os.environ.get("JOURNAL_STREAM"):
        return "scheduled"
    # An interactive shell has a terminal attached to stdout; cron/systemd/CI
    # redirect it to a log file (no TTY). stdout is the reliable signal here --
    # schedulers always redirect it, whereas stdin state is inconsistent across
    # platforms. No/closed TTY -> scheduled.
    try:
        return "manual" if sys.stdout.isatty() else "scheduled"
    except (ValueError, AttributeError):
        # Detached/closed stream behaves like a scheduler.
        return "scheduled"


def main():
    """
    Main function
    """
    # Parse arguments
    parser = argparse.ArgumentParser(description="Universal Data Orchestrator")
    parser.add_argument(
        "--source",
        required=False,
        default=None,
        help="Source type (facebook, wordpress, mailchimp, etc.). Omit when using --etl-suite.",
    )

    # Environment selection (required for runs; optional only with --list-etl-suites)
    parser.add_argument(
        "--env",
        "--environment",
        dest="env",
        choices=["prod", "dev", "test"],
        required=False,
        default=None,
        help="Target environment: prod (bi-data-391216), dev (bi-dev-391216), test (uses dev project). Required unless using --list-etl-suites.",
    )

    parser.add_argument(
        "--etl-suite",
        metavar="NAME",
        default=None,
        help="Run a named multi-step ETL from configs/etl_suites.yaml (requires --env). Do not pass --source.",
    )
    parser.add_argument(
        "--list-etl-suites",
        action="store_true",
        help="Print suite names from configs/etl_suites.yaml and exit (no --env or --source required).",
    )

    # Table selection
    parser.add_argument("--tables", nargs="+", help="Tables/resources to extract")
    parser.add_argument(
        "--table", nargs="+", help="Single table/resource (alias for --tables)"
    )
    parser.add_argument("--groups", nargs="+", help="Process all tables in groups")
    parser.add_argument("--group", nargs="+", help="Single group (alias for --groups)")

    # Site/Account selection
    parser.add_argument(
        "--sites",
        nargs="+",
        help="""Sites/accounts to extract from (source-specific):
  Facebook ad accounts: act_123456789
  Facebook pages: 123456789 (numeric page ID only)
  WordPress sites: sitename-prd
  Use "all" for automatic discovery (Facebook: finds ad accounts only)""",
    )
    parser.add_argument(
        "--site", nargs="+", help="Single site/account (alias for --sites)"
    )
    parser.add_argument(
        "--all-sites", action="store_true", help="Use all configured sites"
    )

    # Extraction mode
    parser.add_argument(
        "--refresh",
        choices=["full", "incremental"],
        default="incremental",
        help="Refresh mode",
    )
    parser.add_argument(
        "--load-pattern",
        choices=["standard", "multi", "incremental"],
        help="Load pattern: standard (one table at a time), multi (multiple tables in one pass), incremental (smart lookback). Default: from config or standard",
    )
    parser.add_argument("--lookback", type=int, help="Days to look back")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")

    # Processing options
    parser.add_argument(
        "--test-mode", action="store_true", help="Test with limited data"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done"
    )
    parser.add_argument("--batch-size", type=int, help="Batch size for processing")
    parser.add_argument(
        "--parallel",
        type=int,
        default=None,
        help="Number of parallel workers (default: from YAML config)",
    )
    parser.add_argument(
        "--survey-limit",
        type=int,
        default=None,
        help="[LimeSurvey] Limit number of surveys to process (for testing)",
    )
    parser.add_argument(
        "--survey-created-days",
        type=int,
        default=None,
        help="[LimeSurvey] Surface surveys where lime_surveys.datecreated >= today - N days "
        "(default: discovery.survey_created_lookback_days in configs/limesurvey.yaml)",
    )
    parser.add_argument(
        "--no-reconcile",
        action="store_true",
        help="[LimeSurvey] Disable the BQ reconciliation pass that surfaces surveys "
        "present in lime_surveys but missing from lime_surveys_columnar_completed",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of records/items to process (for testing)",
    )
    parser.add_argument(
        "--test-emails",
        nargs="+",
        help="[Testing] Specific email addresses to check (bypasses normal discovery)",
    )
    parser.add_argument(
        "--test-list-ids",
        nargs="+",
        help="[Testing] Filter results to specific list IDs",
    )
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum retry")

    # Data management options
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="DANGEROUS: Delete and rebuild production tables (for schema changes/full rebuilds)",
    )
    parser.add_argument(
        "--truncate-staging",
        action="store_true",
        help="Truncate staging tables before load (default: True for full refresh)",
    )
    parser.add_argument(
        "--no-truncate-staging",
        action="store_true",
        help="Do not truncate staging tables (keep existing staging data)",
    )
    parser.add_argument(
        "--staging-wipe-scope",
        choices=["run", "dataset"],
        default=None,
        help='Pre-run staging cleanup scope: "run" drops only the staging tables for the '
        "tables/groups in this run (default; other sub-sources' staging survives); "
        '"dataset" drops every table in the staging dataset (legacy behavior). '
        "Default comes from pipeline.staging.scope in config (run).",
    )

    # Pipeline flow control
    parser.add_argument(
        "--extract-only", action="store_true", help="Only extract data to local files"
    )
    parser.add_argument("--skip-gcs", action="store_true", help="Skip exporting to GCS")
    parser.add_argument(
        "--skip-external-tables",
        action="store_true",
        help="Skip creating external tables",
    )
    parser.add_argument(
        "--skip-transform",
        action="store_true",
        help="Skip transformation to final tables",
    )
    parser.add_argument(
        "--skip-post-process",
        action="store_true",
        help="Skip post-processing SQL execution",
    )
    parser.add_argument(
        "--post-process-only",
        action="store_true",
        help=(
            "Run ONLY the pipeline post_process steps (no discovery, extraction, "
            "GCS, staging, or merge) and exit. Use for recovery when a load "
            "already succeeded but post-processing failed. Writes to production "
            "(UPDATE/MERGE/DELETE); pair with --dry-run to just list the resolved "
            "steps. Restrict to specific steps with --pp-step."
        ),
    )
    parser.add_argument(
        "--pp-step",
        action="append",
        metavar="PATH",
        help=(
            "With --post-process-only, run only the post_process step(s) whose "
            "configured path matches PATH (repeatable). PATH may be the exact "
            "config entry or just the filename (e.g. sync_standardized_lookups.sql). "
            "Steps run in config order regardless of the order given here."
        ),
    )

    # SQL-only pipeline options (e.g. profile_database)
    parser.add_argument(
        "--build-mode",
        help=(
            "Build mode for SQL-only pipelines (e.g. profile_database: rebuild, refresh, "
            "reenrich, views, resume_rebuild, resume_publish; legacy: incremental, enrich)"
        ),
    )

    # List/recovery commands
    parser.add_argument(
        "--list-sites", action="store_true", help="List available sites"
    )
    parser.add_argument(
        "--list-tables", action="store_true", help="List available tables"
    )
    parser.add_argument(
        "--list-groups", action="store_true", help="List available table groups"
    )
    parser.add_argument(
        "--list-archives",
        action="store_true",
        help="List archived staging tables from orchestrator_monitoring.staging_archive_manifest "
        "(filter with --source/--tables, window via --archive-days). Shows the BQ archive "
        "table (queryable until expiry) and the raw GCS parquet URIs for reload.",
    )
    parser.add_argument(
        "--archive-days",
        type=int,
        default=30,
        help="Lookback window in days for --list-archives (default: 30)",
    )
    parser.add_argument("--list", action="store_true", help="List all available")
    # NOTE: --resume / --recover / --reset-state were removed -- the checkpoint
    # system they relied on never wrote or consumed checkpoints (it was a non-
    # functional facade). Re-running a partially-failed job is already SAFE because
    # the per-table hash-merge is idempotent; it simply re-extracts the completed
    # tables. --clear-state remains (it clears incremental extraction state).
    parser.add_argument(
        "--clear-state", action="store_true", help="Clear incremental extraction state"
    )
    parser.add_argument(
        "--retry-dlq",
        action="store_true",
        help="Re-run only this source's unresolved dead-letter-queue tables "
        "(failed in a prior run, under max_retries). Resolves them on success.",
    )

    # Job lock management
    parser.add_argument(
        "--job-locks",
        action="store_true",
        help="View active job locks across all sources",
    )
    parser.add_argument(
        "--scorecard",
        action="store_true",
        help="Print the source reliability scorecard (read-only) and exit. "
        "Use --scorecard-window N for the rolling window (default 14 days).",
    )
    parser.add_argument(
        "--scorecard-window",
        type=int,
        default=14,
        help="Rolling window in days for --scorecard (default 14)",
    )
    parser.add_argument(
        "--scorecard-alert",
        action="store_true",
        help="With --scorecard: check for source-reliability degradation (below "
        "floor, or sharp drop vs a 30-day baseline) and email a WARNING if any. "
        "For the nightly run.",
    )
    parser.add_argument(
        "--explain-metric",
        metavar="METRIC",
        help="Read-only: explain what a metric meant on a date (requires --source). "
        "What it currently means + when its definition last changed (from the "
        "source's metric_definitions). Use --as-of YYYY-MM-DD for a past date.",
    )
    parser.add_argument(
        "--as-of",
        metavar="YYYY-MM-DD",
        help="Date for --explain-metric (default today).",
    )
    parser.add_argument(
        "--cleanup-locks", action="store_true", help="Clean up expired job locks"
    )
    parser.add_argument(
        "--force-release-lock",
        metavar="LOCK_ID",
        help="Force release a specific job lock by lock_id (admin only)",
    )
    parser.add_argument(
        "--force-release-lock-source",
        help="Source/scope for --force-release-lock. Defaults to --source.",
    )

    # Validation options
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip configuration validation (not recommended)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration, do not run pipeline",
    )

    # Job metadata options
    parser.add_argument(
        "--triggered-by",
        default=_detect_trigger_source(),
        choices=["manual", "scheduled", "api", "chained"],
        help=(
            "How the job was triggered. Defaults to auto-detected "
            "(scheduled under cron/systemd, manual in an interactive shell); "
            "pass explicitly to override."
        ),
    )

    # Alerting options
    parser.add_argument(
        "--no-alerts",
        action="store_true",
        help="Suppress email alerts (useful for testing)",
    )
    parser.add_argument("--no-alert", action="store_true", help="Alias for --no-alerts")

    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["minimal", "normal", "verbose"],
        default="normal",
        help="Log verbosity: minimal (errors/warnings only), normal (default), verbose (full detail)",
    )

    # Notification options
    parser.add_argument(
        "--notify",
        dest="notify_emails",
        help="Additional email addresses for alerts (comma-separated). Example: --notify user1@example.com,user2@example.com",
    )

    # Table status override
    parser.add_argument(
        "--force-inactive",
        action="store_true",
        help="Allow running tables marked as inactive",
    )

    # Profile_database / identity_hub: bypass preflight gates (single_generation, xref scale, etc.)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass preflight gates for SQL-only pipelines (profile_database). "
        "Allows proceeding when soft heuristic checks like single_generation fail.",
    )

    # Schema naming options
    parser.add_argument(
        "--schema-prefix",
        type=str,
        help="Prefix for table names (e.g., test creates test_tablename)",
    )
    parser.add_argument(
        "--schema-suffix",
        type=str,
        help="Suffix for table names (e.g., _v2 creates tablename_v2)",
    )

    # Runtime configuration overrides
    parser.add_argument(
        "--vars",
        action="append",
        help="Override config values (key=value or nested.key=value). Can be specified multiple times. Example: --vars max_rows=1000 --vars optimization.workers=10",
    )

    # Pipeline sequencing
    parser.add_argument("--on-success", help="Command to run on success")
    parser.add_argument("--on-failure", help="Command to run on failure")
    parser.add_argument(
        "--on-finish",
        help="Command to run on completion (regardless of success/failure)",
    )
    parser.add_argument(
        "--pass-execution-id",
        action="store_true",
        help="Pass execution ID to next pipeline",
    )
    parser.add_argument(
        "--pass-job-id", action="store_true", help="Pass job ID to next pipeline"
    )

    # Plugin-specific
    parser.add_argument("--ssh-key", help="SSH key path")

    # Mailchimp-specific options
    # Batch API default lives in configs/mailchimp.yaml (extractor.use_batch_api).
    # These CLI flags are per-run overrides; omit them to use the YAML default.
    batch_api_group = parser.add_mutually_exclusive_group()
    batch_api_group.add_argument(
        "--use-batch-api",
        dest="use_batch_api",
        action="store_true",
        default=None,
        help="Override: force Mailchimp Batch API for multi-table extraction",
    )
    batch_api_group.add_argument(
        "--no-batch-api",
        dest="use_batch_api",
        action="store_false",
        help="Override: force REST extraction (slower, legacy escape hatch)",
    )
    # Force the namespace default to None so the YAML default wins when neither
    # flag is given. argparse's per-action default handling is unreliable inside
    # mutually-exclusive groups when one action is store_true and the other is
    # store_false on the same dest — observed on prod producing False instead
    # of None even though --use-batch-api declared default=None.
    parser.set_defaults(use_batch_api=None)

    # Passed from previous pipeline
    parser.add_argument("--execution-id", help="Execution ID from previous pipeline")
    parser.add_argument(
        "--parent-job-id", help="Parent job ID (for job chaining/lineage)"
    )
    parser.add_argument("--parent-status", help="Status from previous pipeline")

    args = parser.parse_args()

    # --rebuild implies --refresh full: a rebuild fetches maximum data from the
    # source AND enables DELETE-not-matched in the merge. Without this, --rebuild
    # alone would use an incremental fetch window, which together with the merge's
    # DELETE clause would wipe out historical rows the partial fetch didn't cover.
    # argparse defaults --refresh to 'incremental', so we detect explicit user
    # intent by scanning sys.argv. Match both '--refresh X' and '--refresh=X' forms.
    if args.rebuild:
        refresh_explicit = any(
            tok == "--refresh" or tok.startswith("--refresh=") for tok in sys.argv
        )
        if refresh_explicit and args.refresh == "incremental":
            print(
                "ERROR: --rebuild requires --refresh full (or omit --refresh). "
                "--rebuild + --refresh incremental would delete historical rows.",
                file=sys.stderr,
            )
            sys.exit(2)
        # Default-or-full case: force full so plugins fetch maximum data.
        args.refresh = "full"

    # --build-mode validation (profile_database-specific flag).
    # Reject contradictions between --rebuild and an explicit --build-mode that
    # asks for non-rebuild work; reject --build-mode used outside profile_database.
    if getattr(args, "build_mode", None):
        if args.source and args.source != "profile_database":
            print(
                f"ERROR: --build-mode is only valid with --source profile_database "
                f"(got --source {args.source}).",
                file=sys.stderr,
            )
            sys.exit(2)
        if args.rebuild and args.build_mode not in ("rebuild", "resume_rebuild"):
            print(
                f"ERROR: --rebuild conflicts with --build-mode {args.build_mode}. "
                f"--rebuild asserts a destructive blue/green rebuild; "
                f"--build-mode {args.build_mode} runs a non-rebuild step. "
                f"Drop one of the flags.",
                file=sys.stderr,
            )
            sys.exit(2)

    # Configure logging based on --log-level argument (do this first)
    configure_logging(level=args.log_level)
    logger.info(
        f"Log level: {args.log_level} ({get_log_level_description(args.log_level)})"
    )

    if getattr(args, "list_etl_suites", False):
        list_etl_suite_names()
        return

    if not args.env:
        logger.error(
            "--env is required (use --list-etl-suites to print suite names without --env)"
        )
        sys.exit(1)

    # Map environment to GCP project ID and set environment variable (MANDATORY)
    # test uses dev project until bi-test-391216 exists
    ENV_TO_PROJECT = {
        "prod": "bi-data-391216",
        "dev": "bi-dev-391216",
        "test": "bi-dev-391216",
    }

    project_id = ENV_TO_PROJECT[args.env]
    os.environ["GCP_PROJECT_ID"] = project_id

    logger.info(f"Environment: {args.env} -> GCP Project: {project_id}")

    # Parse and validate additional notification emails
    additional_emails = []
    if args.notify_emails:
        import re

        email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

        # Split by comma and strip whitespace
        raw_emails = [e.strip() for e in args.notify_emails.split(",")]

        for email in raw_emails:
            if email and email_pattern.match(email):
                additional_emails.append(email)
                logger.info(f"Added notification recipient: {email}")
            elif email:
                logger.warning(f"Invalid email address ignored: {email}")

        if not additional_emails:
            logger.warning("--notify specified but no valid email addresses provided")

    # Validate argument combinations
    def validate_arguments(args):
        # Validate argument combinations and catch conflicts
        errors = []

        etl_suite = getattr(args, "etl_suite", None)
        list_commands = [args.list_sites, args.list_tables, args.list_groups, args.list]
        lock_commands = [
            args.job_locks,
            args.cleanup_locks,
            bool(args.force_release_lock),
        ]

        if sum(1 for command in lock_commands if command) > 1:
            errors.append("Use only one job lock management command at a time")
        if args.force_release_lock_source and not args.force_release_lock:
            errors.append("--force-release-lock-source requires --force-release-lock")

        if etl_suite:
            if not args.env:
                errors.append("--etl-suite requires --env")
            if args.source:
                errors.append(
                    "Do not pass --source with --etl-suite (each step sets its own --source)"
                )
            if any(lock_commands):
                errors.append(
                    "--etl-suite cannot be combined with job lock management commands"
                )
            if any(list_commands):
                errors.append(
                    "--etl-suite cannot be combined with --list / --list-sites / --list-tables / --list-groups"
                )
            if args.validate_only:
                errors.append("--validate-only is not supported with --etl-suite")
            if args.build_mode:
                errors.append(
                    "Do not pass --build-mode with --etl-suite; set flags per step in configs/etl_suites.yaml"
                )
            if args.rebuild:
                errors.append(
                    "Do not pass --rebuild with --etl-suite; use suite 'full_rebuild' or add --rebuild to step orchestrate_args"
                )
            if args.clear_state:
                errors.append("--clear-state is not supported with --etl-suite")
            if (
                args.extract_only
                or args.skip_gcs
                or args.skip_external_tables
                or args.skip_transform
                or args.skip_post_process
            ):
                errors.append(
                    "--extract-only and --skip-* pipeline flags are not supported with --etl-suite"
                )
            if (
                getattr(args, "on_success", None)
                or getattr(args, "on_failure", None)
                or getattr(args, "on_finish", None)
            ):
                errors.append(
                    "--on-success / --on-failure / --on-finish are not supported with --etl-suite"
                )
            if args.execution_id or args.parent_job_id or args.parent_status:
                errors.append(
                    "--execution-id / --parent-job-id / --parent-status are not supported with --etl-suite"
                )
            if args.pass_execution_id or args.pass_job_id:
                errors.append(
                    "--pass-execution-id / --pass-job-id are not supported with --etl-suite"
                )
            if args.table or args.tables or args.group or args.groups:
                errors.append("Table/group selection is not supported with --etl-suite")
            if args.site or args.sites or args.all_sites:
                errors.append("Site selection is not supported with --etl-suite")
        else:
            if any(lock_commands) or getattr(args, "scorecard", False):
                # --scorecard is a standalone read-only report; no --source needed.
                pass
            elif any(list_commands) and not args.source:
                errors.append("List commands require --source")
            elif not any(list_commands) and not args.source:
                errors.append(
                    "Must pass --source (single pipeline) or --etl-suite NAME for multi-pipeline runs"
                )

        if any(lock_commands) and (
            any(list_commands)
            or args.table
            or args.tables
            or args.group
            or args.groups
            or args.site
            or args.sites
            or args.all_sites
            or args.clear_state
        ):
            errors.append(
                "Job lock management commands cannot be combined with list, pipeline selection, or recovery flags"
            )

        # Check for conflicting table selection (only --table/--tables conflict with each other)
        table_args = [args.table, args.tables]
        non_none_table_args = [arg for arg in table_args if arg is not None]
        if len(non_none_table_args) > 1:
            errors.append(
                "Cannot specify both --table and --tables (use --tables for multiple tables)"
            )

        # Check for conflicting group selection (only --group/--groups conflict with each other)
        group_args = [args.group, args.groups]
        non_none_group_args = [arg for arg in group_args if arg is not None]
        if len(non_none_group_args) > 1:
            errors.append(
                "Cannot specify both --group and --groups (use --groups for multiple groups)"
            )

        # Check for conflicting site selection
        site_args = [args.site, args.sites, args.all_sites]
        non_none_site_args = [
            arg
            for arg in site_args
            if arg is not None and arg != [] and arg is not False
        ]
        if len(non_none_site_args) > 1:
            errors.append(
                "Cannot specify multiple site selection methods (--site, --sites, --all-sites)"
            )

        # Check for conflicting truncation options
        if args.truncate_staging and args.no_truncate_staging:
            errors.append(
                "Cannot specify both --truncate-staging and --no-truncate-staging"
            )

        # Check for conflicting date range options
        if args.lookback and (args.start_date or args.end_date):
            errors.append(
                "Cannot specify both --lookback and explicit date range (--start-date/--end-date)"
            )

        # Check for conflicting refresh modes
        if args.refresh == "full" and args.lookback:
            errors.append(
                "Cannot specify both --refresh full and --lookback (use --refresh incremental with --lookback)"
            )

        # Check for refresh full with explicit date range
        if args.refresh == "full" and (args.start_date or args.end_date):
            errors.append(
                "Cannot specify --refresh full with explicit date range (--start-date/--end-date). Use --refresh incremental or remove --refresh full"
            )

        # Check for load-pattern multi requirements
        if args.load_pattern == "multi":
            if not (args.group or args.groups or args.table or args.tables):
                errors.append(
                    "--load-pattern multi requires --group or --tables to be specified"
                )
            if (args.table or args.tables) and (args.group or args.groups):
                errors.append(
                    "--load-pattern multi: Cannot specify both --group and --tables (use one or the other)"
                )

        # Check for incomplete date range
        if args.start_date and not args.end_date:
            errors.append("--start-date requires --end-date to be specified")

        # Check for list commands vs execution
        list_commands = [args.list_sites, args.list_tables, args.list_groups, args.list]
        if any(list_commands) and (
            args.table or args.tables or args.group or args.groups
        ):
            errors.append(
                "List commands (--list-sites, --list-tables, --list-groups, --list) cannot be used with table selection"
            )

        # Check for validate-only vs execution
        if args.validate_only and (
            args.table or args.tables or args.group or args.groups
        ):
            errors.append("--validate-only cannot be used with table selection")

        # --retry-dlq computes its own table set from the dead-letter queue, so it
        # cannot be combined with explicit table/group selection.
        if getattr(args, "retry_dlq", False) and (
            args.table or args.tables or args.group or args.groups
        ):
            errors.append(
                "--retry-dlq cannot be combined with --table/--tables/--group/--groups "
                "(it re-runs the source's failed DLQ tables automatically)"
            )

        # Check for validation conflicts
        if args.skip_validation and args.validate_only:
            errors.append("Cannot specify both --skip-validation and --validate-only")

        # Check for dry-run conflicts
        if args.dry_run and args.rebuild:
            errors.append(
                "Cannot use --rebuild with --dry-run (dry-run prevents data modification)"
            )

        if args.dry_run and args.truncate_staging:
            errors.append(
                "Cannot use --truncate-staging with --dry-run (dry-run prevents data modification)"
            )

        # Check for validate-only conflicts
        if args.validate_only and args.dry_run:
            errors.append(
                "Cannot specify both --validate-only and --dry-run (redundant - both skip execution)"
            )

        if args.validate_only and args.extract_only:
            errors.append(
                "Cannot specify both --validate-only and --extract-only (contradictory intentions)"
            )

        if args.validate_only and (
            args.skip_gcs or args.skip_external_tables or args.skip_transform
        ):
            errors.append(
                "Cannot use --skip-* flags with --validate-only (validate-only doesn't execute pipeline)"
            )

        # Check for list command conflicts
        if any([args.list_sites, args.list_tables, args.list]) and (
            args.dry_run or args.test_mode
        ):
            errors.append(
                "List commands don't execute pipelines - --dry-run and --test-mode are redundant"
            )

        # Check for extract-only redundancies
        if args.extract_only and args.skip_transform:
            errors.append(
                "--skip-transform is redundant with --extract-only (extract-only already skips transform)"
            )

        if args.extract_only and args.skip_gcs:
            errors.append(
                "--skip-gcs is redundant with --extract-only (extract-only already skips GCS)"
            )

        if args.extract_only and args.skip_external_tables:
            errors.append(
                "--skip-external-tables is redundant with --extract-only (extract-only already skips external tables)"
            )

        # Check for rebuild conflicts
        if args.rebuild and args.skip_transform:
            errors.append(
                "Cannot use --rebuild with --skip-transform (rebuild requires transform phase to drop/recreate tables). Use --skip-post-process instead to skip only SQL transformations."
            )

        # Check for incomplete date range (reverse direction)
        if args.end_date and not args.start_date:
            errors.append("--end-date requires --start-date to be specified")

        # Validate date format (YYYY-MM-DD)
        def validate_date_format(date_str, arg_name):
            """Validate date is in YYYY-MM-DD format."""
            if not date_str:
                return None
            try:
                # Try parsing as YYYY-MM-DD
                datetime.strptime(date_str, "%Y-%m-%d")
                return None
            except ValueError:
                # Check if it looks like MM-DD-YYYY format (common mistake)
                try:
                    datetime.strptime(date_str, "%m-%d-%Y")
                    return f"{arg_name} must be in YYYY-MM-DD format (you provided MM-DD-YYYY format: {date_str})"
                except ValueError:
                    pass
                # Check if it looks like DD-MM-YYYY format
                try:
                    datetime.strptime(date_str, "%d-%m-%Y")
                    return f"{arg_name} must be in YYYY-MM-DD format (you provided DD-MM-YYYY format: {date_str})"
                except ValueError:
                    pass
                return f"{arg_name} must be in YYYY-MM-DD format (you provided: {date_str})"

        if args.start_date:
            error = validate_date_format(args.start_date, "--start-date")
            if error:
                errors.append(error)

        if args.end_date:
            error = validate_date_format(args.end_date, "--end-date")
            if error:
                errors.append(error)

        return errors

    # Run validation
    validation_errors = validate_arguments(args)
    if validation_errors:
        logger.error("Argument validation failed:")
        for error in validation_errors:
            logger.error(f"- {error}")
        sys.exit(1)

    if getattr(args, "etl_suite", None):
        sys.exit(run_etl_suite(args))

    # Handle listing options
    if args.list_tables:
        list_available_tables(args.source)
        return

    if args.list_sites:
        list_available_sites(args.source)
        return

    if args.list_groups:
        list_available_groups(args.source)
        return

    if args.list_archives:
        from shared.staging_lifecycle import list_staging_archives

        archives = list_staging_archives(
            get_bigquery_client(),
            source=args.source,
            tables=args.table or args.tables,
            days=args.archive_days,
        )
        if not archives:
            logger.info(
                f"No staging archives found in the last {args.archive_days} day(s). "
                f"Archives are written before each scoped staging wipe."
            )
        else:
            logger.info(
                f"Staging archives (last {args.archive_days} day(s), newest first):"
            )
            for row in archives:
                expires = row.get("expires_at")
                expires_str = expires.strftime("%Y-%m-%d") if expires else "never"
                logger.info(
                    f"  {row['archived_at']:%Y-%m-%d %H:%M} {row['source']}.{row['table_name']} "
                    f"rows={row.get('row_count') or 0:,} method={row.get('archive_method')} "
                    f"expires={expires_str}"
                )
                logger.info(f"    archive: {row['archive_table']}")
                for uri in row.get("gcs_parquet_uris") or []:
                    logger.info(f"    parquet: {uri}")
            logger.info("")
            logger.info(
                "Reload expired archives from parquet: CREATE EXTERNAL TABLE ... "
                "OPTIONS(format='PARQUET', uris=[<parquet URIs above>])"
            )
        return

    # Handle job lock commands
    if getattr(args, "scorecard", False):
        # Read-only: score every source from data already in the monitoring tables.
        from shared.scorecard import (
            query_scorecard,
            format_scorecard,
            check_scorecard_degradation,
            load_notification_config,
        )

        client = get_bigquery_client()
        rows = query_scorecard(client, window_days=args.scorecard_window)
        print(format_scorecard(rows))

        if getattr(args, "scorecard_alert", False):
            # Nightly degradation check: alert when a source falls below the floor
            # or drops sharply vs its 30-day baseline. Reuses the notification path.
            degraded = check_scorecard_degradation(client)
            if degraded:
                summary = "; ".join(
                    f"{d['source']} ({d['score']:.0f}: {d['flag_reason']})"
                    for d in degraded
                )
                msg = f"Source reliability degradation -- {summary}"
                logger.warning(msg)
                if not (
                    getattr(args, "no_alert", False)
                    or getattr(args, "no_alerts", False)
                ):
                    try:
                        # Import ALIASED. A bare `from ... import send_notification`
                        # here makes Python treat the name as local for the WHOLE of
                        # main(), shadowing the module-level import at the top of this
                        # file -- so the failure-notification call sites below raised
                        # UnboundLocalError instead of sending mail, silently
                        # swallowing build-failure alerts (observed 2026-07-30 when
                        # profile_database failed its acceptance gate).
                        from shared.monitoring import (
                            send_notification as send_monitoring_notification,
                        )

                        cfg = load_notification_config()
                        send_monitoring_notification("warning", msg, cfg)
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"Scorecard alert send failed: {e}")
            else:
                logger.info("Scorecard degradation check: no sources degraded.")
        return

    if getattr(args, "explain_metric", None):
        # Read-only: what did this metric mean on a given date (per-source registry).
        from datetime import datetime as _dt

        from shared.metric_registry import explain_metric

        if not args.source:
            logger.error("--explain-metric requires --source")
            return
        as_of = None
        if getattr(args, "as_of", None):
            try:
                as_of = _dt.strptime(args.as_of, "%Y-%m-%d").date()
            except ValueError:
                logger.error("--as-of must be YYYY-MM-DD")
                return
        cfg = load_config(args.source)
        print(explain_metric(cfg, args.explain_metric, as_of=as_of))
        return

    if args.job_locks:
        locks = get_active_locks()
        if not locks:
            logger.info("No active job locks found")
        else:
            import json

            logger.info("Active job locks:")
            print(json.dumps(locks, indent=2))
        return

    if args.cleanup_locks:
        cleanup_expired_locks()
        logger.info("Cleaned up all expired job locks in BigQuery")
        return

    if args.force_release_lock:
        lock_source = args.force_release_lock_source or args.source
        if not lock_source:
            logger.error(
                "--force-release-lock requires either --force-release-lock-source or --source "
                "to identify which source's lock row to delete."
            )
            return
        force_release_lock(lock_source, args.force_release_lock)
        return

    # Load configuration
    config = load_config(args.source)

    # Skip sources explicitly disabled at config level (pipeline.enabled: false
    # or source.active: false). This encodes "not part of the standard ETL" at
    # the source itself -- e.g. OneTrust, which is configured but not
    # operationally run. --validate-only is still allowed so the config can be
    # inspected; an actual run is refused with a clear message.
    pipeline_enabled = config.get("pipeline", {}).get("enabled", True)
    source_active = config.get("source", {}).get("active", True)
    if (pipeline_enabled is False or source_active is False) and not args.validate_only:
        logger.error(
            "Source '%s' is disabled (pipeline.enabled: false / source.active: "
            "false) and will not be run. Remove the disable flag in its config to "
            "re-enable, or use --validate-only to inspect it.",
            args.source,
        )
        sys.exit(0)

    # Add environment to config (used for environment-specific GCS buckets, datasets, etc.)
    if "pipeline" not in config:
        config["pipeline"] = {}
    config["environment"] = (
        args.env
    )  # Make environment available throughout the pipeline

    # Apply --vars overrides if provided
    if hasattr(args, "vars") and args.vars:
        runtime_overrides = parse_vars(args.vars)
        if runtime_overrides:
            logger.info(
                f"Applying {len(runtime_overrides)} runtime override(s) from --vars"
            )
            for key, value in runtime_overrides.items():
                # Log top-level overrides (don't expand nested dicts in log)
                if isinstance(value, dict):
                    logger.info(f"Override: {key} = <dict with {len(value)} key(s)>")
                else:
                    logger.info(f"Override: {key} = {value}")

            # Deep merge overrides into config
            config = deep_merge(config, runtime_overrides)
            logger.info("Runtime overrides applied successfully")

    # Override alerting if --no-alerts flag is set
    if args.no_alerts:
        if "alerting" not in config:
            config["alerting"] = {}
        config["alerting"]["enabled"] = False
        logger.info("Alerts suppressed via --no-alerts flag")

    # Validate configuration (unless explicitly skipped)
    if not args.skip_validation:
        logger.info(f"Validating configuration for {args.source}...")

        # Determine if we need to check GCS (skip if --extract-only or --skip-gcs)
        check_gcs = not (args.extract_only or args.skip_gcs)

        is_valid, validation_errors = validate_pipeline(
            args.source, config, check_gcs=check_gcs
        )

        # Generate and display report
        report = generate_validation_report(args.source, is_valid, validation_errors)
        print(report)

        if not is_valid:
            logger.error(
                f"Configuration validation failed with {len(validation_errors)} error(s)"
            )
            sys.exit(1)

        logger.info("Configuration validation passed")
    else:
        logger.warning("Configuration validation skipped (--skip-validation flag)")

    # If validate-only, exit here
    if args.validate_only:
        logger.info("Validation complete. Exiting (--validate-only flag)")
        return

    # --post-process-only: run ONLY the pipeline post_process steps and exit.
    # Recovery path for "load succeeded but post-processing failed" -- skips
    # discovery, extraction, GCS, staging, and merge entirely. Reuses the same
    # run_post_processing / evaluate_post_process_outcome the normal flow uses,
    # so SQL-vs-Python dispatch, ordering, and the WARNING/alert semantics match.
    if getattr(args, "post_process_only", False):
        return run_post_process_only(config, args)

    # Initialize features
    initialize_features(config)

    # Get tables to extract
    tables = []
    groups = []
    if args.table:
        tables = args.table
    elif args.tables:
        tables = args.tables
    elif args.group:
        groups = args.group
    elif args.groups:
        groups = args.groups
    else:
        tables = get_available_tables(args.source)

    # Prior DLQ retry_count per table (populated only under --retry-dlq). Empty on a
    # normal run -- record_failures then records retry_count 0 for first failures.
    dlq_retry_counts = {}

    # --retry-dlq: re-run ONLY the source's unresolved dead-letter-queue tables
    # (the read-back half of the DLQ). Overrides the table list with exactly the
    # tables that failed in a prior run and are still under max_retries. The rest
    # of the pipeline is unchanged -- it re-extracts those tables via the normal
    # idempotent path, and the end-of-run mark_resolved() flips them to resolved
    # on success / record_failures() bumps retry_count on another failure.
    if getattr(args, "retry_dlq", False):
        from shared.dead_letter import find_retryable_tables

        dlq_config = load_config(args.source)
        retry_rows = find_retryable_tables(
            get_bigquery_client(), dlq_config, args.source
        )
        if not retry_rows:
            logger.info(
                "--retry-dlq: no unresolved dead-letter tables to retry for %s "
                "(nothing failed, all resolved, or all at max_retries).",
                args.source,
            )
            sys.exit(0)
        tables = [r["table_name"] for r in retry_rows]
        groups = []
        # Prior retry_count per table, so a re-failure during this retry records
        # retry_count = prior + 1 (otherwise it would reset to 0 and never reach
        # max_retries -> infinite retries).
        dlq_retry_counts = {r["table_name"]: int(r["retry_count"]) for r in retry_rows}
        logger.info(
            "--retry-dlq: retrying %d failed table(s) for %s: %s",
            len(tables),
            args.source,
            tables,
        )
        for r in retry_rows:
            logger.info(
                "  %s (attempt %d, last error: %s)",
                r["table_name"],
                int(r["retry_count"]) + 1,
                str(r.get("error_message") or "")[:80],
            )

    # Validate groups if provided
    if groups:
        validate_groups(args.source, groups)

        # Resolve tables from groups BEFORE creating job (so job tracking has correct table list)
        # This ensures the jobs table has the actual table names, not just the group name.
        # Dedup across groups: a run spanning multiple groups must not list a table twice.
        if not tables:  # Only resolve if tables weren't explicitly specified
            config = load_config(args.source)
            resources = config.get("resources", {})
            seen = set()
            for group in groups:
                for name, conf in resources.items():
                    if (
                        conf.get("group") == group
                        and conf.get("active", True) is not False
                        and name not in seen
                    ):
                        seen.add(name)
                        tables.append(name)
            logger.info(f"Resolved {len(tables)} tables from groups: {groups}")

    # Validate tables if provided
    if (
        tables and not groups
    ):  # Only validate if user explicitly provided tables (not from get_available_tables)
        # Check if tables were explicitly provided by user (not defaulted)
        # Skip validation in dry-run mode (user may want to test with invalid tables)
        if (args.table or args.tables) and not args.dry_run:
            validate_tables(args.source, tables)

    # Get sites to extract from
    sites = []
    if args.site:
        sites = args.site
    elif args.sites:
        sites = args.sites
    elif args.all_sites:
        sites = ["all"]  # Pass 'all' to let extractor use discovery API
    else:
        # Default: let extractor use discovery API or default sites
        sites = ["all"]

    # If using 'all', we need to ensure account discovery works
    if sites == ["all"]:
        logger.info("Using 'all' sites - will attempt account discovery")

    # Process sites to replace environment variables
    processed_sites = []
    for site in sites:
        if isinstance(site, str) and site.startswith("${") and site.endswith("}"):
            env_var = site[2:-1]
            env_value = os.environ.get(
                env_var, f"https://example-{env_var.lower()}.com"
            )
            processed_sites.append(env_value)
        else:
            processed_sites.append(site)
    sites = processed_sites

    # Check if we have sites
    if not sites:
        logger.error("No sites/accounts specified")
        return

    # Generate a unique execution ID (resume/checkpoint removed -- re-running a
    # partially-failed job is safe because the per-table hash-merge is idempotent).
    execution_id = args.execution_id or str(uuid.uuid4())

    # Dry run: report the execution and staging plan, then exit before any
    # job tracking, locks, or BigQuery writes.
    if args.dry_run:
        from shared.staging_lifecycle import get_staging_scope, prepare_staging_for_run
        from shared.transform import get_datasets

        dry_bq_client = get_bigquery_client()
        staging_dataset, production_dataset = get_datasets(config, "production")
        wipe_scope = get_staging_scope(config, args.staging_wipe_scope)
        # Normalize affixes here too (the main path normalizes later, after this
        # early dry-run return) so the staging plan reflects affixed targets.
        dry_schema_prefix, dry_schema_suffix = normalize_schema_affix(
            getattr(args, "schema_prefix", None),
            getattr(args, "schema_suffix", None),
        )

        logger.info("")
        logger.info("=" * 80)
        logger.info("DRY RUN: no jobs, locks, or BigQuery changes will be made")
        logger.info("=" * 80)
        logger.info(f"Source: {args.source}")
        logger.info(f"Groups: {groups or '(none)'}")
        logger.info(f"Tables ({len(tables)}): {tables}")
        logger.info(f"Sites: {sites}")
        logger.info(f"Refresh mode: {args.refresh or 'incremental'}")
        logger.info(f"Staging dataset: {staging_dataset} (wipe scope: {wipe_scope})")
        logger.info(f"Production dataset: {production_dataset}")

        try:
            plan = prepare_staging_for_run(
                dry_bq_client,
                config,
                args.source,
                tables,
                staging_dataset=staging_dataset,
                groups=groups,
                scope=wipe_scope,
                plan_only=True,
                schema_prefix=dry_schema_prefix,
                schema_suffix=dry_schema_suffix,
            )
            logger.info("")
            logger.info("Staging plan:")
            for entry in plan.archived:
                logger.info(
                    f"  Would archive: {entry['table']} ({entry['row_count']:,} rows)"
                )
            for table_id in plan.dropped:
                logger.info(f"  Would drop: {table_id}")
            for table_id in plan.skipped:
                logger.info(f"  No archive needed (empty/missing): {table_id}")
        except Exception as e:
            logger.warning(f"Could not compute staging plan: {e}")

        logger.info("")
        logger.info("Dry run complete. Exiting.")
        return

    # Create centralized job tracking
    command_line = " ".join(sys.argv[1:])  # Get full command line
    job_config = {
        "tables": tables,
        "sites": sites,
        "groups": groups,
        "refresh_mode": args.refresh or "incremental",
        "environment": args.env,  # Use actual environment from --env argument
        "triggered_by": args.triggered_by,
        "parent_job_id": getattr(
            args, "parent_job_id", None
        ),  # For job chaining/lineage
    }

    job_id = create_centralized_job(
        source=args.source,
        job_name=f"{args.source}_extraction",
        command_line=command_line,
        config=job_config,
    )
    # Propagate to extractors / post-process SQL so BigQuery child jobs can be
    # cost-attributed via INFORMATION_SCHEMA (label orchestrator_job_id).
    config["_orchestrator_job_id"] = job_id
    # Stamp the label on the shared client so EVERY client.query() in this run carries
    # it automatically -- covers sources that run raw bq_client.query() without setting
    # labels themselves (e.g. identity_hub, adops_sheet_sync), so their cost is captured
    # too. Plugins that already set the label explicitly are unaffected (same value).
    try:
        from shared.bigquery_client import set_default_job_labels

        set_default_job_labels({"orchestrator_job_id": job_id})
    except Exception as _lbl_err:  # noqa: BLE001
        logger.debug("Could not stamp orchestrator_job_id label: %s", _lbl_err)

    # Start monitoring (keep existing for backward compatibility)
    execution_start_time = datetime.now()
    bq_client = get_bigquery_client()

    # Sync this source's semantic metric definitions to the registry table so the
    # effective-dated history stays current and queryable (best-effort, never blocks).
    try:
        from shared.metric_registry import (
            load_entries_for_source,
            sync_definitions_to_bq,
        )

        _mdefs = load_entries_for_source(config)
        if _mdefs:
            _n = sync_definitions_to_bq(bq_client, args.source, _mdefs)
            logger.debug("Synced %d metric definition(s) to registry", _n)
    except Exception as _mderr:  # noqa: BLE001
        logger.debug("Metric-definition sync skipped: %s", _mderr)

    # Reap dead PIDs on this host + stale RUNNING rows on any host (see monitoring.stale_running_job_hours)
    stale_hours_arg = None
    _mon = config.get("monitoring")
    if isinstance(_mon, dict) and _mon.get("stale_running_job_hours") is not None:
        try:
            stale_hours_arg = int(_mon["stale_running_job_hours"])
        except (TypeError, ValueError):
            stale_hours_arg = None
    try:
        cleaned = cleanup_orphaned_jobs(
            bq_client,
            MONITORING_DATASET,
            stale_running_hours=stale_hours_arg,
        )
        if cleaned:
            logger.info(
                "Pre-run job cleanup: %s row(s) terminalized (orphan PID and/or stale RUNNING)",
                cleaned,
            )
    except Exception as clean_err:
        logger.warning("Pre-run job cleanup failed (continuing): %s", clean_err)

    monitoring_execution_id = start_execution(
        bq_client, args.source, tables, "staging", config
    )

    try:
        # Update centralized job status to running
        update_job_status(job_id, "RUNNING")

        # Pre-flight: project run time from historical per-table durations and
        # block (unless --force) when it exceeds the configured ceiling. Long
        # runs stay safe via the lock heartbeat, but a multi-hour projection
        # usually means the run should be split by group -- surface that BEFORE
        # taking the lock or touching staging. Best-effort: never hard-fails on
        # its own errors. Skipped for SQL-only sources with no per-table history.
        if not args.dry_run and args.source != "profile_database":
            try:
                from shared.run_preflight import preflight_check

                pf = preflight_check(
                    bq_client,
                    config,
                    args.source,
                    tables,
                    sites,
                    force=getattr(args, "force", False),
                )
                if pf["ok"]:
                    logger.info(pf["message"])
                else:
                    logger.error(pf["message"])
                    update_job_status(
                        job_id, "FAILED", error_message="Preflight size gate"
                    )
                    # A preflight block exits here, BEFORE the main try/except that
                    # normally fires the failure notification -- so without this an
                    # operator gets NO email and the source silently stops loading
                    # (root cause of the WordPress June 2026 multi-day stall). Mark
                    # the monitoring execution failed and send the failure email for
                    # ANY source, honoring the same notifications.on_failure flag the
                    # main handler uses.
                    try:
                        fail_execution(
                            bq_client,
                            monitoring_execution_id,
                            f"Preflight size gate: {pf['message']}",
                            config,
                        )
                    except Exception as fe:  # noqa: BLE001
                        logger.warning("Preflight: fail_execution failed: %s", fe)
                    # Default ON when unset: a missing on_failure must NOT mean
                    # "stay silent" -- matches send_notification's own default and
                    # the requirement that ANY blocked ETL emails an operator.
                    if config.get("notifications", {}).get("on_failure", True):
                        try:
                            send_notification(
                                "failure",
                                (
                                    f"Pipeline NOT run for source "
                                    f"'{args.source}': preflight run-size gate "
                                    f"blocked it (projected run time over the "
                                    f"configured ceiling). No data was extracted.\n\n"
                                    f"{pf['message']}\n\n"
                                    f"To override for a one-off run, re-run with "
                                    f"--force. To raise the limit, set "
                                    f"pipeline.preflight.max_run_minutes in the "
                                    f"source config."
                                ),
                                config,
                                severity="high",
                                additional_recipients=additional_emails,
                            )
                        except Exception as ne:  # noqa: BLE001
                            logger.warning(
                                "Preflight: failure notification failed: %s", ne
                            )
                    sys.exit(1)
            except SystemExit:
                raise
            except Exception as pf_err:  # noqa: BLE001
                logger.debug("Preflight check skipped (error): %s", pf_err)

        # Acquire per-source ETL lock to prevent concurrent runs of the same source.
        # Different sources run concurrently; suites hold their own etl_suite:<name>
        # lock independently and each step still acquires its source lock here.
        if not acquire_job_lock(args.source, job_id):
            logger.error(
                f"Cannot acquire ETL lock for source '{args.source}'. "
                f"Another job for this source is already running. "
                f"Aborting to prevent data corruption."
            )
            logger.info(
                "To view active locks, run: python orchestrate.py --env %s --job-locks",
                args.env,
            )
            update_job_status(
                job_id, "FAILED", error_message="Could not acquire ETL lock"
            )
            sys.exit(1)

        # Prepare staging before extraction: archive the previous run's staging
        # tables (snapshot with TTL + manifest row) then drop them, scoped to
        # ONLY the tables this run extracts so concurrent sub-sources within the
        # same source do not destroy each other's staging data.
        # Legacy dataset-wide wipe is available via --staging-wipe-scope dataset.
        from shared.transform import get_datasets
        from shared.bigquery_utils import ensure_dataset_exists
        from shared.staging_lifecycle import get_staging_scope, prepare_staging_for_run

        staging_dataset, production_dataset = get_datasets(config, "production")
        ensure_dataset_exists(bq_client, staging_dataset)
        ensure_dataset_exists(bq_client, production_dataset)

        # Normalize schema prefix/suffix (auto-add underscores) BEFORE staging prep
        # so the scoped staging cleanup/archive targets the same affixed tables the
        # transform phase writes (prefixed test runs). Reused by the transform below.
        schema_prefix, schema_suffix = normalize_schema_affix(
            args.schema_prefix if hasattr(args, "schema_prefix") else None,
            args.schema_suffix if hasattr(args, "schema_suffix") else None,
        )

        preserve_staging = args.source == "profile_database" and getattr(
            args, "build_mode", None
        ) in ("resume_rebuild", "resume_publish")
        if preserve_staging:
            logger.info(
                f"Preserving staging dataset: {staging_dataset} "
                f"(profile_database {getattr(args, 'build_mode', None)} needs the existing snapshot/candidate context)"
            )
        else:
            wipe_scope = get_staging_scope(config, args.staging_wipe_scope)
            logger.info(
                f"Preparing staging dataset: {staging_dataset} (scope: {wipe_scope})"
            )
            try:
                staging_prep = prepare_staging_for_run(
                    bq_client,
                    config,
                    args.source,
                    tables,
                    staging_dataset=staging_dataset,
                    execution_id=monitoring_execution_id,
                    job_id=job_id,
                    groups=groups,
                    scope=wipe_scope,
                    schema_prefix=schema_prefix,
                    schema_suffix=schema_suffix,
                )
            except Exception as e:
                # A total failure of staging prep means we cannot guarantee the
                # archive-before-wipe contract -- fail closed rather than extract
                # on top of an unknown staging state.
                logger.error(f"Staging preparation failed: {e}")
                update_job_status(
                    job_id, "FAILED", error_message=f"Staging prep failed: {e}"
                )
                raise

            # Fail closed when a table that held data could not be archived: its
            # staging was deliberately NOT dropped, but proceeding would extract
            # over un-archived state that the rollback/comparison guarantee
            # depends on. Drop/manifest failures alone are logged, not fatal.
            if getattr(staging_prep, "had_unsafe_failure", False):
                logger.error(
                    "Staging prep could not archive one or more non-empty tables; "
                    "aborting before extraction to preserve the archive-before-wipe "
                    "guarantee. Errors: %s",
                    "; ".join(staging_prep.errors[:5]),
                )
                update_job_status(
                    job_id,
                    "FAILED",
                    error_message="Staging archive-before-wipe failed (fail closed)",
                )
                raise RuntimeError(
                    "Staging prep failed to archive non-empty table(s); aborted to "
                    "avoid un-recoverable staging loss."
                )
            if staging_prep.errors:
                logger.warning(
                    "Staging prep completed with non-fatal issues: %s",
                    "; ".join(staging_prep.errors[:5]),
                )

        # Validate Facebook entity types if applicable
        if args.source == "facebook":
            validate_facebook_entity_types(sites, tables)

        # Get the appropriate extractor function
        extractor_func = get_extractor_function(args.source)

        # Check for table dependencies and inform user
        table_dependencies = config.get("pipeline", {}).get("table_dependencies", {})
        dependent_tables = []
        parent_tables_needed = set()

        for table in tables:
            if table in table_dependencies:
                dep_info = table_dependencies[table]
                parent_table = dep_info.get("extracted_via")
                reason = dep_info.get("reason", "API limitation")

                logger.info(f"")
                logger.info(f"NOTE: '{table}' is extracted via '{parent_table}' table")
                logger.info(f"    Reason: {reason}")
                logger.info(f"    To extract '{table}', run: --tables {parent_table}")
                logger.info(f"")

                dependent_tables.append(table)
                parent_tables_needed.add(parent_table)

        # Remove dependent tables from extraction list
        if dependent_tables:
            tables = [t for t in tables if t not in dependent_tables]

            # If user requested ONLY dependent tables, suggest parent tables
            if not tables and parent_tables_needed:
                logger.info(f"All requested tables are dependent tables.")
                logger.info(
                    f"To extract them, run with parent table(s): --tables {' '.join(parent_tables_needed)}"
                )
                logger.info(f"")
                sys.exit(0)

        # schema_prefix/schema_suffix already normalized above (before staging prep).

        # Determine load pattern (use command line, then group's recommended_pattern, then config default)
        load_pattern = args.load_pattern
        if not load_pattern and groups:
            # Use the group's recommended_pattern if available
            group_configs = config.get("groups", {})
            for g in groups:
                g_conf = group_configs.get(g, {})
                if g_conf.get("recommended_pattern"):
                    load_pattern = g_conf["recommended_pattern"]
                    logger.info(
                        f"Using load pattern '{load_pattern}' from group '{g}' config"
                    )
                    break
        if not load_pattern:
            extractor_cfg = config.get("extractor", {})
            load_pattern = extractor_cfg.get("default_load_pattern", "standard")

        # Set up extraction parameters
        extraction_params = {
            "config": config,
            "sites": sites,
            "tables": tables,
            "groups": groups,  # Add groups parameter
            "test_mode": args.test_mode,
            "refresh_mode": args.refresh or "incremental",
            "lookback_days": args.lookback,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "group": groups[0] if groups else None,  # For backward compatibility
            "force_inactive": args.force_inactive,  # Allow running inactive tables
            "schema_prefix": schema_prefix,  # Apply prefix to table names
            "schema_suffix": schema_suffix,  # Apply suffix to table names
            "load_pattern": load_pattern,  # Add load pattern
            "use_batch_api": getattr(args, "use_batch_api", None),
            # SQL-only pipelines (e.g. profile_database).
            # Back-compat: allow profile_database runs to omit --build-mode and behave like other ETLs.
            # Mapping aligned with the refresh_mode semantics enforced in bigquery_utils:
            #   --rebuild                   -> build-mode=rebuild (destructive, blue/green rebuild)
            #   --refresh full (no rebuild) -> build-mode=refresh (non-destructive max-data refresh)
            #   default / --refresh incremental -> build-mode=refresh
            # Note: --refresh full alone NO LONGER triggers a destructive rebuild - that requires
            # an explicit --rebuild or --build-mode rebuild. This matches the ETL-side fix where
            # --refresh full means "fetch maximum data" without DELETE-not-matched.
            "build_mode": (
                getattr(args, "build_mode", None)
                if args.source != "profile_database"
                or getattr(args, "build_mode", None)
                else ("rebuild" if args.rebuild else "refresh")
            ),
            "force": getattr(args, "force", False),
            # Let plugins that do their own out-of-band loads (e.g. limesurvey's
            # token table direct-load) honor extract-only / skip-transform and not
            # write to BigQuery when the operator asked for no data update.
            "extract_only": getattr(args, "extract_only", False),
            "skip_transform": getattr(args, "skip_transform", False),
        }

        # Run the extraction pipeline
        if args.source == "profile_database" and not tables and not groups:
            logger.info(f"Running {args.source} SQL pipeline (no table extraction)")
        elif groups:
            logger.info(
                f"Running {args.source} extraction for groups {groups} from {len(sites)} sites"
            )
        else:
            logger.info(
                f"Running {args.source} extraction for {len(tables)} tables from {len(sites)} sites"
            )

        # Call the extractor function
        # Determine truncate_staging behavior
        if args.no_truncate_staging:
            truncate_staging = False
        elif args.truncate_staging:
            truncate_staging = True
        else:
            # Default: truncate staging for full refresh, keep for incremental
            truncate_staging = args.refresh == "full"

        # Add common parameters required by all extractors
        extraction_params.update(
            {
                "batch_size": args.batch_size,
                "max_retries": args.max_retries,
                "skip_hash_merge": False,
                "archive_staging": False,
                "truncate_staging": truncate_staging,
                "rebuild": args.rebuild,  # Only rebuild production if explicitly requested
                "bq_client": bq_client,
                "execution_id": monitoring_execution_id,
                "centralized_job_id": job_id,
                "parallel_workers": args.parallel,
                "skip_validation": (
                    args.skip_validation if hasattr(args, "skip_validation") else False
                ),
                "export_format": None,  # Not used in current pipeline
                "export_dir": "./data/exports",  # Default export directory
                "survey_limit": (
                    args.survey_limit if hasattr(args, "survey_limit") else None
                ),  # LimeSurvey testing
                "survey_created_days": (
                    args.survey_created_days
                    if hasattr(args, "survey_created_days")
                    else None
                ),  # LimeSurvey discovery window
                "no_reconcile": (
                    args.no_reconcile if hasattr(args, "no_reconcile") else False
                ),  # LimeSurvey BQ reconciliation toggle
                "limit": (
                    args.limit if hasattr(args, "limit") else None
                ),  # General row/item limit for testing
                "test_emails": (
                    args.test_emails if hasattr(args, "test_emails") else None
                ),  # Test specific emails
                "test_list_ids": (
                    args.test_list_ids if hasattr(args, "test_list_ids") else None
                ),  # Filter to specific lists
            }
        )

        # Call the extractor function with automatic log capture
        extraction_start_time = datetime.now()

        def run_extractor():
            return extractor_func(**extraction_params)

        results = capture_extractor_logs(job_id, run_extractor, config)

        # Check if extraction explicitly failed
        if results and results.get("success") is False:
            extraction_errors = results.get("errors", [])
            error_msg = (
                "; ".join(extraction_errors)
                if extraction_errors
                else "Extractor returned success=False"
            )
            logger.error(f"Extraction failed: {error_msg}")
            raise RuntimeError(f"Extraction failed: {error_msg}")

        # Check if any data was extracted
        total_rows = results.get("total_rows", 0) if results else 0
        if total_rows == 0:
            logger.info(
                "No data extracted - skipping remaining pipeline steps (GCS upload, external tables, transform)"
            )
        else:
            logger.info(
                f"Extracted {total_rows} rows - proceeding with remaining pipeline steps"
            )

            # Show extraction summary
            logger.info("")
            logger.info("=" * 80)
            logger.info("EXTRACTION PHASE COMPLETE")
            logger.info("=" * 80)
            table_rows_summary = results.get("table_rows", {}) if results else {}
            if table_rows_summary:
                logger.info(f"Successfully extracted {len(table_rows_summary)} tables:")
                for table_name in sorted(table_rows_summary.keys()):
                    logger.info(
                        f"[OK] {table_name}: {table_rows_summary[table_name]:,} rows"
                    )
            elif results and results.get("table_files"):
                logger.info(
                    f"Successfully extracted {len(results['table_files'])} tables:"
                )
                for table_name in sorted(results["table_files"].keys()):
                    logger.info(f"[OK] {table_name}")
            logger.info(f"Total rows: {total_rows:,}")
            logger.info("=" * 80)
            logger.info("")

        # Initialize variables that may be referenced later in error handling
        transform_errors = []
        has_extraction_failures = False
        # Schema-drift governance: set when a new column is auto-added to a
        # production table during this run (escalates the job to WARNING -> email).
        schema_drift_detected = False
        schema_drift_detail = []

        # Capture extraction timing and file sizes for monitoring
        extraction_end_time = datetime.now()
        extraction_file_sizes = {}  # table -> file size in MB
        table_rows = results.get("table_rows", {}) if results else {}
        table_files = results.get("table_files", {}) if results else {}

        # Post-extraction contract validation (backlog B2): before any GCS
        # upload, assert the plugin handed back valid table_files -- logical
        # resource keys (not affixed/physical names), non-empty string paths to
        # files that exist locally. None entries are the documented "skip this
        # table" signal and are filtered out. Fail fast on real violations: each
        # is a bug that would otherwise surface deep in upload/merge or
        # mis-target a table. Skipped for SQL-only sources that return no
        # table_files at all.
        if table_files:
            from shared.extraction_contract import validate_table_files

            table_files, contract_violations = validate_table_files(table_files, config)
            if contract_violations:
                for v in contract_violations:
                    logger.error("Extraction contract violation: %s", v)
                raise RuntimeError(
                    f"{args.source}: extraction returned invalid table_files "
                    f"({len(contract_violations)} violation(s)); aborting before "
                    f"GCS upload. First: {contract_violations[0]}"
                )

        for table, file_path in table_files.items():
            try:
                import os as _os

                if _os.path.exists(file_path):
                    extraction_file_sizes[table] = round(
                        _os.path.getsize(file_path) / (1024 * 1024), 2
                    )
            except Exception:
                pass

        # Process results and upload to GCS if needed (only if data was extracted)
        gcs_paths = {}
        if total_rows > 0 and not args.extract_only and not args.skip_gcs:
            logger.info("Uploading extracted Parquet files to GCS...")

            # Get GCS bucket name from config
            bucket_name = get_gcs_bucket_name(config)
            if not bucket_name:
                logger.error("GCS bucket name not configured - skipping GCS upload")
            else:
                # Verify GCS access
                if verify_gcs_object_access(bucket_name):
                    # NOTE: use the contract-validated `table_files` from above
                    # (None entries filtered, violations already aborted) -- do
                    # NOT re-read results["table_files"] here or the validation
                    # is undone.

                    def _upload_table(table_and_path):
                        """Upload a single table to GCS (for parallel execution)."""
                        tbl, local_path = table_and_path
                        try:
                            logger.info(f"Uploading {tbl} from {local_path}")
                            gcs_result = upload_file_to_gcs(
                                local_file_path=local_path,
                                config=config,
                                source=args.source,
                                resource=tbl,
                                execution_id=execution_id,
                            )
                            if gcs_result.get("staging_gcs_path"):
                                logger.info(
                                    f"Successfully uploaded {tbl} to {gcs_result['staging_gcs_path']}"
                                )
                                # Clean up local Parquet file after successful upload
                                try:
                                    # No local `from pathlib import Path` here: it
                                    # would mark Path local to all of main() and
                                    # shadow the module-level import (line 26).
                                    p = Path(local_path)
                                    if p.exists():
                                        p.unlink()
                                        logger.debug(
                                            f"Removed local file: {local_path}"
                                        )
                                except Exception as cleanup_error:
                                    logger.warning(
                                        f"Could not remove local file {local_path}: {cleanup_error}"
                                    )
                                return tbl, gcs_result["staging_gcs_path"]
                            else:
                                logger.error(f"Failed to upload {tbl} to GCS")
                                return tbl, None
                        except Exception as e:
                            logger.error(f"Error uploading {tbl} to GCS: {e}")
                            return tbl, None

                    # Parallel GCS uploads (3 concurrent)
                    from concurrent.futures import ThreadPoolExecutor, as_completed

                    max_upload_workers = min(3, max(1, len(table_files)))
                    if table_files:
                        with ThreadPoolExecutor(max_workers=max_upload_workers) as pool:
                            futures = {
                                pool.submit(_upload_table, item): item[0]
                                for item in table_files.items()
                            }
                            for future in as_completed(futures):
                                tbl, staging_path = future.result()
                                if staging_path:
                                    gcs_paths[tbl] = staging_path
                    else:
                        logger.info(
                            "No pending table files to upload (already handled by pipeline)"
                        )
                else:
                    logger.error(
                        f"Cannot access GCS bucket {bucket_name} - skipping upload"
                    )

        # Create external tables if needed (only if data was extracted)
        external_tables = {}
        if (
            total_rows > 0
            and not args.extract_only
            and not args.skip_external_tables
            and not args.skip_gcs
        ):
            logger.info("Creating external tables from GCS Parquet files...")

            from google.cloud import bigquery
            from shared.external_tables import create_external_table

            for table, gcs_path in gcs_paths.items():
                try:
                    # Create external table in STAGING dataset
                    staging_dataset = config.get("pipeline", {}).get(
                        "staging_dataset", f"{args.source}_staging"
                    )

                    # Get actual table name from YAML config (same as used for staging/production)
                    table_config = config.get("resources", {}).get(table, {})
                    actual_table_name = table_config.get(
                        "table_name", f"{args.source}_{table}"
                    )
                    # Apply schema prefix/suffix so prefixed test runs (--schema-prefix)
                    # target the affixed physical table, not production. Affixes are
                    # already normalized (underscores added by normalize_schema_affix),
                    # so pass them directly with no separators.
                    actual_table_name = apply_table_affix(
                        actual_table_name, schema_prefix, schema_suffix
                    )
                    external_table_name = f"{actual_table_name}_external"

                    logger.info(
                        f"Creating external table {staging_dataset}.{external_table_name} from {gcs_path}"
                    )

                    # Create external table (Parquet format with auto-schema detection)
                    success = create_external_table(
                        client=bq_client,
                        dataset_id=staging_dataset,
                        table_id=external_table_name,
                        gcs_path=gcs_path,
                        schema=None,  # Let BigQuery auto-detect from Parquet
                    )

                    if success:
                        external_tables[table] = (
                            f"{bq_client.project}.{staging_dataset}.{external_table_name}"
                        )
                        logger.info(f"Created external table: {external_tables[table]}")
                    else:
                        logger.error(f"Failed to create external table for {table}")

                except Exception as e:
                    logger.error(f"Error creating external table for {table}: {e}")

        # Transform to final tables if needed (only if data was extracted)
        if (
            total_rows > 0
            and not args.extract_only
            and not args.skip_transform
            and not args.skip_external_tables
        ):
            logger.info("")
            logger.info("=" * 80)
            logger.info("TRANSFORM PHASE STARTING")
            logger.info("=" * 80)
            logger.info("Transforming external tables -> staging -> production...")
            logger.info("")

            # Import transform function
            from shared.transform import get_datasets, get_hash_merge_keys
            from shared.schema_builder import get_schema_for_table

            # process_hash_merge is NOT re-imported here: it is already imported at
            # module level (line 58), and a local import would mark it local to all
            # of main() and shadow it.
            from shared.bigquery_utils import ensure_dataset_exists
            from shared.schema_evolution import apply_schema_evolution

            # Get datasets (already ensured to exist at pipeline start)
            staging_dataset, production_dataset = get_datasets(config, "production")

            # Transform each table that was actually extracted AND has an external table
            # Only process tables that successfully completed extraction -> GCS -> external table creation
            extracted_tables = list(external_tables.keys())
            transform_success = True
            # transform_errors already initialized earlier to handle early failures

            if extracted_tables:
                logger.info(
                    f"Transform processing {len(extracted_tables)} tables: {extracted_tables}"
                )
                logger.info("")
            else:
                logger.info(
                    "No Parquet/GCS tables to transform (may be legacy direct-load tables like leads)"
                )

            for table in extracted_tables:
                # Abort before the next destructive MERGE if our ETL lock was
                # lost mid-run (heartbeat found no owned row). Proceeding could
                # race a concurrent run that now holds the lock. See
                # shared/job_lock_bq.is_lock_lost.
                if is_lock_lost(args.source):
                    logger.error(
                        "ETL lock for source '%s' was lost mid-run; aborting "
                        "before merging '%s' to avoid a concurrent-MERGE race. "
                        "Re-run once the other holder finishes.",
                        args.source,
                        table,
                    )
                    raise RuntimeError(
                        f"ETL lock lost mid-run for source '{args.source}'; "
                        f"aborted before merging '{table}' to prevent corruption."
                    )

                # Start tracking this table processing
                processing_id = None
                try:
                    # Get table_name from config first (needed for tracking)
                    table_config = config.get("resources", {}).get(table, {})
                    actual_table_name = table_config.get(
                        "table_name", f"{args.source}_{table}"
                    )
                    # Apply schema prefix/suffix (see note at the external-table
                    # resolution above) so staging/production targets are affixed
                    # consistently with the external table.
                    actual_table_name = apply_table_affix(
                        actual_table_name, schema_prefix, schema_suffix
                    )
                    primary_keys = table_config.get("primary_key", [])

                    # Initialize table tracking
                    processing_id = start_table_processing(
                        job_id=job_id,
                        execution_id=execution_id,
                        source=args.source,
                        table_name=table,
                        project_id=bq_client.project,
                        staging_dataset=staging_dataset,
                        production_dataset=production_dataset,
                        environment=args.env,
                        refresh_mode=(
                            "rebuild"
                            if args.rebuild
                            else (args.refresh or "incremental")
                        ),
                        test_mode=args.test_mode,
                        primary_keys=primary_keys,
                    )

                    logger.info(
                        f"Processing {table}: external -> {staging_dataset} -> {production_dataset}"
                    )

                    # Get external table reference
                    external_table_id = external_tables.get(table)
                    if not external_table_id:
                        logger.warning(
                            f"No external table found for {table} - skipping transform"
                        )
                        if processing_id:
                            complete_table_processing(
                                processing_id,
                                status="skipped",
                                error_message="No external table found",
                            )
                        continue

                    # Use actual_table_name (from YAML) for BOTH staging and production
                    # Note: table_config and actual_table_name already fetched above for tracking
                    staging_table_id = (
                        f"{bq_client.project}.{staging_dataset}.{actual_table_name}"
                    )
                    production_table_name = actual_table_name
                    production_table_id = f"{bq_client.project}.{production_dataset}.{production_table_name}"

                    # If --rebuild flag is set, DROP production table BEFORE copying external to staging
                    # Production table will be recreated with correct YAML schema during merge
                    # PROTECTED TABLES: These tables are NEVER deleted (source of truth)
                    PROTECTED_TABLES = [
                        "lime_surveys_columnar_completed",  # LimeSurvey source of truth - append only
                    ]
                    if args.rebuild:
                        # Check if this table is protected
                        if actual_table_name in PROTECTED_TABLES:
                            logger.warning(
                                f"PROTECTED TABLE: {actual_table_name} cannot be deleted (source of truth)"
                            )
                            logger.warning(
                                f"Skipping rebuild for this table - data will be preserved"
                            )
                            continue

                        logger.info(
                            f"REBUILD MODE: Dropping production table {production_table_id}"
                        )
                        try:
                            from google.cloud.exceptions import NotFound

                            # Delete the table
                            bq_client.delete_table(
                                production_table_id, not_found_ok=True
                            )
                            logger.info(f"Production table delete command executed")

                            # VERIFY table is actually gone
                            try:
                                table_check = bq_client.get_table(production_table_id)
                                # If we get here, table still exists - this is BAD
                                logger.error(
                                    f"CRITICAL: Table still exists after delete command!"
                                )
                                logger.error(f"Table has {table_check.num_rows} rows")
                                logger.error(
                                    f"Rebuild mode requires empty table - CANNOT CONTINUE"
                                )
                                raise RuntimeError(
                                    f"Rebuild mode failed: Production table {production_table_id} could not be dropped. "
                                    f"Table still contains {table_check.num_rows} rows. "
                                    f"Check BigQuery permissions (bigquery.tables.delete) and table locks."
                                )
                            except NotFound:
                                # This is the EXPECTED outcome - table is gone
                                logger.info(
                                    f"SUCCESS: Production table dropped and verified deleted"
                                )
                                logger.info(
                                    f"Table will be recreated with fresh YAML schema during merge"
                                )
                        except NotFound:
                            # Table didn't exist - this is fine for rebuild
                            logger.info(
                                f"Production table didn't exist (normal for first rebuild)"
                            )
                        except RuntimeError:
                            # Re-raise our verification error
                            raise
                        except Exception as e:
                            # Unexpected error during delete
                            logger.error(
                                f"CRITICAL: Failed to drop production table: {e}"
                            )
                            logger.error(
                                f"Rebuild mode cannot continue with existing data"
                            )
                            logger.error(
                                f"Check service account permissions and BigQuery table locks"
                            )
                            raise RuntimeError(
                                f"Rebuild mode failed: Could not drop production table {production_table_id}. "
                                f"Error: {e}. Check permissions and table locks."
                            )

                    # Step 1: Copy external table to staging table
                    logger.info(
                        f"Step 1: Copying {external_table_id} -> {staging_table_id}"
                    )

                    # Get expected schema for validation
                    expected_schema = get_schema_for_table(args.source, table)

                    # Create or replace staging table from external table
                    copy_query = f"""
                    CREATE OR REPLACE TABLE `{staging_table_id}`
                    AS SELECT * FROM `{external_table_id}`
                    """
                    query_job = bq_client.query(copy_query)
                    query_job.result()  # Wait for completion

                    logger.info(f"Copied to staging table")

                    logger.info(
                        f"Step 2: Validating schema and evolving production table"
                    )

                    # Apply schema evolution (auto-add new columns from staging to production)
                    schema_success, schema_warnings = apply_schema_evolution(
                        client=bq_client,
                        source=args.source,
                        table=table,
                        staging_table_id=staging_table_id,
                        production_table_id=production_table_id,
                        execution_id=execution_id,
                        auto_add_columns=True,  # Automatically add new columns
                        allow_type_changes=False,  # Schema validation only (rebuild handled above)
                    )

                    if schema_warnings:
                        for warning in schema_warnings:
                            logger.warning(f"{warning}")
                            # Schema drift (a new column auto-added to production)
                            # escalates the job to WARNING -> email, so upstream
                            # schema changes are noticed, not silently absorbed.
                            if str(warning).startswith("SCHEMA DRIFT:"):
                                schema_drift_detected = True
                                schema_drift_detail.append(
                                    {
                                        "table": table,
                                        "stage": "schema_drift",
                                        "error": str(warning)[:500],
                                    }
                                )

                    if not schema_success:
                        error_msg = f"Schema validation failed"
                        logger.error(f"{error_msg} for {table}")
                        logger.error(f"[FAILED] {table}: Schema validation failed")
                        transform_success = False
                        transform_errors.append(
                            {
                                "table": table,
                                "error": error_msg,
                                "stage": "schema_validation",
                            }
                        )
                        continue

                    # Step 3: Load staging to production (merge or snapshot replace)
                    primary_keys = get_hash_merge_keys(args.source, table)

                    # Check write disposition: 'replace' for snapshot sources, 'merge' (default) for incremental
                    write_disposition = config.get("hash_merge", {}).get(
                        "write_disposition", "merge"
                    )
                    resource_disposition = table_config.get("write_disposition")
                    if resource_disposition:
                        write_disposition = resource_disposition

                    # The standard transform implements only 'replace' (snapshot)
                    # and 'merge'/'incremental' (hash merge). Anything else --
                    # notably 'append', which validation accepts but this path
                    # does NOT implement -- previously fell silently into the
                    # merge branch. Fail loudly instead of silently doing the
                    # wrong thing. (Nonstandard sources like the audit pipelines
                    # use 'append' via their own runners and never reach here.)
                    _STANDARD_DISPOSITIONS = {"replace", "merge", "incremental"}
                    if write_disposition not in _STANDARD_DISPOSITIONS:
                        error_msg = (
                            f"write_disposition '{write_disposition}' is not "
                            f"implemented on the standard transform path "
                            f"(supported: replace, merge). 'append' is accepted "
                            f"by validation but only handled by nonstandard "
                            f"source runners."
                        )
                        logger.error(f"[FAILED] {table}: {error_msg}")
                        transform_success = False
                        transform_errors.append(
                            {
                                "table": table,
                                "error": error_msg,
                                "stage": "write_disposition",
                            }
                        )
                        continue

                    if write_disposition == "replace":
                        logger.info(
                            f"Step 3: Snapshot replace {staging_table_id} -> {production_table_id}"
                        )

                        replace_config = {
                            "source": args.source,
                            "rebuild": args.rebuild,
                            "exclude_from_hash": config.get("hash_merge", {}).get(
                                "exclude_from_hash", []
                            ),
                            "execution_id": monitoring_execution_id,
                            "partitioning": table_config.get("partitioning"),
                            "clustering": table_config.get("clustering"),
                        }

                        merge_result = process_snapshot_replace(
                            client=bq_client,
                            staging_table=staging_table_id,
                            main_table=production_table_id,
                            primary_keys=primary_keys,
                            config=replace_config,
                        )
                    else:
                        logger.info(
                            f"Step 3: Merging {staging_table_id} -> {production_table_id}"
                        )

                        merge_config = {
                            "source": args.source,
                            "rebuild": args.rebuild,
                            # Merge layer: 'rebuild' -> DELETE-not-matched; 'full'/'incremental' -> no DELETE.
                            # Plugins still see args.refresh ('full' or 'incremental') for fetch-window decisions.
                            "refresh_mode": (
                                "rebuild"
                                if args.rebuild
                                else (args.refresh or "incremental")
                            ),
                            "exclude_from_hash": config.get("hash_merge", {}).get(
                                "exclude_from_hash", []
                            ),
                            "execution_id": monitoring_execution_id,
                            # Applied at production-table CREATE time (cannot be
                            # added to an existing table). No-op if the resource
                            # declares neither.
                            "partitioning": table_config.get("partitioning"),
                            "clustering": table_config.get("clustering"),
                        }

                        merge_result = process_hash_merge(
                            client=bq_client,
                            staging_table=staging_table_id,
                            main_table=production_table_id,
                            primary_keys=primary_keys,
                            config=merge_config,
                        )
                    success = merge_result.get("success", False)

                    if success:
                        rows_inserted = merge_result.get("rows_inserted", 0)
                        rows_updated = merge_result.get("rows_updated", 0)
                        rows_unchanged = merge_result.get("rows_unchanged", 0)
                        staging_rows = merge_result.get("staging_rows", 0)

                        rows_removed = merge_result.get("rows_removed", 0)
                        if write_disposition == "replace":
                            logger.info(f"Successfully replaced production table")
                            logger.info(
                                f"  Added: {rows_inserted}, Updated: {rows_updated}, Removed: {rows_removed}, Total: {staging_rows}"
                            )
                            logger.info(
                                f"[SUCCESS] {table}: Snapshot replace complete (+{rows_inserted} ~{rows_updated} -{rows_removed})"
                            )
                        else:
                            logger.info(f"Successfully merged to production table")
                            logger.info(
                                f"  Inserted: {rows_inserted}, Updated: {rows_updated}"
                            )
                            logger.info(
                                f"[SUCCESS] {table}: Completed successfully (Inserted: {rows_inserted}, Updated: {rows_updated})"
                            )

                        # Adaptive watermark: record this successful extraction so the
                        # NEXT run can size its window from "since we last succeeded"
                        # instead of a fixed lookback. Opt-in per table
                        # (incremental.use_watermark); a no-op otherwise, so default
                        # behavior is unchanged. Best-effort -- never blocks the run.
                        try:
                            from shared.watermark import (
                                watermark_enabled,
                                save_watermark,
                            )

                            _wm_cfg = config.get("resources", {}).get(table, {})
                            if watermark_enabled(_wm_cfg):
                                save_watermark(
                                    bq_client,
                                    production_dataset,
                                    args.source,
                                    table,
                                    execution_id,
                                    int(rows_inserted or 0) + int(rows_updated or 0),
                                )
                                logger.debug(
                                    "Saved watermark for %s.%s", args.source, table
                                )
                        except Exception as _wm_err:  # noqa: BLE001
                            logger.debug("Watermark save skipped: %s", _wm_err)

                        # Complete table tracking with success metrics
                        if processing_id:
                            try:
                                # Get production table row count after merge
                                prod_table = bq_client.get_table(production_table_id)
                                production_rows_after = prod_table.num_rows
                                production_size_mb = (
                                    prod_table.num_bytes / (1024 * 1024)
                                    if prod_table.num_bytes
                                    else 0
                                )

                                # Calculate extraction metrics for this table
                                table_rows_extracted = table_rows.get(table, 0)
                                table_gcs_path = gcs_paths.get(table, "")
                                table_parquet_size = extraction_file_sizes.get(table, 0)
                                extraction_duration = (
                                    (
                                        extraction_end_time - extraction_start_time
                                    ).total_seconds()
                                    if extraction_start_time
                                    else 0
                                )

                                complete_table_processing(
                                    processing_id,
                                    status="success",
                                    staging_table=staging_table_id,
                                    production_table=production_table_id,
                                    staging_rows=staging_rows,
                                    rows_inserted=rows_inserted,
                                    rows_updated=rows_updated,
                                    rows_unchanged=rows_unchanged,
                                    production_rows_after=production_rows_after,
                                    production_size_mb=production_size_mb,
                                    # Extraction metrics
                                    rows_extracted=table_rows_extracted,
                                    gcs_path=table_gcs_path,
                                    parquet_file_size_mb=table_parquet_size,
                                    extraction_duration_seconds=round(
                                        extraction_duration, 2
                                    ),
                                    extraction_method="api",
                                )
                            except Exception as track_err:
                                logger.warning(
                                    f"Failed to complete table tracking: {track_err}"
                                )
                    else:
                        error_msg = merge_result.get("error", "Unknown error")
                        logger.error(f"Failed to merge {table}: {error_msg}")
                        logger.error(f"[FAILED] {table}: {error_msg}")

                        # Complete table tracking with failure
                        if processing_id:
                            try:
                                complete_table_processing(
                                    processing_id,
                                    status="failed",
                                    error_message=error_msg,
                                    error_phase="merge",
                                )
                            except Exception as track_err:
                                logger.warning(
                                    f"Failed to complete table tracking: {track_err}"
                                )
                        transform_success = False
                        transform_errors.append(
                            {"table": table, "error": error_msg, "stage": "merge"}
                        )

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Transform error for {table}: {error_msg}")
                    logger.error(traceback.format_exc())
                    logger.error(f"[FAILED] {table}: {error_msg[:100]}")
                    transform_success = False

                    # Complete table tracking with exception
                    if processing_id:
                        try:
                            complete_table_processing(
                                processing_id,
                                status="failed",
                                error_message=error_msg[:500],  # Truncate long errors
                                error_phase="transform",
                                error_type=type(e).__name__,
                            )
                        except Exception as track_err:
                            logger.warning(
                                f"Failed to complete table tracking: {track_err}"
                            )

                    # Only add unique errors (don't repeat same error message)
                    if not any(
                        err["table"] == table and err["error"] == error_msg
                        for err in transform_errors
                    ):
                        transform_errors.append(
                            {"table": table, "error": error_msg, "stage": "transform"}
                        )

            # Print summary table of successful vs failed tables
            logger.info("")
            logger.info("=" * 80)
            logger.info("TABLE PROCESSING SUMMARY")
            logger.info("=" * 80)

            # Get all tables that were requested plus any child tables
            requested_tables = tables if tables else []

            # Check for child tables that were automatically extracted
            table_dependencies = config.get("pipeline", {}).get(
                "table_dependencies", {}
            )
            child_tables = []
            for table in requested_tables:
                for child_table, dep_info in table_dependencies.items():
                    if dep_info.get("extracted_via") == table:
                        child_tables.append(child_table)

            # Combine requested and child tables for summary
            all_processed_tables = requested_tables + child_tables

            # Categorize tables
            successful_tables = []
            failed_tables_detail = []
            rate_limited_tables = []
            table_rows_summary = results.get("table_rows", {}) if results else {}

            # Extractors may report an authoritative per-table verdict. When
            # present it is preferred over inferring status from row counts,
            # which mislabels zero-row and rate-limited tables as failures
            # (and previously double-counted a table as both failed and
            # successful). Transform errors still take precedence because they
            # occur after extraction succeeds.
            table_status = results.get("table_status", {}) if results else {}

            for table in all_processed_tables:
                # Check if table was extracted
                # Tables using Parquet/GCS pattern will be in table_files
                # Tables using direct BigQuery load (insights) won't have files but may have succeeded
                was_extracted_parquet = table in results.get("table_files", {})

                # Check extractor's success count - if we extracted data and it equals requested tables,
                # assume success for non-Parquet tables (insights)
                extractor_successful_count = results.get(
                    "tables", 0
                )  # Number of successful tables from extractor
                extractor_failed_count = results.get("failed_tables", 0)

                # For non-Parquet tables, check if extraction reported success
                # If total_rows > 0 and no transform errors, it succeeded
                was_extracted_direct = (
                    total_rows > 0
                    and extractor_failed_count == 0
                    and not was_extracted_parquet
                )  # Only for tables not using Parquet

                was_extracted = was_extracted_parquet or was_extracted_direct

                # Check if table had transform errors
                table_errors = [e for e in transform_errors if e["table"] == table]

                reported_status = table_status.get(table)

                if table_errors:
                    # Failed during transform
                    for err in table_errors:
                        failed_tables_detail.append(
                            {
                                "table": table,
                                "stage": err["stage"],
                                "error": err["error"][:100],  # Truncate long errors
                            }
                        )
                elif reported_status is not None:
                    # Authoritative verdict from the extractor.
                    label = f"{table} (via parent)" if table in child_tables else table
                    if reported_status == "failed":
                        failed_tables_detail.append(
                            {
                                "table": table,
                                "stage": "extraction",
                                "error": "Extraction failed (see job log)",
                            }
                        )
                    elif reported_status == "rate_limited":
                        # Recoverable -- next incremental run backfills it.
                        rate_limited_tables.append({"table": table, "label": label})
                    else:  # success or zero_rows
                        successful_tables.append({"table": table, "label": label})
                elif was_extracted:
                    # Successfully extracted and transformed
                    # Mark child tables differently
                    if table in child_tables:
                        label = f"{table} (via parent)"
                    else:
                        label = table
                    successful_tables.append({"table": table, "label": label})
                else:
                    # Not extracted (skipped or failed early)
                    failed_tables_detail.append(
                        {
                            "table": table,
                            "stage": "extraction",
                            "error": "No data extracted or extraction skipped",
                        }
                    )

            # Print successful tables
            if successful_tables:
                logger.info("")
                logger.info(f"SUCCESSFUL ({len(successful_tables)} tables):")

                # Group auto-discovered survey response tables for cleaner output
                survey_response_tables = [
                    e
                    for e in successful_tables
                    if e["label"].startswith("survey_responses_")
                ]
                other_tables = [
                    e
                    for e in successful_tables
                    if not e["label"].startswith("survey_responses_")
                ]

                # Show non-survey tables (main tables like lime_surveys_columnar)
                for entry in sorted(other_tables, key=lambda x: x["label"]):
                    row_count = table_rows_summary.get(entry["table"])
                    if row_count is not None:
                        logger.info(f"[OK] {entry['label']} - {row_count:,} rows")
                    else:
                        logger.info(f"[OK] {entry['label']}")

                # Summarize auto-discovered survey response tables
                if survey_response_tables:
                    # Get row count from lime_surveys_columnar (the consolidated table) if available
                    columnar_rows = table_rows_summary.get("lime_surveys_columnar", 0)
                    if columnar_rows > 0:
                        logger.info(
                            f"[OK] {len(survey_response_tables)} auto-discovered survey response tables -> lime_surveys_columnar ({columnar_rows:,} rows)"
                        )
                    else:
                        # Fallback to summing individual tables
                        total_survey_rows = sum(
                            table_rows_summary.get(e["table"], 0)
                            for e in survey_response_tables
                        )
                        logger.info(
                            f"[OK] {len(survey_response_tables)} auto-discovered survey response tables - {total_survey_rows:,} total rows"
                        )

            # Print rate-limited tables. These are NOT failures -- the next
            # incremental run backfills the window -- but they are surfaced so
            # the deferral is visible rather than silent.
            if rate_limited_tables:
                logger.info("")
                logger.info(
                    f"RATE LIMITED ({len(rate_limited_tables)} tables - will backfill next run):"
                )
                for entry in sorted(rate_limited_tables, key=lambda x: x["label"]):
                    logger.info(f"[DEFER] {entry['label']}")

            # Print failed tables with details
            if failed_tables_detail:
                logger.info("")
                logger.info(f"FAILED ({len(failed_tables_detail)} tables):")
                for failure in failed_tables_detail:
                    logger.info(f"[FAIL] {failure['table']}")
                    logger.info(f"    Stage: {failure['stage']}")
                    logger.info(f"    Error: {failure['error']}")

            # Print re-run command for failed tables
            if failed_tables_detail:
                failed_table_names = [f["table"] for f in failed_tables_detail]
                logger.info("")
                logger.info("To re-run only failed tables, use:")
                logger.info(
                    f"python orchestrate.py --source {args.source} --env {args.env} \\"
                )
                logger.info(f"  --tables {' '.join(failed_table_names)} \\")
                if args.refresh:
                    logger.info(f"  --refresh {args.refresh} \\")
                if args.start_date:
                    logger.info(
                        f"  --start-date {args.start_date} --end-date {args.end_date} \\"
                    )
                if args.rebuild:
                    logger.info(f"  --rebuild \\")
                logger.info(f"  [other options as needed]")

            has_extraction_failures = bool(failed_tables_detail)

            # Dead-letter the failed tables so they are durably recorded for
            # retry/triage instead of living only in logs, and mark any tables
            # that succeeded this run as resolved in prior DLQ entries. Both are
            # best-effort and never raise into the pipeline.
            try:
                from shared.dead_letter import mark_resolved, record_failures

                if failed_tables_detail:
                    record_failures(
                        bq_client,
                        config,
                        args.source,
                        [
                            {
                                "table": f["table"],
                                "error": f["error"],
                                "stage": f["stage"],
                                # On a --retry-dlq re-failure, advance retry_count
                                # past the prior attempts so max_retries is honored
                                # (a permanently-broken table eventually stops).
                                "retry_count": (
                                    dlq_retry_counts.get(f["table"], 0) + 1
                                    if dlq_retry_counts.get(f["table"]) is not None
                                    else 0
                                ),
                            }
                            for f in failed_tables_detail
                            if f["table"] != "(per-site)"
                        ],
                        execution_id=monitoring_execution_id,
                        job_id=job_id,
                        refresh_mode=(args.refresh or "incremental"),
                    )
                if successful_tables:
                    mark_resolved(
                        bq_client,
                        config,
                        args.source,
                        [e["table"] for e in successful_tables],
                    )
            except Exception as dlq_err:  # noqa: BLE001
                logger.debug("Dead-letter recording skipped (error): %s", dlq_err)

            # Detect per-site failures escalated by plugins (e.g. wp-config missing
            # in wordpress_extractor). If any sites failed for non-transient reasons,
            # the job is partially failed even if every requested table loaded SOME data.
            site_failures = (results or {}).get("site_failures", {})
            fatal_site_failure_msg = (results or {}).get("error_message")
            if fatal_site_failure_msg:
                has_extraction_failures = True
                failed_tables_detail.append(
                    {
                        "table": "(per-site)",
                        "stage": "site_extraction",
                        "error": fatal_site_failure_msg,
                    }
                )

            # Make the anomaly audit LOUD: when it finds ACTIVE anomalies, escalate
            # the job to WARNING (+ alert) instead of completing silently as
            # COMPLETED. Previously active anomalies were only written to BigQuery
            # and emailed, so a missed email left a real data problem invisible
            # (the BI-486 class). The audit result carries active_count.
            if args.source == "anomaly_audit":
                active_anoms = int((results or {}).get("active_count", 0) or 0)
                if active_anoms > 0:
                    has_extraction_failures = True
                    failed_tables_detail.append(
                        {
                            "table": "(anomaly-audit)",
                            "stage": "data_quality",
                            "error": f"{active_anoms} active anomaly finding(s) -- "
                            f"see anomaly_audit.anomaly_results",
                        }
                    )

            logger.info("=" * 80)
            logger.info("")

            if not transform_success or has_extraction_failures:
                logger.error("Some transformations failed")
                # Continue execution but note the failure

        # Complete monitoring (keep existing for backward compatibility)
        execution_end_time = datetime.now()
        duration = (execution_end_time - execution_start_time).total_seconds()

        # Count actually extracted tables (tables that have data files), not requested tables
        if results and results.get("table_rows"):
            actual_tables_extracted = len(results.get("table_rows", {}))
            tables_extracted_list = list(results.get("table_rows", {}).keys())
        else:
            actual_tables_extracted = (
                len(results.get("table_files", {})) if results else 0
            )
            tables_extracted_list = (
                list(results.get("table_files", {}).keys()) if results else []
            )

        stats = {
            "total_rows": results.get("total_rows", 0) if results else 0,
            "sites": len(sites),
            "tables": actual_tables_extracted,  # Use actual extracted count, not requested count
            "tables_extracted": tables_extracted_list,  # List of actual table names extracted
            "table_rows": results.get("table_rows", {}) if results else {},
        }

        # Determine final job status
        if transform_errors or has_extraction_failures or schema_drift_detected:
            final_status = "WARNING"

            warning_reasons = []
            if transform_errors:
                transform_summary = ", ".join(
                    [f"{e['table']} ({e['stage']})" for e in transform_errors[:3]]
                )
                summary = (
                    f"{len(transform_errors)} table(s) failed: {transform_summary}"
                )
                if len(transform_errors) > 3:
                    summary += f" and {len(transform_errors) - 3} more"
                warning_reasons.append(summary)

            if has_extraction_failures:
                extraction_summary = ", ".join(
                    [f"{f['table']} ({f['stage']})" for f in failed_tables_detail[:3]]
                )
                summary = f"{len(failed_tables_detail)} table(s) failed during extraction: {extraction_summary}"
                if len(failed_tables_detail) > 3:
                    summary += f" and {len(failed_tables_detail) - 3} more"
                warning_reasons.append(summary)

            if schema_drift_detected:
                drift_tables = ", ".join(
                    sorted({d["table"] for d in schema_drift_detail})[:5]
                )
                warning_reasons.append(
                    f"schema drift -- new column(s) auto-added to: {drift_tables}"
                )

            error_summary = " ; ".join(warning_reasons)

            # Classify the WARNING for the alert. If any table actually failed to
            # extract or load it's a PARTIAL failure; if the only reason is schema
            # drift (columns auto-absorbed, data loaded fine) it's purely INFO.
            if transform_errors or has_extraction_failures:
                alert_severity = "PARTIAL"
            else:
                alert_severity = "INFO"

            logger.warning(f"Pipeline completed with warnings: {error_summary}")
            logger.warning(f"Total duration: {duration:.2f} seconds")
            stats = attach_bigquery_usage_to_stats(
                stats, job_id, execution_start_time, execution_end_time, config=config
            )
            update_job_status(
                job_id,
                "WARNING",
                error_message=error_summary,
                stats=stats,
                duration_seconds=int(duration),
            )

            # Send warning alert unless --no-alert or --no-alerts specified
            if not (
                getattr(args, "no_alert", False) or getattr(args, "no_alerts", False)
            ):
                # Extract parameters for recovery commands
                group = getattr(args, "group", None)
                if isinstance(group, list) and len(group) > 0:
                    group = group[0]
                table = getattr(args, "table", None)
                if isinstance(table, list) and len(table) > 0:
                    table = table[0]
                lookback = getattr(args, "lookback", None)
                start_date = getattr(args, "start_date", None)
                end_date = getattr(args, "end_date", None)

                send_job_failure_alert(
                    job_id,
                    args.source,
                    error_summary,
                    config,
                    group=group,
                    table=table,
                    lookback=lookback,
                    start_date=start_date,
                    end_date=end_date,
                    severity=alert_severity,
                )
        else:
            # Job completed successfully
            final_status = "COMPLETED"
            # Escalated to WARNING below if a required post-process SQL step fails
            # (the derived/rollup tables are part of the job's output).
            post_process_failed = False

            # Run post-processing SQL before final job row + BQ rollup (post jobs share labels).
            if args.skip_post_process or args.extract_only:
                logger.info("")
                logger.info("=" * 80)
                logger.info("POST-PROCESSING PHASE SKIPPED")
                logger.info("=" * 80)
            else:
                logger.info("")
                logger.info("=" * 80)
                logger.info("POST-PROCESSING PHASE STARTING")
                logger.info("=" * 80)

                try:
                    from shared.post_processor import (
                        run_post_processing,
                        evaluate_post_process_outcome,
                    )

                    post_results = run_post_processing(
                        bq_client=bq_client,
                        config=config,
                        source=args.source,
                        execution_id=job_id,
                        level="pipeline",
                    )

                    if post_results["status"] == "completed":
                        if post_results["total"] > 0:
                            logger.info(
                                f"[OK] Post-processing: {post_results['success']}/{post_results['total']} succeeded"
                            )
                            if post_results["failed"] > 0:
                                logger.warning(
                                    f"[WARN] Post-processing: {post_results['failed']} failed"
                                )
                        else:
                            logger.info("No post-processing SQL files executed")
                    elif post_results["status"] == "skipped":
                        logger.debug("No post-processing configured")

                    # Post-process SQL builds the derived/rollup tables. A failure
                    # means the job OUTPUT is broken even though extract+merge
                    # succeeded -- escalate to WARNING below (BI-486 principle).
                    # The escalation rule lives in a pure, unit-tested helper.
                    _pp_outcome = evaluate_post_process_outcome(post_results)
                    if _pp_outcome["failed"]:
                        post_process_failed = True
                        failed_tables_detail.extend(_pp_outcome["failed_details"])

                except Exception as post_err:
                    logger.error(f"Post-processing failed: {post_err}")
                    # A post-process EXCEPTION (not just a per-step failure) also
                    # means the job output is incomplete -- escalate, do not report
                    # COMPLETED. Same helper, exception shape.
                    _pp_outcome = evaluate_post_process_outcome(
                        {"status": "exception", "error": str(post_err)}
                    )
                    post_process_failed = True
                    failed_tables_detail.extend(_pp_outcome["failed_details"])

                logger.info("=" * 80)
                logger.info("")

            execution_end_time = datetime.now()
            duration = (execution_end_time - execution_start_time).total_seconds()
            stats = attach_bigquery_usage_to_stats(
                stats, job_id, execution_start_time, execution_end_time, config=config
            )

            # Post-processing builds the derived/rollup tables AFTER the main
            # extract/merge succeeded. If it failed, the job OUTPUT is broken, so
            # report WARNING (+ email alert) rather than a misleading COMPLETED.
            if post_process_failed:
                pp_detail = ", ".join(
                    f["table"]
                    for f in failed_tables_detail
                    if f.get("stage") == "post_process"
                )
                pp_summary = (
                    f"Post-processing failed after a successful load: {pp_detail}"
                )
                logger.warning(pp_summary)
                final_status = "WARNING"
                update_job_status(
                    job_id,
                    "WARNING",
                    error_message=pp_summary,
                    stats=stats,
                    duration_seconds=int(duration),
                )
                if not (
                    getattr(args, "no_alert", False)
                    or getattr(args, "no_alerts", False)
                ):
                    send_job_failure_alert(
                        job_id, args.source, pp_summary, config, severity="WARNING"
                    )
            else:
                update_job_status(
                    job_id, "COMPLETED", stats=stats, duration_seconds=int(duration)
                )

            if args.rebuild:
                try:
                    cleared = clear_incremental_state(args.source, tables, bq_client)
                    logger.info(
                        f"Cleared {cleared} incremental state rows for {args.source}"
                    )
                except Exception as state_err:
                    logger.warning(
                        f"Could not clear incremental state for {args.source}: {state_err}"
                    )

            # Send success alert only when post-processing also succeeded.
            if not post_process_failed:
                send_job_success_alert(job_id, args.source, stats, config)
                logger.info(
                    f"Pipeline completed successfully in {duration:.2f} seconds"
                )
            else:
                logger.warning(
                    f"Pipeline finished with post-process WARNING in {duration:.2f} seconds"
                )

        # Complete existing monitoring
        complete_execution(bq_client, monitoring_execution_id, stats, config)

        # Clean up old recovery files (keep files < 24h for active recovery, delete files > 7 days)
        try:
            cleanup_stats = cleanup_recovery_files(max_age_days=7.0, min_age_hours=24.0)
            if cleanup_stats["deleted"] > 0:
                logger.info(
                    f"Cleaned up {cleanup_stats['deleted']} old recovery file(s), freed {cleanup_stats['space_freed_mb']:.2f} MB"
                )
        except Exception as cleanup_err:
            logger.debug(f"Recovery file cleanup failed (non-fatal): {cleanup_err}")

        # Execute chained commands based on job completion status
        pass_job_id = getattr(args, "pass_job_id", False)
        pass_exec_id = getattr(args, "pass_execution_id", False)

        # Always execute --on-finish (regardless of success/warning/failure)
        if getattr(args, "on_finish", None):
            execute_chained_command(
                args.on_finish,
                job_id=job_id,
                execution_id=monitoring_execution_id,
                status=final_status,
                pass_job_id=pass_job_id,
                pass_execution_id=pass_exec_id,
            )

        # Execute --on-success only if job completed successfully (not WARNING)
        if final_status == "COMPLETED" and getattr(args, "on_success", None):
            execute_chained_command(
                args.on_success,
                job_id=job_id,
                execution_id=monitoring_execution_id,
                status=final_status,
                pass_job_id=pass_job_id,
                pass_execution_id=pass_exec_id,
            )

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        logger.error(traceback.format_exc())

        # Update centralized job status to failed
        execution_end_time_fail = datetime.now()
        duration_fail = (execution_end_time_fail - execution_start_time).total_seconds()
        fail_stats = attach_bigquery_usage_to_stats(
            {}, job_id, execution_start_time, execution_end_time_fail, config=config
        )
        update_job_status(
            job_id,
            "FAILED",
            error_message=str(e),
            stats=fail_stats,
            duration_seconds=int(duration_fail),
        )

        # Send failure alert with context for recovery commands
        group = getattr(args, "group", None)
        if isinstance(group, list) and len(group) > 0:
            group = group[0]
        table = getattr(args, "table", None)
        if isinstance(table, list) and len(table) > 0:
            table = table[0]
        lookback = getattr(args, "lookback", None)
        start_date = getattr(args, "start_date", None)
        end_date = getattr(args, "end_date", None)

        send_job_failure_alert(
            job_id,
            args.source,
            str(e),
            config,
            group=group,
            table=table,
            lookback=lookback,
            start_date=start_date,
            end_date=end_date,
        )

        # Fail monitoring (keep existing for backward compatibility)
        execution_end_time = datetime.now()
        duration = (execution_end_time - execution_start_time).total_seconds()

        fail_execution(bq_client, monitoring_execution_id, str(e), config)

        # Send failure notification
        if config.get("notifications", {}).get("on_failure"):
            send_notification(
                "failure",
                f"Pipeline failed: {e}",
                config,
                additional_recipients=additional_emails,
            )

        # Execute chained commands for failure
        pass_job_id = getattr(args, "pass_job_id", False)
        pass_exec_id = getattr(args, "pass_execution_id", False)

        # Always execute --on-finish (regardless of success/warning/failure)
        if getattr(args, "on_finish", None):
            execute_chained_command(
                args.on_finish,
                job_id=job_id,
                execution_id=monitoring_execution_id,
                status="FAILED",
                pass_job_id=pass_job_id,
                pass_execution_id=pass_exec_id,
            )

        # Execute --on-failure only on failure
        if getattr(args, "on_failure", None):
            execute_chained_command(
                args.on_failure,
                job_id=job_id,
                execution_id=monitoring_execution_id,
                status="FAILED",
                pass_job_id=pass_job_id,
                pass_execution_id=pass_exec_id,
            )

        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        # Only suppress traceback for user input validation errors
        error_msg = str(e)
        # Check if this is a validation error (contains specific patterns)
        is_validation_error = any(
            pattern in error_msg
            for pattern in [
                "Invalid group(s)",
                "Invalid table(s)",
                "Invalid Facebook ID format",
                "does not support groups",
            ]
        )

        if is_validation_error:
            # User input validation error - clean error message without traceback
            logger.error(error_msg)
        else:
            # Unexpected ValueError - show full traceback for debugging
            logger.error(f"Unexpected error: {error_msg}")
            traceback.print_exc()
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(130)
