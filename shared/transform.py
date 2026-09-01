#!/usr/bin/env python3
"""
Data Transformation Pipeline
YAML-driven transformation from staging to production tables with hash-based merging.
"""

import os
import sys
import uuid
import logging
import argparse
import dotenv
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Load environment variables
dotenv.load_dotenv()

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))

# Import shared modules
from shared.config_loader import load_config
from shared.bigquery_utils import simple_staging_to_production
from shared.monitoring import get_bigquery_client
from shared.monitoring import complete_execution, fail_execution
from shared.notifications import initialize_notifications, send_notification

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_transformation_config(source: str) -> Dict[str, Any]:
    """
    Get transformation configuration from YAML
    """
    config = load_config(source)
    return config.get("transformation", {})


def get_datasets(config: Dict[str, Any], dataset_type: str) -> tuple:
    """
    Get staging and production dataset names
    """
    pipeline_config = config.get("pipeline", {})

    if dataset_type == "staging":
        staging_dataset = pipeline_config.get(
            "staging_dataset", f"{config.get('source', {}).get('name', 'data')}_staging"
        )
        production_dataset = pipeline_config.get(
            "production_dataset", f"{config.get('source', {}).get('name', 'data')}_data"
        )
    else:
        staging_dataset = pipeline_config.get(
            "staging_dataset", f"{config.get('source', {}).get('name', 'data')}_staging"
        )
        production_dataset = pipeline_config.get(
            "production_dataset", f"{config.get('source', {}).get('name', 'data')}_data"
        )

    return staging_dataset, production_dataset


def get_hash_merge_keys(source: str, table: str) -> List[str]:
    """
    Get hash merge keys from YAML configuration
    """
    config = load_config(source)

    # Get from hash_merge config section
    hash_keys = config.get("hash_merge", {}).get("keys", {}).get(table)
    if hash_keys:
        return hash_keys if isinstance(hash_keys, list) else [hash_keys]

    # Fallback to table config. Merge keys live on resources.<table>.primary_key
    # (the former hard-coded facebook defaults were folded into facebook.yaml).
    # Accept either the logical resource key or the actual table_name
    # (e.g. 'posts' or 'facebook_posts').
    resources = config.get("resources", {})
    table_config = resources.get(table)
    if table_config is None:
        table_config = next(
            (
                rc
                for rc in resources.values()
                if isinstance(rc, dict) and rc.get("table_name") == table
            ),
            {},
        )
    primary_keys = table_config.get("primary_key", ["id"])

    # Handle WordPress-specific defaults (site_id + natural_id for multitenancy)
    if source == "wordpress":
        # For WordPress, the primary_keys from YAML config should already be correct
        # They were updated to use [site_id, natural_id] pattern
        # Just return what's in the YAML config
        return primary_keys if primary_keys else ["site_id", "id"]

    # Default primary key for other sources
    return primary_keys if primary_keys else ["id"]


def execute_custom_sql(client, sql_file: str, source: str, table: str) -> bool:
    """
    Execute custom SQL from file
    """
    try:
        with open(sql_file, "r") as f:
            sql_content = f.read()

        # Replace placeholders with actual values
        sql_content = sql_content.replace("{source}", source)
        sql_content = sql_content.replace("{table}", table)
        sql_content = sql_content.replace("{project}", client.project)

        logger.info(f"Executing custom SQL from {sql_file}")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"SQL: {sql_content}")

        job = client.query(sql_content)
        job.result()

        logger.info(f"Custom SQL executed successfully")
        return True

    except Exception as e:
        logger.error(f"Error executing custom SQL: {e}")
        return False


def execute_default_transform(client, source: str, table: str, datasets: tuple) -> bool:
    """
    Execute default transform: simple staging to production
    """
    try:
        staging_dataset, production_dataset = datasets

        # Get hash merge keys
        primary_keys = get_hash_merge_keys(source, table)

        logger.info(
            f"Running default transform: {staging_dataset}.{table} -> {production_dataset}.{table}"
        )

        # Use the simple wrapper from bigquery_utils
        stats = simple_staging_to_production(
            client=client,
            source=source,
            table=table,
            staging_dataset=staging_dataset,
            production_dataset=production_dataset,
            primary_keys=primary_keys,
        )

        logger.info(f"Transform complete: {stats}")
        return True

    except Exception as e:
        logger.error(f"Error in default transform: {e}")
        return False


def transform_table(
    client,
    source: str,
    table: str,
    datasets: tuple,
    config: Dict[str, Any],
    sql_file: str = None,
) -> bool:
    """
    Transform a single table from staging to production
    """
    try:
        # If custom SQL file provided, execute it
        if sql_file:
            return execute_custom_sql(client, sql_file, source, table)

        # Otherwise, use default transform (hash merge)
        return execute_default_transform(client, source, table, datasets)

    except Exception as e:
        logger.error(f"Error transforming {table}: {e}")
        return False


def initialize_features(config: Dict[str, Any]) -> None:
    """
    Initialize monitoring and notifications
    """
    initialize_notifications(config)
    logger.info("Initialized transformation features")


def main():
    """
    Main transformation pipeline
    """
    parser = argparse.ArgumentParser(description="Data Transformation Pipeline")
    parser.add_argument(
        "--source", required=True, help="Source type (facebook, wordpress, etc.)"
    )

    # Table selection
    parser.add_argument("--tables", nargs="+", help="Tables to transform")
    parser.add_argument("--table", help="Single table to transform")
    parser.add_argument(
        "--sql-file",
        help="Custom SQL file to execute (if not provided, uses simple SELECT * FROM staging)",
    )

    # Dataset configuration
    parser.add_argument(
        "--dataset-type",
        choices=["staging", "production", "test"],
        default="staging",
        help="Dataset type",
    )

    # Processing options
    parser.add_argument(
        "--all-tables", action="store_true", help="Transform all available tables"
    )
    parser.add_argument(
        "--skip-validation", action="store_true", help="Skip validation"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done"
    )

    # Pipeline sequencing
    parser.add_argument("--execution-id", help="Execution ID from parent pipeline")
    parser.add_argument("--parent-status", help="Status from parent pipeline")

    # Next pipeline
    parser.add_argument("--on-success", help="Command to run on success")
    parser.add_argument("--on-failure", help="Command to run on failure")

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.source)

    # Initialize features
    initialize_features(config)

    # Get tables to transform
    tables = []
    if args.table:
        tables = [args.table]
    elif args.tables:
        tables = args.tables
    elif args.all_tables:
        # Get all tables from YAML resources
        tables = list(config.get("resources", {}).keys())
    else:
        # Default: common tables for source
        if args.source == "facebook":
            tables = ["campaigns", "adsets", "ads", "adcreatives"]
        elif args.source == "wordpress":
            tables = ["posts", "pages", "users"]
        else:
            tables = ["*"]  # Transform all

    logger.info(f"NODE: Data Transformation Pipeline")
    logger.info(f"SOURCE: {args.source}")
    logger.info(f"TABLES: {', '.join(tables)}")
    logger.info(f"DATASET TYPE: {args.dataset_type}")

    # Get datasets
    staging_dataset, production_dataset = get_datasets(config, args.dataset_type)

    # Initialize BigQuery client
    client = get_bigquery_client()

    # Create production dataset if it doesn't exist
    try:
        client.get_dataset(f"{client.project}.{production_dataset}")
    except Exception as e:
        logger.debug(f"Dataset {production_dataset} not found, creating: {e}")
        from google.cloud.bigquery import Dataset

        dataset_obj = Dataset(f"{client.project}.{production_dataset}")
        dataset_obj.location = "US"
        client.create_dataset(dataset_obj)
        logger.info(f"Created production dataset: {production_dataset}")

    # Generate execution ID
    execution_id = args.execution_id or str(uuid.uuid4())

    try:
        successful_transformations = 0
        failed_transformations = 0

        for table in tables:
            logger.info(f"\nProcessing table: {table}")

            success = transform_table(
                client=client,
                source=args.source,
                table=table,
                datasets=(staging_dataset, production_dataset),
                config=config,
                sql_file=args.sql_file,
            )

            if success:
                successful_transformations += 1
                logger.info(f"Successfully transformed {table}")
            else:
                failed_transformations += 1
                logger.warning(f"Failed to transform {table}")

        # Summary
        total_transformations = successful_transformations + failed_transformations
        logger.info("\n" + "=" * 50)
        logger.info(f"NODE COMPLETE: Data Transformation")
        logger.info("=" * 50)
        logger.info(f"Tables processed: {total_transformations}")
        logger.info(f"Successful: {successful_transformations}")
        logger.info(f"Failed: {failed_transformations}")
        logger.info(f"Source: {args.source}")
        logger.info(f"Flow: {staging_dataset} -> {production_dataset}")
        logger.info("=" * 50)

        # Send success notification
        if config.get("notifications", {}).get("on_success"):
            send_notification(
                "success",
                f"Transform pipeline completed: {successful_transformations}/{total_transformations} successful",
                config,
            )

        # Run next pipeline if configured
        if args.on_success:
            logger.info(f"Executing next pipeline: {args.on_success}")
            import subprocess

            env = os.environ.copy()
            result = subprocess.run(args.on_success, shell=True, env=env)

            if result.returncode != 0:
                logger.error(f"Next pipeline failed with code {result.returncode}")

        return 0

    except Exception as e:
        logger.error(f"Transform pipeline failed: {e}")

        # Send failure notification
        if config.get("notifications", {}).get("on_failure"):
            send_notification("failure", f"Transform pipeline failed: {e}", config)

        # Run error pipeline if configured
        if args.on_failure:
            logger.info(f"Executing error pipeline: {args.on_failure}")
            import subprocess

            env = os.environ.copy()
            subprocess.run(args.on_failure, shell=True, env=env)

        return 1


if __name__ == "__main__":
    sys.exit(main())
