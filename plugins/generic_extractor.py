#!/usr/bin/env python3
"""
Generic Extractor - Routes to source-specific extractors
========================================================

This extractor:
1. Routes to source-specific extractors (facebook_extractor, wordpress_extractor, etc.)
2. Provides fallback/placeholder for unsupported sources
3. Supports BOTH incremental strategies for ANY source:
   - Date-based: For tables with timestamp/date fields
   - ID-based: For tables without date fields (uses extracted_at lookback)

Incremental Strategy Selection:
-------------------------------
Date-based (Preferred):
  - If table has incremental.strategy='date' with fields (e.g., updated_time, post_modified_gmt)
  - Filters by: WHERE date_field >= start_date AND date_field <= end_date
  - Examples: posts, users, campaigns, adsets

ID-based (Fallback):
  - If table has NO date fields
  - Looks back N days using extracted_at timestamp
  - Finds minimum ID from that window
  - Filters by: WHERE id > min_id
  - Examples: postmeta, usermeta, options, adcreatives

Author: Data Pipeline Team
Created: 2025-10-03
Updated: 2025-10-03 - Removed DLT dependency
"""

import importlib
import os
from typing import Dict, Any, List, Iterator, Optional, Tuple
from datetime import datetime, timedelta, date
import logging

# Import incremental strategy utilities
from shared.incremental_id_strategy import (
    get_min_id_from_lookback,
    should_use_id_strategy,
    get_id_field_for_table,
    log_id_strategy_info,
)
from shared.extraction_utils import determine_date_range

logger = logging.getLogger(__name__)


def discover_sources(config: Dict[str, Any]) -> List[str]:
    # Dynamic discovery of sites/accounts based on source type
    source_type = config["source"]["type"]

    if source_type == "mysql" and config["source"].get("discovery_api"):
        # WordPress: Call API to get sites
        import requests

        response = requests.get(config["source"]["discovery_api"])
        sites = []
        for site in response.json().get("topics", []):
            if site.get("name", "").endswith("prd"):
                sites.append(site["name"])
        return sites

    elif source_type == "facebook":
        # Facebook: Use API to get accounts
        from facebook_business.adobjects.user import User
        from facebook_business.api import FacebookAdsApi

        # Initialize API
        access_token = config["source"]["connection"].get("access_token") or os.getenv(
            "FACEBOOK_ACCESS_TOKEN"
        )
        FacebookAdsApi.init(None, None, access_token)

        user = User(fbid="me")
        accounts = user.get_ad_accounts(fields=["id"])
        return [acc["id"] for acc in accounts]

    elif source_type == "rest_api" and config["source"].get("discovery_endpoint"):
        # Generic REST: Call discovery endpoint
        import requests

        response = requests.get(config["source"]["discovery_endpoint"])
        return response.json()

    return []


def get_available_sites(config: Dict[str, Any]) -> List[str]:
    # Get available sites/accounts
    # Check if sites are configured
    if "sites" in config["source"]:
        return config["source"]["sites"]
    if "accounts" in config["source"]:
        return config["source"]["accounts"]

    # Otherwise discover
    return discover_sources(config)


def get_available_tables(config: Dict[str, Any], group: str = None) -> List[str]:
    # Get available tables/resources
    if group:
        return [k for k, v in config["resources"].items() if v.get("group") == group]
    return list(config["resources"].keys())


def recover_from_checkpoint(config: Dict[str, Any], checkpoint_id: str) -> None:
    # Recover from a saved checkpoint
    from shared.monitoring import get_last_checkpoint
    from shared.pipeline_utils import get_bigquery_client

    bq_client = get_bigquery_client()
    checkpoint = get_last_checkpoint(bq_client, checkpoint_id, config)

    if checkpoint:
        logger.info(f"Recovering from checkpoint: {checkpoint}")
        # Recovery logic would resume from this point
    else:
        logger.debug(f"No checkpoint found for {checkpoint_id}")


def run_pipeline(
    config: Dict[str, Any],
    sites: List[str],
    tables: List[str],
    group: str,
    refresh_mode: str,
    lookback_days: int,
    start_date: str,
    end_date: str,
    test_mode: bool,
    batch_size: int,
    max_retries: int,
    skip_validation: bool = False,
    export_format: str = None,
    export_dir: str = None,
    skip_hash_merge: bool = False,
    archive_staging: bool = False,
    truncate_staging: bool = False,
    bq_client: Any = None,
    execution_id: str = None,
    schema_prefix: Optional[str] = None,
    schema_suffix: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Main pipeline orchestration - routes to source-specific extractors.

    This is the entry point for the generic extractor. It attempts to route
    to the appropriate source-specific extractor (facebook_extractor.py,
    wordpress_extractor.py, etc.). If no specific extractor is found, it
    returns a message indicating generic extraction is not implemented.

    Args:
        config: Source configuration dictionary
        sites: List of sites/accounts to extract from
        tables: List of tables/resources to extract
        group: Optional group filter
        refresh_mode: 'full' or 'incremental'
        lookback_days: Days to look back for incremental
        start_date: Start date for extraction
        end_date: End date for extraction
        test_mode: Whether to run in test mode
        batch_size: Batch size for processing
        max_retries: Maximum retry attempts
        skip_validation: Skip validation checks
        export_format: Export format (json, csv, etc.)
        export_dir: Export directory path
        skip_hash_merge: Skip hash-based merge
        archive_staging: Archive staging data
        truncate_staging: Truncate staging tables
        bq_client: BigQuery client instance
        execution_id: Unique execution identifier
        **kwargs: Additional parameters

    Returns:
        Dictionary with pipeline statistics
    """

    source_type = config["source"]["type"]
    source_name = config["source"]["name"]

    # Map source types to extractor modules
    type_to_module = {
        "mysql": "wordpress_extractor",
        "facebook": "facebook_extractor",
        "rest_api": "rest_extractor",
        "bigquery": "bigquery_extractor",
        "postgres": "postgres_extractor",
        "mailchimp": "mailchimp_extractor",
        "salesforce": "salesforce_extractor",
        "dcm": "dcm_extractor",
        "limesurvey": "limesurvey_extractor",
        "bio_acceptor": "bio_acceptor_extractor",
        "onetrust": "onetrust_extractor",
        "surveyengine": "postgres_extractor",
    }

    # Check if source_name has a dedicated extractor (overrides type-based routing)
    # This handles cases like limesurvey (type: mysql but has dedicated extractor)
    if source_name in type_to_module:
        module_name = type_to_module[source_name]
    else:
        module_name = type_to_module.get(source_type)

    # Try to load source-specific extractor.
    #
    # The import is deliberately NOT wrapped around the run_pipeline() call: an
    # ImportError raised INSIDE a working extractor (e.g. a missing DB driver
    # reported by its own dependency guard) is a real failure, not a "module not
    # found". Catching it here once caused a missing psycopg2 to be logged as a
    # WARNING and degraded into a silent zero-row run that still reported
    # success. Import first, then call outside the handler.
    if module_name:
        try:
            extractor_module = importlib.import_module(f"plugins.{module_name}")
        except ImportError as e:
            logger.warning(f"Could not load {module_name}: {e}")
            extractor_module = None
        else:
            logger.info(f"Loaded {module_name} for source type '{source_type}'")

        if extractor_module is not None:
            if not hasattr(extractor_module, "run_pipeline"):
                raise RuntimeError(
                    f"Extractor '{module_name}' for source type '{source_type}' has no "
                    "run_pipeline() function"
                )
            return extractor_module.run_pipeline(
                config=config,
                sites=sites,
                tables=tables,
                group=group,
                refresh_mode=refresh_mode,
                lookback_days=lookback_days,
                start_date=start_date,
                end_date=end_date,
                test_mode=test_mode,
                batch_size=batch_size,
                max_retries=max_retries,
                skip_validation=skip_validation,
                export_format=export_format,
                export_dir=export_dir,
                skip_hash_merge=skip_hash_merge,
                archive_staging=archive_staging,
                truncate_staging=truncate_staging,
                bq_client=bq_client,
                execution_id=execution_id,
                schema_prefix=schema_prefix,
                schema_suffix=schema_suffix,
                **kwargs,
            )

    # No usable extractor. This is a hard failure, NOT an empty result: returning
    # total_rows=0 here made orchestrate.py treat a broken source as a quiet day
    # and report "Pipeline completed successfully" having loaded nothing, which
    # on a nightly cron is indistinguishable from healthy and pages nobody.
    raise RuntimeError(
        f"No usable extractor for source type '{source_type}'"
        + (
            f" (plugins/{module_name}.py failed to import -- see the warning above; "
            "a missing dependency is the usual cause)"
            if module_name
            else f" -- create plugins/{source_type}_extractor.py with a run_pipeline() function"
        )
    )


# Generic extraction function template for direct use
def extract(
    config: Dict[str, Any],
    table_config: Dict[str, Any],
    sites: List[str],
    refresh_mode: str = "full",
    lookback_days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bq_client: Any = None,
    **kwargs,
) -> Iterator[Dict[str, Any]]:
    """
    Generic extraction function template with dual incremental strategy support.

    This is a template function showing how to implement extraction with
    both date-based and ID-based incremental strategies. Source-specific
    extractors should implement their own extraction logic.

    Implements:
    1. Date-based incremental: If table has incremental.strategy='date' with fields
    2. ID-based incremental: If table has NO date fields

    Args:
        config: Source configuration
        table_config: Table-specific configuration
        sites: List of sites/accounts to extract from
        refresh_mode: 'full' or 'incremental'
        lookback_days: Days to look back for incremental
        start_date: Explicit start date
        end_date: Explicit end date
        bq_client: BigQuery client for ID-based strategy
        **kwargs: Additional parameters

    Yields:
        Dictionary records from the source

    Example for date-based (posts table):
        >>> table_config = {'incremental': {'strategy': 'date', 'fields': ['post_modified_gmt']}, ...}
        >>> for record in extract(config, table_config, sites, 'incremental', 7):
        ...     # Only posts modified in last 7 days

    Example for ID-based (postmeta table):
        >>> table_config = {'incremental': {'strategy': 'none'}, 'primary_key': ['site_url', 'meta_id'], ...}
        >>> for record in extract(config, table_config, sites, 'incremental', 7, bq_client=client):
        ...     # Only postmeta with meta_id > min_id from 7-day lookback
    """

    source_type = config["source"]["type"]
    table_name = table_config.get("table_name", "unknown")

    logger.info(f"Generic extraction template for {source_type}/{table_name}")

    # Determine date range for date-based incremental
    inc = table_config.get("incremental", {})
    inc_fields = inc.get("fields", [])
    incremental_field = inc_fields[0] if inc_fields else None
    start_dt, end_dt = None, None
    if incremental_field and inc.get("strategy") == "date":
        start_dt, end_dt = determine_date_range(
            refresh_mode, lookback_days, start_date, end_date, config
        )
        logger.info(
            f"Date-based incremental: {incremental_field} "
            f"between {start_dt} and {end_dt}"
        )

    # Process each site
    for site in sites:
        logger.info(f"Processing {site}/{table_name}")

        # Determine ID-based strategy if applicable
        min_id = None
        if not incremental_field and refresh_mode == "incremental":
            # ID-based strategy for tables without date fields
            if bq_client:
                id_field = get_id_field_for_table(table_name, table_config)
                if id_field:
                    # Get production dataset
                    production_dataset = config.get("pipeline", {}).get(
                        "production_dataset", f"{config['source']['name']}_data"
                    )

                    min_id = get_min_id_from_lookback(
                        bq_client,
                        production_dataset,
                        table_name,
                        id_field,
                        lookback_days if lookback_days else 7,
                        site_filter=site,
                    )
                    log_id_strategy_info(
                        table_name,
                        id_field,
                        min_id,
                        lookback_days if lookback_days else 7,
                    )

        # TODO: Implement actual extraction logic based on source_type
        #
        # For date-based incremental:
        #   if start_dt and end_dt:
        #       query += f" WHERE {incremental_field} >= '{start_dt}' AND {incremental_field} <= '{end_dt}'"
        #
        # For ID-based incremental:
        #   if min_id is not None:
        #       query += f" WHERE {id_field} > {min_id}"
        #
        # Example for REST API:
        # if source_type == 'rest_api':
        #     endpoint = table_config.get('endpoint')
        #     params = {}
        #     if start_dt:
        #         params['start_date'] = start_dt.isoformat()
        #     if end_dt:
        #         params['end_date'] = end_dt.isoformat()
        #     if min_id:
        #         params['min_id'] = min_id
        #
        #     response = requests.get(endpoint, params=params)
        #     for record in response.json():
        #         # Filter client-side if API doesn't support filtering
        #         if min_id and record.get('id', 0) <= min_id:
        #             continue
        #         yield record

        logger.warning(f"Generic extraction not fully implemented for {source_type}")
        logger.info(f"Please use source-specific extractor: {source_type}_extractor.py")

        # Placeholder to maintain generator contract (no actual data yielded)
        return
        yield {}  # Unreachable but maintains type signature
