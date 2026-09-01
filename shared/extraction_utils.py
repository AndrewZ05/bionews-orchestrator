#!/usr/bin/env python3
"""
Extraction Pipeline Utilities
============================

Common extraction pipeline functions shared across all extractors.
Handles dataset management, BigQuery operations, state tracking,
and pipeline orchestration logic.

Author: Data Pipeline Team
Created: 2025-01-02
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logger = logging.getLogger(__name__)

# Import dataset management from bigquery_utils (canonical location)
from shared.bigquery_utils import ensure_dataset_exists, drop_staging_tables


def truncate_staging_table(client: bigquery.Client, table_id: str) -> bool:
    """
    DEPRECATED: Use drop_staging_tables() for external table workflows.

    Safely truncate staging table to prepare for new data load.

    NOTE: This function does NOT work with external tables (cannot TRUNCATE).
    For external table workflows, use drop_staging_tables() instead.

    Args:
        client: BigQuery client instance
        table_id: Full table identifier (project.dataset.table)

    Returns:
        True if truncation succeeded or table didn't exist

    Example:
        >>> truncate_staging_table(client, 'project.facebook_staging.campaigns')
        True
    """
    try:
        # Attempt to truncate the table
        query_job = client.query(f"TRUNCATE TABLE `{table_id}`")
        query_job.result()  # Wait for completion
        logger.info(f"Truncated staging table: {table_id}")
        return True

    except NotFound:
        # Table doesn't exist yet - this is fine for staging
        logger.info(f"Staging table doesn't exist yet: {table_id} (will be created)")
        return True

    except Exception as e:
        logger.error(f"Failed to truncate staging table '{table_id}': {e}")
        return False


def get_table_row_count(client: bigquery.Client, table_id: str) -> int:
    """
    Get the current row count of a BigQuery table.
    
    Performs an efficient COUNT query on the table. Returns 0 if
    table doesn't exist or query fails.
    
    Args:
        client: BigQuery client instance
        table_id: Full table identifier (project.dataset.table)
    
    Returns:
        Number of rows in the table, or 0 if error
        
    Example:
        >>> count = get_table_row_count(client, 'project.facebook_staging.campaigns')
        >>> print(f"Table has {count} rows")
    """
    try:
        query_job = client.query(f"SELECT COUNT(*) as row_count FROM `{table_id}`")
        results = query_job.result()
        
        # Extract count from first row
        row_count = next(results).row_count
        logger.debug(f"Table '{table_id}' contains {row_count} rows")
        return row_count
        
    except NotFound:
        logger.debug(f"Table '{table_id}' does not exist - row count = 0")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to get row count for '{table_id}': {e}")
        return 0


def check_table_exists(client: bigquery.Client, table_id: str) -> bool:
    """
    Check if BigQuery table exists without retrieving it.
    
    Lightweight existence check using metadata queries rather
    than loading full table objects.
    
    Args:
        client: BigQuery client instance
        table_id: Full table identifier (project.dataset.table)
    
    Returns:
        True if table exists, False otherwise
        
    Example:
        >>> exists = check_table_exists(client, 'project.facebook_staging.campaigns')
        >>> print(f"Table exists: {exists}")
    """
    try:
        # Try to get table metadata (lighter than full table object)
        client.get_table(table_id)
        return True
        
    except NotFound:
        return False
        
    except Exception as e:
        logger.error(f"Error checking table existence '{table_id}': {e}")
        return False


def determine_date_range(refresh_mode: str, 
                        lookback_days: Optional[int],
                        start_date: Optional[str],
                        end_date: Optional[str],
                        config: Dict[str, Any]) -> Tuple[datetime.date, datetime.date]:
    """
    Determine extraction date range based on refresh mode and parameters.
    
    Handles different refresh strategies:
    - full: Maximum historical data based on configuration
    - incremental: Based on lookback days or last successful run
    - custom: Based on explicit start/end dates
    
    Args:
        refresh_mode: Refresh strategy ('full', 'incremental', etc.)
        lookback_days: Number of days to look back for incremental refresh
        start_date: Explicit start date (YYYY-MM-DD format)
        end_date: Explicit end date (YYYY-MM-DD format)
        config: Source configuration containing refresh defaults
    
    Returns:
        Tuple of (start_date, end_date) objects
        
    Example:
        >>> start, end = determine_date_range('full', None, None, None, config)
        >>> print(f"Date range: {start} to {end}")
    """
    try:
        # Parse end date - default to today
        if end_date:
            if isinstance(end_date, str):
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            else:
                end_dt = end_date
        else:
            end_dt = datetime.now().date()
        
        # Handle explicit start and end dates
        if start_date and end_date:
            if isinstance(start_date, str):
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            else:
                start_dt = start_date
                
            logger.info(f"Using explicit date range: {start_dt} to {end_dt}")
            return start_dt, end_dt
        
        # Handle full refresh mode
        if refresh_mode == 'full':
            # Check if skip_date_filter is enabled (for database sources like WordPress)
            skip_date_filter = config.get('extractor', {}).get('full_refresh', {}).get('skip_date_filter', False)

            if skip_date_filter:
                # For database sources: extract ALL data without date limit
                # Use a very old start date (effectively no limit)
                start_dt = datetime(2000, 1, 1).date()
                logger.info(f"FULL REFRESH MODE: Extracting ALL historical data (no date limit)")
                logger.info(f"Date range: {start_dt} to {end_dt}")
                return start_dt, end_dt

            # For API sources (like Facebook): limit to max_months
            max_months = config.get('extractor', {}).get('full_refresh', {}).get('max_months', 37)

            if max_months is None:
                # No max_months specified and skip_date_filter not enabled
                # Default to 37 months for API sources
                max_months = 37
                logger.warning(f"max_months is null but skip_date_filter is false - defaulting to {max_months} months")

            # Calculate start date based on max_months (assume 30 days per month)
            start_dt = end_dt - timedelta(days=max_months * 30)

            logger.info(f"FULL REFRESH MODE: Extracting {max_months} months of data")
            logger.info(f"Full date range: {start_dt} to {end_dt}")
            return start_dt, end_dt
        
        # Handle incremental refresh with lookback
        if lookback_days:
            days_back = lookback_days
        else:
            days_back = config.get('extractor', {}).get('lookback_days', 30)
        
        start_dt = end_dt - timedelta(days=days_back)
        
        logger.info(f"Incremental refresh: {days_back} days back from {end_dt}")
        logger.info(f"Date range: {start_dt} to {end_dt}")
        return start_dt, end_dt
        
    except Exception as e:
        logger.error(f"Failed to determine date range: {e}")
        # Fallback to basic 30-day lookback
        end_dt = datetime.now().date()
        start_dt = end_dt - timedelta(days=30)
        logger.warning(f"Using fallback date range: {start_dt} to {end_dt}")
        return start_dt, end_dt


def resolve_accounts(sites_param: List[str], 
                     config: Dict[str, Any],
                     get_available_sites_func: Optional[callable] = None) -> List[str]:
    """
    Resolve user-specified sites/accounts to actual account identifiers.
    
    Handles various site specification patterns:
    - Explicit account lists: ['account1', 'account2']
    - Environment variable references: ['env:FACEBOOK_ACCOUNT_ID']
    - Discovery via API: ['all']
    - Empty/None inputs
    
    Args:
        sites_param: User-provided site specification
        config: Source configuration
        get_available_sites_func: Optional function to discover available sites
    
    Returns:
        List of resolved account identifiers
        
    Example:
        >>> accounts = resolve_accounts(['all'], config, discovery_function)
        >>> print(f"Resolved to {len(accounts)} accounts")
    """
    try:
        if not sites_param or sites_param == [''] or sites_param == [None]:
            # No sites specified - check environment variable
            env_var_name = config.get('source', {}).get('default_account_env_var')
            if env_var_name:
                env_value = os.getenv(env_var_name)
                if env_value:
                    # Split comma-separated values
                    accounts = [aid.strip() for aid in env_value.split(',')]
                    logger.info(f"Using accounts from environment variable '{env_var_name}': {accounts}")
                    return accounts
            
            logger.error(f"No sites specified and no environment variable found")
            return []
        
        if sites_param == ['all']:
            # Discovery mode - use provided function to find available sites
            if get_available_sites_func:
                try:
                    discovered_sites = get_available_sites_func(config)
                    if discovered_sites:
                        logger.info(f"Discovered {len(discovered_sites)} sites via API")
                        return discovered_sites
                    else:
                        logger.error("Site discovery returned no results - cannot proceed")
                        raise Exception("Site discovery failed - no accounts found")
                except Exception as e:
                    logger.error(f"Site discovery failed: {e}")
                    raise Exception(f"Site discovery failed: {e}")
            else:
                logger.warning("Site discovery requested but no discovery function provided")
            
            # Fallback to environment variable
            env_var_name = config.get('source', {}).get('default_account_env_var')
            if env_var_name:
                env_value = os.getenv(env_var_name)
                if env_value:
                    accounts = [aid.strip() for aid in env_value.split(',')]
                    logger.info(f"Using fallback accounts from environment: {accounts}")
                    return accounts
        
        # Explicit site list provided - normalize format
        normalized_sites = []
        for site in sites_param:
            if site and site != '':
                normalized_sites.append(site.strip())
        
        logger.info(f"Using specified sites: {normalized_sites}")
        return normalized_sites
        
    except Exception as e:
        logger.error(f"Failed to resolve sites: {e}")
        return []


def resolve_tables(tables_param: Optional[List[str]],
                  config: Dict[str, Any],
                  group_filter: Optional[str] = None,
                  force_inactive: bool = False) -> List[str]:
    """
    Resolve user-specified tables to actual table identifiers.

    Handles table specification patterns:
    - Explicit table lists: ['campaigns', 'adsets', 'ads']
    - Group-based selection: tables belonging to specified group
    - All tables: when no specific tables requested

    Args:
        tables_param: User-provided table specification
        config: Source configuration containing resource definitions
        group_filter: Optional group name for filtering tables
        force_inactive: If True, allow running tables marked as inactive

    Returns:
        List of resolved table identifiers

    Example:
        >>> tables = resolve_tables(None, config, 'core')
        >>> print(f"Found {len(tables)} tables in 'core' group")
    """
    try:
        resources = config.get('resources', {})

        if not resources:
            logger.error("No resource definitions found in configuration")
            return []

        if tables_param:
            # Explicit table list provided - check active status and warn
            valid_tables = []
            for table in tables_param:
                if table in resources:
                    table_config = resources[table]
                    is_active = table_config.get('active', True)

                    if is_active:
                        valid_tables.append(table)
                        logger.debug(f"Valid table: {table}")
                    elif force_inactive:
                        # Allow inactive table with --force-inactive flag
                        valid_tables.append(table)
                        logger.warning(f"Table '{table}' is marked as INACTIVE but will run due to --force-inactive flag.")
                    else:
                        # Warn user that explicitly requested table is inactive
                        logger.warning(f"Table '{table}' is marked as INACTIVE and will be skipped. "
                                     f"To enable this table, set 'active: true' in the configuration, or use --force-inactive flag.")
                else:
                    logger.warning(f"Unknown table in configuration: {table}")

            logger.info(f"Using {len(valid_tables)} explicitly specified tables")
            return valid_tables
        
        # No specific tables requested - apply filters
        if group_filter:
            # Filter by group and active status
            tables = [
                table_name for table_name, table_config in resources.items()
                if table_config.get('group') == group_filter and table_config.get('active', True) != False
            ]
            logger.info(f"Using tables from '{group_filter}' group: {len(tables)} tables")
        else:
            # Use all tables (filter by active status)
            tables = [
                table_name for table_name in resources.keys()
                if resources[table_name].get('active', True) != False
            ]
            logger.info(f"Using all available tables: {len(tables)} tables")

        return tables
        
    except Exception as e:
        logger.error(f"Failed to resolve tables: {e}")
        return []


def create_pipeline_stats() -> Dict[str, Any]:
    """
    Create standardized pipeline statistics structure.
    
    Initializes statistics tracking for extraction pipelines with
    consistent metrics across all extractors.
    
    Returns:
        Statistics dictionary with initialized counters
        
    Example:
        >>> stats = create_pipeline_stats()
        >>> stats['total_rows']
        0
    """
    return {
        'total_rows': 0,
        'successful_tables': 0,
        'failed_tables': 0,
        'rate_limit_hits': 0,
        'processing_errors': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'duration_seconds': None
    }


def finalize_pipeline_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Complete pipeline statistics with final calculations.
    
    Adds timing information and calculates derived metrics
    for pipeline completion reporting.
    
    Args:
        stats: Pipeline statistics dictionary to finalize
    
    Returns:
        Completed statistics with timing information
        
    Example:
        >>> stats = finalize_pipeline_stats(stats)
        >>> stats['duration_seconds']
        120.5
    """
    try:
        stats['end_time'] = datetime.now()
        
        # Calculate duration
        if stats['start_time']:
            duration = (stats['end_time'] - stats['start_time']).total_seconds()
            stats['duration_seconds'] = duration
            stats['duration_minutes'] = duration / 60
            
        # Calculate success rate
        total_tables = stats['successful_tables'] + stats['failed_tables']
        if total_tables > 0:
            stats['success_rate'] = stats['successful_tables'] / total_tables * 100
        else:
            stats['success_rate'] = 100.0
            
        logger.info(f"Pipeline completed in {stats['duration_minutes']:.1f} minutes")
        logger.info(f"Success rate: {stats['success_rate']:.1f}%")
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to finalize pipeline stats: {e}")
        return stats


def log_pipeline_summary(stats: Dict[str, Any], source_name: str) -> None:
    """
    Log comprehensive pipeline completion summary.
    
    Provides detailed summary of extraction results including row counts,
    table processing, error rates, and performance metrics.
    
    Args:
        stats: Finalized pipeline statistics
        source_name: Source system name for logging context
        
    Example:
        >>> log_pipeline_summary(stats, 'facebook')
        # Logs comprehensive extraction summary
    """
    try:
        logger.info("\n" + "="*60)
        logger.info(f"{source_name.upper()} EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Total rows extracted: {stats.get('total_rows', 0):,}")
        logger.info(f"Tables processed: {stats.get('successful_tables', 0) + stats.get('failed_tables', 0)}")
        logger.info(f"  Successful: {stats.get('successful_tables', 0)}")
        logger.info(f"  Failed: {stats.get('failed_tables', 0)}")
        logger.info(f"Success rate: {stats.get('success_rate', 0):.1f}%")
        logger.info(f"Duration: {stats.get('duration_minutes', 0):.1f} minutes")
        
        # Log rate limiting information
        if stats.get('rate_limit_hits', 0) > 0:
            logger.info(f"Rate limit hits: {stats.get('rate_limit_hits', 0)}")
            logger.info("TIP: Consider running extractions at different times or reducing batch sizes")
        
        # Log performance tips
        if stats.get('duration_minutes', 0) > 10:
            logger.info("TIP: Extraction took longer than expected - consider optimization")
        
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Failed to log pipeline summary: {e}")


# Module-level utility functions for common patterns
def format_table_id(project: str, dataset: str, table: str) -> str:
    """
    Format table identifier in standard BigQuery format.
    
    Args:
        project: BigQuery project ID
        dataset: Dataset name
        table: Table name
    
    Returns:
        Formatted table identifier
    """
    return f"{project}.{dataset}.{table}"


def parse_table_id(table_id: str) -> Tuple[str, str, str]:
    """
    Parse BigQuery table identifier into components.
    
    Args:
        table_id: Full table identifier
    
    Returns:
        Tuple of (project, dataset, table)
    """
    parts = table_id.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    else:
        raise ValueError(f"Invalid table ID format: {table_id}")


# Configuration constants
# Default configuration values for extraction utilities.

DEFAULT_DATASET_LOCATION = "US"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MAX_HISTORICAL_MONTHS = 37

# Environment variable naming conventions
DEFAULT_ENV_VAR_PATTERNS = {
    'facebook': 'FACEBOOK_ACCOUNT_ID',
    'wordpress': 'WPENGINE_SITES',
    'generic': 'EXTRACTION_SITES'
}

# Performance thresholds
PERFORMANCE_WARNING_MINUTES = 10
RATE_LIMIT_WARNING_THRESHOLD = 1