#!/usr/bin/env python3
"""
ID-Based Incremental Strategy
==============================

For tables without date fields, implements incremental extraction using
ID-based lookback tied to extracted_at timestamps.

Strategy:
1. Look back N days in the production table using extracted_at
2. Find the minimum ID from that time window
3. Extract only records with ID > min_id

This provides incremental behavior for tables like:
- postmeta (no date fields)
- usermeta (no date fields)
- options (no date fields)
- term_relationships (no date fields)
etc.

Note: Requires pipeline to have been running for at least lookback_days
      before incremental mode becomes effective.

Author: Data Pipeline Team
Created: 2025-10-03
"""

from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timedelta, date
from google.cloud import bigquery
from shared.bigquery_client import get_bigquery_client
import logging

logger = logging.getLogger(__name__)


def get_min_id_from_lookback(
    bq_client: bigquery.Client,
    dataset: str,
    table_name: str,
    id_field: str,
    lookback_days: int,
    site_filter: Optional[str] = None
) -> Optional[int]:
    """
    Get the minimum ID from records extracted within lookback window.
    
    Queries the production table to find the earliest ID that was
    extracted within the lookback period based on extracted_at timestamp.
    
    Args:
        bq_client: BigQuery client instance
        dataset: Production dataset name (e.g., 'WORDPRESS_DATA')
        table_name: Table name (e.g., 'wordpress_postmeta')
        id_field: Name of the ID field (e.g., 'meta_id', 'option_id')
        lookback_days: Number of days to look back
        site_filter: Optional site filter for multi-tenant tables (e.g., 'example.com')
    
    Returns:
        Minimum ID from lookback window, or None if no data or table doesn't exist
        
    Example:
        >>> min_id = get_min_id_from_lookback(client, 'WORDPRESS_DATA', 
        ...                                   'wordpress_postmeta', 'meta_id', 7)
        >>> logger.info(f"Incremental extraction starting from ID: {min_id}")
    """
    try:
        # Calculate lookback date
        lookback_date = datetime.now() - timedelta(days=lookback_days)
        
        # Build query to find minimum ID in lookback window
        query = f"""
        SELECT MIN({id_field}) as min_id
        FROM `{bq_client.project}.{dataset}.{table_name}`
        WHERE extracted_at >= TIMESTAMP('{lookback_date.isoformat()}')
        """
        
        # Add site filter for multi-tenant tables
        if site_filter:
            query += f" AND site_url = '{site_filter}'"
        
        logger.debug(f"ID lookback query: {query}")
        
        # Execute query
        query_job = bq_client.query(query)
        results = list(query_job.result())
        
        if not results or results[0].min_id is None:
            logger.info(f"No data in lookback window for {table_name} "
                       f"(looking back {lookback_days} days)")
            logger.info(f"   This is expected if pipeline hasn't been running for {lookback_days} days")
            logger.info(f"   Will perform FULL extraction for this run")
            return None
        
        min_id = results[0].min_id
        logger.info(f"ID-based incremental: {table_name} starting from ID {min_id} "
                   f"({lookback_days} day lookback)")
        return min_id
        
    except Exception as e:
        if "Not found: Table" in str(e):
            logger.info(f"Table {dataset}.{table_name} doesn't exist yet - will create on first run")
            return None
        else:
            logger.warning(f"Error getting min ID for {table_name}: {e}")
            logger.info(f"   Falling back to FULL extraction")
            return None


def get_id_field_for_table(table_name: str, table_config: Dict[str, Any]) -> Optional[str]:
    """
    Determine the ID field name for a table.
    
    Extracts the ID field from table configuration, typically from primary_key.
    Handles both single and composite keys, returning the ID field.
    
    Args:
        table_name: Name of the table
        table_config: Table configuration from YAML
    
    Returns:
        Name of the ID field, or None if no suitable field found
        
    Example:
        >>> config = {'primary_key': ['site_url', 'meta_id']}
        >>> id_field = get_id_field_for_table('postmeta', config)
        >>> print(id_field)
        'meta_id'
    """
    try:
        primary_keys = table_config.get('primary_key', [])
        
        if not primary_keys:
            logger.debug(f"No primary key defined for {table_name}")
            return None
        
        # Filter out common multi-tenant keys (tenant identifiers should never be used for incremental tracking)
        non_site_keys = [k for k in primary_keys if k not in ['site_url', 'site', 'site_name', 'site_id', 'account_id']]
        
        if not non_site_keys:
            logger.debug(f"No ID field found in primary keys for {table_name}")
            return None
        
        # Prefer fields ending in '_id' or 'ID'
        id_candidates = [k for k in non_site_keys if k.endswith('_id') or k.endswith('ID') or k == 'id']
        
        if id_candidates:
            return id_candidates[0]
        
        # Otherwise return first non-site key
        return non_site_keys[0]
        
    except Exception as e:
        logger.debug(f"Error determining ID field for {table_name}: {e}")
        return None


def should_use_id_strategy(
    table_config: Dict[str, Any],
    refresh_mode: str
) -> bool:
    """
    Determine if ID-based incremental strategy should be used.
    
    Criteria:
    1. Refresh mode is 'incremental'
    2. Table has no date-based incremental strategy
    3. Table has an identifiable ID field

    Args:
        table_config: Table configuration from YAML
        refresh_mode: Current refresh mode ('full', 'incremental', etc.)

    Returns:
        True if ID-based strategy should be used, False otherwise

    Example:
        >>> config = {'incremental': {'strategy': 'none'}, 'primary_key': ['site_url', 'meta_id']}
        >>> should_use_id_strategy(config, 'incremental')
        True
    """
    # Only use for incremental mode
    if refresh_mode != 'incremental':
        return False
    
    # Check if table already has date-based incremental field
    inc = table_config.get('incremental', {})
    if inc.get('strategy') == 'date' and inc.get('fields'):
        return False
    
    # Check if table has an ID field we can use
    id_field = get_id_field_for_table('', table_config)
    if not id_field:
        return False
    
    return True


def build_id_filter_clause(
    min_id: Optional[int],
    id_field: str
) -> str:
    """
    Build SQL WHERE clause for ID-based filtering.
    
    Args:
        min_id: Minimum ID to filter from (exclusive)
        id_field: Name of the ID field
    
    Returns:
        SQL WHERE clause string, or empty string if no filtering needed
        
    Example:
        >>> clause = build_id_filter_clause(12345, 'meta_id')
        >>> print(clause)
        'WHERE meta_id > 12345'
    """
    if min_id is None:
        return ""
    
    return f"WHERE {id_field} > {min_id}"


def get_max_date_from_lookback(
    bq_client: bigquery.Client,
    dataset: str,
    table_name: str,
    date_field: str,
    lookback_days: int,
    site_filter: Optional[str] = None
) -> Optional[date]:
    """
    Get the maximum date from records extracted within lookback window.

    Queries the production table to find the latest date value that was
    extracted within the lookback period based on extracted_at timestamp.
    This provides a safe starting point for date-based incremental extraction.

    Args:
        bq_client: BigQuery client instance
        dataset: Production dataset name (e.g., 'facebook_DATA', 'WORDPRESS_DATA')
        table_name: Table name (e.g., 'facebook_campaigns', 'wordpress_posts')
        date_field: Name of the date field (e.g., 'updated_time', 'post_modified_gmt')
        lookback_days: Number of days to look back
        site_filter: Optional site filter for multi-tenant tables (e.g., 'example.com')

    Returns:
        Maximum date from lookback window, or None if no data or table doesn't exist

    Example:
        >>> max_date = get_max_date_from_lookback(client, 'facebook_DATA',
        ...                                        'facebook_campaigns', 'updated_time', 7)
        >>> logger.info(f"Incremental extraction starting from: {max_date}")
    """
    try:
        # Calculate lookback date
        lookback_date = datetime.now() - timedelta(days=lookback_days)

        # Build query to find maximum date in lookback window
        query = f"""
        SELECT MAX({date_field}) as max_date
        FROM `{bq_client.project}.{dataset}.{table_name}`
        WHERE extracted_at >= TIMESTAMP('{lookback_date.isoformat()}')
        """

        # Add site filter for multi-tenant tables
        if site_filter:
            query += f" AND site_url = '{site_filter}'"

        logger.debug(f"Date lookback query: {query}")

        # Execute query
        query_job = bq_client.query(query)
        results = list(query_job.result())

        if not results or results[0].max_date is None:
            logger.info(f"No data in lookback window for {table_name} "
                       f"(looking back {lookback_days} days)")
            logger.info(f"   This is expected if pipeline hasn't been running for {lookback_days} days")
            logger.info(f"   Will perform FULL extraction for this run")
            return None

        max_date = results[0].max_date

        # Convert to date if it's a datetime
        if isinstance(max_date, datetime):
            max_date = max_date.date()

        logger.info(f"Date-based incremental: {table_name} starting from {max_date} "
                   f"({lookback_days} day lookback)")
        return max_date

    except Exception as e:
        if "Not found: Table" in str(e):
            logger.info(f"Table {dataset}.{table_name} doesn't exist yet - will create on first run")
            return None
        else:
            logger.warning(f"Error getting max date for {table_name}: {e}")
            logger.info(f"   Falling back to FULL extraction")
            return None


def log_id_strategy_info(
    table_name: str,
    id_field: str,
    min_id: Optional[int],
    lookback_days: int
):
    """
    Log information about ID-based incremental strategy.

    Provides clear logging about what strategy is being used and why.

    Args:
        table_name: Name of the table
        id_field: ID field being used
        min_id: Minimum ID found (or None for full extraction)
        lookback_days: Lookback period
    """
    if min_id is None:
        logger.info(f"{table_name}: Using FULL extraction")
        logger.info(f"   Reason: No data in {lookback_days}-day lookback window or table is new")
        logger.info(f"   ID field: {id_field}")
    else:
        logger.info(f"{table_name}: Using ID-based incremental extraction")
        logger.info(f"   Strategy: Extract {id_field} > {min_id}")
        logger.info(f"   Basis: {lookback_days}-day lookback on extracted_at")
        logger.info(f"   Expected: Only records added since last {lookback_days} days")


def log_date_strategy_info(
    table_name: str,
    date_field: str,
    start_date: Optional[date],
    lookback_days: int
):
    """
    Log information about date-based incremental strategy.

    Args:
        table_name: Name of the table
        date_field: Date field being used
        start_date: Starting date found (or None for full extraction)
        lookback_days: Lookback period
    """
    if start_date is None:
        logger.info(f"{table_name}: Using FULL extraction")
        logger.info(f"   Reason: No data in {lookback_days}-day lookback window or table is new")
        logger.info(f"   Date field: {date_field}")
    else:
        logger.info(f"{table_name}: Using date-based incremental extraction")
        logger.info(f"   Strategy: Extract {date_field} >= {start_date}")
        logger.info(f"   Basis: {lookback_days}-day lookback on extracted_at")
        logger.info(f"   Expected: Records modified/created since {start_date}")


# Example usage patterns
if __name__ == "__main__":
    """
    Example usage of ID-based incremental strategy.
    """
    import os

    # Example 1: WordPress postmeta (no date fields)
    print("Example 1: WordPress postmeta extraction")
    print("-" * 60)

    client = get_bigquery_client()
    
    # Configuration from wordpress.yaml
    table_config = {
        'primary_key': ['site_url', 'meta_id'],
        'incremental': {'strategy': 'none'},
        'fields': ['meta_id', 'post_id', 'meta_key', 'meta_value']
    }
    
    # Check if we should use ID strategy
    if should_use_id_strategy(table_config, 'incremental'):
        print("ID-based strategy applicable")
        
        # Get ID field
        id_field = get_id_field_for_table('postmeta', table_config)
        print(f"   ID field: {id_field}")
        
        # Get minimum ID from 7-day lookback
        min_id = get_min_id_from_lookback(
            client,
            'WORDPRESS_DATA',
            'wordpress_postmeta',
            id_field,
            lookback_days=7,
            site_filter='example.com'
        )
        
        # Build filter clause
        filter_clause = build_id_filter_clause(min_id, id_field)
        print(f"   SQL filter: {filter_clause}")
        
        # Log strategy
        log_id_strategy_info('wordpress_postmeta', id_field, min_id, 7)
    else:
        print("ID-based strategy not applicable")
    
    print()
    
    # Example 2: WordPress posts (HAS date fields - should NOT use ID strategy)
    print("Example 2: WordPress posts extraction")
    print("-" * 60)
    
    table_config_with_date = {
        'primary_key': ['site_url', 'ID'],
        'incremental': {
            'strategy': 'date',
            'fields': ['post_modified_gmt']
        },
        'fields': ['ID', 'post_title', 'post_content', 'post_modified_gmt']
    }

    if should_use_id_strategy(table_config_with_date, 'incremental'):
        print("ID-based strategy applicable")
    else:
        print("ID-based strategy not applicable")
        print("   Reason: Table has incremental date strategy")
        print("   Strategy: Will use date-based filtering instead")

