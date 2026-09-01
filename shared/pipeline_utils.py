#!/usr/bin/env python3
"""
Pipeline Utilities - BigQuery Helper Functions
==============================================

Provides utility functions for BigQuery operations including:
- Client creation with credential management
- Dataset and table management
- Schema operations
- Data operations
"""

import logging
from typing import Dict, Any, Optional, List
import os
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest

# Import centralized client management and dataset utilities
from shared.bigquery_client import get_bigquery_client
from shared.bigquery_utils import ensure_dataset_exists

# Configure logging
logger = logging.getLogger(__name__)

def get_table_schema(bq_client: bigquery.Client, table_id: str) -> Optional[List[bigquery.SchemaField]]:
    """
    Get table schema from BigQuery.
    
    Args:
        bq_client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        
    Returns:
        List of schema fields or None if table doesn't exist
    """
    try:
        table = bq_client.get_table(table_id)
        logger.debug(f"Retrieved schema for table: {table_id}")
        return table.schema
    except NotFound:
        logger.debug(f"Table not found: {table_id}")
        return None
    except BadRequest as e:
        logger.error(f"Bad request error getting schema for {table_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting schema for {table_id}: {e}")
        return None

def table_exists(bq_client: bigquery.Client, table_id: str) -> bool:
    """
    Check if table exists in BigQuery.
    
    Args:
        bq_client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        bq_client.get_table(table_id)
        logger.debug(f"Table exists: {table_id}")
        return True
    except NotFound:
        logger.debug(f"Table does not exist: {table_id}")
        return False
    except BadRequest as e:
        logger.error(f"Bad request error checking table {table_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking table {table_id}: {e}")
        return False

def create_table_from_schema(bq_client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField],
                           description: str = None) -> bool:
    """
    Create table from schema definition.
    
    Args:
        bq_client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        schema: List of schema fields
        description: Optional table description
        
    Returns:
        True if successful, False otherwise
    """
    try:
        table = bigquery.Table(table_id, schema=schema)
        if description:
            table.description = description
        
        created_table = bq_client.create_table(table)
        logger.info(f"Created table {table_id} with {len(schema)} fields")
        return True
        
    except BadRequest as e:
        logger.error(f"Bad request error creating table {table_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error creating table {table_id}: {e}")
        return False

def truncate_table(bq_client: bigquery.Client, table_id: str) -> bool:
    """
    Truncate table (remove all rows).
    
    Args:
        bq_client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        
    Returns:
        True if successful, False otherwise
    """
    query = f"TRUNCATE TABLE `{table_id}`"
    try:
        bq_client.query(query).result()
        logger.info(f"Truncated table: {table_id}")
        return True
    except NotFound as e:
        logger.error(f"Table not found for truncation: {table_id} - {e}")
        return False
    except BadRequest as e:
        logger.error(f"Bad request error truncating table {table_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error truncating table {table_id}: {e}")
        return False

def copy_table(bq_client: bigquery.Client, source_table: str, dest_table: str,
               write_disposition: str = 'WRITE_TRUNCATE') -> bool:
    """
    Copy table from source to destination.
    
    Args:
        bq_client: BigQuery client instance
        source_table: Source table ID
        dest_table: Destination table ID
        write_disposition: Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        job_config = bigquery.CopyJobConfig()
        
        if write_disposition == 'WRITE_TRUNCATE':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif write_disposition == 'WRITE_APPEND':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif write_disposition == 'WRITE_EMPTY':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        else:
            logger.error(f"Invalid write disposition: {write_disposition}")
            return False
        
        job = bq_client.copy_table(source_table, dest_table, job_config=job_config)
        job.result()  # Wait for job to complete
        
        logger.info(f"Successfully copied {source_table} to {dest_table}")
        return True
        
    except NotFound as e:
        logger.error(f"Source table not found: {source_table} - {e}")
        return False
    except BadRequest as e:
        logger.error(f"Bad request error copying table {source_table} to {dest_table}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error copying table {source_table} to {dest_table}: {e}")
        return False

def get_row_count(bq_client: bigquery.Client, table_id: str) -> int:
    """
    Get row count for table.
    
    Args:
        bq_client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        
    Returns:
        Number of rows in table, 0 if error or table doesn't exist
    """
    query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
    try:
        result = bq_client.query(query).result()
        for row in result:
            count = row.cnt
            logger.debug(f"Table {table_id} has {count} rows")
            return count
    except NotFound as e:
        logger.debug(f"Table not found for row count: {table_id} - {e}")
        return 0
    except BadRequest as e:
        logger.error(f"Bad request error getting row count for {table_id}: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error getting row count for {table_id}: {e}")
        return 0