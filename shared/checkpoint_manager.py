#!/usr/bin/env python3
"""
Checkpoint Manager for Pipeline State Management
===============================================

Manages critical checkpoints throughout pipeline execution for
retry logic and failure recovery. Supports graceful degradation
and state restoration from failure points.

Checkpoints are "critical only" - focusing on expensive operations
and external dependencies that cannot be easily repeated.

Author: Data Pipeline Team
Created: 2025-01-02
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configure logging
logger = logging.getLogger(__name__)

# Checkpoint types and their critical operations
CHECKPOINT_TYPES = {
    'EXTRACTION_START': {
        'description': 'Data extraction from source API/database initiated',
        'persistent': False,  # Not saved to DB
        'retry_strategy': 'full_restart'
    },
    'EXTRACTION_COMPLETE': {
        'description': 'Data successfully extracted from source',
        'persistent': True,
        'retry_strategy': 'partial_retry'  # Skip extraction on retry
    },
    'GCS_UPLOAD_COMPLETE': {
        'description': 'Data successfully uploaded to GCS raw bucket',
        'persistent': True,
        'retry_strategy': 'skip_upload'
    },
    'EXTERNAL_TABLE_CREATED': {
        'description': 'BigQuery external table successfully created',
        'persistent': True,
        'retry_strategy': 'reuse_existing'
    },
    'PRODUCTION_LOAD_COMPLETE': {
        'description': 'Data successfully loaded to production tables',
        'persistent': True,
        'retry_strategy': 'upsert_mode'
    },
    'ARCHIVE_COMPLETE': {
        'description': 'External table successfully archived',
        'persistent': True,
        'retry_strategy': 'delete_and_retry'
    },
    'PIPELINE_COMPLETE': {
        'description': 'Full pipeline execution completed successfully',
        'persistent': True,
        'retry_strategy': 'no_retry'
    }
}

def create_checkpoint_table(client: bigquery.Client, dataset: str) -> None:
    """
    Create checkpoint log table for persistent state tracking.

    Tracks critical pipeline checkpoints with metadata for
    retry logic and failure recovery.

    Args:
        client: BigQuery client instance
        dataset: Dataset name for checkpoint table
    """
    table_id = f"{client.project}.{dataset}._pipeline_checkpoints"

    # Check if table exists and has correct schema
    try:
        existing_table = client.get_table(table_id)
        existing_columns = {field.name for field in existing_table.schema}

        # Check if job_id column exists
        if 'job_id' not in existing_columns:
            logger.warning(f"Checkpoint table exists but missing job_id column, recreating...")
            client.delete_table(table_id, not_found_ok=True)
            logger.info(f"Deleted old checkpoint table: {table_id}")
        else:
            logger.debug(f"Checkpoint table exists with correct schema: {table_id}")
            return
    except Exception:
        # Table doesn't exist, will create it
        pass

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        checkpoint_id STRING NOT NULL,
        job_id STRING NOT NULL,
        source STRING NOT NULL,
        table_name STRING NOT NULL,
        checkpoint_type STRING NOT NULL,
        checkpoint_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        checkpoint_data STRING,
        step_number INT64,
        retry_strategy STRING
    )
    PARTITION BY DATE(checkpoint_timestamp)
    CLUSTER BY job_id, checkpoint_type
    """

    try:
        client.query(create_sql).result()
        logger.info(f"Checkpoint table ready: {table_id}")
    except Exception as e:
        logger.error(f"Failed to create checkpoint table: {e}")
        raise

def save_checkpoint(client: bigquery.Client, dataset: str, job_id: str,
                   source: str, table: str, checkpoint_type: str,
                   checkpoint_data: Dict[str, Any] = None) -> str:
    """
    
    """
    
    """
    Save critical checkpoint with job metadata.
    
    Records checkpoint state for retry logic and failure recovery.
    Only saves checkpoints marked as 'persistent': True.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing checkpoint table
        job_id: Unique job identifier
        source: Source system name
        table: Table name being processed
        checkpoint_type: Type of checkpoint (from CHECKPOINT_TYPES)
        checkpoint_data: Optional checkpoint-specific data
    
    Returns:
        Unique checkpoint identifier
    """
    try:
        # Validate checkpoint type
        if checkpoint_type not in CHECKPOINT_TYPES:
            raise ValueError(f"Invalid checkpoint type: {checkpoint_type}")
        
        checkpoint_config = CHECKPOINT_TYPES[checkpoint_type]
        
        # Only save persistent checkpoints
        if not checkpoint_config['persistent']:
            logger.debug(f"Skipping non-persistent checkpoint: {checkpoint_type}")
            return f"{job_id}_{checkpoint_type}_ephemeral"
        
        # Generate checkpoint ID
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        checkpoint_id = f"cp_{job_id}_{checkpoint_type}_{timestamp}"
        
        # Prepare checkpoint data
        checkpoint_data_json = json.dumps(checkpoint_data or {}, default=str)
        retry_strategy = checkpoint_config['retry_strategy']
        
        # Determine step number
        step_number = list(CHECKPOINT_TYPES.keys()).index(checkpoint_type)
        
        # Insert checkpoint record
        insert_sql = f"""
        INSERT INTO `{client.project}.{dataset}._pipeline_checkpoints`
        (checkpoint_id, job_id, source, table_name, checkpoint_type,
         checkpoint_data, step_number, retry_strategy)
        VALUES ('{checkpoint_id}', '{job_id}', '{source}', '{table}',
                '{checkpoint_type}', '{checkpoint_data_json}', {step_number}, '{retry_strategy}')
        """
        
        client.query(insert_sql).result()
        
        logger.info(f"Checkpoint saved: {checkpoint_type}")
        logger.info(f"   ID: {checkpoint_id}")
        logger.info(f"   Strategy: {retry_strategy}")
        
        return checkpoint_id
        
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")
        raise

def get_latest_checkpoint(client: bigquery.Client, dataset: str, job_id: str,
                         checkpoint_type: str = None) -> Optional[Dict[str, Any]]:
    """
    Retrieve latest checkpoint for job with optional type filter.
    
    Gets the most recent checkpoint to determine retry starting point.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing checkpoint table
        job_id: Job identifier to query
        checkpoint_type: Optional specific checkpoint type to retrieve
    
    Returns:
        Checkpoint data dictionary or None if not found
    """
    try:
        where_clause = f"job_id = '{job_id}'"
        if checkpoint_type:
            where_clause += f" AND checkpoint_type = '{checkpoint_type}'"
        
        query_sql = f"""
        SELECT checkpoint_id, job_id, source, table_name, checkpoint_type,
               checkpoint_timestamp, checkpoint_data, step_number, retry_strategy
        FROM `{client.project}.{dataset}._pipeline_checkpoints`
        WHERE {where_clause}
        ORDER BY checkpoint_timestamp DESC
        LIMIT 1
        """
        
        result = client.query(query_sql).result()
        row = next(result, None)
        
        if row:
            checkpoint = {
                'checkpoint_id': row.checkpoint_id,
                'job_id': row.job_id,
                'source': row.source,
                'table_name': row.table_name,
                'checkpoint_type': row.checkpoint_type,
                'checkpoint_timestamp': row.checkpoint_timestamp,
                'checkpoint_data': json.loads(row.checkpoint_data) if row.checkpoint_data else {},
                'step_number': row.step_number,
                'retry_strategy': row.retry_strategy
            }
            
            logger.debug(f"Retrieved checkpoint: {checkpoint_type or 'latest'}")
            return checkpoint
        else:
            logger.info(f"No checkpoint found for job {job_id}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to get checkpoint: {e}")
        return None

def get_job_checkpoint_history(client: bigquery.Client, dataset: str, job_id: str) -> List[Dict[str, Any]]:
    """
    Get complete checkpoint history for a job.
    
    Useful for debugging and understanding pipeline progression.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing checkpoint table
        job_id: Job identifier
    
    Returns:
        List of checkpoint dictionaries in chronological order
    """
    try:
        query_sql = f"""
        SELECT checkpoint_id, job_id, source, table_name, checkpoint_type,
               checkpoint_timestamp, checkpoint_data, step_number, retry_strategy
        FROM `{client.project}.{dataset}._pipeline_checkpoints`
        WHERE job_id = '{job_id}'
        ORDER BY checkpoint_timestamp ASC
        """
        
        result = client.query(query_sql).result()
        checkpoints = []
        
        for row in result:
            checkpoints.append({
                'checkpoint_id': row.checkpoint_id,
                'job_id': row.job_id,
                'source': row.source,
                'table_name': row.table_name,
                'checkpoint_type': row.checkpoint_type,
                'checkpoint_timestamp': row.checkpoint_timestamp,
                'checkpoint_data': json.loads(row.checkpoint_data) if row.checkpoint_data else {},
                'step_number': row.step_number,
                'retry_strategy': row.retry_strategy
            })
        
        logger.info(f"Retrieved {len(checkpoints)} checkpoints for job {job_id}")
        return checkpoints
        
    except Exception as e:
        logger.error(f"Failed to get checkpoint history: {e}")
        return []

def determine_retry_start_point(client: bigquery.Client, dataset: str, job_id: str) -> Tuple[str, Dict[str, Any]]:
    """
    Determine optimal restart point for failed pipeline.
    
    Analyzes checkpoint history to find the last successful checkpoint
    and determines retry strategy.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing checkpoint table
        job_id: Job identifier for failed job
    
    Returns:
        Tuple of (restart_checkpoint_type, checkpoint_data)
    """
    try:
        checkpoints = get_job_checkpoint_history(client, dataset, job_id)
        
        if not checkpoints:
            logger.info("No checkpoints found - starting from beginning")
            return 'EXTRACTION_START', {}
        
        # Find the highest completed checkpoint
        latest_completed = None
        for checkpoint in reversed(checkpoints):
            if checkpoint['checkpoint_type'] in ['EXTRACTION_COMPLETE', 'GCS_UPLOAD_COMPLETE',
                                                'EXTERNAL_TABLE_CREATED', 'PRODUCTION_LOAD_COMPLETE',
                                                'ARCHIVE_COMPLETE', 'PIPELINE_COMPLETE']:
                latest_completed = checkpoint
                break
        
        if latest_completed:
            restart_type = latest_completed['checkpoint_type']
            restart_data = latest_completed['checkpoint_data']
            
            logger.info(f"Retry starting point: {restart_type}")
            logger.info(f"Retry strategy: {latest_completed['retry_strategy']}")
            
            return restart_type, restart_data
        else:
            logger.info("No completed checkpoints found - starting from extraction")
            return 'EXTRACTION_START', {}
            
    except Exception as e:
        logger.error(f"Failed to determine retry start point: {e}")
        return 'EXTRACTION_START', {}

def cleanup_checkpoints(client: bigquery.Client, dataset: str, 
                       days_to_keep: int = 7) -> int:
    """
    Clean up old checkpoint records beyond retention period.
    
    More aggressive cleanup than job logs since checkpoints are
    primarily for active retry scenarios.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing checkpoint table
        days_to_keep: Number of days to retain checkpoints
    
    Returns:
        Number of checkpoint records deleted
    """
    try:
        delete_sql = f"""
        DELETE FROM `{client.project}.{dataset}._pipeline_checkpoints`
        WHERE checkpoint_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_to_keep} DAY)
        """
        
        client.query(delete_sql).result()
        
        logger.info(f"Cleaned up checkpoints older than {days_to_keep} days")
        return 1  # Simplified - BigQuery doesn't return deleted count easily
        
    except Exception as e:
        logger.error(f"Failed to cleanup checkpoints: {e}")
        return 0

def with_checkpoint(client: bigquery.Client, dataset: str, job_id: str,
                  source: str, table: str, checkpoint_type: str,
                  checkpoint_data: Dict[str, Any] = None):
    """
    Decorator for automatic checkpoint management.
    
    Context manager that automatically saves checkpoints for
    critical operations with proper error handling.
    
    Args:
        client: BigQuery client instance
        dataset: Dataset containing checkpoint table
        job_id: Job identifier
        source: Source system name
        table: Table name
        checkpoint_type: Checkpoint type string
        checkpoint_data: Optional checkpoint-specific data
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                logger.info(f"Starting checkpointed operation: {checkpoint_type}")
                
                # Execute function
                result = func(*args, **kwargs)
                
                # Save success checkpoint
                checkpoint_id = save_checkpoint(
                    client, dataset, job_id, source, table,
                    checkpoint_type, checkpoint_data or {}
                )
                
                logger.info(f"Completed checkpointed operation: {checkpoint_type}")
                return result, checkpoint_id
                
            except Exception as e:
                logger.error(f"Checkpointed operation failed: {checkpoint_type} - {e}")
                raise
        
        return wrapper
    return decorator

# Configuration constants
DEFAULT_CHECKPOINT_DATASET = "orchestrator_monitoring"
DEFAULT_CHECKPOINT_RETENTION_DAYS = 7
CHECKPOINT_STEP_MAPPING = {
    0: 'EXTRACTION_START',
    1: 'EXTRACTION_COMPLETE',
    2: 'GCS_UPLOAD_COMPLETE',
    3: 'EXTERNAL_TABLE_CREATED',
    4: 'PRODUCTION_LOAD_COMPLETE',
    5: 'ARCHIVE_COMPLETE',
    6: 'PIPELINE_COMPLETE'
}

def find_resumable_job(client: bigquery.Client, source: str,
                       tables: List[str]) -> Optional[Dict[str, Any]]:
    """
    Find most recent incomplete job for specified source/tables.

    Searches for the most recent job where at least one specified table
    has not completed the full pipeline. Returns per-table progress to
    enable smart resumption.

    Args:
        client: BigQuery client instance
        source: Source name (facebook, wordpress, generic, etc.)
        tables: List of table names to check for incomplete jobs

    Returns:
        Dictionary with job details and per-table progress:
        {
            'job_id': 'job_20250115T120000Z',
            'source': 'facebook',
            'timestamp': datetime,
            'tables': {
                'page_insights': {
                    'checkpoint_type': 'EXTRACTION_COMPLETE',
                    'step_number': 1,
                    'checkpoint_data': {'recovery_file': '...', 'rows_extracted': 26355},
                    'timestamp': datetime
                }
            }
        }
        Returns None if no incomplete jobs found.

    Example:
        >>> resumable = find_resumable_job(client, 'facebook', ['page_insights', 'campaigns'])
        >>> if resumable:
        >>>     print(f"Can resume job {resumable['job_id']}")
        >>>     for table, progress in resumable['tables'].items():
        >>>         print(f"  {table}: {progress['checkpoint_type']}")
    """
    try:
        # Ensure checkpoint table exists
        dataset = DEFAULT_CHECKPOINT_DATASET
        create_checkpoint_table(client, dataset)

        # Build table filter for SQL
        table_list = "', '".join(tables)

        # Query to find most recent incomplete job
        query = f"""
        WITH latest_incomplete_job AS (
            -- Find most recent job with at least one incomplete table
            SELECT DISTINCT
                job_id,
                MAX(checkpoint_timestamp) as last_activity
            FROM `{client.project}.{dataset}._pipeline_checkpoints`
            WHERE source = @source
              AND table_name IN UNNEST(@tables)
              AND checkpoint_type != 'PIPELINE_COMPLETE'
            GROUP BY job_id
            ORDER BY last_activity DESC
            LIMIT 1
        ),
        table_progress AS (
            -- Get latest checkpoint for each table in the incomplete job
            SELECT
                cp.job_id,
                cp.source,
                cp.table_name,
                cp.checkpoint_type,
                cp.checkpoint_data,
                cp.step_number,
                cp.checkpoint_timestamp
            FROM `{client.project}.{dataset}._pipeline_checkpoints` cp
            INNER JOIN latest_incomplete_job lij ON cp.job_id = lij.job_id
            WHERE cp.table_name IN UNNEST(@tables)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cp.table_name
                ORDER BY cp.checkpoint_timestamp DESC
            ) = 1
        )
        SELECT * FROM table_progress
        ORDER BY table_name
        """

        # Execute query with parameters
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ArrayQueryParameter("tables", "STRING", tables)
            ]
        )

        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        if results.total_rows == 0:
            logger.debug(f"No incomplete jobs found for {source} / {tables}")
            return None

        # Build result dictionary
        job_info = {
            'job_id': None,
            'source': source,
            'timestamp': None,
            'tables': {}
        }

        for row in results:
            if not job_info['job_id']:
                job_info['job_id'] = row.job_id
                job_info['timestamp'] = row.checkpoint_timestamp

            # Parse checkpoint data JSON
            checkpoint_data = {}
            if row.checkpoint_data:
                try:
                    checkpoint_data = json.loads(row.checkpoint_data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in checkpoint_data for {row.table_name}")

            job_info['tables'][row.table_name] = {
                'checkpoint_type': row.checkpoint_type,
                'step_number': row.step_number or 0,
                'checkpoint_data': checkpoint_data,
                'timestamp': row.checkpoint_timestamp
            }

        logger.info(f"Found resumable job: {job_info['job_id']}")
        logger.debug(f"  Tables: {list(job_info['tables'].keys())}")

        return job_info

    except Exception as e:
        logger.error(f"Failed to find resumable job: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return None
