#!/usr/bin/env python3
"""
CLI Utilities for Pipeline Management
======================================

Shared utilities for command-line interface tools including client setup,
command decorators, and common constants.

Author: Data Pipeline Team
Created: 2025-10-03
"""

import logging
import sys
from functools import wraps
from typing import Tuple, Callable, Any
from google.cloud import bigquery
from google.cloud import storage as gcs
from shared.bigquery_client import get_bigquery_client as _get_bq_client
from shared.bigquery_client import get_gcs_client as _get_gcs_client

# Configure logging
logger = logging.getLogger(__name__)

# Default configuration constants
DEFAULT_MONITORING_DATASET = 'orchestrator_monitoring'
DEFAULT_JOB_LOG_RETENTION_DAYS = 30
DEFAULT_CHECKPOINT_RETENTION_DAYS = 7
DEFAULT_TTL_DAYS = 30
DEFAULT_HEALTH_CHECK_HOURS = 24
DEFAULT_JOB_LIST_LIMIT = 20

def setup_pipeline_clients() -> Tuple[bigquery.Client, gcs.Client]:
    """
    Initialize BigQuery and GCS clients with proper credential handling.

    Returns:
        Tuple of (BigQuery client, GCS client)
    """
    # Use centralized client management
    bq_client = _get_bq_client()
    gcs_client = _get_gcs_client()
    return bq_client, gcs_client

def get_bigquery_client() -> bigquery.Client:
    """
    Get BigQuery client only.

    Returns:
        BigQuery client
    """
    return _get_bq_client()

def get_gcs_client() -> gcs.Client:
    """
    Get GCS client only.

    Returns:
        GCS client
    """
    return _get_gcs_client()

def pipeline_command(requires_gcs: bool = False) -> Callable:
    """
    Decorator for pipeline management commands.
    
    Provides automatic client setup, error handling, and logging for CLI commands.
    
    Args:
        requires_gcs: If True, provides both BQ and GCS clients. If False, only BQ.
    
    Usage:
        @pipeline_command(requires_gcs=False)
        def cmd_job_status(args, bq_client):
            # Command implementation
            return 0
        
        @pipeline_command(requires_gcs=True)
        def cmd_cleanup(args, bq_client, gcs_client):
            # Command implementation
            return 0
    
    Returns:
        Decorated function that returns 0 on success, 1 on error
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(args) -> int:
            try:
                # Setup clients
                if requires_gcs:
                    bq_client, gcs_client = setup_pipeline_clients()
                    return func(args, bq_client, gcs_client)
                else:
                    bq_client, _ = setup_pipeline_clients()
                    return func(args, bq_client)
                    
            except KeyboardInterrupt:
                logger.info("Command interrupted by user")
                return 130  # Standard exit code for SIGINT
                
            except Exception as e:
                logger.error(f"Command failed: {e}")
                if logger.level == logging.DEBUG:
                    import traceback
                    logger.debug(traceback.format_exc())
                return 1
                
        return wrapper
    return decorator

def validate_monitoring_dataset(dataset_name: str) -> bool:
    """
    Validate monitoring dataset name follows BigQuery naming rules.
    
    Args:
        dataset_name: Dataset name to validate
    
    Returns:
        True if valid, False otherwise
    """
    import re
    
    # BigQuery dataset naming rules
    if not dataset_name:
        return False
    
    if len(dataset_name) > 1024:
        return False
    
    # Must contain only letters, numbers, and underscores
    if not re.match(r'^[a-zA-Z0-9_]+$', dataset_name):
        return False
    
    return True

def get_monitoring_dataset(args: Any) -> str:
    """
    Get monitoring dataset name from args with validation.
    
    Args:
        args: Parsed command line arguments
    
    Returns:
        Monitoring dataset name
    
    Raises:
        ValueError: If dataset name is invalid
    """
    dataset = getattr(args, 'monitoring_dataset', DEFAULT_MONITORING_DATASET)
    
    if not validate_monitoring_dataset(dataset):
        raise ValueError(f"Invalid monitoring dataset name: {dataset}")
    
    return dataset













