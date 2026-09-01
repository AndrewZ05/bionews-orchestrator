#!/usr/bin/env python3
# GCS integration for data pipeline

import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import uuid
import glob
import sys
from shared.log_config import should_log_detail
from shared.bigquery_client import get_gcs_client

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_gcs_bucket_name(config: Dict[str, Any]) -> str:
    """
    Get GCS bucket name from configuration, with environment-aware suffix.

    Returns environment-specific bucket names:
    - prod: orchestrator-data-lake (or configured bucket)
    - dev: orchestrator-data-lake-dev
    - test: orchestrator-data-lake-test
    """
    base_bucket = config.get('pipeline', {}).get('gcs_bucket', os.environ.get('GCS_BUCKET', 'orchestrator-data-lake'))
    environment = config.get('environment', 'production')

    # Map environment to suffix
    # prod/production uses base bucket name without suffix
    if environment in ['dev', 'test']:
        return f"{base_bucket}-{environment}"

    return base_bucket

def verify_gcs_object_access(bucket_name: str) -> bool:
    """
    Verify that we have access to create objects in the bucket.
    Uses a test object write/read/delete approach instead of bucket metadata access.
    """
    try:
        from google.cloud import storage
        from google.cloud.exceptions import NotFound
        
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        
        # Create a test object
        test_blob_name = f"access_test_{uuid.uuid4()}.txt"
        blob = bucket.blob(test_blob_name)
        
        from shared.error_handler import retry_on_transient_error, TransientError

        @retry_on_transient_error(max_attempts=3, base_delay=1.0)
        def _do_access_test():
            # Try to upload a small test file
            try:
                blob.upload_from_string("GCS access test", content_type="text/plain")
                if should_log_detail(logger, 'debug'):
                    logger.debug(f"Successfully wrote test object to bucket {bucket_name}")

                # Try to read the test file
                content = blob.download_as_text()
                if content == "GCS access test":
                    if should_log_detail(logger, 'debug'):
                        logger.debug(f"Successfully read test object from bucket {bucket_name}")
                else:
                    logger.error(f"CRITICAL ERROR: Test object content mismatch in bucket {bucket_name}")
                    return False

                # Clean up the test file
                blob.delete()
                if should_log_detail(logger, 'debug'):
                    logger.debug(f"Successfully deleted test object from bucket {bucket_name}")

                return True
            except Exception as e:
                error_str = str(e).lower()
                # Retry on transient network errors
                if any(keyword in error_str for keyword in ['timeout', 'connection', 'network', 'service unavailable', '503']):
                    raise TransientError(f"Transient GCS error: {e}")
                # Don't retry on permission or not found errors
                raise

        try:
            return _do_access_test()
        except NotFound:
            logger.error(f"CRITICAL ERROR: Bucket {bucket_name} does not exist")
            logger.error("Please create the bucket manually before running this script.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"CRITICAL ERROR: Cannot access objects in bucket {bucket_name}: {e}")
            logger.error("Please check permissions for the service account.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"CRITICAL ERROR: Error verifying object access: {e}")
        sys.exit(1)

def get_raw_gcs_path(config: Dict[str, Any], source: str, resource: str, execution_id: str) -> str:
    # Get GCS path for raw data
    bucket = config.get('pipeline', {}).get('gcs_bucket')
    if not bucket:
        bucket = os.environ.get('GCS_BUCKET')
        if not bucket:
            bucket = "orchestrator-data-lake"  # Default bucket name
            logger.warning(f"GCS bucket not specified in config or environment, using default: {bucket}")
    
    # Get timestamp for partitioning
    now = datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    timestamp = now.strftime('%Y%m%dT%H%M%S')
    
    # Build path
    path = f"{source}/raw/{resource}/{year}/{month}/{day}/{execution_id}_{timestamp}.parquet"
    
    return f"gs://{bucket}/{path}"

def get_staging_gcs_path(config: Dict[str, Any], source: str, resource: str) -> str:
    # Get GCS path for staging data
    bucket = config.get('pipeline', {}).get('gcs_bucket')
    if not bucket:
        bucket = os.environ.get('GCS_BUCKET')
        if not bucket:
            bucket = "orchestrator-data-lake"  # Default bucket name
            logger.warning(f"GCS bucket not specified in config or environment, using default: {bucket}")
    
    # Build path
    path = f"{source}/staging/{resource}/latest/data.parquet"
    
    return f"gs://{bucket}/{path}"

def find_parquet_files(source: str, dataset_name: str, resource: str) -> List[str]:
    """
    Find parquet files for a specific resource in the output directory
    """
    try:
        # Look in the data directory
        base_path = f"./data/{source}/{dataset_name}"
        
        # Look for job directories
        job_dirs = sorted(glob.glob(f"{base_path}/job_*"), reverse=True)
        
        if not job_dirs:
            logger.error(f"No job directories found in {base_path}")
            return []
        
        # Use the latest job directory
        latest_job_dir = job_dirs[0]
        logger.info(f"Using latest job directory: {latest_job_dir}")
        
        # Look for parquet files in the job directory
        parquet_files = []
        
        # Try all possible patterns for parquet files
        patterns = [
            f"{latest_job_dir}/**/*.parquet",  # Any parquet file in the job directory
            f"{latest_job_dir}/**/{resource}/*.parquet",  # Resource-named directory
            f"{latest_job_dir}/**/{source}_{resource}/*.parquet",  # Source-prefixed resource directory
            f"{latest_job_dir}/**/{resource}*.parquet",  # Resource-prefixed filename
            f"{latest_job_dir}/**/{source}_{resource}*.parquet",  # Source and resource prefixed filename
            f"{latest_job_dir}/**/parquet/**/*.parquet",  # Parquet subdirectory
            f"{latest_job_dir}/{dataset_name}/**/*.parquet",  # Dataset subdirectory
        ]
        
        # Try each pattern
        for pattern in patterns:
            files = glob.glob(pattern, recursive=True)
            if files:
                logger.info(f"Found {len(files)} parquet files with pattern {pattern}: {files}")
                parquet_files.extend(files)
        
        # If no parquet files found, try to find any file and log it for debugging
        if not parquet_files:
            all_files = []
            for root, dirs, files in os.walk(latest_job_dir):
                for file in files:
                    all_files.append(os.path.join(root, file))
            
            if all_files:
                logger.info(f"Found {len(all_files)} files in job directory: {all_files}")
            else:
                logger.error(f"No files found in job directory: {latest_job_dir}")
            
            return []
        
        # Return unique files
        return list(set(parquet_files))
    except Exception as e:
        logger.error(f"Error finding parquet files: {e}")
        return []

def upload_file_to_gcs(local_file_path: str, config: Dict[str, Any], source: str,
                       resource: str, execution_id: str, max_retries: int = 3) -> Dict[str, str]:
    """
    Upload a specific local Parquet file to GCS with retry logic.

    Args:
        local_file_path: Path to local Parquet file
        config: Pipeline configuration
        source: Source name (e.g., 'facebook')
        resource: Resource/table name (e.g., 'campaigns')
        execution_id: Unique execution identifier
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        Dict with raw_gcs_path and staging_gcs_path
    """
    import time
    from google.api_core import exceptions as gcp_exceptions

    if not os.path.exists(local_file_path):
        logger.error(f"Local file not found: {local_file_path}")
        return {"raw_gcs_path": None, "staging_gcs_path": None}

    # Get GCS paths
    raw_gcs_path = get_raw_gcs_path(config, source, resource, execution_id)
    staging_gcs_path = get_staging_gcs_path(config, source, resource)

    # Fix #14: Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            # Get GCS client (new client each retry in case of connection issues)
            client = get_gcs_client()

            # Get bucket name
            bucket_name = raw_gcs_path.split('/')[2]  # gs://{bucket}/...
            bucket = client.bucket(bucket_name)

            # Upload to raw path (with timestamp for archival)
            raw_blob = bucket.blob('/'.join(raw_gcs_path.split('/')[3:]))  # Remove gs://{bucket}/ prefix
            raw_blob.upload_from_filename(local_file_path)
            logger.info(f"Uploaded to raw: {raw_gcs_path}")

            # Server-side copy to staging path (no network re-upload)
            staging_blob = bucket.blob('/'.join(staging_gcs_path.split('/')[3:]))  # Remove gs://{bucket}/ prefix
            rewrite_token = None
            while True:
                rewrite_token, bytes_rewritten, total_bytes = staging_blob.rewrite(raw_blob, token=rewrite_token)
                if rewrite_token is None:
                    break
            logger.info(f"Copied to staging (server-side): {staging_gcs_path}")

            return {
                "raw_gcs_path": raw_gcs_path,
                "staging_gcs_path": staging_gcs_path
            }

        except (gcp_exceptions.ServiceUnavailable, gcp_exceptions.TooManyRequests,
                gcp_exceptions.InternalServerError, ConnectionError, TimeoutError) as e:
            # Transient errors - retry with exponential backoff
            wait_time = (2 ** attempt) * 1  # 1s, 2s, 4s
            logger.warning(f"GCS upload attempt {attempt + 1}/{max_retries} failed with transient error: {e}")

            if attempt < max_retries - 1:
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"GCS upload failed after {max_retries} attempts")
                import traceback
                logger.error(traceback.format_exc())
                return {"raw_gcs_path": None, "staging_gcs_path": None}

        except Exception as e:
            # Non-transient errors - fail immediately
            logger.error(f"GCS upload failed with non-retryable error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"raw_gcs_path": None, "staging_gcs_path": None}

    # Should never reach here, but just in case
    return {"raw_gcs_path": None, "staging_gcs_path": None}


def upload_to_gcs(config: Dict[str, Any], source: str, resource: str,
                execution_id: str, dataset_name: str) -> Dict[str, str]:
    """
    Upload local parquet files to GCS (legacy directory search).
    For new pipelines, use upload_file_to_gcs() instead.
    """
    try:
        # Find parquet files
        parquet_files = find_parquet_files(source, dataset_name, resource)

        if not parquet_files:
            logger.error(f"No parquet files found for {source}/{resource}")
            return {
                "raw_gcs_path": None,
                "staging_gcs_path": None
            }

        # Use the latest parquet file
        parquet_file = parquet_files[-1]

        # Delegate to upload_file_to_gcs
        return upload_file_to_gcs(parquet_file, config, source, resource, execution_id)

    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        return {
            "raw_gcs_path": None,
            "staging_gcs_path": None
        }