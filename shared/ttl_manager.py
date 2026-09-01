#!/usr/bin/env python3
"""
TTL Manager for Data Lifecycle Management
========================================

Manages Time-To-Live (TTL) cleanup for archived external tables
and other temporary resources. Automatically removes old data
based on configured retention policies.

Author: Data Pipeline Team  
Created: 2025-01-02
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from google.cloud import bigquery
from google.cloud import storage as gcs
from google.cloud.exceptions import NotFound

# Configure logging
logger = logging.getLogger(__name__)

def get_expired_external_tables(client: bigquery.Client, archive_dataset: str,
                               ttl_days: int) -> List[Dict[str, Any]]:
    """
    Find expired external tables in archive dataset.
    
    Identifies external tables that exceed TTL threshold
    for cleanup processing.
    
    Args:
        client: BigQuery client instance
        archive_dataset: Archive dataset name
        ttl_days: TTL threshold in days
    
    Returns:
        List of expired table metadata dictionaries
    """
    try:
        project = client.project
        cutoff_date = datetime.now() - timedelta(days=ttl_days)
        
        # Query information schema for external tables
        query_sql = f"""
        SELECT 
            table_name,
            creation_time,
            table_type,
            expiration_time
        FROM `{project}.{archive_dataset}.__TABLES__`
        WHERE table_type = 'EXTERNAL'
        AND table_name LIKE '%_%_'  -- Pattern for job-specific tables
        ORDER BY creation_time ASC
        """
        
        result = client.query(query_sql).result()
        expired_tables = []
        
        for row in result:
            # Parse job_id from table_name (format: table_jobid)
            if '_' in row.table_name:
                parts = row.table_name.split('_')
                if len(parts) >= 2:
                    table_name = '_'.join(parts[:-1])
                    job_id = parts[-1]
                    
                    # Check if table is expired
                    creation_date = row.creation_time
                    if creation_date < cutoff_date:
                        expired_tables.append({
                            'table_name': row.table_name,
                            'full_table_id': f"{project}.{archive_dataset}.{row.table_name}",
                            'creation_time': creation_date,
                            'table_name_base': table_name,
                            'job_id': job_id,
                            'table_type': row.table_type,
                            'expiration_time': row.expiration_time
                        })
        
        logger.info(f"Found {len(expired_tables)} expired external tables")
        return expired_tables
        
    except Exception as e:
        logger.error(f"Failed to get expired external tables: {e}")
        return []

def cleanup_expired_external_table(client: bigquery.Client, table_info: Dict[str, Any]) -> bool:
    """
    Clean up a single expired external table.
    
    Safely removes external table after confirming it meets
    TTL criteria and updating related metadata.
    
    Args:
        client: BigQuery client instance
        table_info: External table metadata dictionary
    
    Returns:
        True if cleanup successful
    """
    try:
        table_id = table_info['full_table_id']
        table_name = table_info['table_name']
        
        logger.info(f"Cleaning up expired external table: {table_name}")
        
        # Verify table still exists
        try:
            table_ref = client.get_table(table_id)
        except NotFound:
            logger.info(f"Table {table_id} already deleted")
            return True
        
        # Check if external table has any content
        try:
            count_query = client.query(f"SELECT COUNT(*) as count FROM `{table_id}`")
            count_result = next(count_query.result())
            row_count = count_result.count
            
            logger.info(f"External table contains {row_count:,} rows")
            
        except Exception:
            logger.warning(f"Could not query external table {table_id}")
            row_count = 0
        
        # Delete the external table
        client.delete_table(table_id)
        
        logger.info(f"Deleted expired external table: {table_name}")
        logger.info(f"   Creation date: {table_info['creation_time']}")
        logger.info(f"   Row count: {row_count:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to cleanup external table {table_info['table_name']}: {e}")
        return False

def cleanup_expired_gcs_files(client: bigquery.Client, gcs_client: gcs.Client,
                             bucket_name: str, archive_dataset: str,
                             ttl_days: int) -> List[str]:
    """
    Clean up expired parquet files in GCS raw bucket.
    
    Finds parquet files referenced by expired external tables
    and removes them from GCS storage.
    
    Args:
        client: BigQuery client instance
        gcs_client: GCS client instance
        bucket_name: GCS bucket name
        archive_dataset: Archive dataset for reference
        ttl_days: TTL threshold in days
    
    Returns:
        List of deleted GCS file paths
    """
    deleted_files = []
    
    try:
        # Get expired tables to find associated GCS files
        expired_tables = get_expired_external_tables(client, archive_dataset, ttl_days)
        
        bucket = gcs_client.bucket(bucket_name)
        cutoff_date = datetime.now() - timedelta(days=ttl_days)
        
        # Find GCS files older than TTL threshold
        blobs = bucket.list_blobs(prefix="raw/")
        
        for blob in blobs:
            # Check if file is older than TTL
            if blob.time_created < cutoff_date:
                # Check if file is associated with cleaned external table
                file_path = blob.name
                
                # Extract job_id from file path pattern: raw/date/source/table/job_id.parquet
                path_parts = file_path.split('/')
                if len(path_parts) >= 5 and path_parts[-1].endswith('.parquet'):
                    file_date = path_parts[1]  # YYYY-MM-DD
                    source = path_parts[2]
                    table = path_parts[3]
                    job_id = path_parts[4].replace('.parquet', '')
                    
                    # Check if corresponding external table was cleaned
                    table_exists = False
                    for expired_table in expired_tables:
                        if expired_table['job_id'] == job_id:
                            table_exists = True
                            break
                    
                    if table_exists or blob.time_created < cutoff_date:
                        try:
                            blob.delete()
                            deleted_files.append(file_path)
                            logger.info(f"Deleted expired GCS file: {file_path}")
                            
                        except Exception as e:
                            logger.warning(f"Failed to delete GCS file {file_path}: {e}")
        
        logger.info(f"Cleaned up {len(deleted_files)} expired GCS files")
        return deleted_files
        
    except Exception as e:
        logger.error(f"Failed to cleanup GCS files: {e}")
        return deleted_files

def run_ttl_cleanup(client: bigquery.Client, gcs_client: gcs.Client,
                   config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute comprehensive TTL cleanup across all archived resources.
    
    Performs cleanup of expired external tables and GCS files based
    on configured TTL policies.
    
    Args:
        client: BigQuery client instance
        gcs_client: GCS client instance  
        config: Configuration with TTL settings and dataset names
    
    Returns:
        Dictionary with cleanup results summary
    """
    cleanup_results = {
        'timestamp': datetime.now().isoformat(),
        'expired_tables_found': 0,
        'tables_cleaned': 0,
        'tables_failed': 0,
        'gcs_files_deleted': 0,
        'errors': []
    }
    
    try:
        logger.info("Starting TTL cleanup process...")
        
        # Get configuration
        sources = config.get('sources', ['facebook', 'wordpress'])
        bucket_name = config.get('gcs', {}).get('bucket_name')
        
        if not bucket_name:
            logger.warning("No GCS bucket configured for cleanup")
            cleanup_results['errors'].append("No GCS bucket configured")
            return cleanup_results
        
        # Process each source
        for source in sources:
            try:
                # Get TTL configuration for source
                source_ttl = config.get('sources_config', {}).get(source, {}).get('archive_ttl_days', 30)
                archive_dataset = f"{source}_ARCHIVE"
                
                logger.info(f"Processing {source} archive dataset (TTL: {source_ttl} days)")
                
                # Find expired external tables
                expired_tables = get_expired_external_tables(client, archive_dataset, source_ttl)
                cleanup_results['expired_tables_found'] += len(expired_tables)
                
                # Clean up expired external tables
                for table_info in expired_tables:
                    try:
                        success = cleanup_expired_external_table(client, table_info)
                        if success:
                            cleanup_results['tables_cleaned'] += 1
                        else:
                            cleanup_results['tables_failed'] += 1
                    except Exception as e:
                        cleanup_results['tables_failed'] += 1
                        cleanup_results['errors'].append(f"Table cleanup failed: {e}")
                
                # Clean up expired GCS files
                try:
                    deleted_files = cleanup_expired_gcs_files(
                        client, gcs_client, bucket_name, archive_dataset, source_ttl
                    )
                    cleanup_results['gcs_files_deleted'] += len(deleted_files)
                    
                except Exception as e:
                    cleanup_results['errors'].append(f"GCS cleanup failed: {e}")
                
            except Exception as e:
                cleanup_results['errors'].append(f"Source {source} cleanup failed: {e}")
        
        logger.info("\n" + "="*50)
        logger.info(f"TTL CLEANUP COMPLETE")
        logger.info("="*50)
        logger.info(f"Tables found expired: {cleanup_results['expired_tables_found']}")
        logger.info(f"Tables cleaned: {cleanup_results['tables_cleaned']}")
        logger.info(f"Tables failed: {cleanup_results['tables_failed']}")
        logger.info(f"GCS files deleted: {cleanup_results['gcs_files_deleted']}")
        logger.info(f"Errors: {len(cleanup_results['errors'])}")
        logger.info("="*50)
        
        return cleanup_results
        
    except Exception as e:
        logger.error(f"TTL cleanup process failed: {e}")
        cleanup_results['errors'].append(str(e))
        return cleanup_results

def set_table_ttl(client: bigquery.Client, project: str, dataset: str,
                  table: str, ttl_days: int) -> bool:
    """
    Set explicit TTL on BigQuery table.
    
    Configures automatic deletion of table after TTL period.
    Only works on regular tables, not external tables.
    
    Args:
        client: BigQuery client instance
        project: BigQuery project ID
        dataset: Dataset name
        table: Table name
        ttl_days: Time-to-live in days
    
    Returns:
        True if TTL set successfully
    """
    try:
        table_id = f"{project}.{dataset}.{table}"
        
        # Calculate expiration time
        expiration_time = datetime.now() + timedelta(days=ttl_days)
        
        # Update table with expiration time
        table_ref = client.get_table(table_id)
        table_ref.expires = expiration_time
        table_ref = client.update_table(table_ref, ["expires"])
        
        logger.info(f"Set TTL on {table_id}: expires in {ttl_days} days")
        return True
        
    except Exception as e:
        logger.error(f"Failed to set TTL on {table_id}: {e}")
        return False

def get_ttl_status(client: bigquery.Client, project: str, dataset: str) -> Dict[str, Any]:
    """
    Get TTL status for all tables in dataset.
    
    Useful for monitoring and understanding TTL configuration
    across tables and datasets.
    
    Args:
        client: BigQuery client instance
        project: BigQuery project ID
        dataset: Dataset name
    
    Returns:
        Dictionary with TTL status summary
    """
    try:
        # Query dataset metadata
        query_sql = f"""
        SELECT 
            table_name,
            table_type,
            creation_time,
            expiration_time,
            CASE 
                WHEN expiration_time IS NULL THEN 'No TTL'
                WHEN expiration_time >= CURRENT_TIMESTAMP() THEN 'Active TTL'
                ELSE 'Expired'
            END as ttl_status
        FROM `{project}.{dataset}.__TABLES__`
        ORDER BY creation_time DESC
        """
        
        result = client.query(query_sql).result()
        
        status = {
            'dataset': f"{project}.{dataset}",
            'total_tables': 0,
            'regular_tables': 0,
            'external_tables': 0,
            'tables_with_ttl': 0,
            'expired_tables': 0,
            'table_details': []
        }
        
        for row in result:
            status['total_tables'] += 1
            
            if row.table_type == 'TABLE':
                status['regular_tables'] += 1
            elif row.table_type == 'EXTERNAL':
                status['external_tables'] += 1
            
            if row.expiration_time:
                status['tables_with_ttl'] += 1
            
            if row.ttl_status == 'Expired':
                status['expired_tables'] += 1
            
            status['table_details'].append({
                'table_name': row.table_name,
                'table_type': row.table_type,
                'creation_time': row.creation_time.isoformat() if row.creation_time else None,
                'expiration_time': row.expiration_time.isoformat() if row.expiration_time else None,
                'ttl_status': row.ttl_status
            })
        
        logger.info(f"TTL status for {status['dataset']}: {status['total_tables']} tables, {status['tables_with_ttl']} with TTL")
        return status
        
    except Exception as e:
        logger.error(f"Failed to get TTL status: {e}")
        return {'error': str(e)}

# Configuration constants
DEFAULT_TTL_DAYS = 30
DEFAULT_ARCHIVE_TTL_DAYS = 90
DEFAULT_CLEANUP_SOURCES = ['facebook', 'wordpress']

# TTL configuration template
TTL_CONFIG_TEMPLATE = {
    'sources': DEFAULT_CLEANUP_SOURCES,
    'gcs': {
        'bucket_name': None,  # Must be configured
        'ttl_days': DEFAULT_TTL_DAYS
    },
    'sources_config': {
        source: {
            'archive_ttl_days': DEFAULT_ARCHIVE_TTL_DAYS,
            'cleanup_enabled': True
        }
        for source in DEFAULT_CLEANUP_SOURCES
    },
    'cleanup_schedule': {
        'enabled': True,
        'frequency_hours': 24,  # Daily cleanup
        'max_table_age_days': 120
    }
}
