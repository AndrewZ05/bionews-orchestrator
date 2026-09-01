#!/usr/bin/env python3
# BigQuery external table management

import logging
from typing import Dict, Any, List, Optional
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import client management and dataset utilities
from shared.bigquery_client import get_bigquery_client
from shared.bigquery_utils import ensure_dataset_exists

def get_schema_from_parquet_gcs(gcs_path: str) -> Optional[List[bigquery.SchemaField]]:
    """
    Extract BigQuery schema from a Parquet file in GCS.

    Args:
        gcs_path: GCS path to parquet file (gs://bucket/path/file.parquet)

    Returns:
        List of BigQuery SchemaField objects, or None if extraction fails
    """
    try:
        from google.cloud import storage

        # Parse GCS path
        if not gcs_path.startswith('gs://'):
            logger.error(f"Invalid GCS path: {gcs_path}")
            return None

        path_parts = gcs_path[5:].split('/', 1)
        bucket_name = path_parts[0]
        blob_name = path_parts[1] if len(path_parts) > 1 else ''

        # Download parquet file to temp location
        import tempfile
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            blob.download_to_filename(tmp_file.name)

            # Read parquet schema
            parquet_file = pq.ParquetFile(tmp_file.name)
            arrow_schema = parquet_file.schema_arrow

            # Convert Arrow schema to BigQuery schema
            bq_fields = []
            field_name_map = {}  # Track original -> sanitized mapping
            for field in arrow_schema:
                original_name = field.name
                sanitized_name = _sanitize_field_name(original_name)
                bq_type = _arrow_type_to_bigquery_type(str(field.type))

                # Handle duplicate field names after sanitization
                if sanitized_name in field_name_map.values():
                    counter = 1
                    base_name = sanitized_name
                    while f"{base_name}_{counter}" in field_name_map.values():
                        counter += 1
                    sanitized_name = f"{base_name}_{counter}"

                field_name_map[original_name] = sanitized_name
                bq_fields.append(bigquery.SchemaField(sanitized_name, bq_type))

                # Log if field name was changed
                if original_name != sanitized_name:
                    logger.debug(f"Sanitized field name: '{original_name}' -> '{sanitized_name}'")

            logger.info(f"Extracted schema from {gcs_path}: {len(bq_fields)} fields")
            return bq_fields

    except Exception as e:
        logger.warning(f"Could not extract schema from Parquet file: {e}")
        return None

def _sanitize_field_name(field_name: str) -> str:
    """
    Sanitize field name to meet BigQuery requirements:
    - Only letters, numbers, and underscores
    - Start with letter or underscore
    - Max 300 characters

    Args:
        field_name: Raw field name from source

    Returns:
        Sanitized field name
    """
    import re

    # Replace spaces with underscores
    sanitized = field_name.replace(' ', '_')

    # Replace dashes with underscores
    sanitized = sanitized.replace('-', '_')

    # Remove any character that's not alphanumeric or underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', sanitized)

    # Ensure it starts with letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized

    # Collapse multiple underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Ensure it starts with letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"

    # Truncate to 300 characters
    if len(sanitized) > 300:
        sanitized = sanitized[:300]

    # Truncate to 300 characters
    if len(sanitized) > 300:
        sanitized = sanitized[:300]

    # If somehow empty, use generic name
    if not sanitized:
        sanitized = 'field'

    return sanitized

def _arrow_type_to_bigquery_type(arrow_type: str) -> str:
    # Convert Arrow/Parquet type to BigQuery type.
    arrow_type_lower = arrow_type.lower()

    # Timestamp types
    if 'timestamp' in arrow_type_lower or 'datetime' in arrow_type_lower:
        return 'TIMESTAMP'
    # Integer types
    elif 'int64' in arrow_type_lower or 'int32' in arrow_type_lower:
        return 'INT64'
    # Float types
    elif 'float' in arrow_type_lower or 'double' in arrow_type_lower:
        return 'FLOAT64'
    # Boolean
    elif 'bool' in arrow_type_lower:
        return 'BOOLEAN'
    # Date
    elif 'date' in arrow_type_lower:
        return 'DATE'
    # Default to STRING
    else:
        return 'STRING'

def cleanup_old_external_table(client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
    """
    Delete an old external table before creating a new one.
    Fix #18: Prevents orphaned external tables from accumulating.

    Args:
        client: BigQuery client
        dataset_id: Dataset containing the external table
        table_id: External table to delete

    Returns:
        True if table was deleted or didn't exist, False on error
    """
    try:
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        client.delete_table(table_ref)
        logger.info(f"  Deleted old external table: {table_ref}")
        return True
    except NotFound:
        # Table doesn't exist - that's fine
        logger.debug(f"No existing external table to cleanup: {table_ref}")
        return True
    except Exception as e:
        logger.warning(f"Could not delete old external table {table_ref}: {e}")
        return False

def create_external_table(client: bigquery.Client, dataset_id: str, table_id: str,
                         gcs_path: str, schema: Optional[List[bigquery.SchemaField]] = None,
                         source_format: str = "PARQUET",
                         cleanup_old: bool = True) -> bool:
    """
    Create or update an external table pointing to GCS.

    Args:
        client: BigQuery client
        dataset_id: Target dataset ID
        table_id: Target table ID
        gcs_path: GCS path (gs://bucket/path/to/file)
        schema: Optional explicit schema (None for auto-detection from Parquet)
        source_format: File format (PARQUET, NEWLINE_DELIMITED_JSON, CSV, etc.)
        cleanup_old: If True, delete old external table before creating (default: True)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Fix #18: Delete old external table first to prevent orphans
        if cleanup_old:
            cleanup_old_external_table(client, dataset_id, table_id)

        # Ensure dataset exists
        ensure_dataset_exists(client, dataset_id)

        # Create external table definition
        table_ref = client.dataset(dataset_id).table(table_id)
        external_config = bigquery.ExternalConfig(source_format)

        # Handle wildcard paths for partitioned files
        if '*' in gcs_path or gcs_path.endswith('/'):
            external_config.source_uris = [gcs_path]
        else:
            external_config.source_uris = [gcs_path]

        # For Parquet files, try to extract schema from the file first
        if source_format == "PARQUET" and not schema:
            logger.info(f"Extracting schema from Parquet file: {gcs_path}")
            schema = get_schema_from_parquet_gcs(gcs_path)
            if schema:
                logger.info(f"Using extracted Parquet schema with {len(schema)} fields")
            else:
                logger.warning("Could not extract schema from Parquet, falling back to autodetect")
                external_config.autodetect = True

        # Create table definition
        table = bigquery.Table(table_ref)
        table.external_data_configuration = external_config

        # If schema is provided or extracted, use it
        if schema:
            if schema:
                table.schema = schema
                external_config.schema = schema

        # Fix #18: Always create (old table already deleted above if cleanup_old=True)
        table = client.create_table(table)
        logger.info(f"  Created external table {dataset_id}.{table_id}")

        # Verify table can be queried
        row_count_query = client.query(f"SELECT COUNT(*) as cnt FROM `{client.project}.{dataset_id}.{table_id}`")
        row_count_result = next(row_count_query.result())
        logger.info(f"External table has {row_count_result.cnt} rows")

        return True
    except Exception as e:
        logger.error(f"Error creating external table: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def create_external_table_from_staging(config: Dict[str, Any], source: str, 
                                      resource: str, staging_gcs_path: str) -> bool:
    # Create an external table from staging data
    try:
        # Get dataset name
        dataset_id = config.get('pipeline', {}).get('staging_dataset', f"{source}_staging")
        
        # Get table name
        table_id = f"{resource}_external"
        
        # Create external table
        client = get_bigquery_client()
        return create_external_table(client, dataset_id, table_id, staging_gcs_path)
    except Exception as e:
        logger.error(f"Error creating external table from staging: {e}")
        return False

def get_table_schema(client: bigquery.Client, dataset_id: str, table_id: str) -> List[bigquery.SchemaField]:
    # Get the schema of a table
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        return table.schema
    except Exception as e:
        logger.error(f"Error getting table schema: {e}")
        return []

def create_or_update_table(client: bigquery.Client, dataset_id: str, table_id: str, 
                          schema: List[bigquery.SchemaField], 
                          partition_field: Optional[str] = None,
                          clustering_fields: Optional[List[str]] = None) -> bool:
    # Create or update a regular BigQuery table
    try:
        # Ensure dataset exists
        ensure_dataset_exists(client, dataset_id)
        
        # Create table reference
        table_ref = client.dataset(dataset_id).table(table_id)
        
        # Create table with schema
        table = bigquery.Table(table_ref, schema=schema)
        
        # Set partitioning if specified
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        
        # Set clustering if specified
        if clustering_fields:
            table.clustering_fields = clustering_fields
        
        # Create or update the table
        try:
            existing_table = client.get_table(table_ref)
            # Update existing table
            client.update_table(table, ["schema", "time_partitioning", "clustering_fields"])
            logger.info(f"Updated table {dataset_id}.{table_id}")
        except NotFound:
            # Create new table
            client.create_table(table)
            logger.info(f"Created table {dataset_id}.{table_id}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating or updating table: {e}")
        return False

def execute_query(client: bigquery.Client, query: str) -> bool:
    # Execute a BigQuery query
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        logger.info(f"Query executed successfully: {query[:100]}...")
        return True
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return False
