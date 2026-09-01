#!/usr/bin/env python3
"""
Table Processing Tracking Module

Tracks individual table processing through extraction, transformation, and merge phases.
Provides detailed metrics for monitoring pipeline performance and debugging.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional
from google.cloud import bigquery
import logging
import uuid

logger = logging.getLogger(__name__)

def ensure_table_processing_table(client: bigquery.Client) -> None:
    """
    Create table_processing tracking table if it doesn't exist.

    This table tracks every individual table processed through the pipeline,
    capturing detailed metrics at each stage (extraction, transformation, merge).
    """
    table_id = f"{client.project}.orchestrator_monitoring.table_processing"

    try:
        client.get_table(table_id)
        logger.debug(f"Table processing table exists: {table_id}")
        return
    except Exception:
        pass

    schema = [
        # Identifiers
        bigquery.SchemaField("processing_id", "STRING", mode="REQUIRED",
                           description="Unique ID for this table processing operation"),
        bigquery.SchemaField("job_id", "STRING", mode="REQUIRED",
                           description="Parent job ID from orchestrator_monitoring.jobs"),
        bigquery.SchemaField("execution_id", "STRING", mode="REQUIRED",
                           description="Execution ID for this pipeline run"),

        # Table identification
        bigquery.SchemaField("source", "STRING", mode="REQUIRED",
                           description="Source system (facebook, wordpress, mailchimp, etc)"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED",
                           description="Logical table name (campaigns, posts, etc)"),
        bigquery.SchemaField("project_id", "STRING", mode="REQUIRED",
                           description="GCP project ID"),
        bigquery.SchemaField("staging_dataset", "STRING", mode="REQUIRED",
                           description="Staging dataset name"),
        bigquery.SchemaField("production_dataset", "STRING", mode="REQUIRED",
                           description="Production dataset name"),
        bigquery.SchemaField("staging_table", "STRING",
                           description="Full staging table ID (project.dataset.table)"),
        bigquery.SchemaField("production_table", "STRING",
                           description="Full production table ID (project.dataset.table)"),

        # Timing
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED",
                           description="When tracking record was created"),
        bigquery.SchemaField("extraction_started_at", "TIMESTAMP",
                           description="When extraction phase started"),
        bigquery.SchemaField("extraction_completed_at", "TIMESTAMP",
                           description="When extraction phase completed"),
        bigquery.SchemaField("transform_started_at", "TIMESTAMP",
                           description="When transform phase started"),
        bigquery.SchemaField("transform_completed_at", "TIMESTAMP",
                           description="When transform phase completed"),
        bigquery.SchemaField("merge_started_at", "TIMESTAMP",
                           description="When merge phase started"),
        bigquery.SchemaField("merge_completed_at", "TIMESTAMP",
                           description="When merge phase completed"),
        bigquery.SchemaField("completed_at", "TIMESTAMP",
                           description="When entire table processing completed"),
        bigquery.SchemaField("duration_seconds", "FLOAT64",
                           description="Total duration in seconds"),

        # Status
        bigquery.SchemaField("status", "STRING", mode="REQUIRED",
                           description="Current status: processing, success, failed, skipped"),
        bigquery.SchemaField("phase", "STRING",
                           description="Current/last phase: extraction, transform, merge"),

        # Extraction metrics
        bigquery.SchemaField("rows_extracted", "INT64",
                           description="Number of rows extracted from source"),
        bigquery.SchemaField("extraction_method", "STRING",
                           description="Extraction method (api, sdk, database, etc)"),
        bigquery.SchemaField("incremental_mode", "BOOLEAN",
                           description="Whether this was an incremental extraction"),
        bigquery.SchemaField("date_range_start", "TIMESTAMP",
                           description="Start date for incremental extraction"),
        bigquery.SchemaField("date_range_end", "TIMESTAMP",
                           description="End date for incremental extraction"),

        # Staging metrics
        bigquery.SchemaField("staging_rows", "INT64",
                           description="Number of rows in staging table"),
        bigquery.SchemaField("staging_size_mb", "FLOAT64",
                           description="Size of staging table in MB"),

        # Merge metrics
        bigquery.SchemaField("rows_inserted", "INT64",
                           description="Number of new rows inserted into production"),
        bigquery.SchemaField("rows_updated", "INT64",
                           description="Number of existing rows updated"),
        bigquery.SchemaField("rows_unchanged", "INT64",
                           description="Number of rows that didn't change"),
        bigquery.SchemaField("rows_deleted", "INT64",
                           description="Number of rows deleted (for full refresh)"),

        # Production table metrics
        bigquery.SchemaField("production_rows_before", "INT64",
                           description="Row count in production table before merge"),
        bigquery.SchemaField("production_rows_after", "INT64",
                           description="Row count in production table after merge"),
        bigquery.SchemaField("production_size_mb", "FLOAT64",
                           description="Size of production table in MB after merge"),

        # Performance metrics
        bigquery.SchemaField("extraction_duration_seconds", "FLOAT64",
                           description="Time spent in extraction phase"),
        bigquery.SchemaField("transform_duration_seconds", "FLOAT64",
                           description="Time spent in transform phase"),
        bigquery.SchemaField("merge_duration_seconds", "FLOAT64",
                           description="Time spent in merge phase"),

        # Error tracking
        bigquery.SchemaField("error_message", "STRING",
                           description="Error message if processing failed"),
        bigquery.SchemaField("error_phase", "STRING",
                           description="Phase where error occurred"),
        bigquery.SchemaField("error_type", "STRING",
                           description="Type/category of error"),

        # Configuration
        bigquery.SchemaField("refresh_mode", "STRING",
                           description="Refresh mode: incremental, full, rebuild"),
        bigquery.SchemaField("test_mode", "BOOLEAN",
                           description="Whether this was a test run"),
        bigquery.SchemaField("primary_keys", "STRING", mode="REPEATED",
                           description="Primary key fields for this table"),

        # GCS tracking
        bigquery.SchemaField("gcs_path", "STRING",
                           description="GCS path for extracted data"),
        bigquery.SchemaField("parquet_file_size_mb", "FLOAT64",
                           description="Size of parquet file in MB"),

        # Metadata
        bigquery.SchemaField("environment", "STRING",
                           description="Environment: prod, dev, test"),
        bigquery.SchemaField("updated_at", "TIMESTAMP",
                           description="Last update timestamp"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    # Partition by created_at for better query performance
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at"
    )

    # Cluster by frequently queried fields
    table.clustering_fields = ["source", "status", "table_name", "environment"]

    client.create_table(table)
    logger.info(f"Created table processing tracking table: {table_id}")


def start_table_processing(
    job_id: str,
    execution_id: str,
    source: str,
    table_name: str,
    project_id: str,
    staging_dataset: str,
    production_dataset: str,
    environment: str = "prod",
    refresh_mode: str = "incremental",
    test_mode: bool = False,
    primary_keys: list = None
) -> str:
    """
    Start tracking a table processing operation.

    Returns:
        processing_id: Unique ID for this table processing operation
    """
    from shared.bigquery_client import get_bigquery_client

    client = get_bigquery_client()
    ensure_table_processing_table(client)

    processing_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    table_id = f"{client.project}.orchestrator_monitoring.table_processing"

    insert_query = f"""
    INSERT INTO `{table_id}` (
        processing_id,
        job_id,
        execution_id,
        source,
        table_name,
        project_id,
        staging_dataset,
        production_dataset,
        status,
        phase,
        created_at,
        updated_at,
        environment,
        refresh_mode,
        test_mode,
        primary_keys
    ) VALUES (
        @processing_id,
        @job_id,
        @execution_id,
        @source,
        @table_name,
        @project_id,
        @staging_dataset,
        @production_dataset,
        'processing',
        'extraction',
        @created_at,
        @updated_at,
        @environment,
        @refresh_mode,
        @test_mode,
        @primary_keys
    )
    """

    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("processing_id", "STRING", processing_id),
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
            bigquery.ScalarQueryParameter("execution_id", "STRING", execution_id),
            bigquery.ScalarQueryParameter("source", "STRING", source),
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("staging_dataset", "STRING", staging_dataset),
            bigquery.ScalarQueryParameter("production_dataset", "STRING", production_dataset),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("environment", "STRING", environment),
            bigquery.ScalarQueryParameter("refresh_mode", "STRING", refresh_mode),
            bigquery.ScalarQueryParameter("test_mode", "BOOL", bool(test_mode)),
            bigquery.ArrayQueryParameter("primary_keys", "STRING", primary_keys or []),
        ]
    )

    try:
        client.query(insert_query, job_config=query_config).result()
        logger.debug(f"Started tracking table processing: {processing_id} ({source}.{table_name})")
        return processing_id
    except Exception as e:
        logger.error(f"Error starting table processing tracking: {e}")
        return processing_id


def update_table_processing(
    processing_id: str,
    **fields
) -> bool:
    """
    Update a table processing record with new data.

    Args:
        processing_id: The processing ID to update
        **fields: Fields to update (see schema for available fields)

    Returns:
        True if successful, False otherwise
    """
    from shared.bigquery_client import get_bigquery_client

    client = get_bigquery_client()
    table_id = f"{client.project}.orchestrator_monitoring.table_processing"

    # Add updated_at timestamp
    fields['updated_at'] = datetime.now(timezone.utc)

    set_clauses = []
    query_parameters = [bigquery.ScalarQueryParameter("processing_id", "STRING", processing_id)]

    def _infer_scalar_parameter(name: str, value: Any) -> bigquery.ScalarQueryParameter:
        if isinstance(value, bool):
            return bigquery.ScalarQueryParameter(name, "BOOL", value)
        if isinstance(value, int):
            return bigquery.ScalarQueryParameter(name, "INT64", value)
        if isinstance(value, float):
            return bigquery.ScalarQueryParameter(name, "FLOAT64", value)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return bigquery.ScalarQueryParameter(name, "TIMESTAMP", value)
        return bigquery.ScalarQueryParameter(name, "STRING", str(value))

    def _infer_array_parameter(name: str, values: list) -> Optional[bigquery.ArrayQueryParameter]:
        if not values:
            return bigquery.ArrayQueryParameter(name, "STRING", [])
        sample = values[0]
        if isinstance(sample, bool):
            return bigquery.ArrayQueryParameter(name, "BOOL", values)
        if isinstance(sample, int):
            return bigquery.ArrayQueryParameter(name, "INT64", values)
        if isinstance(sample, float):
            return bigquery.ArrayQueryParameter(name, "FLOAT64", values)
        return bigquery.ArrayQueryParameter(name, "STRING", [str(v) for v in values])

    for idx, (key, value) in enumerate(fields.items()):
        param_name = f"param_{idx}"

        if value is None:
            set_clauses.append(f"{key} = NULL")
            continue

        if isinstance(value, list):
            array_param = _infer_array_parameter(param_name, value)
            if array_param:
                query_parameters.append(array_param)
                set_clauses.append(f"{key} = @{param_name}")
            else:
                set_clauses.append(f"{key} = []")
            continue

        scalar_param = _infer_scalar_parameter(param_name, value)
        query_parameters.append(scalar_param)
        set_clauses.append(f"{key} = @{param_name}")

    if not set_clauses:
        return True

    query = f"""
    UPDATE `{table_id}`
    SET {', '.join(set_clauses)}
    WHERE processing_id = @processing_id
    """

    try:
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        client.query(query, job_config=job_config).result()
        logger.debug(f"Updated table processing: {processing_id}")
        return True
    except Exception as e:
        logger.error(f"Error updating table processing {processing_id}: {e}")
        return False


def complete_table_processing(
    processing_id: str,
    status: str = "success",
    **final_metrics
) -> bool:
    """
    Mark table processing as complete with final metrics.

    Args:
        processing_id: The processing ID to complete
        status: Final status (success, failed, skipped)
        **final_metrics: Final metrics to record

    Returns:
        True if successful, False otherwise
    """
    final_metrics['status'] = status
    completion_time = datetime.now(timezone.utc)
    final_metrics['completed_at'] = completion_time

    # Calculate total duration if we have created_at
    try:
        from shared.bigquery_client import get_bigquery_client
        client = get_bigquery_client()
        table_id = f"{client.project}.orchestrator_monitoring.table_processing"

        # Get created_at to calculate duration
        query = f"""
        SELECT created_at
        FROM `{table_id}`
        WHERE processing_id = @processing_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("processing_id", "STRING", processing_id)]
        )
        result = client.query(query, job_config=job_config).result()
        for row in result:
            created_at = row.created_at
            if isinstance(created_at, datetime):
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
            else:
                created_at = datetime.fromisoformat(str(created_at)).replace(tzinfo=timezone.utc)

            completed_at = completion_time
            if completed_at.tzinfo is None:
                completed_at = completed_at.replace(tzinfo=timezone.utc)

            duration = (completed_at - created_at).total_seconds()
            final_metrics['duration_seconds'] = duration
            break
    except Exception as e:
        logger.warning(f"Could not calculate duration for {processing_id}: {e}")

    return update_table_processing(processing_id, **final_metrics)
