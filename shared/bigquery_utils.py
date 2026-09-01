#!/usr/bin/env python3
# Consolidated BigQuery utilities for data processing and loading

import os
import json
import hashlib
import re
import tempfile
import logging
import uuid
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Generator, Tuple
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Setup logging
logger = logging.getLogger(__name__)

#
# Dataset management
#


def drop_staging_tables(client: bigquery.Client, table_id: str) -> bool:
    """
    Drop both external and regular staging tables for full refresh.

    When using external tables backed by GCS, we cannot TRUNCATE.
    Instead, we must DROP both the external table (table_external) and
    the regular staging table (table) to ensure a clean refresh.

    Args:
        client: BigQuery client instance
        table_id: Full table identifier (project.dataset.table)

    Returns:
        True if drop succeeded or tables didn't exist

    Example:
        >>> drop_staging_tables(client, 'project.facebook_staging.campaigns')
        True  # Drops both campaigns_external and campaigns tables
    """
    success = True

    # Parse table_id to get base name
    parts = table_id.split(".")
    if len(parts) == 3:
        project, dataset, table_name = parts
        base_table = f"{project}.{dataset}.{table_name}"
        external_table = f"{project}.{dataset}.{table_name}_external"
    else:
        logger.error(f"Invalid table_id format: {table_id}")
        return False

    # Drop external table first (if exists)
    try:
        client.delete_table(external_table, not_found_ok=True)
        logger.info(f"Dropped external staging table: {external_table}")
    except Exception as e:
        logger.warning(f"Could not drop external table {external_table}: {e}")
        success = False

    # Drop regular staging table (if exists)
    try:
        client.delete_table(base_table, not_found_ok=True)
        logger.info(f"Dropped staging table: {base_table}")
    except Exception as e:
        logger.warning(f"Could not drop staging table {base_table}: {e}")
        success = False

    return success


def ensure_dataset_exists(
    client: bigquery.Client, dataset_id: str, location: str = "US"
) -> bool:
    """
    Create BigQuery dataset if it doesn't exist.

    Safely creates datasets with proper location and configuration.
    Handles existing datasets gracefully without errors.

    Args:
        client: BigQuery client instance
        dataset_id: Dataset identifier to create (can be 'dataset' or 'project.dataset')
        location: Geographic location for dataset (default: "US")

    Returns:
        True if dataset exists or was created successfully

    Example:
        >>> client = bigquery.Client()
        >>> ensure_dataset_exists(client, 'facebook_staging')
        True
        >>> ensure_dataset_exists(client, 'my-project.wordpress_data', 'US')
        True
    """
    # Handle both 'dataset' and 'project.dataset' formats
    if "." not in dataset_id:
        dataset_id = f"{client.project}.{dataset_id}"

    try:
        # Check if dataset already exists
        client.get_dataset(dataset_id)
        logger.info(f"Dataset '{dataset_id}' already exists")
        return True

    except NotFound:
        # Dataset doesn't exist, create it
        try:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location

            # Create dataset
            created_dataset = client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset: {dataset_id} in {location}")
            return True

        except Exception as e:
            logger.error(f"Failed to create dataset '{dataset_id}': {e}")
            return False

    except Exception as e:
        logger.error(f"Error checking dataset '{dataset_id}': {e}")
        return False


#
# Change tracking and archive functionality
#


def ensure_archive_table(
    client: bigquery.Client, main_table: str, pk_columns: List[str]
) -> str:
    """
    Create archive table for removed records if it doesn't exist.

    Archive tables store full row snapshots of records that were removed
    (existed in production but not in staging during a full refresh).

    Args:
        client: BigQuery client instance
        main_table: Full production table ID (project.dataset.table)
        pk_columns: Primary key columns for clustering

    Returns:
        Full archive table ID (project.dataset_archive.table_removed)
    """
    # Parse main table ID
    parts = main_table.split(".")
    if len(parts) == 3:
        project, dataset, table = parts
    else:
        project = client.project
        dataset = parts[-2] if len(parts) > 1 else "unknown"
        table = parts[-1]

    archive_dataset = f"{dataset}_archive"
    archive_table_name = f"{table}_removed"
    full_archive_table = f"{project}.{archive_dataset}.{archive_table_name}"

    # Ensure archive dataset exists
    ensure_dataset_exists(client, f"{project}.{archive_dataset}")

    # Check if archive table exists
    try:
        client.get_table(full_archive_table)
        logger.debug(f"Archive table {full_archive_table} already exists")
        return full_archive_table
    except NotFound:
        pass

    # Create archive table by copying schema from main table and adding archive columns
    try:
        main_ref = client.get_table(main_table)
        main_schema = list(main_ref.schema)

        # Archive-specific columns (prepended to schema)
        archive_columns = [
            bigquery.SchemaField(
                "removed_at",
                "TIMESTAMP",
                description="When record was removed from source",
            ),
            bigquery.SchemaField(
                "archive_execution_id",
                "STRING",
                description="Pipeline execution ID when archived",
            ),
            bigquery.SchemaField(
                "removal_reason",
                "STRING",
                description="Reason for removal (e.g., NOT_IN_SOURCE)",
            ),
        ]

        # Combine archive columns + original schema
        archive_schema = archive_columns + main_schema

        # Create table with partitioning and clustering
        table_obj = bigquery.Table(full_archive_table, schema=archive_schema)
        table_obj.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="removed_at"
        )

        # Cluster by primary key columns (up to 4)
        cluster_columns = pk_columns[:4] if len(pk_columns) <= 4 else pk_columns[:4]
        if cluster_columns:
            table_obj.clustering_fields = cluster_columns

        client.create_table(table_obj)
        logger.info(f"Created archive table: {full_archive_table}")

    except Exception as e:
        logger.error(f"Failed to create archive table {full_archive_table}: {e}")
        raise

    return full_archive_table


def archive_removed_records(
    client: bigquery.Client,
    staging_table: str,
    main_table: str,
    primary_keys: List[str],
    execution_id: str = None,
) -> int:
    """
    Archive records that exist in production but not in staging (will be removed).

    This should be called BEFORE the MERGE operation to capture the full row
    data before it's deleted.

    Args:
        client: BigQuery client instance
        staging_table: Full staging table ID
        main_table: Full production table ID
        primary_keys: List of primary key column names
        execution_id: Pipeline execution ID

    Returns:
        Number of records archived
    """
    # Get archive table (creates if doesn't exist)
    archive_table = ensure_archive_table(client, main_table, primary_keys)

    # Build join conditions
    pk_columns = ", ".join([f"`{k}`" for k in primary_keys])
    join_conditions = " AND ".join(
        [f"main.`{k}` = staging.`{k}`" for k in primary_keys]
    )
    first_pk = primary_keys[0]

    # Get main table columns (excluding archive-specific columns)
    main_ref = client.get_table(main_table)
    main_columns = [field.name for field in main_ref.schema]
    main_columns_str = ", ".join([f"main.`{col}`" for col in main_columns])

    # Archive query: find records in main that don't exist in staging
    archive_query = f"""
    INSERT INTO `{archive_table}` (removed_at, archive_execution_id, removal_reason, {', '.join([f'`{col}`' for col in main_columns])})
    SELECT
        CURRENT_TIMESTAMP() as removed_at,
        '{execution_id or "unknown"}' as archive_execution_id,
        'NOT_IN_SOURCE' as removal_reason,
        {main_columns_str}
    FROM `{main_table}` main
    LEFT JOIN (
        SELECT DISTINCT {pk_columns}
        FROM `{staging_table}`
    ) staging ON {join_conditions}
    WHERE staging.`{first_pk}` IS NULL
    """

    try:
        job = client.query(archive_query)
        job.result()
        rows_archived = job.num_dml_affected_rows or 0

        if rows_archived > 0:
            logger.info(
                f"  Archived {rows_archived} removed records to {archive_table}"
            )

        return rows_archived

    except Exception as e:
        logger.error(f"Failed to archive removed records: {e}")
        # Don't fail the merge if archiving fails - log and continue
        return 0


def log_data_changes(
    client: bigquery.Client,
    dataset: str,
    table_name: str,
    rows_inserted: int,
    rows_updated: int,
    rows_removed: int,
    rows_total: int,
    execution_id: str = None,
) -> bool:
    """
    Log merge change statistics to centralized monitoring table.

    Args:
        client: BigQuery client instance
        dataset: Target dataset name (e.g., 'npi_data')
        table_name: Target table name (e.g., 'npi_main')
        rows_inserted: Count of new rows added
        rows_updated: Count of existing rows modified
        rows_removed: Count of rows removed from source
        rows_total: Total rows after merge
        execution_id: Pipeline execution ID

    Returns:
        True if logged successfully
    """
    # Skip if no changes
    if rows_inserted == 0 and rows_updated == 0 and rows_removed == 0:
        return True

    monitoring_table = f"{client.project}.orchestrator_monitoring.data_changes"

    # Ensure data_changes table exists
    try:
        client.get_table(monitoring_table)
    except NotFound:
        # Create the table
        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{monitoring_table}` (
            change_timestamp TIMESTAMP,
            dataset STRING,
            table_name STRING,
            rows_inserted INT64,
            rows_updated INT64,
            rows_removed INT64,
            rows_total INT64,
            execution_id STRING
        )
        PARTITION BY DATE(change_timestamp)
        CLUSTER BY dataset, table_name
        """
        try:
            client.query(ddl).result()
            logger.info(f"Created data_changes monitoring table")
        except Exception as e:
            logger.warning(f"Could not create data_changes table: {e}")
            return False

    # Insert change record
    insert_query = f"""
    INSERT INTO `{monitoring_table}`
    (change_timestamp, dataset, table_name, rows_inserted, rows_updated, rows_removed, rows_total, execution_id)
    VALUES (
        CURRENT_TIMESTAMP(),
        '{dataset}',
        '{table_name}',
        {rows_inserted},
        {rows_updated},
        {rows_removed},
        {rows_total},
        '{execution_id or "unknown"}'
    )
    """

    try:
        client.query(insert_query).result()
        logger.debug(
            f"Logged data changes: +{rows_inserted} ~{rows_updated} -{rows_removed}"
        )
        return True
    except Exception as e:
        logger.warning(f"Could not log data changes: {e}")
        return False


#
# Hash-based merge functionality (from bigquery_hash.py)
#


def generate_hash(row: Dict[str, Any], keys: List[str]) -> str:
    # Generate a hash for a row based on specified keys
    hash_values = []
    for key in sorted(keys):
        value = row.get(key, "")
        if value is None:
            hash_values.append("")
        elif isinstance(value, (dict, list)):
            hash_values.append(json.dumps(value, sort_keys=True))
        else:
            hash_values.append(str(value))

    hash_string = "|".join(hash_values)
    return hashlib.md5(hash_string.encode()).hexdigest()


def resolve_preserve_nulls(config: Dict[str, Any]) -> bool:
    """
    Whether a re-pulled NULL should PRESERVE the existing stored value instead of
    overwriting it (BI-486 NULL-overwrite guard). Default ON -- overwriting good
    historical data with a transient-outage NULL is never desired for these
    append/upsert pipelines. A source opts out with
    `hash_merge.preserve_nulls_on_update: false` (or top-level
    `preserve_nulls_on_update: false`).
    """
    config = config or {}
    return config.get("hash_merge", {}).get(
        "preserve_nulls_on_update",
        config.get("preserve_nulls_on_update", True),
    )


def build_update_assignment(col: str, preserve_nulls: bool) -> str:
    """
    Build one MERGE UPDATE SET assignment for a business column.

    preserve_nulls=True -> `main.col = COALESCE(staging.col, main.col)`: a NULL in
    staging keeps the existing value (transient API NULL can't clobber good data);
    a genuine new non-null value still overwrites.
    preserve_nulls=False -> `main.col = staging.col` (legacy overwrite).
    """
    if preserve_nulls:
        return f"main.`{col}` = COALESCE(staging.`{col}`, main.`{col}`)"
    return f"main.`{col}` = staging.`{col}`"


def process_hash_merge(
    client: bigquery.Client,
    staging_table: str,
    main_table: str,
    primary_keys: List[str],
    config: Dict[str, Any] = None,
) -> Dict[str, Any]:
    # Process hash-based merge from staging to main table
    # ALWAYS uses MERGE (upsert) logic - NEVER deletes existing data unless --rebuild flag used
    config = config or {}
    stats = {
        "staging_rows": 0,
        "new_rows": 0,
        "updated_rows": 0,
        "unchanged_rows": 0,
        "total_rows": 0,
    }

    try:
        # NOTE: Production table deletion for --rebuild is now handled
        # in extractors BEFORE hash merge to avoid schema conflicts
        # This function only creates the table if it doesn't exist

        # Extract source, execution_id, and rebuild_mode from config for YAML schema lookup
        source = config.get("source", None)
        execution_id = config.get("execution_id", None)
        rebuild_mode = config.get("rebuild", False)

        # Ensure production table exists with all 6 system metadata fields.
        # Apply the resource's configured partitioning/clustering at CREATE time
        # (it can only be set when the table is first made) + lineage labels.
        # Business field schema evolution is handled separately by apply_schema_evolution()
        ensure_system_metadata_fields(
            client,
            staging_table,
            main_table,
            source=source,
            execution_id=execution_id,
            rebuild_mode=rebuild_mode,
            partitioning=config.get("partitioning"),
            clustering=config.get("clustering"),
        )

        # Get exclude_from_hash fields from config
        exclude_from_hash = config.get("exclude_from_hash", [])

        # Add row_hash to staging if it doesn't exist
        # row_hash will include ALL fields except tracking/metadata fields
        add_hash_column(
            client, staging_table, primary_keys, exclude_fields=exclude_from_hash
        )

        # Get staging row count - use streaming query to avoid loading all results in memory
        count_query = f"SELECT COUNT(*) as cnt FROM `{staging_table}`"
        query_job = client.query(count_query)
        for row in query_job:
            stats["staging_rows"] = row["cnt"]
            break

        if stats["staging_rows"] == 0:
            logger.info("  No rows in staging table to merge")
            return stats

        # Get column names from staging table - use iterator to reduce memory usage
        table_ref = client.get_table(staging_table)
        columns = [field.name for field in table_ref.schema]

        # Get production table schema for SAFE_CAST
        try:
            main_ref = client.get_table(main_table)
            main_schema = {field.name: field.field_type for field in main_ref.schema}

            # DEBUG: Log all field type mismatches between staging and production
            staging_types = {field.name: field.field_type for field in table_ref.schema}
            mismatches = []
            for col_name in columns:
                if col_name in main_schema and col_name in staging_types:
                    if staging_types[col_name] != main_schema[col_name]:
                        mismatches.append(
                            f"{col_name}: {staging_types[col_name]} (staging) -> {main_schema[col_name]} (production)"
                        )

            if mismatches:
                logger.debug(f"  Type conversions (SAFE_CAST will handle):")
                for mismatch in mismatches:
                    logger.debug(f"    {mismatch}")
        except NotFound:
            # Table doesn't exist yet, will be created
            main_schema = {}

        # Build column lists with SAFE_CAST to match production schema
        # EXCLUDE system fields that are set explicitly in MERGE (loaded_at, updated_at)
        # loaded_at and updated_at should NOT be in the staging CTE since we set them with CURRENT_TIMESTAMP()
        # source MUST be included since we reference staging.source in the INSERT clause
        columns_for_select = [
            col for col in columns if col not in ["loaded_at", "updated_at"]
        ]

        if main_schema:
            # Normalize type names (BigQuery doesn't have FLOAT, only FLOAT64)
            def normalize_bq_type(type_name):
                if type_name == "FLOAT":
                    return "FLOAT64"
                elif type_name == "INTEGER":
                    return "INT64"
                elif type_name == "BOOLEAN":
                    return "BOOL"
                return type_name

            # Use SAFE_CAST to convert staging data to match production types
            select_columns = ", ".join(
                [
                    (
                        f"SAFE_CAST(`{col}` AS {normalize_bq_type(main_schema[col])}) as `{col}`"
                        if col in main_schema
                        else f"`{col}`"
                    )
                    for col in columns_for_select
                ]
            )
            logger.debug(
                f"  Using SAFE_CAST for {len([c for c in columns_for_select if c in main_schema])} columns to match production schema"
            )
        else:
            # No production table yet, use staging columns as-is (still excluding system fields)
            select_columns = ", ".join([f"`{col}`" for col in columns_for_select])

        partition_by = ", ".join([f"`{pk}`" for pk in primary_keys])
        join_conditions = " AND ".join(
            [f"main.`{pk}` = staging.`{pk}`" for pk in primary_keys]
        )

        # Build UPDATE SET clause (exclude primary keys and ALL system fields)
        # System fields are set explicitly below (execution_id, updated_at, row_hash)
        # Immutable fields (loaded_at, source) are never updated
        system_fields = [
            "row_hash",
            "loaded_at",
            "source",
            "execution_id",
            "updated_at",
        ] + primary_keys
        update_columns = [col for col in columns if col not in system_fields]

        # NULL-overwrite guard (BI-486). The pipeline re-extracts a trailing
        # lookback window every run and upserts it; when an upstream API has a
        # transient outage (or deprecates a metric) it returns NULL for a value
        # that was previously populated. Without this guard,
        # `main.col = staging.col` would overwrite the good stored value with
        # NULL -- silently turning a temporary source hiccup into PERMANENT loss
        # of historical data, which violates the rule that the ETL preserves old
        # data (intentional removals are handled as deliberate one-offs).
        #
        # With preserve_nulls_on_update (default ON), a NULL in staging keeps the
        # existing main value: `main.col = COALESCE(staging.col, main.col)`. A
        # genuine new non-null value still overwrites as before. Set
        # `hash_merge.preserve_nulls_on_update: false` in a source config to opt
        # out (e.g. if a source legitimately needs to clear a field to NULL).
        preserve_nulls = resolve_preserve_nulls(config)

        if update_columns:
            update_set = ", ".join(
                build_update_assignment(col, preserve_nulls) for col in update_columns
            )
            update_set += ", `row_hash` = staging.`row_hash`, `execution_id` = staging.`execution_id`, `updated_at` = CURRENT_TIMESTAMP()"
        else:
            update_set = "`row_hash` = staging.`row_hash`, `execution_id` = staging.`execution_id`, `updated_at` = CURRENT_TIMESTAMP()"
        if preserve_nulls and update_columns:
            logger.info(
                "  NULL-overwrite guard ON: re-pulled NULLs preserve existing values (%d cols)",
                len(update_columns),
            )

        # Build INSERT column list (exclude loaded_at, source, updated_at - will be added explicitly)
        insert_cols = [
            col for col in columns if col not in ["updated_at", "loaded_at", "source"]
        ]
        insert_columns = ", ".join([f"`{col}`" for col in insert_cols])
        insert_values = ", ".join([f"staging.`{col}`" for col in insert_cols])

        # Build ORDER BY clause for ROW_NUMBER() - only use columns that exist in staging
        order_by_parts = []

        # Check if this is a metric-based table (has engagement columns) - prioritize by total engagement
        metric_columns = [
            "impressions",
            "clicks",
            "active_view_measurable_impressions",
            "active_view_viewable_impressions",
            "total_conversions",
        ]
        has_metrics = all(col in columns for col in metric_columns)

        if has_metrics:
            # For tables with engagement metrics, keep row with highest total engagement
            # This handles cases where same PK has multiple rows with different metric values
            metric_sum = " + ".join([f"COALESCE(`{col}`, 0)" for col in metric_columns])
            order_by_parts.append(f"({metric_sum}) DESC")

        # Then by timestamp (for tables without metrics, or as tiebreaker)
        if "extracted_at" in columns:
            order_by_parts.append("`extracted_at` DESC NULLS LAST")
        if "loaded_at" in columns:
            order_by_parts.append("`loaded_at` DESC NULLS LAST")

        if not order_by_parts:
            # Fallback: order by first primary key if no timestamp columns
            order_by_parts.append(f"`{primary_keys[0]}` DESC")

        order_by_clause = ", ".join(order_by_parts)

        # Fix #15: Add merge lock to prevent deadlocks from concurrent jobs
        import time

        lock_table = f"{client.project}.orchestrator_monitoring.merge_locks"
        lock_key = main_table
        lock_acquired = False

        # Ensure lock table exists
        try:
            from shared.monitoring import ensure_monitoring_infrastructure

            ensure_monitoring_infrastructure(client)
        except Exception as e:
            logger.warning(f"Could not create monitoring infrastructure: {e}")

        # Try to acquire lock with retry
        max_lock_attempts = 12  # 12 attempts × 5s = 60s max wait
        for attempt in range(max_lock_attempts):
            try:
                # Try to insert lock (fails if lock already exists)
                lock_insert_query = f"""
                INSERT INTO `{lock_table}` (lock_key, job_id, execution_id, expires_at)
                VALUES (
                    '{lock_key}',
                    '{execution_id or "unknown"}',
                    '{execution_id or "unknown"}',
                    TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
                )
                """
                client.query(lock_insert_query).result()
                lock_acquired = True
                logger.info(f"  Acquired merge lock for {main_table}")
                break
            except Exception as e:
                if attempt < max_lock_attempts - 1:
                    logger.info(
                        f"  Merge lock held by another job, waiting... (attempt {attempt+1}/{max_lock_attempts})"
                    )
                    time.sleep(5)

                    # Clean up expired locks
                    try:
                        cleanup_query = f"""
                        DELETE FROM `{lock_table}`
                        WHERE lock_key = '{lock_key}'
                          AND expires_at < CURRENT_TIMESTAMP()
                        """
                        client.query(cleanup_query).result()
                    except Exception as e:
                        logger.debug(f"Could not clean up expired locks: {e}")
                else:
                    raise RuntimeError(
                        f"Could not acquire merge lock for {main_table} after {max_lock_attempts} attempts"
                    )

        # Get row count BEFORE merge to calculate inserts correctly
        count_before_query = f"SELECT COUNT(*) as cnt FROM `{main_table}`"
        query_job_before = client.query(count_before_query)
        rows_before_merge = 0
        for row in query_job_before:
            rows_before_merge = row["cnt"]
            break

        # Archive removed records BEFORE merge (if enabled)
        # This captures full row snapshots of records that will be deleted.
        #
        # refresh_mode semantics:
        #   'incremental' — staging holds a windowed subset; never DELETE non-matched (default safe).
        #   'full'        — staging holds maximum data fetched from source, but may still be
        #                   incomplete (e.g. API retention limits). Treated as non-destructive:
        #                   never DELETE non-matched. Use this when you want the largest possible
        #                   refresh without risking history loss.
        #   'rebuild'     — caller asserts staging IS the complete source-of-truth. Enables
        #                   DELETE-not-matched so production matches staging exactly.
        #
        # archive_removed config flag overrides the mode-based default in either direction.
        refresh_mode = config.get("refresh_mode", "incremental")
        archive_removed_explicit = config.get("archive_removed")  # None if not set

        if refresh_mode == "rebuild":
            archive_enabled = (
                archive_removed_explicit
                if archive_removed_explicit is not None
                else True
            )
        else:
            # 'full' and 'incremental' both default to NO deletion to protect historical data.
            archive_enabled = (
                archive_removed_explicit
                if archive_removed_explicit is not None
                else False
            )
            if archive_enabled:
                logger.warning(
                    f"archive_removed=True with refresh_mode={refresh_mode} - this will delete historical data!"
                )

        rows_removed = 0
        if archive_enabled and rows_before_merge > 0:
            try:
                rows_removed = archive_removed_records(
                    client, staging_table, main_table, primary_keys, execution_id
                )
            except Exception as archive_err:
                logger.warning(
                    f"Archive removed records failed (continuing): {archive_err}"
                )
                rows_removed = 0

        try:
            # Build MERGE query with optional metric sum comparison for DCM tables
            if has_metrics:
                # For metric-based tables, also add metric sum to staging CTE for comparison
                # This ensures we only update if staging has higher or equal engagement
                staging_metric_sum = f"({metric_sum}) as staging_metric_sum"
                staging_cte_select = f"{select_columns}, {staging_metric_sum}"

                # Build condition: update only if hash differs AND staging sum >= production sum
                # Use the precomputed staging_metric_sum from CTE to avoid ambiguous column references
                main_metric_sum = " + ".join(
                    [f"COALESCE(main.`{col}`, 0)" for col in metric_columns]
                )
                update_condition = f"staging.`row_hash` != main.`row_hash` AND staging.`staging_metric_sum` >= ({main_metric_sum})"
            else:
                # Non-metric tables: use original logic (hash comparison only)
                staging_cte_select = select_columns
                update_condition = "staging.`row_hash` != main.`row_hash`"

            # Build MERGE query with optional DELETE clause for removed records.
            # DELETE clause is added only when archive_enabled is True, which happens when:
            #   - refresh_mode='rebuild' (caller asserts staging is the complete source-of-truth), OR
            #   - archive_removed=True explicitly set in config (user override).
            # DELETE is DISABLED by default for both 'full' and 'incremental' refreshes to
            # protect historical data — staging in those modes may be a subset of production.
            delete_clause = ""
            if archive_enabled:
                delete_clause = "\n            WHEN NOT MATCHED BY SOURCE THEN DELETE"
                logger.info(
                    f"  DELETE clause enabled (refresh_mode={refresh_mode}, archive_removed={archive_removed_explicit})"
                )

            merge_query = f"""
            MERGE `{main_table}` AS main
            USING (
              SELECT {staging_cte_select},
                ROW_NUMBER() OVER (
                  PARTITION BY {partition_by}
                  ORDER BY {order_by_clause}
                ) as rn
              FROM `{staging_table}`
            ) AS staging
            ON {join_conditions} AND staging.rn = 1
            WHEN MATCHED AND {update_condition} THEN
              UPDATE SET {update_set}
            WHEN NOT MATCHED BY TARGET AND staging.rn = 1 THEN
              INSERT ({insert_columns}, `loaded_at`, `source`, `updated_at`)
              VALUES ({insert_values}, CURRENT_TIMESTAMP(), staging.`source`, CURRENT_TIMESTAMP()){delete_clause}
            """

            # Execute merge with job configuration for better performance
            job_config = bigquery.QueryJobConfig(
                priority=bigquery.QueryPriority.INTERACTIVE, use_query_cache=True
            )

            job = client.query(merge_query, job_config=job_config)
            job.result()  # Wait for the job to complete

            # Capture affected rows from MERGE
            affected_rows = job.num_dml_affected_rows or 0

        finally:
            # Always release lock
            if lock_acquired:
                try:
                    release_lock_query = f"""
                    DELETE FROM `{lock_table}`
                    WHERE lock_key = '{lock_key}'
                      AND execution_id = '{execution_id or "unknown"}'
                    """
                    client.query(release_lock_query).result()
                    logger.info(f"  Released merge lock for {main_table}")
                except Exception as e:
                    logger.warning(f"Could not release lock: {e}")

        # Get total rows in main table AFTER merge
        count_after_query = f"SELECT COUNT(*) as cnt FROM `{main_table}`"
        query_job_after = client.query(count_after_query)
        for row in query_job_after:
            stats["total_rows"] = row["cnt"]
            break

        # Handle staging table based on config
        # Archive MUST run before truncate — otherwise the archive is empty.
        if config.get("archive_staging", False):
            archive_staging_table(
                client, staging_table, config=config, execution_id=execution_id
            )

        # For external table workflows, must DROP both external and regular tables
        # (cannot TRUNCATE external tables)
        if config.get("truncate_staging", False):
            drop_staging_tables(client, staging_table)
            logger.info("  Staging tables dropped (external + regular)")

        stats["success"] = True
        # Calculate inserts: new rows added = total after - total before
        stats["rows_inserted"] = stats["total_rows"] - rows_before_merge
        # Calculate updates: affected rows - inserts
        stats["rows_updated"] = affected_rows - stats["rows_inserted"]

        # Validation: ensure no negative values
        if stats["rows_inserted"] < 0:
            logger.warning(
                f"Negative inserts detected ({stats['rows_inserted']}), setting to 0"
            )
            stats["rows_inserted"] = 0
            stats["rows_updated"] = affected_rows

        # Track removed records in stats
        stats["rows_removed"] = rows_removed

        # Log change statistics to monitoring table
        # Parse dataset and table name from main_table for logging
        table_parts = main_table.split(".")
        log_dataset = table_parts[-2] if len(table_parts) > 1 else "unknown"
        log_table = table_parts[-1]
        log_data_changes(
            client=client,
            dataset=log_dataset,
            table_name=log_table,
            rows_inserted=stats["rows_inserted"],
            rows_updated=stats["rows_updated"],
            rows_removed=rows_removed,
            rows_total=stats["total_rows"],
            execution_id=execution_id,
        )

        return stats

    except Exception as e:
        logger.error(f"  Hash merge error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        stats["success"] = False
        stats["error"] = str(e)
        return stats


def _bq_type_for_field(field: bigquery.SchemaField) -> str:
    """Map a BigQuery SchemaField to a SQL type string for CAST(NULL AS ...)."""
    type_map = {
        "STRING": "STRING",
        "BYTES": "BYTES",
        "INTEGER": "INT64",
        "INT64": "INT64",
        "FLOAT": "FLOAT64",
        "FLOAT64": "FLOAT64",
        "BOOLEAN": "BOOL",
        "BOOL": "BOOL",
        "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE",
        "TIME": "TIME",
        "DATETIME": "DATETIME",
        "NUMERIC": "NUMERIC",
        "BIGNUMERIC": "BIGNUMERIC",
        "GEOGRAPHY": "GEOGRAPHY",
        "JSON": "JSON",
    }
    return type_map.get(field.field_type, "STRING")


def _ensure_changelog_table(
    client: bigquery.Client, main_table: str, primary_keys: List[str]
) -> str:
    """
    Create changelog table for snapshot replace change tracking.

    Changelog tables mirror the production table schema with prepended
    changelog metadata columns (changelog_timestamp, change_type,
    changelog_execution_id). This allows direct querying of changed
    records without JSON parsing.

    Args:
        client: BigQuery client instance
        main_table: Full production table ID (project.dataset.table)
        primary_keys: Primary key columns for clustering

    Returns:
        Full changelog table ID (project.dataset_archive.table_changelog)
    """
    parts = main_table.split(".")
    if len(parts) == 3:
        project, dataset, table = parts
    else:
        project = client.project
        dataset = parts[-2] if len(parts) > 1 else "unknown"
        table = parts[-1]

    archive_dataset = f"{dataset}_archive"
    changelog_table = f"{project}.{archive_dataset}.{table}_changelog"

    ensure_dataset_exists(client, f"{project}.{archive_dataset}")

    # Check if changelog table already exists
    try:
        client.get_table(changelog_table)
        logger.debug(f"Changelog table {changelog_table} already exists")
        return changelog_table
    except NotFound:
        pass

    # Create changelog table by copying schema from production + changelog columns
    try:
        main_ref = client.get_table(main_table)
        main_schema = list(main_ref.schema)

        changelog_columns = [
            bigquery.SchemaField(
                "changelog_timestamp",
                "TIMESTAMP",
                description="When the change was detected",
            ),
            bigquery.SchemaField(
                "change_type", "STRING", description="ADDED, REMOVED, or UPDATED"
            ),
            bigquery.SchemaField(
                "changelog_execution_id", "STRING", description="Pipeline execution ID"
            ),
        ]

        changelog_schema = changelog_columns + main_schema

        table_obj = bigquery.Table(changelog_table, schema=changelog_schema)
        table_obj.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="changelog_timestamp"
        )

        # Cluster by change_type + primary keys (up to 4 total)
        cluster_cols = ["change_type"] + primary_keys[:3]
        table_obj.clustering_fields = cluster_cols

        client.create_table(table_obj)
        logger.info(f"Created changelog table: {changelog_table}")

    except Exception as e:
        logger.error(f"Failed to create changelog table {changelog_table}: {e}")
        raise

    return changelog_table


def _generate_snapshot_changelog(
    client: bigquery.Client,
    staging_table: str,
    main_table: str,
    primary_keys: List[str],
    exclude_from_hash: List[str],
    execution_id: str = None,
) -> Dict[str, int]:
    """
    Compare staging (new snapshot) vs production (old snapshot) and write changelog.

    Identifies ADDED, REMOVED, and UPDATED records by comparing primary keys
    and row hashes. Writes full rows (same columns as production) into the
    changelog table for direct querying.

    Args:
        client: BigQuery client instance
        staging_table: Full staging table ID (new snapshot)
        main_table: Full production table ID (old snapshot)
        primary_keys: List of primary key column names
        exclude_from_hash: Fields excluded from hash (system metadata)
        execution_id: Pipeline execution ID

    Returns:
        Dict with rows_added, rows_removed, rows_updated, rows_unchanged counts
    """
    changelog_table = _ensure_changelog_table(client, main_table, primary_keys)

    pk_select = ", ".join([f"`{k}`" for k in primary_keys])
    join_conditions = " AND ".join([f"s.`{k}` = p.`{k}`" for k in primary_keys])
    first_pk = primary_keys[0]

    # Get column names from both tables — staging may lack system columns
    # (loaded_at, updated_at, row_hash) that exist in production
    main_ref = client.get_table(main_table)
    staging_ref = client.get_table(staging_table)
    prod_columns = [f.name for f in main_ref.schema]
    staging_columns = {f.name for f in staging_ref.schema}

    # For INSERT target and production-side SELECT, use all production columns
    prod_cols_str = ", ".join([f"`{c}`" for c in prod_columns])
    prod_cols_p = ", ".join([f"p.`{c}`" for c in prod_columns])

    # For staging-side SELECT, use actual column if it exists, NULL otherwise
    prod_cols_s = ", ".join(
        [
            (
                f"s.`{c}`"
                if c in staging_columns
                else f"CAST(NULL AS {_bq_type_for_field(f)}) AS `{c}`"
            )
            for c, f in zip(prod_columns, main_ref.schema)
        ]
    )

    exec_id = execution_id or "unknown"
    stats = {"rows_added": 0, "rows_removed": 0, "rows_updated": 0, "rows_unchanged": 0}

    # --- ADDED records (in staging but not in production) ---
    added_query = f"""
    INSERT INTO `{changelog_table}`
    (changelog_timestamp, change_type, changelog_execution_id, {prod_cols_str})
    SELECT
        CURRENT_TIMESTAMP(),
        'ADDED',
        '{exec_id}',
        {prod_cols_s}
    FROM `{staging_table}` s
    LEFT JOIN (SELECT DISTINCT {pk_select} FROM `{main_table}`) p ON {join_conditions}
    WHERE p.`{first_pk}` IS NULL
    """
    try:
        job = client.query(added_query)
        job.result()
        stats["rows_added"] = job.num_dml_affected_rows or 0
        if stats["rows_added"] > 0:
            logger.info(f"  Changelog: {stats['rows_added']} ADDED records")
    except Exception as e:
        logger.warning(f"  Changelog ADDED query failed: {e}")

    # --- REMOVED records (in production but not in staging) ---
    removed_query = f"""
    INSERT INTO `{changelog_table}`
    (changelog_timestamp, change_type, changelog_execution_id, {prod_cols_str})
    SELECT
        CURRENT_TIMESTAMP(),
        'REMOVED',
        '{exec_id}',
        {prod_cols_p}
    FROM `{main_table}` p
    LEFT JOIN (SELECT DISTINCT {pk_select} FROM `{staging_table}`) s ON {join_conditions}
    WHERE s.`{first_pk}` IS NULL
    """
    try:
        job = client.query(removed_query)
        job.result()
        stats["rows_removed"] = job.num_dml_affected_rows or 0
        if stats["rows_removed"] > 0:
            logger.info(f"  Changelog: {stats['rows_removed']} REMOVED records")
    except Exception as e:
        logger.warning(f"  Changelog REMOVED query failed: {e}")

    # --- UPDATED records (in both but row_hash differs — store NEW values) ---
    updated_query = f"""
    INSERT INTO `{changelog_table}`
    (changelog_timestamp, change_type, changelog_execution_id, {prod_cols_str})
    SELECT
        CURRENT_TIMESTAMP(),
        'UPDATED',
        '{exec_id}',
        {prod_cols_s}
    FROM `{staging_table}` s
    INNER JOIN `{main_table}` p ON {join_conditions}
    WHERE s.`row_hash` != p.`row_hash`
    """
    try:
        job = client.query(updated_query)
        job.result()
        stats["rows_updated"] = job.num_dml_affected_rows or 0
        if stats["rows_updated"] > 0:
            logger.info(f"  Changelog: {stats['rows_updated']} UPDATED records")
    except Exception as e:
        logger.warning(f"  Changelog UPDATED query failed: {e}")

    # --- Count UNCHANGED ---
    unchanged_query = f"""
    SELECT COUNT(*) as cnt
    FROM `{staging_table}` s
    INNER JOIN `{main_table}` p ON {join_conditions}
    WHERE s.`row_hash` = p.`row_hash`
    """
    try:
        job = client.query(unchanged_query)
        for row in job:
            stats["rows_unchanged"] = row["cnt"]
            break
    except Exception as e:
        logger.debug(f"  Changelog unchanged count failed: {e}")

    logger.info(
        f"  Changelog summary: +{stats['rows_added']} -{stats['rows_removed']} ~{stats['rows_updated']} ={stats['rows_unchanged']}"
    )

    return stats


def process_snapshot_replace(
    client: bigquery.Client,
    staging_table: str,
    main_table: str,
    primary_keys: List[str],
    config: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Replace production table with staging snapshot, tracking changes via changelog.

    For full-snapshot sources (e.g., NPI registry) where each run provides
    the complete current dataset. Instead of MERGE (which marks all rows as
    updated), this:
    1. Computes row hashes on staging for change detection
    2. Compares staging vs production to generate a changelog (ADDED/REMOVED/UPDATED)
    3. Archives removed records (full row snapshots)
    4. Replaces production table entirely with staging data + system metadata
    5. Logs change statistics to monitoring

    Args:
        client: BigQuery client instance
        staging_table: Full staging table ID (project.dataset.table)
        main_table: Full production table ID (project.dataset.table)
        primary_keys: List of primary key column names
        config: Configuration dict with keys:
            - source: Source name for YAML schema lookup
            - execution_id: Pipeline execution ID
            - exclude_from_hash: Fields to exclude from hash calculation
            - rebuild: Whether this is a rebuild operation

    Returns:
        Dict with keys: success, staging_rows, rows_inserted, rows_updated,
        rows_removed, rows_unchanged, total_rows
    """
    config = config or {}
    source = config.get("source")
    execution_id = config.get("execution_id")
    exclude_from_hash = config.get("exclude_from_hash", [])
    rebuild_mode = config.get("rebuild", False)

    stats = {
        "staging_rows": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "rows_removed": 0,
        "rows_unchanged": 0,
        "total_rows": 0,
        "success": False,
    }

    try:
        # Step 1: Ensure production table exists with system metadata fields
        # (+ configured partitioning/clustering/labels, applied at CREATE time).
        ensure_system_metadata_fields(
            client,
            staging_table,
            main_table,
            source=source,
            execution_id=execution_id,
            rebuild_mode=rebuild_mode,
            partitioning=config.get("partitioning"),
            clustering=config.get("clustering"),
        )

        # Step 2: Add row_hash to staging for change detection
        add_hash_column(
            client, staging_table, primary_keys, exclude_fields=exclude_from_hash
        )

        # Step 3: Get staging row count
        count_query = f"SELECT COUNT(*) as cnt FROM `{staging_table}`"
        for row in client.query(count_query):
            stats["staging_rows"] = row["cnt"]
            break

        if stats["staging_rows"] == 0:
            logger.info("  No rows in staging table - skipping snapshot replace")
            return stats

        # Step 4: Check if production has data (skip changelog on first run or rebuild)
        production_rows = 0
        try:
            prod_count_query = f"SELECT COUNT(*) as cnt FROM `{main_table}`"
            for row in client.query(prod_count_query):
                production_rows = row["cnt"]
                break
        except Exception:
            production_rows = 0

        if production_rows > 0 and not rebuild_mode:
            # Step 4a: Ensure production has row_hash for comparison
            add_hash_column(
                client, main_table, primary_keys, exclude_fields=exclude_from_hash
            )

            # Step 4b: Generate changelog (ADDED/REMOVED/UPDATED)
            logger.info(
                f"  Generating changelog ({stats['staging_rows']} staging vs {production_rows} production rows)"
            )
            changelog_stats = _generate_snapshot_changelog(
                client,
                staging_table,
                main_table,
                primary_keys,
                exclude_from_hash,
                execution_id,
            )
            stats["rows_inserted"] = changelog_stats.get("rows_added", 0)
            stats["rows_updated"] = changelog_stats.get("rows_updated", 0)
            stats["rows_removed"] = changelog_stats.get("rows_removed", 0)
            stats["rows_unchanged"] = changelog_stats.get("rows_unchanged", 0)

            # Step 4c: Archive removed records (full row snapshots to _removed table)
            if changelog_stats.get("rows_removed", 0) > 0:
                try:
                    archive_removed_records(
                        client, staging_table, main_table, primary_keys, execution_id
                    )
                except Exception as e:
                    logger.warning(
                        f"  Archive removed records failed (continuing): {e}"
                    )
        else:
            # First run or rebuild: all staging rows are new
            stats["rows_inserted"] = stats["staging_rows"]
            logger.info(
                f"  First load or rebuild - all {stats['staging_rows']} rows are new"
            )

        # Step 5: Replace production table (TRUNCATE + INSERT)
        logger.info(f"  Replacing production table ({stats['staging_rows']} rows)")

        # Get column mapping for INSERT
        staging_ref = client.get_table(staging_table)
        staging_columns = [f.name for f in staging_ref.schema]

        try:
            main_ref = client.get_table(main_table)
            main_schema = {f.name: f.field_type for f in main_ref.schema}
        except NotFound:
            main_schema = {}

        # Build column lists - exclude loaded_at/updated_at (set explicitly)
        columns_for_insert = [
            c for c in staging_columns if c not in ["loaded_at", "updated_at"]
        ]

        def normalize_bq_type(type_name):
            if type_name == "FLOAT":
                return "FLOAT64"
            elif type_name == "INTEGER":
                return "INT64"
            elif type_name == "BOOLEAN":
                return "BOOL"
            return type_name

        if main_schema:
            select_expr = ", ".join(
                [
                    (
                        f"SAFE_CAST(`{col}` AS {normalize_bq_type(main_schema[col])}) AS `{col}`"
                        if col in main_schema
                        else f"`{col}`"
                    )
                    for col in columns_for_insert
                ]
            )
        else:
            select_expr = ", ".join([f"`{col}`" for col in columns_for_insert])

        insert_cols = ", ".join(
            [f"`{col}`" for col in columns_for_insert] + ["`loaded_at`", "`updated_at`"]
        )

        # TRUNCATE production
        truncate_query = f"TRUNCATE TABLE `{main_table}`"
        client.query(truncate_query).result()
        logger.info(f"  Truncated production table")

        # INSERT from staging with system metadata
        insert_query = f"""
        INSERT INTO `{main_table}` ({insert_cols})
        SELECT
            {select_expr},
            CURRENT_TIMESTAMP() AS `loaded_at`,
            CURRENT_TIMESTAMP() AS `updated_at`
        FROM `{staging_table}`
        """
        job = client.query(insert_query)
        job.result()
        rows_loaded = job.num_dml_affected_rows or 0
        logger.info(f"  Loaded {rows_loaded} rows into production")

        # Step 6: Get final row count
        for row in client.query(f"SELECT COUNT(*) as cnt FROM `{main_table}`"):
            stats["total_rows"] = row["cnt"]
            break

        # Step 7: Handle staging tables
        # Archive MUST run before truncate — otherwise the archive is empty.
        if config.get("archive_staging", False):
            archive_staging_table(
                client, staging_table, config=config, execution_id=execution_id
            )

        if config.get("truncate_staging", False):
            drop_staging_tables(client, staging_table)
            logger.info("  Staging tables dropped (external + regular)")

        # Step 8: Log change statistics to monitoring
        table_parts = main_table.split(".")
        log_dataset = table_parts[-2] if len(table_parts) > 1 else "unknown"
        log_table = table_parts[-1]
        log_data_changes(
            client=client,
            dataset=log_dataset,
            table_name=log_table,
            rows_inserted=stats["rows_inserted"],
            rows_updated=stats["rows_updated"],
            rows_removed=stats["rows_removed"],
            rows_total=stats["total_rows"],
            execution_id=execution_id,
        )

        stats["success"] = True
        return stats

    except Exception as e:
        logger.error(f"  Snapshot replace error: {e}")
        import traceback

        logger.error(traceback.format_exc())
        stats["success"] = False
        stats["error"] = str(e)
        return stats


def _apply_table_options(table, partitioning, clustering, source):
    """Apply configured partitioning, clustering, and lineage labels to a new
    bigquery.Table BEFORE create_table. All are best-effort/no-op when absent.

    partitioning: {"field": <col>, "type": "DAY"|"MONTH"|"YEAR"|"HOUR"} (None -> none).
    clustering: list of column names (None/[] -> none; BQ caps at 4).
    source: tag the table with source/pipeline labels for lineage/cost attribution.
    """
    if partitioning and isinstance(partitioning, dict) and partitioning.get("field"):
        ptype = str(partitioning.get("type", "DAY")).upper()
        type_map = {
            "DAY": bigquery.TimePartitioningType.DAY,
            "MONTH": bigquery.TimePartitioningType.MONTH,
            "YEAR": bigquery.TimePartitioningType.YEAR,
            "HOUR": bigquery.TimePartitioningType.HOUR,
        }
        table.time_partitioning = bigquery.TimePartitioning(
            type_=type_map.get(ptype, bigquery.TimePartitioningType.DAY),
            field=partitioning["field"],
        )
    if clustering:
        # BigQuery allows up to 4 clustering columns; keep config order.
        table.clustering_fields = [str(c) for c in clustering][:4]
    # Lineage labels (BigQuery label values are lowercase alnum/_/-; sanitize).
    labels = {"pipeline": "bi"}
    if source:
        labels["source"] = re.sub(r"[^a-z0-9_-]", "_", str(source).lower())[:63]
    table.labels = labels


def ensure_system_metadata_fields(
    client: bigquery.Client,
    staging_table: str,
    main_table: str,
    source: str = None,
    execution_id: str = None,
    rebuild_mode: bool = False,
    partitioning: dict = None,
    clustering: list = None,
):
    """
    Ensure production table exists with all 6 required system metadata fields.

    This function has ONE responsibility: ensure system metadata fields exist in production.
    Business field schema evolution is handled separately by apply_schema_evolution().

    System metadata fields (always added):
    - row_hash: STRING - Hash of business fields for change detection
    - loaded_at: TIMESTAMP - Immutable timestamp when row was first inserted
    - updated_at: TIMESTAMP - Last update timestamp
    - source: STRING - Source system identifier
    - execution_id: STRING - Pipeline execution ID
    - extracted_at: TIMESTAMP - When data was extracted from source (set by extractor)

    When creating new table:
    - Copies business fields from staging (or YAML if available)
    - Adds all 6 system metadata fields

    When table exists:
    - ONLY adds missing system metadata fields
    - Does NOT add business fields (that's apply_schema_evolution's job)

    Args:
        client: BigQuery client
        staging_table: Staging table ID (for schema reference when creating table)
        main_table: Production table ID
        source: Source name for YAML schema lookup (optional)
        execution_id: Execution ID for tracking (optional)
        rebuild_mode: Whether this is a rebuild operation (optional)
    """
    # Get staging table schema
    staging_ref = client.get_table(staging_table)
    staging_schema = list(staging_ref.schema)
    staging_field_names = {field.name for field in staging_schema}

    # Import schema evolution for tracking (optional)
    try:
        from shared.schema_evolution import log_schema_change
    except ImportError:
        log_schema_change = None

    try:
        # Check if main table exists
        main_ref = client.get_table(main_table)
        main_field_names = {field.name for field in main_ref.schema}

        # Extract table name and dataset from full table ID
        table_parts = main_table.split(".")
        dataset_name = table_parts[1] if len(table_parts) > 1 else "unknown"
        table_name = table_parts[2] if len(table_parts) > 2 else table_parts[-1]

        # Add missing SYSTEM METADATA FIELDS to existing main table
        # NOTE: Business fields are added by apply_schema_evolution() separately
        missing_columns = []

        # Check for ALL 6 system metadata fields (ONLY - no business fields)
        if "row_hash" not in main_field_names:
            missing_columns.append(bigquery.SchemaField("row_hash", "STRING"))

        if "loaded_at" not in main_field_names:
            missing_columns.append(bigquery.SchemaField("loaded_at", "TIMESTAMP"))

        if "updated_at" not in main_field_names:
            missing_columns.append(bigquery.SchemaField("updated_at", "TIMESTAMP"))

        if "source" not in main_field_names:
            missing_columns.append(bigquery.SchemaField("source", "STRING"))

        if "execution_id" not in main_field_names:
            missing_columns.append(bigquery.SchemaField("execution_id", "STRING"))

        if "extracted_at" not in main_field_names:
            missing_columns.append(bigquery.SchemaField("extracted_at", "TIMESTAMP"))

        # Add missing columns one by one
        if missing_columns:
            for field in missing_columns:
                field_type = field.field_type
                nullable = "NULL" if field.mode != "REQUIRED" else "NOT NULL"

                # Retry ALTER TABLE with exponential backoff to handle concurrent schema modifications
                # BigQuery's "rate limit" error is misleading - it's actually a concurrent modification conflict
                max_retries = 3
                base_delay = 1.0
                added = False

                for attempt in range(max_retries):
                    try:
                        alter_query = f"""
                        ALTER TABLE `{main_table}`
                        ADD COLUMN IF NOT EXISTS `{field.name}` {field_type}
                        """
                        client.query(alter_query).result()
                        logger.info(f"  Added column {field.name} to main table")
                        added = True

                        # Log schema change (if tracking enabled)
                        if log_schema_change and source:
                            log_schema_change(
                                client=client,
                                source=source,
                                table_name=table_name,
                                dataset_name=dataset_name,
                                change_type="ADD_COLUMN",
                                field_name=field.name,
                                field_type_after=field_type,
                                field_mode_after=field.mode or "NULLABLE",
                                execution_id=execution_id,
                                applied=True,
                            )
                        break  # Success - exit retry loop

                    except Exception as e:
                        error_msg = str(e).lower()
                        if "already exists" in error_msg or "duplicate" in error_msg:
                            # Column exists - that's fine
                            logger.debug(
                                f"  Column {field.name} already exists in main table"
                            )
                            added = True
                            break
                        elif (
                            "exceeded rate limit" in error_msg
                            or "too many table update" in error_msg
                        ):
                            # Concurrent schema modification conflict
                            if attempt < max_retries - 1:
                                retry_delay = base_delay * (2**attempt)
                                logger.debug(
                                    f"  Concurrent modification on {field.name} - retrying in {retry_delay:.1f}s"
                                )
                                time.sleep(retry_delay)

                                # Re-check if column was added by concurrent process
                                try:
                                    fresh_table = client.get_table(main_table)
                                    if field.name in [
                                        f.name for f in fresh_table.schema
                                    ]:
                                        logger.debug(
                                            f"  Column {field.name} added by concurrent process"
                                        )
                                        added = True
                                        break
                                except Exception:
                                    pass  # Continue retry if re-check fails
                            else:
                                # Final attempt - check one more time
                                try:
                                    final_table = client.get_table(main_table)
                                    if field.name in [
                                        f.name for f in final_table.schema
                                    ]:
                                        logger.debug(
                                            f"  Column {field.name} exists after concurrent modifications"
                                        )
                                        added = True
                                        break
                                except Exception:
                                    pass
                                logger.warning(
                                    f"  Could not add {field.name} after {max_retries} attempts due to concurrent modifications"
                                )
                                break
                        else:
                            # Other errors - log and exit retry loop
                            if field.field_type in ["RECORD", "REPEATED"]:
                                logger.warning(
                                    f"  Warning: Could not add complex field {field.name}: {e}"
                                )
                            else:
                                logger.warning(
                                    f"  Warning: Could not add field {field.name}: {e}"
                                )
                            break

    except Exception as e:
        # Main table doesn't exist - create it with YAML schema (if available)

        # TRY TO GET YAML SCHEMA FIRST
        yaml_schema = None
        if source:
            # Extract table name from main_table (remove dataset prefix)
            table_parts = main_table.split(".")
            table_name = table_parts[2] if len(table_parts) > 2 else table_parts[-1]
            # Remove source prefix (e.g., wordpress_users -> users)
            clean_table_name = table_name.replace(f"{source}_", "")

            # Read YAML schema
            try:
                from shared.gcs_pipeline import read_yaml_schema

                config_path = f"configs/{source}.yaml"
                yaml_schema = read_yaml_schema(config_path, clean_table_name)

                if yaml_schema:
                    logger.info(
                        f"  Using YAML schema for production table ({len(yaml_schema)} columns defined)"
                    )
            except Exception as yaml_err:
                logger.debug(f"  Could not read YAML schema: {yaml_err}")

        # BUILD SCHEMA: YAML types override staging types
        if yaml_schema:
            schema = []
            type_overrides = 0
            yaml_only_fields = 0

            # Step 1: Process all fields in staging_schema
            # Use YAML types for columns that are defined in YAML
            staging_field_names = set()
            for field in staging_schema:
                staging_field_names.add(field.name)
                if field.name in yaml_schema:
                    # Use YAML type (override staging type)
                    yaml_type = yaml_schema[field.name]
                    if yaml_type != field.field_type:
                        logger.debug(
                            f"    {field.name}: {field.field_type} (staging) -> {yaml_type} (YAML)"
                        )
                        type_overrides += 1
                    schema.append(
                        bigquery.SchemaField(
                            field.name, yaml_type, mode=field.mode or "NULLABLE"
                        )
                    )
                else:
                    # Use staging type (not defined in YAML)
                    schema.append(field)

            # Step 2: Add YAML-only fields (fields in YAML but not in staging)
            # This is critical for system fields (row_hash, source, loaded_at, etc.)
            # which are computed later and not present in staging at table creation time
            for field_name, field_type in yaml_schema.items():
                if field_name not in staging_field_names:
                    schema.append(
                        bigquery.SchemaField(field_name, field_type, mode="NULLABLE")
                    )
                    yaml_only_fields += 1
                    logger.debug(f"    {field_name}: {field_type} (YAML-only field)")

            if type_overrides > 0:
                logger.info(
                    f"  Applied {type_overrides} type overrides from YAML schema"
                )
            if yaml_only_fields > 0:
                logger.info(
                    f"  Added {yaml_only_fields} YAML-only fields (e.g., system fields)"
                )
        else:
            # No YAML schema - fall back to staging schema
            logger.info(
                f"  No YAML schema found - using staging schema (pandas inference)"
            )
            logger.info(
                f"  Tip: Define schema in configs/{source}.yaml to ensure correct types"
            )
            schema = staging_schema.copy()

        # Add ALL 6 system metadata fields if not present in staging
        # These should match defaults.yaml exactly
        schema_field_names = {field.name for field in schema}

        if "row_hash" not in schema_field_names:
            schema.append(bigquery.SchemaField("row_hash", "STRING"))

        if "loaded_at" not in schema_field_names:
            schema.append(bigquery.SchemaField("loaded_at", "TIMESTAMP"))

        if "updated_at" not in schema_field_names:
            schema.append(bigquery.SchemaField("updated_at", "TIMESTAMP"))

        if "source" not in schema_field_names:
            schema.append(bigquery.SchemaField("source", "STRING"))

        if "execution_id" not in schema_field_names:
            schema.append(bigquery.SchemaField("execution_id", "STRING"))

        if "extracted_at" not in schema_field_names:
            schema.append(bigquery.SchemaField("extracted_at", "TIMESTAMP"))

        # Create the table, applying configured partitioning / clustering /
        # lineage labels at CREATE time (BigQuery cannot add partitioning to an
        # existing table, so it must be set here when the table is first made).
        table = bigquery.Table(main_table, schema=schema)
        _apply_table_options(table, partitioning, clustering, source)
        table = client.create_table(table)
        logger.info(f"  Created production table: {main_table}")

        # REBUILD MODE: SKIP YAML auto-update
        # When user specifies --rebuild, they want to use their explicitly defined YAML schema
        # Auto-updating YAML from the production table (which was just created from YAML)
        # creates a circular dependency and perpetuates bad schemas across runs
        # Therefore, in rebuild mode, trust the user's YAML and don't overwrite it
        if rebuild_mode and source:
            logger.info(
                f"REBUILD MODE: Using YAML schema as authoritative source (skipping auto-update)"
            )
            logger.info(f"  Production table created from configs/{source}.yaml")
            logger.info(f"  To update YAML from production, run without --rebuild flag")


import threading

_alter_table_locks = {}
_alter_table_locks_lock = threading.Lock()


def add_hash_column(
    client: bigquery.Client,
    table_name: str,
    primary_keys: List[str],
    exclude_fields: Optional[List[str]] = None,
):
    # Add row_hash column to staging table if it doesn't exist
    # row_hash should include ALL business data fields, excluding only tracking/metadata fields
    #
    # IMPORTANT: Hash calculation uses TIMESTAMP_TRUNC to minute precision for consistency
    # This ensures timestamps with different seconds/microseconds produce the same hash
    # NOTE: If upgrading from a previous version, run --rebuild on all tables once to regenerate hashes

    # Retry logic to handle BigQuery eventual consistency after table creation
    # When a table is dropped and immediately recreated, there can be a brief window
    # where BigQuery metadata hasn't fully propagated across all nodes
    import time

    max_retries = 3
    retry_delay = 2  # seconds

    # Get or create a lock for this specific table to prevent concurrent ALTER TABLE operations
    # This prevents "rate limit exceeded" errors caused by multiple workers trying to ALTER simultaneously
    with _alter_table_locks_lock:
        if table_name not in _alter_table_locks:
            _alter_table_locks[table_name] = threading.Lock()
        table_lock = _alter_table_locks[table_name]

    for attempt in range(max_retries):
        try:
            table_ref = client.get_table(table_name)
            field_names = [field.name for field in table_ref.schema]
            break  # Success - table exists
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Table {table_name} not ready, retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    f"Table {table_name} not found after {max_retries} attempts: {e}"
                )
                raise

    if "row_hash" not in field_names:
        # Use table-specific lock to serialize ALTER TABLE operations
        # This prevents multiple workers from trying to ALTER the same table simultaneously
        with table_lock:
            # Re-check after acquiring lock - another worker might have added the column
            try:
                table_ref = client.get_table(table_name)
                current_field_names = [field.name for field in table_ref.schema]
                if "row_hash" in current_field_names:
                    logger.debug(
                        f"row_hash column already exists in {table_name} (added by concurrent worker)"
                    )
                    return  # Column exists now, skip ALTER
            except Exception:
                pass  # Continue with ALTER if re-check fails

            # Retry ALTER TABLE with exponential backoff to handle concurrent schema modifications
            # Note: "rate limit" error from BigQuery is misleading - it's actually a concurrent
            # ALTER TABLE conflict, not a true rate limit. When multiple processes try to modify
            # the same table's schema simultaneously, BigQuery serializes them and returns this error.
            max_alter_retries = 3
            base_delay = 1.0  # seconds

            for alter_attempt in range(max_alter_retries):
                try:
                    query = f"ALTER TABLE `{table_name}` ADD COLUMN IF NOT EXISTS `row_hash` STRING"
                    client.query(query).result()
                    logger.debug(f"Added row_hash column to {table_name}")
                    break  # Success - exit retry loop
                except Exception as e:
                    error_msg = str(e).lower()

                    # Column already exists - success
                    if "already exists" in error_msg or "duplicate" in error_msg:
                        logger.debug(
                            f"row_hash column already exists in {table_name} (concurrent add or cache delay)"
                        )
                        break

                    # Concurrent schema modification conflict (misleadingly called "rate limit")
                    elif (
                        "exceeded rate limit" in error_msg
                        or "too many table update" in error_msg
                    ):
                        if alter_attempt < max_alter_retries - 1:
                            # Wait for concurrent ALTER to complete, then re-check if column exists
                            retry_delay = base_delay * (2**alter_attempt)
                            logger.debug(
                                f"Concurrent schema modification on {table_name} - retrying in {retry_delay:.1f}s (attempt {alter_attempt + 1}/{max_alter_retries})"
                            )
                            time.sleep(retry_delay)

                            # Re-check if column was added by concurrent process
                            try:
                                fresh_table = client.get_table(table_name)
                                if "row_hash" in [f.name for f in fresh_table.schema]:
                                    logger.debug(
                                        f"row_hash column added by concurrent process"
                                    )
                                    return  # Column exists - we're done
                            except Exception:
                                pass  # Continue retry if re-check fails
                        else:
                            # Final attempt - do one more check before giving up
                            try:
                                final_check = client.get_table(table_name)
                                if "row_hash" in [f.name for f in final_check.schema]:
                                    logger.debug(
                                        f"row_hash column exists after concurrent modifications"
                                    )
                                    return
                            except Exception:
                                pass
                            logger.warning(
                                f"Could not add row_hash to {table_name} after {max_alter_retries} attempts due to concurrent modifications - column may already exist"
                            )
                            break
                    else:
                        # Unexpected error - re-raise
                        raise

    # Default excluded fields (our tracking/metadata columns)
    default_excluded = {
        "loaded_at",
        "extracted_at",
        "source",
        "execution_id",
        "updated_at",
        "row_hash",
    }

    # Merge with config-specified excluded fields
    if exclude_fields:
        default_excluded.update(exclude_fields)

    # Build hash from ALL fields except excluded ones
    hash_fields = [f for f in field_names if f not in default_excluded]

    if not hash_fields:
        logger.warning(f"No fields available for hash calculation in {table_name}")
        return

    # Sort fields for consistent hashing
    hash_fields.sort()

    # Get field types to handle TIMESTAMP fields specially
    field_types = {field.name: field.field_type for field in table_ref.schema}

    # Build the concatenation with COALESCE to handle NULLs
    # For TIMESTAMP fields, truncate to minute precision before casting to STRING
    # This ensures timestamps with different seconds/microseconds hash the same
    hash_parts = []
    for field in hash_fields:
        field_type = field_types.get(field, "STRING")
        if field_type == "TIMESTAMP" or field_type == "DATETIME":
            # Truncate TIMESTAMP to minute precision, then cast to STRING
            # TIMESTAMP_TRUNC removes seconds and microseconds before conversion
            hash_parts.append(
                f"COALESCE(CAST(TIMESTAMP_TRUNC(`{field}`, MINUTE) AS STRING), '')"
            )
        else:
            # Non-timestamp fields: cast directly to STRING
            hash_parts.append(f"COALESCE(CAST(`{field}` AS STRING), '')")

    hash_concat = " || '|' || ".join(hash_parts)

    # Debug: Log which fields are using TIMESTAMP_TRUNC
    timestamp_fields = [
        field
        for field in hash_fields
        if field_types.get(field) in ["TIMESTAMP", "DATETIME"]
    ]
    if timestamp_fields:
        logger.debug(
            f"  Hash calculation: using TIMESTAMP_TRUNC for {len(timestamp_fields)} timestamp fields: {timestamp_fields}"
        )

    # Update hash values using FARM_FINGERPRINT (faster and more efficient than MD5)
    update_query = f"""
    UPDATE `{table_name}`
    SET `row_hash` = CAST(FARM_FINGERPRINT({hash_concat}) AS STRING)
    WHERE `row_hash` IS NULL
    """

    job = client.query(update_query)
    job.result()

    if job.num_dml_affected_rows and job.num_dml_affected_rows > 0:
        logger.info(
            f"  Added hash to {job.num_dml_affected_rows} rows using {len(hash_fields)} fields"
        )


def archive_staging_table(
    client: bigquery.Client,
    staging_table: str,
    config: Optional[Dict[str, Any]] = None,
    execution_id: Optional[str] = None,
):
    # Archive staging table via the staging lifecycle (zero-copy snapshot with
    # TTL, CTAS fallback). With config, honors pipeline.archive_dataset /
    # pipeline.staging.archive.ttl_days and writes a manifest row; without it,
    # falls back to the legacy {staging_dataset}_archive location with the
    # default TTL so old call sites keep working but archives now expire.
    from shared.staging_lifecycle import (
        DEFAULT_ARCHIVE_TTL_DAYS,
        archive_staging_table_snapshot,
        get_archive_settings,
        write_staging_archive_manifest,
    )

    clean_id = staging_table.replace("`", "")
    project, dataset, table = clean_id.split(".")
    # facebook_staging -> facebook (used when config lacks a usable source name)
    dataset_source = (
        dataset[: -len("_staging")] if dataset.endswith("_staging") else dataset
    )

    if config:
        settings = get_archive_settings(config, default_source_name=dataset_source)
        archive_dataset = settings["archive_dataset"]
        ttl_days = settings["ttl_days"]
        source_field = config.get("source")
        if isinstance(source_field, dict):
            source_name = source_field.get("name") or dataset_source
        elif isinstance(source_field, str) and source_field:
            source_name = source_field
        else:
            source_name = dataset_source
    else:
        archive_dataset = f"{dataset_source}_archive"
        ttl_days = DEFAULT_ARCHIVE_TTL_DAYS
        source_name = dataset_source

    try:
        archived = archive_staging_table_snapshot(
            client,
            clean_id,
            archive_dataset=archive_dataset,
            execution_id=execution_id,
            ttl_days=ttl_days,
        )
    except Exception as e:
        logger.warning(f"  Warning: Could not archive staging table: {e}")
        return

    if archived:
        try:
            write_staging_archive_manifest(
                client,
                [
                    {
                        "execution_id": execution_id,
                        "job_id": None,
                        "source": source_name,
                        "group_name": None,
                        "table_name": table,
                        "staging_table": clean_id,
                        "archive_table": archived["archive_table"],
                        "gcs_parquet_uris": [],
                        "row_count": archived["row_count"],
                        "archive_method": archived["archive_method"],
                    }
                ],
                ttl_days=ttl_days,
            )
        except Exception as e:
            logger.warning(f"  Could not write staging archive manifest: {e}")


#
# Batch processing functionality (from bigquery_batch.py)
#


def batch_load_to_bigquery(
    client: bigquery.Client,
    data: List[Dict[str, Any]],
    table_id: str,
    batch_size: int = 10000,
    write_disposition: str = "WRITE_APPEND",
    schema: Optional[List[bigquery.SchemaField]] = None,
) -> Dict[str, Any]:
    # Load data to BigQuery in optimized batches

    # Track statistics
    stats = {"total_rows": len(data), "batches": 0, "errors": 0, "bytes_processed": 0}

    if not data:
        logger.warning(f"No data to load to {table_id}")
        return stats

    # Calculate optimal batch size based on data size
    avg_row_size = estimate_row_size(data[0])
    optimal_batch_size = min(
        batch_size,
        max(1000, int(10 * 1024 * 1024 / avg_row_size)),  # Aim for ~10MB batches
    )

    logger.info(
        f"Loading {len(data)} rows to {table_id} in batches of {optimal_batch_size}"
    )

    # Process in batches
    for i in range(0, len(data), optimal_batch_size):
        batch = data[i : i + optimal_batch_size]
        batch_num = stats["batches"] + 1

        try:
            # Load batch
            load_job = load_batch(
                client=client,
                batch=batch,
                table_id=table_id,
                batch_num=batch_num,
                write_disposition=write_disposition,
                schema=schema,
            )

            # Update statistics
            stats["batches"] += 1
            if load_job.error_result:
                stats["errors"] += 1
            if hasattr(load_job, "output_rows") and load_job.output_rows:
                stats["output_rows"] = (
                    stats.get("output_rows", 0) + load_job.output_rows
                )
            if (
                hasattr(load_job, "total_bytes_processed")
                and load_job.total_bytes_processed
            ):
                stats["bytes_processed"] += load_job.total_bytes_processed

            logger.info(f"Batch {batch_num}: Loaded {len(batch)} rows to {table_id}")

        except Exception as e:
            logger.error(f"Error loading batch {batch_num} to {table_id}: {e}")
            stats["errors"] += 1

    return stats


def load_batch(
    client: bigquery.Client,
    batch: List[Dict[str, Any]],
    table_id: str,
    batch_num: int,
    write_disposition: str = "WRITE_APPEND",
    schema: Optional[List[Dict[str, str]]] = None,
) -> bigquery.LoadJob:
    # Load a single batch to BigQuery

    # Determine best loading method based on batch size
    if len(batch) < 500:
        # For small batches, use direct API
        return load_batch_direct(client, batch, table_id, write_disposition, schema)
    else:
        # For larger batches, use file-based loading
        return load_batch_from_file(
            client, batch, table_id, batch_num, write_disposition, schema
        )


def load_batch_direct(
    client: bigquery.Client,
    batch: List[Dict[str, Any]],
    table_id: str,
    write_disposition: str = "WRITE_APPEND",
    schema: Optional[List[bigquery.SchemaField]] = None,
) -> bigquery.LoadJob:
    # Load batch directly via API

    # Configure job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
        autodetect=schema is None,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    # Load data
    load_job = client.load_table_from_json(batch, table_id, job_config=job_config)

    # Wait for job to complete
    load_job.result()

    return load_job


def load_batch_from_file(
    client: bigquery.Client,
    batch: List[Dict[str, Any]],
    table_id: str,
    batch_num: int,
    write_disposition: str = "WRITE_APPEND",
    schema: Optional[List[bigquery.SchemaField]] = None,
) -> bigquery.LoadJob:
    # Load batch from temporary file for better performance with large batches

    # Create temporary file
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".jsonl", delete=False
    ) as temp_file:
        # Write batch to file
        for row in batch:
            temp_file.write(json.dumps(row) + "\n")
        temp_file_path = temp_file.name

    try:
        # Configure job
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema=schema,
            autodetect=schema is None,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

        # Load from file
        with open(temp_file_path, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )

            # Wait for job to complete
            load_job.result()

        return load_job

    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file_path)
        except Exception as e:
            logger.debug(f"Could not delete temporary file {temp_file_path}: {e}")


def estimate_row_size(row: Dict[str, Any]) -> int:
    # Estimate the size of a row in bytes
    return len(json.dumps(row))


def process_dataframe_in_batches(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    batch_size: int = 10000,
    write_disposition: str = "WRITE_APPEND",
) -> Dict[str, Any]:
    # Process a DataFrame in batches

    # Convert DataFrame to list of dicts
    records = df.to_dict("records")

    # Load in batches
    return batch_load_to_bigquery(
        client=client,
        data=records,
        table_id=table_id,
        batch_size=batch_size,
        write_disposition=write_disposition,
    )


def create_table_if_not_exists(
    client: bigquery.Client,
    table_id: str,
    schema: Optional[List[bigquery.SchemaField]] = None,
    partition_field: Optional[str] = None,
    clustering_fields: Optional[List[str]] = None,
) -> None:
    # Create table if it doesn't exist

    try:
        # Check if table exists
        client.get_table(table_id)
    except NotFound:
        # Create table
        table = bigquery.Table(table_id, schema=schema)

        # Configure partitioning
        if partition_field:
            # For field-based partitioning (when schema is None), don't set partitioning yet
            # Let BigQuery auto-detect schema first, then let load job set partitioning
            if not schema:
                logger.info(f"Auto-detecting schema for partitioned table: {table_id}")
            else:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field=partition_field
                )

        # Configure clustering
        if clustering_fields:
            table.clustering_fields = clustering_fields

        # Create table
        client.create_table(table)
        logger.info(f"Created table {table_id}")


def generate_optimized_schema(data: List[Dict[str, Any]]) -> List[bigquery.SchemaField]:
    # Generate optimized schema from data

    # Sample data
    sample = data[0] if data else {}

    # Generate schema
    schema = []
    for key, value in sample.items():
        field_type = infer_field_type(value)
        schema.append(bigquery.SchemaField(key, field_type))

    return schema


def infer_field_type(value: Any) -> str:
    # Infer BigQuery field type from value

    if value is None:
        return "STRING"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, dict):
        return "RECORD"
    elif isinstance(value, list):
        return "REPEATED"
    elif isinstance(value, str):
        # Check if it's a date/timestamp
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return "DATE"
        elif "T" in value and ":" in value and len(value) >= 19:
            return "TIMESTAMP"
        else:
            return "STRING"
    else:
        return "STRING"


#
# Data Quality Functions
#


def run_data_quality_checks(
    client: bigquery.Client, table_id: str, quality_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Run data quality checks on a BigQuery table based on YAML configuration.

    Args:
        client: BigQuery client instance
        table_id: Full table ID (project.dataset.table)
        quality_config: Data quality configuration from YAML

    Returns:
        Dictionary with quality check results
    """
    results = {
        "table_id": table_id,
        "checks_passed": 0,
        "checks_failed": 0,
        "total_checks": 0,
        "details": [],
    }

    try:
        # Get table schema for column validation
        table_ref = client.get_table(table_id)
        available_columns = [field.name for field in table_ref.schema]

        # Run null checks
        if "null_checks" in quality_config:
            null_results = check_nulls(
                client, table_id, quality_config["null_checks"], available_columns
            )
            results["details"].extend(null_results)
            results["checks_passed"] += sum(1 for r in null_results if r["passed"])
            results["checks_failed"] += sum(1 for r in null_results if not r["passed"])
            results["total_checks"] += len(null_results)

        # Run duplicate checks
        if "duplicate_checks" in quality_config:
            dup_results = check_duplicates(
                client, table_id, quality_config["duplicate_checks"]
            )
            results["details"].extend(dup_results)
            results["checks_passed"] += sum(1 for r in dup_results if r["passed"])
            results["checks_failed"] += sum(1 for r in dup_results if not r["passed"])
            results["total_checks"] += len(dup_results)

        # Run freshness checks
        if "freshness_checks" in quality_config:
            fresh_results = check_freshness(
                client, table_id, quality_config["freshness_checks"]
            )
            results["details"].extend(fresh_results)
            results["checks_passed"] += sum(1 for r in fresh_results if r["passed"])
            results["checks_failed"] += sum(1 for r in fresh_results if not r["passed"])
            results["total_checks"] += len(fresh_results)

        # Run row count checks
        if "row_count_checks" in quality_config:
            count_results = check_row_counts(
                client, table_id, quality_config["row_count_checks"]
            )
            results["details"].extend(count_results)
            results["checks_passed"] += sum(1 for r in count_results if r["passed"])
            results["checks_failed"] += sum(1 for r in count_results if r["passed"])
            results["total_checks"] += len(count_results)

        logger.info(
            f"Data quality checks completed: {results['checks_passed']}/{results['total_checks']} passed"
        )
        return results

    except Exception as e:
        logger.error(f"Data quality check failed for {table_id}: {e}")
        return results


def check_nulls(
    client: bigquery.Client,
    table_id: str,
    null_config: Dict[str, Any],
    available_columns: List[str],
) -> List[Dict[str, Any]]:
    # Check null percentage in specified columns
    results = []

    for column_config in null_config:
        column = column_config.get("column")
        max_percent = column_config.get("max_percent", 5.0)

        if column not in available_columns:
            results.append(
                {
                    "check_type": "null_check",
                    "column": column,
                    "passed": False,
                    "message": f"Column {column} not found in table",
                }
            )
            continue

        sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNTIF(`{column}` IS NULL) as null_count,
            ROUND(COUNTIF(`{column}` IS NULL) / COUNT(*) * 100, 2) as null_percent
        FROM `{table_id}`
        """

        try:
            result = list(client.query(sql).result())[0]
            passed = result.null_percent <= max_percent

            results.append(
                {
                    "check_type": "null_check",
                    "column": column,
                    "passed": passed,
                    "null_percent": float(result.null_percent),
                    "max_percent": max_percent,
                    "total_rows": result.total_rows,
                    "null_count": result.null_count,
                    "message": f"{column}: {result.null_percent}% nulls (max: {max_percent}%)",
                }
            )

            if passed:
                logger.info(f"{table_id}.{column}: {result.null_percent}% nulls")
            else:
                logger.error(
                    f"{table_id}.{column}: {result.null_percent}% nulls (max: {max_percent}%)"
                )

        except Exception as e:
            logger.error(f"Error checking nulls for {table_id}.{column}: {e}")
            results.append(
                {
                    "check_type": "null_check",
                    "column": column,
                    "passed": False,
                    "message": f"Error: {e}",
                }
            )

    return results


def check_duplicates(
    client: bigquery.Client, table_id: str, duplicate_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    # Check for duplicate records based on key columns
    results = []

    for dup_config in duplicate_config:
        key_columns = dup_config.get("key_columns", [])
        max_duplicates = dup_config.get("max_duplicates", 0)

        if not key_columns:
            continue

        keys = ", ".join([f"`{c}`" for c in key_columns])
        sql = f"""
        SELECT COUNT(*) as dup_count
        FROM (
            SELECT {keys}, COUNT(*) as cnt
            FROM `{table_id}`
            GROUP BY {keys}
            HAVING cnt > 1
        )
        """

        try:
            result = list(client.query(sql).result())[0]
            passed = result.dup_count <= max_duplicates

            results.append(
                {
                    "check_type": "duplicate_check",
                    "key_columns": key_columns,
                    "passed": passed,
                    "duplicate_count": result.dup_count,
                    "max_duplicates": max_duplicates,
                    "message": f"Found {result.dup_count} duplicate groups (max: {max_duplicates})",
                }
            )

            if passed:
                logger.info(f"{table_id}: {result.dup_count} duplicate groups")
            else:
                logger.error(
                    f"{table_id}: {result.dup_count} duplicate groups (max: {max_duplicates})"
                )

        except Exception as e:
            logger.error(f"Error checking duplicates for {table_id}: {e}")
            results.append(
                {
                    "check_type": "duplicate_check",
                    "key_columns": key_columns,
                    "passed": False,
                    "message": f"Error: {e}",
                }
            )

    return results


def check_freshness(
    client: bigquery.Client, table_id: str, freshness_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    # Check data freshness based on timestamp columns
    results = []

    for fresh_config in freshness_config:
        timestamp_column = fresh_config.get("timestamp_column", "updated_at")
        max_age_hours = fresh_config.get("max_age_hours", 24)

        sql = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), `{timestamp_column}`, HOUR) <= {max_age_hours}) as fresh_rows,
            MAX(`{timestamp_column}`) as latest_timestamp,
            MIN(`{timestamp_column}`) as earliest_timestamp
        FROM `{table_id}`
        WHERE `{timestamp_column}` IS NOT NULL
        """

        try:
            result = list(client.query(sql).result())[0]
            fresh_percent = (
                (result.fresh_rows / result.total_rows * 100)
                if result.total_rows > 0
                else 0
            )
            passed = fresh_percent >= 80  # At least 80% fresh data

            results.append(
                {
                    "check_type": "freshness_check",
                    "timestamp_column": timestamp_column,
                    "passed": passed,
                    "fresh_percent": fresh_percent,
                    "max_age_hours": max_age_hours,
                    "total_rows": result.total_rows,
                    "fresh_rows": result.fresh_rows,
                    "latest_timestamp": (
                        str(result.latest_timestamp)
                        if result.latest_timestamp
                        else None
                    ),
                    "message": f"{fresh_percent:.1f}% data is fresh (max age: {max_age_hours}h)",
                }
            )

            if passed:
                logger.info(f"{table_id}: {fresh_percent:.1f}% fresh data")
            else:
                logger.error(
                    f"{table_id}: {fresh_percent:.1f}% fresh data (threshold: 80%)"
                )

        except Exception as e:
            logger.error(f"Error checking freshness for {table_id}: {e}")
            results.append(
                {
                    "check_type": "freshness_check",
                    "timestamp_column": timestamp_column,
                    "passed": False,
                    "message": f"Error: {e}",
                }
            )

    return results


def check_row_counts(
    client: bigquery.Client, table_id: str, count_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    # Check row count thresholds
    results = []

    for count_check in count_config:
        min_rows = count_check.get("min_rows", 1)
        max_rows = count_check.get("max_rows", None)

        sql = f"SELECT COUNT(*) as row_count FROM `{table_id}`"

        try:
            result = list(client.query(sql).result())[0]
            row_count = result.row_count

            passed = row_count >= min_rows
            if max_rows is not None:
                passed = passed and row_count <= max_rows

            results.append(
                {
                    "check_type": "row_count_check",
                    "passed": passed,
                    "row_count": row_count,
                    "min_rows": min_rows,
                    "max_rows": max_rows,
                    "message": f"Table has {row_count} rows (min: {min_rows}, max: {max_rows})",
                }
            )

            if passed:
                logger.info(f"{table_id}: {row_count} rows")
            else:
                logger.error(
                    f"{table_id}: {row_count} rows (min: {min_rows}, max: {max_rows})"
                )

        except Exception as e:
            logger.error(f"Error checking row count for {table_id}: {e}")
            results.append(
                {
                    "check_type": "row_count_check",
                    "passed": False,
                    "message": f"Error: {e}",
                }
            )

    return results


def simple_staging_to_production(
    client: bigquery.Client,
    source: str,
    table: str,
    staging_dataset: str,
    production_dataset: str,
    primary_keys: List[str] = None,
) -> Dict[str, Any]:
    """
    Simple wrapper to move data from staging to production with hash merge.
    This is the default transform behavior.
    """
    staging_table = f"{client.project}.{staging_dataset}.{table}"
    production_table = f"{client.project}.{production_dataset}.{table}"

    # Default primary keys if not provided
    if not primary_keys:
        primary_keys = ["id"]

    config = {
        "truncate_staging": True,  # Clean up staging after merge
        "archive_staging": False,
    }

    return process_hash_merge(
        client=client,
        staging_table=staging_table,
        main_table=production_table,
        primary_keys=primary_keys,
        config=config,
    )
