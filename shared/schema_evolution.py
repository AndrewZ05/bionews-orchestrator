#!/usr/bin/env python3
"""
Schema Evolution and Change Tracking
====================================

Tracks all schema changes to BigQuery tables and provides
intelligent schema validation and evolution capabilities.

Features:
- Automatic schema change detection
- Schema change history tracking in BigQuery
- Smart column addition (staging -> production)
- Type compatibility validation
- Schema drift alerts

Author: Data Pipeline Team
Created: 2025-01-07
"""

import logging
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from shared.log_config import should_log_detail

logger = logging.getLogger(__name__)

# Schema change tracking table - part of orchestrator monitoring dataset
# All job-specific, data health, lineage, etc should go in orchestrator_monitoring
SCHEMA_CHANGES_TABLE = "orchestrator_monitoring.schema_changes"
SCHEMA_VERSIONS_TABLE = (
    "orchestrator_monitoring.schema_versions"  # Fix #16: Schema versioning
)

# System fields managed by ensure_system_metadata_fields(), not schema evolution
SYSTEM_FIELDS = {
    "row_hash",
    "loaded_at",
    "updated_at",
    "source",
    "execution_id",
    "extracted_at",
}


def ensure_schema_tracking_table(client: bigquery.Client) -> bool:
    """
    Ensure the schema_changes tracking table exists.

    Creates orchestrator_monitoring.schema_changes table if it doesn't exist.
    This table tracks all schema evolution events alongside other monitoring data.

    Note: This function now delegates to ensure_monitoring_infrastructure()
    from shared.monitoring for centralized monitoring setup.
    """
    try:
        # Use centralized monitoring infrastructure setup
        from shared.monitoring import ensure_monitoring_infrastructure

        ensure_monitoring_infrastructure(client)
        return True
    except Exception as e:
        logger.error(f"Error ensuring schema tracking table: {e}")
        return False


def log_schema_change(
    client: bigquery.Client,
    source: str,
    table_name: str,
    dataset_name: str,
    change_type: str,
    field_name: Optional[str] = None,
    field_type_before: Optional[str] = None,
    field_type_after: Optional[str] = None,
    field_mode_before: Optional[str] = None,
    field_mode_after: Optional[str] = None,
    execution_id: Optional[str] = None,
    applied: bool = True,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Log a schema change event to the tracking table.
    """
    try:
        # Ensure tracking table exists
        ensure_schema_tracking_table(client)

        # Generate change ID
        import uuid

        change_id = str(uuid.uuid4())

        # Prepare row data
        row = {
            "change_id": change_id,
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "table_name": table_name,
            "dataset_name": dataset_name,
            "change_type": change_type,
            "field_name": field_name,
            "field_type_before": field_type_before,
            "field_type_after": field_type_after,
            "field_mode_before": field_mode_before,
            "field_mode_after": field_mode_after,
            "execution_id": execution_id,
            "applied": applied,
            "error_message": error_message,
            "metadata": metadata,
        }

        # Insert row
        table_id = f"{client.project}.{SCHEMA_CHANGES_TABLE}"
        errors = client.insert_rows_json(table_id, [row])

        if errors:
            logger.error(f"Error logging schema change: {errors}")
            return False

        if should_log_detail(logger, "debug"):
            logger.debug(
                f"Logged schema change: {change_type} for {table_name}.{field_name if field_name else 'N/A'}"
            )
        return True

    except Exception as e:
        logger.error(f"Error logging schema change: {e}")
        return False


def convert_string_column_to_timestamp(
    client: bigquery.Client, table_id: str, column_name: str
) -> bool:
    """Convert a STRING column to TIMESTAMP in-place using SAFE parsing."""

    temp_column = f"{column_name}_ts_tmp"

    try:
        add_sql = f"""
        ALTER TABLE `{table_id}`
        ADD COLUMN IF NOT EXISTS `{temp_column}` TIMESTAMP
        """
        client.query(add_sql).result()

        update_sql = f"""
        UPDATE `{table_id}`
        SET `{temp_column}` = SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', `{column_name}`)
        WHERE `{column_name}` IS NOT NULL
        """
        client.query(update_sql).result()

        drop_sql = f"""
        ALTER TABLE `{table_id}`
        DROP COLUMN `{column_name}`
        """
        client.query(drop_sql).result()

        rename_sql = f"""
        ALTER TABLE `{table_id}`
        RENAME COLUMN `{temp_column}` TO `{column_name}`
        """
        client.query(rename_sql).result()

        logger.info(
            "Converted %s.%s from STRING to TIMESTAMP using SAFE parse",
            table_id,
            column_name,
        )
        return True

    except Exception as exc:
        logger.error(
            "Failed converting %s.%s to TIMESTAMP automatically: %s",
            table_id,
            column_name,
            exc,
        )
        # Attempt to clean up temp column if conversion failed mid-flight
        try:
            cleanup_sql = f"""
            ALTER TABLE `{table_id}`
            DROP COLUMN `{temp_column}`
            """
            client.query(cleanup_sql).result()
        except Exception:
            pass
        return False


def compare_schemas(
    staging_schema: List[bigquery.SchemaField],
    production_schema: Optional[List[bigquery.SchemaField]],
) -> Dict[str, Any]:
    """
    Compare staging and production schemas and identify differences.

    Returns:
        Dict with schema comparison results:
        - new_fields: Fields in staging but not in production
        - type_changes: Fields with different types
        - mode_changes: Fields with different modes (NULLABLE/REQUIRED)
        - missing_in_staging: Fields in production but not in staging
    """
    if not production_schema:
        return {
            "new_fields": staging_schema,
            "type_changes": [],
            "mode_changes": [],
            "missing_in_staging": [],
            "is_compatible": True,
        }

    staging_fields = {f.name: f for f in staging_schema}
    prod_fields = {f.name: f for f in production_schema}

    new_fields = []
    type_changes = []
    mode_changes = []
    missing_in_staging = []

    # Check for new fields in staging
    # Exclude system fields - they're managed by ensure_system_metadata_fields()
    for name, field in staging_fields.items():
        # Skip system fields entirely - they're not managed by schema evolution
        if name in SYSTEM_FIELDS:
            continue

        if name not in prod_fields:
            # New business field detected
            new_fields.append(field)
        else:
            # Field exists in both - check for type/mode changes
            prod_field = prod_fields[name]

            # Check for type changes
            if field.field_type != prod_field.field_type:
                type_changes.append(
                    {
                        "field": name,
                        "staging_type": field.field_type,
                        "production_type": prod_field.field_type,
                    }
                )

            # Check for mode changes
            if field.mode != prod_field.mode:
                mode_changes.append(
                    {
                        "field": name,
                        "staging_mode": field.mode or "NULLABLE",
                        "production_mode": prod_field.mode or "NULLABLE",
                    }
                )

    # Check for fields missing in staging
    # Exclude system fields - they're managed by ensure_system_metadata_fields()
    for name in prod_fields:
        if name not in staging_fields and name not in SYSTEM_FIELDS:
            missing_in_staging.append(prod_fields[name])

    # Determine if schemas are compatible
    # Compatible if: only new fields (additive changes) or mode relaxation (REQUIRED -> NULLABLE)
    # System fields (row_hash, updated_at) are excluded from compatibility check
    is_compatible = (
        len(type_changes) == 0
        and len(missing_in_staging) == 0
        and all(change["staging_mode"] == "NULLABLE" for change in mode_changes)
    )

    return {
        "new_fields": new_fields,
        "type_changes": type_changes,
        "mode_changes": mode_changes,
        "missing_in_staging": missing_in_staging,
        "is_compatible": is_compatible,
    }


def apply_schema_evolution(
    client: bigquery.Client,
    source: str,
    table: str,
    staging_table_id: str,
    production_table_id: str,
    execution_id: Optional[str] = None,
    auto_add_columns: bool = True,
    allow_type_changes: bool = False,
) -> Tuple[bool, List[str]]:
    """
    Apply schema evolution from staging to production table.

    Args:
        client: BigQuery client
        source: Source name (e.g., 'facebook')
        table: Table name (e.g., 'campaigns')
        staging_table_id: Full staging table ID
        production_table_id: Full production table ID
        execution_id: Execution ID for tracking
        auto_add_columns: Automatically add new columns to production
        allow_type_changes: Allow type changes (dangerous!)

    Returns:
        Tuple of (success, list of warnings)
    """
    warnings = []

    try:
        # Get schemas
        staging_table = client.get_table(staging_table_id)
        staging_schema = list(staging_table.schema)

        try:
            production_table = client.get_table(production_table_id)
            production_schema = list(production_table.schema)
        except NotFound:
            production_schema = None
            logger.info(
                f"Production table {production_table_id} does not exist - will be created"
            )

        # Compare schemas
        comparison = compare_schemas(staging_schema, production_schema)

        # Attempt automatic alignment for safe type upgrades (STRING -> TIMESTAMP)
        if comparison["type_changes"]:
            auto_converted_fields: List[str] = []
            for change in list(comparison["type_changes"]):
                prod_type = change.get("production_type")
                staging_type = change.get("staging_type")
                field_name = change.get("field")

                if (
                    prod_type == "STRING"
                    and staging_type in {"TIMESTAMP", "DATETIME"}
                    and field_name
                    and production_table_id
                ):
                    table_converted = convert_string_column_to_timestamp(
                        client, production_table_id, field_name
                    )
                    if table_converted:
                        auto_converted_fields.append(field_name)
                        # Refresh production schema for accurate comparison
                        production_table = client.get_table(production_table_id)
                        production_schema = list(production_table.schema)
                        comparison = compare_schemas(staging_schema, production_schema)

                        log_schema_change(
                            client=client,
                            source=source,
                            table_name=table,
                            dataset_name=production_table_id.split(".")[1],
                            change_type="MODIFY_TYPE",
                            field_name=field_name,
                            field_type_before=prod_type,
                            field_type_after=staging_type,
                            execution_id=execution_id,
                            applied=True,
                            metadata={
                                "automated": True,
                                "strategy": "SAFE_PARSE_TIMESTAMP",
                            },
                        )

            if auto_converted_fields:
                logger.info(
                    "Automatically upgraded production column types for: %s",
                    ", ".join(auto_converted_fields),
                )

        # Extract dataset name from table ID
        dataset_name = production_table_id.split(".")[1]

        # Handle new fields
        if comparison["new_fields"]:
            logger.info(f"Found {len(comparison['new_fields'])} new fields in staging")
            if should_log_detail(logger, "detail"):
                for field in comparison["new_fields"]:
                    logger.info(f"  + {field.name} ({field.field_type})")

            if auto_add_columns:
                successfully_added_fields = []

                for field in comparison["new_fields"]:
                    # CRITICAL: Skip system metadata fields
                    # System fields are set by MERGE operation in bigquery_utils.py, not via ALTER TABLE
                    # Adding them here would create duplicate columns (e.g., row_hash_1)
                    if field.name in SYSTEM_FIELDS:
                        logger.warning(
                            f"  Skipping system field '{field.name}' - managed by MERGE operation, not ALTER TABLE"
                        )
                        continue

                    try:
                        # Add column to production table
                        if production_schema is not None:
                            alter_query = f"""
                            ALTER TABLE `{production_table_id}`
                            ADD COLUMN IF NOT EXISTS `{field.name}` {field.field_type}
                            """
                            client.query(alter_query).result()
                            logger.info(f"  Added column: {field.name}")
                            successfully_added_fields.append(field)

                        # Log schema change
                        log_schema_change(
                            client=client,
                            source=source,
                            table_name=table,
                            dataset_name=dataset_name,
                            change_type="ADD_COLUMN",
                            field_name=field.name,
                            field_type_after=field.field_type,
                            field_mode_after=field.mode or "NULLABLE",
                            execution_id=execution_id,
                            applied=True,
                        )

                    except Exception as e:
                        error_msg = f"Failed to add column {field.name}: {e}"
                        warnings.append(error_msg)
                        logger.warning(f"  {error_msg}")

                        # Log failed schema change
                        log_schema_change(
                            client=client,
                            source=source,
                            table_name=table,
                            dataset_name=dataset_name,
                            change_type="ADD_COLUMN",
                            field_name=field.name,
                            field_type_after=field.field_type,
                            execution_id=execution_id,
                            applied=False,
                            error_message=str(e),
                        )

                # Update YAML config with successfully added fields
                if successfully_added_fields:
                    try:
                        from shared.yaml_updater import update_yaml_with_new_columns

                        update_yaml_with_new_columns(
                            source, table, successfully_added_fields
                        )
                    except Exception as e:
                        logger.warning(f"Could not update YAML config: {e}")
                        # Non-fatal - YAML update is informational only

                # Schema-drift governance: auto-add is convenient but silent drift
                # is how an upstream change reshapes production unnoticed. Emit ONE
                # loud WARNING summarizing the drift so it is visible (and emailed,
                # given the alert wiring) without blocking the run. Auto-add behavior
                # itself is unchanged.
                if successfully_added_fields:
                    cols = ", ".join(
                        f"{f.name} ({f.field_type})" for f in successfully_added_fields
                    )
                    drift_msg = (
                        f"SCHEMA DRIFT: auto-added {len(successfully_added_fields)} "
                        f"new column(s) to {dataset_name}.{table} -- {cols}. "
                        f"Verify this is an intended upstream change."
                    )
                    logger.warning(drift_msg)
                    # Returned to the orchestrator: the "SCHEMA DRIFT:" marker lets
                    # the caller escalate the job to WARNING (-> email) so drift is
                    # not merely logged. Other warnings here do NOT escalate.
                    warnings.append(drift_msg)

            else:
                warnings.append(f"New fields detected but auto_add_columns=False")

        # Handle type changes
        # Fix #17: Enhanced type change detection with explicit blocking and alerting
        if comparison["type_changes"]:
            logger.error(
                f"CRITICAL: Found {len(comparison['type_changes'])} incompatible type changes:"
            )
            logger.error("")

            for change in comparison["type_changes"]:
                logger.error(f"  Field: {change['field']}")
                logger.error(f"    Production: {change['production_type']}")
                logger.error(f"    Staging:    {change['staging_type']}")
                logger.error("")

                # Log type change to tracking table
                log_schema_change(
                    client=client,
                    source=source,
                    table_name=table,
                    dataset_name=dataset_name,
                    change_type="MODIFY_TYPE",
                    field_name=change["field"],
                    field_type_before=change["production_type"],
                    field_type_after=change["staging_type"],
                    execution_id=execution_id,
                    applied=False,
                    error_message="Type changes blocked - requires manual ALTER TABLE",
                )

            if not allow_type_changes:
                # Add detailed error message
                type_change_summary = ", ".join(
                    [
                        f"{c['field']} ({c['production_type']}->{c['staging_type']})"
                        for c in comparison["type_changes"]
                    ]
                )
                error_msg = f"Type changes detected: {type_change_summary}"
                warnings.append(error_msg)

                logger.error("Type changes CANNOT be automatically applied.")
                logger.error("")
                logger.error("RESOLUTION OPTIONS:")
                logger.error("")
                logger.error(
                    "  Option 1: Use --rebuild to recreate table with new schema"
                )
                logger.error(
                    "            WARNING: This will DELETE ALL DATA in production table"
                )
                logger.error(
                    f"            Command: python orchestrate.py --source {source} --table {table} --rebuild ..."
                )
                logger.error("")
                logger.error("  Option 2: Manually ALTER TABLE to change column types")
                logger.error(
                    "            (Safer - preserves data if types are compatible)"
                )
                for change in comparison["type_changes"]:
                    logger.error(
                        f"            ALTER TABLE `{production_table_id}` ALTER COLUMN `{change['field']}` SET DATA TYPE {change['staging_type']};"
                    )
                logger.error("")
                logger.error("  Option 3: Fix source data to match production schema")
                logger.error("            (Recommended if staging type is incorrect)")
                logger.error("")

        # Handle mode changes
        if comparison["mode_changes"]:
            logger.info(f"Found {len(comparison['mode_changes'])} mode changes:")
            for change in comparison["mode_changes"]:
                logger.info(
                    f"  ~ {change['field']}: {change['production_mode']} -> {change['staging_mode']}"
                )

                # Log mode change
                log_schema_change(
                    client=client,
                    source=source,
                    table_name=table,
                    dataset_name=dataset_name,
                    change_type="MODIFY_MODE",
                    field_name=change["field"],
                    field_mode_before=change["production_mode"],
                    field_mode_after=change["staging_mode"],
                    execution_id=execution_id,
                    applied=False,
                    error_message="Mode changes logged for tracking",
                )

        # Handle missing fields in staging (production has extra columns)
        # This is OK - extra production columns will be NULL for new rows
        if comparison["missing_in_staging"]:
            logger.info(
                f"Found {len(comparison['missing_in_staging'])} fields in production but not in staging (will be NULL for new rows):"
            )
            for field in comparison["missing_in_staging"]:
                logger.info(
                    f"  - {field.name} (exists in production, will be NULL in new rows)"
                )
            warnings.append(
                f"{len(comparison['missing_in_staging'])} production fields will be NULL for new rows"
            )

        # Schema validation summary
        # Missing fields in staging is OK (they'll be NULL), only fail on type changes
        # UNLESS allow_type_changes=True (for --rebuild flag)
        has_type_changes = len(comparison["type_changes"]) > 0
        schema_changed = (
            len(comparison["new_fields"]) > 0
            or len(comparison["type_changes"]) > 0
            or len(comparison["mode_changes"]) > 0
        )

        # Fix #16: Record schema version when schema changes or when production table is created
        if schema_changed or production_schema is None:
            try:
                # Get final production schema (after changes applied)
                final_production_table = client.get_table(production_table_id)
                final_schema = list(final_production_table.schema)

                # Record new schema version
                record_schema_version(
                    client=client,
                    source=source,
                    table_name=table,
                    dataset_name=dataset_name,
                    schema=final_schema,
                    execution_id=execution_id,
                    yaml_version=None,  # Could be read from YAML config if available
                )
            except NotFound:
                # Production table doesn't exist yet - version will be recorded after merge
                pass
            except Exception as e:
                logger.warning(f"Could not record schema version: {e}")
                # Non-fatal - continue with merge

        if has_type_changes and not allow_type_changes:
            logger.warning("  Schemas have incompatible changes (type mismatches)")
            logger.warning("  Use --rebuild to recreate tables with new schema")
            return False, warnings
        elif has_type_changes and allow_type_changes:
            logger.warning("  Type changes detected but --rebuild is set")
            logger.warning(
                "  Production table will be recreated from staging with new schema"
            )
            return True, warnings  # Allow merge when rebuild flag is set
        else:
            logger.info("  Schemas are compatible")
            return True, warnings

    except Exception as e:
        error_msg = f"Schema evolution error: {e}"
        logger.error(error_msg)
        import traceback

        logger.error(traceback.format_exc())
        return False, [error_msg]


def get_schema_change_history(
    client: bigquery.Client,
    source: Optional[str] = None,
    table: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """
    Get schema change history from tracking table.

    Args:
        client: BigQuery client
        source: Filter by source (optional)
        table: Filter by table name (optional)
        limit: Max number of records to return

    Returns:
        List of schema change records
    """
    try:
        ensure_schema_tracking_table(client)

        # Build query
        where_clauses = []
        if source:
            where_clauses.append(f"source = '{source}'")
        if table:
            where_clauses.append(f"table_name = '{table}'")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        query = f"""
        SELECT *
        FROM `{client.project}.{SCHEMA_CHANGES_TABLE}`
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
        """

        query_job = client.query(query)
        results = []

        for row in query_job:
            results.append(dict(row))

        return results

    except Exception as e:
        logger.error(f"Error getting schema change history: {e}")
        return []


# ============================================================================
# Fix #16: Schema Versioning System
# ============================================================================


def ensure_schema_versions_table(client: bigquery.Client) -> bool:
    """
    Ensure the schema_versions tracking table exists.
    Fix #16: Tracks schema versions for each table over time.

    Note: This function now delegates to ensure_monitoring_infrastructure()
    from shared.monitoring for centralized monitoring setup.
    """
    try:
        # Use centralized monitoring infrastructure setup
        from shared.monitoring import ensure_monitoring_infrastructure

        ensure_monitoring_infrastructure(client)
        return True
    except Exception as e:
        logger.error(f"Error ensuring schema versions table: {e}")
        return False


def get_current_schema_version(
    client: bigquery.Client, source: str, table_name: str, dataset_name: str
) -> Optional[int]:
    """
    Get the current schema version for a table.
    Fix #16: Returns the latest version number or None if no versions exist.
    """
    try:
        ensure_schema_versions_table(client)

        query = f"""
        SELECT MAX(version) as current_version
        FROM `{client.project}.{SCHEMA_VERSIONS_TABLE}`
        WHERE source = '{source}'
          AND table_name = '{table_name}'
          AND dataset_name = '{dataset_name}'
        """

        query_job = client.query(query)
        result = next(query_job.result(), None)

        if result and result.current_version is not None:
            return int(result.current_version)
        return None

    except Exception as e:
        logger.warning(f"Could not get schema version: {e}")
        return None


def record_schema_version(
    client: bigquery.Client,
    source: str,
    table_name: str,
    dataset_name: str,
    schema: List[bigquery.SchemaField],
    execution_id: Optional[str] = None,
    yaml_version: Optional[int] = None,
) -> bool:
    """
    Record a new schema version for a table.
    Fix #16: Creates a new version record in the schema_versions table.
    """
    try:
        ensure_schema_versions_table(client)

        # Get current version and increment
        current_version = get_current_schema_version(
            client, source, table_name, dataset_name
        )
        new_version = (current_version + 1) if current_version is not None else 1

        # Convert schema to JSON
        import json
        import uuid

        schema_json = [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode or "NULLABLE",
                "description": field.description or "",
            }
            for field in schema
        ]

        # Prepare row data
        row = {
            "version_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "source": source,
            "table_name": table_name,
            "dataset_name": dataset_name,
            "version": new_version,
            "schema_json": json.dumps(schema_json),
            "yaml_version": yaml_version,
            "execution_id": execution_id,
            "created_by": "schema_evolution",
            "metadata": json.dumps({"field_count": len(schema)}),
        }

        # Insert row
        table_id = f"{client.project}.{SCHEMA_VERSIONS_TABLE}"
        errors = client.insert_rows_json(table_id, [row])

        if errors:
            logger.error(f"Error recording schema version: {errors}")
            return False

        logger.info(
            f"  Recorded schema version {new_version} for {source}.{table_name}"
        )
        return True

    except Exception as e:
        logger.error(f"Error recording schema version: {e}")
        return False


def get_schema_version_history(
    client: bigquery.Client, source: str, table_name: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Get schema version history for a table.
    Fix #16: Returns list of schema versions ordered by version number.
    """
    try:
        ensure_schema_versions_table(client)

        query = f"""
        SELECT
            version,
            timestamp,
            dataset_name,
            yaml_version,
            execution_id,
            JSON_EXTRACT_SCALAR(metadata, '$.field_count') as field_count
        FROM `{client.project}.{SCHEMA_VERSIONS_TABLE}`
        WHERE source = '{source}'
          AND table_name = '{table_name}'
        ORDER BY version DESC
        LIMIT {limit}
        """

        query_job = client.query(query)
        results = []

        for row in query_job:
            results.append(dict(row))

        return results

    except Exception as e:
        logger.error(f"Error getting schema version history: {e}")
        return []
