#!/usr/bin/env python3
"""
Schema Registry - Single Source of Truth for Table Schemas
===========================================================

Provides centralized schema management with multiple source support:
1. YAML configuration files (primary)
2. Production BigQuery tables (fallback)
3. Runtime schema discovery (last resort)

This replaces the previous approach of hardcoded Python schemas with
a flexible YAML-based system that's easier to maintain and version.

Usage:
    from shared.schema_registry import get_schema, get_bq_schema

    # Get schema as dict
    schema = get_schema('facebook', 'campaigns')
    # Returns: {'id': 'STRING', 'name': 'STRING', ...}

    # Get schema as BigQuery SchemaField list
    bq_schema = get_bq_schema('facebook', 'campaigns')
    # Returns: [SchemaField('id', 'STRING'), ...]

Author: Data Pipeline Team
Created: 2025-01-20
"""

import os
import logging
import threading
import time
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)

# Schema cache with TTL (thread-safe)
# Format: {cache_key: (schema_dict, timestamp)}
_schema_cache: Dict[str, Tuple[Dict[str, str], float]] = {}
_cache_lock = threading.Lock()

# Cache TTL in seconds (5 minutes)
SCHEMA_CACHE_TTL = 300

# Standard ETL fields added to all tables
STANDARD_ETL_FIELDS = {
    'source': 'STRING',
    'execution_id': 'STRING',
    'extracted_at': 'TIMESTAMP',
    'loaded_at': 'TIMESTAMP',
    'row_hash': 'STRING',
    'updated_at': 'TIMESTAMP'
}


def get_schema(
    source: str,
    table_name: str,
    include_etl_fields: bool = True,
    use_cache: bool = True
) -> Dict[str, str]:
    """
    Get schema for a table from the schema registry.

    Schema Resolution Order:
    1. YAML config file (configs/{source}.yaml -> resources.{table}.schema)
    2. Production BigQuery table (if exists)
    3. Empty dict (let schema discovery handle it)

    Args:
        source: Data source (facebook, wordpress, etc.)
        table_name: Table name without source prefix
        include_etl_fields: Add standard ETL fields to schema (default: True)
        use_cache: Use cached schema if available (default: True)

    Returns:
        Dict mapping column names to BigQuery type strings
        Example: {'id': 'STRING', 'name': 'STRING', 'age': 'INT64'}

    Example:
        >>> schema = get_schema('facebook', 'campaigns')
        >>> print(schema)
        {'id': 'STRING', 'name': 'STRING', 'status': 'STRING', ...}
    """
    # Check cache first (with TTL validation)
    cache_key = f"{source}:{table_name}"
    if use_cache:
        with _cache_lock:
            if cache_key in _schema_cache:
                cached_schema, cache_time = _schema_cache[cache_key]
                current_time = time.time()

                # Check if cache is still valid
                if current_time - cache_time < SCHEMA_CACHE_TTL:
                    schema = cached_schema.copy()
                    if include_etl_fields:
                        schema.update(STANDARD_ETL_FIELDS)
                    logger.debug(f"Schema cache HIT for {cache_key} (age: {int(current_time - cache_time)}s)")
                    return schema
                else:
                    # Cache expired, remove it
                    del _schema_cache[cache_key]
                    logger.debug(f"Schema cache EXPIRED for {cache_key} (age: {int(current_time - cache_time)}s)")
            else:
                logger.debug(f"Schema cache MISS for {cache_key}")

    # Try to load from YAML config
    schema = _load_schema_from_yaml(source, table_name)

    if schema:
        logger.debug(f"Loaded schema for {source}.{table_name} from YAML config")
    else:
        logger.debug(f"No YAML schema found for {source}.{table_name}, will use discovery")
        schema = {}

    # Cache the result with timestamp
    with _cache_lock:
        _schema_cache[cache_key] = (schema.copy(), time.time())

    # Add ETL fields if requested
    if include_etl_fields:
        schema.update(STANDARD_ETL_FIELDS)

    return schema


def get_bq_schema(
    source: str,
    table_name: str,
    include_etl_fields: bool = True,
    use_cache: bool = True
) -> List[bigquery.SchemaField]:
    """
    Get BigQuery schema as list of SchemaField objects.

    Args:
        source: Data source (facebook, wordpress, etc.)
        table_name: Table name without source prefix
        include_etl_fields: Add standard ETL fields (default: True)
        use_cache: Use cached schema if available (default: True)

    Returns:
        List of BigQuery SchemaField objects

    Example:
        >>> bq_schema = get_bq_schema('facebook', 'campaigns')
        >>> print(bq_schema)
        [SchemaField('id', 'STRING'), SchemaField('name', 'STRING'), ...]
    """
    schema_dict = get_schema(source, table_name, include_etl_fields, use_cache)

    if not schema_dict:
        return []

    # Convert dict to BigQuery SchemaField list
    # Preserve field order from YAML by using the dict's insertion order (Python 3.7+)
    schema_fields = []
    for field_name, field_type in schema_dict.items():
        schema_fields.append(
            bigquery.SchemaField(field_name, field_type, mode="NULLABLE")
        )

    return schema_fields


def get_schema_from_bigquery(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table_name: str
) -> Dict[str, str]:
    """
    Get schema from existing BigQuery table.

    Args:
        client: BigQuery client
        project: GCP project ID
        dataset: Dataset name
        table_name: Table name

    Returns:
        Dict mapping column names to BigQuery type strings
        Empty dict if table doesn't exist

    Example:
        >>> from shared.bigquery_client import get_bigquery_client
        >>> client = get_bigquery_client()
        >>> schema = get_schema_from_bigquery(client, 'my-project', 'facebook_data', 'campaigns')
    """
    try:
        table_id = f"{project}.{dataset}.{table_name}"
        table = client.get_table(table_id)

        schema = {}
        for field in table.schema:
            schema[field.name] = field.field_type

        logger.debug(f"Loaded schema from BigQuery table: {table_id}")
        return schema

    except NotFound:
        logger.debug(f"BigQuery table not found: {project}.{dataset}.{table_name}")
        return {}
    except Exception as e:
        logger.warning(f"Error loading schema from BigQuery: {e}")
        return {}


def clear_schema_cache(source: Optional[str] = None, table_name: Optional[str] = None):
    """
    Clear schema cache.

    Args:
        source: If specified, clear only this source (default: clear all)
        table_name: If specified with source, clear only this table

    Example:
        >>> clear_schema_cache()  # Clear all
        >>> clear_schema_cache('facebook')  # Clear all facebook tables
        >>> clear_schema_cache('facebook', 'campaigns')  # Clear specific table
    """
    with _cache_lock:
        if source is None and table_name is None:
            # Clear all
            _schema_cache.clear()
            logger.debug("Cleared all schema cache")
        elif table_name is None:
            # Clear all tables for source
            keys_to_remove = [key for key in _schema_cache.keys() if key.startswith(f"{source}:")]
            for key in keys_to_remove:
                del _schema_cache[key]
            logger.debug(f"Cleared schema cache for source: {source}")
        else:
            # Clear specific table
            cache_key = f"{source}:{table_name}"
            if cache_key in _schema_cache:
                del _schema_cache[cache_key]
                logger.debug(f"Cleared schema cache for: {cache_key}")


def _load_schema_from_yaml(source: str, table_name: str) -> Dict[str, str]:
    """
    Load schema from YAML configuration file.

    Looks for schema in: configs/{source}.yaml -> resources.{table_name}.schema

    Args:
        source: Data source (facebook, wordpress, etc.)
        table_name: Table name

    Returns:
        Dict mapping column names to BigQuery type strings
        Empty dict if not found

    Note:
        This is a private function. Use get_schema() instead.
    """
    try:
        from shared.config_loader import load_config

        # Load source config
        config = load_config(source, use_cache=True)

        # Look for schema in resources section
        resources = config.get('resources', {})
        table_config = resources.get(table_name, {})
        schema = table_config.get('schema', {})

        if schema:
            # Validate schema format
            if not isinstance(schema, dict):
                logger.warning(f"Invalid schema format for {source}.{table_name}: expected dict, got {type(schema)}")
                return {}

            # Validate types
            for field_name, field_type in schema.items():
                if not isinstance(field_type, str):
                    logger.warning(f"Invalid type for {source}.{table_name}.{field_name}: expected string, got {type(field_type)}")

            return schema
        else:
            logger.debug(f"No schema found in YAML for {source}.{table_name}")
            return {}

    except FileNotFoundError:
        logger.debug(f"Config file not found for source: {source}")
        return {}
    except Exception as e:
        logger.warning(f"Error loading schema from YAML for {source}.{table_name}: {e}")
        return {}


def validate_schema(schema: Dict[str, str], table_name: str) -> tuple[bool, List[str]]:
    """
    Validate schema format and types.

    Args:
        schema: Schema dict to validate
        table_name: Table name (for error messages)

    Returns:
        Tuple of (is_valid, error_messages)

    Example:
        >>> schema = {'id': 'STRING', 'age': 'INT64'}
        >>> valid, errors = validate_schema(schema, 'users')
        >>> if not valid:
        ...     print(errors)
    """
    errors = []

    if not isinstance(schema, dict):
        errors.append(f"Schema must be a dict, got {type(schema)}")
        return False, errors

    if not schema:
        errors.append("Schema is empty")
        return False, errors

    # Valid BigQuery types
    valid_types = {
        'STRING', 'INT64', 'FLOAT64', 'BOOL', 'BOOLEAN',
        'TIMESTAMP', 'DATE', 'TIME', 'DATETIME',
        'BYTES', 'NUMERIC', 'BIGNUMERIC', 'JSON',
        'GEOGRAPHY', 'RECORD', 'STRUCT'
    }

    for field_name, field_type in schema.items():
        # Validate field name
        if not isinstance(field_name, str):
            errors.append(f"Field name must be string, got {type(field_name)}")
            continue

        if not field_name:
            errors.append("Field name cannot be empty")
            continue

        # Validate field type
        if not isinstance(field_type, str):
            errors.append(f"Field '{field_name}' type must be string, got {type(field_type)}")
            continue

        if field_type.upper() not in valid_types:
            errors.append(f"Field '{field_name}' has invalid type '{field_type}'. Valid types: {', '.join(sorted(valid_types))}")

    is_valid = len(errors) == 0
    return is_valid, errors


def compare_schemas(
    schema1: Dict[str, str],
    schema2: Dict[str, str],
    schema1_name: str = "Schema 1",
    schema2_name: str = "Schema 2"
) -> Dict[str, Any]:
    """
    Compare two schemas and return differences.

    Args:
        schema1: First schema dict
        schema2: Second schema dict
        schema1_name: Name for first schema (for reporting)
        schema2_name: Name for second schema (for reporting)

    Returns:
        Dict with comparison results:
        {
            'matching_fields': int,
            'type_mismatches': List[Dict],
            'only_in_schema1': List[str],
            'only_in_schema2': List[str],
            'is_compatible': bool
        }

    Example:
        >>> yaml_schema = get_schema('facebook', 'campaigns')
        >>> bq_schema = get_schema_from_bigquery(client, 'proj', 'dataset', 'campaigns')
        >>> diff = compare_schemas(yaml_schema, bq_schema, "YAML", "BigQuery")
        >>> if not diff['is_compatible']:
        ...     print("Schemas are incompatible!")
    """
    result = {
        'matching_fields': 0,
        'type_mismatches': [],
        'only_in_schema1': [],
        'only_in_schema2': [],
        'is_compatible': True
    }

    # Find fields only in schema1
    for field in schema1:
        if field not in schema2:
            result['only_in_schema1'].append(field)

    # Find fields only in schema2
    for field in schema2:
        if field not in schema1:
            result['only_in_schema2'].append(field)

    # Compare common fields
    common_fields = set(schema1.keys()) & set(schema2.keys())
    for field in common_fields:
        type1 = schema1[field].upper()
        type2 = schema2[field].upper()

        if type1 == type2:
            result['matching_fields'] += 1
        else:
            result['type_mismatches'].append({
                'field': field,
                f'{schema1_name}_type': type1,
                f'{schema2_name}_type': type2
            })
            result['is_compatible'] = False

    # Schema is incompatible if there are type mismatches or missing required fields
    if result['type_mismatches'] or result['only_in_schema2']:
        result['is_compatible'] = False

    return result


def log_schema_comparison(comparison: Dict[str, Any], logger_obj: logging.Logger = None):
    """
    Log schema comparison results in a readable format.

    Args:
        comparison: Result from compare_schemas()
        logger_obj: Logger to use (default: module logger)

    Example:
        >>> diff = compare_schemas(schema1, schema2, "YAML", "BigQuery")
        >>> log_schema_comparison(diff)
    """
    log = logger_obj or logger

    log.info("Schema Comparison Results:")
    log.info(f"  Matching fields: {comparison['matching_fields']}")

    if comparison['type_mismatches']:
        log.warning(f"  Type mismatches: {len(comparison['type_mismatches'])}")
        for mismatch in comparison['type_mismatches']:
            log.warning(f"    {mismatch['field']}: {mismatch}")

    if comparison['only_in_schema1']:
        log.info(f"  Only in first schema: {len(comparison['only_in_schema1'])}")
        for field in comparison['only_in_schema1']:
            log.info(f"    {field}")

    if comparison['only_in_schema2']:
        log.warning(f"  Only in second schema: {len(comparison['only_in_schema2'])}")
        for field in comparison['only_in_schema2']:
            log.warning(f"    {field}")

    if comparison['is_compatible']:
        log.info("  [OK] Schemas are compatible")
    else:
        log.warning("  [FAILED] Schemas are NOT compatible")
