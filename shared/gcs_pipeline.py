#!/usr/bin/env python3
"""
GCS Pipeline Flow Management
============================

Implements the 6-step data lake pipeline:
1. Extract data to local parquet file (with early validation)
2. Copy parquet file to GCS raw bucket
3. Create external table using parquet file in source_staging
4. Create/transform staging table to source_data
5. Delete external table after loading, redefine in source_archive
6. Clean up local files

Author: Data Pipeline Team
Created: 2025-01-02
Updated: 2025-01-09 - Added early validation system
"""

import os
import logging
import shutil
import sys
import tempfile
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Any, Optional, Tuple

import pandas as pd
import numpy as np

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage as gcs

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy import text

# Import early validation system
from shared.data_validator import validate_sample_data
from shared.bigquery_utils import process_hash_merge
from shared.schema_evolution import apply_schema_evolution
from shared.table_tracking import (
    start_table_processing,
    update_table_processing,
    complete_table_processing,
)

# Note: Most GCS functions not needed for extract_to_local_parquet
# The upload_to_gcs_raw function uses Google Cloud Storage client directly

# Configure logging
logger = logging.getLogger(__name__)

# Cache for system fields from defaults.yaml
_SYSTEM_FIELDS_CACHE = None

# Known system fields injected by extractors (shared across sources)
SYSTEM_FIELD_TYPE_OVERRIDES = {
    "execution_id": "STRING",
    "source": "STRING",
    "extracted_at": "TIMESTAMP",
}


def get_system_fields() -> Dict[str, str]:
    """
    Get system fields from defaults.yaml that should be automatically added to ALL tables.
    These fields are defined centrally and should NOT be in source-specific YAML files.

    Returns:
        Dict of {field_name: bigquery_type} for required system fields
    """
    global _SYSTEM_FIELDS_CACHE

    if _SYSTEM_FIELDS_CACHE is not None:
        return _SYSTEM_FIELDS_CACHE

    try:
        defaults_path = os.path.join(
            os.path.dirname(__file__), "..", "configs", "defaults.yaml"
        )
        if not os.path.exists(defaults_path):
            logger.warning(
                f"defaults.yaml not found at {defaults_path}, using fallback system fields"
            )
            _SYSTEM_FIELDS_CACHE = {
                "extracted_at": "TIMESTAMP",
                "loaded_at": "TIMESTAMP",
                "execution_id": "STRING",
            }
            return _SYSTEM_FIELDS_CACHE

        with open(defaults_path, "r") as f:
            defaults = yaml.safe_load(f) or {}

        system_fields_config = defaults.get("system_fields", {})
        system_fields = {}

        for field_name, field_config in system_fields_config.items():
            if isinstance(field_config, dict) and "type" in field_config:
                # Auto-inject ALL system fields (simpler approach)
                system_fields[field_name] = field_config["type"]

        logger.debug(
            f"Loaded {len(system_fields)} system fields from defaults.yaml: {list(system_fields.keys())}"
        )
        _SYSTEM_FIELDS_CACHE = system_fields
        return system_fields

    except Exception as e:
        logger.warning(
            f"Error loading system fields from defaults.yaml: {e}, using fallback"
        )
        _SYSTEM_FIELDS_CACHE = {
            "extracted_at": "TIMESTAMP",
            "loaded_at": "TIMESTAMP",
            "execution_id": "STRING",
        }
        return _SYSTEM_FIELDS_CACHE


#
# Schema Enforcement Functions
#


def get_wordpress_schema_from_mysql(engine, table_name: str) -> Dict[str, str]:
    """
    Get authoritative schema from MySQL information_schema.

    For WordPress tables, this queries the MySQL database to get the exact
    column types as defined in the database, which is the source of truth.

    Args:
        engine: SQLAlchemy engine connected to WordPress MySQL database
        table_name: Table name (e.g., 'wp_users', 'wp_posts')

    Returns:
        Dict mapping column_name -> BigQuery type

    Example:
        >>> engine = create_engine('mysql://...')
        >>> schema = get_wordpress_schema_from_mysql(engine, 'wp_users')
        >>> schema['user_registered']
        'TIMESTAMP'
    """
    query = f"""
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        COLUMN_TYPE
    FROM information_schema.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))

            schema = {}
            for row in result:
                col_name = row[0]  # COLUMN_NAME
                data_type = row[1].lower()  # DATA_TYPE

                # Map MySQL -> BigQuery types
                if data_type in ["datetime", "timestamp"]:
                    bq_type = "TIMESTAMP"
                elif data_type in ["date"]:
                    bq_type = "DATE"
                elif data_type in ["bigint", "int", "tinyint", "smallint", "mediumint"]:
                    bq_type = "INT64"
                elif data_type in ["decimal", "numeric", "float", "double"]:
                    bq_type = "FLOAT64"
                elif data_type in [
                    "varchar",
                    "char",
                    "text",
                    "longtext",
                    "mediumtext",
                    "tinytext",
                ]:
                    bq_type = "STRING"
                elif data_type in ["blob", "longblob", "mediumblob", "tinyblob"]:
                    bq_type = "BYTES"
                else:
                    bq_type = "STRING"  # Safe default

                schema[col_name] = bq_type

            return schema
    except Exception as e:
        logger.warning(f"Could not get MySQL schema for {table_name}: {e}")
        return {}


def get_facebook_schema_from_sdk(table_name: str) -> Dict[str, str]:
    """
    Get authoritative schema from Facebook SDK object definitions.

    The Facebook Python Business SDK contains _field_types definitions for all
    API objects, which is the official schema from Facebook.

    Args:
        table_name: Table name (e.g., 'campaigns', 'adsets', 'ads')

    Returns:
        Dict mapping field_name -> BigQuery type

    Example:
        >>> schema = get_facebook_schema_from_sdk('campaigns')
        >>> schema['created_time']
        'TIMESTAMP'
    """
    try:
        # Import SDK classes
        from facebook_business.adobjects.campaign import Campaign
        from facebook_business.adobjects.adset import AdSet
        from facebook_business.adobjects.ad import Ad

        # Map table name to SDK class
        sdk_classes = {
            "campaigns": Campaign,
            "adsets": AdSet,
            "ads": Ad,
        }

        if table_name not in sdk_classes:
            logger.debug(f"No SDK class mapping for table: {table_name}")
            return {}

        sdk_class = sdk_classes[table_name]
        field_types = getattr(sdk_class, "Field", {})

        schema = {}
        # Facebook SDK defines fields as class attributes
        for attr_name in dir(field_types):
            if not attr_name.startswith("_"):
                field_name = getattr(field_types, attr_name)
                # Infer type from field name patterns
                if "time" in field_name or "date" in field_name:
                    bq_type = "TIMESTAMP"
                elif "id" in field_name:
                    bq_type = "STRING"
                elif "count" in field_name or "budget" in field_name:
                    bq_type = "INT64"
                elif "status" in field_name or "name" in field_name:
                    bq_type = "STRING"
                else:
                    bq_type = "STRING"  # Default

                schema[field_name] = bq_type

        return schema
    except Exception as e:
        logger.warning(f"Could not get Facebook SDK schema for {table_name}: {e}")
        return {}


def infer_schema_robust(df: pd.DataFrame, sample_size: int = 10000) -> Dict[str, str]:
    """
    Robust type inference for unknown sources.

    Uses a large sample and checks ALL unique values to determine types.
    This is more reliable than pandas default inference but still not perfect.

    Args:
        df: DataFrame with extracted data
        sample_size: Number of rows to sample (default 10000)

    Returns:
        Dict mapping column_name -> BigQuery type

    Note:
        This is a FALLBACK for sources without authoritative schema.
        WordPress and Facebook should use their authoritative schemas.
    """
    schema = {}

    for col in df.columns:
        # Sample data for analysis
        if len(df) > sample_size:
            sample = df[col].sample(n=sample_size, random_state=42)
        else:
            sample = df[col]

        # Get non-null unique values
        unique_vals = sample.dropna().unique()

        if len(unique_vals) == 0:
            # All nulls - default to STRING
            schema[col] = "STRING"
            continue

        # Check if all values are valid timestamps
        try:
            pd.to_datetime(unique_vals, errors="raise")
            schema[col] = "TIMESTAMP"
            continue
        except:
            pass

        # Check if all values are integers
        try:
            int_vals = pd.to_numeric(unique_vals, errors="raise")
            if all(int_vals == int_vals.astype(int)):
                schema[col] = "INT64"
                continue
        except:
            pass

        # Check if all values are floats
        try:
            pd.to_numeric(unique_vals, errors="raise")
            schema[col] = "FLOAT64"
            continue
        except:
            pass

        # Check if all values are booleans
        if set(unique_vals).issubset({True, False, "true", "false", "1", "0", 1, 0}):
            schema[col] = "BOOLEAN"
            continue

        # Default to STRING
        schema[col] = "STRING"

    return schema


def get_authoritative_schema(
    source: str,
    table: str,
    bq_client: Optional[bigquery.Client] = None,
    production_table_id: Optional[str] = None,
    mysql_engine: Optional[Any] = None,
    df: Optional[pd.DataFrame] = None,
    config_path: Optional[str] = None,
) -> Tuple[Optional[Dict[str, str]], str]:
    """
    Get authoritative schema from best available source.

    Schema Resolution Priority (simplified):
    1. Schema Registry (YAML config via schema_registry) - PRIMARY SOURCE
    2. Production BigQuery table (if exists) - FALLBACK
    3. Runtime schema discovery (data analysis) - LAST RESORT

    Args:
        source: Source name ('wordpress', 'facebook', etc.)
        table: Table name
        bq_client: BigQuery client (optional)
        production_table_id: Production table ID (optional)
        mysql_engine: MySQL engine for WordPress (optional)
        df: DataFrame for type inference fallback (optional)
        config_path: Path to YAML config file (optional, default: configs/{source}.yaml)

    Returns:
        Tuple of (schema_dict, source_description)
        schema_dict: Dict mapping column_name -> BigQuery type, or None
        source_description: String describing where schema came from

    Example:
        >>> schema, source = get_authoritative_schema(
        ...     'wordpress', 'users',
        ...     bq_client=client,
        ...     production_table_id='project.dataset.users'
        ... )
        >>> print(f"Schema from: {source}")
        Schema from: Schema Registry (YAML config)
    """
    logger.info(f"Finding authoritative schema for {source}.{table}")

    # Priority 1: Schema Registry (NEW - uses YAML config)
    try:
        from shared.schema_registry import get_schema

        schema = get_schema(source, table, include_etl_fields=False, use_cache=True)
        if schema:
            logger.info(
                f"Using Schema Registry: {len(schema)} fields (source: YAML config)"
            )
            return schema, "Schema Registry (YAML config)"
    except Exception as e:
        logger.debug(f"Schema Registry not available: {e}")

    # Priority 1b: Legacy YAML reading (DEPRECATED - for backward compatibility)
    if config_path is None:
        config_path = f"configs/{source}.yaml"

    yaml_schema = read_yaml_schema(config_path, table)
    if yaml_schema:
        # Check if schema is complete (no missing types)
        missing_types = [
            col for col, typ in yaml_schema.items() if typ is None or typ == ""
        ]
        if not missing_types:
            logger.info(
                f"Using legacy YAML schema from {config_path} ({len(yaml_schema)} columns)"
            )
            return yaml_schema, f"Legacy YAML config ({config_path})"
        else:
            logger.info(
                f"YAML schema incomplete - missing types for {len(missing_types)} columns"
            )
            logger.info(f"   Will discover missing types and update YAML")

    # Priority 2: Production table (most reliable for incremental)
    if bq_client and production_table_id:
        try:
            prod_table = bq_client.get_table(production_table_id)
            schema = {}
            for field in prod_table.schema:
                # Skip system fields added by merge process
                if field.name not in [
                    "row_hash",
                    "updated_at",
                    "loaded_at",
                    "execution_id",
                    "extracted_at",
                ]:
                    schema[field.name] = field.field_type

            if schema:
                logger.info(f"Using production table schema: {len(schema)} fields")
                return schema, "production table"
        except NotFound:
            logger.debug(f"Production table not found: {production_table_id}")
        except Exception as e:
            from shared.error_messages import format_bigquery_error

            logger.warning(
                format_bigquery_error(
                    "Get production table schema",
                    e,
                    table=table,
                    dataset=production_table_id,
                    context={"source": source},
                )
            )

    # Priority 2a: WordPress MySQL schema
    if source == "wordpress" and mysql_engine:
        try:
            # Determine table prefix (usually 'wp_' but could be custom)
            wp_table_name = f"wp_{table}" if not table.startswith("wp_") else table
            schema = get_wordpress_schema_from_mysql(mysql_engine, wp_table_name)

            if schema:
                logger.info(f"Using MySQL schema: {len(schema)} fields")
                return schema, "MySQL information_schema"
        except Exception as e:
            logger.warning(f"Could not get MySQL schema: {e}")

    # Priority 2b: Facebook SDK schema
    if source == "facebook":
        try:
            schema = get_facebook_schema_from_sdk(table)
            if schema:
                logger.info(f"Using Facebook SDK schema: {len(schema)} fields")
                return schema, "Facebook SDK"
        except Exception as e:
            logger.warning(f"Could not get Facebook SDK schema: {e}")

    # Priority 3: Robust type inference (fallback)
    if df is not None:
        logger.warning(f"No authoritative schema found for {source}.{table}")
        logger.warning(
            "Using robust type inference (may cause schema conflicts on data changes)"
        )
        schema = infer_schema_robust(df, sample_size=10000)
        return schema, "robust type inference"

    # No schema available
    logger.warning(f"No schema source available for {source}.{table}")
    logger.warning("Falling back to Parquet auto-inference (UNRELIABLE)")
    return None, "Parquet auto-inference"


def read_yaml_schema(config_path: str, table: str) -> Optional[Dict[str, str]]:
    """
    Read schema for a specific table from YAML config.
    Automatically injects required system fields from defaults.yaml.

    Args:
        config_path: Path to YAML config file (e.g., 'configs/wordpress.yaml')
        table: Table name to look up

    Returns:
        Dict of {column_name: bigquery_type} or None if not found
    """
    try:
        if not os.path.exists(config_path):
            logger.debug(f"Config file not found: {config_path}")
            return None

        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        # Look for schema in resources section
        resources = config.get("resources", {})
        table_schema = None

        # Try exact table name first
        if table in resources:
            table_config = resources[table]
            if isinstance(table_config, dict) and "schema" in table_config:
                table_schema = table_config["schema"]

        # Try matching by table_name field
        if not table_schema:
            for resource_name, resource_config in resources.items():
                if isinstance(resource_config, dict):
                    if resource_config.get("table_name") == table:
                        if "schema" in resource_config:
                            table_schema = resource_config["schema"]
                            break

        if not table_schema:
            logger.debug(f"No schema found for table '{table}' in {config_path}")
            return None

        # DO NOT inject system fields into Parquet schema
        # System fields (row_hash, loaded_at, updated_at, execution_id, source) are added
        # during BigQuery table creation only, not during Parquet extraction
        # This prevents duplicate fields when staging table copies Parquet schema
        #
        # ONLY extracted_at can be in Parquet since it's set during extraction by the extractor
        return table_schema

    except Exception as e:
        logger.warning(f"Error reading YAML schema: {e}")
        return None


def _apply_schema_edits_preserving_comments(
    config_path: str,
    resource_key: str,
    new_columns: Dict[str, str],
    metadata: Dict[str, str],
) -> bool:
    """Write schema changes back to the config as a TEXT edit, keeping comments.

    The previous implementation re-dumped the whole parsed document with
    yaml.safe_dump, which silently STRIPPED every comment and blank line and
    reformatted quotes/indentation on each run. On the prod box (where schema
    auto-discovery fires nightly) that quietly destroyed hand-written config
    documentation. This edits only the specific lines that change:

      * appends each brand-new column under the resource's `schema:` block,
        matching the block's existing indentation
      * updates the `last_updated` / `discovery_source` / `column_count` lines
        inside that resource's `schema_metadata:` block

    Everything else in the file -- comments, blank lines, key order, quoting --
    is left byte-for-byte identical. Returns True if the file was written.

    Only NEW columns are inserted here; type-fills of existing columns are rare
    and still handled by the caller's fallback. If the text anchors can't be
    found (unexpected layout), returns False so the caller can fall back.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Locate the `  <resource_key>:` line (2-space indent under `resources:`).
    res_idx = None
    for i, line in enumerate(lines):
        if line.rstrip("\n") == f"  {resource_key}:":
            res_idx = i
            break
    if res_idx is None:
        return False

    # Find the end of this resource block (next line indented <= 2 spaces that
    # isn't blank/comment), so we only edit within it.
    block_end = len(lines)
    for i in range(res_idx + 1, len(lines)):
        stripped = lines[i].lstrip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(lines[i]) - len(lines[i].lstrip(" "))
        if indent <= 2:
            block_end = i
            break

    # --- Insert new columns under this resource's `schema:` block. ---
    if new_columns:
        schema_idx = None
        for i in range(res_idx + 1, block_end):
            if (
                lines[i].rstrip("\n").strip() == "schema:"
                and (len(lines[i]) - len(lines[i].lstrip(" "))) == 4
            ):
                schema_idx = i
                break
        if schema_idx is None:
            return False  # no schema block to append to -> let caller fall back

        # Column entries sit at schema-indent + 2. Find the last existing entry
        # (or the schema: line itself) to insert after, and capture its indent.
        col_indent = "      "  # default: 6 spaces (schema at 4 + 2)
        insert_after = schema_idx
        for i in range(schema_idx + 1, block_end):
            stripped = lines[i].lstrip()
            if not stripped or stripped.startswith("#"):
                continue
            indent = len(lines[i]) - len(lines[i].lstrip(" "))
            if indent <= 4:  # left the schema block
                break
            col_indent = " " * indent
            insert_after = i

        new_lines = [
            f"{col_indent}{name}: {typ}\n" for name, typ in new_columns.items()
        ]
        lines[insert_after + 1 : insert_after + 1] = new_lines
        # Re-find block_end after insertion (indices shifted).
        block_end += len(new_lines)

    # --- Update the 3 schema_metadata lines in place (create block if absent). ---
    def _set_meta_line(key: str, value: str):
        for i in range(res_idx + 1, block_end):
            stripped = lines[i].lstrip()
            indent = len(lines[i]) - len(lines[i].lstrip(" "))
            if stripped.startswith(f"{key}:") and indent >= 6:
                lines[i] = f"{' ' * indent}{key}: {value}\n"
                return True
        return False

    for k, v in metadata.items():
        _set_meta_line(k, v)

    with open(config_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    return True


def update_yaml_schema_selective(
    config_path: str,
    table: str,
    discovered_schema: Dict[str, str],
    discovery_source: str,
) -> Dict[str, Any]:
    """
    Update YAML schema only for columns without types (column-level selective update).

    This function:
    - Reads existing YAML schema
    - For each column in discovered schema:
      * If column exists in YAML WITH type -> Keep YAML type (preserve human edits)
      * If column exists in YAML WITHOUT type -> Fill from discovery
      * If column NOT in YAML -> Add from discovery
    - Preserves all other YAML fields
    - Writes updated schema back to YAML

    Args:
        config_path: Path to YAML config file
        table: Table name
        discovered_schema: Schema discovered from authoritative source
        discovery_source: Description of where schema came from

    Returns:
        Dict with statistics: {
            'columns_filled': int,    # Existing columns that got types filled
            'columns_added': int,     # New columns added
            'columns_kept': int,      # Columns with existing types (preserved)
            'updated': bool           # Whether file was modified
        }
    """
    stats = {
        "columns_filled": 0,
        "columns_added": 0,
        "columns_kept": 0,
        "columns_updated": 0,
        "updated": False,
    }

    try:
        # Read existing config
        if not os.path.exists(config_path):
            logger.warning(
                f"Config file not found: {config_path} - cannot update schema"
            )
            return stats

        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        # Ensure resources section exists
        if "resources" not in config:
            config["resources"] = {}

        # Find or create table entry
        table_config = None
        resource_key = None

        # Try to find existing table config
        for key, res_config in config["resources"].items():
            if isinstance(res_config, dict):
                if key == table or res_config.get("table_name") == table:
                    table_config = res_config
                    resource_key = key
                    break

        # If not found, create new entry
        if table_config is None:
            resource_key = table
            table_config = {"table_name": table, "schema": {}}
            config["resources"][resource_key] = table_config

        # Ensure schema section exists
        if "schema" not in table_config:
            table_config["schema"] = {}

        existing_schema = table_config["schema"]

        # Track the NEW columns added this run (name -> type), in insertion order,
        # so the comment-preserving text writer can append exactly these.
        newly_added_columns: Dict[str, str] = {}

        # Process each discovered column
        for col_name, col_type in discovered_schema.items():
            if col_name in existing_schema:
                existing_type = existing_schema[col_name]

                if existing_type is None or existing_type == "":
                    existing_schema[col_name] = col_type
                    stats["columns_filled"] += 1
                    stats["updated"] = True
                    logger.info(f"  Filled type for '{col_name}': {col_type}")
                else:
                    # Preserve existing YAML types - do NOT overwrite with inferred types
                    # This prevents auto-discovery from changing manually corrected schemas
                    stats["columns_kept"] += 1
                    normalized_existing = str(existing_type).upper()
                    normalized_new = str(col_type).upper()
                    if normalized_existing != normalized_new:
                        logger.debug(
                            "  -> Kept YAML type for '%s': %s (inferred: %s)",
                            col_name,
                            existing_type,
                            col_type,
                        )
                    else:
                        logger.debug(
                            f"  -> Kept existing type for '{col_name}': {existing_type}"
                        )
            else:
                # New column - add it
                existing_schema[col_name] = col_type
                newly_added_columns[col_name] = col_type
                stats["columns_added"] += 1
                stats["updated"] = True
                logger.info(f"  + Added new column '{col_name}': {col_type}")

        # Add schema metadata
        if "schema_metadata" not in table_config:
            table_config["schema_metadata"] = {}

        metadata = table_config["schema_metadata"]
        metadata["last_updated"] = datetime.now().isoformat()
        metadata["discovery_source"] = discovery_source
        metadata["column_count"] = len(existing_schema)

        # Write back to file if updated.
        if stats["updated"]:
            # Prefer a comment-preserving TEXT edit that touches only the changed
            # lines. Fall back to a full re-dump only if the text anchors can't be
            # found (e.g. a brand-new table with no existing block to edit) -- that
            # path still strips comments, but it is the rare case, not the nightly one.
            meta_updates = {
                "last_updated": f"'{metadata['last_updated']}'",
                "discovery_source": str(metadata["discovery_source"]),
                "column_count": str(metadata["column_count"]),
            }
            wrote_preserving = False
            try:
                wrote_preserving = _apply_schema_edits_preserving_comments(
                    config_path, resource_key, newly_added_columns, meta_updates
                )
            except (
                Exception
            ) as e:  # noqa: BLE001 - never let the writer crash discovery
                logger.warning(
                    "Comment-preserving schema write failed (%s); falling back to re-dump.",
                    e,
                )
            if not wrote_preserving:
                with open(config_path, "w") as f:
                    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)
                logger.info("Wrote schema via full re-dump (comments not preserved).")

            logger.info(f"Updated YAML schema in {config_path}")
            logger.info(
                "   Filled: %d, Added: %d, Updated: %d, Kept: %d",
                stats["columns_filled"],
                stats["columns_added"],
                stats["columns_updated"],
                stats["columns_kept"],
            )

            # Event-driven config sync: mark this config so it is auto-committed
            # and pushed to both repos when the process exits (any hour, one
            # commit per run). No-op unless CONFIG_AUTO_PUSH is enabled; never
            # raises. The change then flows box -> GitHub -> laptop `git pull`.
            try:
                from shared.config_autostage import mark_config_dirty

                mark_config_dirty(config_path)
            except Exception:  # noqa: BLE001 - sync signalling must not affect the run
                pass
        else:
            logger.info(f"No schema updates needed for {table}")

        return stats

    except Exception as e:
        logger.error(f"Error updating YAML schema: {e}")
        return stats


def validate_yaml_schema(config_path: str, source: str) -> Dict[str, Any]:
    """
    Validate YAML schema and identify columns missing field types.

    This function:
    - Reads YAML config
    - Finds all tables with schemas
    - Identifies columns with missing/empty types
    - Suggests discovery methods for missing types

    Args:
        config_path: Path to YAML config file
        source: Source name (wordpress, facebook, etc.)

    Returns:
        Dict with validation results: {
            'valid': bool,
            'tables_checked': int,
            'missing_types': {
                'table_name': ['col1', 'col2', ...]
            },
            'recommendations': List[str]
        }
    """
    results = {
        "valid": True,
        "tables_checked": 0,
        "missing_types": {},
        "recommendations": [],
    }

    try:
        if not os.path.exists(config_path):
            logger.warning(f"Config file not found: {config_path}")
            results["valid"] = False
            results["recommendations"].append(f"Create config file: {config_path}")
            return results

        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        resources = config.get("resources", {})

        for resource_name, resource_config in resources.items():
            if not isinstance(resource_config, dict):
                continue

            schema = resource_config.get("schema", {})
            if not schema:
                continue

            results["tables_checked"] += 1
            table_name = resource_config.get("table_name", resource_name)

            # Find columns with missing types
            missing_cols = []
            for col_name, col_type in schema.items():
                if col_type is None or col_type == "":
                    missing_cols.append(col_name)

            if missing_cols:
                results["valid"] = False
                results["missing_types"][table_name] = missing_cols

                # Add recommendations based on source
                if source == "wordpress":
                    results["recommendations"].append(
                        f"Run schema discovery for {table_name}: "
                        f"Use MySQL information_schema to get types for {len(missing_cols)} columns"
                    )
                elif source == "facebook":
                    results["recommendations"].append(
                        f"Run schema discovery for {table_name}: "
                        f"Use Facebook SDK field types to get types for {len(missing_cols)} columns"
                    )
                else:
                    results["recommendations"].append(
                        f"Run schema discovery for {table_name}: "
                        f"Use robust type inference on existing data for {len(missing_cols)} columns"
                    )

        return results

    except Exception as e:
        logger.error(f"Error validating YAML schema: {e}")
        results["valid"] = False
        results["recommendations"].append(f"Fix YAML syntax error: {e}")
        return results


def extract_to_local_parquet(
    data: List[Dict[str, Any]],
    source: str,
    table: str,
    job_id: str,
    local_temp_dir: str = None,
    validate_sample: bool = True,
    validation_sample_size: int = 100,
    bq_client: Optional[bigquery.Client] = None,
    production_table_id: Optional[str] = None,
    mysql_engine: Optional[Any] = None,
    rebuild_mode: bool = False,
) -> Optional[str]:
    """
    STEP 1: Extract data to local parquet file with schema enforcement.

    Creates a temporary parquet file with extracted data for
    subsequent GCS upload and BigQuery processing.

    CRITICAL: If production_table_id is provided, enforces production schema
    to prevent Parquet type inference issues that cause schema conflicts.

    Includes EARLY VALIDATION to catch type issues before processing all data.

    Args:
        data: Extracted data records
        source: Source system name
        table: Table name
        job_id: Unique job identifier
        local_temp_dir: Optional directory for temp files
        validate_sample: Enable early validation (default: True)
        validation_sample_size: Number of records to validate (default: 100)
        bq_client: BigQuery client (optional - for schema enforcement)
        production_table_id: Production table ID (optional - for schema enforcement)
        mysql_engine: MySQL engine (optional - for schema discovery)

    Returns:
        Path to created local parquet file, or None if no data to extract

    Example:
        >>> file_path = extract_to_local_parquet(
        ...     data, 'facebook', 'campaigns', 'job_001',
        ...     bq_client=client,
        ...     production_table_id='project.dataset.table'
        ... )
        >>> if file_path:
        ...     print(f"Created: {file_path}")
        ... else:
        ...     print("No data extracted (empty result)")

    Raises:
        ValueError: If validation fails and data cannot be converted to Parquet
    """
    try:
        if not data:
            logger.info(
                f"No data extracted for {source}/{table} - this is normal for incremental lookbacks with no new data"
            )
            return None

        # EARLY VALIDATION: Check sample before processing all data
        # Note: Validation reports issues but type conversion handles mixed types automatically
        if validate_sample and len(data) > validation_sample_size:
            logger.info(
                f"Running early validation on sample ({validation_sample_size} records)..."
            )
            sample = data[:validation_sample_size]

            validation_result = validate_sample_data(
                sample, table, validation_sample_size
            )

            # Check if errors are ONLY about known mixed-type columns that type conversion handles
            # Expanded list to cover all problematic Facebook API columns
            known_mixed_type_patterns = [
                "action",
                "cost_per",
                "conversion",
                "offsite",
                "fb_pixel",
                "post_reaction",
                "page_engagement",
                "video_view",
            ]
            has_known_mixed_type_errors = False
            has_other_errors = False

            # Access dict keys instead of object attributes
            if validation_result["errors"]:
                for error in validation_result["errors"]:
                    error_str = str(error).lower()
                    # Check if error mentions a known problematic column pattern
                    # AND it's a mixed type or Parquet conversion issue
                    is_known_pattern = any(
                        pattern in error_str for pattern in known_mixed_type_patterns
                    )
                    is_type_issue = (
                        "mixed type" in error_str
                        or "parquet" in error_str
                        or "could not convert" in error_str
                    )

                    if is_known_pattern and is_type_issue:
                        has_known_mixed_type_errors = True
                        logger.debug(f"Known mixed-type issue (will auto-fix): {error}")
                    else:
                        has_other_errors = True
                        logger.debug(f"Unknown error: {error}")

            # Only FAIL on unknown errors (not mixed-type errors in action/cost_per columns)
            if has_other_errors:
                logger.error(
                    f"Early validation FAILED for {source}/{table} with unknown errors"
                )
                from shared.data_validator import print_validation_report

                print_validation_report(validation_result)
                raise ValueError(
                    f"Data validation failed with {len(validation_result['errors'])} errors. "
                    f"Fix schema issues before extraction. See validation report above."
                )
            elif has_known_mixed_type_errors:
                logger.info(
                    f"Validation found mixed-type issues in Facebook API columns - these will be auto-converted to strings"
                )
                logger.info(
                    f"   Patterns detected: {', '.join(known_mixed_type_patterns)}"
                )
                logger.info(
                    f"Continuing extraction (type conversion at line 904+ will handle mixed types)"
                )

            # Log warnings but continue
            if validation_result["warnings"]:
                logger.debug(
                    f"Early validation found {len(validation_result['warnings'])} warnings for {source}/{table}"
                )
                logger.debug("Warnings will be handled during type conversion")

        logger.debug(f"Processing {len(data)} records for {source}/{table}")

        # Create temporary directory if not provided
        if not local_temp_dir:
            temp_dir = tempfile.mkdtemp(prefix="orchestrator_")
        else:
            temp_dir = local_temp_dir
            os.makedirs(temp_dir, exist_ok=True)

        # Create filename with job timestamp (include table name to avoid collisions
        # when multiple tables share the same temp_dir and job_id)
        filename = f"{job_id}_{table}.parquet"
        file_path = os.path.join(temp_dir, filename)

        # =======================================================================
        # PHASE 1 ARCHITECTURE: Chunked PyArrow processing for ALL datasets
        # =======================================================================
        # This approach:
        # 1. Uses a SAMPLE for schema discovery (not full dataset)
        # 2. Updates YAML if new columns are found
        # 3. Processes ALL data in chunks using PyArrow
        # 4. Never loads full dataset into memory at once
        # =======================================================================

        import pyarrow as pa
        import pyarrow.parquet as pq
        import json

        CHUNK_SIZE_RECORDS = 100_000
        SAMPLE_SIZE = 1000

        config_path = f"configs/{source}.yaml"
        total_rows = len(data)

        logger.info(f"Processing {total_rows:,} records using chunked PyArrow pipeline")

        # ---------------------------------------------------------------------
        # STEP 1: Get authoritative schema from YAML or production
        # ---------------------------------------------------------------------
        yaml_schema = read_yaml_schema(config_path, table)

        # Read force_string_fields configuration from YAML
        force_string_fields = []
        try:
            import yaml as yaml_module

            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    config_data = yaml_module.safe_load(f)
                    if config_data and "resources" in config_data:
                        for resource_name, resource_config in config_data[
                            "resources"
                        ].items():
                            if isinstance(resource_config, dict):
                                if (
                                    resource_name == table
                                    or resource_config.get("table_name") == table
                                ):
                                    force_string_fields = resource_config.get(
                                        "force_string_fields", []
                                    )
                                    if force_string_fields:
                                        logger.debug(
                                            f"Loaded force_string_fields for {table}: {force_string_fields}"
                                        )
                                    break
        except Exception as e:
            logger.debug(f"Could not read force_string_fields from config: {e}")

        production_schema = None
        if bq_client and production_table_id:
            try:
                prod_table = bq_client.get_table(production_table_id)
                production_schema = {
                    field.name: field.field_type for field in prod_table.schema
                }
            except NotFound:
                logger.debug(
                    f"Production table {production_table_id} not found (first run)"
                )
            except Exception as e:
                logger.debug(f"Could not get production schema: {e}")

        base_schema = yaml_schema or production_schema

        # ---------------------------------------------------------------------
        # STEP 2: Use DuckDB for schema discovery if rebuild_mode or no schema
        # ---------------------------------------------------------------------
        duckdb_schema = None
        if rebuild_mode or base_schema is None:
            if rebuild_mode:
                logger.info(
                    "REBUILD MODE: Using PyArrow + DuckDB for accurate schema discovery"
                )
            else:
                logger.info(
                    "No authoritative schema available - using PyArrow + DuckDB for discovery"
                )

            # Use sample for schema discovery to avoid memory issues
            sample_for_discovery = data[: min(SAMPLE_SIZE * 10, total_rows)]
            logger.info(
                f"   Analyzing {len(sample_for_discovery):,} sample records to discover types"
            )

            try:
                from shared.schema_discovery import discover_schema_with_pyarrow_duckdb

                duckdb_schema = discover_schema_with_pyarrow_duckdb(
                    sample_for_discovery,
                    table,
                    batch_size=10000,
                    force_string_fields=force_string_fields,
                )
                if duckdb_schema:
                    logger.info(
                        f"DuckDB discovered schema with {len(duckdb_schema)} columns"
                    )
                else:
                    logger.warning("DuckDB schema discovery returned empty")
            except Exception as e:
                logger.warning(f"DuckDB schema discovery failed: {e}")
        else:
            logger.info(
                "Authoritative schema found (YAML/production); skipping DuckDB discovery"
            )

        # ---------------------------------------------------------------------
        # STEP 3: Merge schemas (YAML/Production base + DuckDB for new fields)
        # ---------------------------------------------------------------------
        if duckdb_schema and (yaml_schema or production_schema):
            base = yaml_schema or production_schema
            authoritative_schema = dict(base)
            duckdb_only = 0
            for field_name, field_type in duckdb_schema.items():
                if field_name not in authoritative_schema:
                    authoritative_schema[field_name] = field_type
                    duckdb_only += 1
            if duckdb_only > 0:
                logger.info(
                    f"Merged schema: {len(base)} base + {duckdb_only} discovered = {len(authoritative_schema)} fields"
                )
        else:
            authoritative_schema = (
                yaml_schema or production_schema or duckdb_schema or {}
            )

        # ---------------------------------------------------------------------
        # STEP 4: Detect new columns from sample and update YAML if needed
        # ---------------------------------------------------------------------
        sample_data = data[: min(SAMPLE_SIZE, total_rows)]
        sample_df = pd.DataFrame(sample_data)

        existing_columns = (
            set(authoritative_schema.keys()) if authoritative_schema else set()
        )
        new_columns = [col for col in sample_df.columns if col not in existing_columns]

        if new_columns:
            sorted_new_columns = sorted(new_columns)
            logger.info(
                f"Detected {len(sorted_new_columns)} new column(s): {sorted_new_columns}"
            )
            for col in sorted_new_columns:
                override_type = SYSTEM_FIELD_TYPE_OVERRIDES.get(col)
                assigned_type = override_type or "STRING"
                authoritative_schema[col] = assigned_type

            # Update YAML with new columns
            if config_path and os.path.exists(config_path):
                try:
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    backup_path = f"{config_path}.bak_{timestamp}"
                    shutil.copy2(config_path, backup_path)
                    logger.info(f"Created YAML backup: {backup_path}")

                    update_stats = update_yaml_schema_selective(
                        config_path, table, authoritative_schema, "runtime_auto_update"
                    )
                    if update_stats.get("updated"):
                        logger.info(
                            f"Updated YAML schema: +{update_stats['columns_added']} columns"
                        )
                except Exception as yaml_err:
                    logger.warning(f"Failed to auto-update YAML: {yaml_err}")

        del sample_df  # Free sample memory

        # ---------------------------------------------------------------------
        # STEP 5: Build PyArrow schema from authoritative schema
        # ---------------------------------------------------------------------
        if not authoritative_schema:
            # Fallback: infer from sample
            logger.warning("No schema available - inferring from sample")
            sample_df = pd.DataFrame(data[: min(SAMPLE_SIZE, total_rows)])
            authoritative_schema = {}
            for col in sample_df.columns:
                if col in force_string_fields:
                    authoritative_schema[col] = "STRING"
                elif pd.api.types.is_datetime64_any_dtype(sample_df[col]):
                    authoritative_schema[col] = "TIMESTAMP"
                elif pd.api.types.is_integer_dtype(sample_df[col]):
                    authoritative_schema[col] = "INT64"
                elif pd.api.types.is_float_dtype(sample_df[col]):
                    authoritative_schema[col] = "FLOAT64"
                elif pd.api.types.is_bool_dtype(sample_df[col]):
                    authoritative_schema[col] = "BOOLEAN"
                else:
                    authoritative_schema[col] = "STRING"
            del sample_df

        # Build PyArrow schema
        parquet_fields = []
        for field_name, field_type in authoritative_schema.items():
            if field_type == "STRING":
                pa_type = pa.string()
            elif field_type in ["INT64", "INTEGER", "INT", "BIGINT"]:
                pa_type = pa.int64()
            elif field_type in ["FLOAT64", "FLOAT", "NUMERIC", "DECIMAL", "BIGNUMERIC"]:
                pa_type = pa.float64()
            elif field_type in ["BOOL", "BOOLEAN"]:
                pa_type = pa.bool_()
            elif field_type in ["TIMESTAMP", "DATETIME"]:
                pa_type = pa.timestamp("us", tz="UTC")
            elif field_type == "DATE":
                pa_type = pa.date32()
            else:
                pa_type = pa.string()
            parquet_fields.append(pa.field(field_name, pa_type, nullable=True))

        parquet_schema = pa.schema(parquet_fields)
        logger.info(f"Using schema with {len(parquet_fields)} fields")

        # ---------------------------------------------------------------------
        # STEP 6: Process data in chunks and write to Parquet
        # ---------------------------------------------------------------------
        def convert_value_to_string(x):
            """Convert any value to string, handling complex types."""
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            if isinstance(x, str):
                return x
            if isinstance(x, (dict, list)):
                try:
                    return json.dumps(x)
                except (TypeError, ValueError):
                    return str(x)
            if hasattr(x, "isoformat"):
                return x.isoformat()
            return str(x)

        writer = None
        rows_written = 0

        for start_idx in range(0, total_rows, CHUNK_SIZE_RECORDS):
            end_idx = min(start_idx + CHUNK_SIZE_RECORDS, total_rows)
            chunk_data = data[start_idx:end_idx]

            # Create DataFrame for this chunk only
            chunk_df = pd.DataFrame(chunk_data)

            # Ensure all schema columns exist
            for field_name in authoritative_schema.keys():
                if field_name not in chunk_df.columns:
                    chunk_df[field_name] = None

            # Apply schema enforcement to chunk
            for col_name in list(chunk_df.columns):
                if col_name in authoritative_schema:
                    bq_type = authoritative_schema[col_name]

                    if bq_type == "STRING" or col_name in force_string_fields:
                        chunk_df[col_name] = chunk_df[col_name].apply(
                            convert_value_to_string
                        )
                    elif bq_type in ["INT64", "INTEGER", "INT", "BIGINT"]:
                        chunk_df[col_name] = pd.to_numeric(
                            chunk_df[col_name], errors="coerce"
                        ).astype("Int64")
                    elif bq_type in [
                        "FLOAT64",
                        "FLOAT",
                        "NUMERIC",
                        "DECIMAL",
                        "BIGNUMERIC",
                    ]:
                        chunk_df[col_name] = pd.to_numeric(
                            chunk_df[col_name], errors="coerce"
                        ).astype("float64")
                    elif bq_type in ["BOOL", "BOOLEAN"]:
                        bool_map = {
                            True: True,
                            False: False,
                            "true": True,
                            "false": False,
                            "True": True,
                            "False": False,
                            1: True,
                            0: False,
                            "1": True,
                            "0": False,
                            "yes": True,
                            "no": False,
                        }
                        chunk_df[col_name] = chunk_df[col_name].map(bool_map)
                    elif bq_type in ["TIMESTAMP", "DATETIME"]:
                        chunk_df[col_name] = pd.to_datetime(
                            chunk_df[col_name], errors="coerce"
                        )
                        if (
                            hasattr(chunk_df[col_name].dtype, "tz")
                            and chunk_df[col_name].dtype.tz is not None
                        ):
                            chunk_df[col_name] = chunk_df[col_name].dt.tz_localize(None)
                        chunk_df[col_name] = chunk_df[col_name].dt.floor("min")
                        if len(chunk_df) > 0:
                            try:
                                chunk_df[col_name] = chunk_df[col_name].astype(
                                    "datetime64[us]"
                                )
                            except Exception:
                                pass
                    elif bq_type == "DATE":
                        chunk_df[col_name] = pd.to_datetime(
                            chunk_df[col_name], errors="coerce"
                        ).dt.date
                else:
                    # Column not in schema - convert to string
                    chunk_df[col_name] = chunk_df[col_name].apply(
                        convert_value_to_string
                    )

            # Keep only columns in schema (in schema order)
            schema_columns = list(authoritative_schema.keys())
            chunk_df = chunk_df[
                [col for col in schema_columns if col in chunk_df.columns]
            ]

            # Convert chunk to PyArrow Table
            try:
                table_chunk = pa.Table.from_pandas(
                    chunk_df, schema=parquet_schema, preserve_index=False
                )
            except Exception as conv_err:
                logger.warning(
                    f"Schema conversion issue in chunk, using flexible conversion: {conv_err}"
                )
                table_chunk = pa.Table.from_pandas(chunk_df, preserve_index=False)

            # Write chunk to Parquet file
            if writer is None:
                writer = pq.ParquetWriter(
                    file_path, schema=parquet_schema, compression="snappy"
                )

            writer.write_table(table_chunk)
            rows_written += len(chunk_df)

            # Log progress every 500k rows or at completion
            if rows_written % 500_000 < CHUNK_SIZE_RECORDS or end_idx == total_rows:
                pct = rows_written * 100 // total_rows if total_rows > 0 else 100
                logger.info(
                    f"   Processed {rows_written:,}/{total_rows:,} rows ({pct}%)"
                )

            # Free memory
            del chunk_df
            del chunk_data
            del table_chunk

        if writer:
            writer.close()

        # Report file size
        file_size = os.path.getsize(file_path)
        if file_size < 1024:
            size_str = f"{file_size} bytes"
        elif file_size < 1024 * 1024:
            size_str = f"{file_size / 1024:.1f} KB"
        else:
            size_str = f"{file_size / (1024 * 1024):.1f} MB"

        logger.info(f"   Extracted {rows_written:,} rows to Parquet ({size_str})")

        # Store schema info for rebuild mode YAML updates
        if rebuild_mode and duckdb_schema:
            logger.info(
                "DuckDB schema will be saved to YAML after production table creation"
            )

        return file_path

    except Exception as e:
        logger.error(f"STEP 1 Failed: {e}")
        raise


def stream_chunks_to_local_parquet(
    chunk_iter: Iterable["pd.DataFrame"],
    source: str,
    table: str,
    job_id: str,
    local_temp_dir: Optional[str] = None,
    bq_client: Optional["bigquery.Client"] = None,
    production_table_id: Optional[str] = None,
) -> Optional[str]:
    """
    Streaming variant of extract_to_local_parquet for large datasets.

    Unlike extract_to_local_parquet which requires the full record list to be
    materialized in memory (it indexes data[i:j] and computes len(data)), this
    function consumes a GENERATOR of DataFrame chunks and writes each chunk
    straight to Parquet via pyarrow.parquet.ParquetWriter. Peak memory stays
    bounded at one chunk's worth, regardless of total row count.

    Built for the NPI registry extractor, where the source CSV is ~9-10 million
    rows (~1.1 GB compressed, ~10 GB+ as Python dicts). The non-streaming path
    accumulates every chunk into a Python list before calling
    extract_to_local_parquet, causing the kernel OOM-killer to terminate the
    process at ~7-8M rows. Switching to this helper keeps the process under
    typical worker-VM memory ceilings.

    Schema enforcement matches extract_to_local_parquet's behavior:
      - The authoritative schema is resolved from the YAML (configs/<source>.yaml,
        resources.<table>.schema) with a fallback to the production BigQuery
        table's schema. We do NOT run DuckDB schema discovery here; the caller
        is responsible for having the YAML / production schema in place. For
        truly new tables, use extract_to_local_parquet for the first run.
      - Each incoming chunk_df is coerced column-by-column to the BQ type
        declared in the authoritative schema (STRING / INT64 / FLOAT64 / BOOL /
        TIMESTAMP / DATETIME / DATE), missing schema columns are added as NULL,
        and the chunk is reordered to schema order before write.

    Args:
        chunk_iter: iterable (typically a generator) yielding pandas DataFrames.
            Each chunk should already have columns named per the schema. Empty
            chunks are skipped. If the iterator yields nothing, returns None.
        source: source system name (used for log strings and YAML lookup).
        table: table name (used in the output filename and YAML resource lookup).
        job_id: unique job identifier, used in the output filename.
        local_temp_dir: optional directory for the parquet file. A tempdir
            is created if not provided.
        bq_client: BigQuery client. Used only when production_table_id is set
            and the YAML schema is missing -- to fall back to the production
            table's schema.
        production_table_id: full table id 'project.dataset.table'. Used only
            as a schema fallback when the YAML schema is missing.

    Returns:
        Path to the created local parquet file, or None when the iterator
        yielded no chunks (no data to write).

    Raises:
        ValueError: if no authoritative schema can be resolved.
        Exception: any pyarrow / pandas write error is re-raised after closing
            the ParquetWriter to avoid leaving a partial file open.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    import json

    # ----- 1. Resolve authoritative schema (YAML > production table). --------
    config_path = f"configs/{source}.yaml"
    yaml_schema = read_yaml_schema(config_path, table)

    # force_string_fields override from the resource block (e.g. IDs that are
    # numeric in the source but must be persisted as STRING).
    force_string_fields: List[str] = []
    try:
        import yaml as yaml_module

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_data = yaml_module.safe_load(f)
            if config_data and "resources" in config_data:
                for resource_name, resource_config in config_data["resources"].items():
                    if not isinstance(resource_config, dict):
                        continue
                    if (
                        resource_name == table
                        or resource_config.get("table_name") == table
                    ):
                        force_string_fields = (
                            resource_config.get("force_string_fields") or []
                        )
                        break
    except Exception as e:
        logger.debug(f"Could not read force_string_fields from {config_path}: {e}")

    production_schema = None
    if bq_client and production_table_id:
        try:
            prod_table = bq_client.get_table(production_table_id)
            production_schema = {
                field.name: field.field_type for field in prod_table.schema
            }
        except NotFound:
            logger.debug(
                f"Production table {production_table_id} not found "
                "(first run; YAML schema must be supplied)"
            )
        except Exception as e:
            logger.debug(f"Could not get production schema: {e}")

    authoritative_schema = yaml_schema or production_schema
    if not authoritative_schema:
        raise ValueError(
            f"stream_chunks_to_local_parquet({source}/{table}): no "
            "authoritative schema available. The YAML schema or production "
            "BigQuery table must exist before the streaming path can be used. "
            "For brand-new tables, run extract_to_local_parquet for the first "
            "load so DuckDB schema discovery can populate the schema."
        )

    # ----- 2. Build PyArrow schema from the authoritative dict. --------------
    parquet_fields = []
    for field_name, field_type in authoritative_schema.items():
        if field_type == "STRING":
            pa_type = pa.string()
        elif field_type in ("INT64", "INTEGER", "INT", "BIGINT"):
            pa_type = pa.int64()
        elif field_type in ("FLOAT64", "FLOAT", "NUMERIC", "DECIMAL", "BIGNUMERIC"):
            pa_type = pa.float64()
        elif field_type in ("BOOL", "BOOLEAN"):
            pa_type = pa.bool_()
        elif field_type in ("TIMESTAMP", "DATETIME"):
            pa_type = pa.timestamp("us", tz="UTC")
        elif field_type == "DATE":
            pa_type = pa.date32()
        else:
            pa_type = pa.string()
        parquet_fields.append(pa.field(field_name, pa_type, nullable=True))
    parquet_schema = pa.schema(parquet_fields)

    # ----- 3. Set up the output file. ----------------------------------------
    if not local_temp_dir:
        temp_dir = tempfile.mkdtemp(prefix="orchestrator_")
    else:
        temp_dir = local_temp_dir
        os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, f"{job_id}_{table}.parquet")

    def convert_value_to_string(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        if isinstance(x, str):
            return x
        if isinstance(x, (dict, list)):
            try:
                return json.dumps(x)
            except (TypeError, ValueError):
                return str(x)
        if hasattr(x, "isoformat"):
            return x.isoformat()
        return str(x)

    schema_columns = list(authoritative_schema.keys())

    # ----- 4. Stream chunks through the writer. ------------------------------
    writer = None
    rows_written = 0
    chunk_count = 0
    try:
        for chunk_df in chunk_iter:
            if chunk_df is None or len(chunk_df) == 0:
                continue
            chunk_count += 1

            # Ensure all schema columns exist on this chunk.
            for field_name in schema_columns:
                if field_name not in chunk_df.columns:
                    chunk_df[field_name] = None

            # Coerce each column to its BQ type. Mirrors the per-chunk loop in
            # extract_to_local_parquet so behavior is consistent.
            for col_name in list(chunk_df.columns):
                if col_name in authoritative_schema:
                    bq_type = authoritative_schema[col_name]
                    if bq_type == "STRING" or col_name in force_string_fields:
                        chunk_df[col_name] = chunk_df[col_name].apply(
                            convert_value_to_string
                        )
                    elif bq_type in ("INT64", "INTEGER", "INT", "BIGINT"):
                        chunk_df[col_name] = pd.to_numeric(
                            chunk_df[col_name], errors="coerce"
                        ).astype("Int64")
                    elif bq_type in (
                        "FLOAT64",
                        "FLOAT",
                        "NUMERIC",
                        "DECIMAL",
                        "BIGNUMERIC",
                    ):
                        chunk_df[col_name] = pd.to_numeric(
                            chunk_df[col_name], errors="coerce"
                        ).astype("float64")
                    elif bq_type in ("BOOL", "BOOLEAN"):
                        bool_map = {
                            True: True,
                            False: False,
                            "true": True,
                            "false": False,
                            "True": True,
                            "False": False,
                            1: True,
                            0: False,
                            "1": True,
                            "0": False,
                            "yes": True,
                            "no": False,
                        }
                        chunk_df[col_name] = chunk_df[col_name].map(bool_map)
                    elif bq_type in ("TIMESTAMP", "DATETIME"):
                        chunk_df[col_name] = pd.to_datetime(
                            chunk_df[col_name], errors="coerce"
                        )
                        if (
                            hasattr(chunk_df[col_name].dtype, "tz")
                            and chunk_df[col_name].dtype.tz is not None
                        ):
                            chunk_df[col_name] = chunk_df[col_name].dt.tz_localize(None)
                        chunk_df[col_name] = chunk_df[col_name].dt.floor("min")
                        if len(chunk_df) > 0:
                            try:
                                chunk_df[col_name] = chunk_df[col_name].astype(
                                    "datetime64[us]"
                                )
                            except Exception:
                                pass
                    elif bq_type == "DATE":
                        chunk_df[col_name] = pd.to_datetime(
                            chunk_df[col_name], errors="coerce"
                        ).dt.date
                else:
                    # Column not in schema -- string-encode it (matches the
                    # non-streaming path's defensive behavior).
                    chunk_df[col_name] = chunk_df[col_name].apply(
                        convert_value_to_string
                    )

            # Reorder to schema order, dropping any columns the schema doesn't
            # declare. Off-schema columns would otherwise blow up the PyArrow
            # conversion against parquet_schema.
            chunk_df = chunk_df[
                [col for col in schema_columns if col in chunk_df.columns]
            ]

            # Convert to PyArrow Table against the resolved schema.
            try:
                table_chunk = pa.Table.from_pandas(
                    chunk_df, schema=parquet_schema, preserve_index=False
                )
            except Exception as conv_err:
                logger.warning(
                    f"Streaming chunk {chunk_count} for {source}/{table}: schema "
                    f"conversion fell back to flexible: {conv_err}"
                )
                table_chunk = pa.Table.from_pandas(chunk_df, preserve_index=False)

            # Open the writer lazily so we know we have at least one row.
            if writer is None:
                writer = pq.ParquetWriter(
                    file_path, schema=parquet_schema, compression="snappy"
                )

            writer.write_table(table_chunk)
            rows_written += len(chunk_df)

            # Periodic progress log.
            if rows_written % 500_000 < len(chunk_df) or rows_written == len(chunk_df):
                logger.info(
                    f"   Streamed {rows_written:,} rows to Parquet "
                    f"({chunk_count} chunk(s))"
                )

            # Free this chunk's memory before the next iteration. Without
            # these deletes the previous chunk_df / table_chunk would linger
            # until the next loop's reassignment.
            del chunk_df
            del table_chunk
    finally:
        if writer is not None:
            writer.close()

    if rows_written == 0:
        logger.info(f"No data streamed for {source}/{table} -- removing empty file.")
        try:
            os.remove(file_path)
        except OSError:
            pass
        return None

    file_size = os.path.getsize(file_path)
    if file_size < 1024:
        size_str = f"{file_size} bytes"
    elif file_size < 1024 * 1024:
        size_str = f"{file_size / 1024:.1f} KB"
    else:
        size_str = f"{file_size / (1024 * 1024):.1f} MB"
    logger.info(
        f"   Streamed {rows_written:,} rows from {chunk_count} chunk(s) to "
        f"Parquet ({size_str}): {file_path}"
    )
    return file_path


def upload_to_gcs_raw(
    source: str, table: str, job_id: str, local_file_path: str, bucket_name: str
) -> str:
    """
    STEP 2: Copy parquet file to raw GCS bucket.

    Uploads parquet file to GCS raw bucket with structured path:
    raw/YYYY-MM-DD/source/table/job_id.parquet

    Args:
        source: Source system name
        table: Table name
        job_id: Unique job identifier
        local_file_path: Path to local parquet file
        bucket_name: GCS bucket name

    Returns:
        GCS path where file was uploaded

    Example:
        >>> gcs_path = upload_to_gcs_raw('facebook', 'campaigns', 'job_001', '/tmp/file.parquet', 'my-bucket')
        >>> print(f"Uploaded to: {gcs_path}")
    """
    try:
        # Create GCS path structure: raw/YYYY-MM-DD/source/table/job_id.parquet
        date_str = datetime.now().strftime("%Y-%m-%d")
        gcs_path = f"raw/{date_str}/{source}/{table}/{job_id}.parquet"

        # Upload file to GCS using GCS client
        client = gcs.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file_path)

        logger.info(f"STEP 2 Complete: Uploaded to GCS raw bucket")
        logger.info(f"   Bucket: {bucket_name}")
        logger.info(f"   Path: {gcs_path}")

        return gcs_path

    except Exception as e:
        logger.error(f"STEP 2 Failed: {e}")
        raise


def create_external_table_staging(
    client: bigquery.Client,
    project: str,
    source: str,
    table: str,
    job_id: str,
    gcs_path: str,
    bucket_name: str,
    staging_dataset: Optional[str] = None,
) -> str:
    """
    STEP 3: Create external table using parquet file in source_staging dataset.

    Creates BigQuery external table pointing to GCS parquet file
    for initial staging data access.

    Args:
        client: BigQuery client instance
        project: BigQuery project ID
        source: Source system name
        table: Table name
        job_id: Unique job identifier
        gcs_path: GCS path to parquet file
        bucket_name: GCS bucket name
        temp_file_path: Path to temporary file

    Returns:
        Full external table ID

    Example:
        >>> ext_table = create_external_table_staging(client, 'project', 'facebook', 'campaigns', 'job_001', 'gs://bucket/raw/path.parquet')
        >>> print(f"Created external table: {ext_table}")
    """
    try:
        # Define dataset and external table names
        staging_dataset_name = staging_dataset or f"{source}_staging"
        external_table_name = f"{table}_external"

        full_table_id = f"{project}.{staging_dataset_name}.{external_table_name}"

        # Always drop stale external table first (failed runs leave schemas behind)
        try:
            client.delete_table(full_table_id)
            logger.debug(
                f"Deleted existing external table to refresh schema: {full_table_id}"
            )
        except NotFound:
            pass

        # Create external table configuration
        external_config = bigquery.ExternalConfig("PARQUET")

        # Set GCS URI
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        external_config.source_uris = [gcs_uri]

        # Configure external table
        external_config.options.autodetect = True
        external_config.options.max_bad_records = 10

        # Create external table reference
        external_table_ref = bigquery.TableReference.from_string(full_table_id)
        external_table = bigquery.Table(external_table_ref)
        external_table.external_data_configuration = external_config

        # Create external table with fresh schema
        external_table = client.create_table(external_table)

        logger.info(f"STEP 3 Complete: Created external table")
        logger.info(f"   Table: {full_table_id}")
        logger.info(f"   Source: {gcs_uri}")

        dbg_query = client.query(f"SELECT COUNT(*) as row_count FROM `{full_table_id}`")
        dbg_result = next(dbg_query.result())
        logger.info(f"   Rows in external table: {dbg_result.row_count:,}")

        return full_table_id

    except Exception as e:
        logger.error(f"STEP 3 Failed: {e}")
        raise


def _ensure_staging_system_columns(
    client: bigquery.Client, staging_table_id: str, source: str, execution_id: str
) -> None:
    system_columns = [
        ("execution_id", "STRING"),
        ("source", "STRING"),
        ("loaded_at", "TIMESTAMP"),
        ("updated_at", "TIMESTAMP"),
    ]

    try:
        table = client.get_table(staging_table_id)
        field_types = {field.name: field.field_type.upper() for field in table.schema}
    except Exception as exc:
        logger.warning(f"Could not inspect schema for {staging_table_id}: {exc}")
        field_types = {}

    managed_columns: Dict[str, str] = {}

    for column_name, desired_type in system_columns:
        existing_type = field_types.get(column_name)
        if existing_type:
            if existing_type == desired_type:
                managed_columns[column_name] = desired_type
            else:
                logger.debug(
                    "Skipping system column '%s' on %s due to type mismatch (existing=%s, desired=%s)",
                    column_name,
                    staging_table_id,
                    existing_type,
                    desired_type,
                )
            continue

        # Retry ALTER TABLE with exponential backoff to handle concurrent schema modifications
        # BigQuery's "rate limit" error is misleading - it's actually a concurrent modification conflict
        max_retries = 3
        base_delay = 1.0
        added = False

        for attempt in range(max_retries):
            try:
                client.query(
                    f"ALTER TABLE `{staging_table_id}` ADD COLUMN IF NOT EXISTS `{column_name}` {desired_type}"
                ).result()
                managed_columns[column_name] = desired_type
                field_types[column_name] = desired_type
                added = True
                break
            except Exception as exc:
                error_msg = str(exc).lower()
                if "already exists" in error_msg or "duplicate" in error_msg:
                    # Column exists - that's fine
                    managed_columns[column_name] = desired_type
                    field_types[column_name] = desired_type
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
                            f"Concurrent modification on {staging_table_id}.{column_name} - retrying in {retry_delay:.1f}s"
                        )
                        import time

                        time.sleep(retry_delay)

                        # Re-check if column was added by concurrent process
                        try:
                            fresh_table = client.get_table(staging_table_id)
                            fresh_fields = {
                                f.name: f.field_type for f in fresh_table.schema
                            }
                            if column_name in fresh_fields:
                                logger.debug(
                                    f"Column {column_name} added by concurrent process"
                                )
                                managed_columns[column_name] = desired_type
                                field_types[column_name] = fresh_fields[column_name]
                                added = True
                                break
                        except Exception:
                            pass  # Continue retry if re-check fails
                    else:
                        # Final attempt - check one more time
                        try:
                            final_table = client.get_table(staging_table_id)
                            final_fields = {
                                f.name: f.field_type for f in final_table.schema
                            }
                            if column_name in final_fields:
                                logger.debug(
                                    f"Column {column_name} exists after concurrent modifications"
                                )
                                managed_columns[column_name] = desired_type
                                field_types[column_name] = final_fields[column_name]
                                added = True
                                break
                        except Exception:
                            pass
                        logger.warning(
                            f"Could not add {column_name} to {staging_table_id} after {max_retries} attempts due to concurrent modifications"
                        )
                        break
                else:
                    logger.warning(
                        f"Could not add system column {column_name} on {staging_table_id}: {exc}"
                    )
                    break

    assignments = []
    where_clauses = []

    exec_id_value = execution_id or "unknown"
    source_value = source or "unknown"

    if managed_columns.get("execution_id"):
        assignments.append("execution_id = COALESCE(execution_id, @exec_id)")
        where_clauses.append("execution_id IS NULL")

    if managed_columns.get("source"):
        assignments.append("source = COALESCE(source, @source_name)")
        where_clauses.append("source IS NULL")

    if managed_columns.get("loaded_at"):
        loaded_type = field_types.get("loaded_at")
        if loaded_type == "TIMESTAMP":
            assignments.append("loaded_at = COALESCE(loaded_at, CURRENT_TIMESTAMP())")
        elif loaded_type == "DATETIME":
            assignments.append(
                "loaded_at = COALESCE(loaded_at, DATETIME(CURRENT_TIMESTAMP()))"
            )
        elif loaded_type == "STRING":
            assignments.append(
                "loaded_at = COALESCE(loaded_at, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', CURRENT_TIMESTAMP()))"
            )
        else:
            assignments.append("loaded_at = COALESCE(loaded_at, CURRENT_TIMESTAMP())")
        where_clauses.append("loaded_at IS NULL")

    if managed_columns.get("updated_at"):
        updated_type = field_types.get("updated_at")
        if updated_type == "TIMESTAMP":
            assignments.append("updated_at = CURRENT_TIMESTAMP()")
        elif updated_type == "DATETIME":
            assignments.append("updated_at = DATETIME(CURRENT_TIMESTAMP())")
        elif updated_type == "STRING":
            assignments.append(
                "updated_at = FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', CURRENT_TIMESTAMP())"
            )
        else:
            assignments.append("updated_at = CURRENT_TIMESTAMP()")

    if not assignments:
        return

    set_clause = ",\n        ".join(assignments)
    where_clause = ""
    if where_clauses:
        where_clause = "WHERE " + "\n       OR ".join(where_clauses)

    update_query = f"""
    UPDATE `{staging_table_id}`
    SET {set_clause}
    {where_clause}
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("exec_id", "STRING", exec_id_value),
            bigquery.ScalarQueryParameter("source_name", "STRING", source_value),
        ]
    )
    client.query(update_query, job_config=job_config).result()


def transform_staging_to_production(
    client: bigquery.Client,
    project: str,
    source: str,
    table: str,
    external_table_id: str,
    job_id: str,
    staging_dataset: Optional[str] = None,
    production_dataset: Optional[str] = None,
    primary_keys: Optional[List[str]] = None,
    merge_config: Optional[Dict[str, Any]] = None,
    base_table_name: Optional[str] = None,
) -> bool:
    """
    STEP 4: Create/transform staging table to source_data dataset.

    Performs hash-based merge from external table to production dataset
    with proper schema evolution and deduplication.

    Args:
        client: BigQuery client instance
        project: BigQuery project ID
        source: Source system name
        table: Table name (with any affixes applied)
        external_table_id: External table ID to copy from
        job_id: Unique job identifier
        base_table_name: Logical table name used for schema evolution/YAML updates

    Returns:
        True if transformation successful

    Example:
        >>> success = transform_staging_to_production(client,('facebook', 'campaigns', 'project.facebook_STAGING.campaigns_job001')
        >>> print(f"Transform success: {success}")
    """
    try:
        staging_dataset_name = staging_dataset or f"{source}_staging"
        production_dataset_name = production_dataset or f"{source}_data"

        staging_table_id = f"{project}.{staging_dataset_name}.{table}"
        production_table_id = f"{project}.{production_dataset_name}.{table}"
        logical_table = base_table_name or table

        keys: List[str]
        if not primary_keys:
            keys = ["id"]
        elif isinstance(primary_keys, str):
            keys = [primary_keys]
        else:
            keys = list(primary_keys)

        logger.info(f"Materializing staging table: {staging_table_id}")
        materialize_sql = f"""
        CREATE OR REPLACE TABLE `{staging_table_id}` AS
        SELECT * FROM `{external_table_id}`
        """
        client.query(materialize_sql).result()

        merge_cfg = dict(merge_config) if merge_config else {}
        merge_cfg.setdefault("source", source)
        merge_cfg.setdefault("execution_id", job_id)
        merge_cfg.setdefault("rebuild", False)
        merge_cfg.setdefault(
            "refresh_mode", "incremental"
        )  # Default safe: no DELETE on incremental

        _ensure_staging_system_columns(
            client, staging_table_id, source, merge_cfg.get("execution_id")
        )

        schema_source = merge_cfg.get("source") or source
        schema_success, schema_warnings = apply_schema_evolution(
            client=client,
            source=schema_source,
            table=logical_table,
            staging_table_id=staging_table_id,
            production_table_id=production_table_id,
            execution_id=merge_cfg.get("execution_id"),
            auto_add_columns=True,
            allow_type_changes=False,
        )

        if schema_warnings:
            for warning in schema_warnings:
                logger.warning(warning)

        if not schema_success:
            raise RuntimeError(f"Schema validation failed for {logical_table}")

        stats = process_hash_merge(
            client=client,
            staging_table=staging_table_id,
            main_table=production_table_id,
            primary_keys=keys,
            config=merge_cfg,
        )

        logger.info("STEP 4 Complete: Transformed to production")
        logger.info(f"   Merge stats: {stats}")

        prod_count_query = client.query(
            f"SELECT COUNT(*) as count FROM `{production_table_id}`"
        )
        prod_count = next(prod_count_query.result()).count
        logger.info(f"   Production table rows: {prod_count:,}")

        # Staging retention: keep the staging table after merge so it stays
        # queryable in BigQuery for run-over-run validation. The next run of
        # this table archives it (snapshot + manifest) and drops it via the
        # scoped staging lifecycle (shared/staging_lifecycle.py). Opt back
        # into the legacy drop-after-merge with merge_config truncate_staging.
        if merge_cfg.get("truncate_staging", False):
            try:
                client.delete_table(staging_table_id, not_found_ok=True)
                logger.debug(f"Deleted staging table: {staging_table_id}")
            except Exception as e:
                logger.warning(
                    f"Could not delete staging table {staging_table_id}: {e}"
                )
        else:
            logger.info(f"   Staging retained for validation: {staging_table_id}")

        return True

    except Exception as e:
        logger.error(f"STEP 4 Failed: {e}")
        raise


def archive_external_table(
    client: bigquery.Client,
    project: str,
    source: str,
    table: str,
    job_id: str,
    gcs_path: str,
    bucket_name: str,
    external_table_id: str,
    ttl_days: int = 30,
) -> str:
    """
    STEP 5: Move external table to source_archive dataset.

    After successful production load, moves external table to archive
    dataset with TTL for automatic cleanup.

    Args:
        client: BigQuery client instance
        project: BigQuery project ID
        source: Source system name
        table: Table name
        job_id: Unique job identifier
        gcs_path: GCS path to parquet file
        bucket_name: GCS bucket name
        external_table_id: Current external table ID
        ttl_days: Time-to-live in days for archive tables

    Returns:
        Archive external table ID

    Example:
        >>> archive_table = archive_external_table(client,'project','facebook','campaigns','job_001',...)
        >>> print(f"Archived table: {archive_table}")
    """
    try:
        # Delete original external table (if it still exists)
        client.delete_table(external_table_id, not_found_ok=True)
        logger.info(f"Deleted staging external table: {external_table_id}")

        # Create archive dataset if not exists
        archive_dataset = f"{source}_archive"
        try:
            client.get_dataset(f"{project}.{archive_dataset}")
        except:
            archive_dataset_ref = bigquery.DatasetReference.from_string(
                f"{project}.{archive_dataset}"
            )
            archive_dataset_obj = bigquery.Dataset(archive_dataset_ref)
            archive_dataset_obj.location = "US"
            client.create_dataset(archive_dataset_obj)

        # Create archive external table
        archive_table_name = f"{table}_{job_id}"
        archive_table_id = f"{project}.{archive_dataset}.{archive_table_name}"

        # Configure archive external table
        archive_config = bigquery.ExternalConfig("PARQUET")
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        archive_config.source_uris = [gcs_uri]
        archive_config.options.autodetect = True

        # Create archive external table
        archive_table_ref = bigquery.TableReference.from_string(archive_table_id)
        archive_table = bigquery.Table(archive_table_ref)
        archive_table.external_data_configuration = archive_config

        # Add TTL metadata and a real expiration (the description alone never
        # expired anything; hundreds of these accumulated in *_archive).
        archive_table.description = (
            f"Archive table for {source}/{table} job {job_id}. TTL: {ttl_days} days"
        )
        from datetime import timedelta, timezone

        archive_table.expires = datetime.now(timezone.utc) + timedelta(days=ttl_days)

        archive_table = client.create_table(archive_table)

        logger.info(f"STEP 5 Complete: Created archive table")
        logger.info(f"   Archive table: {archive_table_id}")
        logger.info(f"   Source: {gcs_uri}")
        logger.info(f"   TTL: {ttl_days} days")

        return archive_table_id

    except Exception as e:
        logger.error(f"STEP 5 Failed: {e}")
        raise


def cleanup_local_files(
    local_file_path: str,
    job_id: str,
    cleanup_on_success: bool = True,
    cleanup_on_failure: bool = False,
) -> None:
    """
    STEP 6: Clean up local files after processing.

    Optionally removes temporary local parquet files based on
    processing outcome and configuration.

    Args:
        local_file_path: Path to local parquet file
        job_id: Job identifier for logging
        cleanup_on_success: Whether to cleanup on successful completion
        cleanup_on_failure: Whether to cleanup on failure (debug mode)

    Example:
        >>> cleanup_local_files('/tmp/file.parquet', 'job_001')
    """
    try:
        if os.path.exists(local_file_path):
            if cleanup_on_success or cleanup_on_failure:
                os.remove(local_file_path)
                logger.info(f"STEP 6 Complete: Cleaned up local file")
                logger.info(f"   Deleted: {local_file_path}")
            else:
                logger.info(f"Preserving local file: {local_file_path}")
        else:
            logger.info(f"No local file to clean up")

    except Exception as e:
        logger.error(f"STEP 6 Failed: {e}")


def execute_full_pipeline(
    data: Optional[List[Dict[str, Any]]],
    source: str,
    table: str,
    bq_client: bigquery.Client,
    project: str,
    bucket_name: str,
    job_id: str,
    ttl_days: int = 30,
    cleanup_local: bool = True,
    staging_dataset: Optional[str] = None,
    production_dataset: Optional[str] = None,
    primary_keys: Optional[List[str]] = None,
    merge_config: Optional[Dict[str, Any]] = None,
    prebuilt_parquet_path: Optional[str] = None,
    row_count_override: Optional[int] = None,
    base_table_name: Optional[str] = None,
    authoritative_schema: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Execute complete 6-step GCS pipeline workflow.

    Orchestrates the full data lake pipeline from extraction
    through archival and cleanup.

    Args:
        data: Extracted data records (optional when using prebuilt_parquet_path)
        source: Source system name
        table: Table name
        bq_client: BigQuery client instance
        project: BigQuery project ID
        bucket_name: GCS bucket for raw data
        job_id: Unique job identifier
        ttl_days: TTL in days for archive tables
        cleanup_local: Whether to cleanup local files
        base_table_name: Logical table name used for schema registry updates (defaults to table)
        authoritative_schema: Optional schema mapping discovered upstream (column -> BigQuery type)

    Returns:
        Dictionary with pipeline results and metadata

    Example:
        >>> results = execute_full_pipeline(data,'facebook','campaigns',bq_client,'project','bucket','job001')
        >>> print(f"Pipeline result: {results}")
    """

    logical_table = base_table_name or table

    pipeline_stats = {
        "job_id": job_id,
        "source": source,
        "table": table,
        "base_table": logical_table,
        "total_rows": len(data) if data is not None else (row_count_override or 0),
        "steps_completed": 0,
        "steps_failed": 0,
        "local_file_path": None,
        "gcs_path": None,
        "external_table_id": None,
        "archive_table_id": None,
        "execution_time": None,
    }

    start_time = datetime.now()
    processing_id = None

    # Resolve datasets for tracking
    staging_dataset_name = staging_dataset or f"{source}_staging"
    production_dataset_name = production_dataset or f"{source}_data"

    # Extract config values from merge_config if available
    # Use dict.get() with defaults to handle missing keys safely
    execution_id = None
    refresh_mode = "incremental"
    test_mode = False
    if merge_config and isinstance(merge_config, dict):
        execution_id = merge_config.get("execution_id")
        refresh_mode = merge_config.get("refresh_mode", "incremental")
        test_mode = merge_config.get("test_mode", False)

    try:
        logger.info(f"\nStarting 6-step GCS pipeline for {source}/{table}")
        logger.info(f"Job ID: {job_id}")
        if data is not None:
            logger.info(f"Rows to process: {len(data):,}")
        elif row_count_override is not None:
            logger.info(f"Rows to process: {row_count_override:,}")
        else:
            logger.info("Rows to process: unknown (prebuilt parquet)")

        # Initialize table processing tracking
        try:
            processing_id = start_table_processing(
                job_id=job_id,
                execution_id=execution_id or job_id,
                source=source,
                table_name=logical_table,
                project_id=project,
                staging_dataset=staging_dataset_name,
                production_dataset=production_dataset_name,
                environment="prod",
                refresh_mode=refresh_mode,
                test_mode=test_mode,
                primary_keys=primary_keys or ["id"],
            )
            logger.debug(f"Started table processing tracking: {processing_id}")
        except Exception as track_err:
            logger.warning(
                f"Failed to start table tracking (continuing without): {track_err}"
            )
            processing_id = None

        # STEP 1: Extract to local parquet
        logger.info("\nSTEP 1: Extract to local parquet file...")
        if prebuilt_parquet_path:
            logger.info(f"Using prebuilt parquet file: {prebuilt_parquet_path}")
            if not os.path.exists(prebuilt_parquet_path):
                raise FileNotFoundError(
                    f"Prebuilt parquet file not found: {prebuilt_parquet_path}"
                )
            local_file_path = prebuilt_parquet_path
        else:
            if data is None:
                raise ValueError(
                    "execute_full_pipeline requires data or prebuilt_parquet_path"
                )
            local_file_path = extract_to_local_parquet(data, source, table, job_id)
        pipeline_stats["local_file_path"] = local_file_path
        pipeline_stats["steps_completed"] += 1

        # Track extraction metrics
        extraction_end = datetime.now()
        rows_extracted = len(data) if data is not None else 0
        parquet_size_mb = (
            os.path.getsize(local_file_path) / (1024 * 1024)
            if os.path.exists(local_file_path)
            else 0
        )
        pipeline_stats["rows_extracted"] = rows_extracted
        pipeline_stats["parquet_file_size_mb"] = round(parquet_size_mb, 2)

        if processing_id:
            try:
                update_table_processing(
                    processing_id,
                    phase="extraction",
                    extraction_completed_at=extraction_end,
                    rows_extracted=rows_extracted,
                    extraction_method="api",
                    parquet_file_size_mb=round(parquet_size_mb, 2),
                    extraction_duration_seconds=round(
                        (extraction_end - start_time).total_seconds(), 2
                    ),
                )
            except Exception as track_err:
                logger.debug(f"Failed to update extraction tracking: {track_err}")

        if authoritative_schema:
            try:
                configs_dir = Path(__file__).resolve().parents[1] / "configs"
                config_path = configs_dir / f"{source}.yaml"
                if config_path.exists():
                    stats = update_yaml_schema_selective(
                        str(config_path),
                        logical_table,
                        authoritative_schema,
                        "streaming_autodiscovery",
                    )
                    if stats.get("updated"):
                        logger.info(
                            "Updated YAML schema for %s: +%d columns, %d filled",
                            logical_table,
                            stats.get("columns_added", 0),
                            stats.get("columns_filled", 0),
                        )
                else:
                    logger.debug(
                        "Config file %s not found; skipping YAML auto-update for %s",
                        config_path,
                        logical_table,
                    )
            except Exception as yaml_err:
                logger.warning(
                    "Could not auto-update YAML schema for %s: %s",
                    logical_table,
                    yaml_err,
                )

        # STEP 2: Upload to GCS raw bucket
        logger.info("\nSTEP 2: Upload to GCS raw bucket...")
        gcs_path = upload_to_gcs_raw(
            source, table, job_id, local_file_path, bucket_name
        )
        pipeline_stats["gcs_path"] = gcs_path
        pipeline_stats["steps_completed"] += 1

        # STEP 3: Create external table in staging
        logger.info("\nSTEP 3: Create external table in staging...")
        external_table_id = create_external_table_staging(
            bq_client,
            project,
            source,
            table,
            job_id,
            gcs_path,
            bucket_name,
            staging_dataset=staging_dataset,
        )
        pipeline_stats["external_table_id"] = external_table_id
        pipeline_stats["steps_completed"] += 1

        # Update tracking: GCS upload complete, moving to transform phase
        if processing_id:
            try:
                update_table_processing(
                    processing_id,
                    phase="transform",
                    gcs_path=gcs_path,
                    transform_started_at=datetime.now(),
                )
            except Exception as track_err:
                logger.debug(f"Failed to update phase tracking: {track_err}")

        # STEP 4: Transform staging to production
        logger.info("\nSTEP 4: Transform to production...")
        transform_success = transform_staging_to_production(
            bq_client,
            project,
            source,
            table,
            external_table_id,
            job_id,
            staging_dataset=staging_dataset,
            production_dataset=production_dataset,
            primary_keys=primary_keys,
            merge_config=merge_config,
            base_table_name=logical_table,
        )
        if transform_success:
            pipeline_stats["steps_completed"] += 1
            # Update tracking: transform/merge complete
            if processing_id:
                try:
                    update_table_processing(
                        processing_id,
                        phase="merge",
                        transform_completed_at=datetime.now(),
                        merge_started_at=datetime.now(),
                        merge_completed_at=datetime.now(),
                    )
                except Exception as track_err:
                    logger.debug(f"Failed to update phase tracking: {track_err}")
        else:
            pipeline_stats["steps_failed"] += 1

        # STEP 5: Archive external table
        logger.info("\nSTEP 5: Archive external table...")
        archive_table_id = archive_external_table(
            bq_client,
            project,
            source,
            table,
            job_id,
            gcs_path,
            bucket_name,
            external_table_id,
            ttl_days,
        )
        pipeline_stats["archive_table_id"] = archive_table_id
        pipeline_stats["steps_completed"] += 1

        # STEP 6: Cleanup local files
        logger.info("\nSTEP 6: Cleanup local files...")
        cleanup_local_files(local_file_path, job_id, cleanup_local)
        pipeline_stats["steps_completed"] += 1

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        pipeline_stats["execution_time"] = execution_time

        logger.info("\n" + "=" * 60)
        logger.info(f"PIPELINE COMPLETE SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Job ID: {job_id}")
        logger.info(f"Steps completed: {pipeline_stats['steps_completed']}/6")
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        final_row_count = pipeline_stats["total_rows"] or (
            len(data) if data is not None else 0
        )
        logger.info(f"Total rows: {final_row_count:,}")
        logger.info("=" * 60)

        # Complete table tracking with success
        if processing_id:
            try:
                complete_table_processing(
                    processing_id, status="success", rows_extracted=final_row_count
                )
                logger.debug(f"Completed table tracking: {processing_id}")
            except Exception as track_err:
                logger.warning(f"Failed to complete table tracking: {track_err}")

        return pipeline_stats

    except Exception as e:
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        pipeline_stats["execution_time"] = execution_time
        pipeline_stats["error"] = str(e)
        pipeline_stats["steps_failed"] = 6 - pipeline_stats["steps_completed"]

        # Cleanup on failure (if configured)
        if pipeline_stats["local_file_path"]:
            cleanup_local_files(
                pipeline_stats["local_file_path"], job_id, cleanup_on_failure=True
            )

        logger.error(f"\nPIPELINE FAILED: {e}")
        logger.error(f"Execution time: {execution_time:.2f} seconds")
        logger.error(f"Steps completed: {pipeline_stats['steps_completed']}/6")

        # Complete table tracking with failure
        if processing_id:
            try:
                # Determine which phase failed based on steps completed
                steps = pipeline_stats["steps_completed"]
                if steps < 3:
                    error_phase = "extraction"
                elif steps < 4:
                    error_phase = "transform"
                else:
                    error_phase = "merge"

                complete_table_processing(
                    processing_id,
                    status="failed",
                    error_message=str(e),
                    error_phase=error_phase,
                )
                logger.debug(f"Completed table tracking (failed): {processing_id}")
            except Exception as track_err:
                logger.warning(f"Failed to complete table tracking: {track_err}")

        raise


# Configuration constants
DEFAULT_TTL_DAYS = 30
DEFAULT_CLEANUP_ON_SUCCESS = True
DEFAULT_CLEANUP_ON_FAILURE = False
DEFAULT_TEMP_DIR_PREFIX = "orchestrator_"

# Pipeline step definitions
PIPELINE_STEPS = [
    "extract_to_local_parquet",
    "upload_to_gcs_raw",
    "create_external_table_staging",
    "transform_staging_to_production",
    "archive_external_table",
    "cleanup_local_files",
]
