#!/usr/bin/env python3
"""
YAML Configuration Auto-Updater
================================

Automatically updates YAML configuration files when new columns are discovered
during schema evolution. Preserves all existing formatting, comments, and structure.

Features:
- Backs up YAML before modification
- Preserves comments and formatting
- Adds new columns with appropriate metadata
- Logs all changes

Author: Data Pipeline Team
Created: 2025-10-08
"""

import os
import yaml
import logging
import shutil
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Backup directory for YAML configs
BACKUP_DIR = "configs/backups"


def ensure_backup_dir() -> None:
    # Create backup directory if it doesn't exist
    os.makedirs(BACKUP_DIR, exist_ok=True)


def backup_yaml(config_path: str) -> Optional[str]:
    """
    Create a dated backup of the YAML configuration file.

    Args:
        config_path: Path to the YAML file to backup

    Returns:
        Path to backup file or None if failed
    """
    try:
        ensure_backup_dir()

        # Get filename without extension
        config_file = Path(config_path)
        base_name = config_file.stem  # e.g., 'facebook'

        # Create backup filename with date: facebook_20251008_103045.yaml
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{base_name}_{timestamp}.yaml"
        backup_path = os.path.join(BACKUP_DIR, backup_filename)

        # Copy file
        shutil.copy2(config_path, backup_path)
        logger.info(f"Created YAML backup: {backup_path}")

        return backup_path

    except Exception as e:
        logger.error(f"Failed to backup YAML: {e}")
        return None


def get_field_metadata_from_bigquery(
    field: bigquery.SchemaField,
    table_name: str
) -> Dict[str, Any]:
    """
    Generate YAML metadata for a field based on BigQuery schema field.

    Args:
        field: BigQuery SchemaField object
        table_name: Name of the table (for context)

    Returns:
        Dict with YAML-compatible metadata
    """
    metadata = {}

    # Field type mapping (BigQuery -> YAML convention)
    # Keep simple - just the field name in the fields list
    # The actual schema is managed by BigQuery

    return metadata


def find_resource_in_yaml(
    yaml_content: str,
    table_name: str
) -> Optional[tuple]:
    """
    Find the resource section for a given table in YAML content.

    Args:
        yaml_content: Raw YAML file content as string
        table_name: Table name to find (e.g., 'gf_entry', 'campaigns')

    Returns:
        Tuple of (start_line_index, end_line_index, indent_level) or None
    """
    lines = yaml_content.split('\n')

    # Look for the resource key (e.g., "gf_entry:" or "campaigns:")
    resource_key = f"{table_name}:"

    for i, line in enumerate(lines):
        # Check if this line contains the resource key at the start (accounting for indent)
        stripped = line.lstrip()
        if stripped.startswith(resource_key):
            # Found the resource - now find its indent level
            indent = len(line) - len(stripped)

            # Find the end of this resource section
            # It ends when we hit another line at the same or lower indent level
            end_line = len(lines)
            for j in range(i + 1, len(lines)):
                next_line = lines[j]
                if next_line.strip():  # Non-empty line
                    next_indent = len(next_line) - len(next_line.lstrip())
                    if next_indent <= indent:
                        end_line = j
                        break

            return (i, end_line, indent)

    return None


def add_fields_to_yaml(
    config_path: str,
    table_name: str,
    new_fields: List[bigquery.SchemaField]
) -> bool:
    """
    Add new fields to a table's YAML configuration.

    Args:
        config_path: Path to YAML configuration file
        table_name: Name of the table resource
        new_fields: List of BigQuery SchemaField objects to add

    Returns:
        True if successful
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            yaml_content = f.read()

        config_data = yaml.safe_load(yaml_content) or {}
        resources_section = config_data.get('resources') or {}
        resource_config = resources_section.get(table_name)

        if not resource_config:
            logger.warning(f"Could not find resource '{table_name}' in {config_path}")
            return False

        existing_schema = resource_config.get('schema') or {}
        existing_fields = set(existing_schema.keys())
        if not existing_fields:
            legacy_fields = resource_config.get('fields') or []
            existing_fields.update(legacy_fields)

        fields_to_add = [field for field in new_fields if field.name not in existing_fields]
        if not fields_to_add:
            logger.debug(f"No new fields to add for '{table_name}' in {config_path}")
            return True

        lines = yaml_content.split('\n')
        resource_location = find_resource_in_yaml(yaml_content, table_name)
        if not resource_location:
            logger.warning(f"Could not locate resource block for '{table_name}' in {config_path}")
            return False

        start_line, end_line, base_indent = resource_location

        schema_line_idx = None
        schema_indent = None
        for i in range(start_line, end_line):
            line = lines[i]
            stripped = line.lstrip()
            if stripped.startswith('schema:'):
                schema_line_idx = i
                schema_indent = len(line) - len(stripped)
                break

        timestamp = datetime.utcnow().isoformat()

        if schema_line_idx is not None:
            insert_position = end_line
            field_indent = None
            for i in range(schema_line_idx + 1, end_line):
                line = lines[i]
                if not line.strip():
                    continue
                line_indent = len(line) - len(line.lstrip())
                if line_indent <= schema_indent:
                    insert_position = i
                    break
                if field_indent is None and ':' in line:
                    field_indent = line_indent
            if field_indent is None:
                field_indent = schema_indent + 2

            indent_spaces = ' ' * field_indent
            new_lines = [
                f"{indent_spaces}{field.name}: {(field.field_type or 'STRING').upper()}"
                for field in fields_to_add
            ]

            lines[insert_position:insert_position] = new_lines

            new_column_count = len(existing_schema) + len(fields_to_add)
            metadata_line_idx = None
            metadata_indent = None
            for i in range(start_line, end_line):
                stripped = lines[i].lstrip()
                if stripped.startswith('schema_metadata:'):
                    metadata_line_idx = i
                    metadata_indent = len(lines[i]) - len(stripped)
                    break

            if metadata_line_idx is not None:
                metadata_end = end_line
                for i in range(metadata_line_idx + 1, end_line):
                    current = lines[i]
                    if not current.strip():
                        continue
                    current_indent = len(current) - len(current.lstrip())
                    if current_indent <= metadata_indent:
                        metadata_end = i
                        break

                metadata_body_indent = ' ' * (metadata_indent + 2)
                updated_last = False
                updated_count = False
                for i in range(metadata_line_idx + 1, metadata_end):
                    stripped = lines[i].strip()
                    if stripped.startswith('last_updated:'):
                        lines[i] = f"{metadata_body_indent}last_updated: '{timestamp}'"
                        updated_last = True
                    elif stripped.startswith('column_count:'):
                        lines[i] = f"{metadata_body_indent}column_count: {new_column_count}"
                        updated_count = True
                if not updated_last:
                    lines.insert(metadata_line_idx + 1, f"{metadata_body_indent}last_updated: '{timestamp}'")
                    metadata_end += 1
                if not updated_count:
                    lines.insert(metadata_end, f"{metadata_body_indent}column_count: {new_column_count}")

            updated_content = '\n'.join(lines)
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)

            logger.info(
                f"Added {len(fields_to_add)} field(s) to '{table_name}' schema in {config_path}"
            )
            logger.debug(
                "  New fields: %s",
                ', '.join(
                    f"{field.name}:{(field.field_type or 'STRING')}" for field in fields_to_add
                )
            )
            return True

        # Fallback for legacy 'fields:' list format
        fields_line_idx = None
        fields_indent = None
        for i in range(start_line, end_line):
            line = lines[i]
            stripped = line.lstrip()
            if stripped.startswith('fields:'):
                fields_line_idx = i
                fields_indent = len(line) - len(stripped)
                break

        if fields_line_idx is None:
            logger.warning(f"No 'schema:' or 'fields:' block found for '{table_name}' in {config_path}")
            return False

        field_indent = fields_indent + 2
        last_field_line = fields_line_idx

        for i in range(fields_line_idx + 1, end_line):
            line = lines[i]
            if not line.strip():
                continue
            line_indent = len(line) - len(line.lstrip())
            if line_indent == field_indent and line.lstrip().startswith('- '):
                last_field_line = i
            elif line_indent <= fields_indent:
                break

        insert_position = last_field_line + 1
        indent_spaces = ' ' * field_indent
        legacy_lines = [f"{indent_spaces}- {field.name}" for field in fields_to_add]
        lines[insert_position:insert_position] = legacy_lines

        updated_content = '\n'.join(lines)
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)

        logger.info(
            f"Added {len(fields_to_add)} field(s) to '{table_name}' legacy fields list in {config_path}"
        )
        logger.debug(
            "  New fields: %s",
            ', '.join(field.name for field in fields_to_add)
        )
        return True

    except Exception as e:
        logger.error(f"Failed to add fields to YAML: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def update_yaml_with_new_columns(
    source: str,
    table_name: str,
    new_fields: List[bigquery.SchemaField]
) -> bool:
    """
    Update YAML configuration file with newly discovered columns.

    This is called after schema evolution successfully adds columns to BigQuery.

    Args:
        source: Source name ('facebook', 'wordpress', etc.)
        table_name: Table name (e.g., 'campaigns', 'gf_entry')
        new_fields: List of BigQuery SchemaField objects that were added

    Returns:
        True if successful
    """
    try:
        # Determine config file path
        config_path = f"configs/{source}.yaml"

        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            return False

        if not new_fields:
            logger.debug(f"No new fields to add for {table_name}")
            return True

        # Extract just the table name without prefix
        # e.g., 'wordpress_posts' -> 'posts', 'facebook_campaigns' -> 'campaigns'
        resource_name = table_name
        if table_name.startswith(f'{source}_'):
            resource_name = table_name[len(source)+1:]

        # Create backup before modifying
        backup_path = backup_yaml(config_path)
        if not backup_path:
            logger.error("Failed to create backup - aborting YAML update")
            return False

        # Add fields to YAML
        success = add_fields_to_yaml(config_path, resource_name, new_fields)

        if success:
            logger.info(f"Successfully updated {config_path}")
            logger.info(f"  Table: {table_name}")
            logger.info(
                "  Added fields: %s",
                ', '.join(
                    f"{field.name}:{(field.field_type or 'STRING')}" for field in new_fields
                )
            )
            logger.info(f"  Backup: {backup_path}")
        else:
            logger.error(f"Failed to update {config_path} - backup preserved at {backup_path}")

        return success

    except Exception as e:
        logger.error(f"Error updating YAML: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
