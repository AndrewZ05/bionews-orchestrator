#!/usr/bin/env python3
"""
Schema Discovery using PyArrow + DuckDB for accurate type inference.

This module provides high-performance, memory-efficient schema discovery for rebuilds.
Uses PyArrow for robust data handling and DuckDB for accurate SQL-based type inference.
"""

import logging
import tempfile
import uuid
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# System columns that should be excluded from schema discovery
# IMPORTANT: Only exclude truly internal/temporary fields that should NOT persist in production
# Fields like extracted_at, source, execution_id ARE part of the schema and should be discovered
SYSTEM_COLUMNS = {
    'row_hash',      # Internal hash for deduplication, computed during merge
    '_creative_obj'  # Temporary field used during Facebook ads extraction
}

def discover_schema_with_pyarrow_duckdb(
    data: List[Dict[str, Any]],
    table_name: str,
    batch_size: int = 10000,
    force_string_fields: Optional[List[str]] = None
) -> Dict[str, str]:
    """
    Discover optimal schema from data using PyArrow + DuckDB.

    This function:
    1. Converts Python dicts to PyArrow table (handles mixed types gracefully)
    2. Writes to temporary Parquet file using DuckDB
    3. Uses DuckDB to analyze Parquet and infer optimal types
    4. Applies intelligent field type rules (ID fields, force_string_fields)
    5. Returns BigQuery-compatible schema

    Args:
        data: List of dictionaries (extracted data)
        table_name: Name of table (for logging)
        batch_size: Number of rows per batch (default: 10000, not used but kept for compatibility)
        force_string_fields: Optional list of field names that should always be STRING

    Returns:
        Dict mapping column names to BigQuery types

    Example:
        >>> data = [{'id': 1, 'name': 'John', 'age': 30}, ...]
        >>> schema = discover_schema_with_pyarrow_duckdb(data, 'users', force_string_fields=['id'])
        >>> print(schema)
        {'id': 'STRING', 'name': 'STRING', 'age': 'INT64'}
    """
    force_string_fields = force_string_fields or []
    conn = None
    duckdb_db_path: Optional[Path] = None

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        import duckdb
    except ImportError as e:
        logger.error(f"Required libraries not installed: {e}")
        logger.error("Install with: pip install pyarrow duckdb")
        raise

    if not data:
        logger.warning(f"No data provided for schema discovery: {table_name}")
        return {}

    logger.info(f"Discovering schema for {table_name} using PyArrow + DuckDB...")
    logger.info(f"  Total rows: {len(data):,}")

    # Create temporary Parquet file
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_parquet_path = temp_file.name
    temp_file.close()

    try:
        from datetime import datetime, date
        logger.info("  Streaming data to Parquet in 100k-row batches...")
        temp_dir = Path(temp_parquet_path).parent
        duckdb_db_path = temp_dir / f"duckdb_schema_{uuid.uuid4().hex}.db"
        conn = duckdb.connect(database=str(duckdb_db_path), read_only=False)

        temp_dir_posix = temp_dir.as_posix()
        temp_dir_literal = temp_dir_posix.replace("'", "''")
        conn.execute(f"PRAGMA temp_directory='{temp_dir_literal}'")
        conn.execute("PRAGMA memory_limit='4GB'")

        chunk_size = 100_000
        writer: Optional[pq.ParquetWriter] = None
        rows_processed = 0

        for start in range(0, len(data), chunk_size):
            chunk = data[start:start + chunk_size]
            processed_chunk = []

            for record in chunk:
                processed_record = {}
                for key, value in record.items():
                    if value is None or value == '':
                        # CRITICAL: Use empty string instead of None to prevent null-type inference
                        # When PyArrow sees all None values in a batch, it infers 'null' type
                        # Then if next batch has strings, schema mismatch occurs
                        processed_record[key] = ''
                    elif isinstance(value, (datetime, date)):
                        # Convert datetime to ISO string for consistent schema across batches
                        # PyArrow will infer this as string, then DuckDB will convert back to TIMESTAMP
                        processed_record[key] = value.isoformat()
                    else:
                        # Convert everything else to string
                        processed_record[key] = str(value)
                processed_chunk.append(processed_record)

            if not processed_chunk:
                continue

            pa_table = pa.Table.from_pylist(processed_chunk)

            if writer is None:
                writer = pq.ParquetWriter(temp_parquet_path, pa_table.schema)

            writer.write_table(pa_table)

            rows_processed += len(processed_chunk)
            if rows_processed % 100_000 == 0 or rows_processed == len(data):
                pct_complete = (rows_processed / len(data)) * 100
                logger.info(f"    Processed {rows_processed:,}/{len(data):,} rows ({pct_complete:.1f}%)")

        if writer is None:
            # No data rows were processed (should not happen due to earlier check)
            logger.warning("  No rows written during schema discovery")
            pq.ParquetWriter(temp_parquet_path, pa.schema([])).close()
        else:
            writer.close()

        logger.info(f"  [OK] Wrote {rows_processed:,} rows to temporary Parquet file")

        # Use DuckDB to analyze Parquet and infer types
        logger.info(f"  Analyzing Parquet with DuckDB for accurate type inference...")

        # Reuse existing DuckDB connection
        # Get schema inferred by DuckDB
        schema_query = f"""
            DESCRIBE SELECT * FROM read_parquet('{temp_parquet_path}')
        """

        schema_df = conn.execute(schema_query).fetchdf()

        # Convert DuckDB types to BigQuery types
        bigquery_schema = {}

        # Known problematic patterns from Facebook API that have mixed types
        problematic_patterns = [
            'action', 'cost_per', 'conversion', 'offsite', 'fb_pixel',
            'post_reaction', 'page_engagement', 'video_view'
        ]

        for _, row in schema_df.iterrows():
            column_name = row['column_name']
            duckdb_type = row['column_type'].upper()

            # Skip system columns
            if column_name in SYSTEM_COLUMNS:
                continue

            # RULE 1: Explicit force_string_fields override (highest priority)
            if column_name in force_string_fields:
                bigquery_type = 'STRING'
                logger.debug(f"  Forced STRING (explicit config): {column_name}")

            # RULE 2: Intelligent ID field detection
            # Fields ending in _id, _hash, or named 'id' are semantic identifiers
            # Even if numeric, they should be STRING to preserve:
            # - Leading zeros (zip codes, phone numbers)
            # - API compatibility (IDs may become non-numeric)
            # - JOIN compatibility (avoid type mismatches)
            elif _is_id_field(column_name):
                bigquery_type = 'STRING'
                logger.debug(f"  Forced STRING (ID field pattern): {column_name}")

            # RULE 3: Force STRING for known problematic Facebook action columns
            # These columns have mixed types (int/str) throughout the dataset
            elif any(pattern in column_name.lower() for pattern in problematic_patterns):
                bigquery_type = 'STRING'
                logger.debug(f"  Forced STRING (action column): {column_name}")

            # RULE 4: Standard type mapping
            else:
                bigquery_type = _duckdb_to_bigquery_type(duckdb_type)

            bigquery_schema[column_name] = bigquery_type

        logger.info(f"  [OK] Discovered schema: {len(bigquery_schema)} columns")

        # Log schema for debugging
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("  Discovered schema:")
            for col, dtype in sorted(bigquery_schema.items()):
                logger.debug(f"    {col}: {dtype}")

        return bigquery_schema

    except Exception as e:
        logger.error(f"Error during schema discovery: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if duckdb_db_path is not None:
            for cleanup_target in (duckdb_db_path, duckdb_db_path.with_suffix(duckdb_db_path.suffix + '.wal')):
                try:
                    cleanup_target.unlink()
                except FileNotFoundError:
                    pass
                except Exception:
                    pass
        # Clean up temporary file
        try:
            Path(temp_parquet_path).unlink()
        except Exception:
            pass


def _is_id_field(field_name: str) -> bool:
    """
    Determine if a field is semantically an ID/identifier field.

    ID fields should be STRING even if they contain numeric values to:
    - Preserve leading zeros (zip codes, account numbers)
    - Maintain API compatibility (IDs may become alphanumeric)
    - Prevent JOIN type mismatches
    - Avoid integer overflow for large IDs

    Args:
        field_name: Name of the field to check

    Returns:
        True if field should be treated as an ID (STRING), False otherwise

    Examples:
        >>> _is_id_field('user_id')
        True
        >>> _is_id_field('subscriber_hash')
        True
        >>> _is_id_field('web_id')
        True
        >>> _is_id_field('count')
        False
    """
    field_lower = field_name.lower()

    # Exact matches
    if field_lower in {'id', 'guid', 'uuid', 'hash'}:
        return True

    # Suffix patterns (most common)
    id_suffixes = {'_id', '_guid', '_uuid', '_hash', '_key', '_code', '_number'}
    if any(field_lower.endswith(suffix) for suffix in id_suffixes):
        return True

    # Prefix patterns
    id_prefixes = {'id_', 'guid_', 'uuid_', 'hash_'}
    if any(field_lower.startswith(prefix) for prefix in id_prefixes):
        return True

    # Special cases - phone numbers, zip codes, etc.
    special_id_fields = {'phone', 'zip', 'postal', 'ssn', 'ein', 'account'}
    if any(pattern in field_lower for pattern in special_id_fields):
        return True

    return False


def _duckdb_to_bigquery_type(duckdb_type: str) -> str:
    """
    Map DuckDB type to BigQuery type.

    Args:
        duckdb_type: DuckDB type (e.g., 'VARCHAR', 'BIGINT', 'DOUBLE')

    Returns:
        BigQuery type (e.g., 'STRING', 'INT64', 'FLOAT64')
    """
    duckdb_type = duckdb_type.upper()

    # String types
    if 'VARCHAR' in duckdb_type or 'TEXT' in duckdb_type or 'STRING' in duckdb_type:
        return 'STRING'

    # Integer types
    if 'BIGINT' in duckdb_type or 'INT64' in duckdb_type or 'LONG' in duckdb_type:
        return 'INT64'
    if 'INTEGER' in duckdb_type or 'INT' in duckdb_type:
        return 'INT64'
    if 'TINYINT' in duckdb_type or 'SMALLINT' in duckdb_type:
        return 'INT64'

    # Float types
    if 'DOUBLE' in duckdb_type or 'FLOAT64' in duckdb_type:
        return 'FLOAT64'
    if 'FLOAT' in duckdb_type or 'REAL' in duckdb_type:
        return 'FLOAT64'
    if 'DECIMAL' in duckdb_type or 'NUMERIC' in duckdb_type:
        return 'NUMERIC'

    # Boolean
    if 'BOOLEAN' in duckdb_type or 'BOOL' in duckdb_type:
        return 'BOOL'

    # Timestamp/Date types
    if 'TIMESTAMP' in duckdb_type:
        return 'TIMESTAMP'
    if 'DATE' in duckdb_type:
        return 'DATE'
    if 'TIME' in duckdb_type:
        return 'TIME'

    # Binary types
    if 'BLOB' in duckdb_type or 'BINARY' in duckdb_type:
        return 'BYTES'

    # JSON
    if 'JSON' in duckdb_type:
        return 'JSON'

    # Default to STRING for unknown types
    logger.debug(f"Unknown DuckDB type '{duckdb_type}', defaulting to STRING")
    return 'STRING'


def compare_schemas(
    yaml_schema: Dict[str, str],
    discovered_schema: Dict[str, str],
    table_name: str
) -> Dict[str, Any]:
    """
    Compare YAML schema with DuckDB-discovered schema.

    Args:
        yaml_schema: Schema from YAML config
        discovered_schema: Schema discovered by DuckDB
        table_name: Name of table (for logging)

    Returns:
        Dict with comparison results:
        {
            'matches': int,
            'mismatches': List[Dict],
            'missing_in_yaml': List[str],
            'missing_in_discovered': List[str]
        }
    """
    result = {
        'matches': 0,
        'mismatches': [],
        'missing_in_yaml': [],
        'missing_in_discovered': []
    }

    # Check for columns in discovered but not in YAML
    for col in discovered_schema:
        if col not in yaml_schema:
            result['missing_in_yaml'].append(col)

    # Check for columns in YAML but not in discovered
    for col in yaml_schema:
        if col not in discovered_schema:
            result['missing_in_discovered'].append(col)

    # Check for type mismatches
    for col in discovered_schema:
        if col in yaml_schema:
            yaml_type = yaml_schema[col]
            disc_type = discovered_schema[col]

            if yaml_type == disc_type:
                result['matches'] += 1
            else:
                result['mismatches'].append({
                    'column': col,
                    'yaml_type': yaml_type,
                    'discovered_type': disc_type
                })

    # Log comparison results
    logger.info(f"Schema comparison for {table_name}:")
    logger.info(f"  Matching columns: {result['matches']}")

    if result['mismatches']:
        logger.warning(f"  Type mismatches: {len(result['mismatches'])}")
        for mismatch in result['mismatches']:
            logger.warning(f"    {mismatch['column']}: YAML={mismatch['yaml_type']}, Discovered={mismatch['discovered_type']}")

    if result['missing_in_yaml']:
        logger.warning(f"  Columns missing in YAML: {len(result['missing_in_yaml'])}")
        for col in result['missing_in_yaml']:
            logger.warning(f"    {col}: {discovered_schema[col]}")

    if result['missing_in_discovered']:
        logger.warning(f"  Columns missing in discovered data: {len(result['missing_in_discovered'])}")
        for col in result['missing_in_discovered']:
            logger.warning(f"    {col}: {yaml_schema[col]}")

    return result
