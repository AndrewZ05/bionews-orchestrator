#!/usr/bin/env python3
"""
Data Validator - Early Detection System
=========================================

Catches data type issues BEFORE extraction begins to prevent wasted time.

Features:
- Sample-based validation (quick)
- Schema profiling
- Type compatibility checks
- Parquet compatibility validation
- Early warnings for potential issues

Author: Data Pipeline Team
Created: 2025-01-09
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
import json
from google.cloud import bigquery

logger = logging.getLogger(__name__)

#
# Validation result functions (dict-based approach)
#

def create_validation_result() -> Dict[str, Any]:
    """
    Create a new validation result dictionary.

    Returns:
        Dict with validation state and issue tracking
    """
    return {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'recommendations': [],
        'column_issues': {}
    }


def add_validation_error(result: Dict[str, Any], message: str, column: str = None) -> None:
    """
    Add a critical error to validation result.

    Args:
        result: Validation result dict
        message: Error message
        column: Optional column name
    """
    result['is_valid'] = False
    result['errors'].append(message)
    if column:
        if column not in result['column_issues']:
            result['column_issues'][column] = []
        result['column_issues'][column].append(('ERROR', message))


def add_validation_warning(result: Dict[str, Any], message: str, column: str = None) -> None:
    """
    Add a warning to validation result.

    Args:
        result: Validation result dict
        message: Warning message
        column: Optional column name
    """
    result['warnings'].append(message)
    if column:
        if column not in result['column_issues']:
            result['column_issues'][column] = []
        result['column_issues'][column].append(('WARNING', message))


def add_validation_recommendation(result: Dict[str, Any], message: str) -> None:
    """
    Add a recommendation to validation result.

    Args:
        result: Validation result dict
        message: Recommendation message
    """
    result['recommendations'].append(message)


def print_validation_report(result: Dict[str, Any]) -> None:
    """
    Print validation report using logger so it appears in logs.

    Args:
        result: Validation result dict
    """
    logger.error("="*70)
    logger.error("  DATA VALIDATION REPORT")
    logger.error("="*70)

    if result['is_valid']:
        logger.info("Status: PASS")
    else:
        logger.error("Status: FAIL")

    if result['errors']:
        logger.error(f"\nERRORS ({len(result['errors'])}):")
        for i, error in enumerate(result['errors'], 1):
            logger.error(f"  {i}. {error}")

    if result['warnings']:
        logger.warning(f"\nWARNINGS ({len(result['warnings'])}):")
        for i, warning in enumerate(result['warnings'], 1):
            logger.warning(f"  {i}. {warning}")

    if result['recommendations']:
        logger.info(f"\nRECOMMENDATIONS ({len(result['recommendations'])}):")
        for i, rec in enumerate(result['recommendations'], 1):
            logger.info(f"  {i}. {rec}")

    if result['column_issues']:
        logger.error(f"\nCOLUMN-SPECIFIC ISSUES:")
        for column, issues in result['column_issues'].items():
            logger.error(f"\n  Column: {column}")
            for severity, message in issues:
                if severity == 'ERROR':
                    logger.error(f"    [{severity}] {message}")
                else:
                    logger.warning(f"    [{severity}] {message}")

    logger.error("="*70)


def validate_sample_data(sample_data: List[Dict[str, Any]],
                        table_name: str,
                        sample_size: int = 100) -> Dict[str, Any]:
    """
    Validate a sample of data BEFORE full extraction.

    This is the EARLY WARNING SYSTEM that prevents wasted extraction time.

    Args:
        sample_data: Small sample of records (e.g., first 100 rows)
        table_name: Name of the table being extracted
        sample_size: Expected sample size (for validation)

    Returns:
        Dict with validation results (use with add_validation_* functions)
    """
    result = create_validation_result()

    if not sample_data:
        add_validation_error(result, "No sample data provided - cannot validate")
        return result

    logger.info(f"Validating sample data for table '{table_name}' ({len(sample_data)} records)")

    # Convert to DataFrame for analysis
    try:
        df = pd.DataFrame(sample_data)
    except Exception as e:
        add_validation_error(result, f"Failed to convert sample to DataFrame: {e}")
        return result

    # Run validation checks
    _check_mixed_types(df, result)
    _check_parquet_compatibility(df, result)
    _check_problematic_patterns(df, result)
    _check_null_handling(df, result)
    _check_string_length(df, result)

    logger.info(f"Validation complete: {len(result['errors'])} errors, {len(result['warnings'])} warnings")

    return result


def _check_mixed_types(df: pd.DataFrame, result: Dict[str, Any]):
    # Check for mixed type columns that will fail Parquet conversion
    logger.debug("Checking for mixed type columns...")

    for col in df.columns:
        if df[col].dtype == 'object':
            # Sample non-null values
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue

            # Get unique types
            types = set(type(x).__name__ for x in non_null.head(100))

            # Check for mixed types
            if len(types) > 1:
                add_validation_error(
                    result,
                    f"Mixed types detected: {types} - Will fail Parquet conversion",
                    column=col
                )
                add_validation_recommendation(
                    result,
                    f"Column '{col}' should be converted to JSON strings or single type"
                )

            # Check for complex types
            has_dict = any(isinstance(x, dict) for x in non_null.head(100))
            has_list = any(isinstance(x, list) for x in non_null.head(100))

            if has_dict or has_list:
                add_validation_warning(
                    result,
                    f"Complex types detected (dict/list) - Will be converted to JSON",
                    column=col
                )


def _check_parquet_compatibility(df: pd.DataFrame, result: Dict[str, Any]):
    # Check if data can be written to Parquet format
    logger.debug("Checking Parquet compatibility...")

    # Try to write a sample to Parquet in memory
    try:
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Try writing sample
            df.head(10).to_parquet(tmp_path, index=False, engine='pyarrow')
            logger.debug("Parquet compatibility check: PASS")
        except Exception as e:
            add_validation_error(result, f"Parquet compatibility check FAILED: {e}")

            # Try to identify problematic columns
            for col in df.columns:
                try:
                    df[[col]].head(10).to_parquet(tmp_path, index=False, engine='pyarrow')
                except Exception as col_error:
                    add_validation_error(
                        result,
                        f"Column cannot be written to Parquet: {col_error}",
                        column=col
                    )
        finally:
            # Clean up temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    except Exception as e:
        add_validation_warning(result, f"Could not perform Parquet compatibility test: {e}")


def _check_problematic_patterns(df: pd.DataFrame, result: Dict[str, Any]):
    # Check for known problematic column patterns
    logger.debug("Checking for problematic column patterns...")

    problematic_patterns = {
        'action': ['action', 'actions', '_action_', 'action_type'],
        'cost_per': ['cost_per_'],
        'value': ['_value', 'values'],
        'type': ['_type_', '_types'],
        'metadata': ['metadata', 'meta_'],
        'config': ['config', 'configuration'],
        'settings': ['settings', 'options']
    }

    for pattern_name, patterns in problematic_patterns.items():
        for col in df.columns:
            col_lower = col.lower()
            if any(p in col_lower for p in patterns):
                # Check if it's an object column
                if df[col].dtype == 'object':
                    add_validation_warning(
                        result,
                        f"Potentially problematic column pattern '{pattern_name}' - May contain mixed types",
                        column=col
                    )


def _check_null_handling(df: pd.DataFrame, result: Dict[str, Any]):
    # Check for columns with high null percentage
    logger.debug("Checking null handling...")

    for col in df.columns:
        null_pct = (df[col].isna().sum() / len(df)) * 100

        if null_pct == 100:
            add_validation_warning(
                result,
                f"Column is 100% null - Consider excluding from schema",
                column=col
            )
        elif null_pct > 90:
            add_validation_warning(
                result,
                f"Column is {null_pct:.1f}% null - May cause schema issues",
                column=col
            )


def _check_string_length(df: pd.DataFrame, result: Dict[str, Any]):
    # Check for extremely long strings that may exceed BigQuery limits
    logger.debug("Checking string lengths...")

    max_string_length = 65536  # BigQuery STRING limit

    for col in df.columns:
        if df[col].dtype == 'object':
            # Check max string length in sample
            non_null = df[col].dropna()
            if len(non_null) > 0:
                max_len = max(len(str(x)) for x in non_null.head(100))

                if max_len > max_string_length:
                    add_validation_error(
                        result,
                        f"String exceeds BigQuery limit: {max_len} > {max_string_length}",
                        column=col
                    )
                    add_validation_recommendation(
                        result,
                        f"Column '{col}' strings should be truncated to {max_string_length} characters"
                    )
                elif max_len > max_string_length * 0.8:
                    add_validation_warning(
                        result,
                        f"String approaching BigQuery limit: {max_len} chars",
                        column=col
                    )


def profile_data_types(sample_data: List[Dict[str, Any]],
                      table_name: str) -> Dict[str, Any]:
    """
    Profile data types in sample to understand schema.

    Args:
        sample_data: Sample of records
        table_name: Table name

    Returns:
        Dictionary with type profiling information
    """
    logger.info(f"Profiling data types for table '{table_name}'")

    if not sample_data:
        return {'error': 'No data to profile'}

    df = pd.DataFrame(sample_data)

    profile = {
        'table_name': table_name,
        'sample_size': len(df),
        'column_count': len(df.columns),
        'columns': {}
    }

    for col in df.columns:
        col_profile = {
            'dtype': str(df[col].dtype),
            'null_count': int(df[col].isna().sum()),
            'null_percentage': float((df[col].isna().sum() / len(df)) * 100),
            'unique_count': int(df[col].nunique()),
        }

        # For object columns, profile types
        if df[col].dtype == 'object':
            non_null = df[col].dropna()
            if len(non_null) > 0:
                types = [type(x).__name__ for x in non_null.head(100)]
                type_counts = {}
                for t in types:
                    type_counts[t] = type_counts.get(t, 0) + 1

                col_profile['type_distribution'] = type_counts
                col_profile['has_mixed_types'] = len(set(types)) > 1

                # Check for complex types
                col_profile['has_dict'] = 'dict' in type_counts
                col_profile['has_list'] = 'list' in type_counts

                # String length stats
                if 'str' in type_counts:
                    str_values = [str(x) for x in non_null if isinstance(x, str)]
                    if str_values:
                        col_profile['max_string_length'] = max(len(s) for s in str_values[:100])
                        col_profile['avg_string_length'] = sum(len(s) for s in str_values[:100]) / len(str_values[:100])

        profile['columns'][col] = col_profile

    return profile


def suggest_schema_fixes(validation_result: Dict[str, Any]) -> List[str]:
    """
    Suggest fixes for validation issues.

    Args:
        validation_result: Result dict from validate_sample_data()

    Returns:
        List of suggested fixes
    """
    fixes = []

    for column, issues in validation_result['column_issues'].items():
        for severity, message in issues:
            if 'mixed types' in message.lower():
                fixes.append(
                    f"Convert '{column}' to JSON strings: "
                    f"df['{column}'] = df['{column}'].apply(lambda x: json.dumps(x) if x is not None else None)"
                )

            if 'complex types' in message.lower():
                fixes.append(
                    f"Serialize '{column}' complex objects: "
                    f"Use serialize_facebook_object() or json.dumps()"
                )

            if 'exceeds bigquery limit' in message.lower():
                fixes.append(
                    f"Truncate '{column}': "
                    f"df['{column}'] = df['{column}'].str[:65536]"
                )

    return fixes


#
# Consolidated Validation Functions (from various files)
#

def validate_required_fields(data: Dict[str, Any], required_fields: List[str],
                            context: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    Validate that required fields are present in data.

    Consolidated from: shared/error_handler.py, shared/pipeline_validator.py

    Args:
        data: Dictionary to validate
        required_fields: List of required field names
        context: Optional context for error messages

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    missing = [field for field in required_fields if field not in data or data[field] is None]

    if missing:
        context_str = f" in {context}" if context else ""
        errors = [f"Missing required fields{context_str}: {', '.join(missing)}"]
        return False, errors

    return True, []


def validate_record_fields(record: Dict[str, Any],
                          required_fields: Optional[List[str]] = None,
                          optional_fields: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
    """
    Validate record has required fields and no unexpected fields.

    Consolidated from: shared/data_processor.py

    Args:
        record: Record to validate
        required_fields: Fields that must be present
        optional_fields: Fields that are allowed but not required

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check required fields
    if required_fields:
        is_valid, field_errors = validate_required_fields(record, required_fields)
        if not is_valid:
            errors.extend(field_errors)

    # Check for unexpected fields
    if required_fields or optional_fields:
        allowed_fields = set(required_fields or []) | set(optional_fields or [])
        unexpected = set(record.keys()) - allowed_fields

        if unexpected:
            errors.append(f"Unexpected fields: {', '.join(unexpected)}")

    return len(errors) == 0, errors


def check_table_exists(client: bigquery.Client, table_id: str) -> bool:
    """
    Check if BigQuery table exists.

    Consolidated from: shared/extraction_utils.py

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)

    Returns:
        True if table exists
    """
    try:
        client.get_table(table_id)
        return True
    except Exception as e:
        logger.debug(f"Table {table_id} does not exist: {e}")
        return False


def validate_table_config(config: Dict[str, Any], table_name: str) -> Tuple[bool, List[str]]:
    """
    Validate table configuration has required settings.

    Args:
        config: Table configuration dict
        table_name: Name of table being validated

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check required config fields
    required = ['primary_key']
    for field in required:
        if field not in config:
            errors.append(f"Table '{table_name}' missing required config: {field}")

    # Validate primary key
    if 'primary_key' in config:
        pk = config['primary_key']
        if not pk or (isinstance(pk, list) and len(pk) == 0):
            errors.append(f"Table '{table_name}' has empty primary_key")

    # Validate data quality checks if present
    if 'data_quality' in config:
        dq = config['data_quality']

        if 'null_check' in dq:
            null_cfg = dq['null_check']
            if 'columns' not in null_cfg:
                errors.append(f"Table '{table_name}' null_check missing 'columns'")

        if 'duplicate_check' in dq:
            dup_cfg = dq['duplicate_check']
            if 'columns' not in dup_cfg:
                errors.append(f"Table '{table_name}' duplicate_check missing 'columns'")

    return len(errors) == 0, errors


def validate_schema_compatibility(staging_schema: List[bigquery.SchemaField],
                                  production_schema: List[bigquery.SchemaField],
                                  table_name: str) -> Tuple[bool, List[str]]:
    """
    Validate staging schema is compatible with production schema.

    Args:
        staging_schema: Schema from staging table
        production_schema: Schema from production table
        table_name: Name of table

    Returns:
        Tuple of (is_compatible, list_of_issues)
    """
    issues = []

    # Create field maps for easy lookup
    staging_fields = {f.name: f for f in staging_schema}
    prod_fields = {f.name: f for f in production_schema}

    # Check for missing fields in staging
    for field_name in prod_fields:
        if field_name not in staging_fields:
            issues.append(f"Field '{field_name}' exists in production but missing in staging")

    # Check for type mismatches
    for field_name in staging_fields:
        if field_name in prod_fields:
            staging_field = staging_fields[field_name]
            prod_field = prod_fields[field_name]

            if staging_field.field_type != prod_field.field_type:
                issues.append(
                    f"Field '{field_name}' type mismatch: "
                    f"staging={staging_field.field_type}, production={prod_field.field_type}"
                )

            # Check mode compatibility (REQUIRED -> NULLABLE is OK, reverse is not)
            if staging_field.mode == 'REQUIRED' and prod_field.mode == 'NULLABLE':
                issues.append(
                    f"Field '{field_name}' mode incompatible: "
                    f"staging=REQUIRED but production=NULLABLE"
                )

    return len(issues) == 0, issues


def validate_data_quality(client: bigquery.Client, table_id: str,
                         quality_config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Run data quality checks on table.

    Consolidated from: shared/bigquery_utils.py (check_nulls, check_duplicates, etc.)

    Args:
        client: BigQuery client
        table_id: Full table ID
        quality_config: Data quality configuration

    Returns:
        Tuple of (passed, list_of_failures)
    """
    failures = []

    # Check nulls
    if 'null_check' in quality_config:
        null_cfg = quality_config['null_check']
        columns = null_cfg.get('columns', [])
        threshold = null_cfg.get('threshold', 0)

        for column in columns:
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNTIF({column} IS NULL) as null_count,
                SAFE_DIVIDE(COUNTIF({column} IS NULL), COUNT(*)) * 100 as null_percentage
            FROM `{table_id}`
            """

            try:
                result = list(client.query(query).result())[0]
                if result.null_percentage > threshold:
                    failures.append(
                        f"Column '{column}' has {result.null_percentage:.2f}% nulls "
                        f"(threshold: {threshold}%)"
                    )
            except Exception as e:
                logger.warning(f"Could not check nulls for {column}: {e}")

    # Check duplicates
    if 'duplicate_check' in quality_config:
        dup_cfg = quality_config['duplicate_check']
        columns = dup_cfg.get('columns', [])

        if columns:
            columns_str = ', '.join(columns)
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT ({columns_str})) as unique_rows
            FROM `{table_id}`
            """

            try:
                result = list(client.query(query).result())[0]
                if result.total_rows != result.unique_rows:
                    dup_count = result.total_rows - result.unique_rows
                    failures.append(
                        f"Found {dup_count} duplicate rows on columns: {columns_str}"
                    )
            except Exception as e:
                logger.warning(f"Could not check duplicates: {e}")

    # Check freshness
    if 'freshness_check' in quality_config:
        fresh_cfg = quality_config['freshness_check']
        timestamp_column = fresh_cfg.get('column', 'extracted_at')
        max_age_hours = fresh_cfg.get('max_age_hours', 24)

        query = f"""
        SELECT
            MAX({timestamp_column}) as latest_timestamp,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({timestamp_column}), HOUR) as age_hours
        FROM `{table_id}`
        """

        try:
            result = list(client.query(query).result())[0]
            if result.age_hours > max_age_hours:
                failures.append(
                    f"Data is stale: {result.age_hours} hours old "
                    f"(threshold: {max_age_hours} hours)"
                )
        except Exception as e:
            logger.warning(f"Could not check freshness: {e}")

    return len(failures) == 0, failures


def validate_dataset_name(dataset_name: str) -> Tuple[bool, List[str]]:
    """
    Validate BigQuery dataset name follows naming conventions.

    Consolidated from: shared/cli_utils.py

    Args:
        dataset_name: Dataset name to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check length
    if len(dataset_name) > 1024:
        errors.append(f"Dataset name too long: {len(dataset_name)} > 1024")

    # Check characters (must start with letter/underscore, then alphanumeric/underscore)
    if not dataset_name:
        errors.append("Dataset name cannot be empty")
    elif not (dataset_name[0].isalpha() or dataset_name[0] == '_'):
        errors.append("Dataset name must start with letter or underscore")

    # Check for invalid characters
    valid_chars = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_')
    invalid = set(dataset_name) - valid_chars
    if invalid:
        errors.append(f"Dataset name contains invalid characters: {invalid}")

    return len(errors) == 0, errors


def validate_config_structure(config: Dict[str, Any], config_type: str = "pipeline") -> Tuple[bool, List[str]]:
    """
    Validate configuration structure has required sections.

    Args:
        config: Configuration dict to validate
        config_type: Type of config (pipeline, source, etc.)

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    if config_type == "pipeline":
        # Check required sections
        required_sections = ['source', 'pipeline']
        for section in required_sections:
            if section not in config:
                errors.append(f"Missing required section: {section}")

        # Validate source section
        if 'source' in config:
            source = config['source']
            if 'type' not in source:
                errors.append("Source section missing 'type'")

        # Validate pipeline section
        if 'pipeline' in config:
            pipeline = config['pipeline']
            if 'dataset' not in pipeline:
                errors.append("Pipeline section missing 'dataset'")

    return len(errors) == 0, errors


# Example usage
if __name__ == "__main__":
    # Example: Test with sample data
    sample = [
        {'id': 1, 'name': 'Test', 'value': 100, 'metadata': {'key': 'val'}},
        {'id': 2, 'name': 'Test2', 'value': 'string', 'metadata': [1, 2, 3]},  # Mixed type!
        {'id': 3, 'name': 'Test3', 'value': None, 'metadata': None},
    ]

    result = validate_sample_data(sample, 'test_table')
    print_validation_report(result)

    if not result['is_valid']:
        print("Suggested fixes:")
        for fix in suggest_schema_fixes(result):
            print(f"  - {fix}")

    # Test consolidated validators
    test_data = {'name': 'John', 'email': 'john@example.com'}
    is_valid, errors = validate_required_fields(test_data, ['name', 'email', 'age'])
    print(f"\nRequired fields validation: {'PASS' if is_valid else 'FAIL'}")
    if errors:
        for error in errors:
            print(f"  - {error}")
