#!/usr/bin/env python3
"""
Data Processing Utilities
========================

Common data processing functions used across all extractors.
Handles metadata addition, hash computation, JSON serialization, 
and data transformation operations.

Author: Data Pipeline Team
Created: 2025-01-02
"""

import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

# Configure logging
logger = logging.getLogger(__name__)

# Default fields to exclude from hash computation
DEFAULT_HASH_EXCLUDE_FIELDS = [
    '_extracted_at', 'loaded_at', 'execution_id', 'extracted_at'
]

def add_record_metadata(record: Dict[str, Any], execution_id: str, 
                       source_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Add standard metadata fields to extracted records.
    
    Args:
        record: The original data record
        execution_id: Unique execution identifier for this pipeline run
        source_name: Optional source name (e.g., 'facebook', 'wordpress')
    
    Returns:
        Enhanced record with metadata fields
        
    Example:
        >>> record = {'id': '123', 'name': 'Campaign A'}
        >>> enhanced = add_record_metadata(record, 'exec_20250102_001')
        >>> enhanced['execution_id']
        'exec_20250102_001'
        >>> enhanced['loaded_at'] 
        '2025-01-02T12:00:00.000000'
    """
    try:
        # Copy record to avoid modifying original
        enhanced_record = record.copy()
        
        # Add standard metadata fields
        enhanced_record['execution_id'] = execution_id
        enhanced_record['loaded_at'] = datetime.utcnow().isoformat()
        
        # Add extracted_at timestamp if not present
        if 'extracted_at' not in enhanced_record:
            enhanced_record['extracted_at'] = datetime.now()
            
        # Add source metadata if provided
        if source_name:
            enhanced_record['source'] = source_name
            
        logger.debug(f"Added metadata to record: execution_id={execution_id}")
        return enhanced_record
        
    except Exception as e:
        logger.error(f"Failed to add metadata to record: {e}")
        return record  # Return original record on error


def process_json_fields(data: List[Dict[str, Any]], 
                       json_field_mapping: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Process and serialize complex fields to JSON strings for BigQuery compatibility.
    
    BigQuery requires complex objects (dicts, lists) to be serialized as JSON strings.
    This function identifies and converts these fields automatically.
    
    Args:
        data: List of records to process
        json_field_mapping: Optional mapping of field names to JSON conversion hints
    
    Returns:
        Processed records with JSON fields serialized
        
    Example:
        >>> records = [{'id': '1', 'targeting': {'age_range': [25, 35]}}]
        >>> processed = process_json_fields(records)
        >>> processed[0]['targeting']
        '{"age_range": [25, 35]}'
    """
    try:
        if not data:
            return data
            
        processed_records = []
        
        # Determine fields that typically contain complex data
        complex_field_patterns = [
            'targeting', 'creative', 'object_story_spec', 'custom_audiences',
            'excluded_custom_audiences', 'actions', 'action_values', 'conversions',
            'conversion_values', 'field_data', 'metadata', 'settings', 'options'
        ]
        
        for record in data:
            processed_record = record.copy()
            
            for key, value in list(processed_record.items()):
                # Check if field contains complex data
                should_serialize = False
                
                # Check against known complex field patterns
                if any(pattern in key.lower() for pattern in complex_field_patterns):
                    should_serialize = True
                elif json_field_mapping and key in json_field_mapping:
                    should_serialize = True
                elif isinstance(value, (dict, list)) and value is not None:
                    should_serialize = True
                
                # Serialize complex fields to JSON string
                if should_serialize:
                    try:
                        # First convert Facebook objects to JSON-serializable format
                        serializable_value = serialize_facebook_object(value)
                        processed_record[key] = json.dumps(serializable_value) if serializable_value is not None else None
                        logger.debug(f"Serialized field '{key}' to JSON")
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Failed to serialize field '{key}': {e}")
                        # Try one more time with just str() as fallback
                        try:
                            processed_record[key] = str(value) if value is not None else None
                        except:
                            # Keep original value on serialization failure
                            pass
                        
            processed_records.append(processed_record)
        
        # Don't log here - logging moved to batch_process_records
        return processed_records
        
    except Exception as e:
        logger.error(f"Failed to process JSON fields: {e}")
        return data  # Return original data on error


def normalize_datetime_fields(data: List[Dict[str, Any]], 
                             date_fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Normalize datetime fields to ISO format strings.
    
    Different sources may provide dates in various formats. This function
    attempts to normalize them to ISO format for consistency in BigQuery.
    
    Args:
        data: List of records to process
        date_fields: Specific fields to normalize (auto-detect if None)
    
    Returns:
        Records with normalized datetime fields
        
    Example:
        >>> records = [{'created': '2025-01-01 10:30:00', 'updated': 'invalid_date'}]
        >>> normalized = normalize_datetime_fields[records)
        >>> normalized[0]['created']
        '2025-01-01T10:30:00'
    """
    try:
        if not data:
            return data
            
        processed_records = []
        
        # Auto-detect datetime fields if not specified
        if date_fields is None:
            date_fields = []
            for record in data:
                for key in record.keys():
                    if any(pattern in key.lower() for pattern in ['date', 'time', 'created', 'updated', 'modified']):
                        if key not in date_fields:
                            date_fields.append(key)
        
        logger.debug(f"Normalizing datetime fields: {date_fields}")
        
        for record in data:
            processed_record = record.copy()

            for field in date_fields:
                if field in processed_record and processed_record[field] is not None:
                    try:
                        value = processed_record[field]

                        # Already ISO format
                        if isinstance(value, str) and 'T' in value and value.endswith('Z'):
                            continue

                        # Parse and reformat
                        if isinstance(value, str):
                            # Check if this is a DATE-only field (date_start, date_stop)
                            # These should remain as YYYY-MM-DD format, not ISO timestamp
                            if field in ['date_start', 'date_stop']:
                                # Extract just the date portion if it contains time
                                if 'T' in value or ' ' in value:
                                    # Has time component - extract date only
                                    date_part = value.split('T')[0].split(' ')[0]
                                    processed_record[field] = date_part
                                else:
                                    # Already date-only format
                                    processed_record[field] = value
                                continue

                            # Try parsing common formats including timezone offsets
                            from dateutil import parser as dateutil_parser
                            try:
                                # Use dateutil parser which handles timezone offsets automatically
                                parsed_date = dateutil_parser.parse(value)
                                # Convert to ISO format without timezone for BigQuery TIMESTAMP
                                processed_record[field] = parsed_date.replace(tzinfo=None).isoformat()
                            except Exception:
                                # Fallback to manual parsing for common formats
                                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                                    try:
                                        parsed_date = datetime.strptime(value, fmt)
                                        processed_record[field] = parsed_date.isoformat()
                                        break
                                    except ValueError:
                                        continue
                                else:
                                    # If no format worked, keep as string (BigQuery may auto-parse)
                                    logger.debug(f"Keeping datetime field '{field}' as-is: {value}")

                        elif hasattr(value, 'isoformat'):
                            # Check if this is a date-only field
                            if field in ['date_start', 'date_stop']:
                                # For date objects, convert to YYYY-MM-DD only
                                processed_record[field] = value.strftime('%Y-%m-%d') if hasattr(value, 'strftime') else str(value)[:10]
                            else:
                                # Convert datetime objects to ISO string
                                processed_record[field] = value.isoformat()

                    except Exception as e:
                        logger.debug(f"Failed to normalize datetime field '{field}': {e}")
                        
            processed_records.append(processed_record)
        
        # Don't log here - logging moved to batch_process_records
        return processed_records
        
    except Exception as e:
        logger.error(f"Failed to normalize datetime fields: {e}")
        return data  # Return original data on error


def validate_record_fields(record: Dict[str, Any], 
                          required_fields: Optional[List[str]] = None,
                          field_type_hints: Optional[Dict[str, type]] = None) -> Dict[str, Any]:
    """
    Validate record fields for data quality and BigQuery compatibility.
    
    Performs basic validation on record fields and can add type hints
    to improve BigQuery schema inference.
    
    Args:
        record: Record to validate
        required_fields: Fields that must be present in the record
        field_type_hints: Optional type validation for specific fields
    
    Returns:
        Validated record with any error information
        
    Example:
        >>> record = {'id': '123', 'name': 'Test'}
        >>> validated = validate_record_fields(record, ['id'], {'id': str})
        >>> validated['validation_errors']
        []
    """
    try:
        validation_result = {
            'record': record.copy(),
            'validation_errors': [],
            'validation_warnings': []
        }
        
        # Check required fields
        if required_fields:
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                validation_result['validation_errors'].append(f"Missing required fields: {missing_fields}")
        
        # Check field types
        if field_type_hints:
            for field, expected_type in field_type_hints.items():
                if field in record:
                    value = record[field]
                    if value is not None and not isinstance(value, expected_type):
                        warning_msg = f"Field '{field}' has unexpected type: {type(value).__name__}, expected {expected_type.__name__}"
                        validation_result['validation_warnings'].append(warning_msg)
        
        # Check for extremely long string fields (BigQuery limitation)
        max_string_length = 65536  # BigQuery STRING field limit
        for field, value in record.items():
            if isinstance(value, str) and len(value) > max_string_length:
                warning_msg = f"Field '{field}' exceeds BigQuery STRING limit ({len(value)} > {max_string_length})"
                validation_result['validation_warnings'].append(warning_msg)
                
                # Truncate the field
                validated_record = validation_result['record']
                validated_record[field] = value[:max_string_length]
        
        logger.debug(f"Record validation complete: {len(validation_result['validation_errors'])} errors, "
                    f"{len(validation_result['validation_warnings'])} warnings")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Failed to validate record: {e}")
        return {
            'record': record,
            'validation_errors': [f"Validation failed: {e}"],
            'validation_warnings': []
        }


def ensure_string_ids(record: Dict[str, Any], id_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Convert ID fields to strings for data type consistency.
    
    Facebook API returns ID fields as INTEGER, but our BigQuery schema and hash 
    functions expect STRING. This ensures consistent data types across the pipeline.
    
    Also strips 'act_' prefix from account_id fields for cleaner storage.
    
    Args:
        record: Data record to normalize
        id_fields: List of ID field names to convert (auto-detects if None)
    
    Returns:
        Normalized record with ID fields as strings
        
    Example:
        >>> record = {'id': 123456, 'account_id': 'act_987654', 'name': 'Campaign A'}
        >>> normalized = ensure_string_ids(record)
        >>> normalized['id']
        '123456'
        >>> normalized['account_id']
        '987654'
    """
    try:
        normalized_record = record.copy()
        
        # Default Facebook ID fields
        if id_fields is None:
            id_fields = ['id', 'account_id', 'campaign_id', 'adset_id', 'ad_id', 
                        'creative_id', 'audience_id', 'page_id', 'post_id']
        
        for field in id_fields:
            if field in normalized_record and normalized_record[field] is not None:
                value = normalized_record[field]
                
                # Convert to string if it's a number
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    normalized_record[field] = str(int(value))
                elif isinstance(value, str):
                    # Strip 'act_' prefix from account_id for cleaner storage
                    if field == 'account_id' and value.startswith('act_'):
                        normalized_record[field] = value[4:]  # Remove 'act_' prefix
                    else:
                        # Ensure it's numeric
                        try:
                            # Validate it's a valid ID (numeric string)
                            int(value)
                        except ValueError:
                            logger.debug(f"ID field '{field}' contains non-numeric string: {value}")
                
        logger.debug(f"Normalized ID fields in record: {id_fields}")
        return normalized_record
        
    except Exception as e:
        logger.error(f"Failed to normalize ID fields: {e}")
        return record  # Return original record on error


def serialize_facebook_object(obj: Any) -> Any:
    """
    Convert Facebook SDK objects to JSON-serializable format recursively.
    
    Handles Facebook-specific objects like Targeting, AdCreative, etc.
    by extracting their underlying data structures.
    
    Args:
        obj: Object to serialize (can be any type)
    
    Returns:
        JSON-serializable representation of the object
        
    Example:
        >>> fb_object = ComplexFacebookObject()
        >>> serialized = serialize_facebook_object(fb_object)
        >>> json.dumps(serialized)
        '{"field": "value"}'
    """
    if obj is None:
        return None
    
    # Already a basic type
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # List - serialize each item
    if isinstance(obj, list):
        return [serialize_facebook_object(item) for item in obj]
    
    # Dictionary - serialize each value
    if isinstance(obj, dict):
        return {k: serialize_facebook_object(v) for k, v in obj.items()}
    
    # Check for Facebook SDK objects by class name
    class_name = obj.__class__.__name__
    facebook_classes = [
        'Targeting', 'TargetingGeoLocation', 'TargetingGeoLocationZip',
        'TargetingGeoLocationMarket', 'TargetingGeoLocationRegion',
        'TargetingGeoLocationCity', 'TargetingGeoLocationCountry',
        'AdCreative', 'AdCreativeObjectStorySpec', 'AdCreativeLinkData',
        'AdAccount', 'Campaign', 'AdSet', 'Ad'
    ]
    
    # If it's a Facebook SDK object, extract the data
    if any(fb_class in class_name for fb_class in facebook_classes) or hasattr(obj, '_data'):
        if hasattr(obj, '_data'):
            # Recursively serialize the _data content
            return serialize_facebook_object(obj._data)
        elif hasattr(obj, 'export_all_data'):
            # Recursively serialize the exported data
            return serialize_facebook_object(obj.export_all_data())
        elif hasattr(obj, 'export_value'):
            # Recursively serialize the exported value
            return serialize_facebook_object(obj.export_value())
        elif hasattr(obj, '__dict__'):
            # Try to serialize the object's dictionary
            return serialize_facebook_object(vars(obj))
    
    # For datetime objects
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    
    # Fallback to string representation
    return str(obj)


def batch_process_records(records: List[Dict[str, Any]],
                         execution_id: str,
                         batch_size: int = 10000,
                         source_name: Optional[str] = None,
                         enable_hash_computation: bool = True,
                         enable_json_processing: bool = True,
                         enable_datetime_normalization: bool = True,
                         enable_id_normalization: bool = True) -> List[Dict[str, Any]]:
    """
    Complete record processing pipeline for extraction workflows.
    
    Applies all standard processing steps: metadata addition, JSON serialization,
    datetime normalization, ID normalization, and hash computation in an optimized batch operation.
    
    Args:
        records: Raw extracted records to process
        batch_size: Size of processing batches (for memory management)
        execution_id: Unique execution identifier
        source_name: Source system name (e.g., 'facebook', 'wordpress')
        enable_hash_computation: Whether to compute row hashes for deduplication
        enable_json_processing: Whether to serialize complex fields to JSON
        enable_datetime_normalization: Whether to normalize datetime fields
        enable_id_normalization: Whether to normalize ID fields to strings
    
    Returns:
        Fully processed records ready for BigQuery loading
        
    Example:
        >>> raw_records = [{'id': '1', 'name': 'Test', 'meta': {'key': 'val'}}]
        >>> processed = batch_process_records(raw_records, execution_id='exec_001')
        >>> processed[0]['execution_id']
        'exec_001'
        >>> 'row_hash' in processed[0]  # Hash added by BigQuery merge, not here
        False
    """
    try:
        if not records:
            logger.info("No records to process")
            return records
            
        logger.info(f"Starting batch processing of {len(records)} records")
        
        processed_records = []
        batch_count = 0
        total_processed = 0

        # Process records in batches for memory efficiency
        for i in range(0, len(records), batch_size):
            batch_end = min(i + batch_size, len(records))
            batch_records = records[i:batch_end]
            batch_count += 1

            logger.debug(f"Processing batch {batch_count}: records {i+1}-{batch_end}")

            # Apply processing steps
            batch_processed = batch_records

            # Normalize ID fields first (for schema consistency)
            if enable_id_normalization:
                batch_processed = [
                    ensure_string_ids(record) for record in batch_processed
                ]

            # Add metadata
            batch_processed = [
                add_record_metadata(record, execution_id, source_name)
                for record in batch_processed
            ]

            # Serialize JSON fields
            if enable_json_processing:
                batch_processed = process_json_fields(batch_processed)

            # Normalize datetime fields
            if enable_datetime_normalization:
                batch_processed = normalize_datetime_fields(batch_processed)

            processed_records.extend(batch_processed)
            total_processed += len(batch_processed)

            # Log only every 1,000,000 records
            if total_processed % 1000000 == 0:
                logger.info(f"Processed {total_processed:,} records for JSON field conversion and datetime normalization")
        
        logger.info(f"Batch processing complete: {len(processed_records)} records processed in {batch_count} batches")
        return processed_records
        
    except Exception as e:
        logger.error(f"Failed to batch process records: {e}")
        return records  # Return original records on error


# Module-level constants for configuration
# Configuration constants for data processing operations.

# Memory management defaults
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_STRING_LENGTH = 65536

# Field detection patterns
DATETIME_FIELD_PATTERNS = ['date', 'time', 'created', 'updated', 'modified']
JSON_FIELD_PATTERNS = [
    'targeting', 'creative', 'object_story_spec', 'custom_audiences',
    'excluded_custom_audiences', 'actions', 'action_values', 'conversions',
    'conversion_values', 'field_data', 'metadata', 'settings', 'options'
]

# Logging configuration
LOG_DATA_PROCESSING_STATS = True
LOG_INDIVIDUAL_RECORD_PROGRESS = False
