#!/usr/bin/env python3
"""
Generic API Data Serialization
===============================

Provides source-agnostic utilities for serializing complex API response data.

This module handles:
- Complex nested object serialization to JSON
- Array/list field flattening
- Type-specific field handling
- Metric extraction and normalization

Can be used with any API that returns nested/complex data structures
(Facebook metrics, WordPress metadata, generic JSON responses, etc.)

Usage:
    from shared.api_serialization import serialize_complex_fields, flatten_array_fields

    # Serialize nested objects to JSON strings
    record = serialize_complex_fields(
        record=api_response,
        fields=['actions', 'conversions']
    )

    # Flatten array fields into individual columns
    record = flatten_array_fields(
        record=api_response,
        array_fields=['actions'],
        key_field='action_type',
        value_field='value'
    )

Author: Data Pipeline Team
Created: 2025-01-20
"""

import json
import logging
from typing import Dict, Any, List, Optional, Set, Callable

logger = logging.getLogger(__name__)


def serialize_complex_fields(
    record: Dict[str, Any],
    fields: Optional[List[str]] = None,
    exclude_fields: Optional[Set[str]] = None,
    auto_detect: bool = True,
    suffix: str = '_json',
    in_place: bool = True
) -> Dict[str, Any]:
    """
    Serialize complex fields (dict, list) to JSON strings.

    Handles nested objects that BigQuery can't directly store as JSON strings.

    Args:
        record: Record dictionary to process
        fields: Specific fields to serialize (if None, auto-detect if auto_detect=True)
        exclude_fields: Fields to skip even if complex
        auto_detect: Whether to auto-detect complex fields
        suffix: Suffix to add to field name for JSON version (default: '_json')
        in_place: Whether to modify record in place or create copy

    Returns:
        Record with complex fields serialized

    Example:
        record = {
            'id': '123',
            'actions': [{'type': 'click', 'value': 10}],
            'metadata': {'source': 'api', 'version': 2}
        }

        serialized = serialize_complex_fields(record, auto_detect=True)
        # Returns: {
        #   'id': '123',
        #   'actions_json': '[{"type": "click", "value": 10}]',
        #   'metadata_json': '{"source": "api", "version": 2}'
        # }
    """
    if not in_place:
        record = record.copy()

    if exclude_fields is None:
        exclude_fields = set()

    # Determine fields to serialize
    if fields is None and auto_detect:
        # Auto-detect complex fields
        fields = [
            key for key, value in record.items()
            if key not in exclude_fields and isinstance(value, (dict, list))
        ]
    elif fields is None:
        # No fields to serialize
        return record

    # Serialize each field
    for field in fields:
        if field not in record or field in exclude_fields:
            continue

        value = record[field]

        # Skip if not complex
        if not isinstance(value, (dict, list)):
            continue

        # Skip if empty
        if not value:
            continue

        try:
            # Serialize to JSON
            json_str = json.dumps(value)

            # Store as new field with suffix
            json_field = f"{field}{suffix}"
            record[json_field] = json_str

        except (TypeError, ValueError) as e:
            logger.warning(
                f"  Failed to serialize field '{field}': {e}"
            )

    return record


def flatten_array_fields(
    record: Dict[str, Any],
    array_fields: List[str],
    key_field: str,
    value_field: str,
    prefix: Optional[str] = None,
    keep_original: bool = True,
    also_serialize: bool = True,
    normalize_keys: bool = True
) -> Dict[str, Any]:
    """
    Flatten array fields into individual columns.

    Converts arrays of key-value pairs into flat columns:
    [{'type': 'click', 'value': 10}] → actions_click = 10

    Args:
        record: Record dictionary to process
        array_fields: Fields containing arrays to flatten
        key_field: Field name in array items that contains the key
        value_field: Field name in array items that contains the value
        prefix: Optional prefix for flattened field names (default: array field name)
        keep_original: Whether to keep original array field
        also_serialize: Whether to also create JSON version
        normalize_keys: Whether to normalize keys (replace dots with underscores)

    Returns:
        Record with flattened array fields

    Example:
        record = {
            'id': '123',
            'actions': [
                {'action_type': 'link_click', 'value': 10},
                {'action_type': 'post_engagement', 'value': 25}
            ]
        }

        flattened = flatten_array_fields(
            record=record,
            array_fields=['actions'],
            key_field='action_type',
            value_field='value'
        )
        # Returns: {
        #   'id': '123',
        #   'actions_link_click': 10,
        #   'actions_post_engagement': 25,
        #   'actions_json': '[...]'  # if also_serialize=True
        # }
    """
    for field in array_fields:
        if field not in record:
            continue

        value = record[field]

        # Skip if not a list
        if not isinstance(value, list):
            continue

        # Skip if empty
        if not value:
            continue

        # Determine prefix
        field_prefix = prefix if prefix else field

        # Flatten each item in the array
        for item in value:
            if not isinstance(item, dict):
                continue

            # Extract key and value
            item_key = item.get(key_field)
            item_value = item.get(value_field)

            if item_key is None:
                continue

            # Normalize key if requested
            if normalize_keys:
                item_key = str(item_key).replace('.', '_').replace(' ', '_')

            # Create flattened field name
            flat_field = f"{field_prefix}_{item_key}"

            # Store value
            record[flat_field] = item_value

        # Serialize original if requested
        if also_serialize:
            try:
                record[f"{field}_json"] = json.dumps(value)
            except (TypeError, ValueError) as e:
                logger.warning(
                    f"  Failed to serialize flattened field '{field}': {e}"
                )

        # Remove original if not keeping
        if not keep_original:
            del record[field]

    return record


def extract_nested_value(
    data: Dict[str, Any],
    path: str,
    default: Any = None,
    separator: str = '.'
) -> Any:
    """
    Extract value from nested dictionary using dot notation path.

    Args:
        data: Dictionary to extract from
        path: Dot-notation path (e.g., 'user.profile.name')
        default: Default value if path not found
        separator: Path separator (default: '.')

    Returns:
        Extracted value or default

    Example:
        data = {
            'user': {
                'profile': {
                    'name': 'John'
                }
            }
        }
        name = extract_nested_value(data, 'user.profile.name')
        # Returns: 'John'
    """
    keys = path.split(separator)
    value = data

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


def flatten_nested_dict(
    data: Dict[str, Any],
    parent_key: str = '',
    separator: str = '_',
    max_depth: Optional[int] = None,
    current_depth: int = 0
) -> Dict[str, Any]:
    """
    Flatten nested dictionary into single-level dictionary.

    Args:
        data: Dictionary to flatten
        parent_key: Parent key prefix
        separator: Key separator (default: '_')
        max_depth: Maximum nesting depth to flatten (None = unlimited)
        current_depth: Current nesting depth (internal use)

    Returns:
        Flattened dictionary

    Example:
        data = {
            'user': {
                'name': 'John',
                'address': {
                    'city': 'NYC'
                }
            }
        }
        flat = flatten_nested_dict(data)
        # Returns: {
        #   'user_name': 'John',
        #   'user_address_city': 'NYC'
        # }
    """
    items = []

    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        # Check max depth
        if max_depth is not None and current_depth >= max_depth:
            items.append((new_key, value))
            continue

        # Recursively flatten nested dicts
        if isinstance(value, dict):
            items.extend(
                flatten_nested_dict(
                    value,
                    new_key,
                    separator,
                    max_depth,
                    current_depth + 1
                ).items()
            )
        else:
            items.append((new_key, value))

    return dict(items)


def normalize_field_names(
    record: Dict[str, Any],
    replacements: Optional[Dict[str, str]] = None,
    lowercase: bool = False,
    replace_spaces: bool = True,
    replace_dots: bool = True
) -> Dict[str, Any]:
    """
    Normalize field names to consistent format.

    Args:
        record: Record to normalize
        replacements: Custom character replacements (default: {'.': '_', ' ': '_'})
        lowercase: Whether to convert to lowercase
        replace_spaces: Whether to replace spaces with underscores
        replace_dots: Whether to replace dots with underscores

    Returns:
        Record with normalized field names

    Example:
        record = {
            'User.Name': 'John',
            'Email Address': 'john@example.com'
        }
        normalized = normalize_field_names(record, lowercase=True)
        # Returns: {
        #   'user_name': 'John',
        #   'email_address': 'john@example.com'
        # }
    """
    if replacements is None:
        replacements = {}
        if replace_dots:
            replacements['.'] = '_'
        if replace_spaces:
            replacements[' '] = '_'

    normalized = {}

    for key, value in record.items():
        # Apply replacements
        new_key = key
        for old_char, new_char in replacements.items():
            new_key = new_key.replace(old_char, new_char)

        # Lowercase if requested
        if lowercase:
            new_key = new_key.lower()

        normalized[new_key] = value

    return normalized


def extract_and_flatten_metrics(
    record: Dict[str, Any],
    metric_fields: List[str],
    key_field: str = 'name',
    value_field: str = 'value',
    prefix: str = 'metric'
) -> Dict[str, Any]:
    """
    Extract metrics from array and flatten into columns.

    Common pattern for API responses with metric arrays.

    Args:
        record: Record containing metrics
        metric_fields: Fields containing metric arrays
        key_field: Field in metric items containing metric name
        value_field: Field in metric items containing metric value
        prefix: Prefix for flattened metric columns

    Returns:
        Record with extracted and flattened metrics

    Example:
        record = {
            'id': '123',
            'metrics': [
                {'name': 'impressions', 'value': 1000},
                {'name': 'clicks', 'value': 50}
            ]
        }

        result = extract_and_flatten_metrics(
            record=record,
            metric_fields=['metrics']
        )
        # Returns: {
        #   'id': '123',
        #   'metric_impressions': 1000,
        #   'metric_clicks': 50
        # }
    """
    return flatten_array_fields(
        record=record,
        array_fields=metric_fields,
        key_field=key_field,
        value_field=value_field,
        prefix=prefix,
        keep_original=False,
        also_serialize=True
    )


def safe_json_loads(
    value: Any,
    default: Any = None
) -> Any:
    """
    Safely parse JSON string, returning default on error.

    Args:
        value: Value to parse (string or already parsed)
        default: Default value if parsing fails

    Returns:
        Parsed value or default

    Example:
        data = safe_json_loads('{"key": "value"}', default={})
        # Returns: {'key': 'value'}

        data = safe_json_loads('invalid json', default={})
        # Returns: {}
    """
    # Already parsed
    if isinstance(value, (dict, list)):
        return value

    # Not a string
    if not isinstance(value, str):
        return default

    # Try to parse
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return default
