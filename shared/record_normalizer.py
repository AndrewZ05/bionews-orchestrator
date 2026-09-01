"""
Centralized record normalization for all extractors.

Replaces inline timestamp parsing loops, JSON serialization, and type
coercion that were duplicated across 8+ extractor plugins.

Uses shared/timestamp_utils.normalize_timestamp() as the router for
source-specific timestamp parsing.
"""

import json
import logging
import math
from datetime import datetime, date, time as dt_time
from typing import Any, Dict, List, Optional, Sequence

from shared.timestamp_utils import normalize_timestamp, parse_mailchimp_date

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch Record Normalizer
# ---------------------------------------------------------------------------

def normalize_records(
    records: List[Dict[str, Any]],
    source: str,
    *,
    timestamp_fields: Optional[Sequence[str]] = None,
    json_fields: Optional[Sequence[str]] = None,
    lowercase_keys: bool = False,
) -> List[Dict[str, Any]]:
    """
    Normalise a batch of records in-place.

    1. Optionally lowercase all keys.
    2. Parse timestamp fields via the source-specific parser.
    3. Handle NULL timestamps (``0000-00-00``).
    4. Serialise complex fields (dict / list) to JSON strings.

    Args:
        records: List of record dicts (modified in-place and returned).
        source: Source name for timestamp parser routing.
        timestamp_fields: Field names to parse as timestamps.
        json_fields: Field names to serialise to JSON strings.
        lowercase_keys: If ``True``, lowercase all keys in every record.

    Returns:
        The same list (mutated in-place) for convenience.
    """
    ts_fields = set(timestamp_fields or [])
    js_fields = set(json_fields or [])

    for record in records:
        if lowercase_keys:
            lowered = {k.lower(): v for k, v in record.items()}
            record.clear()
            record.update(lowered)

        # Timestamp normalisation
        for field in ts_fields:
            if field not in record:
                continue
            value = record[field]
            if value is None:
                continue
            # Handle MySQL zero-dates
            if isinstance(value, str) and value.startswith('0000-00-00'):
                record[field] = None
                continue
            try:
                record[field] = normalize_timestamp(value, source)
            except (ValueError, TypeError):
                record[field] = None

        # JSON field serialisation
        for field in js_fields:
            if field not in record:
                continue
            value = record[field]
            if isinstance(value, (dict, list, set, tuple)):
                try:
                    record[field] = json.dumps(value, default=str)
                except Exception:
                    record[field] = str(value)

        # Auto-serialise any remaining complex values not in explicit lists
        for key, value in record.items():
            if key in js_fields or key in ts_fields:
                continue
            if isinstance(value, (dict, list, set, tuple)):
                try:
                    record[key] = json.dumps(value, default=str)
                except Exception:
                    record[key] = str(value)

    return records


# ---------------------------------------------------------------------------
# Type Coercion (extracted from mailchimp_extractor._coerce_value_for_type)
# ---------------------------------------------------------------------------

def coerce_value(value: Any, target_type: str) -> Any:
    """
    Coerce *value* to the BigQuery-canonical *target_type*.

    Supports: STRING, INT64, FLOAT64, BOOLEAN, TIMESTAMP, DATETIME, DATE,
    TIME, BYTES, GEOGRAPHY, JSON.  Also accepts legacy aliases INTEGER,
    FLOAT, BOOL, NUMERIC, etc.
    """
    if value is None:
        return None

    normalized = (target_type or 'STRING').upper()

    # --- String-like types ---
    if normalized in {'STRING', 'GEOGRAPHY', 'JSON'}:
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except Exception:
                return value.decode('latin-1', errors='ignore')
        if isinstance(value, (dict, list, set, tuple)):
            try:
                return json.dumps(value, default=str)
            except Exception:
                return str(value)
        return str(value)

    # --- Integer types ---
    if normalized in {'INT64', 'INTEGER', 'INT', 'BIGINT'}:
        try:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return int(value)
            if isinstance(value, float):
                if math.isnan(value):
                    return None
                return int(value)
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    return None
                return int(float(cleaned))
        except Exception:
            return None
        return None

    # --- Float types ---
    if normalized in {'FLOAT64', 'FLOAT', 'NUMERIC', 'BIGNUMERIC', 'DECIMAL'}:
        try:
            if isinstance(value, bool):
                return float(int(value))
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    return None
                return float(cleaned)
        except Exception:
            return None
        return None

    # --- Boolean ---
    if normalized in {'BOOL', 'BOOLEAN'}:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {'true', '1', 'yes', 'y', 't'}:
                return True
            if lowered in {'false', '0', 'no', 'n', 'f'}:
                return False
        return None

    # --- Timestamp / Datetime ---
    if normalized in {'TIMESTAMP', 'DATETIME'}:
        # Route through the universal timestamp normaliser if possible
        try:
            return normalize_timestamp(value, 'facebook')  # flexible parser
        except Exception:
            return None

    # --- Date ---
    if normalized == 'DATE':
        return parse_mailchimp_date(value)

    # --- Time ---
    if normalized == 'TIME':
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return datetime.strptime(text[:26], "%H:%M:%S.%f").time()
            except ValueError:
                try:
                    return datetime.strptime(text[:8], "%H:%M:%S").time()
                except ValueError:
                    return None
        return None

    # --- Bytes ---
    if normalized == 'BYTES':
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode('utf-8')
        return bytes(str(value), 'utf-8')

    # Fallback – return as-is
    return value


__all__ = [
    'normalize_records',
    'coerce_value',
]
