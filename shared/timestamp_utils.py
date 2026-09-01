"""
Centralized timestamp parsing and normalization for all data sources.

This module provides consistent timestamp handling across Facebook, WordPress,
LimeSurvey, and Mailchimp extractors. Each source has unique timestamp formats
that need to be normalized to UTC timezone-aware datetime objects.

MySQL-based sources (WordPress, LimeSurvey) share the same timestamp parser
since they use the same underlying database format.

See docs/TIMESTAMP_FORMATS.md for detailed compatibility matrix.
"""

import logging
from datetime import datetime, timezone, date
from typing import Optional, Union
from dateutil import parser

logger = logging.getLogger(__name__)


def parse_facebook_timestamp(value: Union[str, int, datetime, None]) -> Optional[datetime]:
    """
    Parse Facebook API timestamp formats.

    Supported formats:
    - ISO 8601 with timezone: "2024-01-15T10:30:00+0000"
    - Date only: "2024-01-15"
    - Unix timestamp (int): 1705318200
    - datetime object: returns as-is with UTC timezone

    Args:
        value: Timestamp in any supported Facebook format

    Returns:
        UTC timezone-aware datetime object, or None if parsing fails

    Examples:
        >>> parse_facebook_timestamp("2024-01-15T10:30:00+0000")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> parse_facebook_timestamp("2024-01-15")
        datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
        >>> parse_facebook_timestamp(1705318200)
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    """
    if value is None:
        return None

    # Already a datetime object
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    # Unix timestamp (integer)
    if isinstance(value, int):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (ValueError, OSError) as e:
            logger.warning(f"Failed to parse Facebook unix timestamp {value}: {e}")
            return None

    # String formats
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        try:
            # Use dateutil parser for flexible ISO 8601 parsing
            dt = parser.parse(value)

            # Ensure UTC timezone
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

            return dt
        except Exception as e:
            logger.warning(f"Failed to parse Facebook timestamp '{value}': {e}")
            return None

    logger.warning(f"Unsupported Facebook timestamp type: {type(value)}")
    return None


def parse_mysql_timestamp(value: Union[str, int, datetime, None]) -> Optional[datetime]:
    """
    Parse MySQL database timestamp formats (generic, used by WordPress, LimeSurvey, etc.).

    Supported formats:
    - MySQL DATETIME: "2024-01-15 10:30:00" (assumes UTC)
    - Unix timestamp (int): 1705318200
    - ISO format from REST API: "2024-01-15T10:30:00"
    - Date only: "2024-01-15"
    - datetime object: returns as-is with UTC timezone

    Args:
        value: Timestamp in any supported MySQL format

    Returns:
        UTC timezone-aware datetime object, or None if parsing fails

    Examples:
        >>> parse_mysql_timestamp("2024-01-15 10:30:00")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> parse_mysql_timestamp(1705318200)
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    """
    if value is None:
        return None

    # Already a datetime object
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    # Unix timestamp (integer)
    if isinstance(value, int):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (ValueError, OSError) as e:
            logger.warning(f"Failed to parse MySQL unix timestamp {value}: {e}")
            return None

    # String formats
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        try:
            # Try MySQL DATETIME format first (most common)
            if ' ' in value and 'T' not in value:
                dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                return dt.replace(tzinfo=timezone.utc)

            # Try date-only format
            if len(value) == 10 and value.count('-') == 2:
                dt = datetime.strptime(value, '%Y-%m-%d')
                return dt.replace(tzinfo=timezone.utc)

            # Fall back to flexible parser for ISO formats
            dt = parser.parse(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

            return dt
        except Exception as e:
            logger.warning(f"Failed to parse MySQL timestamp '{value}': {e}")
            return None

    logger.warning(f"Unsupported MySQL timestamp type: {type(value)}")
    return None


# Backwards compatibility alias
def parse_wordpress_timestamp(value: Union[str, int, datetime, None]) -> Optional[datetime]:
    """
    DEPRECATED: Use parse_mysql_timestamp() instead.

    This is a backwards compatibility alias for parse_mysql_timestamp().
    WordPress uses MySQL timestamps, so this function is just a wrapper.
    """
    return parse_mysql_timestamp(value)


def parse_mailchimp_timestamp(value: Union[str, datetime, None]) -> Optional[datetime]:
    """
    Parse Mailchimp API timestamp formats.

    Supported formats:
    - ISO 8601 UTC: "2024-01-15T10:30:00Z"
    - ISO 8601 with offset: "2024-01-15T10:30:00-05:00"
    - Date only: "2024-01-15"
    - datetime object: returns as-is with UTC timezone

    Args:
        value: Timestamp in any supported Mailchimp format

    Returns:
        UTC timezone-aware datetime object, or None if parsing fails

    Examples:
        >>> parse_mailchimp_timestamp("2024-01-15T10:30:00Z")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> parse_mailchimp_timestamp("2024-01-15T10:30:00-05:00")
        datetime(2024, 1, 15, 15, 30, tzinfo=timezone.utc)  # Converted to UTC
    """
    if value is None:
        return None

    # Already a datetime object
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    # String formats
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        try:
            # Replace 'Z' with '+00:00' for Python 3.6+ fromisoformat compatibility
            cleaned = value.replace('Z', '+00:00')

            try:
                # Try fast path with fromisoformat
                dt = datetime.fromisoformat(cleaned)
            except ValueError:
                # Fall back to flexible parser
                dt = parser.parse(value)

            # Ensure UTC timezone
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)

            return dt
        except Exception as e:
            logger.warning(f"Failed to parse Mailchimp timestamp '{value}': {e}")
            return None

    logger.warning(f"Unsupported Mailchimp timestamp type: {type(value)}")
    return None


def parse_mailchimp_date(value: Union[str, date, datetime, None]) -> Optional[date]:
    """
    Parse Mailchimp date-only fields (e.g., member_since).

    Args:
        value: Date string, date object, or datetime object

    Returns:
        date object, or None if parsing fails

    Examples:
        >>> parse_mailchimp_date("2024-01-15")
        date(2024, 1, 15)
    """
    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], '%Y-%m-%d').date()
        except Exception as e:
            logger.warning(f"Failed to parse Mailchimp date '{value}': {e}")
            return None

    return None


def normalize_timestamp(value: Union[str, int, datetime, None], source: str) -> Optional[datetime]:
    """
    Universal timestamp normalization - routes to appropriate source-specific parser.

    This is the main entry point for timestamp parsing. It routes to the correct
    parser based on the source system.

    Args:
        value: Timestamp in any supported format
        source: Source system ('facebook', 'wordpress', 'limesurvey', 'mailchimp')

    Returns:
        UTC timezone-aware datetime object, or None if parsing fails

    Raises:
        ValueError: If source is not recognized

    Examples:
        >>> normalize_timestamp("2024-01-15T10:30:00+0000", "facebook")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> normalize_timestamp("2024-01-15 10:30:00", "wordpress")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> normalize_timestamp("2024-01-15 10:30:00", "limesurvey")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> normalize_timestamp("2024-01-15T10:30:00Z", "mailchimp")
        datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    """
    if value is None:
        return None

    source_lower = source.lower()

    if source_lower == 'facebook':
        return parse_facebook_timestamp(value)
    elif source_lower in ('wordpress', 'limesurvey'):
        return parse_mysql_timestamp(value)
    elif source_lower == 'mailchimp':
        return parse_mailchimp_timestamp(value)
    else:
        raise ValueError(f"Unknown source: {source}. Supported: facebook, wordpress, limesurvey, mailchimp")


def to_bigquery_timestamp(value: Optional[datetime]) -> Optional[str]:
    """
    Convert datetime to BigQuery-compatible timestamp string.

    Args:
        value: datetime object (preferably UTC timezone-aware)

    Returns:
        Timestamp string in format "YYYY-MM-DD HH:MM:SS" (UTC), or None

    Examples:
        >>> dt = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> to_bigquery_timestamp(dt)
        "2024-01-15 10:30:00"
    """
    if value is None:
        return None

    # Convert to UTC if not already
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)

    # Format as BigQuery timestamp (no timezone suffix)
    return value.strftime('%Y-%m-%d %H:%M:%S')


def to_iso_string(value: Optional[datetime]) -> Optional[str]:
    """
    Convert datetime to ISO 8601 string with UTC timezone.

    Args:
        value: datetime object

    Returns:
        ISO 8601 string with 'Z' suffix, or None

    Examples:
        >>> dt = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> to_iso_string(dt)
        "2024-01-15T10:30:00Z"
    """
    if value is None:
        return None

    # Convert to UTC if not already
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)

    # Format as ISO with Z suffix
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')
