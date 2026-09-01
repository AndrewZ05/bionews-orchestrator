# Timestamp Format Compatibility Matrix

This document describes how timestamps are handled across different data sources in the orchestrator pipeline.

## Overview

Each data source (Facebook, WordPress, Mailchimp) emits timestamps in different formats. The `shared/timestamp_utils.py` module provides centralized parsing and normalization to ensure consistent timestamp handling across the entire pipeline.

**Normalization Standard:**
All timestamps are normalized to **UTC timezone-aware datetime objects** before being stored in BigQuery.

---

## Supported Formats by Source

### Facebook API

| Format | Example | Function | Notes |
|--------|---------|----------|-------|
| ISO 8601 with timezone | `2024-01-15T10:30:00+0000` | `parse_facebook_timestamp()` | Most common format |
| ISO 8601 without timezone | `2024-01-15T10:30:00` | `parse_facebook_timestamp()` | Assumes UTC |
| Date only | `2024-01-15` | `parse_facebook_timestamp()` | Time set to 00:00:00 |
| Unix timestamp (int) | `1705318200` | `parse_facebook_timestamp()` | Rare, but supported |

**Common Fields:**
- `created_time` - ISO 8601 with timezone
- `updated_time` - ISO 8601 with timezone
- `start_time` - ISO 8601 with timezone (can be date-only)
- `stop_time` - ISO 8601 with timezone (can be date-only)
- `date_start` - Date only (insights)
- `date_stop` - Date only (insights)

**Implementation:**
```python
from shared.timestamp_utils import parse_facebook_timestamp

record['created_time'] = parse_facebook_timestamp(record['created_time'])
# Returns: datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
```

---

### WordPress Database

| Format | Example | Function | Notes |
|--------|---------|----------|-------|
| MySQL DATETIME | `2024-01-15 10:30:00` | `parse_wordpress_timestamp()` | Most common, assumes UTC |
| MySQL DATE | `2024-01-15` | `parse_wordpress_timestamp()` | Date-only fields |
| ISO 8601 (REST API) | `2024-01-15T10:30:00` | `parse_wordpress_timestamp()` | From WP REST API |
| Unix timestamp (int) | `1705318200` | `parse_wordpress_timestamp()` | Plugin-generated |

**Common Fields:**
- `post_date` - MySQL DATETIME
- `post_modified` - MySQL DATETIME
- `comment_date` - MySQL DATETIME
- `user_registered` - MySQL DATETIME

**Implementation:**
```python
from shared.timestamp_utils import parse_wordpress_timestamp

row['post_date'] = parse_wordpress_timestamp(row['post_date'])
# Returns: datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
```

**Important Notes:**
- WordPress stores timestamps in site timezone by default
- Our extractors query `post_date_gmt` (GMT/UTC versions) when available
- If only local time available, conversion logic needed (see `wordpress_extractor.py`)

---

### Mailchimp API

| Format | Example | Function | Notes |
|--------|---------|----------|-------|
| ISO 8601 UTC | `2024-01-15T10:30:00Z` | `parse_mailchimp_timestamp()` | Most common format |
| ISO 8601 with offset | `2024-01-15T10:30:00-05:00` | `parse_mailchimp_timestamp()` | User timezone |
| ISO 8601 without timezone | `2024-01-15T10:30:00` | `parse_mailchimp_timestamp()` | Assumes UTC |
| Date only | `2024-01-15` | `parse_mailchimp_date()` | For date-only fields |

**Common Fields:**
- `timestamp_signup` - ISO 8601 with Z
- `timestamp_opt` - ISO 8601 with Z
- `last_changed` - ISO 8601 with Z
- `send_time` - ISO 8601 with Z
- `timestamp` (activity) - ISO 8601 with Z
- `member_since` - Date only (use `parse_mailchimp_date()`)

**Implementation:**
```python
from shared.timestamp_utils import parse_mailchimp_timestamp, parse_mailchimp_date

member['timestamp_signup'] = parse_mailchimp_timestamp(member['timestamp_signup'])
# Returns: datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)

member['member_since'] = parse_mailchimp_date(member['member_since'])
# Returns: date(2024, 1, 15)
```

---

## Universal Normalization Function

For code that needs to handle multiple sources, use `normalize_timestamp()`:

```python
from shared.timestamp_utils import normalize_timestamp

# Automatically routes to correct parser based on source
dt = normalize_timestamp(value, source='facebook')
dt = normalize_timestamp(value, source='wordpress')
dt = normalize_timestamp(value, source='mailchimp')
```

**Parameters:**
- `value`: Timestamp in any supported format (str, int, datetime, or None)
- `source`: Source system name ('facebook', 'wordpress', 'mailchimp')

**Returns:**
- UTC timezone-aware `datetime` object, or `None` if parsing fails

---

## Output Formats

### BigQuery Timestamp Format

BigQuery expects timestamps in `YYYY-MM-DD HH:MM:SS` format (UTC, no timezone suffix):

```python
from shared.timestamp_utils import to_bigquery_timestamp

dt = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
bq_format = to_bigquery_timestamp(dt)
# Returns: "2024-01-15 10:30:00"
```

**Note:** Parquet files handle datetime objects natively, so this conversion is only needed for direct BigQuery inserts or custom SQL.

### ISO 8601 String Format

For API responses or exports:

```python
from shared.timestamp_utils import to_iso_string

dt = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
iso_format = to_iso_string(dt)
# Returns: "2024-01-15T10:30:00Z"
```

---

## Edge Cases and Error Handling

### Invalid Timestamps

All parsing functions return `None` for invalid timestamps and log a warning:

```python
result = parse_facebook_timestamp("invalid")
# Returns: None
# Logs: "WARNING: Failed to parse Facebook timestamp 'invalid': ..."
```

**Handling in extractors:**
```python
record['created_time'] = parse_facebook_timestamp(record.get('created_time'))
if record['created_time'] is None:
    logger.warning(f"Invalid timestamp for record {record['id']}")
    # Continue processing, BigQuery accepts NULL timestamps
```

### Timezone Handling

All parsers ensure UTC timezone:

```python
# Input with different timezone
dt = parse_mailchimp_timestamp("2024-01-15T10:30:00-05:00")
# Returns: datetime(2024, 1, 15, 15, 30, tzinfo=timezone.utc)  # Converted to UTC

# Input without timezone (assumes UTC)
dt = parse_wordpress_timestamp("2024-01-15 10:30:00")
# Returns: datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)  # UTC assumed
```

### Date-Only Fields

Date-only inputs get time set to 00:00:00:

```python
dt = parse_facebook_timestamp("2024-01-15")
# Returns: datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
```

For fields that are truly dates (not timestamps), use `parse_mailchimp_date()`:

```python
member_since = parse_mailchimp_date("2024-01-15")
# Returns: date(2024, 1, 15)  # Not a datetime
```

---

## Migration Guide

### Replacing Inline Parsing

**Before (Facebook extractor):**
```python
from dateutil import parser

if isinstance(record[ts_field], int):
    record[ts_field] = datetime.fromtimestamp(record[ts_field])
elif isinstance(record[ts_field], str):
    record[ts_field] = parser.parse(record[ts_field])
```

**After:**
```python
from shared.timestamp_utils import parse_facebook_timestamp

record[ts_field] = parse_facebook_timestamp(record[ts_field])
```

**Before (WordPress extractor):**
```python
try:
    row[field] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
except:
    row[field] = None
```

**After:**
```python
from shared.timestamp_utils import parse_wordpress_timestamp

row[field] = parse_wordpress_timestamp(value)
```

**Before (Mailchimp extractor):**
```python
cleaned = value.replace('Z', '+00:00').strip()
dt = datetime.fromisoformat(cleaned)
if dt.tzinfo is None:
    return dt.replace(tzinfo=timezone.utc)
```

**After:**
```python
from shared.timestamp_utils import parse_mailchimp_timestamp

dt = parse_mailchimp_timestamp(value)
```

---

## Testing

Test the parsing functions:

```bash
python -c "
from shared.timestamp_utils import parse_facebook_timestamp, parse_wordpress_timestamp, parse_mailchimp_timestamp

# Test Facebook formats
print(parse_facebook_timestamp('2024-01-15T10:30:00+0000'))
print(parse_facebook_timestamp('2024-01-15'))
print(parse_facebook_timestamp(1705318200))

# Test WordPress formats
print(parse_wordpress_timestamp('2024-01-15 10:30:00'))
print(parse_wordpress_timestamp(1705318200))

# Test Mailchimp formats
print(parse_mailchimp_timestamp('2024-01-15T10:30:00Z'))
print(parse_mailchimp_timestamp('2024-01-15T10:30:00-05:00'))
"
```

---

## Future Extensions

When adding new data sources:

1. Add source-specific parser function to `shared/timestamp_utils.py`
2. Document formats in this matrix
3. Update `normalize_timestamp()` to route to new parser
4. Add test cases for new formats

**Example:**
```python
def parse_shopify_timestamp(value: Union[str, datetime, None]) -> Optional[datetime]:
    """
    Parse Shopify API timestamp formats.

    Supported formats:
    - ISO 8601 UTC: "2024-01-15T10:30:00Z"
    """
    # Implementation similar to parse_mailchimp_timestamp
```

---

## Related Documentation

- [shared/timestamp_utils.py](../shared/timestamp_utils.py) - Implementation
- [plugins/facebook_extractor.py](../plugins/facebook_extractor.py) - Facebook usage
- [plugins/wordpress_extractor.py](../plugins/wordpress_extractor.py) - WordPress usage
- [plugins/mailchimp_extractor.py](../plugins/mailchimp_extractor.py) - Mailchimp usage
