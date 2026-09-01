# Logging Standards

This document defines the logging standards for the orchestrator pipeline.

## Overview

Consistent logging output makes it easier to:
- Monitor pipeline execution
- Debug issues
- Parse logs programmatically
- Understand job flow

## Logging Standards

### 1. Message Format

**No Leading Spaces (Top-Level Messages):**
```python
# Correct
logger.info("Starting extraction for table: campaigns")
logger.error("Failed to connect to database")

# Incorrect
logger.info(" Starting extraction for table: campaigns")
logger.error(" Failed to connect to database")
```

**Indentation for Nested Information:**
```python
# Use 2 spaces for nested/child information
logger.info("Processing accounts...")
logger.info("  Account 1: completed")
logger.info("  Account 2: completed")

# Use 4 spaces for deeply nested information
logger.error("Job failed:")
logger.error("  Job ID: 12345")
logger.error("    Error: Connection timeout")
logger.error("    Retry: 3/5")
```

### 2. Log Levels

**INFO:** Normal operation, progress updates
```python
logger.info("Retrieved 1000 rows from API")
logger.info("Extraction complete")
```

**WARNING:** Recoverable issues, retries
```python
logger.warning("Rate limit hit, waiting 5 minutes")
logger.warning("Table not found, skipping")
```

**ERROR:** Failures, exceptions
```python
logger.error("Failed to initialize API")
logger.error(f"Database connection failed: {e}")
```

**DEBUG:** Detailed information for troubleshooting
```python
logger.debug(f"Query: {query[:100]}")
logger.debug(f"Response status: {response.status_code}")
```

### 3. Message Style

**Sentence Case:**
```python
# Correct
logger.info("Starting Facebook extraction")
logger.warning("Rate limit exceeded")

# Incorrect
logger.info("Starting Facebook Extraction")
logger.warning("RATE LIMIT EXCEEDED")
```

**Active Voice:**
```python
# Correct
logger.info("Extracted 1000 rows")
logger.error("Connection failed")

# Incorrect
logger.info("1000 rows were extracted")
logger.error("Connection failure occurred")
```

**No Emojis:**
```python
# Correct
logger.info("Extraction complete")
logger.error("Failed to connect")

# Incorrect
logger.info("Extraction complete!")
logger.error("Failed to connect")
```

### 4. Separator Lines

**Major Sections (80 characters):**
```python
logger.info("=" * 80)
logger.info("Starting Facebook Extraction")
logger.info("=" * 80)
```

**Minor Sections (40 characters):**
```python
logger.info("-" * 40)
logger.info("Processing Table: campaigns")
logger.info("-" * 40)
```

**Inline Separators:**
```python
# For lists or nested data
logger.info("Accounts:")
logger.info("  - account_1")
logger.info("  - account_2")
```

### 5. Contextual Information

**Include Relevant Context:**
```python
# Good: includes table name and row count
logger.info(f"Extracted {row_count} rows from {table_name}")

# Bad: missing context
logger.info(f"Extracted {row_count} rows")
```

**Format Numbers Consistently:**
```python
# Use thousand separators for large numbers
logger.info(f"Processed {count:,} rows")  # Output: "Processed 1,000 rows"

# Use decimal places for durations
logger.info(f"Duration: {duration:.2f} seconds")  # Output: "Duration: 12.34 seconds"

# Use percentages where appropriate
logger.info(f"Progress: {percent:.1f}%")  # Output: "Progress: 45.2%"
```

### 6. Multi-Line Messages

**Stack Traces:**
```python
try:
    # code
except Exception as e:
    logger.error(f"Operation failed: {e}")
    logger.debug(f"Traceback: {traceback.format_exc()}")
```

**Structured Data:**
```python
logger.info("Job details:")
logger.info(f"  Job ID: {job_id}")
logger.info(f"  Status: {status}")
logger.info(f"  Duration: {duration:.2f}s")
logger.info(f"  Rows processed: {rows:,}")
```

### 7. Progress Indicators

**Batch Processing:**
```python
logger.info(f"Processing batch {current}/{total} ({percent:.1f}%)")
```

**Time-Based Updates:**
```python
logger.info(f"Waiting for job completion: {elapsed:.1f}/{max_wait:.1f} minutes")
```

**Counters:**
```python
logger.info(f"Jobs complete: {complete}/{total}")
```

## Examples by Component

### Extraction Phase

```python
logger.info("Starting extraction")
logger.info(f"  Source: {source}")
logger.info(f"  Table: {table_name}")
logger.info(f"  Mode: {refresh_mode}")

logger.info(f"Extracted {row_count:,} rows in {duration:.2f} seconds")
```

### Transform Phase

```python
logger.info("Starting transform")
logger.info(f"  Staging table: {staging_table}")
logger.info(f"  Applying schema: {len(fields)} fields")

logger.info(f"Transform complete: {row_count:,} rows written")
```

### Merge Phase

```python
logger.info("Starting merge")
logger.info(f"  Production table: {production_table}")
logger.info(f"  Merge strategy: {strategy}")

logger.info(f"Merge complete:")
logger.info(f"  Inserted: {inserted:,}")
logger.info(f"  Updated: {updated:,}")
logger.info(f"  Unchanged: {unchanged:,}")
```

### Error Handling

```python
try:
    # operation
except RateLimitError as e:
    logger.warning(f"Rate limit hit, waiting {wait_time:.1f} minutes")
    logger.info(f"  API message: {str(e)[:200]}")
except ConnectionError as e:
    logger.error(f"Connection failed: {e}")
    logger.error(f"  Retry {retry}/{max_retries}")
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    logger.debug(f"  Error type: {type(e).__name__}")
    logger.debug(f"  Traceback: {traceback.format_exc()}")
```

## Standardization Tool

Use the standardization script to ensure consistency:

```bash
# Standardize all logging output
python tools/standardize_logging.py
```

This script:
- Removes inappropriate leading spaces
- Preserves intentional indentation
- Ensures consistent formatting
- Reports number of changes made

## Verification

After making changes, verify logging:

```bash
# Check for leading spaces (should find only intentional indentation)
grep -n 'logger\.info.*" ' plugins/*.py

# Compile check
python -m py_compile plugins/facebook_extractor.py
python -m py_compile plugins/wordpress_extractor.py
python -m py_compile plugins/mailchimp_extractor.py
```

## Related Documentation

- [MODULE_MANUAL.md](MODULE_MANUAL.md) - Module architecture
- [USER_MANUAL.md](USER_MANUAL.md) - Pipeline usage
- [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) - Development guidelines
