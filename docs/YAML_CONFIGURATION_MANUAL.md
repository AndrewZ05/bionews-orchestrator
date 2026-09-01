# YAML Configuration Manual
## Complete Guide to Configuring Data Sources

**Last Updated**: 2025-01-21
**Version**: 1.0

---

## Table of Contents
1. [Overview](#overview)
2. [YAML File Structure](#yaml-file-structure)
3. [Adding a New Data Source](#adding-a-new-data-source)
4. [Configuration Sections](#configuration-sections)
5. [Schema Definitions](#schema-definitions)
6. [Incremental Strategies](#incremental-strategies)
7. [Advanced Configuration](#advanced-configuration)
8. [Validation & Testing](#validation--testing)
9. [Examples](#examples)
10. [Troubleshooting](#troubleshooting)

---

## Overview

The Orchestrator system uses YAML configuration files to define data sources, tables, schemas, and extraction behavior. This approach enables:

- **Configuration without code changes**: Add tables, change schemas, adjust settings without touching Python code
- **Environment variable substitution**: Keep secrets in `.env` file
- **Hierarchical configuration**: Base settings + source overrides
- **Self-documenting**: YAML files serve as documentation

### Configuration Files

```
configs/
├── defaults.yaml       # Base settings for all sources
├── facebook.yaml       # Facebook-specific configuration
├── wordpress.yaml      # WordPress-specific configuration
├── alerting.yaml       # Email/Slack notification settings
└── {newsource}.yaml    # Your new source configuration
```

---

## YAML File Structure

### Minimal Source Configuration

```yaml
# configs/newsource.yaml

# Source connection details
source:
  type: newsource                    # Source identifier
  api_endpoint: https://api.example.com/v1
  api_key: ${API_KEY}                # From .env file

# Pipeline settings
pipeline:
  staging_dataset: newsource_staging
  production_dataset: newsource_data
  gcs_bucket: ${GCS_BUCKET}

# Table/resource definitions
resources:
  tablename:
    table_name: newsource_tablename  # BigQuery table name
    primary_key: [id]                # Primary key columns

    schema:
      id: STRING
      name: STRING
      created_at: TIMESTAMP
```

### Complete Source Configuration

```yaml
# configs/newsource.yaml

# ============================================================================
# SOURCE CONNECTION CONFIGURATION
# ============================================================================
source:
  type: newsource
  api_endpoint: https://api.example.com/v1
  api_version: v1
  api_key: ${NEWSOURCE_API_KEY}          # Environment variable
  api_secret: ${NEWSOURCE_API_SECRET}    # Environment variable
  timeout_seconds: 30
  max_retries: 3

  # Default sites/accounts (can be overridden with --sites)
  sites:
    - site1
    - site2
    - site3

  # Rate limiting
  rate_limit:
    requests_per_second: 10
    requests_per_minute: 600

# ============================================================================
# PIPELINE CONFIGURATION
# ============================================================================
pipeline:
  # BigQuery datasets
  staging_dataset: newsource_staging
  production_dataset: newsource_data

  # GCS bucket for data lake
  gcs_bucket: ${GCS_BUCKET}

  # Local temp directory
  temp_dir: /tmp/orchestrator_newsource

  # Cleanup settings
  cleanup_local_files: true
  keep_files_on_error: true

# ============================================================================
# TABLE/RESOURCE DEFINITIONS
# ============================================================================
resources:
  # -------------------------------------------------------------------------
  # Table 1: Users
  # -------------------------------------------------------------------------
  users:
    # BigQuery table name
    table_name: newsource_users

    # Primary key columns (for MERGE deduplication)
    primary_key: [id]

    # Incremental extraction settings
    incremental:
      mode: lookback              # lookback, cursor, or timestamp
      lookback_days: 7            # How many days to look back
      cursor_field: updated_at    # Field for incremental filtering

    # Validation rules (optional)
    validations:
      - type: not_null
        columns: [id, email]
      - type: unique
        columns: [id]

    # Schema definition
    schema:
      id: STRING
      email: STRING
      username: STRING
      full_name: STRING
      created_at: TIMESTAMP
      updated_at: TIMESTAMP
      is_active: BOOLEAN
      login_count: INT64
      last_login_ip: STRING

  # -------------------------------------------------------------------------
  # Table 2: Orders
  # -------------------------------------------------------------------------
  orders:
    table_name: newsource_orders
    primary_key: [id]

    incremental:
      mode: cursor
      cursor_field: order_date

    schema:
      id: STRING
      user_id: STRING
      order_date: TIMESTAMP
      status: STRING
      total_amount: FLOAT64
      currency: STRING
      items: STRING              # JSON string for complex data
      shipping_address: STRING   # JSON string for complex data

  # -------------------------------------------------------------------------
  # Table 3: Analytics
  # -------------------------------------------------------------------------
  analytics:
    table_name: newsource_analytics
    primary_key: [date, metric_name]

    incremental:
      mode: timestamp
      date_field: date
      lookback_days: 3

    schema:
      date: DATE
      metric_name: STRING
      metric_value: FLOAT64
      dimensions: STRING         # JSON string

# ============================================================================
# EXTRACTION SETTINGS (OPTIONAL)
# ============================================================================
extraction:
  # Batch settings
  batch_size: 1000
  parallel_requests: 5

  # Retry settings
  retry_on_errors:
    - timeout
    - rate_limit
    - server_error

  max_retry_attempts: 3
  retry_backoff_seconds: [1, 5, 15]  # Exponential backoff

# ============================================================================
# MONITORING & ALERTS (OPTIONAL)
# ============================================================================
monitoring:
  # Job execution alerts
  alert_on_failure: true
  alert_on_success: false
  alert_on_long_running: true
  long_running_threshold_minutes: 120

  # Email recipients (if different from alerting.yaml)
  notify_emails:
    - data-team@company.com

# ============================================================================
# SCHEMA METADATA (AUTO-GENERATED)
# ============================================================================
schema_metadata:
  last_updated: 2025-01-21T14:30:00Z
  discovery_source: Manual configuration
  version: 1.0
```

---

## Adding a New Data Source

### Step-by-Step Guide

#### Step 1: Create YAML Configuration File

**File**: `configs/newsource.yaml`

```yaml
# New Source Configuration
# Created: 2025-01-21
# Purpose: Extract data from NewSource API

source:
  type: newsource
  api_endpoint: https://api.newsource.com/v1
  api_key: ${NEWSOURCE_API_KEY}

pipeline:
  staging_dataset: newsource_staging
  production_dataset: newsource_data
  gcs_bucket: ${GCS_BUCKET}

resources:
  # Start with one table
  users:
    table_name: newsource_users
    primary_key: [id]

    incremental:
      mode: lookback
      lookback_days: 7
      cursor_field: updated_at

    schema:
      id: STRING
      email: STRING
      name: STRING
      created_at: TIMESTAMP
      updated_at: TIMESTAMP
```

#### Step 2: Add Environment Variables

**File**: `.env`

```bash
# Add to .env file

# NewSource API credentials
NEWSOURCE_API_KEY=your_api_key_here
NEWSOURCE_API_SECRET=your_api_secret_here
```

#### Step 3: Create BigQuery Datasets

```bash
# Create staging dataset
bq mk --dataset bi-data-391216:newsource_staging

# Create production dataset
bq mk --dataset bi-data-391216:newsource_data
```

#### Step 4: Test Configuration Loading

```bash
# Test YAML loads correctly
python -c "
from shared.config_loader import load_config
config = load_config('newsource')
print('Source type:', config.get('source', {}).get('type'))
print('API endpoint:', config.get('source', {}).get('api_endpoint'))
print('Tables:', list(config.get('resources', {}).keys()))
"
```

**Expected output**:
```
Source type: newsource
API endpoint: https://api.newsource.com/v1
Tables: ['users']
```

#### Step 5: Create Extractor Plugin

See [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) for creating the Python extractor.

#### Step 6: Run First Extraction

```bash
# First run with rebuild mode
python orchestrate.py \
  --source newsource \
  --tables users \
  --refresh full \
  --rebuild \
  --log-level debug
```

---

## Configuration Sections

### 1. Source Section

**Purpose**: Define connection details and authentication

```yaml
source:
  # Required fields
  type: newsource                      # Source identifier (must match file name)
  api_endpoint: https://api.example.com/v1

  # Authentication (use environment variables for secrets)
  api_key: ${NEWSOURCE_API_KEY}
  api_secret: ${NEWSOURCE_API_SECRET}
  api_token: ${NEWSOURCE_TOKEN}

  # Optional fields
  api_version: v1                      # API version
  timeout_seconds: 30                  # Request timeout
  max_retries: 3                       # Retry attempts

  # Default sites/accounts
  sites:
    - site1
    - site2

  # Rate limiting
  rate_limit:
    requests_per_second: 10
    requests_per_minute: 600
    burst_limit: 50
```

**Environment Variable Substitution**:
- Format: `${VARIABLE_NAME}`
- Loaded from `.env` file
- Fails if variable not set (use `${VAR:-default}` for defaults)

### 2. Pipeline Section

**Purpose**: Define BigQuery and GCS settings

```yaml
pipeline:
  # Required: BigQuery datasets
  staging_dataset: newsource_staging        # External tables
  production_dataset: newsource_data        # Final tables

  # Required: GCS bucket
  gcs_bucket: ${GCS_BUCKET}                # Data lake bucket

  # Optional: Override defaults
  temp_dir: /tmp/orchestrator_newsource   # Local temp directory
  cleanup_local_files: true                # Clean up after success
  keep_files_on_error: true                # Keep files on failure
```

### 3. Resources Section

**Purpose**: Define tables and their schemas

```yaml
resources:
  tablename:
    # Table configuration
    table_name: newsource_tablename    # BigQuery table name
    primary_key: [id]                  # Primary key columns
    description: "User account data"   # Optional description

    # Incremental extraction
    incremental:
      mode: lookback                   # lookback, cursor, or timestamp
      lookback_days: 7                 # Days to overlap
      cursor_field: updated_at         # Field for filtering

    # Validations (optional)
    validations:
      - type: not_null
        columns: [id, email]
      - type: unique
        columns: [id]

    # Schema definition
    schema:
      id: STRING
      name: STRING
      created_at: TIMESTAMP
```

---

## Schema Definitions

### Supported BigQuery Types

| BigQuery Type | Description | Example YAML | Example Value |
|---------------|-------------|--------------|---------------|
| `STRING` | Text data | `name: STRING` | `"John Doe"` |
| `INT64` | Integer | `count: INT64` | `12345` |
| `FLOAT64` | Decimal | `amount: FLOAT64` | `123.45` |
| `BOOLEAN` | True/False | `is_active: BOOLEAN` | `true` |
| `TIMESTAMP` | Date + Time | `created_at: TIMESTAMP` | `2025-01-21T14:30:00Z` |
| `DATE` | Date only | `birth_date: DATE` | `2025-01-21` |
| `DATETIME` | Date + Time (no TZ) | `event_time: DATETIME` | `2025-01-21 14:30:00` |
| `TIME` | Time only | `start_time: TIME` | `14:30:00` |
| `JSON` | JSON data (BQ native) | `metadata: JSON` | `{"key": "value"}` |
| `ARRAY<TYPE>` | Arrays | `tags: ARRAY<STRING>` | `["tag1", "tag2"]` |
| `STRUCT` | Nested records | Complex | See below |

### Basic Schema Example

```yaml
schema:
  # Text fields
  id: STRING
  email: STRING
  username: STRING
  description: STRING

  # Numeric fields
  age: INT64
  score: FLOAT64
  login_count: INT64

  # Boolean fields
  is_active: BOOLEAN
  is_verified: BOOLEAN

  # Date/Time fields
  created_at: TIMESTAMP
  updated_at: TIMESTAMP
  birth_date: DATE
  last_login: DATETIME

  # Complex data (stored as JSON strings)
  metadata: STRING          # Use STRING for JSON data
  settings: STRING          # Easier to handle than native JSON
  tags: STRING              # Serialize arrays as JSON strings
```

### Handling Complex Data

**Option 1: JSON Strings** (Recommended)
```yaml
schema:
  user_id: STRING
  preferences: STRING       # Store as JSON string
  tags: STRING             # Store array as JSON string
  address: STRING          # Store nested object as JSON string
```

**In extractor**:
```python
import json

record = {
    'user_id': '123',
    'preferences': json.dumps({'theme': 'dark', 'notifications': True}),
    'tags': json.dumps(['vip', 'beta-tester']),
    'address': json.dumps({'street': '123 Main', 'city': 'NYC'})
}
```

**Querying JSON strings**:
```sql
SELECT
  user_id,
  JSON_EXTRACT_SCALAR(preferences, '$.theme') as theme,
  JSON_EXTRACT_ARRAY(tags) as tags_array
FROM newsource_data.users
```

**Option 2: Native JSON** (Advanced)
```yaml
schema:
  user_id: STRING
  preferences: JSON         # BigQuery native JSON
```

**Note**: Native JSON requires proper type handling in extractor

**Option 3: Flattened Columns** (Best for analytics)
```yaml
schema:
  user_id: STRING
  # Flatten nested preferences
  pref_theme: STRING
  pref_notifications: BOOLEAN
  pref_language: STRING
  # Flatten address
  address_street: STRING
  address_city: STRING
  address_country: STRING
```

### Schema Best Practices

1. **Use STRING for IDs**: Even if numeric, store IDs as STRING
   ```yaml
   id: STRING              # Not INT64
   user_id: STRING         # Not INT64
   account_id: STRING      # Not INT64
   ```

2. **Use TIMESTAMP for dates**: Includes timezone info
   ```yaml
   created_at: TIMESTAMP   # Not DATETIME
   updated_at: TIMESTAMP   # Not DATETIME
   ```

3. **JSON strings for complex data**: Easier to handle than native types
   ```yaml
   metadata: STRING        # Store JSON as string
   tags: STRING           # Store arrays as JSON strings
   ```

4. **Consistent naming**: Use snake_case
   ```yaml
   user_id: STRING         # Not userId or UserID
   created_at: TIMESTAMP   # Not createdAt or CreatedAt
   ```

5. **Add descriptions**: Document complex fields
   ```yaml
   schema:
     metadata: STRING      # JSON object with user settings
     tags: STRING          # JSON array of user tags
   ```

---

## Incremental Strategies

### Mode 1: Lookback

**Best for**: APIs with `updated_at` or `modified_at` fields

```yaml
incremental:
  mode: lookback
  lookback_days: 7              # Days to look back
  cursor_field: updated_at      # Field to filter on
```

**How it works**:
1. Get last extraction time: `2025-01-14T00:00:00Z`
2. Calculate lookback: `2025-01-14 - 7 days = 2025-01-07`
3. Extract: `WHERE updated_at >= '2025-01-07'`
4. Merge to production with hash-based deduplication

**Use case**: Daily updates with 7-day overlap to catch late-arriving data

### Mode 2: Cursor

**Best for**: Paginated APIs with cursor/offset

```yaml
incremental:
  mode: cursor
  cursor_field: id              # Field for cursor
  cursor_type: numeric          # numeric or string
```

**How it works**:
1. Get last cursor: `id = 12345`
2. Extract: `WHERE id > 12345`
3. Store new max cursor for next run

**Use case**: Append-only data (logs, events)

### Mode 3: Timestamp

**Best for**: Time-series data with date dimensions

```yaml
incremental:
  mode: timestamp
  date_field: event_date        # Date field
  lookback_days: 3              # Days to overlap
```

**How it works**:
1. Get last date: `2025-01-18`
2. Calculate lookback: `2025-01-18 - 3 days = 2025-01-15`
3. Extract: `WHERE event_date >= '2025-01-15'`

**Use case**: Daily metrics, analytics data

### Mode 4: Full Refresh Only

**Best for**: Small tables, snapshot tables

```yaml
incremental:
  mode: full                    # Always full refresh
```

**How it works**:
- Always extracts all data
- No date filtering
- MERGE replaces changed rows

**Use case**: Configuration tables, reference data

### Incremental Examples

#### Example 1: User Accounts (Lookback)

```yaml
users:
  table_name: newsource_users
  primary_key: [id]

  incremental:
    mode: lookback
    lookback_days: 7
    cursor_field: updated_at

  schema:
    id: STRING
    email: STRING
    updated_at: TIMESTAMP
```

**Extraction behavior**:
- Daily run: Gets users updated in last 7 days
- First run: Gets all users (no previous extraction)
- Overlap catches late updates

#### Example 2: Event Logs (Cursor)

```yaml
events:
  table_name: newsource_events
  primary_key: [event_id]

  incremental:
    mode: cursor
    cursor_field: event_id
    cursor_type: numeric

  schema:
    event_id: INT64
    event_type: STRING
    created_at: TIMESTAMP
```

**Extraction behavior**:
- Daily run: Gets events with `event_id > last_max_id`
- Append-only (no updates)
- No duplicates

#### Example 3: Daily Metrics (Timestamp)

```yaml
metrics:
  table_name: newsource_metrics
  primary_key: [date, metric_name]

  incremental:
    mode: timestamp
    date_field: date
    lookback_days: 3

  schema:
    date: DATE
    metric_name: STRING
    metric_value: FLOAT64
```

**Extraction behavior**:
- Daily run: Gets last 3 days of metrics
- Allows metric corrections
- Deduplicates by (date, metric_name)

---

## Advanced Configuration

### Multi-Site Configuration

```yaml
source:
  type: newsource

  # Define sites with different credentials
  sites:
    site1:
      api_key: ${SITE1_API_KEY}
      api_endpoint: https://site1.api.com
    site2:
      api_key: ${SITE2_API_KEY}
      api_endpoint: https://site2.api.com
    site3:
      api_key: ${SITE3_API_KEY}
      api_endpoint: https://site3.api.com

pipeline:
  staging_dataset: newsource_staging
  production_dataset: newsource_data
  gcs_bucket: ${GCS_BUCKET}

resources:
  users:
    table_name: newsource_users
    primary_key: [site_id, user_id]  # Include site_id in PK

    schema:
      site_id: STRING               # Add site identifier
      user_id: STRING
      name: STRING
```

### Table-Specific Settings

```yaml
resources:
  users:
    table_name: newsource_users
    primary_key: [id]

    # Override pipeline defaults for this table
    pipeline_overrides:
      batch_size: 5000              # Larger batches
      parallel_requests: 10         # More parallelism

    # Table-specific validation
    validations:
      - type: not_null
        columns: [id, email]
      - type: email_format
        columns: [email]
      - type: range
        column: age
        min: 0
        max: 150

    schema:
      id: STRING
      email: STRING
      age: INT64
```

### Partitioning & Clustering

```yaml
resources:
  events:
    table_name: newsource_events
    primary_key: [event_id]

    # BigQuery table options
    table_options:
      partition_field: event_date   # Partition by date
      partition_type: DAY           # DAY, MONTH, YEAR
      clustering_fields:            # Cluster for query performance
        - event_type
        - user_id

    schema:
      event_id: STRING
      event_date: DATE
      event_type: STRING
      user_id: STRING
```

### Custom Transformations

```yaml
resources:
  users:
    table_name: newsource_users
    primary_key: [id]

    # SQL transformations applied during MERGE
    transformations:
      # Computed columns
      computed:
        full_name: "CONCAT(first_name, ' ', last_name)"
        age_group: "CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END"

      # Filters (exclude rows)
      filters:
        - "is_deleted = FALSE"
        - "is_test_user = FALSE"

    schema:
      id: STRING
      first_name: STRING
      last_name: STRING
      age: INT64
      full_name: STRING             # Computed
      age_group: STRING             # Computed
```

---

## Validation & Testing

### Test Configuration File

```bash
# Test 1: YAML syntax is valid
python -c "
import yaml
with open('configs/newsource.yaml') as f:
    config = yaml.safe_load(f)
print('[PASS] YAML syntax valid')
"

# Test 2: Configuration loads correctly
python -c "
from shared.config_loader import load_config
config = load_config('newsource')
print('[PASS] Configuration loaded')
print(f'  Source type: {config[\"source\"][\"type\"]}')
print(f'  Tables: {list(config[\"resources\"].keys())}')
"

# Test 3: Environment variables are set
python -c "
from shared.config_loader import load_config
config = load_config('newsource')
api_key = config['source'].get('api_key')
if api_key and not api_key.startswith('${'):
    print('[PASS] Environment variables substituted')
else:
    print('[FAIL] Environment variables not set')
"

# Test 4: Schema validation
python -c "
from shared.schema_registry import get_schema
schema = get_schema('newsource', 'users')
print(f'[PASS] Schema loaded: {len(schema)} columns')
print(f'  Columns: {list(schema.keys())}')
"
```

### Validate Against Existing Data

```bash
# Extract sample data
python orchestrate.py \
  --source newsource \
  --tables users \
  --extract-only \
  --keep-local

# Inspect Parquet file
python -c "
import pandas as pd
df = pd.read_parquet('/tmp/orchestrator_newsource_*/users.parquet')
print(df.info())
print(df.head())

# Check for schema issues
print('\nColumn types:')
print(df.dtypes)
"
```

---

## Examples

### Example 1: REST API Source

```yaml
# configs/stripe.yaml
# Extract data from Stripe API

source:
  type: stripe
  api_endpoint: https://api.stripe.com/v1
  api_key: ${STRIPE_API_KEY}
  api_version: 2023-10-16

pipeline:
  staging_dataset: stripe_staging
  production_dataset: stripe_data
  gcs_bucket: ${GCS_BUCKET}

resources:
  customers:
    table_name: stripe_customers
    primary_key: [id]

    incremental:
      mode: lookback
      lookback_days: 7
      cursor_field: created

    schema:
      id: STRING
      email: STRING
      name: STRING
      description: STRING
      created: TIMESTAMP
      metadata: STRING          # JSON string

  charges:
    table_name: stripe_charges
    primary_key: [id]

    incremental:
      mode: lookback
      lookback_days: 7
      cursor_field: created

    schema:
      id: STRING
      customer_id: STRING
      amount: INT64
      currency: STRING
      status: STRING
      created: TIMESTAMP
      metadata: STRING
```

### Example 2: Database Source (PostgreSQL)

```yaml
# configs/postgres.yaml
# Extract from PostgreSQL database

source:
  type: postgres
  host: ${POSTGRES_HOST}
  port: 5432
  database: ${POSTGRES_DATABASE}
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
  ssl_mode: require

pipeline:
  staging_dataset: postgres_staging
  production_dataset: postgres_data
  gcs_bucket: ${GCS_BUCKET}

resources:
  orders:
    table_name: postgres_orders
    primary_key: [order_id]

    # SQL query for extraction
    query: |
      SELECT
        order_id,
        customer_id,
        order_date,
        status,
        total_amount
      FROM orders
      WHERE updated_at >= '{start_date}'

    incremental:
      mode: lookback
      lookback_days: 7
      cursor_field: updated_at

    schema:
      order_id: STRING
      customer_id: STRING
      order_date: DATE
      status: STRING
      total_amount: FLOAT64
```

### Example 3: File-Based Source (CSV/JSON)

```yaml
# configs/files.yaml
# Extract from files in GCS

source:
  type: files
  gcs_bucket: source-data-bucket
  file_format: csv              # csv, json, parquet

pipeline:
  staging_dataset: files_staging
  production_dataset: files_data
  gcs_bucket: ${GCS_BUCKET}

resources:
  sales:
    table_name: files_sales
    primary_key: [transaction_id]

    # File path pattern
    file_path: gs://source-data-bucket/sales/daily/*.csv

    # CSV options
    csv_options:
      delimiter: ','
      header: true
      skip_rows: 0

    incremental:
      mode: full              # Always process all files

    schema:
      transaction_id: STRING
      date: DATE
      product_id: STRING
      quantity: INT64
      amount: FLOAT64
```

---

## Troubleshooting

### Issue 1: YAML Syntax Error

**Error**:
```
yaml.scanner.ScannerError: mapping values are not allowed here
```

**Cause**: Invalid YAML syntax (usually indentation)

**Solution**:
```bash
# Validate YAML syntax
python -c "
import yaml
with open('configs/newsource.yaml') as f:
    yaml.safe_load(f)
"

# Use YAML linter
yamllint configs/newsource.yaml
```

### Issue 2: Environment Variable Not Substituted

**Error**:
```
API key is ${NEWSOURCE_API_KEY} (not substituted)
```

**Cause**: Environment variable not set in `.env`

**Solution**:
```bash
# Check .env file
grep NEWSOURCE_API_KEY .env

# Add to .env if missing
echo "NEWSOURCE_API_KEY=your_key_here" >> .env

# Reload environment
source .env
```

### Issue 3: Schema Mismatch

**Error**:
```
Column 'new_field' exists in data but not in schema
```

**Cause**: Schema in YAML doesn't match actual data

**Solution**:
```bash
# Option 1: Update YAML schema
# Edit configs/newsource.yaml, add missing column

# Option 2: Use rebuild mode to discover schema
python orchestrate.py \
  --source newsource \
  --tables tablename \
  --rebuild
```

### Issue 4: Primary Key Error

**Error**:
```
PRIMARY KEY columns [id] contain NULL values
```

**Cause**: Primary key columns have NULL values in data

**Solution**:
```yaml
# Option 1: Change primary key
primary_key: [user_id]  # Use column without NULLs

# Option 2: Filter out NULLs in extractor
# Add filter in Python extractor:
data = [row for row in data if row.get('id') is not None]
```

### Issue 5: Incremental Not Working

**Error**:
```
Extracting all data instead of incremental
```

**Cause**: No extraction state found (first run) or cursor_field missing

**Solution**:
```sql
-- Check extraction state
SELECT * FROM orchestrator_monitoring.extraction_state
WHERE source = 'newsource' AND table_name = 'tablename'

-- If empty, first run will be full refresh (expected)

-- Verify cursor_field exists in schema
```

---

## Quick Reference

### Minimal Configuration Template

```yaml
source:
  type: newsource
  api_endpoint: https://api.example.com
  api_key: ${API_KEY}

pipeline:
  staging_dataset: newsource_staging
  production_dataset: newsource_data
  gcs_bucket: ${GCS_BUCKET}

resources:
  tablename:
    table_name: newsource_tablename
    primary_key: [id]

    incremental:
      mode: lookback
      lookback_days: 7
      cursor_field: updated_at

    schema:
      id: STRING
      name: STRING
      created_at: TIMESTAMP
      updated_at: TIMESTAMP
```

### Common Field Types

```yaml
schema:
  # IDs (always STRING)
  id: STRING
  user_id: STRING

  # Text
  name: STRING
  description: STRING

  # Numbers
  count: INT64
  amount: FLOAT64

  # Booleans
  is_active: BOOLEAN

  # Dates/Times
  created_at: TIMESTAMP
  birth_date: DATE

  # Complex (JSON strings)
  metadata: STRING
  tags: STRING
```

### Testing Commands

```bash
# 1. Validate YAML
python -c "import yaml; yaml.safe_load(open('configs/newsource.yaml'))"

# 2. Test config loading
python -c "from shared.config_loader import load_config; load_config('newsource')"

# 3. Test extraction
python orchestrate.py --source newsource --tables tablename --extract-only

# 4. First production run
python orchestrate.py --source newsource --tables tablename --rebuild
```

---

## Checklist for Adding New Source

- [ ] Create `configs/newsource.yaml` with source, pipeline, resources sections
- [ ] Add environment variables to `.env` file
- [ ] Test YAML syntax: `python -c "import yaml; yaml.safe_load(open('configs/newsource.yaml'))"`
- [ ] Test config loading: `python -c "from shared.config_loader import load_config; load_config('newsource')"`
- [ ] Create BigQuery datasets: `bq mk newsource_staging` and `bq mk newsource_data`
- [ ] Create Python extractor plugin (see IMPLEMENTATION_GUIDE.md)
- [ ] Test extraction: `python orchestrate.py --source newsource --tables tablename --extract-only`
- [ ] First production run: `python orchestrate.py --source newsource --tables tablename --rebuild`
- [ ] Verify data in BigQuery: `SELECT COUNT(*) FROM newsource_data.tablename`
- [ ] Add to daily cron schedule
- [ ] Document source in README.md

---

**End of YAML Configuration Manual**

For implementation details, see [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md)
For usage examples, see [USER_MANUAL.md](USER_MANUAL.md)
